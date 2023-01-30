#pragma once

#include <cstddef>
#include <iterator>
#include <memory>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include <asio/async_result.hpp>
#include <asio/buffer.hpp>
#include <asio/completion_condition.hpp>
#include <asio/error.hpp>
#include <asio/post.hpp>
#include <asio/read_at.hpp>

#include <earnest/detail/completion_barrier.h>
#include <earnest/detail/completion_handler_fun.h>
#include <earnest/detail/completion_wrapper.h>

namespace earnest::detail {


template<typename UnderlyingFd, typename WalFD, typename Alloc = std::allocator<std::byte>>
class replacement_map_reader {
  public:
  using offset_type = typename replacement_map<WalFD, Alloc>::offset_type;
  using size_type = typename replacement_map<WalFD, Alloc>::size_type;
  using executor_type = typename std::remove_cvref_t<UnderlyingFd>::executor_type;

  replacement_map_reader(std::shared_ptr<UnderlyingFd> fd, std::shared_ptr<const replacement_map<WalFD, Alloc>> replacements, size_type file_size)
  : underlying_fd_(std::move(fd)),
    replacements_(std::move(replacements)),
    file_size_(std::move(file_size))
  {}

  auto get_executor() const -> executor_type { return underlying_fd_->get_executor(); }
  auto size() const -> size_type { return file_size_; }

  template<typename MB, typename CompletionToken>
  auto async_read_some_at(offset_type off, MB&& mb, CompletionToken&& token) const {
    if (off + asio::buffer_size(mb) < off) // Overflow
      throw std::invalid_argument("replacement_map_reader: off+len overflows");

    std::error_code ec;
    if (off > file_size_)
      ec = make_error_code(std::errc::invalid_argument);
    else if (off == file_size_ && asio::buffer_size(mb) > 0)
      ec = make_error_code(asio::stream_errc::eof);

    return asio::async_initiate<CompletionToken, void(std::error_code, std::size_t)>(
        [this](auto handler, offset_type off, auto mb, std::error_code initial_ec) {
          if (initial_ec) [[unlikely]] {
            completion_handler_fun(std::move(handler), this->get_executor()).post(initial_ec, 0);
            return;
          }

          // Clamp rlen to the file size.
          std::size_t rlen = asio::buffer_size(mb);
          if (rlen > file_size_ - off) rlen = file_size_ - off;
          std::vector<asio::mutable_buffer> bufs = mb_to_bufvector_(std::move(mb), rlen);

          if (this->replacements_ == nullptr) {
            this->underlying_fd_->async_read_some_at(off, std::move(bufs), std::move(handler));
            return;
          }

          auto barrier = make_completion_barrier(
              completion_wrapper<void(std::error_code)>(
                  std::move(handler),
                  [rlen](auto handler, std::error_code ec) {
                    std::invoke(handler, ec, rlen);
                  }),
              this->get_executor());

          typename replacement_map<WalFD, Alloc>::const_iterator repl_iter = this->replacements_->find(off);
          // We fan out the read, so that IO won't suffer (much) if it has lots of small edits.
          while (!bufs.empty()) {
            assert(repl_iter == this->replacements_->end() || repl_iter->end_offset() > off);
            assert(repl_iter == this->replacements_->begin() || std::prev(repl_iter)->end_offset() <= off);

            if (repl_iter == this->replacements_->end()) {
              const auto buf_bytes = asio::buffer_size(bufs);
              asio::async_read_at(
                  *this->underlying_fd_,
                  off,
                  std::move(bufs),
                  asio::transfer_exactly(buf_bytes),
                  completion_wrapper<void(std::error_code, std::size_t)>(
                      ++barrier,
                      [buf_bytes](auto handler, std::error_code ec, [[maybe_unused]] std::size_t bytes) {
                        assert(ec || bytes == buf_bytes);
                        std::invoke(handler, ec);
                      }));
              bufs.clear();

              off += buf_bytes;
            } else if (repl_iter->offset() > off) {
              auto segment_bufs = extract_bufs_(bufs, repl_iter->offset() - off);
              const auto buf_bytes = asio::buffer_size(segment_bufs);
              asio::async_read_at(
                  *this->underlying_fd_,
                  off,
                  std::move(segment_bufs),
                  asio::transfer_exactly(buf_bytes),
                  completion_wrapper<void(std::error_code, std::size_t)>(
                      ++barrier,
                      [buf_bytes](auto handler, std::error_code ec, [[maybe_unused]] std::size_t bytes) {
                        assert(ec || bytes == buf_bytes);
                        std::invoke(handler, ec);
                      }));

              off += buf_bytes;
            } else { // repl_iter->offset() < off && repl_iter->end_offset() > off
              auto segment_bufs = extract_bufs_(bufs, repl_iter->end_offset() - off);
              const auto buf_bytes = asio::buffer_size(segment_bufs);
              asio::async_read_at(
                  repl_iter->wal_file(),
                  off - repl_iter->offset() + repl_iter->wal_offset(),
                  std::move(segment_bufs),
                  asio::transfer_exactly(buf_bytes),
                  completion_wrapper<void(std::error_code, std::size_t)>(
                      ++barrier,
                      [buf_bytes](auto handler, std::error_code ec, [[maybe_unused]] std::size_t bytes) {
                        assert(ec || bytes == buf_bytes);
                        std::invoke(handler, ec);
                      }));

              off += buf_bytes;
              assert(off <= repl_iter->end_offset());
              ++repl_iter;
            }
          }

          std::invoke(barrier, std::error_code());
        },
        token, off, std::forward<MB>(mb), ec);
  }

  private:
  // Copy the buffers to a vector.
  // And ensure there are at most `rlen` bytes in that buffer.
  template<typename MB>
  static auto mb_to_bufvector_(MB&& mb, std::size_t rlen) -> std::vector<asio::mutable_buffer> {
    std::vector<asio::mutable_buffer> buffers;
    for (auto i = asio::buffer_sequence_begin(mb);
        i != asio::buffer_sequence_end(mb) && rlen != 0;
        ++i) {
      if (i->size() <= rlen) {
        rlen -= i->size();
        buffers.push_back(*i);
      } else { // i->size() > rlen
        buffers.push_back(asio::buffer(*i, rlen));
        rlen = 0;
      }
    }
    return buffers;
  }

  // Extract `rlen` bytes from bufs.
  // At the end, `rlen` bytes will have been taken from the start of `bufs`, and be returned.
  static auto extract_bufs_(std::vector<asio::mutable_buffer>& bufs, std::size_t rlen) -> std::vector<asio::mutable_buffer> {
    std::vector<asio::mutable_buffer> out;
    for (auto i = bufs.begin();
        i != bufs.end();
        ++i) {
      if (i->size() <= rlen) {
        rlen -= i->size();
        out.push_back(*i);
      } else { // i->size() > rlen
        if (rlen > 0) out.push_back(asio::buffer(*i, rlen));
        *i += rlen;
        bufs.erase(bufs.begin(), i);
        return out;
      }
    }

    // All of bufs was moved.
    bufs.clear();
    return out;
  }

  std::shared_ptr<UnderlyingFd> underlying_fd_;
  std::shared_ptr<const replacement_map<WalFD, Alloc>> replacements_;
  size_type file_size_;
};


} /* namespace earnest::detail */
