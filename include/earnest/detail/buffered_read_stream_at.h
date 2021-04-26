#ifndef EARNEST_DETAIL_BUFFERED_READ_STREAM_AT_H
#define EARNEST_DETAIL_BUFFERED_READ_STREAM_AT_H

#include <cstddef>
#include <limits>
#include <memory>
#include <utility>

#include <earnest/detail/allocated_buffer.h>
#include <earnest/detail/clamp_buffer.h>
#include <earnest/detail/concat_buffer.h>

#include <asio/associated_allocator.hpp>
#include <asio/associated_executor.hpp>
#include <asio/async_result.hpp>
#include <asio/bind_executor.hpp>
#include <asio/buffer.hpp>
#include <asio/error.hpp>

namespace earnest::detail {


template<typename Source,
    typename Executor = typename Source::executor_type,
    typename Alloc = std::allocator<void>>
class buffered_read_stream_at {
  public:
  static constexpr std::uint64_t max_filelen = std::numeric_limits<std::uint64_t>::max();
  static constexpr std::size_t default_bufsiz = 64 * 1024;
  using executor_type = Executor;
  using allocator_type = typename allocated_buffer<Alloc>::allocator_type;

  buffered_read_stream_at() = default;

  buffered_read_stream_at(const buffered_read_stream_at&) = delete;
  buffered_read_stream_at& operator=(const buffered_read_stream_at&) = delete;

  buffered_read_stream_at(buffered_read_stream_at&& o) noexcept
  : ex_(std::move(o.ex_)),
    src_(std::exchange(o.src_, nullptr)),
    storage_(std::move(o.storage_)),
    buf_(std::move(o.buf_)),
    file_off_(std::move(o.file_off_)),
    file_len_(std::move(o.file_len_))
  {}

  buffered_read_stream_at& operator=(buffered_read_stream_at&& o) noexcept {
    ex_ = std::move(o.ex_);
    src_ = std::exchange(o.src_, nullptr);
    storage_ = std::move(o.storage_);
    buf_ = std::move(o.buf_);
    file_off_ = std::move(o.file_off_);
    file_len_ = std::move(o.file_len_);
    return *this;
  }

  buffered_read_stream_at(const executor_type& ex, Source& src, std::uint64_t off, std::uint64_t len, std::size_t bufsiz, allocator_type alloc = allocator_type())
  : ex_(ex),
    src_(&src),
    storage_(bufsiz, alloc),
    file_off_(off),
    file_len_(len)
  {
    void*const nil = nullptr;
    buf_ = asio::buffer(nil, 0);
  }

  buffered_read_stream_at(Source& src, std::uint64_t off, std::uint64_t len, std::size_t bufsiz, allocator_type alloc = allocator_type())
  : buffered_read_stream_at(src.get_executor(), src, off, len, bufsiz, alloc)
  {}

  buffered_read_stream_at(const executor_type& ex, Source& src, std::uint64_t off, std::uint64_t len, allocator_type alloc = allocator_type())
  : buffered_read_stream_at(ex, src, off, len, default_bufsiz, alloc)
  {}

  buffered_read_stream_at(Source& src, std::uint64_t off, std::uint64_t len, allocator_type alloc = allocator_type())
  : buffered_read_stream_at(src.get_executor(), src, off, len, alloc)
  {}

  auto get_allocator() const -> allocator_type {
    return storage_.get_allocator();
  }

  auto get_executor() const -> executor_type {
    return ex_;
  }

  template<typename MB>
  auto read_some(MB&& mb) -> std::size_t {
    std::error_code ec;
    const std::size_t s = read_some(std::forward<MB>(mb), ec);
    if (ec) throw std::system_error(ec);
    return s;
  }

  template<typename MB>
  auto read_some(MB&& mb, std::error_code& ec) -> std::size_t {
    ec.clear();
    const auto mb_size = asio::buffer_size(mb);

    if (mb_size == 0) return 0;
    if (file_len_ == 0) {
      ec = asio::stream_errc::eof;
      return 0;
    }

    if (buf_.size() != 0) {
      const std::size_t s = asio::buffer_copy(mb, buf_, std::min(file_len_, std::numeric_limits<std::size_t>::max()));
      buf_ += s;
      file_off_ += s;
      file_len_ -= s;
      return s;
    }

    const std::size_t s = src_->read_some(
        clamp_buffer(
            concat_buffer(
                std::forward<MB>(mb),
                asio::buffer(storage_)),
            std::min(file_len_, std::numeric_limits<std::size_t>::max())),
        ec);
    if (ec) return 0;
    if (s > mb_size) {
      buf_ = asio::buffer(storage_, s - mb_size);
      s = mb_size;
    }
    file_off_ += s;
    file_len_ -= s;
    return s;
  }

  template<typename MB, typename CompletionToken>
  auto async_read_some(MB&& mb, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, std::size_t)>::return_type {
    using init_type = asio::async_completion<CompletionToken, void(std::error_code, std::size_t)>;

    const auto mb_size = asio::buffer_size(mb);
    init_type init(token);
    auto alloc = asio::associated_allocator<typename init_type::completion_handler_type, allocator_type>::get(init.completion_handler, get_allocator());
    auto ex = asio::associated_executor<typename init_type::completion_handler_type, executor_type>::get(init.completion_handler, ex_);

    if (mb_size == 0) {
      ex.post(
          [h=std::move(init.completion_handler)]() mutable { h(std::error_code(), std::size_t(0)); },
          alloc);
    } else if (file_len_ == 0) {
      ex.post(
          [h=std::move(init.completion_handler)]() mutable { h(std::error_code(asio::stream_errc::eof), std::size_t(0)); },
          alloc);
    } else if (buf_.size() != 0) {
      const std::size_t s = asio::buffer_copy(mb, buf_, std::min(file_len_, std::numeric_limits<std::size_t>::max()));
      buf_ += s;
      file_off_ += s;
      file_len_ -= s;
      ex.post(
          [h=std::move(init.completion_handler), s]() mutable { h(std::error_code(), s); },
          alloc);
    } else {
      src_->async_read_some_at(
          file_off_,
          clamp_buffer(
              concat_buffer(
                  std::forward<MB>(mb),
                  asio::buffer(storage_)),
              std::min(file_len_, std::numeric_limits<std::size_t>::max())),
          asio::bind_executor(
              ex,
              [ this,
                h=std::move(init.completion_handler),
                mb_size
              ](std::error_code ec, std::size_t s) mutable {
                if (!ec) {
                  if (s > mb_size) {
                    buf_ = asio::buffer(storage_, s - mb_size);
                    s = mb_size;
                  }
                  file_off_ += s;
                  file_len_ -= s;
                }

                h(ec, s);
              }));
    }

    return init.result.get();
  }

  auto offset() const noexcept -> std::uint64_t {
    return file_off_;
  }

  auto max_offset() const noexcept -> std::uint64_t {
    return file_off_ + std::min(std::numeric_limits<std::uint64_t>::max() - file_off_, file_len_);
  }

  void max_offset(std::uint64_t new_max_off) {
    file_len_ = std::max(new_max_off, file_off_) - file_off_;
  }

  void advance(std::uint64_t bytes) {
    std::error_code ec;
    advance(bytes, ec);
    if (ec) throw std::system_error(ec, "buffered_reader::advance");
  }

  void advance(std::uint64_t bytes, std::error_code& ec) noexcept {
    if (file_len_ < bytes) {
      ec = asio::stream_errc::eof;
      return;
    }

    ec.clear();
    buf_ += std::min(bytes, buf_.size());
    file_off_ += bytes;
    file_len_ -= bytes;
  }

  auto peek() const noexcept -> asio::const_buffer {
    return asio::buffer(buf_, std::min(file_len_, std::numeric_limits<std::size_t>::max()));
  }

  auto underlying_source() const -> Source& {
    assert(src_ != nullptr);
    return *src_;
  }

  private:
  executor_type ex_;
  Source* src_ = nullptr;
  allocated_buffer<Alloc> storage_;
  asio::mutable_buffer buf_;
  std::uint64_t file_off_;
  std::uint64_t file_len_;
};


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_BUFFERED_READ_STREAM_AT_H */
