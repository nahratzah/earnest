#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include <earnest/detail/overload.h>
#include <earnest/execution.h>
#include <earnest/execution_io.h>
#include <earnest/execution_util.h>

#include "completion_handler_fun.h"
#include "completion_wrapper.h"

namespace earnest::detail {


template<typename AsyncReadDevice, typename Allocator>
class buffered_readstream_adapter {
  public:
  static constexpr std::size_t default_buffer_size = 64 * 1024;
  using offset_type = std::uint64_t;
  using next_layer_type = std::remove_reference_t<AsyncReadDevice>;
  using executor_type = typename next_layer_type::executor_type;
  using allocator_type = Allocator;

  template<typename Arg>
  explicit buffered_readstream_adapter(Arg&& a, std::size_t buffer_size, allocator_type allocator = allocator_type())
  : next_layer_(std::forward<Arg>(a)),
    buffer_space_(buffer_size, allocator),
    buffer_(buffer_space_.data(), 0)
  {}

  template<typename Arg>
  explicit buffered_readstream_adapter(Arg&& a, allocator_type allocator = allocator_type())
  : buffered_readstream_adapter(std::forward<Arg>(a), default_buffer_size, std::move(allocator))
  {}

  buffered_readstream_adapter(const buffered_readstream_adapter&) = delete;
  buffered_readstream_adapter& operator=(const buffered_readstream_adapter&) = delete;
  buffered_readstream_adapter(buffered_readstream_adapter&&) = default;
  buffered_readstream_adapter& operator=(buffered_readstream_adapter&&) = default;

  auto next_layer() -> next_layer_type& {
    return next_layer_;
  }

  auto next_layer() const -> const next_layer_type& {
    return next_layer_;
  }

  auto close() -> void {
    next_layer().close();
  }

  auto close(std::error_code& ec) -> void {
    next_layer().close(ec);
  }

  template<typename ReturnType = decltype(std::declval<const next_layer_type&>().position())>
  auto position() const noexcept(noexcept(std::declval<const next_layer_type&>().position())) -> ReturnType {
    return next_layer().position() - this->buffer_.size();
  }

  auto get_executor() const -> executor_type {
    return next_layer().get_executor();
  }

  auto get_allocator() const -> allocator_type {
    return buffer_space_.get_allocator();
  }

  template<typename ReturnType = decltype(std::declval<next_layer_type&>().skip(std::declval<offset_type>()))>
  auto skip(offset_type bytes) -> ReturnType {
    if (this->buffer_.size() >= bytes) {
      this->buffer_ = this->buffer_.subspan(bytes);
      bytes = 0;
    } else {
      bytes -= this->buffer_.size();
      this->buffer_ = this->buffer_.subspan(this->buffer_.size()); // Make the buffer empty.
    }

    return next_layer().skip(bytes);
  }

  friend auto tag_invoke(execution::io::lazy_read_some_ec_t tag, buffered_readstream_adapter& self, std::span<std::byte> buf) {
    using namespace execution;

    auto when_data_avail = [](buffered_readstream_adapter& self, std::span<std::byte> buf) {
      const std::size_t rlen = std::min(self.buffer_.size(), buf.size());
      std::copy_n(self.buffer_.data(), rlen, buf.data());
      self.buffer_ = self.buffer_.subspan(rlen);
      return just(rlen);
    };
    auto when_no_data_avail = [](buffered_readstream_adapter& self, std::span<std::byte> buf) {
      if constexpr(tag_invocable<tag_t<io::lazy_read_some_ec>, next_layer_type&, std::array<std::span<std::byte>, 2>>) {
        // If the underlying stream can read multiple buffers,
        // we'll read both the requested data and our buffer directly.
        std::array<std::span<std::byte>, 2> read_buffers{ buf, std::span<std::byte>(self.buffer_space_.data(), self.buffer_space_.size()) };
        return io::lazy_read_some_ec(self.next_layer(), read_buffers)
        | lazy_then(
            [&self, bufsize=buf.size()](std::size_t bytes) -> std::size_t {
              if (bytes <= bufsize) return bytes;

              self.buffer_ = std::span<const std::byte>(self.buffer_space_.data(), bytes - bufsize);
              return bufsize;
            });
      } else {
        // If the underlying stream cannot read multiple buffers,
        // we'll read into our local buffer, and next do a copy.
        return io::lazy_read_some_ec(self.next_layer(), std::span<std::byte>(self.buffer_space_.data(), self.buffer_space_.size()))
        | lazy_then(
            [&self, buf](std::size_t bytes) -> std::size_t {
              self.buffer_ = std::span<const std::byte>(self.buffer_space_.data(), bytes);
              const auto rlen = std::min(self.buffer_.size(), buf.size());
              std::copy_n(self.buffer_.data(), rlen, buf.data());
              self.buffer_ = self.buffer_.subspan(rlen);
              return rlen;
            });
      }
    };

    using variant_type = std::variant<
        decltype(when_data_avail(std::declval<buffered_readstream_adapter&>(), std::declval<std::span<std::byte>&>())),
        decltype(when_no_data_avail(std::declval<buffered_readstream_adapter&>(), std::declval<std::span<std::byte>&>()))>;

    return just(std::ref(self), buf)
    | let_variant(
        [when_data_avail, when_no_data_avail](buffered_readstream_adapter& self, std::span<std::byte>& buf) -> variant_type {
          if (!self.buffer_.empty() || buf.empty())
            return variant_type(std::in_place_index<0>, when_data_avail(self, buf));
          else
            return variant_type(std::in_place_index<1>, when_no_data_avail(self, buf));
        });
  }

  template<execution::io::mutable_buffer_sequence Buffers>
  friend auto tag_invoke(execution::io::lazy_read_some_ec_t, buffered_readstream_adapter& self, Buffers&& buffers) {
    using namespace execution;

    auto when_data_avail = [](buffered_readstream_adapter& self, auto& buffers) {
      return just(std::ref(self), std::move(buffers))
      | lazy_then(
          [](buffered_readstream_adapter& self, auto&& buffers) -> std::size_t {
            std::size_t total_rlen = 0;

            for (std::span<std::byte> buf : buffers) {
              const auto rlen = std::min(self.buffer_.size(), buf.size());
              std::copy_n(self.buffer_.data(), rlen, buf.data());
              self.buffer_ = self.buffer_.subspan(rlen);
              total_rlen += rlen;

              if (rlen != buf.size()) break;
            }
            return total_rlen;
          });
    };
    auto when_no_data_avail = [](buffered_readstream_adapter& self, auto& buffers) {
      std::vector<std::span<std::byte>> buffer_vector;
      std::ranges::copy(buffers, std::back_inserter(buffer_vector));

      if constexpr(tag_invocable<tag_t<io::lazy_read_some_ec>, next_layer_type&, std::vector<std::span<std::byte>>>) {
        // If the underlying stream can read multiple buffers,
        // we'll read both the requested data and our buffer directly.
        return just(std::ref(self), std::move(buffer_vector))
        | let_variant(
            [](buffered_readstream_adapter& self, auto& buffers) {
              std::size_t local_buffers_size = std::reduce(
                  buffers.begin(), buffers.end(),
                  std::size_t(0),
                  [](const auto& x, const auto& y) -> std::size_t {
                    auto get_size = overload(
                        [](std::size_t x) -> std::size_t { return x; },
                        [](std::span<std::byte> s) -> std::size_t { return s.size(); });
                    return get_size(x) + get_size(y);
                  });
              buffers.push_back(std::span<std::byte>(self.buffer_space_.data(), self.buffer_space_.size()));

              // If the local-buffers-size is 0 (nothing to read)
              // we want to not produce an eof-error.
              // So in that case, we'll just skip reading entirely.
              auto skip_reading = []() { return just(std::size_t(0)); };
              auto do_reading = [&]() {
                return io::lazy_read_some_ec(self.next_layer(), std::move(buffers))
                | lazy_then(
                    [&self, local_buffers_size](std::size_t bytes) {
                      if (bytes <= local_buffers_size) return bytes;

                      self.buffer_ = std::span<std::byte>(self.buffer_space_.data(), bytes - local_buffers_size);
                      return local_buffers_size;
                    });
              };

              using variant_type = std::variant<decltype(skip_reading()), decltype(do_reading())>;

              if (local_buffers_size == 0)
                return variant_type(std::in_place_index<0>, skip_reading());
              else
                return variant_type(std::in_place_index<1>, do_reading());
            });
      } else {
        // If the underlying stream cannot read multiple buffers,
        // we'll read into our local buffer, and next do a copy.
        return just(std::ref(self), std::move(buffer_vector))
        | lazy_let_value(
            [](buffered_readstream_adapter& self, auto& buffers) {
              std::size_t local_buffers_size = std::reduce(
                  buffers.begin(), buffers.end(),
                  std::size_t(0),
                  [](const auto& x, const auto& y) {
                    auto get_size = overload(
                        [](std::size_t x) -> std::size_t { return x; },
                        [](std::span<std::byte> s) -> std::size_t { return s.size(); });
                  });

              // If the local-buffers-size is 0 (nothing to read)
              // we want to not produce an eof-error.
              // So in that case, we'll just skip reading entirely.
              auto skip_reading = []() { return just(std::size_t(0)); };
              auto do_reading = [&]() {
                return io::lazy_read_some_ec(self.next_layer(), std::span<std::byte>(self.buffer_space_.data(), self.buffer_space_.size()))
                | lazy_then(
                    [&self, &buffers](std::size_t bytes) -> std::size_t {
                      self.buffer_ = std::span<const std::byte>(self.buffer_space_.data(), self.buffer_space_.size());

                      std::size_t total_rlen = 0;
                      for (std::span<std::byte> buf : buffers) {
                        const auto rlen = std::min(self.buffer_.size(), buf.size());
                        std::copy_n(self.buffer_.data(), rlen, buf.data());
                        self.buffer_ = self.buffer_.subspan(rlen);
                        total_rlen += rlen;
                      }

                      return total_rlen;
                    });
              };

              using variant_type = std::variant<decltype(skip_reading()), decltype(do_reading())>;

              if (local_buffers_size == 0)
                return variant_type(std::in_place_index<0>, skip_reading());
              else
                return variant_type(std::in_place_index<1>, do_reading());
            });
      }
    };

    using variant_type = std::variant<
        decltype(when_data_avail(std::declval<buffered_readstream_adapter&>(), std::declval<std::remove_cvref_t<Buffers>&>())),
        decltype(when_no_data_avail(std::declval<buffered_readstream_adapter&>(), std::declval<std::remove_cvref_t<Buffers>&>()))>;

    return just(std::ref(self), std::forward<Buffers>(buffers))
    | let_variant(
        [when_data_avail, when_no_data_avail](buffered_readstream_adapter& self, std::remove_cvref_t<Buffers>& buf) -> variant_type {
          if (!self.buffer_.empty())
            return variant_type(std::in_place_index<0>, when_data_avail(self, buf));
          else
            return variant_type(std::in_place_index<1>, when_no_data_avail(self, buf));
        });
  }

  template<typename MutableBufferSequence>
  auto read_some(const MutableBufferSequence& buffers) -> std::size_t {
    const auto buffers_size = asio::buffer_size(buffers);
    if (this->buffer_.size() > 0 || buffers_size == 0) {
      std::size_t sz = asio::buffer_copy(buffers, asio::buffer(this->buffer_.data(), this->buffer_.size()));
      buffer_ = buffer_.subspan(sz);
      return sz;
    }

    auto tmp_buffer_sequence = std::vector<asio::mutable_buffer>(asio::buffer_sequence_begin(buffers), asio::buffer_sequence_end(buffers));
    tmp_buffer_sequence.push_back(asio::buffer(buffer_space_));
    std::size_t sz = next_layer().read_some(tmp_buffer_sequence);
    if (sz > buffers_size) {
      this->buffer_ = std::span<std::byte>(buffer_space_, sz - buffers_size);
      sz = buffers_size;
    }
    return sz;
  }

  template<typename MutableBufferSequence>
  auto read_some(const MutableBufferSequence& buffers, std::error_code& ec) -> std::size_t {
    const auto buffers_size = asio::buffer_size(buffers);
    if (this->buffer_.size() > 0 || buffers_size == 0) {
      ec = {};
      std::size_t sz = asio::buffer_copy(buffers, this->buffer_);
      buffer_ = buffer_.subspan(sz);
      return sz;
    }

    auto tmp_buffer_sequence = std::vector<asio::mutable_buffer>(asio::buffer_sequence_begin(buffers), asio::buffer_sequence_end(buffers));
    tmp_buffer_sequence.push_back(asio::buffer(buffer_space_));
    std::size_t sz = next_layer().read_some(tmp_buffer_sequence, ec);
    if (!ec && sz > buffers_size) {
      this->buffer_ = asio::buffer(buffer_space_, sz - buffers_size);
      sz = buffers_size;
    }
    return sz;
  }

  template<typename MutableBufferSequence, typename CompletionToken>
  auto async_read_some(const MutableBufferSequence& buffers, CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(std::error_code, std::size_t)>(
        [this](auto completion_handler, auto buffers) {
          const auto buffers_size = asio::buffer_size(buffers);
          if (this->buffer_.size() > 0 || buffers_size == 0) {
            std::size_t sz = asio::buffer_copy(buffers, asio::buffer(this->buffer_.data(), this->buffer_.size()));
            buffer_ = buffer_.subspan(sz);
            std::invoke(completion_handler_fun(std::move(completion_handler), this->get_executor()), std::error_code{}, sz);
            return;
          }

          auto tmp_buffer_sequence = std::vector<asio::mutable_buffer>(asio::buffer_sequence_begin(buffers), asio::buffer_sequence_end(buffers));
          tmp_buffer_sequence.push_back(asio::buffer(buffer_space_));
          next_layer().async_read_some(
              tmp_buffer_sequence,
              completion_wrapper<void(std::error_code, std::size_t)>(
                  std::move(completion_handler),
                  [this, buffers_size](auto&& handler, std::error_code ec, std::size_t sz) -> void {
                    if (!ec && sz > buffers_size) {
                      this->buffer_ = std::span<const std::byte>(buffer_space_.data(), sz - buffers_size);
                      sz = buffers_size;
                    }
                    std::invoke(handler, ec, sz);
                  }));
        },
        token, buffers);
  }

  private:
  AsyncReadDevice next_layer_;
  std::vector<std::byte, allocator_type> buffer_space_;
  std::span<const std::byte> buffer_;
};


} /* namespace earnest::detail */
