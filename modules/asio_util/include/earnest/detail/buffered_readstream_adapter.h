#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include "completion_handler_fun.h"
#include "completion_wrapper.h"

namespace earnest::detail {


template<typename AsyncRandomAccessDevice, typename Allocator>
class buffered_readstream_adapter {
  public:
  static constexpr std::size_t default_buffer_size = 64 * 1024;
  using offset_type = std::uint64_t;
  using next_layer_type = std::remove_reference_t<AsyncRandomAccessDevice>;
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
    if (asio::buffer_size(this->buffer_) >= bytes) {
      this->buffer_ += bytes;
      bytes = 0;
    } else {
      bytes -= asio::buffer_size(this->buffer_);
      this->buffer_ = asio::buffer(this->buffer_, 0);
    }

    return next_layer().skip(bytes);
  }

  template<typename MutableBufferSequence>
  auto read_some(const MutableBufferSequence& buffers) -> std::size_t {
    const auto buffers_size = asio::buffer_size(buffers);
    if (asio::buffer_size(this->buffer_) > 0 || buffers_size == 0) {
      std::size_t sz = asio::buffer_copy(buffers, this->buffer_);
      buffer_ = buffer_ + sz;
      return sz;
    }

    auto tmp_buffer_sequence = std::vector<asio::mutable_buffer>(asio::buffer_sequence_begin(buffers), asio::buffer_sequence_end(buffers));
    tmp_buffer_sequence.push_back(asio::buffer(buffer_space_));
    std::size_t sz = next_layer().read_some(tmp_buffer_sequence);
    if (sz > buffers_size) {
      this->buffer_ = asio::buffer(buffer_space_, sz - buffers_size);
      sz = buffers_size;
    }
    return sz;
  }

  template<typename MutableBufferSequence>
  auto read_some(const MutableBufferSequence& buffers, std::error_code& ec) -> std::size_t {
    const auto buffers_size = asio::buffer_size(buffers);
    if (asio::buffer_size(this->buffer_) > 0 || buffers_size == 0) {
      ec = {};
      std::size_t sz = asio::buffer_copy(buffers, this->buffer_);
      buffer_ = buffer_ + sz;
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
          if (asio::buffer_size(this->buffer_) > 0 || buffers_size == 0) {
            std::size_t sz = asio::buffer_copy(buffers, this->buffer_);
            buffer_ = buffer_ + sz;
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
                      this->buffer_ = asio::buffer(buffer_space_, sz - buffers_size);
                      sz = buffers_size;
                    }
                    std::invoke(handler, ec, sz);
                  }));
        },
        token, buffers);
  }

  private:
  AsyncRandomAccessDevice next_layer_;
  std::vector<std::byte, allocator_type> buffer_space_;
  asio::const_buffer buffer_;
};


} /* namespace earnest::detail */
