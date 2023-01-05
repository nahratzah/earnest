#pragma once

#include <cstdint>
#include <system_error>
#include <type_traits>
#include <utility>

#include <asio/async_result.hpp>

#include "completion_wrapper.h"

namespace earnest::detail {


template<typename AsyncRandomAccessDevice>
class positional_stream_adapter {
  private:
  template<typename Handler>
  struct async_update_wrapper
  : public Handler
  {
    async_update_wrapper(positional_stream_adapter& adapter, Handler handler)
    : Handler(std::move(handler)),
      adapter(adapter)
    {}

    auto operator()(const std::error_code& ec, std::size_t bytes) -> void {
      adapter.pos_ += bytes;
      this->Handler::operator()(ec, bytes);
    }

    private:
    positional_stream_adapter& adapter;
  };

  public:
  using offset_type = std::uint64_t;
  using next_layer_type = std::remove_reference_t<AsyncRandomAccessDevice>;
  using lowest_layer_type = typename next_layer_type::lowest_layer_type;
  using executor_type = typename lowest_layer_type::executor_type;

  template<typename Arg>
  explicit positional_stream_adapter(Arg& a)
  : next_layer_(a)
  {}

  template<typename Arg>
  positional_stream_adapter(Arg& a, offset_type pos)
  : next_layer_(a),
    pos_(pos)
  {}

  positional_stream_adapter(positional_stream_adapter&&) = default;
  positional_stream_adapter& operator=(positional_stream_adapter&&) = default;

  auto next_layer() -> next_layer_type& {
    return next_layer_;
  }

  auto next_layer() const -> const next_layer_type& {
    return next_layer_;
  }

  auto lowest_layer() -> lowest_layer_type& {
    return next_layer_.lowest_layer();
  }

  auto lowest_layer() const -> const lowest_layer_type& {
    return next_layer_.lowest_layer();
  }

  auto close() -> void {
    next_layer_.close();
  }

  auto close(std::error_code& ec) -> void {
    next_layer_.close(ec);
  }

  auto position() const noexcept -> offset_type {
    return pos_;
  }

  auto get_executor() const noexcept -> executor_type {
    return lowest_layer().get_executor();
  }

  auto skip(offset_type bytes) {
    pos_ += bytes;
  }

  template<typename ConstBufferSequence>
  auto write_some(const ConstBufferSequence& buffers) -> std::size_t {
    std::size_t sz = next_layer_.write_some_at(pos_, buffers);
    pos_ += sz;
    return sz;
  }

  template<typename ConstBufferSequence>
  auto write_some(const ConstBufferSequence& buffers, std::error_code& ec) -> std::size_t {
    std::size_t sz = next_layer_.write_some_at(pos_, buffers, ec);
    pos_ += sz;
    return sz;
  }

  template<typename ConstBufferSequence, typename CompletionToken>
  auto async_write_some(const ConstBufferSequence& buffers, CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(std::error_code, std::size_t)>(
        [this](auto completion_handler, const ConstBufferSequence& buffers) {
          next_layer_.async_write_some_at(
              pos_,
              buffers,
              completion_wrapper<void(std::error_code, size_t)>(
                  std::move(completion_handler),
                  [this](auto& fwd, std::error_code ec, size_t sz) {
                    pos_ += sz;
                    std::invoke(fwd, ec, sz);
                  }));
        },
        token, buffers);
  }

  template<typename MutableBufferSequence>
  auto read_some(const MutableBufferSequence& buffers) -> std::size_t {
    std::size_t sz = next_layer_.read_some_at(pos_, buffers);
    pos_ += sz;
    return sz;
  }

  template<typename MutableBufferSequence>
  auto read_some(const MutableBufferSequence& buffers, std::error_code& ec) -> std::size_t {
    std::size_t sz = next_layer_.read_some_at(pos_, buffers, ec);
    pos_ += sz;
    return sz;
  }

  template<typename MutableBufferSequence, typename CompletionToken>
  auto async_read_some(const MutableBufferSequence& buffers, CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(std::error_code, std::size_t)>(
        [this](auto completion_handler, const MutableBufferSequence& buffers) {
          next_layer_.async_read_some_at(
              pos_,
              buffers,
              completion_wrapper<void(std::error_code, size_t)>(
                  std::move(completion_handler),
                  [this](auto& fwd, std::error_code ec, size_t sz) {
                    pos_ += sz;
                    std::invoke(fwd, ec, sz);
                  }));
        },
        token, buffers);
  }

  private:
  offset_type pos_ = 0;
  AsyncRandomAccessDevice next_layer_;
};


} /* namespace earnest::detail */