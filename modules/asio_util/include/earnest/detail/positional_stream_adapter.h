#pragma once

#include <cstdint>
#include <system_error>
#include <type_traits>
#include <utility>

#include <earnest/execution.h>
#include <earnest/execution_io.h>

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

  template<typename Arg>
  explicit positional_stream_adapter(Arg&& a)
  : next_layer_(std::forward<Arg>(a))
  {}

  template<typename Arg>
  positional_stream_adapter(Arg&& a, offset_type pos)
  : pos_(pos),
    next_layer_(std::forward<Arg>(a))
  {}

  positional_stream_adapter(const positional_stream_adapter&) = delete;
  positional_stream_adapter& operator=(const positional_stream_adapter&) = delete;
  positional_stream_adapter(positional_stream_adapter&&) = default;
  positional_stream_adapter& operator=(positional_stream_adapter&&) = default;

  auto next_layer() -> next_layer_type& {
    return next_layer_;
  }

  auto next_layer() const -> const next_layer_type& {
    return next_layer_;
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

  auto skip(offset_type bytes) {
    pos_ += bytes;
  }

  template<execution::io::mutable_buffers Buffers>
  friend auto tag_invoke([[maybe_unused]] execution::io::lazy_read_some_ec_t tag, positional_stream_adapter& self, Buffers&& buffers) {
    return execution::io::lazy_read_some_at_ec(self.next_layer_, self.pos_, std::forward<Buffers>(buffers))
    | execution::lazy_then(self.position_updater_cb());
  }

  template<execution::io::const_buffers Buffers>
  friend auto tag_invoke([[maybe_unused]] execution::io::lazy_write_some_ec_t tag, positional_stream_adapter& self, Buffers&& buffers) {
    return execution::io::lazy_write_some_at_ec(self.next_layer_, self.pos_, std::forward<Buffers>(buffers))
    | execution::lazy_then(self.position_updater_cb());
  }

  template<execution::io::mutable_buffers Buffers>
  friend auto tag_invoke([[maybe_unused]] execution::io::lazy_read_some_t tag, positional_stream_adapter& self, Buffers&& buffers) {
    return execution::io::lazy_read_some_at(self.next_layer_, self.pos_, std::forward<Buffers>(buffers))
    | execution::lazy_then(self.position_updater_cb());
  }

  template<execution::io::const_buffers Buffers>
  friend auto tag_invoke([[maybe_unused]] execution::io::lazy_write_some_t tag, positional_stream_adapter& self, Buffers&& buffers) {
    return execution::io::lazy_write_some_at(self.next_layer_, self.pos_, std::forward<Buffers>(buffers))
    | execution::lazy_then(self.position_updater_cb());
  }

  template<execution::io::mutable_buffers Buffers>
  friend auto tag_invoke([[maybe_unused]] execution::io::lazy_read_ec_t tag, positional_stream_adapter& self, Buffers&& buffers, std::optional<std::size_t> minbytes) {
    return execution::io::lazy_read_at_ec(self.next_layer_, self.pos_, std::forward<Buffers>(buffers), std::move(minbytes))
    | execution::lazy_then(self.position_updater_cb());
  }

  template<execution::io::const_buffers Buffers>
  friend auto tag_invoke([[maybe_unused]] execution::io::lazy_write_ec_t tag, positional_stream_adapter& self, Buffers&& buffers, std::optional<std::size_t> minbytes) {
    return execution::io::lazy_write_at_ec(self.next_layer_, self.pos_, std::forward<Buffers>(buffers), std::move(minbytes))
    | execution::lazy_then(self.position_updater_cb());
  }

  template<execution::io::mutable_buffers Buffers>
  friend auto tag_invoke([[maybe_unused]] execution::io::lazy_read_t tag, positional_stream_adapter& self, Buffers&& buffers, std::optional<std::size_t> minbytes) {
    return execution::io::lazy_read_at(self.next_layer_, self.pos_, std::forward<Buffers>(buffers), std::move(minbytes))
    | execution::lazy_then(self.position_updater_cb());
  }

  template<execution::io::const_buffers Buffers>
  friend auto tag_invoke([[maybe_unused]] execution::io::lazy_write_t tag, positional_stream_adapter& self, Buffers&& buffers, std::optional<std::size_t> minbytes) {
    return execution::io::lazy_write_at(self.next_layer_, self.pos_, std::forward<Buffers>(buffers), std::move(minbytes))
    | execution::lazy_then(self.position_updater_cb());
  }

  private:
  auto position_updater_cb() {
    return [this](std::size_t sz) noexcept -> std::size_t {
      this->pos_ += sz;
      return sz;
    };
  }

  offset_type pos_ = 0;
  AsyncRandomAccessDevice next_layer_;
};


} /* namespace earnest::detail */
