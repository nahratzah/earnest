#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include <asio/buffer.hpp>
#include <asio/error.hpp>
#include <asio/post.hpp>

namespace earnest::detail {


template<typename Executor, typename Allocator = std::allocator<std::byte>>
class byte_positional_stream {
  template<typename OtherExecutor, typename OtherAllocator> friend class byte_positional_stream;

  public:
  using executor_type = Executor;
  using allocator_type = Allocator;

  byte_positional_stream(const Executor& ex, allocator_type alloc = allocator_type())
  : ex_(ex),
    data_(std::move(alloc))
  {}

  byte_positional_stream(std::initializer_list<std::byte> init, const Executor& ex, allocator_type alloc = allocator_type())
  : ex_(ex),
    data_(init, std::move(alloc))
  {}

  byte_positional_stream(std::initializer_list<std::uint8_t> init, const Executor& ex, allocator_type alloc = allocator_type())
  : ex_(ex),
    data_(std::move(alloc))
  {
    data_.reserve(init.size());
    std::transform(init.begin(), init.end(),
        std::back_inserter(data_),
        [](std::uint8_t b) -> std::byte { return std::byte{b}; });
  }

  auto get_executor() const -> executor_type {
    return ex_;
  }

  auto get_allocator() const -> allocator_type {
    return data_.get_allocator();
  }

  auto cdata() const noexcept -> const std::vector<std::byte, allocator_type>& {
    return data_;
  }

  auto data() const & noexcept -> const std::vector<std::byte, allocator_type>& {
    return data_;
  }

  auto data() & noexcept -> std::vector<std::byte, allocator_type>& {
    return data_;
  }

  auto data() && noexcept -> std::vector<std::byte, allocator_type>&& {
    return std::move(data_);
  }

  template<typename MB>
  auto read_some_at(std::uint64_t off, MB mb, std::error_code& ec) const -> std::size_t {
    ec.clear();

    if (asio::buffer_size(mb) == 0) {
      return 0;
    }

    if (off >= data_.size()) {
      ec = make_error_code(asio::stream_errc::eof);
      return 0;
    }

    return asio::buffer_copy(mb, asio::buffer(data_) + off);
  }

  template<typename MB>
  auto read_some_at(std::uint64_t off, MB mb) const -> std::size_t {
    std::error_code ec;
    std::size_t bytes = read_some_at(off, mb, ec);
    if (ec) throw std::system_error(ec);
    return bytes;
  }

  template<typename MB, typename CompletionToken>
  auto async_read_some_at(std::uint64_t off, MB&& mb, CompletionToken&& token) const {
    return asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, std::size_t)>::initiate(
        [this](auto completion_handler, std::uint64_t off, auto mb) {
          std::error_code ec;
          std::size_t bytes = this->read_some_at(off, mb, ec);

          auto x = asio::get_associated_executor(completion_handler, this->ex_);
          asio::post(
              x,
              [ ch=std::move(completion_handler),
                ec,
                bytes
              ]() mutable {
                std::invoke(std::move(ch), ec, bytes);
              });
        }, std::forward<CompletionToken>(token), off, std::forward<MB>(mb));
  }

  template<typename MB>
  auto write_some_at(std::uint64_t off, MB mb, std::error_code& ec) -> std::size_t {
    ec.clear();

    const auto max_off = off + asio::buffer_size(mb);
    if (max_off > data_.size()) data_.resize(max_off);
    return asio::buffer_copy(asio::buffer(data_) + off, mb);
  }

  template<typename MB>
  auto write_some_at(std::uint64_t off, MB mb) -> std::size_t {
    std::error_code ec;
    std::size_t bytes = write_some_at(off, mb, ec);
    if (ec) throw std::system_error(ec);
    return bytes;
  }

  template<typename MB, typename CompletionToken>
  auto async_write_some_at(std::uint64_t off, MB&& mb, CompletionToken&& token) {
    return asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, std::size_t)>::initiate(
        [this](auto completion_handler, std::uint64_t off, auto mb) {
          std::error_code ec;
          std::size_t bytes = this->write_some_at(off, mb, ec);

          auto x = asio::get_associated_executor(completion_handler, this->ex_);
          asio::post(
              x,
              [ ch=std::move(completion_handler),
                ec,
                bytes
              ]() mutable {
                std::invoke(std::move(ch), ec, bytes);
              });
        }, std::forward<CompletionToken>(token), off, std::forward<MB>(mb));
  }

  template<typename Char, typename CharT>
  friend auto operator<<(std::basic_ostream<Char, CharT>& out, const byte_positional_stream& s) -> std::basic_ostream<Char, CharT>& {
    constexpr std::array<char, 16> chars{ '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
    out.put(out.widen('['));
    if (!s.data_.empty()) {
      for (std::byte b : s.data_) {
        const auto b_val = std::to_integer<std::underlying_type_t<std::byte>>(b);
        out.put(out.widen(' '));
        out.put(out.widen(chars[b_val / 16u]));
        out.put(out.widen(chars[b_val % 16u]));
      }
      out.put(out.widen(' '));
    }
    out.put(out.widen(']'));
    return out;
  }

  auto empty() const noexcept -> bool {
    return data_.empty();
  }

  template<typename OtherExecutor, typename OtherAllocator>
  auto operator==(const byte_positional_stream<OtherExecutor, OtherAllocator>& y) const -> bool {
    return data_ == y.data_;
  }

  template<typename OtherExecutor, typename OtherAllocator>
  auto operator!=(const byte_positional_stream<OtherExecutor, OtherAllocator>& y) const -> bool {
    return !(*this == y);
  }

  private:
  Executor ex_;
  std::vector<std::byte, allocator_type> data_;
};


} /* namespace earnest::detail */
