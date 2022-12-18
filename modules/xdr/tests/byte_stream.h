#pragma once

#include <array>
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

template<typename Executor>
class byte_stream {
  template<typename OtherExecutor> friend class byte_stream;

  public:
  using executor_type = Executor;

  byte_stream(const Executor& ex)
  : ex_(ex)
  {}

  byte_stream(std::initializer_list<std::uint8_t> data, const Executor& ex)
  : ex_(ex),
    data_(data)
  {}

  auto get_executor() const -> executor_type {
    return ex_;
  }

  template<typename MB>
  auto read_some(MB mb, std::error_code& ec) -> std::size_t {
    if (asio::buffer_size(mb) == 0) {
      ec = std::error_code();
      return 0;
    }

    if (data_.empty()) {
      ec = make_error_code(asio::stream_errc::eof);
      return 0;
    }

    std::size_t bytes = asio::buffer_copy(mb, asio::buffer(data_));
    data_.erase(data_.begin(), data_.begin() + bytes);
    return bytes;
  }

  template<typename MB>
  auto read_some(MB mb) -> std::size_t {
    std::error_code ec;
    std::size_t bytes = read_some(mb, ec);
    if (ec) throw std::system_error(ec);
    return bytes;
  }

  template<typename MB, typename CompletionToken>
  auto async_read_some(MB mb, CompletionToken&& token) {
    return asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, std::size_t)>::initiate(
        [this, &mb](auto completion_handler) {
          std::error_code ec;
          std::size_t bytes = this->read_some(mb, ec);

          auto x = asio::get_associated_executor(completion_handler, this->ex_);
          asio::post(
              x,
              [ ch=std::move(completion_handler),
                ec,
                bytes
              ]() mutable {
                std::invoke(std::move(ch), ec, bytes);
              });
        }, std::forward<CompletionToken>(token));
  }

  template<typename MB>
  auto write_some(MB mb, std::error_code& ec) -> std::size_t {
    const auto orig_size = data_.size();
    data_.resize(orig_size + asio::buffer_size(mb));
    const auto written_bytes = asio::buffer_copy(asio::buffer(data_) + orig_size, mb);
    data_.resize(orig_size + written_bytes);
    return written_bytes;
  }

  template<typename MB>
  auto write_some(MB mb) -> std::size_t {
    std::error_code ec;
    std::size_t bytes = write_some(mb, ec);
    if (ec) throw std::system_error(ec);
    return bytes;
  }

  template<typename MB, typename CompletionToken>
  auto async_write_some(MB mb, CompletionToken&& token) {
    return asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, std::size_t)>::initiate(
        [this, &mb](auto completion_handler) {
          std::error_code ec;
          std::size_t bytes = this->write_some(mb, ec);

          auto x = asio::get_associated_executor(completion_handler, this->ex_);
          asio::post(
              x,
              [ ch=std::move(completion_handler),
                ec,
                bytes
              ]() mutable {
                std::invoke(std::move(ch), ec, bytes);
              });
        }, std::forward<CompletionToken>(token));
  }

  template<typename Char, typename CharT>
  friend auto operator<<(std::basic_ostream<Char, CharT>& out, const byte_stream& s) -> std::basic_ostream<Char, CharT>& {
    constexpr std::array<char, 16> chars{ '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
    out.put(out.widen('['));
    if (!s.data_.empty()) {
      for (std::uint8_t b : s.data_) {
        out.put(out.widen(' '));
        out.put(out.widen(chars[b / 16u]));
        out.put(out.widen(chars[b % 16u]));
      }
      out.put(out.widen(' '));
    }
    out.put(out.widen(']'));
    return out;
  }

  auto empty() const noexcept -> bool {
    return data_.empty();
  }

  template<typename OtherExecutor>
  auto operator==(const byte_stream<OtherExecutor>& y) const -> bool {
    return data_ == y.data_;
  }

  template<typename OtherExecutor>
  auto operator!=(const byte_stream<OtherExecutor>& y) const -> bool {
    return !(*this == y);
  }

  private:
  Executor ex_;
  std::vector<std::uint8_t> data_;
};
