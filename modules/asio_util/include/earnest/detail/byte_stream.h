#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <optional>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include <asio/buffer.hpp>
#include <asio/error.hpp>
#include <asio/post.hpp>

#include <earnest/execution.h>
#include <earnest/execution_io.h>
#include <earnest/execution_util.h>

namespace earnest::detail {

template<typename Executor, typename Allocator = std::allocator<std::byte>>
class byte_stream {
  template<typename OtherExecutor, typename OtherAllocator> friend class byte_stream;

  public:
  using executor_type = Executor;
  using allocator_type = Allocator;

  byte_stream(const Executor& ex, allocator_type alloc = allocator_type())
  : ex_(ex),
    data_(std::move(alloc))
  {}

  byte_stream(std::initializer_list<std::byte> init, const Executor& ex, allocator_type alloc = allocator_type())
  : ex_(ex),
    data_(init, std::move(alloc))
  {}

  byte_stream(std::initializer_list<std::uint8_t> init, const Executor& ex, allocator_type alloc = allocator_type())
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

  friend auto tag_invoke([[maybe_unused]] execution::io::lazy_read_some_ec_t tag, byte_stream& self, std::span<std::byte> buf) {
    return execution::just(std::move(buf))
    | execution::lazy_then(
        [&self](std::span<std::byte> buf) {
          const auto bytes = std::min(self.data_.size(), buf.size());
          std::copy(self.data_.begin(), self.data_.begin() + bytes, buf.begin());
          self.data_.erase(self.data_.begin(), self.data_.begin() + bytes);
          return bytes;
        })
    | execution::lazy_validation(
        [bufsize=buf.size()](std::size_t bytes) -> std::optional<std::error_code> {
          if (bufsize != 0 && bytes == 0)
            return make_error_code(asio::stream_errc::eof);
          else
            return std::nullopt;
        });
  }

  template<execution::io::mutable_buffer_sequence Buffers>
  friend auto tag_invoke([[maybe_unused]] execution::io::lazy_read_some_ec_t tag, byte_stream& self, Buffers&& buffers) {
    return execution::just(std::forward<Buffers>(buffers))
    | execution::lazy_then(
        [&self](auto&& buffers) {
          std::size_t bytes = 0;
          for (std::span<std::byte> buf : buffers) {
            const auto rlen = std::min(self.data_.size(), buf.size());
            std::copy(self.data_.begin(), self.data_.begin() + rlen, buf.begin());
            self.data_.erase(self.data_.begin(), self.data_.begin() + rlen);
            bytes += rlen;
            if (rlen < buf.size()) return std::make_tuple(bytes, true);
          }
          return std::make_tuple(bytes, false);
        })
    | execution::explode_tuple()
    | execution::lazy_validation(
        [](std::size_t bytes, bool underflow) -> std::optional<std::error_code> {
          if (underflow && bytes == 0)
            return make_error_code(asio::stream_errc::eof);
          else
            return std::nullopt;
        })
    | execution::lazy_then(
        [](std::size_t bytes, [[maybe_unused]] bool underflow) -> std::size_t {
          return bytes;
        });
  }

  friend auto tag_invoke([[maybe_unused]] execution::io::lazy_write_some_ec_t tag, byte_stream& self, std::span<const std::byte> buf) {
    return execution::just(std::move(buf))
    | execution::lazy_then(
        [&self](std::span<const std::byte> buf) {
          const auto orig_size = self.data_.size();
          self.data_.resize(orig_size + buf.size());
          std::copy(buf.begin(), buf.end(), self.data_.begin() + orig_size);
          return buf.size();
        });
  }

  template<execution::io::const_buffer_sequence Buffers>
  friend auto tag_invoke([[maybe_unused]] execution::io::lazy_write_some_ec_t tag, byte_stream& self, Buffers&& buffers) {
    return execution::just(std::forward<Buffers>(buffers))
    | execution::lazy_then(
        [&self](auto&& buffers) {
          std::size_t bytes = 0;
          for (std::span<const std::byte> buf : buffers) {
            const auto orig_size = self.data_.size();
            self.data_.resize(orig_size + buf.size());
            std::copy(buf.begin(), buf.end(), self.data_.begin() + orig_size);
            bytes += buf.size();
          }
          return bytes;
        });
  }

  friend auto tag_invoke([[maybe_unused]] execution::io::lazy_read_ec_t tag, byte_stream& self, std::span<std::byte> buf, std::optional<std::size_t> minbytes) {
    return execution::just(buf)
    | execution::lazy_then(
        [&self](std::span<std::byte> buf) {
          const auto bytes = std::min(self.data_.size(), buf.size());
          std::copy(self.data_.begin(), self.data_.begin() + bytes, buf.begin());
          self.data_.erase(self.data_.begin(), self.data_.begin() + bytes);
          return bytes;
        })
    | execution::lazy_validation(
        [minsize=minbytes.value_or(buf.size())](std::size_t bytes) -> std::optional<std::error_code> {
          if (bytes < minsize)
            return make_error_code(asio::stream_errc::eof);
          else
            return std::nullopt;
        });
  }

  template<execution::io::mutable_buffer_sequence Buffers>
  friend auto tag_invoke([[maybe_unused]] execution::io::lazy_read_ec_t tag, byte_stream& self, Buffers&& buffers, std::optional<std::size_t> minbytes) {
    return execution::just(std::forward<Buffers>(buffers))
    | execution::lazy_then(
        [&self](auto&& buffers) {
          std::size_t bytes = 0;
          for (std::span<std::byte> buf : buffers) {
            const auto rlen = std::min(self.data_.size(), buf.size());
            std::copy(self.data_.begin(), self.data_.begin() + rlen, buf.begin());
            self.data_.erase(self.data_.begin(), self.data_.begin() + rlen);
            bytes += rlen;
            if (rlen < buf.size()) return std::make_tuple(bytes, true);
          }
          return std::make_tuple(bytes, false);
        })
    | execution::explode_tuple()
    | execution::lazy_validation(
        [minbytes](std::size_t bytes, bool underflow) -> std::optional<std::error_code> {
          if (minbytes.has_value() ? bytes < *minbytes : underflow)
            return make_error_code(asio::stream_errc::eof);
          else
            return std::nullopt;
        })
    | execution::lazy_then(
        [](std::size_t bytes, [[maybe_unused]] bool underflow) -> std::size_t {
          return bytes;
        });
  }

  friend auto tag_invoke([[maybe_unused]] execution::io::lazy_write_ec_t tag, byte_stream& self, std::span<const std::byte> buf, [[maybe_unused]] std::optional<std::size_t> minbytes) {
    // We don't need to validate minbytes, since we always write everything.
    return execution::just(std::move(buf))
    | execution::lazy_then(
        [&self](std::span<const std::byte> buf) {
          const auto orig_size = self.data_.size();
          self.data_.resize(orig_size + buf.size());
          std::copy(buf.begin(), buf.end(), self.data_.begin() + orig_size);
          return buf.size();
        });
  }

  template<execution::io::const_buffer_sequence Buffers>
  friend auto tag_invoke([[maybe_unused]] execution::io::lazy_write_ec_t tag, byte_stream& self, Buffers&& buffers, [[maybe_unused]] std::optional<std::size_t> minbytes) {
    // We don't need to validate minbytes, since we always write everything.
    return execution::just(std::forward<Buffers>(buffers))
    | execution::lazy_then(
        [&self](auto&& buffers) {
          std::size_t bytes = 0;
          for (std::span<const std::byte> buf : buffers) {
            const auto orig_size = self.data_.size();
            self.data_.resize(orig_size + buf.size());
            std::copy(buf.begin(), buf.end(), self.data_.begin() + orig_size);
            bytes += buf.size();
          }
          return bytes;
        });
  }

  template<typename MB>
  auto read_some(MB mb, std::error_code& ec) -> std::size_t {
    ec.clear();

    if (asio::buffer_size(mb) == 0) {
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
    ec.clear();

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
  auto operator==(const byte_stream<OtherExecutor, OtherAllocator>& y) const -> bool {
    return data_ == y.data_;
  }

  template<typename OtherExecutor, typename OtherAllocator>
  auto operator!=(const byte_stream<OtherExecutor, OtherAllocator>& y) const -> bool {
    return !(*this == y);
  }

  private:
  Executor ex_;
  std::vector<std::byte, allocator_type> data_;
};

} /* namespace earnest::detail */
