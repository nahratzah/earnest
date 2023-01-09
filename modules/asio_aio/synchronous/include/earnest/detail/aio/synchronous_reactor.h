#pragma once

#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <utility>

#include <asio/associated_allocator.hpp>
#include <asio/associated_executor.hpp>
#include <asio/buffer.hpp>
#include <asio/error.hpp>
#include <asio/execution_context.hpp>

namespace earnest::detail::aio {


// Implements AIO by... not doing it asynchronously.
// The write happens during the function call.
class synchronous_reactor
: public asio::execution_context::service
{
  public:
  using key_type = synchronous_reactor;
  static inline const asio::execution_context::id id;

  protected:
  using iovec_array = std::vector<struct ::iovec>;

  public:
  using asio::execution_context::service::service;
  ~synchronous_reactor() override = default;

  void shutdown() override {}

  template<typename Buffers>
  auto read_some(int fd, Buffers&& buffers, std::error_code& ec) -> std::size_t {
    return do_op_(
        [fd](struct ::iovec* iov, std::size_t iovcnt) {
          return ::readv(fd, iov, iovcnt);
        },
        std::forward<Buffers>(buffers), ec);
  }

  template<typename Buffers>
  auto write_some(int fd, Buffers&& buffers, std::error_code& ec) -> std::size_t {
    return do_op_(
        [fd](struct ::iovec* iov, std::size_t iovcnt) {
          return ::writev(fd, iov, iovcnt);
        },
        std::forward<Buffers>(buffers), ec);
  }

  template<typename Buffers>
  auto read_some_at(int fd, std::uint64_t offset, Buffers&& buffers, std::error_code& ec) -> std::size_t {
    return do_op_(
        [fd, offset](struct ::iovec* iov, std::size_t iovcnt) {
          return ::preadv(fd, iov, iovcnt, offset);
        },
        std::forward<Buffers>(buffers), ec);
  }

  template<typename Buffers>
  auto write_some_at(int fd, std::uint64_t offset, Buffers&& buffers, std::error_code& ec) -> std::size_t {
    return do_op_(
        [fd, offset](struct ::iovec* iov, std::size_t iovcnt) {
          return ::pwritev(fd, iov, iovcnt, offset);
        },
        std::forward<Buffers>(buffers), ec);
  }

  template<typename Buffers, typename CompletionHandler, typename Executor>
  void async_read_some(int fd, Buffers&& buffers, CompletionHandler&& handler, Executor&& executor) {
    do_async_op_(
        [fd](struct ::iovec* iov, std::size_t iovcnt) {
          return ::readv(fd, iov, iovcnt);
        },
        std::forward<Buffers>(buffers), std::forward<CompletionHandler>(handler), std::forward<Executor>(executor));
  }

  template<typename Buffers, typename CompletionHandler, typename Executor>
  void async_write_some(int fd, Buffers&& buffers, CompletionHandler&& handler, Executor&& executor) {
    do_async_op_(
        [fd](struct ::iovec* iov, std::size_t iovcnt) {
          return ::writev(fd, iov, iovcnt);
        },
        std::forward<Buffers>(buffers), std::forward<CompletionHandler>(handler), std::forward<Executor>(executor));
  }

  template<typename Buffers, typename CompletionHandler, typename Executor>
  void async_read_some_at(int fd, std::uint64_t offset, Buffers&& buffers, CompletionHandler&& handler, Executor&& executor) {
    do_async_op_(
        [fd, offset](struct ::iovec* iov, std::size_t iovcnt) {
          return ::preadv(fd, iov, iovcnt, offset);
        },
        std::forward<Buffers>(buffers), std::forward<CompletionHandler>(handler), std::forward<Executor>(executor));
  }

  template<typename Buffers, typename CompletionHandler, typename Executor>
  void async_write_some_at(int fd, std::uint64_t offset, Buffers&& buffers, CompletionHandler&& handler, Executor&& executor) {
    do_async_op_(
        [fd, offset](struct ::iovec* iov, std::size_t iovcnt) {
          return ::pwritev(fd, iov, iovcnt, offset);
        },
        std::forward<Buffers>(buffers), std::forward<CompletionHandler>(handler), std::forward<Executor>(executor));
  }

  auto cancel([[maybe_unused]] int fd) -> std::error_code {
    assert(fd != -1);
    return {};
  }

  auto flush(int fd, [[maybe_unused]] bool data_only) -> std::error_code {
#if _POSIX_SYNCHRONIZED_IO >= 200112L
    if (data_only ? ::fdatasync(fd) : ::fsync(fd))
      return std::error_code(errno, std::generic_category());
#else
    if (::fsync(fd))
      return std::error_code(errno, std::generic_category());
#endif
    return {};
  }

  template<typename CompletionHandler, typename Executor>
  auto async_flush(int fd, bool data_only, CompletionHandler&& handler, Executor&& executor) {
    auto ec = flush(fd, data_only);
    auto ex = asio::get_associated_executor(handler, std::forward<Executor>(executor));
    auto alloc = asio::get_associated_allocator(handler);

    ex.post(
        [ handler=std::forward<CompletionHandler>(handler),
          ec
        ]() mutable {
          std::invoke(handler, ec);
        },
        std::move(alloc));
  }

  private:
  template<typename ImplFn, typename Buffers>
  static auto do_op_(ImplFn&& impl_fn, Buffers&& buffers, std::error_code& ec) -> std::size_t {
    ec.clear();
    if (asio::buffer_size(buffers) == 0) [[unlikely]] {
      return 0;
    }

    auto iov = buffers_to_iovec(std::forward<Buffers>(buffers));
    auto op_result = std::invoke(impl_fn, iov.data(), iov.size()); // side-effect: sets errno

    if (op_result == -1) [[unlikely]] {
      ec.assign(errno, std::generic_category());
      op_result = 0;
    } else if (op_result == 0) [[unlikely]] {
      ec = asio::error::eof;
    }

    return op_result;
  }

  template<typename ImplFn, typename Buffers, typename CompletionHandler, typename Executor>
  static void do_async_op_(ImplFn&& impl_fn, Buffers&& buffers, CompletionHandler&& handler, Executor&& executor) {
    if (asio::buffer_size(buffers) == 0) [[unlikely]] {
      do_empty_buffers(std::forward<CompletionHandler>(handler), std::forward<Executor>(executor));
    } else {
      auto iov = buffers_to_iovec(std::forward<Buffers>(buffers));
      auto op_result = std::invoke(impl_fn, iov.data(), iov.size()); // side-effect: sets errno
      process_result(op_result, std::forward<CompletionHandler>(handler), std::forward<Executor>(executor)); // side-effect: reads errno
    }
  }

  protected:
  template<typename Buffers>
  static auto buffers_to_iovec(Buffers&& buffers) -> iovec_array {
    iovec_array iov;
    auto b = asio::buffer_sequence_begin(buffers);
    auto e = asio::buffer_sequence_end(buffers);

    for (; b != e && iov.size() < IOV_MAX; ++b) {
      if constexpr(asio::is_const_buffer_sequence<std::remove_cvref_t<Buffers>>::value) {
        asio::const_buffer buf = *b;
        struct ::iovec v;
        v.iov_base = const_cast<void*>(buf.data());
        v.iov_len = buf.size();
        iov.push_back(v);
      } else {
        asio::mutable_buffer buf = *b;
        struct ::iovec v;
        v.iov_base = buf.data();
        v.iov_len = buf.size();
        iov.push_back(v);
      }
    }

    return iov;
  }

  template<typename ResultCode, typename CompletionHandler, typename Executor>
  static void process_result(ResultCode op_result, CompletionHandler&& handler, Executor&& executor) {
    auto ex = asio::get_associated_executor(handler, std::forward<Executor>(executor));
    auto alloc = asio::get_associated_allocator(handler);

    std::error_code ec;
    if (op_result == -1) [[unlikely]] {
      ec.assign(errno, std::generic_category());
      op_result = 0;
    } else if (op_result == 0) [[unlikely]] {
      ec = asio::error::eof;
    }

    ex.post(
        [ handler=std::forward<CompletionHandler>(handler),
          ec,
          n=static_cast<std::size_t>(op_result)
        ]() mutable {
          std::invoke(handler, ec, n);
        },
        std::move(alloc));
  }

  template<typename CompletionHandler, typename Executor>
  static void do_empty_buffers(CompletionHandler&& handler, Executor&& executor) {
    auto ex = asio::get_associated_executor(handler, std::forward<Executor>(executor));
    auto alloc = asio::get_associated_allocator(handler);

    ex.post(
        [ handler=std::forward<CompletionHandler>(handler)
        ]() mutable {
          std::invoke(handler, std::error_code(), std::size_t(0));
        },
        std::move(alloc));
  }
};


} /* namespace earnest::detail::aio_synchronous */
