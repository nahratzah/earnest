#pragma once

#include <stdio.h> // DEBUG

#include <aio.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <tuple>
#include <memory>
#include <system_error>
#include <functional>
#include <utility>
#include <type_traits>

#include <asio/associated_allocator.hpp>
#include <asio/associated_executor.hpp>
#include <asio/error.hpp>
#include <asio/execution_context.hpp>
#include <asio/executor_work_guard.hpp>

#include <earnest/detail/aio/synchronous_reactor.h>

namespace earnest::detail::aio {


class aio_reactor
: public synchronous_reactor
{
  public:
  using key_type = aio_reactor;
  static inline const asio::execution_context::id id;

  private:
  template<typename Allocator>
  class alloc_based_deleter
  : private Allocator
  {
    public:
    using allocator_type = Allocator;
    using allocator_traits = std::allocator_traits<allocator_type>;

    alloc_based_deleter(allocator_type alloc) noexcept(std::is_nothrow_move_constructible_v<allocator_type> || std::is_nothrow_copy_constructible_v<allocator_type>)
    : Allocator(std::move_if_noexcept(alloc))
    {}

    auto get_allocator() const noexcept -> const allocator_type& { return *this; }
    auto get_allocator() noexcept -> allocator_type& { return *this; }

    auto operator()(typename allocator_traits::pointer ptr) {
      allocator_type& alloc = *this;
      allocator_traits::destroy(alloc, ptr);
      allocator_traits::deallocate(alloc, ptr, 1);
    }
  };

  template<typename CompletionHandler, typename Executor, typename Allocator>
  class operation final {
    public:
    template<typename Buffers>
    operation(int fd, std::uint64_t offset, Buffers&& buffers, CompletionHandler handler, Executor executor, Allocator allocator)
    : allocator_(std::move(allocator)),
      work_guard_(std::move(executor)),
      handler_(std::move(handler))
    {
      std::memset(&aio_, 0, sizeof(aio_));
      aio_.aio_fildes = fd;
      aio_.aio_offset = offset;
      iov_ = buffers_to_iovec(std::forward<Buffers>(buffers));
      aio_.aio_iov = iov_.data();
      aio_.aio_iovcnt = iov_.size();

      aio_.aio_sigevent.sigev_notify = SIGEV_THREAD;
      aio_.aio_sigevent.sigev_value.sigval_ptr = this;
      aio_.aio_sigevent.sigev_notify_function = [](union ::sigval sv) noexcept {
        using allocator_type = typename std::allocator_traits<Allocator>::template rebind_alloc<operation>;

        operation*const self = static_cast<operation*>(sv.sigval_ptr);
        auto self_ptr = std::unique_ptr<operation, alloc_based_deleter<allocator_type>>(
            self,
            alloc_based_deleter<allocator_type>(self->allocator_));
        self_ptr->run_handler_();
      };
    }

    operation(const operation&) = delete;
    operation(operation&&) = delete;
    operation& operator=(const operation&) = delete;
    operation& operator=(operation&&) = delete;

    auto get_aiocb() const noexcept -> const struct ::aiocb& { return aio_; }
    auto get_aiocb() noexcept -> struct ::aiocb& { return aio_; }

    auto fail_handler(std::error_code ec) {
      work_guard_.get_executor().post(
          [fn=std::move(handler_), ec]() mutable {
            std::invoke(fn, ec, std::size_t(0));
          },
          allocator_);
    }

    private:
    auto run_handler_() {
      work_guard_.get_executor().dispatch(
          wrap_handler_(),
          allocator_);
    }

    auto wrap_handler_() {
      const auto e = ::aio_error(&aio_);
      assert(e != EINPROGRESS);
      if (e == -1) [[unlikely]] {
        // aio_error itself failed somehow.
        // At this point, just give up.
        throw std::system_error(errno, std::generic_category(), "aio_error");
      }

      auto len = ::aio_return(&aio_);

      std::error_code ec;
      if (e != 0) [[unlikely]] {
        ec.assign(e, std::generic_category());
        len = 0;
      } else if (len == 0) {
        ec = asio::error::eof;
      }

      return [fn=std::move(handler_), ec, n=static_cast<std::size_t>(len)]() mutable {
        std::invoke(fn, ec, n);
      };
    }

    Allocator allocator_;
    asio::executor_work_guard<Executor> work_guard_;
    CompletionHandler handler_;
    struct ::aiocb aio_;
    iovec_array iov_;
  };

  public:
  explicit aio_reactor(asio::execution_context& ctx)
  : synchronous_reactor(ctx)
  {}

  ~aio_reactor() override = default;

  using synchronous_reactor::async_read_some;
  using synchronous_reactor::async_write_some;

  template<typename Buffers, typename CompletionHandler, typename Executor>
  void async_read_some_at(int fd, std::uint64_t offset, Buffers&& buffers, CompletionHandler&& handler, Executor&& executor) {
    do_op_(
        [](struct ::aiocb* aio) {
          return ::aio_readv(aio);
        },
        fd, offset, std::forward<Buffers>(buffers), std::forward<CompletionHandler>(handler), std::forward<Executor>(executor));
  }

  template<typename Buffers, typename CompletionHandler, typename Executor>
  void async_write_some_at(int fd, std::uint64_t offset, Buffers&& buffers, CompletionHandler&& handler, Executor&& executor) {
    do_op_(
        [](struct ::aiocb* aio) {
          return ::aio_writev(aio);
        },
        fd, offset, std::forward<Buffers>(buffers), std::forward<CompletionHandler>(handler), std::forward<Executor>(executor));
  }

  private:
  template<typename ImplFn, typename Buffers, typename CompletionHandler, typename Executor>
  static void do_op_(ImplFn&& impl_fn, int fd, std::uint64_t offset, Buffers&& buffers, CompletionHandler&& handler, Executor&& executor) {
    if (asio::buffer_size(buffers) == 0) [[unlikely]] {
      do_empty_buffers(std::forward<CompletionHandler>(handler), std::forward<Executor>(executor));
    } else {
      auto ex = asio::get_associated_executor(handler, std::forward<Executor>(executor));
      auto alloc = asio::get_associated_allocator(handler);

      using operation_type = operation<
          std::remove_cvref_t<CompletionHandler>,
          decltype(ex),
          typename std::allocator_traits<decltype(alloc)>::template rebind_alloc<std::byte>>;
      using allocator_type = typename std::allocator_traits<decltype(alloc)>::template rebind_alloc<operation_type>;
      using deleter_type = alloc_based_deleter<allocator_type>;
      using allocator_traits = std::allocator_traits<allocator_type>;

      auto operation_ptr = std::unique_ptr<operation_type, deleter_type>(nullptr, deleter_type(alloc));
      auto uninitialized_operation_ptr = allocator_traits::allocate(operation_ptr.get_deleter().get_allocator(), 1);
      try {
        allocator_traits::construct(operation_ptr.get_deleter().get_allocator(), uninitialized_operation_ptr,
            fd, offset, std::forward<Buffers>(buffers), std::forward<CompletionHandler>(handler), std::move(ex), alloc);
      } catch (...) {
        allocator_traits::deallocate(operation_ptr.get_deleter().get_allocator(), uninitialized_operation_ptr, 1);
        throw;
      } // uninitialized_operation_ptr is no longer uninitialized.
      operation_ptr.reset(uninitialized_operation_ptr);

      const int op_result = std::invoke(impl_fn, &operation_ptr->get_aiocb()); // side effect: sets errno
      if (op_result == -1) [[unlikely]] {
        ::perror("aio_readv or aio_writev"); // DEBUG
        operation_ptr->fail_handler(std::error_code(errno, std::generic_category()));
        return;
      }
      operation_ptr.release(); // callback will re-acquire the pointer and handle destruction
    }
  }
};


} /* namespace earnest::detail::aio */
