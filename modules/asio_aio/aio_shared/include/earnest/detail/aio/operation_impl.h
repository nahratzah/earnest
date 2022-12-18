#ifndef EARNEST_DETAIL_AIO_OPERATION_IMPL_H
#define EARNEST_DETAIL_AIO_OPERATION_IMPL_H

#include <functional>
#include <memory>
#include <optional>
#include <system_error>
#include <type_traits>
#include <utility>

#include <asio/associated_allocator.hpp>
#include <asio/associated_executor.hpp>

#include <earnest/detail/aio/aio_op_list.h>
#include <earnest/detail/aio/aio_op.h>
#include <earnest/detail/aio/lio_op.h>
#include <earnest/detail/aio/flush_op.h>

namespace earnest::detail::aio {


template<typename BaseAioOp, typename CompletionHandler, typename Executor>
class operation_impl final
: public BaseAioOp
{
  static_assert(std::is_base_of_v<aio_op_list::operation, BaseAioOp>);

  public:
  using associated_allocator_type = typename asio::associated_allocator<CompletionHandler>::type;
  using executor_type = Executor;

  template<typename... BaseArgs>
  operation_impl(CompletionHandler&& handler, executor_type&& executor, BaseArgs&&... base_args)
  : BaseAioOp(std::forward<BaseArgs>(base_args)...),
    handler_(std::move(handler)),
    executor_(std::move(executor))
  {
    executor_.on_work_started();
  }

  ~operation_impl() {
    executor_.on_work_finished();
  }

  private:
  void destructor() && override final {
    using allocator_traits = typename std::allocator_traits<associated_allocator_type>::template rebind_traits<operation_impl>;
    using allocator_type = typename allocator_traits::allocator_type;

    allocator_type alloc = asio::associated_allocator<CompletionHandler>::get(handler_);
    allocator_traits::destroy(alloc, this);
    allocator_traits::deallocate(alloc, this, 1);
  }

  void invoke_handler(std::error_code ec, std::optional<std::size_t> nbytes, bool use_post) && override final {
    assert(!this->in_progress());

    using allocator_traits = typename std::allocator_traits<associated_allocator_type>::template rebind_traits<operation_impl>;
    using allocator_type = typename allocator_traits::allocator_type;

    allocator_type alloc = asio::associated_allocator<CompletionHandler>::get(handler_);

    if (use_post)
      executor_.post(std::bind(std::move(handler_), std::move(ec), nbytes.value_or(0)), alloc);
    else
      executor_.dispatch(std::bind(std::move(handler_), std::move(ec), nbytes.value_or(0)), alloc);

    allocator_traits::destroy(alloc, this);
    allocator_traits::deallocate(alloc, this, 1);
  }

  CompletionHandler handler_;
  executor_type executor_;
};

template<typename CompletionHandler, typename Executor>
class operation_impl<flush_op, CompletionHandler, Executor> final
: public flush_op
{
  static_assert(std::is_base_of_v<aio_op_list::operation, flush_op>);

  public:
  using associated_allocator_type = typename asio::associated_allocator<CompletionHandler>::type;
  using executor_type = Executor;

  template<typename... BaseArgs>
  operation_impl(CompletionHandler&& handler, executor_type&& executor, BaseArgs&&... base_args)
  : flush_op(std::forward<BaseArgs>(base_args)...),
    handler_(std::move(handler)),
    executor_(std::move(executor))
  {
    executor_.on_work_started();
  }

  ~operation_impl() {
    executor_.on_work_finished();
  }

  private:
  void destructor() && override final {
    using allocator_traits = typename std::allocator_traits<associated_allocator_type>::template rebind_traits<operation_impl>;
    using allocator_type = typename allocator_traits::allocator_type;

    allocator_type alloc = asio::associated_allocator<CompletionHandler>::get(handler_);
    allocator_traits::destroy(alloc, this);
    allocator_traits::deallocate(alloc, this, 1);
  }

  void invoke_handler(std::error_code ec, [[maybe_unused]] std::optional<std::size_t> nbytes, bool use_post) && override final {
    assert(!this->in_progress());

    using allocator_traits = typename std::allocator_traits<associated_allocator_type>::template rebind_traits<operation_impl>;
    using allocator_type = typename allocator_traits::allocator_type;

    allocator_type alloc = asio::associated_allocator<CompletionHandler>::get(handler_);

    if (use_post)
      executor_.post(std::bind(std::move(handler_), std::move(ec)), alloc);
    else
      executor_.dispatch(std::bind(std::move(handler_), std::move(ec)), alloc);

    allocator_traits::destroy(alloc, this);
    allocator_traits::deallocate(alloc, this, 1);
  }

  CompletionHandler handler_;
  executor_type executor_;
};


template<
    typename Buffers,
    typename AsioBufferType,
    bool SingleBuffer = std::is_convertible_v<Buffers, AsioBufferType>,
    bool NoOwnershipTracking = std::is_trivially_destructible_v<Buffers>>
struct operation_selector_;

template<typename Buffers, typename AsioBufferType>
struct operation_selector_<Buffers, AsioBufferType, false, false> {
  template<typename Alloc>
  using type = lio_op_with_buffer_ownership<Buffers, Alloc>;
};

template<typename Buffers, typename AsioBufferType>
struct operation_selector_<Buffers, AsioBufferType, false, true> {
  template<typename Alloc>
  using type = lio_op<Alloc>;
};

template<typename Buffers, typename AsioBufferType>
struct operation_selector_<Buffers, AsioBufferType, true, false> {
  template<typename Alloc>
  using type = aio_op_with_buffer_ownership<Buffers>;
};

template<typename Buffers, typename AsioBufferType>
struct operation_selector_<Buffers, AsioBufferType, true, true> {
  template<typename Alloc>
  using type = aio_op;
};


template<typename Buffers, typename AsioBufferType, typename CompletionHandler, typename Executor>
struct operation_selector {
  using allocator_resolver = asio::associated_allocator<std::decay_t<CompletionHandler>>;
  using executor_resolver = asio::associated_executor<std::decay_t<CompletionHandler>, std::decay_t<Executor>>;

  using allocator_type = typename allocator_resolver::type;
  using executor_type = typename executor_resolver::type;
  using op_type = typename operation_selector_<std::decay_t<Buffers>, AsioBufferType>::template type<allocator_type>;
  using type = operation_impl<op_type, std::decay_t<CompletionHandler>, executor_type>;
  using pointer = aio_op_list::operation_ptr<type>;

  template<typename ReadWriteOp>
  static auto make(aio_op_list& list, int fd, std::uint64_t offset, Buffers buffers, CompletionHandler handler, const Executor& file_executor, ReadWriteOp&& op_type)
  -> pointer {
    allocator_type alloc = allocator_resolver::get(handler);
    executor_type executor = executor_resolver::get(handler, file_executor);

    return aio_op_list::allocate_operation<type>(
        alloc,
        std::forward<CompletionHandler>(handler), std::move(executor),
        list, fd, offset, std::forward<Buffers>(buffers),
        std::forward<ReadWriteOp>(op_type));
  }
};


template<typename CompletionHandler, typename Executor>
struct flush_operation_selector {
  using allocator_resolver = asio::associated_allocator<std::decay_t<CompletionHandler>>;
  using executor_resolver = asio::associated_executor<std::decay_t<CompletionHandler>, std::decay_t<Executor>>;

  using allocator_type = typename allocator_resolver::type;
  using executor_type = typename executor_resolver::type;
  using type = operation_impl<flush_op, std::decay_t<CompletionHandler>, executor_type>;
  using pointer = aio_op_list::operation_ptr<type>;

  static auto make(aio_op_list& list, int fd, bool data_only, CompletionHandler handler, const Executor& file_executor)
  -> pointer {
    allocator_type alloc = allocator_resolver::get(handler);
    executor_type executor = executor_resolver::get(handler, file_executor);

    return aio_op_list::allocate_operation<type>(
        alloc,
        std::forward<CompletionHandler>(handler), std::move(executor),
        list, fd, data_only);
  }
};


template<typename Buffers, typename CompletionHandler, typename Executor>
auto new_read_operation(aio_op_list& list, int fd, std::uint64_t offset, Buffers&& buffers, CompletionHandler&& handler, const Executor& executor)
-> typename operation_selector<Buffers, asio::mutable_buffer, CompletionHandler, Executor>::pointer {
  using selector = operation_selector<Buffers, asio::mutable_buffer, CompletionHandler, Executor>;

  return selector::make(
      list, fd, offset, std::forward<Buffers>(buffers), std::forward<CompletionHandler>(handler), executor,
      selector::type::read_op);
}

template<typename Buffers, typename CompletionHandler, typename Executor>
auto new_write_operation(aio_op_list& list, int fd, std::uint64_t offset, Buffers&& buffers, CompletionHandler&& handler, const Executor& executor)
-> typename operation_selector<Buffers, asio::const_buffer, CompletionHandler, Executor>::pointer {
  using selector = operation_selector<Buffers, asio::const_buffer, CompletionHandler, Executor>;

  return selector::make(
      list, fd, offset, std::forward<Buffers>(buffers), std::forward<CompletionHandler>(handler), executor,
      selector::type::write_op);
}

template<typename CompletionHandler, typename Executor>
auto new_flush_operation(aio_op_list& list, int fd, bool data_only, CompletionHandler&& handler, const Executor& executor)
-> typename flush_operation_selector<CompletionHandler, Executor>::pointer {
  using selector = flush_operation_selector<CompletionHandler, Executor>;

  return selector::make(list, fd, data_only, std::forward<CompletionHandler>(handler), executor);
}


} /* namespace earnest::detail::aio */

#endif /* EARNEST_DETAIL_AIO_OPERATION_IMPL_H */
