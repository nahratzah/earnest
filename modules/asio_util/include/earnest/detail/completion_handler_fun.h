#pragma once

#include <concepts>
#include <tuple>
#include <type_traits>
#include <utility>

#include <asio/associated_allocator.hpp>
#include <asio/bind_allocator.hpp>
#include <asio/bind_executor.hpp>
#include <asio/executor_work_guard.hpp>

namespace earnest::detail {


// We place the binder outside the class,
// to have the compiler require less distinct types.
template<typename Handler, typename... Args>
inline auto completion_handler_fun_bind_(Handler&& h, std::tuple<Args...>&& args) {
  return [args=std::move(args), h=std::move(h)]() mutable {
           std::apply(h, std::move(args));
         };
}


template<typename Handler, typename WorkGuard>
class unbound_completion_handler_fun_t {
  private:
  using associated_allocator = asio::associated_allocator<Handler>;

  public:
  using inner_handler_type = Handler;
  using executor_type = typename WorkGuard::executor_type;
  using allocator_type = typename associated_allocator::type;

  unbound_completion_handler_fun_t(WorkGuard wg, Handler h)
  : wg_(std::move(wg)),
    h_(std::move(h))
  {}

  auto get_executor() const -> executor_type { return wg_.get_executor(); }
  auto get_allocator() const -> allocator_type { return associated_allocator::get(h_); }
  auto inner_handler() const & -> const inner_handler_type& { return h_; }
  auto inner_handler() & -> inner_handler_type& { return h_; }
  auto inner_handler() && -> inner_handler_type&& { return std::move(h_); }

  template<typename... Args>
  auto operator()(Args&&... args) -> void {
    post(std::forward<Args>(args)...);
  }

  template<typename... Args>
  auto post(Args&&... args) -> void {
    auto alloc = asio::get_associated_allocator(h_);
    wg_.get_executor().post(bind_(std::forward<Args>(args)...), std::move(alloc));
    wg_.reset();
  }

  template<typename... Args>
  auto dispatch(Args&&... args) -> void {
    auto alloc = asio::get_associated_allocator(h_);
    wg_.get_executor().dispatch(bind_(std::forward<Args>(args)...), std::move(alloc));
    wg_.reset();
  }

  template<typename... Args>
  auto defer(Args&&... args) -> void {
    auto alloc = asio::get_associated_allocator(h_);
    wg_.get_executor().defer(bind_(std::forward<Args>(args)...), std::move(alloc));
    wg_.reset();
  }

  private:
  template<typename... Args>
  auto bind_(Args&&... args) {
    return completion_handler_fun_bind_(std::move(h_), std::make_tuple(std::forward<Args>(args)...));
  }

  WorkGuard wg_;
  Handler h_;
};

template<typename Handler, typename WorkGuard, typename BoundExecutor, typename BoundAllocator>
class completion_handler_fun_t
: private unbound_completion_handler_fun_t<Handler, WorkGuard>
{
  public:
  using inner_handler_type = typename unbound_completion_handler_fun_t<Handler, WorkGuard>::inner_handler_type;
  using executor_type = BoundExecutor;
  using allocator_type = BoundAllocator;

  completion_handler_fun_t(WorkGuard wg, Handler h, BoundExecutor bind_ex, BoundAllocator bind_alloc)
  : unbound_completion_handler_fun_t<Handler, WorkGuard>(std::move(wg), std::move(h)),
    bind_ex_(std::move(bind_ex)),
    bind_alloc_(std::move(bind_alloc))
  {}

  auto get_executor() const -> executor_type { return bind_ex_; }
  auto get_allocator() const -> allocator_type { return bind_alloc_; }

  auto as_unbound_handler() const & -> const unbound_completion_handler_fun_t<Handler, WorkGuard>& { return *this; }
  auto as_unbound_handler() & -> unbound_completion_handler_fun_t<Handler, WorkGuard>& { return *this; }
  auto as_unbound_handler() && -> unbound_completion_handler_fun_t<Handler, WorkGuard>&& { return std::move(*this); }

  using unbound_completion_handler_fun_t<Handler, WorkGuard>::inner_handler;
  using unbound_completion_handler_fun_t<Handler, WorkGuard>::operator();
  using unbound_completion_handler_fun_t<Handler, WorkGuard>::post;
  using unbound_completion_handler_fun_t<Handler, WorkGuard>::dispatch;
  using unbound_completion_handler_fun_t<Handler, WorkGuard>::defer;

  private:
  BoundExecutor bind_ex_;
  BoundAllocator bind_alloc_;
};


template<typename Handler, typename HandlerExecutor>
inline auto completion_handler_fun(Handler&& handler, HandlerExecutor&& ex) {
  auto wg = asio::make_work_guard(handler, std::move(ex));
  return unbound_completion_handler_fun_t<std::remove_cvref_t<Handler>, decltype(wg)>(
      std::move(wg), std::forward<Handler>(handler));
}

template<typename Handler, typename HandlerExecutor, typename BoundExecutor, typename Allocator = std::allocator<std::byte>>
inline auto completion_handler_fun(Handler&& handler, HandlerExecutor&& ex, BoundExecutor&& bind_ex, Allocator bind_alloc = Allocator()) {
  auto wg = asio::make_work_guard(handler, ex);
  return completion_handler_fun_t<std::remove_cvref_t<Handler>, decltype(wg), std::remove_cvref_t<BoundExecutor>, std::remove_cvref_t<Allocator>>(
      std::move(wg), std::forward<Handler>(handler),
      std::forward<BoundExecutor>(bind_ex), std::move(bind_alloc));
}


} /* namespace earnest::detail */
