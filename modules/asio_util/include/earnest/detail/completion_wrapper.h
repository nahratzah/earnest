#ifndef EARNEST_DETAIL_COMPLETION_WRAPPER_H
#define EARNEST_DETAIL_COMPLETION_WRAPPER_H

#include <functional>
#include <type_traits>
#include <utility>

namespace earnest::detail {


template<typename Derived, typename Handler, typename = void>
class completion_wrapper_allocator_aspect_ {
  protected:
  constexpr completion_wrapper_allocator_aspect_() noexcept = default;
  constexpr completion_wrapper_allocator_aspect_(const completion_wrapper_allocator_aspect_&) noexcept = default;
  constexpr completion_wrapper_allocator_aspect_(completion_wrapper_allocator_aspect_&&) noexcept = default;
  constexpr completion_wrapper_allocator_aspect_& operator=(const completion_wrapper_allocator_aspect_&) noexcept = default;
  constexpr completion_wrapper_allocator_aspect_& operator=(completion_wrapper_allocator_aspect_&&) noexcept = default;
  ~completion_wrapper_allocator_aspect_() noexcept = default;
};

template<typename Derived, typename Handler>
class completion_wrapper_allocator_aspect_<Derived, Handler, std::void_t<typename Handler::allocator_type>> {
  protected:
  constexpr completion_wrapper_allocator_aspect_() noexcept = default;
  constexpr completion_wrapper_allocator_aspect_(const completion_wrapper_allocator_aspect_&) noexcept = default;
  constexpr completion_wrapper_allocator_aspect_(completion_wrapper_allocator_aspect_&&) noexcept = default;
  constexpr completion_wrapper_allocator_aspect_& operator=(const completion_wrapper_allocator_aspect_&) noexcept = default;
  constexpr completion_wrapper_allocator_aspect_& operator=(completion_wrapper_allocator_aspect_&&) noexcept = default;
  ~completion_wrapper_allocator_aspect_() noexcept = default;

  public:
  using allocator_type = typename Handler::allocator_type;

  auto get_allocator() const -> allocator_type {
    return static_cast<const Derived&>(*this).handler_.get_allocator();
  }
};


template<typename Derived, typename Handler, typename = void>
class completion_wrapper_executor_aspect_ {
  protected:
  constexpr completion_wrapper_executor_aspect_() noexcept = default;
  constexpr completion_wrapper_executor_aspect_(const completion_wrapper_executor_aspect_&) noexcept = default;
  constexpr completion_wrapper_executor_aspect_(completion_wrapper_executor_aspect_&&) noexcept = default;
  constexpr completion_wrapper_executor_aspect_& operator=(const completion_wrapper_executor_aspect_&) noexcept = default;
  constexpr completion_wrapper_executor_aspect_& operator=(completion_wrapper_executor_aspect_&&) noexcept = default;
  ~completion_wrapper_executor_aspect_() noexcept = default;
};

template<typename Derived, typename Handler>
class completion_wrapper_executor_aspect_<Derived, Handler, std::void_t<typename Handler::executor_type>> {
  protected:
  constexpr completion_wrapper_executor_aspect_() noexcept = default;
  constexpr completion_wrapper_executor_aspect_(const completion_wrapper_executor_aspect_&) noexcept = default;
  constexpr completion_wrapper_executor_aspect_(completion_wrapper_executor_aspect_&&) noexcept = default;
  constexpr completion_wrapper_executor_aspect_& operator=(const completion_wrapper_executor_aspect_&) noexcept = default;
  constexpr completion_wrapper_executor_aspect_& operator=(completion_wrapper_executor_aspect_&&) noexcept = default;
  ~completion_wrapper_executor_aspect_() noexcept = default;

  public:
  using executor_type = typename Handler::executor_type;

  auto get_executor() const -> executor_type {
    return static_cast<const Derived&>(*this).handler_.get_executor();
  }
};


template<typename Handler, typename Signature, typename Adapter> class completion_wrapper_t;

template<typename Handler, typename... Args, typename Adapter>
class completion_wrapper_t<Handler, void(Args...), Adapter>
: public completion_wrapper_allocator_aspect_<completion_wrapper_t<Handler, void(Args...), Adapter>, Handler>,
  public completion_wrapper_executor_aspect_<completion_wrapper_t<Handler, void(Args...), Adapter>, Handler>
{
  static_assert(
      std::is_invocable_v<std::add_lvalue_reference_t<Adapter>, std::add_rvalue_reference_t<Handler>, Args...> ||
      std::is_invocable_v<std::add_lvalue_reference_t<Adapter>, std::add_lvalue_reference_t<Handler>, Args...>,
      "adapter must be invokable with (Handler, Args...)");

  template<typename, typename, typename> friend class ::earnest::detail::completion_wrapper_allocator_aspect_;
  template<typename, typename, typename> friend class ::earnest::detail::completion_wrapper_executor_aspect_;

  public:
  using handler_type = Handler;

  explicit completion_wrapper_t(handler_type handler, Adapter adapter)
      noexcept(std::is_nothrow_move_constructible_v<handler_type> && std::is_nothrow_move_constructible_v<Adapter>)
  : handler_(std::move(handler)),
    adapter_(std::move(adapter))
  {}

  void operator()(Args... args) {
    if constexpr(std::is_invocable_v<std::add_lvalue_reference_t<Adapter>, std::add_rvalue_reference_t<handler_type>, Args...>)
      std::invoke(adapter_, std::move(handler_), std::forward<Args>(args)...);
    else
      std::invoke(adapter_, handler_, std::forward<Args>(args)...);
  }

  private:
  handler_type handler_;
  Adapter adapter_;
};


template<typename Signature, typename Handler, typename Adapter>
auto completion_wrapper(Handler&& handler, Adapter&& adapter) -> completion_wrapper_t<std::decay_t<Handler>, Signature, std::decay_t<Adapter>> {
  return completion_wrapper_t<std::decay_t<Handler>, Signature, std::decay_t<Adapter>>(
      std::forward<Handler>(handler), std::forward<Adapter>(adapter));
}


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_COMPLETION_WRAPPER_H */
