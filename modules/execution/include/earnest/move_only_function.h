#pragma once

#include <functional>

#if __cpp_lib_move_only_function < 202110L
# include <cstddef>
# include <memory>
# include <utility>
#endif

namespace earnest {

#if __cpp_lib_move_only_function >= 202110L
using move_only_function = std::move_only_function;
#else

template<typename> class move_only_function;

#define _move_only_generator_(cv_ref, fn_invocation, noex)                                                                \
                                                                                                                          \
template<typename R, typename... Args>                                                                                    \
class move_only_function<R(Args...) cv_ref noexcept(noex)> {                                                              \
  private:                                                                                                                \
  class intf {                                                                                                            \
    public:                                                                                                               \
    virtual ~intf() = default;                                                                                            \
    virtual auto operator()(std::add_rvalue_reference_t<Args>... args) noexcept(noex) -> R = 0;                           \
  };                                                                                                                      \
                                                                                                                          \
  template<typename Fn>                                                                                                   \
  class impl                                                                                                              \
  : public intf                                                                                                           \
  {                                                                                                                       \
    public:                                                                                                               \
    explicit impl(Fn&& fn) noexcept(std::is_nothrow_move_constructible_v<Fn>)                                             \
    : fn(std::move(fn))                                                                                                   \
    {}                                                                                                                    \
                                                                                                                          \
    explicit impl(const Fn& fn) noexcept(std::is_nothrow_copy_constructible_v<Fn>)                                        \
    : fn(fn)                                                                                                              \
    {}                                                                                                                    \
                                                                                                                          \
    template<typename... CArgs>                                                                                           \
    explicit impl(CArgs&&... cargs) noexcept(std::is_nothrow_constructible_v<Fn, CArgs...>)                               \
    : fn(std::forward<CArgs>(cargs)...)                                                                                   \
    {}                                                                                                                    \
                                                                                                                          \
    template<typename U, typename... CArgs>                                                                               \
    explicit impl(std::initializer_list<U> il, CArgs&&... cargs)                                                          \
    noexcept(std::is_nothrow_constructible_v<Fn, std::initializer_list<U>&, CArgs...>)                                    \
    : fn(il, std::forward<CArgs>(cargs)...)                                                                               \
    {}                                                                                                                    \
                                                                                                                          \
    ~impl() override = default;                                                                                           \
                                                                                                                          \
    auto operator()(std::add_rvalue_reference_t<Args>... args) noexcept(noex) -> R override {                             \
      return std::invoke(fn_invocation, std::forward<Args>(args)...);                                                     \
    }                                                                                                                     \
                                                                                                                          \
    private:                                                                                                              \
    Fn fn;                                                                                                                \
  };                                                                                                                      \
                                                                                                                          \
  public:                                                                                                                 \
  move_only_function() noexcept = default;                                                                                \
  move_only_function([[maybe_unused]] std::nullptr_t) noexcept {}                                                         \
                                                                                                                          \
  move_only_function(move_only_function&& other) noexcept                                                                 \
  : fn(std::move(other.fn))                                                                                               \
  {}                                                                                                                      \
                                                                                                                          \
  move_only_function(const move_only_function&) = delete;                                                                 \
                                                                                                                          \
  template<std::invocable<std::add_rvalue_reference_t<Args>...> Fn>                                                       \
  move_only_function(Fn&& fn)                                                                                             \
  : fn(std::make_unique<impl<std::remove_cvref_t<Fn>>>(std::forward<Fn>(fn)))                                             \
  {}                                                                                                                      \
                                                                                                                          \
  template<typename T, typename... CArgs>                                                                                 \
  explicit move_only_function([[maybe_unused]] std::in_place_type_t<T>, CArgs&&... cargs)                                 \
  : fn(std::make_unique<impl<T>>(std::forward<CArgs>(cargs)...))                                                          \
  {}                                                                                                                      \
                                                                                                                          \
  template<typename T, typename U, typename... CArgs>                                                                     \
  explicit move_only_function([[maybe_unused]] std::in_place_type_t<T>, std::initializer_list<U> il, CArgs&&... cargs)    \
  : fn(std::make_unique<impl<T>>(il, std::forward<CArgs>(cargs)...))                                                      \
  {}                                                                                                                      \
                                                                                                                          \
  auto operator=(move_only_function&& other) -> move_only_function& {                                                     \
    fn = std::move(other.fn);                                                                                             \
    return *this;                                                                                                         \
  }                                                                                                                       \
                                                                                                                          \
  auto operator=(const move_only_function&) -> move_only_function& = delete;                                              \
                                                                                                                          \
  auto operator=([[maybe_unused]] std::nullptr_t) noexcept -> move_only_function& {                                       \
    fn.reset();                                                                                                           \
    return *this;                                                                                                         \
  }                                                                                                                       \
                                                                                                                          \
  template<std::invocable<Args...> Fn>                                                                                    \
  auto operator=(Fn&& new_fn) -> move_only_function& {                                                                    \
    if constexpr(std::is_void_v<std::void_t<decltype(std::declval<const std::remove_cvref_t<Fn>>() == nullptr)>>) {       \
      if (new_fn == nullptr) {                                                                                            \
        this->fn.reset();                                                                                                 \
        return *this;                                                                                                     \
      }                                                                                                                   \
    }                                                                                                                     \
                                                                                                                          \
    fn = std::make_unique<impl<std::remove_cvref_t<Fn>>>(std::forward<Fn>(new_fn));                                       \
    return *this;                                                                                                         \
  }                                                                                                                       \
                                                                                                                          \
  void swap(move_only_function& other) noexcept {                                                                         \
    fn.swap(other.fn);                                                                                                    \
  }                                                                                                                       \
                                                                                                                          \
  explicit operator bool() const noexcept {                                                                               \
    return fn != nullptr;                                                                                                 \
  }                                                                                                                       \
                                                                                                                          \
  auto operator()(Args... args) cv_ref noexcept(noex) -> R {                                                              \
    return std::invoke(*fn, std::forward<Args>(args)...);                                                                 \
  }                                                                                                                       \
                                                                                                                          \
  friend auto operator==(const move_only_function& f, [[maybe_unused]] nullptr_t) noexcept -> bool {                      \
    return f.fn == nullptr;                                                                                               \
  }                                                                                                                       \
                                                                                                                          \
  friend auto swap(move_only_function& lhs, move_only_function& rhs) noexcept -> void {                                   \
    lhs.swap(rhs);                                                                                                        \
  }                                                                                                                       \
                                                                                                                          \
  private:                                                                                                                \
  std::unique_ptr<intf> fn;                                                                                               \
}

_move_only_generator_(        , fn                          , false);
_move_only_generator_(        , fn                          , true );
_move_only_generator_(      & , fn                          , false);
_move_only_generator_(      & , fn                          , true );
_move_only_generator_(      &&, std::move(fn)               , false);
_move_only_generator_(      &&, std::move(fn)               , true );
_move_only_generator_(const   , std::as_const(fn)           , false);
_move_only_generator_(const   , std::as_const(fn)           , true );
_move_only_generator_(const & , std::as_const(fn)           , false);
_move_only_generator_(const & , std::as_const(fn)           , true );
_move_only_generator_(const &&, std::move(std::as_const(fn)), false);
_move_only_generator_(const &&, std::move(std::as_const(fn)), true );

#undef _move_only_generator_

#endif

} /* namespace earnest */
