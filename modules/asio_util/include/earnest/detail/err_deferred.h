#pragma once

#include <system_error>
#include <tuple>
#include <utility>

#include <asio/deferred.hpp>

namespace earnest::detail {


class err_deferred_t {
  private:
  // Tiny marker type to make it so the constructor for impl doesn't try to capture copy/move construction.
  struct constructor {};

  template<typename Fn, typename... FailArgs>
  class impl {
    static_assert((std::copy_constructible<Fn> &&...&& std::copy_constructible<FailArgs>));

    public:
    template<typename Fn_, typename... FailArgs_>
    requires (std::constructible_from<Fn, Fn_> &&...&& std::constructible_from<FailArgs, FailArgs_>)
    explicit impl([[maybe_unused]] constructor, Fn_&& fn, FailArgs_&&... fail_args)
    : fn(std::forward<Fn_>(fn)),
      fail_args_tuple(std::forward<FailArgs_>(fail_args)...)
    {}

    impl(const impl&) = default;
    impl(impl&&) = default;

    template<typename... Args>
    auto operator()(std::error_code ec, Args&&... args) && {
      if constexpr(sizeof...(Args) == 0 && !std::is_invocable_v<Fn>) {
        assert(ec != std::error_code{});
        return fail_args_(ec);
      } else {
        return asio::deferred.when(!ec)
            .then(
                asio::deferred.values(std::forward<Args>(args)...)
                | asio::deferred(std::move(fn)))
            .otherwise(fail_args_(ec));
      }
    }

    private:
    auto fail_args_(std::error_code ec) {
      return std::apply(
          [ec](auto&&... args) {
            return asio::deferred.values(ec, std::move(args)...);
          },
          std::move(fail_args_tuple));
    }

    Fn fn;
    std::tuple<FailArgs...> fail_args_tuple;
  };

  public:
  constexpr err_deferred_t() noexcept = default;

  template<typename Fn, typename... FailArgs>
  auto operator()(Fn&& fn, FailArgs&&... fail_args) const {
    return asio::deferred(impl<std::remove_cvref_t<Fn>, std::remove_cvref_t<FailArgs>...>(constructor{}, std::forward<Fn>(fn), std::forward<FailArgs>(fail_args)...));
  }
};

// Creates an asio-deferred function.
// The created function takes an std::error_code as its first argument.
// If the error-code holds an error, then the wrapped function is skipped, and the error is forwarded.
// Otherwise, the wrapped function is invoked without the error-code, but with any trailing arguments.
//
// Note: if the returned function is invoked with only an error-code, and the wrapped function isn't no-arg invocable,
// it'll assert that the error-code is set, and forward it.
inline constexpr err_deferred_t err_deferred;


} /* namespace earnest::detail */
