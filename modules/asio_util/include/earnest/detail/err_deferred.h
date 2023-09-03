#pragma once

#include <system_error>
#include <tuple>
#include <utility>

#include <asio/deferred.hpp>

namespace earnest::detail {


class err_deferred_t {
  public:
  constexpr err_deferred_t() noexcept = default;

  template<typename Fn, typename... FailArgs>
  auto operator()(Fn&& fn, FailArgs&&... fail_args) const {
    return asio::deferred(
        [fn=std::forward<Fn>(fn), fail_args_tuple=std::make_tuple(std::forward<FailArgs>(fail_args)...)]<typename... Args>(std::error_code ec, Args&&... args) mutable {
          auto fail_args = std::apply(
              [ec](auto&&... args) {
                return asio::deferred.values(ec, std::move(args)...);
              },
              std::move(fail_args_tuple));

          if constexpr(sizeof...(Args) == 0 && !std::is_invocable_v<Fn>) {
            assert(ec != std::error_code{});
            return fail_args;
          } else {
            return asio::deferred.when(!ec)
                .then(
                    asio::deferred.values(std::forward<Args>(args)...)
                    | asio::deferred(std::move(fn)))
                .otherwise(std::move(fail_args));
          }
        });
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
