#pragma once

#include <functional>
#include <type_traits>
#include <utility>

#include <asio/async_result.hpp>
#include <asio/deferred.hpp>

#include <earnest/detail/completion_handler_fun.h>

namespace earnest::detail {


template<typename Ex>
inline auto forward_deferred(Ex&& ex) {
  return asio::deferred(
      [ex=std::forward<Ex>(ex)]<typename... Args>(Args&&... args) mutable {
        return asio::async_initiate<decltype(asio::deferred), void(std::remove_cvref_t<Args>...)>(
            [](auto handler, auto ex, auto... args) {
              std::invoke(
                  completion_handler_fun(std::move(handler), std::move(ex)),
                  std::move(args)...);
            },
            asio::deferred, std::move(ex), std::forward<Args>(args)...);
      });
}


} /* namespace earnest::detail */
