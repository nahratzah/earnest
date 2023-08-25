#pragma once

#include <type_traits>
#include <utility>

#include <asio/bind_executor.hpp>

#include <earnest/detail/completion_handler_fun.h>
#include <earnest/detail/handler_traits.h>
#include <earnest/detail/move_only_function.h>

namespace earnest::detail {


template<typename FunctionType, typename Handler, typename Executor>
auto type_erased_handler(Handler&& handler, Executor&& ex) {
  move_only_function<FunctionType> fn;
  if constexpr(handler_has_executor_v<std::remove_reference_t<decltype(handler)>>)
    fn = completion_handler_fun(std::forward<Handler>(handler), ex);
  else
    fn = std::forward<Handler>(handler);

  return asio::bind_executor(std::forward<Executor>(ex), std::move(fn));
}


} /* namespace earnest::detail */
