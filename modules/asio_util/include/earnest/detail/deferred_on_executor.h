#pragma once

#include <cstddef>
#include <memory>
#include <tuple>
#include <utility>

#include <asio/async_result.hpp>
#include <asio/deferred.hpp>
#include <asio/executor_work_guard.hpp>

namespace earnest::detail {


template<typename Signature, typename Functor, typename Executor, typename... Args>
auto deferred_on_executor(Functor&& functor, Executor&& executor, Args&&... args) {
  return asio::async_initiate<decltype(asio::deferred), Signature>(
      [](auto handler, auto functor, auto wg, auto args_tpl) {
        wg.get_executor().dispatch(
            completion_wrapper<void()>(
                std::move(handler),
                [functor=std::move(functor), args_tpl=std::move(args_tpl)](auto handler) mutable {
                  std::apply(functor, std::move(args_tpl)) | std::move(handler);
                }),
            std::allocator<std::byte>());
        wg.reset();
      },
      asio::deferred, std::forward<Functor>(functor), asio::make_work_guard(std::forward<Executor>(executor)), std::make_tuple(std::forward<Args>(args)...));
}


} /* namespace earnest::detail */
