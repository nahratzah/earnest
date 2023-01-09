#pragma once

#include <functional>
#include <type_traits>
#include <utility>

namespace earnest::detail {


template<typename Fn>
class on_scope_exit {
  public:
  explicit on_scope_exit(Fn fn) noexcept(std::is_nothrow_move_constructible_v<Fn>)
  : fn_(std::move(fn))
  {}

  ~on_scope_exit() noexcept(std::is_nothrow_invocable_v<Fn>) {
    std::invoke(fn_);
  }

  private:
  Fn fn_;
};


} /* namespace earnest */
