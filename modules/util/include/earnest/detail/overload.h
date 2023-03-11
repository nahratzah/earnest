#pragma once

#include <type_traits>
#include <utility>

namespace earnest::detail {


template<typename... Functors>
class overload_t
: private Functors...
{
  public:
  overload_t(Functors... functors) noexcept(std::conjunction_v<std::is_nothrow_move_constructible<Functors>...>)
  : Functors(std::move(functors))...
  {}

  using Functors::operator()...;
};

template<typename... Functors>
auto overload(Functors... functors) noexcept(std::conjunction_v<std::is_nothrow_move_constructible<Functors>...>) -> overload_t<Functors...> {
  return overload_t<Functors...>(std::move(functors)...);
}


} /* namespace earnest::detail */
