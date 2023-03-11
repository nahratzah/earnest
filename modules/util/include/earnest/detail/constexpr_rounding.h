#pragma once

#include <concepts>
#include <type_traits>

namespace earnest::detail {


template<std::unsigned_integral T>
constexpr auto round_up(T v, std::type_identity_t<T> rounding) {
  constexpr T one = 1;
  return (v + (rounding - one)) / rounding * rounding;
}


} /* namespace earnest::detail */
