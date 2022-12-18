#pragma once

#include <type_traits>

namespace earnest::detail {


constexpr auto constexpr_sum() -> unsigned int {
  return 0;
}

template<typename... T>
constexpr auto constexpr_sum(const T&... v) -> std::common_type_t<T...> {
  return (v +...);
}


} /* namespace earnest::detail */
