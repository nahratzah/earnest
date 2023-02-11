#pragma once

#include <cstddef>
#include <utility>

namespace earnest::detail {


inline constexpr auto hash_combine(std::size_t h) -> std::size_t {
  return h;
}

template<typename... Args>
inline constexpr auto hash_combine(std::size_t h0, std::size_t h1, Args&&... args) {
  // Algorithm from boost::hash_combine, but operating on the hashes directly, instead of taking objects.
  const std::size_t h = h1 + std::size_t(0x9e3779b9u) + (h0 << 6u) + (h0 >> 2u);
  return hash_combine(h, std::forward<Args>(args)...);
}


} /* namespace earnest::detail */
