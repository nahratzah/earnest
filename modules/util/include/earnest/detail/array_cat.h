#pragma once

#include <array>
#include <cstddef>
#include <type_traits>

#include <earnest/detail/constexpr_sum.h>

namespace earnest::detail {


template<typename T, std::size_t N, std::size_t M, std::size_t... IdxX, std::size_t... IdxY>
constexpr auto array_cat_(std::array<T, N> x, std::array<T, M> y,
    [[maybe_unused]] std::index_sequence<IdxX...> x_seq,
    [[maybe_unused]] std::index_sequence<IdxY...> y_seq)
    noexcept(std::is_nothrow_move_constructible_v<T>)
-> std::array<T, N + M> {
  return std::array<T, N + M>{
    std::get<IdxX>(std::move(x))...,
    std::get<IdxY>(std::move(y))...,
  };
}


template<typename T, std::size_t N>
constexpr auto array_cat(std::array<T, N> x)
    noexcept(std::is_nothrow_move_constructible_v<T>)
-> std::array<T, N> {
  return x;
}


template<typename T, std::size_t N, std::size_t M>
constexpr auto array_cat(std::array<T, N> x, std::array<T, M> y)
    noexcept(std::is_nothrow_move_constructible_v<T>)
-> std::array<T, N + M> {
  return array_cat_(std::move(x), std::move(y), std::make_index_sequence<N>(), std::make_index_sequence<M>());
}


template<typename T, std::size_t N, std::size_t M, std::size_t... Extra>
constexpr auto array_cat(std::array<T, N> x, std::array<T, M> y, std::array<T, Extra>... extra)
    noexcept(std::is_nothrow_move_constructible_v<T>)
-> std::array<T, constexpr_sum(N, M, Extra...)>
{
  return array_cat(array_cat(std::move(x), std::move(y)), std::move(extra)...);
}


} /* namespace earnest::detail */
