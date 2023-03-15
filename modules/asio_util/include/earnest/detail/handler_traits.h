#pragma once

#include <type_traits>

namespace earnest::detail {


template<typename Handler, typename = void>
struct handler_has_allocator_
: std::false_type
{};

template<typename Handler>
struct handler_has_allocator_<Handler, std::void_t<typename Handler::allocator_type>>
: std::true_type
{};


template<typename Handler, typename = void>
struct handler_has_executor_
: std::false_type
{};

template<typename Handler>
struct handler_has_executor_<Handler, std::void_t<typename Handler::executor_type>>
: std::true_type
{};


template<typename Handler>
using handler_has_allocator = typename handler_has_allocator_<Handler>::type;
template<typename Handler>
using handler_has_executor = typename handler_has_executor_<Handler>::type;

template<typename Handler>
inline constexpr bool handler_has_allocator_v = handler_has_allocator<Handler>::value;
template<typename Handler>
inline constexpr bool handler_has_executor_v = handler_has_executor<Handler>::value;


} /* namespace earnest::detail */
