#ifndef EARNEST_DETAIL_CONCAT_BUFFER_H
#define EARNEST_DETAIL_CONCAT_BUFFER_H

#include <iterator>
#include <tuple>
#include <type_traits>
#include <utility>

#include <asio/buffer.hpp>

namespace earnest::detail {


template<typename... MB>
class concat_buffer_iterator_ {
  public:
  struct at_end {};

  private:
  template<typename ToType>
  using mb_are_convertible_to_sequence_of = std::conjunction<
      std::is_convertible<
          std::remove_reference_t<decltype(*asio::buffer_sequence_begin(std::declval<MB&>()))>,
          ToType>...
      >;
  template<typename ToType>
  static constexpr bool mb_are_convertible_to_sequence_of_v = mb_are_convertible_to_sequence_of<ToType>::value;

  template<typename B>
  using begin_iter_type = std::remove_reference_t<decltype(asio::buffer_sequence_begin(std::declval<B&>()))>;
  template<typename B>
  using end_iter_type = std::remove_reference_t<decltype(asio::buffer_sequence_end(std::declval<B&>()))>;

  public:
  using iterator_category = std::bidirectional_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using value_type = std::conditional_t<mb_are_convertible_to_sequence_of_v<asio::mutable_buffer>, asio::mutable_buffer, asio::const_buffer>;
  using reference = value_type;
  using pointer = void;

  explicit concat_buffer_iterator_(std::add_lvalue_reference_t<MB>... mb)
  : begin_(asio::buffer_sequence_begin(mb)...),
    end_(asio::buffer_sequence_end(mb)...),
    cur_(begin_)
  {}

  explicit concat_buffer_iterator_([[maybe_unused]] at_end, std::add_lvalue_reference_t<MB>... mb)
  : begin_(asio::buffer_sequence_begin(mb)...),
    end_(asio::buffer_sequence_end(mb)...),
    cur_(end_)
  {}

  auto operator==(const concat_buffer_iterator_& y) const
      noexcept(
          std::conjunction_v<
              std::bool_constant<noexcept(std::declval<const begin_iter_type<MB>&>() == std::declval<const begin_iter_type<MB>&>())>...,
              std::bool_constant<noexcept(std::declval<const end_iter_type<MB>&>() == std::declval<const end_iter_type<MB>&>())>...>)
  -> bool {
    return begin_ == y.begin_ && end_ == y.end_;
  }

  auto operator!=(const concat_buffer_iterator_& y) const
      noexcept(
          std::conjunction_v<
              std::bool_constant<noexcept(std::declval<const begin_iter_type<MB>&>() == std::declval<const begin_iter_type<MB>&>())>...,
              std::bool_constant<noexcept(std::declval<const end_iter_type<MB>&>() == std::declval<const end_iter_type<MB>&>())>...>)
  -> bool {
    return !(*this == y);
  }

  auto operator*() const -> std::conditional_t<mb_are_convertible_to_sequence_of_v<asio::mutable_buffer>, asio::mutable_buffer, asio::const_buffer> {
    return deref_(std::index_sequence_for<MB...>());
  }

  auto operator++() -> concat_buffer_iterator_& {
    inc_(std::index_sequence_for<MB...>());
    return *this;
  }

  auto operator++(int) -> concat_buffer_iterator_& {
    auto copy = *this;
    ++*this;
    return copy;
  }

  auto operator--() -> concat_buffer_iterator_& {
    dec_(std::index_sequence_for<MB...>());
    return *this;
  }

  auto operator--(int) -> concat_buffer_iterator_& {
    auto copy = *this;
    --*this;
    return copy;
  }

  private:
  template<std::size_t Idx, std::size_t... Tail>
  auto deref_([[maybe_unused]] std::index_sequence<Idx, Tail...>) const -> std::conditional_t<mb_are_convertible_to_sequence_of_v<asio::mutable_buffer>, asio::mutable_buffer, asio::const_buffer> {
    if (std::get<Idx>(cur_) != std::get<Idx>(end_)) return *std::get<Idx>(cur_);

    if constexpr(sizeof...(Tail) == 0) {
      assert(false); // undefined behaviour

      void* nil = nullptr;
      return asio::buffer(nil, 0);
    } else {
      return deref_(std::index_sequence<Tail...>());
    }
  }

  template<std::size_t Idx, std::size_t... Tail>
  void inc_([[maybe_unused]] std::index_sequence<Idx, Tail...>) {
    if (std::get<Idx>(cur_) != std::get<Idx>(end_)) {
      ++std::get<Idx>(cur_);
      return;
    }

    if constexpr(sizeof...(Tail) == 0) {
      assert(false); // undefined behaviour
    } else {
      inc_(std::index_sequence<Tail...>());
    }
  }

  template<std::size_t Idx, std::size_t... Tail>
  void dec_([[maybe_unused]] std::index_sequence<Idx, Tail...>) {
    if (std::get<Idx>(cur_) != std::get<Idx>(begin_)) {
      --std::get<Idx>(cur_);
      return;
    }

    if constexpr(sizeof...(Tail) == 0) {
      assert(false); // undefined behaviour
    } else {
      dec_(std::index_sequence<Tail...>());
    }
  }

  std::tuple<begin_iter_type<MB>...> begin_;
  std::tuple<end_iter_type<MB>...> end_;
  std::tuple<begin_iter_type<MB>...> cur_;
};


template<typename... MB>
class concat_buffer_t {
  private:
  template<typename ToType>
  using mb_are_convertible_to_sequence_of = std::conjunction<
      std::is_convertible<
          std::remove_reference_t<decltype(*asio::buffer_sequence_begin(std::declval<MB&>()))>,
          ToType>...
      >;
  template<typename ToType>
  static constexpr bool mb_are_convertible_to_sequence_of_v = mb_are_convertible_to_sequence_of<ToType>::value;

  static_assert(mb_are_convertible_to_sequence_of_v<asio::mutable_buffer> || mb_are_convertible_to_sequence_of_v<asio::const_buffer>,
      "buffers must be a buffer sequence");

  public:
  explicit concat_buffer_t(MB... mb)
  : mb_(std::forward<MB>(mb)...)
  {}

  using iterator_type = concat_buffer_iterator_<MB...>;
  using const_iterator_type = concat_buffer_iterator_<std::add_const_t<MB>...>;

  auto begin() -> iterator_type {
    return begin_(std::index_sequence_for<MB...>());
  }

  auto end() -> iterator_type {
    return end_(std::index_sequence_for<MB...>());
  }

  auto begin() const -> const_iterator_type {
    return begin_(std::index_sequence_for<MB...>());
  }

  auto end() const -> const_iterator_type {
    return end_(std::index_sequence_for<MB...>());
  }

  auto cbegin() const -> const_iterator_type {
    return begin();
  }

  auto cend() const -> const_iterator_type {
    return end();
  }

  private:
  template<std::size_t... Idx>
  auto begin_([[maybe_unused]] std::index_sequence<Idx...>) -> iterator_type {
    return iterator_type(std::get<Idx>(mb_)...);
  }

  template<std::size_t... Idx>
  auto end_([[maybe_unused]] std::index_sequence<Idx...>) -> iterator_type {
    return iterator_type(typename iterator_type::at_end(), std::get<Idx>(mb_)...);
  }

  template<std::size_t... Idx>
  auto begin_([[maybe_unused]] std::index_sequence<Idx...>) const -> const_iterator_type {
    return const_iterator_type(std::get<Idx>(mb_)...);
  }

  template<std::size_t... Idx>
  auto end_([[maybe_unused]] std::index_sequence<Idx...>) const -> const_iterator_type {
    return const_iterator_type(typename const_iterator_type::at_end(), std::get<Idx>(mb_)...);
  }

  std::tuple<MB...> mb_;
};


template<typename... MB>
auto concat_buffer(MB... mb) -> concat_buffer_t<std::decay_t<MB>...> {
  return concat_buffer_t<std::decay_t<MB>...>(std::forward<MB>(mb)...);
}


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_CONCAT_BUFFER_H */
