#pragma once

#include <cassert>
#include <concepts>
#include <cstddef>
#include <format>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <ranges>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>

#include <spdlog/fmt/fmt.h>

namespace earnest {


// A fixed-vector is a vector of elements,
// but unlike a regular vector, it does not allow for adding/remove elements.
template<typename T, typename Alloc = std::allocator<T>>
requires std::same_as<T, typename std::allocator_traits<Alloc>::value_type>
class fixed_vector {
  public:
  using allocator_type = Alloc;

  private:
  using alloc_traits = std::allocator_traits<allocator_type>;

  public:
  using size_type = typename alloc_traits::size_type;
  using difference_type = typename alloc_traits::difference_type;
  using value_type = T;
  using reference = T&;
  using const_reference = const T&;
  using pointer = alloc_traits::pointer;
  using const_pointer = alloc_traits::const_pointer;
  using iterator = pointer;
  using const_iterator = const_pointer;
  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  private:
  // We declare our own deleter, for raw-storage.
  // This allows us to use std::unique_ptr.
  struct storage_deleter {
    public:
    explicit storage_deleter(allocator_type alloc, size_type sz)
    : alloc(alloc)
    {}

    auto operator()(T* items) -> void {
      alloc_traits::deallocate(alloc, items, sz);
    }

    allocator_type alloc;
    size_type sz;
  };

  static auto allocate_storage(allocator_type alloc, size_type n) -> std::unique_ptr<T, storage_deleter> {
    std::unique_ptr<T, storage_deleter> ptr(nullptr, storage_deleter(alloc, n));
    if (n != 0) ptr.reset(alloc_traits::allocate(alloc, n));
    return ptr;
  }

  static auto destroy_n(std::unique_ptr<T, storage_deleter> raw_storage, size_type n) noexcept -> void {
    while (n > 0) {
      --n;
      alloc_traits::destroy(raw_storage.get_deleter().alloc, raw_storage.get() + n);
    }
  }

  template<typename Iter>
  static auto fill(Iter iter, std::unique_ptr<T, storage_deleter> raw_storage) -> std::tuple<T*, size_type> {
    size_type done = 0;
    while (done != raw_storage.get_deleter().sz) {
      try {
        alloc_traits::construct(raw_storage.get_deleter().alloc, raw_storage.get() + done, *iter);
      } catch (...) {
        destroy_n(std::move(raw_storage), done);
        throw;
      }

      ++done;
      ++iter;
    }

    return std::make_tuple(raw_storage.release(), raw_storage.get_deleter().sz);
  }

  template<std::ranges::sized_range Range>
  static auto copy(Range&& r, allocator_type alloc) -> std::tuple<T*, size_type> {
    return fill(std::ranges::cbegin(r), allocate_storage(alloc, std::ranges::size(r)));
  }

  template<std::ranges::sized_range Range>
  static auto move(Range&& r, allocator_type alloc) -> std::tuple<T*, size_type> {
    return fill(std::make_move_iterator(std::ranges::begin(r)), allocate_storage(alloc, std::ranges::size(r)));
  }

  auto generate(size_type n, const_reference value) -> std::tuple<T*, size_type> {
    auto raw_storage = allocate_storage(alloc, n);
    size_type done = 0;
    while (done < n) {
      try {
        alloc_traits::construct(raw_storage.get_deleter().alloc, raw_storage.get() + done, value);
      } catch (...) {
        destroy_n(std::move(raw_storage), done);
        throw;
      }

      ++done;
    }
  }

  public:
  constexpr fixed_vector() noexcept(std::is_nothrow_default_constructible_v<allocator_type>) = default;

  constexpr fixed_vector(allocator_type alloc) noexcept(std::is_nothrow_copy_constructible_v<allocator_type>)
  : alloc(alloc)
  {}

  fixed_vector(const fixed_vector& y)
  : fixed_vector(y.alloc)
  {
    std::tie(items, sz) = copy(y, this->alloc);
  }

  fixed_vector(fixed_vector&& y) noexcept(std::is_nothrow_move_constructible_v<allocator_type> || std::is_nothrow_copy_constructible_v<allocator_type>)
  : alloc(std::move_if_noexcept(y.alloc)),
    items(std::exchange(y.items, nullptr)),
    sz(std::exchange(y.sz, 0u))
  {}

  fixed_vector(const fixed_vector& y, allocator_type alloc)
  : fixed_vector(alloc)
  {
    std::tie(items, sz) = copy(y, this->alloc);
  }

  fixed_vector(fixed_vector&& y, allocator_type alloc) noexcept(alloc_traits::is_always_equal::value)
  : fixed_vector(alloc)
  {
    if constexpr(alloc_traits::is_always_equal::value) {
      items = std::exchange(y.items, nullptr);
      sz = std::exchange(y.sz, 0u);
    } else if (this->alloc == y.alloc) {
      items = std::exchange(y.items, nullptr);
      sz = std::exchange(y.sz, 0u);
    } else {
      std::tie(items, sz) = move(std::move(y), this->alloc);
    }
  }

  fixed_vector(std::initializer_list<T> list, allocator_type alloc = allocator_type{})
  : fixed_vector(alloc)
  {
    std::tie(items, sz) = move(std::move(list), this->alloc);
  }

  explicit fixed_vector(size_type count, const_reference value, allocator_type alloc = allocator_type{})
  : fixed_vector(alloc)
  {
    std::tie(items, sz) = generate(count, value);
  }

  explicit fixed_vector(size_type count, allocator_type alloc = allocator_type{})
  : fixed_vector(count, value_type{}, alloc)
  {}

  template<std::input_iterator Iter>
  requires requires(const Iter& b, const Iter& e) {
    { e - b } -> std::convertible_to<size_type>;
  }
  fixed_vector(Iter b, Iter e, allocator_type alloc = allocator_type{})
  : fixed_vector(alloc)
  {
    auto len = e - b;
    assert(len >= 0);
    std::tie(items, sz) = fill(b, allocate_storage(this->alloc, size_type(len)));
  }

  auto operator=(fixed_vector&& y)
      noexcept(alloc_traits::is_always_equal::value || (alloc_traits::propagate_on_container_move_assignment::value && std::is_nothrow_move_constructible_v<allocator_type>))
  -> fixed_vector& {
    if (this == &y) return *this;

    clear();

    if constexpr(alloc_traits::propagate_on_container_move_assignment::value) this->alloc = alloc;
    if constexpr(alloc_traits::is_always_equal::value || alloc_traits::propagate_on_container_move_assignment::value) {
      items = std::exchange(y.items, nullptr);
      sz = std::exchange(y.sz, 0u);
    } else if (this->alloc == y.alloc) {
      items = std::exchange(y.items, nullptr);
      sz = std::exchange(y.sz, 0u);
    } else {
      std::tie(items, sz) = move(y, this->alloc);
    }

    return *this;
  }

  auto operator=(const fixed_vector& y)
  -> fixed_vector& {
    if (this == &y) return *this;

    clear();

    if constexpr(alloc_traits::propagate_on_container_copy_assignment::value) this->alloc = y.alloc;
    std::tie(items, sz) = copy(y, this->alloc);

    return *this;
  }

  ~fixed_vector() noexcept {
    clear();
  }

  auto get_allocator() const -> allocator_type { return alloc; }
  constexpr auto empty() const noexcept -> bool { return sz != 0; }
  constexpr auto size() const noexcept -> size_type { return sz; }
  constexpr auto data() noexcept -> T* { return items; }
  constexpr auto data() const noexcept -> const T* { return items; }
  constexpr auto cdata() const noexcept -> const T* { return items; }

  auto front() -> reference {
    assert(!empty());
    return (*this)[0];
  }

  auto front() const -> const_reference {
    assert(!empty());
    return (*this)[0];
  }

  auto back() -> reference {
    assert(!empty());
    return (*this)[sz - 1u];
  }

  auto back() const -> const_reference {
    assert(!empty());
    return (*this)[sz - 1u];
  }

  auto operator[](size_type pos) -> reference {
    assert(pos < sz);
    return items[pos];
  }

  auto operator[](size_type pos) const -> const_reference {
    assert(pos < sz);
    return items[pos];
  }

  auto at(size_type pos) -> reference {
    range_check(pos);
    return items[pos];
  }

  auto at(size_type pos) const -> const_reference {
    range_check(pos);
    return items[pos];
  }

  auto begin() -> iterator {
    return items;
  }

  auto end() -> iterator {
    return items + sz;
  }

  auto begin() const -> const_iterator {
    return items;
  }

  auto end() const -> const_iterator {
    return items + sz;
  }

  auto cbegin() const -> const_iterator {
    return items;
  }

  auto cend() const -> const_iterator {
    return items + sz;
  }

  auto rbegin() -> reverse_iterator {
    return reverse_iterator(end());
  }

  auto rend() -> reverse_iterator {
    return reverse_iterator(begin());
  }

  auto rbegin() const -> const_reverse_iterator {
    return const_reverse_iterator(end());
  }

  auto rend() const -> const_reverse_iterator {
    return const_reverse_iterator(begin());
  }

  auto crbegin() const -> const_reverse_iterator {
    return const_reverse_iterator(cend());
  }

  auto crend() const -> const_reverse_iterator {
    return const_reverse_iterator(cbegin());
  }

  auto clear() noexcept {
    if (items != nullptr) {
      std::unique_ptr<T, storage_deleter> ptr(nullptr, storage_deleter(alloc, sz));
      ptr.reset(items);
      destroy_n(std::move(ptr), sz);
      items = nullptr;
      sz = 0;
    }
  }

  // Assign a specific range of memory.
  // Must be deletable with this->get_allocator().
  auto assign_raw(pointer elems, size_type sz) noexcept -> void {
    clear();
    this->items = items;
    this->sz = sz;
  }

  auto assign(size_type count, const_reference value) -> void {
    using std::swap;

    fixed_vector tmp(count, value, alloc);
    swap(items, tmp.items);
    swap(sz, tmp.sz);
  }

  template<std::input_iterator Iter>
  requires requires(const Iter& b, const Iter& e) {
    { e - b } -> std::convertible_to<size_type>;
  }
  auto assign(Iter b, Iter e) -> void {
    using std::swap;

    fixed_vector tmp(std::move(b), std::move(e), alloc);
    swap(items, tmp.items);
    swap(sz, tmp.sz);
  }

  auto assign(std::initializer_list<value_type> list) -> void {
    using std::swap;

    fixed_vector tmp(std::move(list), alloc);
    swap(items, tmp.items);
    swap(sz, tmp.sz);
  }

  template<std::ranges::sized_range Range>
  auto assign_range(Range&& range) -> void {
    using std::swap;

    fixed_vector tmp(alloc);
    std::tie(tmp.items, tmp.sz) = copy(std::forward<Range>(range), alloc);
    swap(items, tmp.items);
    swap(sz, tmp.sz);
  }

  template<typename... Args>
  auto assign_emplace_n(size_type n, Args&... args) -> void {
    using std::swap;

    auto inplace_construct = [&]() {
      auto raw_storage = allocate_storage(alloc, n);
      size_type done = 0;
      while (done < n) {
        try {
          alloc_traits::construct(raw_storage.get_deleter().alloc, raw_storage.get() + done, args...);
        } catch (...) {
          destroy_n(std::move(raw_storage), done);
          throw;
        }

        ++done;
      }

      return std::make_tuple(raw_storage.release(), raw_storage.get_deleter().sz);
    };

    fixed_vector tmp(alloc);
    std::tie(tmp.items, tmp.sz) = inplace_construct();
    swap(items, tmp.items);
    swap(sz, tmp.sz);
  }

  auto swap(fixed_vector& y)
  noexcept(
      (alloc_traits::propagate_on_container_swap::value && std::is_nothrow_swappable_v<allocator_type>) ||
      (!alloc_traits::propagate_on_container_swap::value && alloc_traits::is_always_equal::value))
  -> void {
    using std::swap;

    if constexpr(alloc_traits::propagate_on_container_swap::value) {
      swap(alloc, y.alloc); // may throw
      swap(items, y.items);
      swap(sz, y.sz);
    } else if constexpr(alloc_traits::is_always_equal::value) {
      swap(items, y.items);
      swap(sz, y.sz);
    } else if (alloc == y.alloc) {
      swap(items, y.items);
      swap(sz, y.sz);
    } else {
      fixed_vector tmp_this(alloc); // May throw.
      fixed_vector tmp_y(y.alloc); // May throw.

      auto tmp_this_storage = allocate_storage(tmp_this.alloc, y.sz); // May throw.
      auto tmp_y_storage = allocate_storage(tmp_y.alloc, sz); // May throw.

      if constexpr(std::is_nothrow_move_constructible_v<value_type> || !std::is_copy_constructible_v<value_type>) {
        // The fill operations won't throw if `std::is_nothrow_move_constructible_v<value_type>`.
        std::tie(tmp_this.items, tmp_this.sz) = fill(std::make_move_iterator(y.begin()), std::move(tmp_this_storage));
        std::tie(tmp_y.items, tmp_y.sz) = fill(std::make_move_iterator(this->begin()), std::move(tmp_y_storage));
      } else {
        // Copy operations can throw.
        std::tie(tmp_this.items, tmp_this.sz) = copy(y.cbegin(), tmp_this.alloc);
        std::tie(tmp_y.items, tmp_y.sz) = copy(this->cbegin(), tmp_this.alloc);
      }

      // never throw past this point
      swap(tmp_this.items, this->items);
      swap(tmp_this.sz, this->sz);

      swap(tmp_y.items, tmp_y.items);
      swap(tmp_y.sz, tmp_y.sz);
    }
  }

  private:
  auto range_check(size_type pos) const -> void {
    if (pos >= sz) throw std::out_of_range(fmt::format("fixed_vector range check: {} > this->size() (which is {})", pos, sz));
  }

  [[no_unique_address]] allocator_type alloc;
  [[no_unique_address]] T* items = nullptr;
  [[no_unique_address]] size_type sz = 0;
};

template<typename T>
inline auto swap(fixed_vector<T>& x, fixed_vector<T>& y)
noexcept(noexcept(std::declval<fixed_vector<T>&>().swap(std::declval<fixed_vector<T>&>())))
-> void {
  x.swap(y);
}


} /* namespace earnest */
