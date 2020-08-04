#ifndef EARNEST_DETAIL_UNIQUE_ALLOC_PTR_H
#define EARNEST_DETAIL_UNIQUE_ALLOC_PTR_H

#include <memory>
#include <type_traits>

#if __cplusplus <= 201703L
# include <exception>
#endif

namespace earnest::detail {


template<typename T>
struct _is_unbounded_array_ : std::false_type {};
template<typename T>
struct _is_unbounded_array_<T[]> : std::true_type {};

template<typename T>
using _is_unbounded_array = typename _is_unbounded_array_<T>::type;
template<typename T>
inline constexpr bool _is_unbounded_array_v = _is_unbounded_array<T>::value;


///\brief A deleter for use with std::unique_ptr, that uses an allocator.
///\tparam T The type for which this deleter exists.
///\tparam Alloc The allocator type for this deleter.
template<typename T, typename Alloc>
class deleter_with_alloc {
  public:
  using allocator_type = Alloc;
  using allocator_traits = std::allocator_traits<allocator_type>;

  static_assert(std::is_same_v<typename allocator_traits::value_type, T>,
      "allocator must match element type");

  deleter_with_alloc() noexcept(std::is_nothrow_default_constructible_v<Alloc>) = default;

  explicit deleter_with_alloc(Alloc&& alloc)
      noexcept(std::is_nothrow_move_constructible_v<Alloc>)
  : alloc_(std::move(alloc))
  {}

  explicit deleter_with_alloc(const Alloc& alloc)
      noexcept(std::is_nothrow_copy_constructible_v<Alloc>)
  : alloc_(alloc)
  {}

  void operator()(T* ptr) {
    allocator_traits traits;
    traits.destroy(alloc_, ptr);
    traits.deallocate(alloc_, ptr, 1);
  }

  auto get_allocator() noexcept -> Alloc& { return alloc_; }
  auto get_allocator() const noexcept -> const Alloc& { return alloc_; }

  private:
  Alloc alloc_;
};


///\brief A deleter for use with std::unique_ptr, that uses an allocator.
///\tparam T The type for which this deleter exists.
///\tparam Alloc The allocator type for this deleter.
template<typename T, typename Alloc>
class deleter_with_alloc<T[], Alloc> {
  public:
  using allocator_type = Alloc;
  using allocator_traits = std::allocator_traits<allocator_type>;

  static_assert(std::is_same_v<typename allocator_traits::value_type, T>,
      "allocator must match element type");

  deleter_with_alloc() noexcept(std::is_nothrow_default_constructible_v<Alloc>) = default;

  explicit deleter_with_alloc(Alloc&& alloc, std::size_t n)
      noexcept(std::is_nothrow_move_constructible_v<Alloc>)
  : alloc_(std::move(alloc)),
    n_(n)
  {}

  explicit deleter_with_alloc(const Alloc& alloc)
      noexcept(std::is_nothrow_copy_constructible_v<Alloc>)
  : alloc_(alloc)
  {}

  void operator()(std::remove_extent_t<T>* ptr) {
    allocator_traits traits;
    for (std::size_t i = n_; i > 0u; --i)
      traits.destroy(alloc_, ptr + (i - 1u));
    traits.deallocate(alloc_, ptr, n_);
  }

  auto get_allocator() noexcept -> Alloc& { return alloc_; }
  auto get_allocator() const noexcept -> const Alloc& { return alloc_; }

  private:
  Alloc alloc_;
  std::size_t n_ = 0;
};


///\brief Unique pointer with an allocator.
template<typename T, typename Alloc>
using unique_alloc_ptr = std::unique_ptr<
    T,
    deleter_with_alloc<T, Alloc>>;


///\brief Allocate a unique pointer using the given allocator.
///\param alloc The allocator to allocate and deallocate the object with.
///\param args The arguments passed to the constructor of the type.
///\note The allocator is used to construct the object, so for instance scoped-allocator will propagate itself if possible.
template<typename T, typename Alloc, typename... Args>
auto allocate_unique(Alloc&& alloc, Args&&... args)
-> std::enable_if_t<!_is_unbounded_array_v<T>, unique_alloc_ptr<T, std::decay_t<Alloc>>> {
  using d_type = typename unique_alloc_ptr<T, std::decay_t<Alloc>>::deleter_type;

  d_type d = d_type(std::forward<Alloc>(alloc));
  typename d_type::allocator_traits alloc_traits;

  T* ptr = alloc_traits.allocate(d.get_allocator(), 1);
  try {
    alloc_traits.construct(d.get_allocator(), ptr, std::forward<Args>(args)...);
  } catch (...) {
    // Prevent resource leak.
    try {
      alloc_traits.deallocate(d.get_allocator(), ptr, 1);
    } catch (...) {
#if __cplusplus <=  201402L
      std::unexpected();
#else
      // Swallowing the exception and leaking resource.
#endif
    }
    throw;
  }

  return unique_alloc_ptr<T, std::decay_t<Alloc>>(ptr, std::move(d));
};

template<typename T, typename Alloc>
auto allocate_unique(Alloc&& alloc, std::size_t n)
-> std::enable_if_t<_is_unbounded_array_v<T>, unique_alloc_ptr<T, std::decay_t<Alloc>>> {
  using d_type = typename unique_alloc_ptr<T, std::decay_t<Alloc>>::deleter_type;

  d_type d = d_type(std::forward<Alloc>(alloc), n);
  typename d_type::allocator_traits alloc_traits;

  std::remove_extent_t<T>* ptr = alloc_traits.allocate(d.get_allocator(), n);
  std::size_t i = 0;
  try {
    while (i < n) {
      alloc_traits.construct(d.get_allocator(), ptr + i);
      ++i;
    }
  } catch (...) {
    try {
      while (i > 0) {
        alloc_traits.destroy(d.get_allocator(), ptr + (i - 1u));
        --i;
      }
      alloc_traits.deallocate(d.get_allocator(), ptr, n);
    } catch (...) {
#if __cplusplus <=  201402L
      std::unexpected();
#else
      // Swallowing the exception and leaking resource.
#endif
    }
    throw;
  }

  return unique_alloc_ptr<T, std::decay_t<Alloc>>(ptr, std::move(d));
}


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_UNIQUE_ALLOC_PTR_H */
