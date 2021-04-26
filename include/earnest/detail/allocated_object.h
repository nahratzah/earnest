#ifndef EARNEST_DETAIL_ALLOCATED_OBJECT_H
#define EARNEST_DETAIL_ALLOCATED_OBJECT_H

#include <type_traits>
#include <utility>
#include <memory>

namespace earnest::detail {


template<typename T, typename Allocator = std::allocator<T>>
class allocated_object
: private Allocator
{
  static_assert(std::is_same_v<typename std::allocator_traits<Allocator>::value_type, T>,
      "Allocator must be an allocator for T");

  public:
  using allocator_type = Allocator;

  private:
  using alloc_traits = std::allocator_traits<allocator_type>;
  using pointer = typename alloc_traits::pointer;

  public:
  allocated_object() noexcept = default;

  explicit allocated_object(allocator_type alloc)
      noexcept(std::is_nothrow_copy_constructible_v<allocator_type>)
  : allocator_type(std::move(alloc))
  {}

  allocated_object(const allocated_object&) = delete;

  allocated_object(allocated_object&& y) noexcept
  : allocated_object(y.get_allocator())
  {
    ptr_ = std::exchange(y.ptr_, nullptr);
  }

  template<bool Enable = std::is_const_v<T>, typename = std::enable_if_t<Enable>>
  allocated_object(allocated_object<std::remove_const_t<T>>&& y) noexcept
  : allocated_object(y.get_allocator())
  {
    ptr_ = std::exchange(y.ptr_, nullptr);
  }

  ~allocated_object() noexcept {
    if (ptr_ != nullptr) {
      alloc_traits::destroy(get_allocator_(), ptr_);
      alloc_traits::deallocate(get_allocator_(), ptr_, 1);
    }
  }

  auto operator=(allocated_object y) noexcept -> allocated_object& {
    allocated_object copy = std::move(*this); // Handles de-allocation.
    get_allocator_() = y.get_allocator_();
    ptr_ = std::exchange(y.ptr_, nullptr);
    return *this;
  }

  template<typename... Args>
  static auto allocate(allocator_type alloc, Args&&... args) -> allocated_object {
    allocated_object ao(std::move(alloc));
    ao.ptr_ = construct_(ao.get_allocator_(), std::forward<Args>(args)...);
    return ao;
  }

  auto get_allocator() const -> allocator_type {
    return get_allocator_();
  }

  auto get() const -> pointer {
    return ptr_;
  }

  auto operator*() const -> std::add_lvalue_reference_t<typename alloc_traits::value_type> {
    return *ptr_;
  }

  auto operator->() const -> pointer {
    return ptr_;
  }

  private:
  auto get_allocator_() const -> const allocator_type& { return *this; }
  auto get_allocator_() -> allocator_type& { return *this; }

  template<typename... Args>
  static auto construct_(allocator_type& alloc, Args&&... args) {
    pointer ptr = alloc_traits::allocate(alloc, 1);
    try {
      alloc_traits::construct(alloc, ptr, std::forward<Args>(args)...);
    } catch (...) {
      alloc_traits::deallocate(alloc, ptr, 1);
      throw;
    }
    return ptr;
  }

  pointer ptr_ = nullptr;
};


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_ALLOCATED_OBJECT_H */
