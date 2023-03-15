#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <type_traits>

namespace earnest::detail {


class alloc_tracker {
  public:
  auto inc(std::size_t bytes) noexcept -> void {
    bytes_.fetch_add(bytes, std::memory_order_relaxed);
  }

  auto dec(std::size_t bytes) noexcept -> void {
    bytes_.fetch_sub(bytes, std::memory_order_relaxed);
  }

  auto get() const noexcept -> std::size_t {
    return bytes_.load(std::memory_order_relaxed);
  }

  private:
  std::atomic<std::size_t> bytes_{0u};
};


template<typename Alloc = std::allocator<std::byte>>
class tracking_allocator
: public Alloc
{
  template<typename> friend class tracking_allocator;

  public:
  using propagate_on_container_copy_assignment = std::true_type;
  using propagate_on_container_move_assignment = std::true_type;
  using propagate_on_container_swap = std::true_type;
  using is_always_equal = std::false_type;
  using size_type = typename std::allocator_traits<Alloc>::size_type;

  template<typename T>
  struct rebind {
    using other = tracking_allocator<typename std::allocator_traits<Alloc>::template rebind_alloc<T>>;
  };

  tracking_allocator(std::shared_ptr<alloc_tracker> t, Alloc alloc = Alloc()) noexcept(std::is_nothrow_move_constructible_v<Alloc>)
  : Alloc(std::move(alloc)),
    t_(std::move(t))
  {}

  tracking_allocator(const tracking_allocator&) noexcept(std::is_nothrow_copy_constructible_v<Alloc>) = default;
  tracking_allocator(tracking_allocator&&) noexcept(std::is_nothrow_move_constructible_v<Alloc>) = default;
  tracking_allocator& operator=(const tracking_allocator&) noexcept(std::is_nothrow_copy_assignable_v<Alloc>) = default;
  tracking_allocator& operator=(tracking_allocator&&) noexcept(std::is_nothrow_move_assignable_v<Alloc>) = default;

  template<typename OtherAlloc>
  tracking_allocator(const tracking_allocator<OtherAlloc>& y, [[maybe_unused]] std::enable_if_t<std::is_constructible_v<Alloc, const OtherAlloc&>, int> = 0) noexcept(std::is_nothrow_constructible_v<Alloc, const OtherAlloc&>)
  : Alloc(y),
    t_(y.t_)
  {}

  auto allocate(size_type n) -> typename std::allocator_traits<Alloc>::pointer {
    typename std::allocator_traits<Alloc>::pointer ptr = std::allocator_traits<Alloc>::allocate(*this, n);
    if (t_ != nullptr)
      t_->inc(n * sizeof(typename std::allocator_traits<Alloc>::value_type));
    return ptr;
  }

#if __cpp_lib_allocate_at_least >= 202106L
  auto allocate_at_least(size_type n) -> std::allocation_result<typename std::allocator_traits<Alloc>::pointer, typename std::allocator_traits<Alloc>::size_type> {
    auto alloc_result = std::allocator_traits<Alloc>::allocate_at_least(*this, n);
    if (t_ != nullptr)
      t_->inc(alloc_result.count * sizeof(typename std::allocator_traits<Alloc>::value_type));
    return alloc_result;
  }
#endif

  auto deallocate(typename std::allocator_traits<Alloc>::pointer ptr, size_type n) -> void {
    std::allocator_traits<Alloc>::deallocate(*this, ptr, n);
    if (t_ != nullptr)
      t_->dec(n * sizeof(typename std::allocator_traits<Alloc>::value_type));
  }

  template<typename OtherAlloc>
  auto operator==(const tracking_allocator<OtherAlloc>& y) const -> bool {
    if constexpr(!std::allocator_traits<Alloc>::is_always_equal::value) {
      const Alloc& x_alloc = *this;
      const Alloc& y_alloc = y;
      if (x_alloc != y_alloc) return false;
    }
    return t_ == y.t_;
  }

  template<typename OtherAlloc>
  auto operator!=(const tracking_allocator<OtherAlloc>& y) const -> bool {
    return !(*this == y);
  }

  auto select_on_container_copy_construction() -> tracking_allocator {
    return tracking_allocator(t_, std::allocator_traits<Alloc>::select_on_container_copy_construction(*this));
  }

#if __cpp_lib_allocate_at_least >= 202106L
  auto allocate_at_least(size_type n) -> std::allocation_result<typename std::allocator_traits<Alloc>::pointer, size_type> {
    std::allocation_result<typename std::allocator_traits<Alloc>::pointer, size_type> ar = std::allocator_traits<Alloc>::allocate_at_least(*this, n);
    if (t_ != nullptr)
      t_->inc(ar.count * sizeof(typename std::allocator_traits<Alloc>::value_type));
    return ar;
  }
#endif

  auto get_tracker() const noexcept -> std::shared_ptr<alloc_tracker> {
    return t_;
  }

  private:
  std::shared_ptr<alloc_tracker> t_;
};


} /* namespace earnest::detail */
