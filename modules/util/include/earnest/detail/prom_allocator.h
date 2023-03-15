#pragma once

#include <memory>
#include <string_view>
#include <type_traits>
#include <utility>

#include <prometheus/registry.h>
#include <prometheus/gauge.h>

namespace earnest::detail {


template<typename Allocator>
class prom_allocator
: public Allocator
{
  template<typename> friend class prom_allocator;

  private:
  using alloc_traits = std::allocator_traits<Allocator>;

  public:
  using propagate_on_container_copy_assignment = std::true_type;
  using propagate_on_container_move_assignment = std::true_type;
  using propagate_on_container_swap = std::true_type;
  using is_always_equal = std::false_type;

  template<typename T>
  struct rebind_alloc {
    using type = prom_allocator<typename alloc_traits::template rebind_alloc<T>>;
  };

  prom_allocator() noexcept(std::is_nothrow_default_constructible_v<Allocator>) = default;

  template<typename... Args>
  prom_allocator(std::shared_ptr<prometheus::Registry> prom_registry, std::string_view prom_metric_name, const prometheus::Labels& labels, Args&&... args)
  : Allocator(std::forward<Args>(args)...),
    byte_size_(build_byte_size_gauge(prom_registry, prom_metric_name, labels))
  {}

  template<typename OtherAllocator, std::enable_if_t<std::is_constructible_v<Allocator, OtherAllocator>, int> = 0>
  prom_allocator(const prom_allocator<OtherAllocator>& other)
  : Allocator(other),
    byte_size_(other.byte_size_)
  {}

  auto allocate(typename alloc_traits::size_type n) -> typename alloc_traits::pointer {
    typename alloc_traits::pointer p = alloc_traits::allocate(*this, n);
    if (byte_size_ != nullptr) byte_size_->Increment(n * sizeof(typename alloc_traits::value_type));
    return p;
  }

  auto allocate(typename alloc_traits::size_type n, typename alloc_traits::const_void_pointer hint) -> typename alloc_traits::pointer {
    typename alloc_traits::pointer p = alloc_traits::allocate(*this, n, hint);
    if (byte_size_ != nullptr) byte_size_->Increment(n * sizeof(typename alloc_traits::value_type));
    return p;
  }

#if __cpp_lib_allocate_at_least >= 202106L
  auto allocate_at_least(typename alloc_traits::size_type n) -> std::allocation_result<typename alloc_traits::pointer, typename alloc_traits::size_type> {
    auto alloc_result = alloc_traits::allocate_at_least(*this, n);
    if (byte_size_ != nullptr) byte_size_->Increment(alloc_result.count * sizeof(typename alloc_traits::value_type));
    return alloc_result;
  }
#endif

  auto deallocate(typename alloc_traits::pointer p, typename alloc_traits::size_type n) -> void {
    alloc_traits::deallocate(*this, p, n);
    if (byte_size_ != nullptr) byte_size_->Decrement(n * sizeof(typename alloc_traits::value_type));
  }

  auto operator==(const prom_allocator& other) const noexcept(noexcept(std::declval<const Allocator&>() == std::declval<const Allocator&>())) -> bool {
    const Allocator& my_parent = *this;
    const Allocator& other_parent = other;
    return byte_size_ == other.byte_size_ && my_parent == other_parent;
  }

  auto operator!=(const prom_allocator& other) const noexcept(noexcept(std::declval<const Allocator&>() != std::declval<const Allocator&>())) -> bool {
    const Allocator& my_parent = *this;
    const Allocator& other_parent = other;
    return byte_size_ != other.byte_size_ || my_parent != other_parent;
  }

  private:
  static auto build_byte_size_gauge(std::shared_ptr<prometheus::Registry> prom_registry, std::string_view prom_metric_name, const prometheus::Labels& labels) -> std::shared_ptr<prometheus::Gauge> {
    if (prom_registry == nullptr) return nullptr;

    return std::shared_ptr<prometheus::Gauge>(
        prom_registry,
        &prometheus::BuildGauge()
            .Name(std::string(prom_metric_name) + "_bytes")
            .Help("memory use in bytes")
            .Register(*prom_registry)
            .Add(labels));
  }

  std::shared_ptr<prometheus::Gauge> byte_size_;
};


} /* namespace earnest::detail */
