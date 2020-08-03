#ifndef EARNEST_DETAIL_TXFILE_ALLOCATOR_INL_H
#define EARNEST_DETAIL_TXFILE_ALLOCATOR_INL_H

namespace earnest::detail {


inline auto txfile_allocator::key::operator==(const key& y) const noexcept -> bool {
  return addr == y.addr;
}

inline auto txfile_allocator::key::operator!=(const key& y) const noexcept -> bool {
  return !(*this == y);
}

inline auto txfile_allocator::key::operator<(const key& y) const noexcept -> bool {
  return addr < y.addr;
}

inline auto txfile_allocator::key::operator>(const key& y) const noexcept -> bool {
  return y < *this;
}

inline auto txfile_allocator::key::operator<=(const key& y) const noexcept -> bool {
  return !(*this > y);
}

inline auto txfile_allocator::key::operator>=(const key& y) const noexcept -> bool {
  return !(*this < y);
}


inline txfile_allocator::element::element(
    cycle_ptr::cycle_gptr<tree_page_leaf> parent,
    const class key& key, std::uint64_t used, std::uint64_t free)
: abstract_tree_elem(std::move(parent)),
  key(key),
  used(used),
  free(free)
{}


inline auto txfile_allocator::max_free_space_augment::merge(
    const max_free_space_augment& x, const max_free_space_augment& y) noexcept
-> const max_free_space_augment& {
  return (x.free > y.free ? x : y);
}


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_TXFILE_ALLOCATOR_INL_H */
