#ifndef EARNEST_DETAIL_TREE_LEAF_ITERATOR_INL_H
#define EARNEST_DETAIL_TREE_LEAF_ITERATOR_INL_H

namespace earnest::detail::tree {


inline leaf_iterator::leaf_iterator(cycle_ptr::cycle_gptr<const basic_tree> tree, cycle_ptr::cycle_gptr<const value_type> value_ptr) noexcept
: tree_(std::move(tree)),
  value_ptr_(std::move(value_ptr))
{}

inline leaf_iterator::leaf_iterator(const reverse_leaf_iterator& reverse) noexcept
: leaf_iterator(reverse.base_)
{}

inline leaf_iterator::leaf_iterator(reverse_leaf_iterator&& reverse) noexcept
: leaf_iterator(std::move(reverse.base_))
{}

inline auto leaf_iterator::operator++(int) -> leaf_iterator {
  leaf_iterator copy = *this;
  ++*this;
  return copy;
}

inline auto leaf_iterator::operator--(int) -> leaf_iterator {
  leaf_iterator copy = *this;
  --*this;
  return copy;
}

inline auto leaf_iterator::operator==(const leaf_iterator& y) const noexcept {
  return value_ptr_ == y.value_ptr_;
}

inline auto leaf_iterator::operator!=(const leaf_iterator& y) const noexcept {
  return !(*this == y);
}

inline auto leaf_iterator::operator->() const -> const value_type* {
  return value_ptr_.operator->();
}

inline auto leaf_iterator::operator*() const -> const value_type& {
  return value_ptr_.operator*();
}

inline auto leaf_iterator::ptr() const& noexcept -> const cycle_ptr::cycle_gptr<const value_type>& {
  return value_ptr_;
}

inline auto leaf_iterator::ptr() && noexcept -> cycle_ptr::cycle_gptr<const value_type>&& {
  return std::move(value_ptr_);
}


inline reverse_leaf_iterator::reverse_leaf_iterator(const leaf_iterator& base) noexcept
: base_(base)
{}

inline reverse_leaf_iterator::reverse_leaf_iterator(leaf_iterator&& base) noexcept
: base_(std::move(base))
{}

inline auto reverse_leaf_iterator::operator++(int) -> reverse_leaf_iterator {
  reverse_leaf_iterator copy = *this;
  ++*this;
  return copy;
}

inline auto reverse_leaf_iterator::operator--(int) -> reverse_leaf_iterator {
  reverse_leaf_iterator copy = *this;
  --*this;
  return copy;
}

inline auto reverse_leaf_iterator::operator++() -> reverse_leaf_iterator& {
  --base_;
  return *this;
}

inline auto reverse_leaf_iterator::operator--() -> reverse_leaf_iterator& {
  ++base_;
  return *this;
}

inline auto reverse_leaf_iterator::operator==(const reverse_leaf_iterator& y) const noexcept {
  return base_ == y.base_;
}

inline auto reverse_leaf_iterator::operator!=(const reverse_leaf_iterator& y) const noexcept {
  return base_ != y.base_;
}

inline auto reverse_leaf_iterator::operator->() const -> const value_type* {
  return base_.operator->();
}

inline auto reverse_leaf_iterator::operator*() const -> const value_type& {
  return base_.operator*();
}

inline auto reverse_leaf_iterator::ptr() const& noexcept -> const cycle_ptr::cycle_gptr<const value_type>& {
  return base_.ptr();
}

inline auto reverse_leaf_iterator::ptr() && noexcept -> cycle_ptr::cycle_gptr<const value_type>&& {
  return std::move(base_).ptr();
}


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_LEAF_ITERATOR_INL_H */
