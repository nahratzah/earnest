#ifndef EARNEST_DETAIL_TREE_LEAF_ITERATOR_INL_H
#define EARNEST_DETAIL_TREE_LEAF_ITERATOR_INL_H

namespace earnest::detail::tree {


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


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_LEAF_ITERATOR_INL_H */
