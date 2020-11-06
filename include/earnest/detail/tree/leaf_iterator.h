#ifndef EARNEST_DETAIL_TREE_LEAF_ITERATOR_H
#define EARNEST_DETAIL_TREE_LEAF_ITERATOR_H

#include <earnest/detail/export_.h>
#include <earnest/detail/tree/fwd.h>
#include <cycle_ptr/cycle_ptr.h>

namespace earnest::detail::tree {


class earnest_export_ leaf_iterator {
  public:
  leaf_iterator() = default;
  leaf_iterator(cycle_ptr::cycle_gptr<const loader> loader, cycle_ptr::cycle_gptr<const value_type> value_ptr) noexcept;
  explicit leaf_iterator(const reverse_leaf_iterator&) noexcept;
  explicit leaf_iterator(reverse_leaf_iterator&&) noexcept;

  auto operator++(int) -> leaf_iterator;
  auto operator--(int) -> leaf_iterator;

  auto operator++() -> leaf_iterator&;
  auto operator--() -> leaf_iterator&;

  auto operator==(const leaf_iterator& y) const noexcept;
  auto operator!=(const leaf_iterator& y) const noexcept;

  auto operator->() const -> const value_type*;
  auto operator*() const -> const value_type&;

  auto ptr() const& noexcept -> const cycle_ptr::cycle_gptr<const value_type>&;
  auto ptr() && noexcept -> cycle_ptr::cycle_gptr<const value_type>&&;

  private:
  cycle_ptr::cycle_gptr<const loader> loader_;
  cycle_ptr::cycle_gptr<const value_type> value_ptr_;
};

class reverse_leaf_iterator {
  friend leaf_iterator;

  public:
  reverse_leaf_iterator() = default;
  reverse_leaf_iterator(cycle_ptr::cycle_gptr<const loader> loader, cycle_ptr::cycle_gptr<const value_type> value_ptr) noexcept;
  explicit reverse_leaf_iterator(const leaf_iterator&) noexcept;
  explicit reverse_leaf_iterator(leaf_iterator&&) noexcept;

  auto operator++(int) -> reverse_leaf_iterator;
  auto operator--(int) -> reverse_leaf_iterator;

  auto operator++() -> reverse_leaf_iterator&;
  auto operator--() -> reverse_leaf_iterator&;

  auto operator==(const reverse_leaf_iterator& y) const noexcept;
  auto operator!=(const reverse_leaf_iterator& y) const noexcept;

  auto operator->() const -> const value_type*;
  auto operator*() const -> const value_type&;

  auto ptr() const& noexcept -> const cycle_ptr::cycle_gptr<const value_type>&;
  auto ptr() && noexcept -> cycle_ptr::cycle_gptr<const value_type>&&;

  private:
  leaf_iterator base_;
};


} /* namespace earnest::detail::tree */

#include "leaf_iterator-inl.h"

#endif /* EARNEST_DETAIL_TREE_LEAF_ITERATOR_H */
