#ifndef EARNEST_DETAIL_TREE_LEAF_ITERATOR_H
#define EARNEST_DETAIL_TREE_LEAF_ITERATOR_H

#include <earnest/detail/export_.h>
#include <earnest/detail/tree/fwd.h>
#include <cycle_ptr/cycle_ptr.h>
#include <iterator>

namespace earnest::detail::tree {


class earnest_export_ leaf_iterator {
  public:
  using iterator_category = std::bidirectional_iterator_tag;
  using difference_type = tree_difference_type;
  using value_type = earnest::detail::tree::value_type;
  using pointer = const value_type*;
  using reference = const value_type&;

  constexpr leaf_iterator() noexcept = default;
  leaf_iterator(cycle_ptr::cycle_gptr<const basic_tree> tree, cycle_ptr::cycle_gptr<const value_type> value_ptr) noexcept;
  explicit leaf_iterator(const reverse_leaf_iterator&) noexcept;
  explicit leaf_iterator(reverse_leaf_iterator&&) noexcept;

  auto operator++(int) -> leaf_iterator;
  auto operator--(int) -> leaf_iterator;

  auto operator++() -> leaf_iterator&;
  auto operator--() -> leaf_iterator&;

  auto operator==(const leaf_iterator& y) const noexcept;
  auto operator!=(const leaf_iterator& y) const noexcept;

  auto operator->() const -> pointer;
  auto operator*() const -> reference;

  auto ptr() const& noexcept -> const cycle_ptr::cycle_gptr<const value_type>&;
  auto ptr() && noexcept -> cycle_ptr::cycle_gptr<const value_type>&&;

  private:
  cycle_ptr::cycle_gptr<const basic_tree> tree_;
  cycle_ptr::cycle_gptr<const value_type> value_ptr_;
};

class reverse_leaf_iterator {
  friend leaf_iterator;

  public:
  using iterator_category = leaf_iterator::iterator_category;
  using difference_type = leaf_iterator::difference_type;
  using value_type = leaf_iterator::value_type;
  using pointer = leaf_iterator::pointer;
  using reference = leaf_iterator::reference;

  constexpr reverse_leaf_iterator() noexcept = default;
  explicit reverse_leaf_iterator(const leaf_iterator&) noexcept;
  explicit reverse_leaf_iterator(leaf_iterator&&) noexcept;

  auto operator++(int) -> reverse_leaf_iterator;
  auto operator--(int) -> reverse_leaf_iterator;

  auto operator++() -> reverse_leaf_iterator&;
  auto operator--() -> reverse_leaf_iterator&;

  auto operator==(const reverse_leaf_iterator& y) const noexcept;
  auto operator!=(const reverse_leaf_iterator& y) const noexcept;

  auto operator->() const -> pointer;
  auto operator*() const -> reference;

  auto ptr() const& noexcept -> const cycle_ptr::cycle_gptr<const value_type>&;
  auto ptr() && noexcept -> cycle_ptr::cycle_gptr<const value_type>&&;

  private:
  leaf_iterator base_;
};


} /* namespace earnest::detail::tree */

#include "leaf_iterator-inl.h"

#endif /* EARNEST_DETAIL_TREE_LEAF_ITERATOR_H */
