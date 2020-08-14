#ifndef EARNEST_DETAIL_TREE_LEAF_ITERATOR_H
#define EARNEST_DETAIL_TREE_LEAF_ITERATOR_H

#include <earnest/detail/export_.h>
#include <earnest/detail/tree/fwd.h>
#include <cycle_ptr/cycle_ptr.h>

namespace earnest::detail::tree {


class earnest_export_ leaf_iterator {
  public:
  auto operator++(int) -> leaf_iterator;
  auto operator--(int) -> leaf_iterator;

  auto operator++() -> leaf_iterator&;
  auto operator--() -> leaf_iterator&;

  auto operator==(const leaf_iterator& y) const noexcept;
  auto operator!=(const leaf_iterator& y) const noexcept;

  auto operator->() const -> const value_type*;
  auto operator*() const -> const value_type&;

  private:
  cycle_ptr::cycle_gptr<const loader> loader_;
  cycle_ptr::cycle_gptr<const value_type> value_ptr_;
};


} /* namespace earnest::detail::tree */

#include "leaf_iterator-inl.h"

#endif /* EARNEST_DETAIL_TREE_LEAF_ITERATOR_H */
