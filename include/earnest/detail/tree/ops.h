#ifndef EARNEST_DETAIL_TREE_OPS_H
#define EARNEST_DETAIL_TREE_OPS_H

#include <earnest/detail/export_.h>
#include <earnest/detail/tree/fwd.h>
#include <earnest/detail/db_cache.h>
#include <earnest/detail/locked_ptr.h>
#include <earnest/txfile.h>
#include <cycle_ptr/cycle_ptr.h>

namespace earnest::detail::tree {


struct ops {
  private:
  template<typename Child>
  static auto split_child_page_(
      const cycle_ptr::cycle_gptr<basic_tree>& f,
      const unique_lock_ptr<cycle_ptr::cycle_gptr<branch>>& self,
      const unique_lock_ptr<cycle_ptr::cycle_gptr<Child>>& child)
  -> unique_lock_ptr<cycle_ptr::cycle_gptr<Child>>;

  public:
  earnest_export_ static auto split_child_page(
      const cycle_ptr::cycle_gptr<basic_tree>& f,
      const unique_lock_ptr<cycle_ptr::cycle_gptr<branch>>& self,
      const unique_lock_ptr<cycle_ptr::cycle_gptr<branch>>& child)
  -> unique_lock_ptr<cycle_ptr::cycle_gptr<branch>>;

  earnest_export_ static auto split_child_page(
      const cycle_ptr::cycle_gptr<basic_tree>& f,
      const unique_lock_ptr<cycle_ptr::cycle_gptr<branch>>& self,
      const unique_lock_ptr<cycle_ptr::cycle_gptr<leaf>>& child)
  -> unique_lock_ptr<cycle_ptr::cycle_gptr<leaf>>;
};


} /* namespace earnest::detail::tree */

#include "ops-inl.h"

#endif /* EARNEST_DETAIL_TREE_OPS_H */
