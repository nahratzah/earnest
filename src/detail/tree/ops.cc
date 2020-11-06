#include <earnest/detail/tree/ops.h>

namespace earnest::detail::tree {


auto ops::split_child_page(
    const cycle_ptr::cycle_gptr<basic_tree>& f,
    const unique_lock_ptr<cycle_ptr::cycle_gptr<branch>>& self,
    const unique_lock_ptr<cycle_ptr::cycle_gptr<branch>>& child)
-> unique_lock_ptr<cycle_ptr::cycle_gptr<branch>> {
  return split_child_page_(f, self, child);
}

auto ops::split_child_page(
    const cycle_ptr::cycle_gptr<basic_tree>& f,
    const unique_lock_ptr<cycle_ptr::cycle_gptr<branch>>& self,
    const unique_lock_ptr<cycle_ptr::cycle_gptr<leaf>>& child)
-> unique_lock_ptr<cycle_ptr::cycle_gptr<leaf>> {
  return split_child_page_(f, self, child);
}


} /* namespace earnest::detail::tree */
