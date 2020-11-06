#include <earnest/detail/tree/ops.h>

namespace earnest::detail::tree {


auto ops::split_child_page(
    const loader& loader,
    const unique_lock_ptr<cycle_ptr::cycle_gptr<branch>>& self,
    const unique_lock_ptr<cycle_ptr::cycle_gptr<branch>>& child,
    txfile& f,
    db_cache& dbc)
-> unique_lock_ptr<cycle_ptr::cycle_gptr<branch>> {
  return split_child_page_(loader, self, child, f, dbc);
}

auto ops::split_child_page(
    const loader& loader,
    const unique_lock_ptr<cycle_ptr::cycle_gptr<branch>>& self,
    const unique_lock_ptr<cycle_ptr::cycle_gptr<leaf>>& child,
    txfile& f,
    db_cache& dbc)
-> unique_lock_ptr<cycle_ptr::cycle_gptr<leaf>> {
  return split_child_page_(loader, self, child, f, dbc);
}


} /* namespace earnest::detail::tree */
