#include <earnest/detail/tree/ops.h>
#include <earnest/detail/tree/branch.h>
#include <earnest/detail/tree/leaf.h>
#include <earnest/detail/tree/leaf_iterator.h>
#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>
#include <boost/polymorphic_pointer_cast.hpp>

namespace earnest::detail::tree {


auto ops::load_page_(
    const cycle_ptr::cycle_gptr<const basic_tree>& f,
    std::uint64_t offset)
-> cycle_ptr::cycle_gptr<abstract_page>
{
  return boost::polymorphic_pointer_downcast<abstract_page>(
      f->obj_cache()->get(
          offset,
          f,
          [&f](db_cache::allocator_type alloc, txfile::transaction::offset_type offset) -> cycle_ptr::cycle_gptr<abstract_page> {
            auto tx = f->txfile_begin();

            boost::system::error_code ec;
            auto page_ptr = abstract_page::decode(*f->loader, f->cfg, tx, offset, std::move(alloc), ec);
            if (ec) throw boost::system::error_code(ec);
            tx.commit();
            return page_ptr;
          }));
}

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
