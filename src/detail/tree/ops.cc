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

auto ops::begin(const shared_lock_ptr<cycle_ptr::cycle_gptr<const basic_tree>>& f)
-> leaf_iterator {
  return leaf::begin(
      f.mutex(),
      begin_end_leaf_(
          f,
          [](const branch::page_ref_vector& pages) -> branch::page_ref_vector::const_reference {
            return pages.front();
          }));
}

auto ops::end(const shared_lock_ptr<cycle_ptr::cycle_gptr<const basic_tree>>& f)
-> leaf_iterator {
  return leaf::end(
      f.mutex(),
      begin_end_leaf_(
          f,
          [](const branch::page_ref_vector& pages) -> branch::page_ref_vector::const_reference {
            return pages.back();
          }));
}

auto ops::rbegin(const shared_lock_ptr<cycle_ptr::cycle_gptr<const basic_tree>>& f)
-> reverse_leaf_iterator {
  reverse_leaf_iterator iter(end(f));
  ++iter;
  return iter;
}

auto ops::rend(const shared_lock_ptr<cycle_ptr::cycle_gptr<const basic_tree>>& f)
-> reverse_leaf_iterator {
  return reverse_leaf_iterator(
      leaf::before_begin(
          f.mutex(),
          begin_end_leaf_(
              f,
              [](const branch::page_ref_vector& pages) -> branch::page_ref_vector::const_reference {
                return pages.front();
              })));
}


} /* namespace earnest::detail::tree */
