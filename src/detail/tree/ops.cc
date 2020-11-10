#include <earnest/detail/tree/ops.h>
#include <earnest/detail/tree/branch.h>
#include <earnest/detail/tree/leaf.h>
#include <earnest/detail/tree/leaf_iterator.h>
#include <earnest/txfile.h>
#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>
#include <boost/polymorphic_pointer_cast.hpp>
#include <boost/asio/write_at.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/completion_condition.hpp>
#include <memory>
#include <vector>

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

void ops::ensure_root_page(basic_tree& f) {
  using buf_type = std::vector<
      std::uint8_t,
      typename std::allocator_traits<txfile::transaction::allocator_type>::template rebind_alloc<std::uint8_t>>;

  {
    std::shared_lock<basic_tree> lck(f);
    if (f.root_page_ != 0) return;
  }

  std::lock_guard<basic_tree> lck(f);
  if (f.root_page_ != 0) return; // Must re-check, because we released the lock in between.

  auto tx = f.txfile_begin(false);
  const auto offset = f.loader->allocate_disk_space(tx, leaf::bytes_per_page_(*f.cfg));

  buf_type buf(leaf::bytes_per_page_(*f.cfg), std::uint8_t(0), tx.get_allocator());
  leaf::header{ leaf::magic, 0u, 0u, 0u, 0u }.encode(boost::asio::buffer(buf));
  boost::asio::write_at(tx, offset, boost::asio::buffer(buf), boost::asio::transfer_all());

  tx.on_commit(
      [&f, offset]() noexcept {
        f.root_page_ = offset;
      });
  tx.commit();
}


} /* namespace earnest::detail::tree */
