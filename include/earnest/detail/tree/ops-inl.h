#ifndef EARNEST_DETAIL_TREE_OPS_INL_H
#define EARNEST_DETAIL_TREE_OPS_INL_H

#include <earnest/detail/tree/branch.h>
#include <earnest/detail/tree/leaf.h>
#include <earnest/detail/tree/loader.h>
#include <earnest/detail/tree/key_type.h>
#include <earnest/detail/tree/augmented_page_ref.h>
#include <earnest/detail/tree/tree.h>
#include <boost/asio/buffer.hpp>
#include <boost/asio/write_at.hpp>
#include <boost/polymorphic_pointer_cast.hpp>
#include <cassert>
#include <memory>

namespace earnest::detail::tree {


template<typename Child>
auto ops::split_child_page_(
    const cycle_ptr::cycle_gptr<basic_tree>& f,
    const unique_lock_ptr<cycle_ptr::cycle_gptr<branch>>& self,
    const unique_lock_ptr<cycle_ptr::cycle_gptr<Child>>& child)
-> unique_lock_ptr<cycle_ptr::cycle_gptr<Child>> {
  assert(branch::size(self) < self->max_size());
  assert(branch::size(self) > 0);
  assert(self->cfg == child->cfg);

  unique_lock_ptr<cycle_ptr::cycle_gptr<Child>> sibling; // Return value.
  auto tx = f->txfile_begin(false);
  [[maybe_unused]] auto dbc_val = f->obj_cache()->get(
      f->loader->allocate_disk_space(tx, child->bytes_per_page_()),
      f,
      [&f, &sibling, &tx, &self, &child, loader=f->loader.get()](db_cache::allocator_type alloc, txfile::transaction::offset_type sibling_offset) -> cycle_ptr::cycle_gptr<Child> {
        // Remember constants.
        const auto bytes_per_augmented_page_ref = self->bytes_per_augmented_page_ref_();
        const auto bytes_per_key = self->bytes_per_key_();

        const auto child_slot = branch::find_slot(self, child->offset).value();

        // Reserve space.
        self->keys_.reserve(self->keys_.size() + 1u);
        self->pages_.reserve(self->pages_.size() + 1u);

        sibling = unique_lock_ptr<cycle_ptr::cycle_gptr<Child>>(abstract_page::allocate_page<Child>(child->cfg, alloc));
        sibling->offset = loader->allocate_disk_space(tx, sibling_offset);

        cycle_ptr::cycle_gptr<const key_type> key = Child::split(f, child, sibling, tx);

        // Compute augmentation for sibling.
        cycle_ptr::cycle_gptr<augmented_page_ref> sibling_pgref = loader->allocate_augmented_page_ref(*sibling, sibling->get_allocator());
        sibling_pgref->offset_ = sibling->offset;
        // Compute updated augmentation for child page.
        cycle_ptr::cycle_gptr<augmented_page_ref> child_pgref = loader->allocate_augmented_page_ref(*sibling, sibling->get_allocator());
        child_pgref->offset_ = child->offset;

        // Write updated size.
        {
          std::uint32_t new_size = self->pages_.size() + 1u;
          boost::endian::native_to_big_inplace(new_size);
          boost::asio::write_at(
              tx,
              self->offset + branch::header::OFFSET_SIZE,
              boost::asio::buffer(&new_size, sizeof(new_size)));
        }

        // Write updated items.
        {
          std::vector<std::uint8_t, shared_resource_allocator<std::uint8_t>> buf_storage(tx.get_allocator());
          buf_storage.resize(
              (self->pages_.size() - child_slot + 1u) * bytes_per_augmented_page_ref +
              (self->keys_.size() - child_slot + 1u) * bytes_per_key);
          const auto buf_offset = self->offset + child_slot * (bytes_per_key + bytes_per_augmented_page_ref);
          {
            boost::asio::mutable_buffer buf = boost::asio::buffer(buf_storage);

            // Write child replacement.
            child_pgref->encode(boost::asio::buffer(buf, bytes_per_augmented_page_ref));
            buf += bytes_per_augmented_page_ref;
            // Write separator key.
            key->encode(boost::asio::buffer(buf, bytes_per_key));
            buf += bytes_per_key;
            // Write new sibling.
            sibling_pgref->encode(boost::asio::buffer(buf, bytes_per_augmented_page_ref));
            buf += bytes_per_augmented_page_ref;

            // Write remaining pages and keys.
            // While these are unchanged, they've all shifted position, and thus
            // need to be written out again.
            auto pages_iter = self->pages_.begin() + child_slot + 1u;
            auto keys_iter = self->keys_.begin() + child_slot;
            for (; pages_iter != self->pages_.end(); ++pages_iter, ++keys_iter) {
              assert(keys_iter != self->keys_.end());

              (*keys_iter)->encode(boost::asio::buffer(buf, bytes_per_key));
              buf += bytes_per_key;
              (*pages_iter)->encode(boost::asio::buffer(buf, bytes_per_augmented_page_ref));
              buf += bytes_per_augmented_page_ref;
            }
            assert(pages_iter == self->pages_.end() && keys_iter == self->keys_.end());
            assert(buf.size() == 0);
          }
          boost::asio::write_at(tx, buf_offset, boost::asio::buffer(buf_storage));
        }

        tx.on_commit(
            [   self_ptr=self.mutex(),
                child_ptr=child.mutex(),
                sibling_ptr=sibling.mutex(),
                sibling_pgref=std::move(sibling_pgref),
                child_pgref=std::move(child_pgref),
                key=std::move(key),
                child_slot
            ]() noexcept {
              // Update augmentation.
              self_ptr->pages_[child_slot] = child_pgref;
              // Insert sibling page.
              self_ptr->pages_.emplace(
                  self_ptr->pages_.begin() + child_slot + 1u,
                  sibling_pgref);
              // Insert key.
              self_ptr->keys_.emplace(
                  self_ptr->keys_.begin() + child_slot,
                  key);
            });

        tx.commit();
        return sibling.mutex();
      });

  assert(dbc_val == sibling.mutex());
  return sibling;
}

template<typename Tree, typename BranchPageSel>
auto ops::begin_end_leaf_(
    const shared_lock_ptr<cycle_ptr::cycle_gptr<const Tree>>& f_lck,
    BranchPageSel&& branch_page_sel)
-> std::enable_if_t<std::is_base_of_v<basic_tree, Tree>, shared_lock_ptr<cycle_ptr::cycle_gptr<const leaf>>> {
  assert(f_lck.owns_lock());
  const basic_tree& f = *f_lck;

  // XXX maybe throw an exception
  assert(f.root_page_ != 0);

  shared_lock_ptr<cycle_ptr::cycle_gptr<const abstract_page>> locked_page(load_page_(f_lck.mutex(), f.root_page_));
  for (;;) {
    const auto locked_branch = dynamic_cast<const branch*>(std::addressof(*locked_page));
    if (locked_branch == nullptr) break; // Must be a leaf.
    locked_page = shared_lock_ptr<cycle_ptr::cycle_gptr<const abstract_page>>(
        load_page_(f_lck.mutex(), branch_page_sel(locked_branch->pages_)->offset()));
  }

  shared_lock_ptr<cycle_ptr::cycle_gptr<const leaf>> locked_leaf(
      boost::polymorphic_pointer_downcast<const leaf>(locked_page.mutex()),
      std::adopt_lock);
  locked_page.release(); // Because adopt-lock above.

  return locked_leaf;
}

template<typename Page>
inline auto ops::load_page(
    const cycle_ptr::cycle_gptr<const basic_tree>& f,
    std::uint64_t offset)
-> std::enable_if_t<std::is_base_of_v<abstract_page, std::remove_const_t<Page>>, cycle_ptr::cycle_gptr<Page>> {
  if constexpr(std::is_same_v<abstract_page, std::remove_const_t<Page>>)
    return load_page_(f, offset);
  else
    return boost::polymorphic_pointer_downcast<Page>(load_page_(f, offset));
}

template<typename Tree>
inline auto ops::begin(const shared_lock_ptr<cycle_ptr::cycle_gptr<const Tree>>& f)
-> std::enable_if_t<std::is_base_of_v<basic_tree, Tree>, leaf_iterator> {
  return leaf::begin(
      f.mutex(),
      begin_end_leaf_(
          f,
          [](const branch::page_ref_vector& pages) -> branch::page_ref_vector::const_reference {
            return pages.front();
          }));
}

template<typename Tree>
inline auto ops::end(const shared_lock_ptr<cycle_ptr::cycle_gptr<const Tree>>& f)
-> std::enable_if_t<std::is_base_of_v<basic_tree, Tree>, leaf_iterator> {
  return leaf::end(
      f.mutex(),
      begin_end_leaf_(
          f,
          [](const branch::page_ref_vector& pages) -> branch::page_ref_vector::const_reference {
            return pages.back();
          }));
}

template<typename Tree>
inline auto ops::rbegin(const shared_lock_ptr<cycle_ptr::cycle_gptr<const Tree>>& f)
-> std::enable_if_t<std::is_base_of_v<basic_tree, Tree>, reverse_leaf_iterator> {
  reverse_leaf_iterator iter(end(f));
  ++iter;
  return iter;
}

template<typename Tree>
inline auto ops::rend(const shared_lock_ptr<cycle_ptr::cycle_gptr<const Tree>>& f)
-> std::enable_if_t<std::is_base_of_v<basic_tree, Tree>, reverse_leaf_iterator> {
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

#endif /* EARNEST_DETAIL_TREE_OPS_INL_H */
