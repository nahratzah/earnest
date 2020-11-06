#ifndef EARNEST_DETAIL_TREE_OPS_INL_H
#define EARNEST_DETAIL_TREE_OPS_INL_H

#include <earnest/detail/tree/branch.h>
#include <earnest/detail/tree/leaf.h>
#include <earnest/detail/tree/loader.h>
#include <earnest/detail/tree/key_type.h>
#include <earnest/detail/tree/augmented_page_ref.h>
#include <boost/asio/buffer.hpp>
#include <boost/asio/write_at.hpp>
#include <boost/polymorphic_pointer_cast.hpp>
#include <cassert>

namespace earnest::detail::tree {


template<typename Child>
auto ops::split_child_page_(
    const loader& loader,
    const unique_lock_ptr<cycle_ptr::cycle_gptr<branch>>& self,
    const unique_lock_ptr<cycle_ptr::cycle_gptr<Child>>& child,
    txfile& f,
    db_cache& dbc)
-> unique_lock_ptr<cycle_ptr::cycle_gptr<Child>> {
  assert(branch::size(self) < self->max_size());
  assert(branch::size(self) > 0);
  assert(self->cfg == child->cfg);

  unique_lock_ptr<cycle_ptr::cycle_gptr<Child>> sibling; // Return value.
  auto tx = f.begin(false);
  [[maybe_unused]] auto dbc_val = dbc.get(
      loader.allocate_disk_space(tx, child->bytes_per_page_()),
      nullptr,
      [&sibling, &tx, &self, &child, &loader](db_cache::allocator_type alloc, txfile::transaction::offset_type sibling_offset) -> cycle_ptr::cycle_gptr<Child> {
        // Remember constants.
        const auto bytes_per_augmented_page_ref = self->bytes_per_augmented_page_ref_();
        const auto bytes_per_key = self->bytes_per_key_();

        const auto child_slot = branch::find_slot(self, child->offset).value();

        // Reserve space.
        self->keys_.reserve(self->keys_.size() + 1u);
        self->pages_.reserve(self->pages_.size() + 1u);

        sibling = unique_lock_ptr<cycle_ptr::cycle_gptr<Child>>(abstract_page::allocate_page<Child>(child->cfg, alloc));
        sibling->offset = loader.allocate_disk_space(tx, sibling_offset);

        cycle_ptr::cycle_gptr<const key_type> key = Child::split(loader, child, sibling, tx);

        // Compute augmentation for sibling.
        cycle_ptr::cycle_gptr<augmented_page_ref> sibling_pgref = loader.allocate_augmented_page_ref(*sibling, sibling->get_allocator());
        sibling_pgref->offset_ = sibling->offset;
        // Compute updated augmentation for child page.
        cycle_ptr::cycle_gptr<augmented_page_ref> child_pgref = loader.allocate_augmented_page_ref(*sibling, sibling->get_allocator());
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


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_OPS_INL_H */
