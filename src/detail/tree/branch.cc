#include <earnest/detail/tree/branch.h>
#include <earnest/detail/tree/leaf.h>
#include <earnest/detail/tree/loader.h>
#include <earnest/detail/tree/augmented_page_ref.h>
#include <earnest/detail/tree/key_type.h>
#include <algorithm>
#include <iterator>
#include <string>
#include <boost/asio/write_at.hpp>

namespace earnest::detail::tree {


void branch::header::encode(boost::asio::mutable_buffer buf) const {
  assert(buf.size() >= SIZE);
  assert(magic == branch::magic);

  header tmp = *this;
  tmp.native_to_big_endian();
  boost::asio::buffer_copy(buf, boost::asio::const_buffer(&tmp, sizeof(tmp)));
}

const std::size_t branch::header::OFFSET_SIZE = offsetof(header, size);
const std::size_t branch::header::OFFSET_PARENT_PTR = offsetof(header, parent_off);


void branch::init() {
  abstract_page::init();

#ifndef NDEBUG
  std::shared_lock<branch> lck(*this);
  assert(keys_.empty());
  assert(pages_.empty());
#endif
}

branch::branch(std::shared_ptr<const struct cfg> tree_config, allocator_type alloc)
: abstract_page(std::move(tree_config), std::move(alloc)),
  keys_(key_vector::allocator_type(*this, this->alloc)),
  pages_(page_ref_vector::allocator_type(*this, this->alloc))
{}

branch::~branch() noexcept = default;

void branch::merge(const loader& loader, const unique_lock_ptr& front, const unique_lock_ptr& back, txfile::transaction& tx, cycle_ptr::cycle_gptr<const key_type> separator_key) {
  assert(front.owns_lock() && back.owns_lock());
  assert(front.mutex() != back.mutex());
  assert(front->cfg == back->cfg);
  assert(!front->pages_.empty() && !back->pages_.empty());
  assert(separator_key != nullptr);

  // Reserve space.
  front->pages_.reserve(front->pages_.size() + back->pages_.size());
  front->keys_.reserve(front->keys_.size() + 1u + back->keys_.size());

  // Copy the values of bytes_per_... locally,
  // so we won't dereference two pointers all the time.
  const auto bytes_per_key = front->bytes_per_key_();
  const auto bytes_per_augmented_page_ref = front->bytes_per_augmented_page_ref_();

  // Update size of the page.
  {
    std::uint32_t new_size = front->pages_.size() + back->pages_.size();
    boost::endian::native_to_big_inplace(new_size);
    boost::asio::write_at(
        tx,
        front->offset + header::OFFSET_SIZE,
        boost::asio::buffer(&new_size, sizeof(new_size)));
  }

  // Clear the back page.
  {
    std::uint32_t new_size = 0;
    boost::endian::native_to_big_inplace(new_size);
    boost::asio::write_at(
        tx,
        back->offset + header::OFFSET_SIZE,
        boost::asio::buffer(&new_size, sizeof(new_size)));
  }

  // Copy elements.
  {
    const auto append_offset = front->offset + header::SIZE
        + front->keys_.size() * bytes_per_key
        + front->pages_.size() * bytes_per_augmented_page_ref;
    std::vector<std::uint8_t, shared_resource_allocator<std::uint8_t>> append_buf_storage;
    append_buf_storage.resize(back->pages_.size() * (bytes_per_key + bytes_per_augmented_page_ref));

    // Fill append buf.
    {
      // Separator key.
      boost::asio::mutable_buffer append_buf = boost::asio::buffer(append_buf_storage);
      separator_key->encode(boost::asio::buffer(append_buf, bytes_per_key));
      append_buf += bytes_per_key;

      // Followed by first page ref.
      auto pages_iter = back->pages_.begin();
      assert(pages_iter != back->pages_.end());
      (*pages_iter)->encode(boost::asio::buffer(append_buf, bytes_per_augmented_page_ref));
      append_buf += bytes_per_augmented_page_ref;
      ++pages_iter;

      // Followed by key and page ref of the each remaining.
      auto keys_iter = back->keys_.begin();
      for (; pages_iter != back->pages_.end(); ++pages_iter, ++keys_iter) {
        assert(keys_iter != back->keys_.end());

        (*keys_iter)->encode(boost::asio::buffer(append_buf, bytes_per_key));
        append_buf += bytes_per_key;
        (*pages_iter)->encode(boost::asio::buffer(append_buf, bytes_per_augmented_page_ref));
        append_buf += bytes_per_key;
      }
      assert(pages_iter == back->pages_.end() && keys_iter == back->keys_.end());
      assert(append_buf.size() == 0);
    }
    // Write append buf.
    boost::asio::write_at(
        tx,
        append_offset,
        boost::asio::buffer(append_buf_storage));
  }

  // Gather all moved pages.
  // We need to lock them to update their parent pointer.
  std::vector<earnest::detail::unique_lock_ptr<cycle_ptr::cycle_gptr<abstract_page>>, shared_resource_allocator<earnest::detail::unique_lock_ptr<cycle_ptr::cycle_gptr<abstract_page>>>> moved_children(tx.get_allocator());
  for (const auto& pgref : back->pages_) {
    moved_children.emplace_back(
        abstract_page::load_from_disk(pgref->offset(), loader));
  }
  // Update parent pointers of moved pages.
  std::vector<txfile::transaction::offset_type> offsets;
  offsets.reserve(moved_children.size());
  std::transform(
      moved_children.cbegin(), moved_children.cend(),
      std::back_inserter(offsets),
      [](const auto& child_ptr) {
        assert(branch::header::OFFSET_PARENT_PTR == leaf::header::OFFSET_PARENT_PTR);
        return child_ptr->offset + branch::header::OFFSET_PARENT_PTR;
      });
  offset_type moved_parent_be = front->offset;
  boost::endian::native_to_big_inplace(moved_parent_be);
  tx.write_at_many(std::move(offsets), boost::asio::buffer(&moved_parent_be, sizeof(moved_parent_be)));

  tx.on_commit(
      [   front_ptr=front.mutex(),
          back_ptr=back.mutex(),
          separator_key=std::move(separator_key),
          moved_children=std::move(moved_children)
      ]() noexcept {
        // Move keys and pages.
        front_ptr->keys_.emplace_back(separator_key);
        std::copy(
            std::make_move_iterator(back_ptr->keys_.begin()), std::make_move_iterator(back_ptr->keys_.end()),
            std::back_inserter(front_ptr->keys_));
        std::copy(
            std::make_move_iterator(back_ptr->pages_.begin()), std::make_move_iterator(back_ptr->pages_.end()),
            std::back_inserter(front_ptr->pages_));
        // Clear out the pointers in back.
        back_ptr->keys_.clear();
        back_ptr->pages_.clear();

        // Update parent offset.
        std::for_each(moved_children.begin(), moved_children.end(),
            [&front_ptr](auto& child_ptr) {
              child_ptr->parent = front_ptr->offset;
            });

        // Invalidate back page.
        back_ptr->valid_ = false;
      });
}

auto branch::split(const loader& loader, const unique_lock_ptr& front, const unique_lock_ptr& back, txfile::transaction& tx)
-> cycle_ptr::cycle_gptr<const key_type> {
  assert(front.owns_lock() && back.owns_lock());
  assert(front.mutex() != back.mutex());
  assert(front->cfg == back->cfg);
  assert(front->pages_.size() >= 2u);
  assert(back->pages_.empty());

  const auto num_pages_to_keep = front->pages_.size() / 2u;
  const auto num_pages_to_move = front->pages_.size() - num_pages_to_keep;
  const auto num_keys_to_keep = num_pages_to_keep - 1u;
  const auto num_keys_to_move = num_pages_to_move - 1u;

  // Reserve space.
  back->pages_.reserve(num_pages_to_move);
  back->keys_.reserve(num_pages_to_move - 1u);

  // Copy the values of bytes_per_... locally,
  // so we won't dereference two pointers all the time.
  const auto bytes_per_key = front->bytes_per_key_();
  const auto bytes_per_augmented_page_ref = front->bytes_per_augmented_page_ref_();

  // Update size of the front page.
  {
    std::uint32_t new_size = num_pages_to_keep;
    boost::endian::native_to_big_inplace(new_size);
    boost::asio::write_at(
        tx,
        front->offset + header::OFFSET_SIZE,
        boost::asio::buffer(&new_size, sizeof(new_size)));
  }

  // Write data for back page.
  {
    std::vector<std::uint8_t, shared_resource_allocator<std::uint8_t>> append_buf_storage(tx.get_allocator());
    append_buf_storage.resize(header::SIZE + num_pages_to_move * bytes_per_augmented_page_ref + num_keys_to_move * bytes_per_key);
    {
      boost::asio::mutable_buffer append_buf = boost::asio::buffer(append_buf_storage) + header::SIZE;

      auto pages_iter = front->pages_.end() - num_pages_to_move;
      assert(pages_iter != front->pages_.end());
      (*pages_iter)->encode(boost::asio::buffer(append_buf, bytes_per_augmented_page_ref));
      append_buf += bytes_per_augmented_page_ref;
      ++pages_iter;

      auto keys_iter = front->keys_.end() - num_keys_to_move;
      for (; pages_iter != front->pages_.end(); ++pages_iter, ++keys_iter) {
        assert(keys_iter != front->keys_.end());

        (*keys_iter)->encode(boost::asio::buffer(append_buf, bytes_per_key));
        append_buf += bytes_per_key;
        (*pages_iter)->encode(boost::asio::buffer(append_buf, bytes_per_augmented_page_ref));
        append_buf += bytes_per_augmented_page_ref;
      }
      assert(pages_iter == front->pages_.end() && keys_iter == front->keys_.end());
      assert(append_buf.size() == 0);
    }
    // Also ensure we write out a header.
    std::array<std::uint8_t, header::SIZE> header_buf;
    {
      header h;
      h.magic = magic;
      h.size = num_pages_to_move;
      h.parent_off = front->parent;
      h.encode(boost::asio::buffer(append_buf_storage, header::SIZE));
    }
    boost::asio::write_at(
        tx,
        back->offset,
        boost::asio::buffer(append_buf_storage));
  }

  // Gather all moved pages.
  // We need to lock them to update their parent pointer.
  std::vector<earnest::detail::unique_lock_ptr<cycle_ptr::cycle_gptr<abstract_page>>, shared_resource_allocator<earnest::detail::unique_lock_ptr<cycle_ptr::cycle_gptr<abstract_page>>>> moved_children(tx.get_allocator());
  for (auto pgref_iter = front->pages_.cend() - num_pages_to_move;
      pgref_iter != front->pages_.cend();
      ++pgref_iter) {
    moved_children.emplace_back(
        abstract_page::load_from_disk((*pgref_iter)->offset(), loader));
  }
  // Update parent pointers of moved pages.
  std::vector<txfile::transaction::offset_type> offsets;
  offsets.reserve(moved_children.size());
  std::transform(
      moved_children.cbegin(), moved_children.cend(),
      std::back_inserter(offsets),
      [](const auto& child_ptr) {
        assert(branch::header::OFFSET_PARENT_PTR == leaf::header::OFFSET_PARENT_PTR);
        return child_ptr->offset + branch::header::OFFSET_PARENT_PTR;
      });
  offset_type moved_parent_be = back->offset;
  boost::endian::native_to_big_inplace(moved_parent_be);
  tx.write_at_many(std::move(offsets), boost::asio::buffer(&moved_parent_be, sizeof(moved_parent_be)));

  tx.on_commit(
      [   front_ptr=front.mutex(),
          back_ptr=back.mutex(),
          num_pages_to_keep,
          num_keys_to_keep,
          num_pages_to_move,
          num_keys_to_move,
          moved_children=std::move(moved_children)
      ]() noexcept {
        // Move pages.
        std::copy(
            std::make_move_iterator(front_ptr->pages_.end() - num_pages_to_move),
            std::make_move_iterator(front_ptr->pages_.end()),
            std::back_inserter(back_ptr->pages_));
        // Move keys.
        std::copy(
            std::make_move_iterator(front_ptr->keys_.end() - num_keys_to_move),
            std::make_move_iterator(front_ptr->keys_.end()),
            std::back_inserter(back_ptr->keys_));

        // Truncate front.
        front_ptr->keys_.resize(num_keys_to_keep);
        front_ptr->pages_.resize(num_pages_to_keep);

        // Ensure back has the same parent.
        back_ptr->parent = front_ptr->parent;

        // Update parent offset.
        std::for_each(moved_children.begin(), moved_children.end(),
            [&back_ptr](auto& child_ptr) {
              child_ptr->parent = back_ptr->offset;
            });
      });

  // Return the one key in between, that will no longer be present on either page.
  return front->keys_[num_keys_to_keep];
}

auto branch::find_slot_(offset_type page) const noexcept -> std::optional<size_type> {
  const auto search_result = std::find_if(
      pages_.begin(), pages_.end(),
      [&page](const auto& pgref_ptr) {
        return pgref_ptr->offset() == page;
      });
  if (search_result == pages_.end()) return std::nullopt;
  return search_result - pages_.begin();
}

void branch::decode_(const loader& loader, const txfile::transaction& tx, offset_type off, boost::system::error_code& ec) {
  auto stream = buffered_read_stream_at<const txfile::transaction, shared_resource_allocator<void>>(tx, off, bytes_per_page_(), tx.get_allocator());

  header h;
  h.decode(stream, ec);
  if (ec) return;

  if (h.size != 0u) {
    std::string key_buf, pgref_buf;
    key_buf.resize(bytes_per_key_());
    pgref_buf.resize(bytes_per_augmented_page_ref_());

    boost::asio::read(stream, boost::asio::buffer(pgref_buf), ec);
    if (ec) return;

    cycle_ptr::cycle_gptr<augmented_page_ref> page_ref = loader.allocate_augmented_page_ref(alloc);
    page_ref->decode(boost::asio::buffer(pgref_buf));
    pages_.emplace_back(std::move(page_ref));

    for (std::uint32_t i = 1; i < h.size; ++i) {
      boost::asio::read(
          stream,
          std::array<boost::asio::mutable_buffer, 2>{
            boost::asio::buffer(key_buf),
            boost::asio::buffer(pgref_buf)
          },
          ec);
      if (ec) return;

      cycle_ptr::cycle_gptr<augmented_page_ref> page_ref = loader.allocate_augmented_page_ref(alloc);
      page_ref->decode(boost::asio::buffer(pgref_buf));
      pages_.emplace_back(std::move(page_ref));

      cycle_ptr::cycle_gptr<key_type> key = loader.allocate_key(alloc);
      key->decode(boost::asio::buffer(key_buf));
      keys_.emplace_back(std::move(key));
    }
  }

  this->offset = off;
}


} /* namespace earnest::detail::tree */
