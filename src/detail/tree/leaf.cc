#include <earnest/detail/tree/leaf.h>
#include <earnest/detail/tree/value_type.h>
#include <earnest/detail/tree/key_type.h>
#include <earnest/detail/tree/loader.h>
#include <algorithm>
#include <cassert>
#include <deque>
#include <memory>
#include <vector>
#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/write_at.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/completion_condition.hpp>
#include <boost/polymorphic_pointer_cast.hpp>

namespace earnest::detail::tree {
namespace {


class leaf_sentinel final
: public value_type
{
  public:
  using value_type::value_type;

  ~leaf_sentinel() noexcept override = default;

  void lock() override { mtx_.lock(); }
  auto try_lock() -> bool override { return mtx_.try_lock(); }
  void unlock() override { mtx_.unlock(); }
  void lock_shared() const override { mtx_.lock_shared(); }
  auto try_lock_shared() const -> bool override { return mtx_.try_lock_shared(); }
  void unlock_shared() const override { mtx_.unlock_shared(); }

  auto is_never_visible() const noexcept -> bool override { return true; }

  auto get_key(allocator_type alloc) const -> cycle_ptr::cycle_gptr<const key_type> override {
    assert(false);
    throw std::logic_error("leaf sentinels are not supposed to have a key");
  }

  private:
  void encode(boost::asio::mutable_buffer buf) const override {
    assert(false);
    throw std::logic_error("leaf sentinels are not supposed to ever be encoded");
  }

  void decode(boost::asio::const_buffer buf) override {
    assert(false);
    throw std::logic_error("leaf sentinels are not supposed to ever be decoded");
  }

  mutable std::shared_mutex mtx_;
};


} /* namespace earnest::detail::tree::<unnamed> */


void leaf::header::encode(boost::asio::mutable_buffer buf) const {
  assert(buf.size() >= SIZE);
  assert(magic == leaf::magic);

  header tmp = *this;
  tmp.native_to_big_endian();
  boost::asio::buffer_copy(buf, boost::asio::const_buffer(&tmp, sizeof(tmp)));
}

const std::size_t leaf::header::OFFSET_PARENT_PTR = offsetof(header, parent_off);
const std::size_t leaf::header::OFFSET_NEXT_SIBLING_PTR = offsetof(header, next_sibling_off);
const std::size_t leaf::header::OFFSET_PREV_SIBLING_PTR = offsetof(header, prev_sibling_off);


void leaf::init() {
  abstract_page::init();

#ifndef NDEBUG
  value_type::unique_lock_ptr lck(sentinel_);
  assert(sentinel_->parent_ == nullptr);
#endif

  sentinel_->parent_ = shared_from_this(this);
  sentinel_->pred_ = sentinel_->succ_ = sentinel_;
}

leaf::leaf(cycle_ptr::cycle_gptr<abstract_tree> tree, allocator_type alloc)
: abstract_page(std::move(tree), std::move(alloc)),
  sentinel_(*this, cycle_ptr::allocate_cycle<leaf_sentinel>(this->alloc, cfg->items_per_leaf_page, this->alloc)),
  key_(*this)
{}

leaf::~leaf() noexcept = default;

void leaf::merge(const loader& loader, const unique_lock_ptr& front, const unique_lock_ptr& back, txfile::transaction& tx) {
  using moved_vector_t = std::vector<value_type::unique_lock_ptr, shared_resource_allocator<value_type::unique_lock_ptr>>;

  assert(front.owns_lock() && back.owns_lock());
  assert(front.mutex() != back.mutex());
  assert(front->cfg == back->cfg);
  assert(front->size_ > 0 && back->size_ > 0);

  std::deque<index_type, shared_resource_allocator<index_type>> front_to_be_zeroed(tx.get_allocator());
  std::vector<index_type, shared_resource_allocator<index_type>> back_to_be_zeroed(tx.get_allocator());

  std::basic_string<char, std::char_traits<char>, shared_resource_allocator<char>> elem_buffer(tx.get_allocator());
  elem_buffer.resize(front->bytes_per_val_());

  unique_lock_ptr back_successor;
  if (back->successor_off_ != 0) {
    back_successor = unique_lock_ptr(
        boost::polymorphic_pointer_downcast<leaf>(
            load_from_disk(back->successor_off_, loader)));
  }

  index_type next_idx = 0;

  // Compact element in the front page.
  moved_vector_t reslotted(tx.get_allocator());
  reslotted.reserve(front->size_);
  for (cycle_ptr::cycle_gptr<value_type> elem = front->sentinel_->succ_;
      elem != front->sentinel_;
      elem = elem->succ_, ++next_idx) {
    reslotted.emplace_back(elem);
    // Ensure this index won't be zeroed.
    if (!front_to_be_zeroed.empty() && front_to_be_zeroed.front() == next_idx)
      front_to_be_zeroed.pop_front();
    // Skip if the item is already in the correct position.
    if (elem->slot_ == next_idx) continue;

    // Mark space for zeroing out.
    front_to_be_zeroed.push_back(elem->slot_);

    // Update element slot.
    elem->encode(boost::asio::buffer(elem_buffer));
    boost::asio::write_at(
        tx, front->offset_for_idx_(next_idx),
        boost::asio::buffer(elem_buffer));
  }

  value_type::unique_lock_ptr f_sentinel(front->sentinel_);

  moved_vector_t moved(tx.get_allocator());
  moved.reserve(back->size_);

  // Append elements from the back page.
  for (cycle_ptr::cycle_gptr<value_type> elem = back->sentinel_->succ_;
      elem != back->sentinel_;
      elem = elem->succ_, ++next_idx) {
    moved.emplace_back(elem);
    // Ensure we won't zero out this slot.
    if (!front_to_be_zeroed.empty() && front_to_be_zeroed.front() == next_idx)
      front_to_be_zeroed.pop_front();
    // Mark space for zeroing out.
    back_to_be_zeroed.push_back(elem->slot_);

    // Update element slot.
    elem->encode(boost::asio::buffer(elem_buffer));
    boost::asio::write_at(
        tx, front->offset_for_idx_(next_idx),
        boost::asio::buffer(elem_buffer));
  }

  // Update sibling offsets for 'front' and 'back_successor' on disk.
  {
    std::uint64_t front_offset_be = front->offset;
    std::uint64_t back_successor_offset_be = (back_successor ? back_successor->offset : 0u);
    boost::endian::native_to_big_inplace(front_offset_be);
    boost::endian::native_to_big_inplace(back_successor_offset_be);

    boost::asio::write_at(
        tx,
        front->offset + header::OFFSET_NEXT_SIBLING_PTR,
        boost::asio::buffer(&back_successor_offset_be, sizeof(back_successor_offset_be)),
        boost::asio::transfer_all());
    if (back_successor) {
      boost::asio::write_at(
          tx,
          back_successor->offset + header::OFFSET_PREV_SIBLING_PTR,
          boost::asio::buffer(&front_offset_be, sizeof(front_offset_be)),
          boost::asio::transfer_all());
    }
  }

  value_type::unique_lock_ptr b_sentinel(back->sentinel_);
  tx.on_commit(
      [   front_sentinel=std::move(f_sentinel),
          back_sentinel=std::move(b_sentinel),
          moved=std::move(moved),
          reslotted=std::move(reslotted),
          front_ptr=front.mutex(),
          back_ptr=back.mutex(),
          back_successor=std::move(back_successor)
      ]() noexcept {
        // Update slot index and parent.
        index_type next_idx = 0;
        for (auto i = reslotted.cbegin(); i != reslotted.cend(); ++i, ++next_idx)
          (*i)->slot_ = next_idx;
        for (auto i = moved.cbegin(); i != moved.cend(); ++i, ++next_idx) {
          (*i)->slot_ = next_idx;
          (*i)->parent_ = front_ptr;
        }

        // Update page sizes.
        front_ptr->size_ += std::exchange(back_ptr->size_, 0u);

        // Splice elements.
        front_sentinel->pred_ = moved.back().mutex();
        moved.back()->succ_ = front_sentinel.mutex();

        reslotted.back()->succ_ = moved.front().mutex();
        moved.front()->pred_ = reslotted.back().mutex();

        back_sentinel->pred_ = back_sentinel->succ_ = back_sentinel.mutex();

        // Update sibling pointers.
        front_ptr->successor_off_ = (back_successor ? back_successor->offset : 0u);
        if (back_successor) back_successor->predecessor_off_ = front_ptr->offset;
      });

  // Zero out the previously-filled-but-now-empty slots.
  {
    std::vector<offset_type> offsets;
    std::transform(
        front_to_be_zeroed.begin(), front_to_be_zeroed.end(),
        std::back_inserter(offsets),
        [self_ptr=front.mutex().get()](index_type slot) -> offset_type {
          return self_ptr->offset_for_idx_(slot);
        });
    std::transform(
        back_to_be_zeroed.begin(), back_to_be_zeroed.end(),
        std::back_inserter(offsets),
        [self_ptr=back.mutex().get()](index_type slot) -> offset_type {
          return self_ptr->offset_for_idx_(slot);
        });

    assert(elem_buffer.size() == front->bytes_per_val_());
    std::fill(elem_buffer.begin(), elem_buffer.end(), 0u);

    tx.write_at_many(std::move(offsets), boost::asio::buffer(elem_buffer));
  }
}

void leaf::split(const loader& loader, const unique_lock_ptr& front, const unique_lock_ptr& back, txfile::transaction& tx) {
  assert(front.owns_lock() && back.owns_lock());
  assert(front.mutex() != back.mutex());
  assert(front->cfg == back->cfg);
  assert(front->size_ >= 2 && back->size_ == 0);

  const size_type new_front_size = front->size_ / 2u;
  const size_type new_back_size = front->size_ - new_front_size;

  std::basic_string<char, std::char_traits<char>, shared_resource_allocator<char>> elem_buffer(tx.get_allocator());
  elem_buffer.resize(front->bytes_per_val_());

  std::vector<index_type, shared_resource_allocator<index_type>> front_to_be_zeroed(tx.get_allocator());
  front_to_be_zeroed.reserve(new_back_size);

  using moved_vector_t = std::vector<value_type::unique_lock_ptr, shared_resource_allocator<value_type::unique_lock_ptr>>;
  moved_vector_t moved(tx.get_allocator());
  moved.reserve(new_back_size);

  unique_lock_ptr back_successor;
  if (back->successor_off_ != 0) {
    back_successor = unique_lock_ptr(
        boost::polymorphic_pointer_downcast<leaf>(
            load_from_disk(back->successor_off_, loader)));
  }

  value_type::unique_lock_ptr last_kept;
  {
    cycle_ptr::cycle_gptr<value_type> lk = front->sentinel_;
    for (size_type i = 0; i < new_front_size; ++i)
      lk = lk->succ_;
    last_kept = value_type::unique_lock_ptr(std::move(lk));
  }
  for (cycle_ptr::cycle_gptr<value_type> i = last_kept->succ_; i != front->sentinel_; i = i->succ_)
    moved.emplace_back(i);
  value_type::unique_lock_ptr f_sentinel(front->sentinel_);
  value_type::unique_lock_ptr b_sentinel(back->sentinel_);

  index_type target_slot = 0;
  for (auto i = moved.cbegin(); i != moved.cend(); ++i, ++target_slot) {
    front_to_be_zeroed.push_back((*i)->slot_);

    (*i)->encode(boost::asio::buffer(elem_buffer));
    boost::asio::write_at(
        tx, back->offset_for_idx_(target_slot),
        boost::asio::buffer(elem_buffer));
  }

  const cycle_ptr::cycle_gptr<const key_type> back_key = moved.front()->get_key(back->get_allocator());
  if (back_key == nullptr) throw std::logic_error("unable to extract key from value tpye");
  {
    header h;
    h.magic = magic;
    h.flags = header::flag_has_key;
    h.parent_off = front->parent;
    h.next_sibling_off = (back_successor ? back_successor->offset : 0u);
    h.prev_sibling_off = front->offset;

    std::vector<std::uint8_t, shared_resource_allocator<std::uint8_t>> buf;
    buf.resize(header::SIZE + back->cfg->key_bytes);
    h.encode(boost::asio::buffer(boost::asio::buffer(buf), header::SIZE));
    back_key->encode(boost::asio::buffer(buf) + header::SIZE);

    boost::asio::write_at(tx, back->offset, boost::asio::buffer(buf));
  }

  // Update sibling offsets for 'front' and 'back_successor' on disk.
  {
    std::uint64_t back_offset_be = back->offset;
    boost::endian::native_to_big_inplace(back_offset_be);

    std::vector<txfile::transaction::offset_type> offsets;
    offsets.reserve(2);
    offsets.push_back(front->offset + header::OFFSET_NEXT_SIBLING_PTR);
    if (back_successor)
      offsets.push_back(back_successor->offset + header::OFFSET_PREV_SIBLING_PTR);
    tx.write_at_many(
        std::move(offsets),
        boost::asio::buffer(&back_offset_be, sizeof(back_offset_be)));
  }

  tx.on_commit(
      [   last_kept=std::move(last_kept),
          moved=std::move(moved),
          front_sentinel=std::move(f_sentinel),
          back_sentinel=std::move(b_sentinel),
          front_ptr=front.mutex(),
          back_ptr=back.mutex(),
          new_front_size,
          new_back_size,
          back_key,
          back_successor=std::move(back_successor)
      ]() noexcept {
        // Update slot indices and parent pointer of moved elements.
        index_type target_slot = 0;
        for (auto i = moved.cbegin(); i != moved.cend(); ++i, ++target_slot) {
          (*i)->slot_ = target_slot;
          (*i)->parent_ = back_ptr;
        }

        // Install new key.
        back_ptr->key_ = std::move(back_key);

        // Splice elements.
        front_sentinel->pred_ = last_kept.mutex();
        last_kept->succ_ = front_sentinel.mutex();

        moved.front()->pred_ = back_sentinel.mutex();
        back_sentinel->succ_ = moved.front().mutex();
        moved.back()->succ_ = back_sentinel.mutex();
        back_sentinel->pred_ = moved.back().mutex();

        // Update collection sizes.
        front_ptr->size_ = new_front_size;
        back_ptr->size_ = new_back_size;

        // Update page links.
        back_ptr->predecessor_off_ = front_ptr->offset;
        if (back_successor) back_successor->predecessor_off_ = back_ptr->offset;

        front_ptr->successor_off_ = back_ptr->offset;
        back_ptr->successor_off_ = (back_successor ? back_successor->offset : 0u);

        back_ptr->parent = front_ptr->parent;
      });

  // Zero out the previously-filled-but-now-empty slots.
  {
    std::vector<offset_type> offsets;
    std::transform(
        front_to_be_zeroed.begin(), front_to_be_zeroed.end(),
        std::back_inserter(offsets),
        [self_ptr=front.mutex().get()](index_type slot) -> offset_type {
          return self_ptr->offset_for_idx_(slot);
        });

    assert(elem_buffer.size() == front->bytes_per_val_());
    std::fill(elem_buffer.begin(), elem_buffer.end(), 0u);

    tx.write_at_many(std::move(offsets), boost::asio::buffer(elem_buffer));
  }
}

void leaf::unlink(const unique_lock_ptr& self, cycle_ptr::cycle_gptr<value_type> elem, txfile::transaction& tx) {
  assert(self.owns_lock());
  assert(value_type::shared_lock_ptr(elem)->parent_ == self.mutex());
  assert(elem != self->sentinel_);

  value_type::unique_lock_ptr elem_pred, elem_lck, elem_succ;
  std::tie(elem_pred, elem_lck, elem_succ) = lock_elem_with_siblings_(self, elem);

  // Zero out elem.
  {
    std::vector<std::uint8_t, shared_resource_allocator<std::uint8_t>> zero_buf(tx.get_allocator());
    zero_buf.resize(self->bytes_per_val_());
    std::fill(zero_buf.begin(), zero_buf.end(), 0u);

    boost::asio::write_at(
        tx,
        self->offset_for_idx_(elem_lck->slot_),
        boost::asio::buffer(zero_buf),
        boost::asio::transfer_all());
  }

  tx.on_commit(
      [   self=self.mutex(),
          elem_pred=std::move(elem_pred),
          elem_succ=std::move(elem_succ),
          elem_lck=std::move(elem_lck)
      ]() noexcept {
        // Update successor/predessor pointers of remaining elements.
        elem_pred->succ_ = elem_succ.mutex();
        elem_succ->pred_ = elem_pred.mutex();

        // Clear elem list pointers.
        elem_lck->pred_ = nullptr;
        elem_lck->succ_ = nullptr;
        elem_lck->parent_ = nullptr;

        // Update collection size.
        --self->size_;
      });
}

void leaf::link(const unique_lock_ptr& self, cycle_ptr::cycle_gptr<value_type> elem, cycle_ptr::cycle_gptr<value_type> pos, txfile::transaction& tx) {
  using lock_vector = std::vector<value_type::unique_lock_ptr, shared_resource_allocator<value_type::unique_lock_ptr>>;

  assert(self.owns_lock());
  assert(size(self) < self->max_size());
  assert(value_type::shared_lock_ptr(elem)->parent_ == nullptr);

  if (pos == nullptr) pos = self->sentinel_;
  assert(value_type::shared_lock_ptr(pos)->parent_ == self.mutex());

  std::basic_string<char, std::char_traits<char>, shared_resource_allocator<char>> elem_buffer(tx.get_allocator());
  elem_buffer.resize(self->bytes_per_val_());

  const bool no_shifting_needed = (pos->slot_ != 0 && (pos->pred_ == self->sentinel_ || pos->pred_->slot_ < pos->slot_ - 1u));
  const bool shifting_needed = !no_shifting_needed;

  // Attempt to move predecessors forward, to create a gap.
  // This vector will contain the elements that need to be shifted
  // back one position, to create a gap.
  lock_vector predecessors(tx.get_allocator());
  if (shifting_needed) {
    for (cycle_ptr::cycle_gptr<value_type> i = pos->pred_;
        i != self->sentinel_ && (predecessors.empty() || predecessors.back()->slot_ == i->slot_ + 1u);
        i = i->pred_)
      predecessors.emplace_back(i, std::defer_lock);
    if (!predecessors.empty()) {
      if (predecessors.back()->slot_ == 0) { // There is no ability to make a gap.
        predecessors.clear();
      } else {
        // Ensure predecessors are ordered ascending.
        std::reverse(predecessors.begin(), predecessors.end());
        // And then lock them.
        std::for_each(predecessors.begin(), predecessors.end(),
            [](value_type::unique_lock_ptr& plck) {
              plck.lock();
            });
      }
    }
  }

  // Figure out predecessor.
  // We must ensure it's locked only once.
  // But we must _not_ lock it, if it is the sentinel.
  value_type::unique_lock_ptr elem_pred(pos->pred_, std::defer_lock);
  if (predecessors.empty() && elem_pred.mutex() != self->sentinel_)
    elem_pred.lock();

  // Lock the element.
  value_type::unique_lock_ptr elem_lck(elem);

  // Create lock for successor.
  value_type::unique_lock_ptr elem_succ(pos, std::defer_lock);

  // Figure out how to move successors forward to create a gap.
  // Note that we don't have to do this if the predecessors exist.
  lock_vector successors(tx.get_allocator());
  if (shifting_needed && predecessors.empty()) {
    for (cycle_ptr::cycle_gptr<value_type> i = pos;
        i != self->sentinel_ && (successors.empty() || successors.back()->slot_ + 1u == i->slot_);
        i = i->succ_) {
      successors.emplace_back(i);
    }
  }
  if (successors.empty()) elem_succ.lock();

  // Lock sentinel predecessor.
  if (predecessors.empty() && !elem_pred.owns_lock() && elem_pred.mutex() != elem_succ.mutex())
    elem_pred.lock();

  // Assertions so far.
  if (no_shifting_needed) {
    assert(predecessors.empty());
    assert(successors.empty());
  }
  if (shifting_needed) {
    assert(predecessors.empty() != successors.empty()); // Only one set exists.
  }
  if (elem_pred.mutex() == elem_succ.mutex()) {
    assert(!elem_pred.owns_lock());
    assert(predecessors.empty() == elem_succ.owns_lock()); // Predecessor is locked.
  } else {
    assert(predecessors.empty() == elem_pred.owns_lock()); // Predecessor is locked.
  }
  assert(successors.empty() == elem_succ.owns_lock()); // Successor is locked.
  assert(predecessors.empty() || predecessors.back().mutex() == elem_pred.mutex());
  assert(successors.empty() || successors.front().mutex() == elem_succ.mutex());

  // Write predecessors in their new position.
  if (!predecessors.empty()) {
    const auto bytes_per_val = self->bytes_per_val_();
    elem_buffer.resize(predecessors.size() * bytes_per_val);
    {
      boost::asio::mutable_buffer remaining = boost::asio::buffer(elem_buffer);
      for (const auto& i : predecessors) {
        assert(remaining.size() >= bytes_per_val);
        i->encode(boost::asio::buffer(remaining, bytes_per_val));
        remaining += bytes_per_val;
      }
      assert(remaining.size() == 0);
    }

    boost::asio::write_at(
        tx,
        self->offset_for_idx_(predecessors.front()->slot_ - 1u),
        boost::asio::buffer(elem_buffer),
        boost::asio::transfer_all());
  }
  // Write successors in their new position.
  if (!successors.empty()) {
    const auto bytes_per_val = self->bytes_per_val_();
    elem_buffer.resize(successors.size() * self->bytes_per_val_());
    {
      boost::asio::mutable_buffer remaining = boost::asio::buffer(elem_buffer);
      for (const auto& i : successors) {
        assert(remaining.size() >= bytes_per_val);
        i->encode(boost::asio::buffer(remaining, bytes_per_val));
        remaining += bytes_per_val;
      }
      assert(remaining.size() == 0);
    }

    boost::asio::write_at(
        tx,
        self->offset_for_idx_(successors.front()->slot_ + 1u),
        boost::asio::buffer(elem_buffer),
        boost::asio::transfer_all());
  }
  // Write element in its new position.
  index_type elem_slot;
  if (shifting_needed)
    elem_slot = !predecessors.empty() ? predecessors.back()->slot_ : successors.front()->slot_;
  else
    elem_slot = pos->slot_ - 1u;

  elem_buffer.resize(self->bytes_per_val_());
  elem_lck->encode(boost::asio::buffer(elem_buffer));
  boost::asio::write_at(
      tx,
      self->offset_for_idx_(elem_slot),
      boost::asio::buffer(elem_buffer),
      boost::asio::transfer_all());

  tx.on_commit(
      [   self=self.mutex(),
          elem_pred=std::move(elem_pred),
          elem_succ=std::move(elem_succ),
          elem_lck=std::move(elem_lck),
          predecessors=std::move(predecessors),
          successors=std::move(successors),
          elem_slot
      ]() noexcept {
        // Update predecessor slot indices.
        std::for_each(predecessors.cbegin(), predecessors.cend(),
            [](const value_type::unique_lock_ptr& p) {
              --p->slot_;
            });
        // Update successor slot indices.
        std::for_each(successors.cbegin(), successors.cend(),
            [](const value_type::unique_lock_ptr& s) {
              ++s->slot_;
            });

        // Update parent.
        elem_lck->parent_ = self;
        // And also update slot.
        elem_lck->slot_ = elem_slot;

        // Link predecessor <--> elem.
        elem_pred->succ_ = elem_lck.mutex();
        elem_lck->pred_ = elem_pred.mutex();

        // Link elem <--> successor.
        elem_lck->succ_ = elem_succ.mutex();
        elem_succ->pred_ = elem_lck.mutex();

        // Update collection size.
        ++self->size_;
      });
}

auto leaf::get_elements(const shared_lock_ptr& self) -> std::vector<cycle_ptr::cycle_gptr<value_type>> {
  assert(self.owns_lock());

  std::vector<cycle_ptr::cycle_gptr<value_type>> r;
  r.reserve(self->size_);

  for (cycle_ptr::cycle_gptr<value_type> i = self->sentinel_->succ_;
      i != self->sentinel_;
      i = i->succ_)
    r.emplace_back(i);
  return r;
}

auto leaf::get_elements(const unique_lock_ptr& self) -> std::vector<cycle_ptr::cycle_gptr<value_type>> {
  assert(self.owns_lock());

  std::vector<cycle_ptr::cycle_gptr<value_type>> r;
  r.reserve(self->size_);

  for (cycle_ptr::cycle_gptr<value_type> i = self->sentinel_->succ_;
      i != self->sentinel_;
      i = i->succ_)
    r.emplace_back(i);
  return r;
}

auto leaf::lock_elem_with_siblings_(const unique_lock_ptr& self, cycle_ptr::cycle_gptr<value_type> elem) -> std::tuple<value_type::unique_lock_ptr, value_type::unique_lock_ptr, value_type::unique_lock_ptr> {
  assert(self.owns_lock());
  assert(value_type::shared_lock_ptr(elem)->parent_ == self.mutex());

  // Normally, we would need to lock elem first, and then relock backwards
  // into pred.
  // However, since 'self' is locked, we know those pointers can't be changed
  // and thus can use elem->pred_ and elem->succ_ directly.
  std::tuple<value_type::unique_lock_ptr, value_type::unique_lock_ptr, value_type::unique_lock_ptr> r(
      value_type::unique_lock_ptr(elem->pred_, std::defer_lock),
      value_type::unique_lock_ptr(elem, std::defer_lock),
      value_type::unique_lock_ptr(elem->succ_, std::defer_lock));

  if (std::get<0>(r).mutex() != self->sentinel_) // Lock sentinel last.
    std::get<0>(r).lock();
  std::get<1>(r).lock();
  std::get<2>(r).lock();
  // Lock the sentinel, but take care not to lock it, if successor is also sentinel.
  if (!std::get<0>(r).owns_lock() && std::get<0>(r).mutex() != std::get<2>(r).mutex())
    std::get<0>(r).lock();

  return r;
}

void leaf::decode_(const txfile::transaction& tx, offset_type off, boost::system::error_code& ec) {
  auto stream = buffered_read_stream_at<const txfile::transaction, shared_resource_allocator<void>>(tx, off, bytes_per_page_(), tx.get_allocator());

  header h;
  h.decode(stream, ec);
  if (ec) return;

  // Decode the page key.
  std::string buf;
  buf.resize(cfg->key_bytes);
  boost::asio::read(stream, boost::asio::buffer(buf), ec);
  if (ec) return;
  if (h.flags & header::flag_has_key) { // Decode key.
    cycle_ptr::cycle_gptr<key_type> key = cfg->allocate_key(alloc);
    key->decode(boost::asio::buffer(buf));
    key_ = std::move(key);
  }

  buf.resize(bytes_per_val_());
  for (size_type n = 0; n < cfg->items_per_leaf_page; ++n) {
    boost::asio::read(stream, boost::asio::buffer(buf), ec);
    if (ec) return;

    // Decode key-value element.
    cycle_ptr::cycle_gptr<value_type> value = cfg->allocate_elem(alloc);
    value->decode(boost::asio::buffer(buf));

    // If the value is visible, link it.
    if (!value->is_never_visible()) {
      value->parent_ = shared_from_this(this);
      cycle_ptr::cycle_gptr<value_type> pred = sentinel_->pred_;
      value->pred_ = pred;
      pred->succ_ = value;
      value->succ_ = sentinel_;
      sentinel_->pred_ = value;
    }
  }

  this->offset = off;
}


} /* namespace earnest::detail::tree */
