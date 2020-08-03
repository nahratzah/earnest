#ifndef EARNEST_DETAIL_TREE_PAGE_LEAF_ELEMS_H
#define EARNEST_DETAIL_TREE_PAGE_LEAF_ELEMS_H

#include <stdexcept>

namespace earnest::detail {


inline tree_page_leaf_elems_hook::tree_page_leaf_elems_hook(cycle_ptr::cycle_base& owner) noexcept
: succ_(owner),
  pred_(owner)
{
  static_assert(std::is_base_of_v<tree_page_leaf_elems_hook, Elem>,
      "Element must derive from tree_page_leaf_elems<Element>");
}


inline auto tree_page_leaf_elems_hook::index() const -> index_type {
  return index(std::shared_lock<std::shared_mutex>(mtx_ref_()));
}

inline auto tree_page_leaf_elems_hook::index(const std::shared_lock<std::shared_mutex>& lck) const -> index_type {
  assert(lck.owns_lock() && lck.mutex() == &mtx_ref_());
  if (idx_ == no_index) throw std::logic_error("asking for index of unlinked element");
  return idx_;
}

inline auto tree_page_leaf_elems_hook::index(const std::unique_lock<std::shared_mutex>& lck) const -> index_type {
  assert(lck.owns_lock() && lck.mutex() == &mtx_ref_());
  if (idx_ == no_index) throw std::logic_error("asking for index of unlinked element");
  return idx_;
}

inline auto tree_page_leaf_elems_hook::succ(const std::shared_lock<std::shared_mutex>& lck [[maybe_unused]]) const noexcept -> cycle_ptr::cycle_gptr<tree_page_leaf_elems_hook> {
  assert(lck.owns_lock() && lck.mutex() == &mtx_ref_());
  return succ_;
}

inline auto tree_page_leaf_elems_hook::pred(const std::shared_lock<std::shared_mutex>& lck [[maybe_unused]]) const noexcept -> cycle_ptr::cycle_gptr<tree_page_leaf_elems_hook> {
  assert(lck.owns_lock() && lck.mutex() == &mtx_ref_());
  return pred_;
}

inline auto tree_page_leaf_elems_hook::succ(const std::unique_lock<std::shared_mutex>& lck [[maybe_unused]]) const noexcept -> cycle_ptr::cycle_gptr<tree_page_leaf_elems_hook> {
  assert(lck.owns_lock() && lck.mutex() == &mtx_ref_());
  return succ_;
}

inline auto tree_page_leaf_elems_hook::pred(const std::unique_lock<std::shared_mutex>& lck [[maybe_unused]]) const noexcept -> cycle_ptr::cycle_gptr<tree_page_leaf_elems_hook> {
  assert(lck.owns_lock() && lck.mutex() == &mtx_ref_());
  return pred_;
}


template<typename OtherIter, typename = std::enable_if_t<std::is_constructible_v<ElemPtrIter, const OtherIter&>>>
inline tree_page_leaf_elems_iterator<ElemPtrIter>::tree_page_leaf_elems_iterator(const tree_page_leaf_elems_iterator<OtherIter>& other)
: iter_(other.iter_)
{}

template<typename ElemPtrIter>
inline tree_page_leaf_elems_iterator<ElemPtrIter>::tree_page_leaf_elems_iterator(const ElemPtrIter& iter)
    noexcept(std::is_nothrow_copy_constructible_v<ElemPtrIter>)
: iter_(iter)
{}

template<typename ElemPtrIter>
inline tree_page_leaf_elems_iterator<ElemPtrIter>::tree_page_leaf_elems_iterator(ElemPtrIter&& iter)
    noexcept(std::is_nothrow_move_constructible_v<ElemPtrIter>)
: iter_(std::move(iter))
{}

template<typename ElemPtrIter>
inline auto tree_page_leaf_elems_iterator<ElemPtrIter>::operator==(const tree_page_leaf_elems_iterator& y) const noexcept -> bool {
  return iter_ == y.iter_;
}

template<typename ElemPtrIter>
inline auto tree_page_leaf_elems_iterator<ElemPtrIter>::operator!=(const tree_page_leaf_elems_iterator& y) const noexcept -> bool {
  return iter_ != y.iter_;
}

template<typename ElemPtrIter>
inline auto tree_page_leaf_elems_iterator<ElemPtrIter>::operator<(const tree_page_leaf_elems_iterator& y) const noexcept -> bool {
  return iter_ < y.iter_;
}

template<typename ElemPtrIter>
inline auto tree_page_leaf_elems_iterator<ElemPtrIter>::operator>(const tree_page_leaf_elems_iterator& y) const noexcept -> bool {
  return iter_ > y.iter_;
}

template<typename ElemPtrIter>
inline auto tree_page_leaf_elems_iterator<ElemPtrIter>::operator>=(const tree_page_leaf_elems_iterator& y) const noexcept -> bool {
  return iter_ >= y.iter_;
}

template<typename ElemPtrIter>
inline auto tree_page_leaf_elems_iterator<ElemPtrIter>::operator<=(const tree_page_leaf_elems_iterator& y) const noexcept -> bool {
  return iter_ <= y.iter_;
}

template<typename ElemPtrIter>
inline auto tree_page_leaf_elems_iterator<ElemPtrIter>::operator-(const tree_page_leaf_elems_iterator& y) const noexcept -> difference_type {
  return iter_ - y.iter_;
}

template<typename ElemPtrIter>
inline auto tree_page_leaf_elems_iterator<ElemPtrIter>::operator++() noexcept(noexcept(++std::declval<ElemPtrIter>())) -> tree_page_leaf_elems_iterator& {
  ++iter_;
  return *this;
}

template<typename ElemPtrIter>
inline auto tree_page_leaf_elems_iterator<ElemPtrIter>::operator--() noexcept(noexcept(--std::declval<ElemPtrIter>())) -> tree_page_leaf_elems_iterator& {
  --iter_;
  return *this;
}

template<typename ElemPtrIter>
inline auto tree_page_leaf_elems_iterator<ElemPtrIter>::operator++(int) noexcept(noexcept(std::declval<ElemPtrIter>()++)) -> tree_page_leaf_elems_iterator {
  return tree_page_leaf_elems_iterator(iter_++);
}

template<typename ElemPtrIter>
inline auto tree_page_leaf_elems_iterator<ElemPtrIter>::operator--(int) noexcept(noexcept(std::declval<ElemPtrIter>()--)) -> tree_page_leaf_elems_iterator {
  return tree_page_leaf_elems_iterator(iter_--);
}

template<typename ElemPtrIter>
inline auto tree_page_leaf_elems_iterator<ElemPtrIter>::operator+=(difference_type n) noexcept(noexcept(std::declval<ElemPtrIter>() += std::declval<difference_type>())) -> tree_page_leaf_elems_iterator& {
  iter_ += n;
  return *this;
}

template<typename ElemPtrIter>
inline auto tree_page_leaf_elems_iterator<ElemPtrIter>::operator-=(difference_type n) noexcept(noexcept(std::declval<ElemPtrIter>() -= std::declval<difference_type>())) -> tree_page_leaf_elems_iterator& {
  iter_ -= n;
  return *this;
}

template<typename ElemPtrIter>
inline auto tree_page_leaf_elems_iterator<ElemPtrIter>::operator+(difference_type n) const noexcept(noexcept(std::declval<const ElemPtrIter>() + std::declval<difference_type>())) -> tree_page_leaf_elems_iterator {
  return tree_page_leaf_elems_iterator(iter_ + n);
}

template<typename ElemPtrIter>
inline auto tree_page_leaf_elems_iterator<ElemPtrIter>::operator-(difference_type n) const noexcept(noexcept(std::declval<const ElemPtrIter>() - std::declval<difference_type>())) -> tree_page_leaf_elems_iterator {
  return tree_page_leaf_elems_iterator(iter_ - n);
}

template<typename ElemPtrIter>
inline auto tree_page_leaf_elems_iterator<ElemPtrIter>::operator*() const noexcept(noexcept(std::declval<const ElemPtrIter>()->operator*())) -> reference {
  return iter_->operator*();
}

template<typename ElemPtrIter>
inline auto tree_page_leaf_elems_iterator<ElemPtrIter>::operator->() const noexcept(noexcept(std::declval<const ElemPtrIter>()->operator->())) -> pointer {
  return iter_->operator->();
}

template<typename ElemPtrIter>
inline auto operator[](difference_type n) const noexcept(noexcept(std::declval<const ElemPtrIter>()[std::declval<difference_type>()].operator*())) -> reference {
  return iter_[n].operator*();
}

template<typename ElemPtrIter>
inline auto tree_page_leaf_elems_iterator<ElemPtrIter>::get() const noexcept(noexcept(*std::declval<const ElemPtrIter>())) -> cycle_ptr::cycle_gptr<value_type> {
  return *iter_;
}


template<typename Elem, typename Alloc>
inline tree_page_leaf_elems<Elem, Alloc>::tree_page_leaf_elems(cycle_ptr::cycle_base& owner, std::size_t sz, allocator_type allocator = allocator_type())
: vector_(typename elems_vector::allocator_type(owner, allocator))
{
  vector_.reserve(sz);
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::begin() -> iterator {
  return iterator(vector_.begin());
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::end() -> iterator {
  return iterator(vector_.end());
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::begin() const -> const_iterator {
  return const_iterator(vector_.cbegin());
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::end() const -> const_iterator {
  return const_iterator(vector_.cend());
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::cbegin() const -> const_iterator {
  return const_iterator(vector_.cbegin());
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::cend() const -> const_iterator {
  return const_iterator(vector_.cend());
}

template<typename Elem, typename Alloc>
inline void tree_page_leaf_elems<Elem, Alloc>::clear() noexcept {
  invariant_check invariant(*this);

  for (auto& e : vector_) {
    std::unique_lock<std::shared_mutex> lck{ e.mtx_ref_() };
    e.idx_ = tree_page_leaf_elems_hook::no_index;
    e.parent_idx_ = tree_page_leaf_elems_hook::no_index;
  }
  vector_.clear();
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::empty() const noexcept -> bool {
  return vector_.empty();
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::space_avail() const noexcept -> bool {
  return vector_.size() < vector_.capacity();
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::size() const noexcept -> size_type {
  return vector_.size();
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::capacity() const noexcept -> size_type {
  return vector_.capacity();
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::insert(index_type idx, const cycle_ptr::cycle_gptr<Elem>& elem) -> std::pair<iterator, bool> {
  invariant_check invariant(*this);

  if (idx >= vector_.capacity()) throw std::out_of_range("index too large for leaf");
  if (!space_avail()) throw std::length_error("no space available for leaf");

  auto ipos = std::lower_bound(vector_.begin(), vector_.end(), idx, tree_page_leaf_elems_idx_compare_());
  if (ipos != vector_.end() && ipos->index() == idx) return std::make_pair(iterator(ipos), false);

  // Lock predecessor and successor.
  std::unique_lock<std::shared_mutex> pred_lck, succ_lck;
  if (ipos != vector_.begin())
    pred_lck = std::unique_lock<std::shared_mutex>((*std::prev(ipos))->mtx_ref_());
  std::unique_lock<std::shared_mutex> elem_lck(elem->mtx_ref_());
  if (std::next(ipos) != vector_.end())
    succ_lck = std::unique_lock<std::shared_mutex>((*std::next(ipos))->mtx_ref_());

  // Insert the element.
  assert(vector_.size() < vector_.capacity());
  ipos = vector_.emplace(ipos, elem); // Never throws.
  // Assign the index.
  elem->idx_ = idx;
  // Update parent index.
  elem->parent_idx_ = ipos - vector_.begin();
  // Also update parent indices in all elements that got shifted a position.
  std::for_each(
      std::next(ipos), vector_.end(),
      [](auto& e) noexcept {
        ++e.parent_idx_;
      });

  // Fix link with predecessor.
  if (ipos == vector_.begin()) {
    elem->pred_ = nullptr;
  } else {
    const auto& prev = *std::prev(ipos);
    prev->succ_ = elem;
    elem->pred_ = prev;
  }

  // Fix link with successor.
  if (std::next(ipos) == vector_.end()) {
    elem->succ_ = nullptr;
  } else {
    const auto& succ = *std::next(ipos);
    succ->pred_ = elem;
    elem->succ_ = succ;
  }

  return std::make_pair(iterator(ipos), true);
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::erase(const_iterator pos) -> iterator {
  invariant_check invariant(*this);

  const cycle_ptr::cycle_gptr<hook_type> erased_ptr = *pos.iter_;
  cycle_ptr::cycle_gptr<hook_type> pred, succ;
  // Note: locks must be scoped after the pointers (erased_ptr, pred, succ).
  std::unique_lock<std::shared_mutex> pred_lck, e_lck, succ_lck;

  // Find and lock predecessor.
  if (pos.iter_ != vector_.begin()) {
    pred = *std::prev(pos.iter_);
    pred_lck = std::unique_lock<std::shared_mutex>(pred->mtx_ref_());
  }

  // Lock element at pos.
  e_lck = std::unique_lock<std::shared_mutex>(erased_ptr->mtx_ref_());

  // Find and lock successor.
  if (std::next(pos.iter_) != vector.end()) {
    succ = *std::next(pos.iter_);
    succ_lck = std::unique_lock<std::shared_mutex>(succ->mtx_ref_());
  }

  // Invariant validation.
  assert(pred == erased_ptr->pred_);
  assert(succ == erased_ptr->succ_);

  // Remove the element from our list.
  auto new_pos = vector_.erase(pos.iter_);

  // Clear indices.
  erased_ptr->idx_ = hook_type::no_index;
  erased_ptr->parent_idx_ = hook_type::no_index;

  // Clear link pointers.
  erased_ptr->pred_ = nullptr;
  erased_ptr->succ_ = nullptr;
  if (pred != nullptr) pred->succ_ = succ;
  if (succ != nullptr) succ->pred_ = pred;

  // Update parent indices for elements that got shifted a position.
  std::for_each(
      new_pos, vector_.end(),
      [](auto& e) noexcept {
        --e.parent_idx_;
      });

  return iterator(new_pos);
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::insert_and_shift_before(tx_op_collection& ops, const_iterator pos, cycle_ptr::cycle_gptr<Elem> elem)
-> std::tuple<iterator, bool, const_iterator, const_iterator> {
  invariant_check invariant(*this);

  if (!space_avail()) throw std::length_error("no space available for leaf");

  index_type pos_idx;
  if (pos.iter_ == vector_.end()) {
    index_type pos_idx = (vector_.empty() ? 0u : vector_.back().index() + 1u);

    // Special case:
    // if there is space at the end available
    // just append and call it a day.
    if (pos_idx < vector_.capacity()) {
      std::unique_lock<std::shared_mutex> pred_lck;
      if (!vector_.empty()) pred_lck = std::unique_lock<std::shared_mutex>(vector_.back()->mtx_ref_());
      std::unique_lock<std::shared_mutex> lck{ elem->mtx_ref_() };

      if (!vector_.empty()) {
        elem->pred_ = vector_.back();
        vector_.back()->succ_ = elem;
      } else {
        elem->pred_ = nullptr;
      }
      elem->succ_ = nullptr;

      vector_.emplace_back(elem);
      elem->idx_ = pos_idx;
      elem->parent_idx_ = vector_.size() - 1u;

      // Release the lock, because if the undo operation executes immediately, it'll grab it.
      if (pred_lck.owns_lock()) pred_lck.unlock();
      lck.unlock();

      // Create the undo operation.
      ops.on_rollback(
          [elem, this]() {
            invariant_check invariant(*this);

            std::unique_lock<std::shared_mutex> pred_lck;
            if (!vector_.empty())
              pred_lck = std::unique_lock<std::shared_mutex>(vector_.end()[-2]->mtx_ref_());
            std::unique_lock<std::shared_mutex> lck{ elem->mtx_ref_() };
            assert(vector_.back() == elem);

            // Unlink the element.
            vector_.pop_back();

            // Fix links.
            if (!vector_.empty()) vector_.back()->succ_ = nullptr;

            // Clear data on elem.
            elem->idx_ = hook_type::no_index;
            elem->parent_idx_ = hook_type::no_index;
            elem->pred_ = nullptr;
            elem->succ_ = nullptr;
          });

      return std::make_tuple(
          iterator(std::prev(vector_.end())),
          true,
          const_iterator(std::prev(vector_.end())),
          const_iterator(vector_.end()));
    }
  } else {
    pos_idx = pos->index();
  }

  // Search forward for an index gap we can use.
  if (pos.iter_ != vector_.end()) {
    auto iter_pred = pos.iter_;
    auto iter = std::next(pos.iter_);
    while (iter != vector_.end()) {
      if (iter->index() > iter_pred->index() + 1u) goto forward_found;

      ++iter;
      ++iter_pred;
    }
    if (vector_.capacity() > iter_pred->index() + 1u) goto forward_found;
    goto forward_failed;

forward_found:
    const auto len = iter - pos;

    // Lock everything that will have its index modified.
    std::vector<std::unique_lock<std::shared_mutex>> lcks;
    if (pos != vector_.begin()) lcks.emplace_back(pred->mtx_ref_());
    lcks.emplace_back(elem->mtx_ref_());

    // Fetch the indices for the inserted element.
    // (pos is moved out of the way.)
    const auto ins_idx = pos->index(lcks.back());
    const auto ins_parent_idx = pos->parent_idx_;

    std::for_each_n(pos.iter_, len,
        [&lcks](const auto& ptr) {
          lcks.emplace_back(ptr->mtx_ref_());
        });

    // Insert the element.
    auto ins_pos = vector_.emplace(pos.iter_, elem);

    // Update indices.
    std::for_each_n(std::next(ins_pos), len,
        [](const auto& ptr) {
          ++ptr->idx_;
          ++ptr->parent_idx_;
        });
    elem->idx_ = ins_idx;
    elem->parent_idx_ = ins_parent_idx;

    // Release locks.
    // The on_rollback, if it is invoked immediately, may grab those locks.
    lcks.clear();

    // Create the on_rollback operation.
    ops.on_rollback(
        [elem, this, ins_parent_idx, len]() noexcept {
          invariant_check invariant(*this);

          // Reacquire all the locks.
          std::vector<std::unique_lock<std::shared_mutex>> lcks;
          if (ins_parent_idx > 0) lcks.emplace_back(vector_[ins_parent_idx - 1u].get_mtx_());
          std::for_each_n(vector_.begin() + ins_parent_idx, len + 1u,
              [&lcks](const auto& ptr) {
                lcks.emplace_back(ptr->get_mtx_());
              });
          assert(vector_[ins_parent_idx] == elem);

          // Unlink the element.
          auto pos = vector_.erase(vector_.begin() + ins_parent_idx);
          // Fix predecessor and successor links.
          if (pos == vector_.end()) {
            if (pos != vector_.begin()) pos[-1]->succ_ = nullptr;
          } else {
            if (pos != vector_.begin()) {
              pos[-1]->succ_ = *pos;
              (*pos)->pred_ = pos[-1];
            } else {
              (*pos)->pred_ = nullptr;
            }
          }

          // Undo update indices.
          std::for_each_n(pos, len,
              [](const auto& ptr) {
                --ptr->idx_;
                --ptr->parent_idx_;
              });
          // Reset records on unlinked element.
          elem->idx_ = hook_type::no_index;
          elem->parent_idx_ = hook_type::no_index;
          elem->pred_ = nullptr;
          elem->succ_ = nullptr;
        });

    return std::make_tuple(
        iterator(ins_pos),
        true,
        const_iterator(ins_pos),
        const_iterator(std::next(ins_pos) + n));
  }
forward_failed:

  // Search backward for an index gap we can use.
  assert(false); // XXX actually implement this

  ... // XXX implement
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::iterator_to(const Elem& e_ref) -> iterator {
  const hook_type& e = e_ref;
  const index_type idx = e.parent_idx_;

  if (idx == tree_page_leaf_elems_hook::no_index) throw std::range_error("element not in any collection");
  if (idx >= vector_.size()) throw std::range_error("element not in this collection");
  if (vector_[idx].get() != &e) throw std::range_error("element not in this collection");

  return begin() + idx;
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::iterator_to(const Elem& e_ref) const -> const_iterator {
  const hook_type& e = e_ref;
  const index_type idx = e.parent_idx_;

  if (idx == tree_page_leaf_elems_hook::no_index) throw std::range_error("element not in any collection");
  if (idx >= vector_.size()) throw std::range_error("element not in this collection");
  if (vector_[idx].get() != &e) throw std::range_error("element not in this collection");

  return begin() + idx;
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::operator[](index_type idx) -> cycle_ptr::cycle_gptr<Elem> {
  assert(idx < vector_.capacity());

  auto pos = std::lower_bound(vector_.begin(), vector_.end(), idx, tree_page_leaf_elems_idx_compare_());
  if (pos == vector_.end() || pos->idx_ != idx) return nullptr;
  return *pos;
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::operator[](index_type idx) const -> cycle_ptr::cycle_gptr<const Elem> {
  assert(idx < vector_.capacity());

  auto pos = std::lower_bound(vector_.begin(), vector_.end(), idx, tree_page_leaf_elems_idx_compare_());
  if (pos == vector_.end() || pos->idx_ != idx) return nullptr;
  return *pos;
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::front() -> cycle_ptr::cycle_gptr<Elem> {
  return vector_.front();
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::front() const -> cycle_ptr::cycle_gptr<const Elem> {
  return vector_.front();
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::back() -> cycle_ptr::cycle_gptr<Elem> {
  return vector_.back();
}

template<typename Elem, typename Alloc>
inline auto tree_page_leaf_elems<Elem, Alloc>::back() const -> cycle_ptr::cycle_gptr<const Elem> {
  return vector_.back();
}

template<typename Elem, typename Alloc>
inline void tree_page_leaf_elems<Elem, Alloc>::splice(tx_op_collection& ops, tree_page_leaf_elems& list) {
  if (!vector_.empty()) {
    splice(list, list.cbegin(), list.cend());
    return;
  }

  using std::swap;

  invariant_check invariant(*this);
  invariant_check list_invariant(list);
  tx_op_collection local_ops(ops.get_allocator());

  // Assertions trip on undefined behaviour.
  assert(&list != this);
  assert(get_allocator() == list.get_allocator());
  assert(vector_.size() == 0u);

  if (list.vector_.size() > vector_.capacity())
    throw std::lenth_error("insufficient space for splice");

  if (list.vector_.capacity() == vector_.capacity()
      && list.vector_.get_allocator() == vector_.get_allocator()) {
    swap(vector_, list.vector_);
    local_ops.on_rollback(
        [v1=&list.vector_, v2=&vector_]() {
          swap(v1, v2);
        });
  } else {
    std::copy(list.vector_.cbegin(), list.vector_.cend(), std::back_inserter(vector_));
    local_ops.on_rollback([this]() { vector_.clear(); });

    list.vector_.clear();
    local_ops.on_rollback(
        [list_ptr=&list, this]() {
          std::copy(vector_.cbegin(), vector.cend(), std::back_inserter(list_ptr->vector_));
        });
  }

  ops += std::move(local_ops);
}

template<typename Elem, typename Alloc>
inline void tree_page_leaf_elems<Elem, Alloc>::splice(tx_op_collection& ops, tree_page_leaf_elems&& list) {
  splice(ops, list);
}

template<typename Elem, typename Alloc>
inline void tree_page_leaf_elems<Elem, Alloc>::splice(tx_op_collection& ops, tree_page_leaf_elems& list, const_iterator it) {
  splice(ops, list, it, std::next(it));
}

template<typename Elem, typename Alloc>
inline void tree_page_leaf_elems<Elem, Alloc>::splice(tx_op_collection& ops, tree_page_leaf_elems&& list, const_iterator it) {
  splice(ops, std::move(list), it, std::next(it));
}

template<typename Elem, typename Alloc>
inline void tree_page_leaf_elems<Elem, Alloc>::splice(tx_op_collection& ops, tree_page_leaf_elems& list, const_iterator first, const_iterator last) {
  invariant_check invariant(*this);
  invariant_check list_invariant(list);
  tx_op_collection local_ops(ops.get_allocator());

  // Assertions on undefined behaviour in parameters.
  assert(list.cbegin() <= first);
  assert(first <= last);
  assert(last <= list.cend());
  // Assertions trip on undefined behaviour.
  assert(&list != this);

  const size_type n = last - first;
  assert(std::next(first, n) == last);

  if (vector_.capacity() - vector_.size() < n)
    throw std::length_error("insufficient space for splice");

  // Save the list of elements.
  using elem_list_type = std::vector<
      std::pair<cycle_ptr::cycle_gptr<Elem>, index_type>,
      std::allocator_traits<tx_op_collection::allocator_type>::rebind_alloc<std::pair<cycle_ptr::cycle_gptr<Elem>, index_type>>>;
  const auto elem_list = std::allocate_shared<elem_list_type>(ops.get_allocator(), ops.get_allocator());
  elem_list->reserve(n);
  std::transform(
      first.iter_, last.iter_,
      std::back_inserter(*elem_list),
      [](const auto& elem_ptr) {
        return elem_list_type::value_type(elem_ptr, elem_ptr->index());
      });

  // ...

  ops += std::move(local_ops);
}

template<typename Elem, typename Alloc>
inline void tree_page_leaf_elems<Elem, Alloc>::splice(tx_op_collection& ops, tree_page_leaf_elems&& list, const_iterator first, const_iterator last) {
  splice(ops, list, first, last);
}

inline auto verify_invariant(size_type expected_capacity) const noexcept {
#ifndef NDEBUG
  assert(vector_.capacity() == expected_capacity);

  if (vector_.empty()) return;

  // We don't have null pointers.
  assert(std::none_of(vector_.begin(), vector_.end(),
          [](const auto& ptr) {
            ptr != nullptr;
          }));
  // The parent indices match with the element positions.
  for (size_type i = 0; i < vector_.size(); ++i)
    assert(vector_[i]->parent_idx_ == i);
  // The position indices are sorted.
  for (size_type i = 1; i < vector_.size(); ++i)
    assert(vector_[i - 1]->index() < vector[i]->index());
  // The forward link is correct.
  for (size_type i = 1; i < vector_.size(); ++i) {
    std::shared_lock<std::shared_mutex> lck{ vector_[i - 1]->mtx_ref_() };
    assert(vector_[i - 1]->succ_ == vector_[i]);
  }
  // The backward link is correct.
  for (size_type i = 1; i < vector_.size(); ++i) {
    std::shared_lock<std::shared_mutex> lck{ vector_[i - 1]->mtx_ref_() };
    assert(vector_[i]->pred_ == vector_[i - 1]);
  }

  // The last element has no successor.
  {
    std::shared_lock<std::shared_mutex> lck{ vector_.back()->mtx_ref_() };
    assert(vector_.back().succ_ == nullptr);
  }
  // The first element has no predessor.
  {
    std::shared_lock<std::shared_mutex> lck{ vector_.front()->mtx_ref_() };
    assert(vector_.front().pred_ == nullptr);
  }
#endif // !NDEBUG
}


template<typename X, typename Y>
constexpr auto tree_page_leaf_elems_idx_compare_::operator()(const X& x, const Y& y) const noexcept -> bool {
  return get_idx_(x) < get_idx_(y);
}

private:
constexpr auto tree_page_leaf_elems_idx_compare_::get_idx_(index_type idx) noexcept -> index_type {
  return idx;
}

inline auto tree_page_leaf_elems_idx_compare_::get_idx_(const tree_page_leaf_elems_hook& hook) noexcept -> index_type {
  return hook.idx_;
}

inline auto tree_page_leaf_elems_idx_compare_::get_idx_(const cycle_ptr::cycle_member_ptr<tree_page_leaf_elems_hook>& hook) noexcept -> index_type {
  return get_idx_(*hook);
}

inline auto tree_page_leaf_elems_idx_compare_::get_idx_(const cycle_ptr::cycle_member_ptr<const tree_page_leaf_elems_hook>& hook) noexcept -> index_type {
  return get_idx_(*hook);
}

inline auto tree_page_leaf_elems_idx_compare_::get_idx_(const cycle_ptr::cycle_gptr<tree_page_leaf_elems_hook>& hook) noexcept -> index_type {
  return get_idx_(*hook);
}

inline auto tree_page_leaf_elems_idx_compare_::get_idx_(const cycle_ptr::cycle_gptr<const tree_page_leaf_elems_hook>& hook) noexcept -> index_type {
  return get_idx_(*hook);
}


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_TREE_PAGE_LEAF_ELEMS_H */
