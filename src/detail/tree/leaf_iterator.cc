#include <earnest/detail/tree/leaf_iterator.h>
#include <earnest/detail/tree/leaf.h>
#include <earnest/detail/tree/page.h>
#include <earnest/detail/tree/value_type.h>
#include <boost/polymorphic_pointer_cast.hpp>

#include <cassert>
#include <iterator>
#include <vector>

namespace earnest::detail::tree {


auto leaf_iterator::operator++() -> leaf_iterator& {
  assert(value_ptr_ != nullptr);
  assert(loader_ != nullptr);

  // Helper function.
  const auto seek_forward = [](value_type::shared_lock_ptr& cur_lck) -> value_type::shared_lock_ptr {
    value_type::shared_lock_ptr succ;
    while (cur_lck->succ_ != nullptr && !succ.owns_lock()) {
      succ = value_type::shared_lock_ptr(cur_lck->succ_);
      if (succ->is_never_visible())
        cur_lck = std::move(succ);
    }
    return succ;
  };

  value_type::shared_lock_ptr cur_lck(value_ptr_);

  // Try to use the successor pointers in the page.
  {
    value_type::shared_lock_ptr succ = seek_forward(cur_lck);
    if (succ) {
      value_ptr_ = succ.mutex();
      return *this;
    }
  }

  leaf::shared_lock_ptr parent_page;
  bool relocked;
  std::tie(parent_page, relocked) = value_type::parent_page_for_read(cur_lck);

  // Retest successor pointer.
  if (relocked) {
    value_type::shared_lock_ptr succ = seek_forward(cur_lck);
    if (succ) {
      value_ptr_ = succ.mutex();
      return *this;
    }
  }

  cur_lck.unlock(); // We no longer need this lock.

  for (;;) {
    // If there are no more pages, install the sentinel.
    if (parent_page->successor_off_ == 0) {
      value_ptr_ = parent_page->sentinel_;
      return *this;
    }

    // Load the next page.
    parent_page = leaf::shared_lock_ptr(
        boost::polymorphic_pointer_downcast<leaf>(
            abstract_page::load_from_disk(parent_page->successor_off_, *loader_)));

    // Seek in the next page.
    cur_lck = value_type::shared_lock_ptr(parent_page->sentinel_);
    value_type::shared_lock_ptr succ = seek_forward(cur_lck);
    cur_lck.unlock();
    if (succ) {
      value_ptr_ = succ.mutex();
      return *this;
    }
  }
}

auto leaf_iterator::operator--() -> leaf_iterator& {
  assert(value_ptr_ != nullptr);
  assert(loader_ != nullptr);

  // Seeking backward, using pointer-and-locks logic.
  const auto seek_backward = [](const cycle_ptr::cycle_gptr<const value_type>& start) -> std::tuple<value_type::shared_lock_ptr, value_type::shared_lock_ptr> {
restart:
    std::vector<value_type::shared_lock_ptr> locks;
    locks.emplace_back(start);

    while (locks.back()->pred_ != nullptr) {
      locks.emplace_back(locks.back()->pred_, std::try_to_lock);
      if (!locks.back().owns_lock()) {
        std::for_each(locks.begin(), std::prev(locks.end()), [](auto& lck) { lck.unlock(); });
        for (auto lock_iter = std::prev(locks.end()); lock_iter != locks.begin(); --lock_iter) {
          lock_iter->lock();

          if (lock_iter[-1].mutex() != (*lock_iter)->pred_) goto restart;
        }
      }

      if (!locks.back()->is_never_visible())
        return std::make_tuple(std::move(locks.front()), std::move(locks.back()));
    }

    return std::make_tuple(std::move(locks.front()), value_type::shared_lock_ptr());
  };

  // Seeking backward, taking advantage that the parent page of the elements is locked.
  const auto seek_backwards_in_locked_page = [](cycle_ptr::cycle_gptr<const value_type> start) -> value_type::shared_lock_ptr {
    value_type::shared_lock_ptr cur_lck(start);
    while (cur_lck->pred_ != nullptr) {
      // We don't need to be careful with locks, because we lock the parent page.
      // Note that we can't do a pure replacement, because seeking backwards always
      // involves deadlock resolution.
      cycle_ptr::cycle_gptr<const value_type> cpred = cur_lck->pred_;
      cur_lck.unlock();
      cur_lck = value_type::shared_lock_ptr(std::move(cpred));
      if (!cur_lck->is_never_visible()) return cur_lck;
    }
    return {};
  };

  // Seek backwards in this page.
  value_type::shared_lock_ptr cur_lck;
  {
    value_type::shared_lock_ptr pred;
    std::tie(cur_lck, pred) = seek_backward(value_ptr_);
    if (pred) {
      value_ptr_ = pred.mutex();
      return *this;
    }
  }

  leaf::shared_lock_ptr parent_page;
  bool relocked;
  std::tie(parent_page, relocked) = value_type::parent_page_for_read(cur_lck);
  cur_lck.unlock();

  // Recheck the predecessors of the element, as we had to relock.
  if (relocked) {
    cur_lck = seek_backwards_in_locked_page(value_ptr_);
    if (cur_lck) {
      value_ptr_ = cur_lck.mutex();
      return *this;
    }
  }

  for (;;) {
    // If there are no more pages, install the sentinel.
    if (parent_page->predecessor_off_ == 0) {
      value_ptr_ = parent_page->sentinel_;
      return *this;
    }

    // Load the previous page.
    parent_page = leaf::shared_lock_ptr(
        boost::polymorphic_pointer_downcast<leaf>(
            abstract_page::load_from_disk(parent_page->predecessor_off_, *loader_)));

    // Seek in the next page.
    cur_lck = seek_backwards_in_locked_page(parent_page->sentinel_);
    if (cur_lck) {
      value_ptr_ = cur_lck.mutex();
      return *this;
    }
    cur_lck.unlock();
  }
}


} /* namespace earnest::detail::tree */
