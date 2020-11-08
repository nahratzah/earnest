#include <earnest/detail/tree/tree.h>
#include <earnest/detail/tree/leaf_iterator.h>
#include <earnest/detail/tree/ops.h>
#include <earnest/detail/locked_ptr.h>
#include <shared_mutex>

namespace earnest::detail::tree {


basic_tree::~basic_tree() noexcept = default;


basic_tree::tx_object::~tx_object() noexcept = default;

auto basic_tree::tx_object::empty() const -> bool {
  std::shared_lock<basic_tree> lck(*tree_);
  return tree_->root_page_ == 0u;
}

auto basic_tree::tx_object::begin() const -> leaf_iterator {
  return ops::begin(shared_lock_ptr<cycle_ptr::cycle_gptr<const basic_tree>>(tree_));
}

auto basic_tree::tx_object::end() const -> leaf_iterator {
  return ops::end(shared_lock_ptr<cycle_ptr::cycle_gptr<const basic_tree>>(tree_));
}

auto basic_tree::tx_object::rbegin() const -> reverse_leaf_iterator {
  return ops::rbegin(shared_lock_ptr<cycle_ptr::cycle_gptr<const basic_tree>>(tree_));
}

auto basic_tree::tx_object::rend() const -> reverse_leaf_iterator {
  return ops::rend(shared_lock_ptr<cycle_ptr::cycle_gptr<const basic_tree>>(tree_));
}


} /* namespace earnest::detail::tree */
