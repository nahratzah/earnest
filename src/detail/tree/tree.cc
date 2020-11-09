#include <earnest/detail/tree/tree.h>
#include <earnest/detail/tree/leaf_iterator.h>
#include <earnest/detail/tree/ops.h>
#include <earnest/detail/locked_ptr.h>
#include <shared_mutex>

namespace earnest::detail::tree {


basic_tree::~basic_tree() noexcept = default;

auto basic_tree::has_pages() const -> bool {
  return root_page_ != 0u;
}


basic_tx_aware_tree::~basic_tx_aware_tree() noexcept = default;


basic_tx_aware_tree::tx_object::~tx_object() noexcept = default;

auto basic_tx_aware_tree::tx_object::empty() const -> bool {
  basic_tx_aware_tree::shared_lock_ptr locked_tree(tree_);
  return !locked_tree->has_pages()
      || begin_(locked_tree).is_sentinel();
}

auto basic_tx_aware_tree::tx_object::begin() const -> iterator {
  return begin_(basic_tx_aware_tree::shared_lock_ptr(tree_));
}

auto basic_tx_aware_tree::tx_object::end() const -> iterator {
  return end_(basic_tx_aware_tree::shared_lock_ptr(tree_));
}


} /* namespace earnest::detail::tree */
