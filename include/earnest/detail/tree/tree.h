#ifndef EARNEST_DETAIL_TREE_TREE_H
#define EARNEST_DETAIL_TREE_TREE_H

#include <memory>
#include <shared_mutex>

#include <earnest/db.h>
#include <earnest/detail/export_.h>
#include <earnest/detail/db_cache.h>
#include <earnest/detail/locked_ptr.h>

#include <earnest/detail/tree/fwd.h>
#include <earnest/detail/tree/leaf_iterator.h>
#include <earnest/detail/tree/loader_impl.h>

namespace earnest::detail::tree {


class earnest_export_ basic_tree
: public db_cache::domain,
  public db::db_obj
{
  friend ops;

  public:
  using size_type = tree_size_type;

  explicit basic_tree(std::shared_ptr<class db> db, std::shared_ptr<const struct cfg> cfg, std::shared_ptr<const class loader> loader);
  ~basic_tree() noexcept override;

  void lock() { mtx_.lock(); }
  auto try_lock() -> bool { return mtx_.try_lock(); }
  void unlock() { mtx_.unlock(); }
  void lock_shared() const { mtx_.lock_shared(); }
  auto try_lock_shared() const -> bool { return mtx_.try_lock_shared(); }
  void unlock_shared() const { mtx_.unlock_shared(); }

  const std::shared_ptr<const cfg> cfg;
  const std::shared_ptr<const class loader> loader;

  protected:
  auto has_pages() const -> bool; // Must be called with mtx_ held for share or exclusive.

  private:
  std::uint64_t root_page_ = 0;
  mutable std::shared_mutex mtx_;
};


class basic_tx_aware_tree
: public basic_tree,
  public db_cache::cache_obj
{
  public:
  using shared_lock_ptr = earnest::detail::shared_lock_ptr<cycle_ptr::cycle_gptr<const basic_tx_aware_tree>>;
  using unique_lock_ptr = earnest::detail::unique_lock_ptr<cycle_ptr::cycle_gptr<basic_tx_aware_tree>>;

  class tx_object;

  using basic_tree::basic_tree;
  ~basic_tx_aware_tree() noexcept override;
};


class basic_tx_aware_tree::tx_object
: public db::transaction_obj,
  public cycle_ptr::cycle_base
{
  private:
  template<typename LeafIterator> class iterator_;

  public:
  using iterator = iterator_<leaf_iterator>;
  using reverse_iterator = iterator_<reverse_leaf_iterator>;

  explicit tx_object(db::transaction& tx, cycle_ptr::cycle_gptr<basic_tx_aware_tree> tree);
  ~tx_object() noexcept override;

  auto empty() const -> bool;
  auto begin() const -> iterator;
  auto end() const -> iterator;
  auto rbegin() const -> reverse_iterator;
  auto rend() const -> reverse_iterator;

  private:
  auto begin_(const basic_tx_aware_tree::shared_lock_ptr& tree) const -> iterator;
  auto end_(const basic_tx_aware_tree::shared_lock_ptr& tree) const -> iterator;
  auto rbegin_(const basic_tx_aware_tree::shared_lock_ptr& tree) const -> reverse_iterator;
  auto rend_(const basic_tx_aware_tree::shared_lock_ptr& tree) const -> reverse_iterator;

  db::transaction& tx;
  const cycle_ptr::cycle_member_ptr<basic_tx_aware_tree> tree_;
};


template<typename LeafIterator>
class basic_tx_aware_tree::tx_object::iterator_ {
  friend tx_object;
  template<typename> friend class basic_tx_aware_tree::tx_object::iterator_;

  public:
  using iterator_category = typename LeafIterator::iterator_category;
  using difference_type = typename LeafIterator::difference_type;
  using value_type = tx_aware_value_type;
  using pointer = const tx_aware_value_type*;
  using reference = const tx_aware_value_type&;

  constexpr iterator_() noexcept = default;

  template<typename OtherLeafIterator>
  constexpr iterator_(const iterator_<OtherLeafIterator>& y) noexcept;
  template<typename OtherLeafIterator>
  constexpr iterator_(iterator_<OtherLeafIterator>&& y) noexcept;

  private:
  iterator_(cycle_ptr::cycle_gptr<const tx_object> txo, const LeafIterator& iter, bool skip_seek = false) noexcept;
  iterator_(cycle_ptr::cycle_gptr<const tx_object> txo, LeafIterator&& iter, bool skip_seek = false) noexcept;

  public:
  auto operator++(int) -> iterator_;
  auto operator--(int) -> iterator_;
  auto operator++() -> iterator_&;
  auto operator--() -> iterator_&;

  auto operator==(const iterator_& y) const -> bool;
  auto operator!=(const iterator_& y) const -> bool;

  auto operator*() const -> reference;
  auto operator->() const -> pointer;
  auto ptr() const -> cycle_ptr::cycle_gptr<const tx_aware_value_type>;
  auto is_sentinel() const -> bool;

  private:
  void seek_forward_until_valid_();
  void seek_backward_until_valid_();

  cycle_ptr::cycle_gptr<const tx_object> txo_;
  LeafIterator iter_;
};


template<typename KeyType, typename ValueType, typename... Augments>
class tx_aware_tree
: public db::db_obj,
  public cycle_ptr::cycle_base
{
  private:
  using key_type = typename tx_aware_loader<KeyType, ValueType, Augments...>::key_type;
  using value_type = typename tx_aware_loader<KeyType, ValueType, Augments...>::value_type;
  using augment_type = typename tx_aware_loader<KeyType, ValueType, Augments...>::augment_type;

  public:
  class tx_object;

  tx_aware_tree(std::shared_ptr<class db> db, txfile::transaction::offset_type offset, std::shared_ptr<const tx_aware_loader<KeyType, ValueType, Augments...>> loader);

  private:
  cycle_ptr::cycle_member_ptr<basic_tx_aware_tree> impl_;
};


template<typename ValueType, typename Iterator>
class tx_aware_tree_iterator {
  public:
  using iterator_category = typename Iterator::iterator_category;
  using difference_type = typename Iterator::difference_type;
  using value_type = ValueType;
  using pointer = const ValueType*;
  using reference = const ValueType&;
  template<typename, typename> friend class tx_aware_tree_iterator;

  constexpr tx_aware_tree_iterator() noexcept = default;

  tx_aware_tree_iterator(const tx_aware_tree_iterator<ValueType, leaf_iterator>& y) noexcept;
  tx_aware_tree_iterator(const tx_aware_tree_iterator<ValueType, reverse_leaf_iterator>& y) noexcept;
  tx_aware_tree_iterator(tx_aware_tree_iterator<ValueType, leaf_iterator>&& y) noexcept;
  tx_aware_tree_iterator(tx_aware_tree_iterator<ValueType, reverse_leaf_iterator>&& y) noexcept;

  private:
  explicit tx_aware_tree_iterator(const Iterator& iter) noexcept;
  explicit tx_aware_tree_iterator(Iterator&& iter) noexcept;

  public:
  auto operator++(int) -> tx_aware_tree_iterator;
  auto operator--(int) -> tx_aware_tree_iterator;
  auto operator++() -> tx_aware_tree_iterator&;
  auto operator--() -> tx_aware_tree_iterator&;

  auto operator==(const tx_aware_tree_iterator& y) const -> bool;
  auto operator!=(const tx_aware_tree_iterator& y) const -> bool;

  auto operator*() const -> reference;
  auto operator->() const -> pointer;
  auto ptr() const -> cycle_ptr::cycle_gptr<const tx_aware_value_type_impl<ValueType>>;

  private:
  Iterator iter_;
};


template<typename KeyType, typename ValueType, typename... Augments>
class tx_aware_tree<KeyType, ValueType, Augments...>::tx_object
: public db::transaction_obj,
  public cycle_ptr::cycle_base
{
  public:
  using size_type = tree_size_type;
  using iterator = tx_aware_tree_iterator<ValueType, basic_tx_aware_tree::tx_object::iterator>;
  using reverse_iterator = tx_aware_tree_iterator<ValueType, basic_tx_aware_tree::tx_object::reverse_iterator>;

  tx_object(db::transaction& tx, cycle_ptr::cycle_gptr<tx_aware_tree> tree);

  auto empty() const -> bool;
  auto begin() const -> iterator;
  auto end() const -> iterator;
  auto rbegin() const -> reverse_iterator;
  auto rend() const -> reverse_iterator;

  private:
  const cycle_ptr::cycle_member_ptr<basic_tx_aware_tree::tx_object> impl_;
};


} /* namespace earnest::detail::tree */

#include "tree-inl.h"

#endif /* EARNEST_DETAIL_TREE_TREE_H */
