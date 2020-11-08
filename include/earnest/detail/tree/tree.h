#ifndef EARNEST_DETAIL_TREE_TREE_H
#define EARNEST_DETAIL_TREE_TREE_H

#include <memory>
#include <shared_mutex>

#include <earnest/db.h>
#include <earnest/detail/export_.h>
#include <earnest/detail/db_cache.h>

#include <earnest/detail/tree/fwd.h>

namespace earnest::detail::tree {


class earnest_export_ basic_tree
: public db_cache::domain,
  public db::db_obj,
  public db_cache::cache_obj
{
  friend ops;

  public:
  class tx_object;

  explicit basic_tree(std::shared_ptr<class db> db, std::shared_ptr<const struct cfg> cfg, std::shared_ptr<const class loader> loader);
  virtual ~basic_tree() noexcept;

  void lock() { mtx_.lock(); }
  auto try_lock() -> bool { return mtx_.try_lock(); }
  void unlock() { mtx_.unlock(); }
  void lock_shared() const { mtx_.lock_shared(); }
  auto try_lock_shared() const -> bool { return mtx_.try_lock_shared(); }
  void unlock_shared() const { mtx_.unlock_shared(); }

  const std::shared_ptr<const cfg> cfg;
  const std::shared_ptr<const class loader> loader;

  private:
  std::uint64_t root_page_ = 0;
  mutable std::shared_mutex mtx_;
};


class earnest_export_ basic_tree::tx_object
: public db::transaction_obj,
  public cycle_ptr::cycle_base
{
  public:
  explicit tx_object(db::transaction& tx, cycle_ptr::cycle_gptr<basic_tree> tree);
  ~tx_object() noexcept override;

  auto empty() const -> bool;
  auto begin() const -> leaf_iterator;
  auto end() const -> leaf_iterator;
  auto rbegin() const -> reverse_leaf_iterator;
  auto rend() const -> reverse_leaf_iterator;

  private:
  db::transaction& tx;
  const cycle_ptr::cycle_member_ptr<basic_tree> tree_;
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
  cycle_ptr::cycle_member_ptr<basic_tree> impl_;
};


template<typename ValueType>
class tx_aware_tree_iterator; // XXX


template<typename KeyType, typename ValueType, typename... Augments>
class tx_aware_tree<KeyType, ValueType, Augments...>::tx_object
: public db::transaction_obj,
  public cycle_ptr::cycle_base
{
  public:
  using iterator = tx_aware_tree_iterator<ValueType>;

  tx_object(db::transaction& tx, cycle_ptr::cycle_gptr<tx_aware_tree> tree);

  auto empty() const -> bool;
  auto begin() const -> iterator;
  auto end() const -> iterator;

  private:
  const cycle_ptr::cycle_member_ptr<basic_tree::tx_object> impl_;
};


} /* namespace earnest::detail::tree */

#include "tree-inl.h"

#endif /* EARNEST_DETAIL_TREE_TREE_H */
