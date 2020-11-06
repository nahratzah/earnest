#ifndef EARNEST_DETAIL_TREE_TREE_H
#define EARNEST_DETAIL_TREE_TREE_H

#include <memory>

#include <earnest/db.h>
#include <earnest/detail/export_.h>
#include <earnest/detail/db_cache.h>

#include <earnest/detail/tree/fwd.h>

namespace earnest::detail::tree {


class earnest_export_ basic_tree
: public db_cache::domain,
  public db::db_obj
{
  friend ops;

  public:
  class tx_object;

  explicit basic_tree(std::shared_ptr<class db> db, std::shared_ptr<const struct cfg> cfg, std::shared_ptr<const class loader> loader);
  virtual ~basic_tree() noexcept;

  const std::shared_ptr<const cfg> cfg;
  const std::shared_ptr<const class loader> loader;

  private:
  std::uint64_t root_page_ = 0;
};


class earnest_export_ basic_tree::tx_object
: public db::transaction_obj,
  public cycle_ptr::cycle_base
{
  public:
  explicit tx_object(db::transaction& tx, cycle_ptr::cycle_gptr<basic_tree> tree);
  ~tx_object() noexcept override;

  private:
  db::transaction& tx;
  cycle_ptr::cycle_member_ptr<basic_tree> tree_;
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


template<typename KeyType, typename ValueType, typename... Augments>
class tx_aware_tree<KeyType, ValueType, Augments...>::tx_object
: public db::transaction_obj,
  public cycle_ptr::cycle_base
{
  public:
  tx_object(db::transaction& tx, cycle_ptr::cycle_gptr<tx_aware_tree> tree);

  private:
  cycle_ptr::cycle_member_ptr<basic_tree::tx_object> impl_;
};


} /* namespace earnest::detail::tree */

#include "tree-inl.h"

#endif /* EARNEST_DETAIL_TREE_TREE_H */
