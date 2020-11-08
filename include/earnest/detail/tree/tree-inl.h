#ifndef EARNEST_DETAIL_TREE_TREE_INL_H
#define EARNEST_DETAIL_TREE_TREE_INL_H

#include <utility>

namespace earnest::detail::tree {


inline basic_tree::basic_tree(
    std::shared_ptr<class db> db,
    std::shared_ptr<const struct cfg> cfg,
    std::shared_ptr<const class loader> loader)
: db::db_obj(std::move(db)),
  cfg(std::move(cfg)),
  loader(std::move(loader))
{}


inline basic_tree::tx_object::tx_object(
    db::transaction& tx,
    cycle_ptr::cycle_gptr<basic_tree> tree)
: tx(tx),
  tree_(*this, std::move(tree))
{}


template<typename KeyType, typename ValueType, typename... Augments>
inline tx_aware_tree<KeyType, ValueType, Augments...>::tx_aware_tree(std::shared_ptr<class db> db, txfile::transaction::offset_type offset, std::shared_ptr<const tx_aware_loader<KeyType, ValueType, Augments...>> loader)
: impl_(*this, cycle_ptr::allocate_cycle<basic_tree>(std::move(db), offset, std::move(loader)))
{}


template<typename KeyType, typename ValueType, typename... Augments>
inline tx_aware_tree<KeyType, ValueType, Augments...>::tx_object::tx_object(
    db::transaction& tx,
    cycle_ptr::cycle_gptr<tx_aware_tree> tree)
: impl_(*this, tx.on(tree.impl_))
{}

template<typename KeyType, typename ValueType, typename... Augments>
inline auto tx_aware_tree<KeyType, ValueType, Augments...>::tx_object::empty() const -> bool {
  return impl_->empty() || begin() == end();
}


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_TREE_INL_H */
