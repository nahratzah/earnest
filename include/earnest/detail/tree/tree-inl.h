#ifndef EARNEST_DETAIL_TREE_TREE_INL_H
#define EARNEST_DETAIL_TREE_TREE_INL_H

#include <utility>
#include <boost/polymorphic_cast.hpp>
#include <boost/polymorphic_pointer_cast.hpp>
#include <earnest/detail/tree/value_type.h>
#include <earnest/detail/tree/tx_aware_value_type.h>

namespace earnest::detail::tree {


inline basic_tree::basic_tree(
    std::shared_ptr<class db> db,
    std::shared_ptr<const struct cfg> cfg,
    std::shared_ptr<const class loader> loader)
: db::db_obj(std::move(db)),
  cfg(std::move(cfg)),
  loader(std::move(loader))
{}


inline basic_tx_aware_tree::tx_object::tx_object(
    db::transaction& tx,
    cycle_ptr::cycle_gptr<basic_tx_aware_tree> tree)
: tx(tx),
  tree_(*this, std::move(tree))
{}


inline basic_tx_aware_tree::tx_object::iterator::iterator(const leaf_iterator& iter) noexcept
: iter_(iter)
{}

inline basic_tx_aware_tree::tx_object::iterator::iterator(leaf_iterator&& iter) noexcept
: iter_(std::move(iter))
{}

inline auto basic_tx_aware_tree::tx_object::iterator::operator++(int) -> iterator {
  auto r = iterator(iter_++);
  seek_forward_until_valid_();
  return r;
}

inline auto basic_tx_aware_tree::tx_object::iterator::operator--(int) -> iterator {
  auto r = iterator(iter_--);
  seek_backward_until_valid_();
  return r;
}

inline auto basic_tx_aware_tree::tx_object::iterator::operator++() -> iterator& {
  ++iter_;
  seek_forward_until_valid_();
  return *this;
}

inline auto basic_tx_aware_tree::tx_object::iterator::operator--() -> iterator& {
  --iter_;
  seek_backward_until_valid_();
  return *this;
}

inline auto basic_tx_aware_tree::tx_object::iterator::operator==(const iterator& y) const -> bool {
  return txo_ == y.txo_ && iter_ == y.iter_;
}

inline auto basic_tx_aware_tree::tx_object::iterator::operator!=(const iterator& y) const -> bool {
  return txo_ == y.txo_ && iter_ != y.iter_;
}

inline auto basic_tx_aware_tree::tx_object::iterator::operator*() const -> const tx_aware_value_type& {
  return *boost::polymorphic_cast<const tx_aware_value_type*>(&iter_.operator*());
}

inline auto basic_tx_aware_tree::tx_object::iterator::operator->() const -> const tx_aware_value_type* {
  return boost::polymorphic_cast<const tx_aware_value_type*>(iter_.operator->());
}

inline auto basic_tx_aware_tree::tx_object::iterator::ptr() const -> cycle_ptr::cycle_gptr<const tx_aware_value_type> {
  return boost::polymorphic_pointer_downcast<const tx_aware_value_type>(iter_.ptr());
}

inline auto basic_tx_aware_tree::tx_object::iterator::is_sentinel() const -> bool {
  return iter_->is_sentinel();
}


template<typename KeyType, typename ValueType, typename... Augments>
inline tx_aware_tree<KeyType, ValueType, Augments...>::tx_aware_tree(std::shared_ptr<class db> db, txfile::transaction::offset_type offset, std::shared_ptr<const tx_aware_loader<KeyType, ValueType, Augments...>> loader)
: impl_(*this, cycle_ptr::allocate_cycle<basic_tree>(std::move(db), offset, std::move(loader)))
{}


template<typename ValueType>
inline tx_aware_tree_iterator<ValueType>::tx_aware_tree_iterator(const basic_tx_aware_tree::tx_object::iterator& iter) noexcept
: iter_(iter)
{}

template<typename ValueType>
inline tx_aware_tree_iterator<ValueType>::tx_aware_tree_iterator(basic_tx_aware_tree::tx_object::iterator&& iter) noexcept
: iter_(std::move(iter))
{}

template<typename ValueType>
inline auto tx_aware_tree_iterator<ValueType>::operator++(int) -> tx_aware_tree_iterator {
  return tx_aware_tree_iterator(iter_++);
}

template<typename ValueType>
inline auto tx_aware_tree_iterator<ValueType>::operator--(int) -> tx_aware_tree_iterator {
  return tx_aware_tree_iterator(iter_--);
}

template<typename ValueType>
inline auto tx_aware_tree_iterator<ValueType>::operator++() -> tx_aware_tree_iterator& {
  ++iter_;
  return *this;
}

template<typename ValueType>
inline auto tx_aware_tree_iterator<ValueType>::operator--() -> tx_aware_tree_iterator& {
  --iter_;
  return *this;
}

template<typename ValueType>
inline auto tx_aware_tree_iterator<ValueType>::operator==(const tx_aware_tree_iterator& y) const -> bool {
  return iter_ == y.iter_;
}

template<typename ValueType>
inline auto tx_aware_tree_iterator<ValueType>::operator!=(const tx_aware_tree_iterator& y) const -> bool {
  return iter_ != y.iter_;
}

template<typename ValueType>
inline auto tx_aware_tree_iterator<ValueType>::operator*() const -> reference {
  return boost::polymorphic_downcast<const tx_aware_value_type_impl<ValueType>*>(&iter_.operator*())->value;
}

template<typename ValueType>
inline auto tx_aware_tree_iterator<ValueType>::operator->() const -> pointer {
  return std::addressof(boost::polymorphic_downcast<const tx_aware_value_type_impl<ValueType>*>(&iter_.operator*())->value);
}

template<typename ValueType>
inline auto tx_aware_tree_iterator<ValueType>::ptr() const -> cycle_ptr::cycle_gptr<const tx_aware_value_type_impl<ValueType>> {
  return boost::polymorphic_pointer_downcast<const tx_aware_value_type_impl<ValueType>>(iter_.ptr());
}


template<typename KeyType, typename ValueType, typename... Augments>
inline tx_aware_tree<KeyType, ValueType, Augments...>::tx_object::tx_object(
    db::transaction& tx,
    cycle_ptr::cycle_gptr<tx_aware_tree> tree)
: impl_(*this, tx.on(tree.impl_))
{}

template<typename KeyType, typename ValueType, typename... Augments>
inline auto tx_aware_tree<KeyType, ValueType, Augments...>::tx_object::empty() const -> bool {
  return impl_->empty() || impl_->begin() == impl_->end();
}

template<typename KeyType, typename ValueType, typename... Augments>
inline auto tx_aware_tree<KeyType, ValueType, Augments...>::tx_object::begin() const -> iterator {
  return iterator(impl_->begin());
}

template<typename KeyType, typename ValueType, typename... Augments>
inline auto tx_aware_tree<KeyType, ValueType, Augments...>::tx_object::end() const -> iterator {
  return iterator(impl_->end());
}


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_TREE_INL_H */
