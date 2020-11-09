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


template<typename LeafIterator>
template<typename OtherLeafIterator>
constexpr basic_tx_aware_tree::tx_object::iterator_<LeafIterator>::iterator_(const iterator_<OtherLeafIterator>& y) noexcept
: txo_(y.txo_),
  iter_(y.iter_)
{}

template<typename LeafIterator>
template<typename OtherLeafIterator>
constexpr basic_tx_aware_tree::tx_object::iterator_<LeafIterator>::iterator_(iterator_<OtherLeafIterator>&& y) noexcept
: txo_(std::move(y.txo_)),
  iter_(std::move(y.iter_))
{}

template<typename LeafIterator>
inline basic_tx_aware_tree::tx_object::iterator_<LeafIterator>::iterator_(cycle_ptr::cycle_gptr<const tx_object> txo, const LeafIterator& iter, bool skip_seek) noexcept
: txo_(std::move(txo)),
  iter_(iter)
{
  if (!skip_seek) seek_forward_until_valid_();
}

template<typename LeafIterator>
inline basic_tx_aware_tree::tx_object::iterator_<LeafIterator>::iterator_(cycle_ptr::cycle_gptr<const tx_object> txo, LeafIterator&& iter, bool skip_seek) noexcept
: txo_(std::move(txo)),
  iter_(std::move(iter))
{
  if (!skip_seek) seek_forward_until_valid_();
}

template<typename LeafIterator>
inline auto basic_tx_aware_tree::tx_object::iterator_<LeafIterator>::operator++(int) -> iterator_ {
  auto r = iterator(txo_, iter_++, true);
  seek_forward_until_valid_();
  return r;
}

template<typename LeafIterator>
inline auto basic_tx_aware_tree::tx_object::iterator_<LeafIterator>::operator--(int) -> iterator_ {
  auto r = iterator(txo_, iter_--, true);
  seek_backward_until_valid_();
  return r;
}

template<typename LeafIterator>
inline auto basic_tx_aware_tree::tx_object::iterator_<LeafIterator>::operator++() -> iterator_& {
  ++iter_;
  seek_forward_until_valid_();
  return *this;
}

template<typename LeafIterator>
inline auto basic_tx_aware_tree::tx_object::iterator_<LeafIterator>::operator--() -> iterator_& {
  --iter_;
  seek_backward_until_valid_();
  return *this;
}

template<typename LeafIterator>
inline auto basic_tx_aware_tree::tx_object::iterator_<LeafIterator>::operator==(const iterator_& y) const -> bool {
  return txo_ == y.txo_ && iter_ == y.iter_;
}

template<typename LeafIterator>
inline auto basic_tx_aware_tree::tx_object::iterator_<LeafIterator>::operator!=(const iterator_& y) const -> bool {
  return txo_ == y.txo_ && iter_ != y.iter_;
}

template<typename LeafIterator>
inline auto basic_tx_aware_tree::tx_object::iterator_<LeafIterator>::operator*() const -> reference {
  return *boost::polymorphic_cast<const tx_aware_value_type*>(&iter_.operator*());
}

template<typename LeafIterator>
inline auto basic_tx_aware_tree::tx_object::iterator_<LeafIterator>::operator->() const -> pointer {
  return boost::polymorphic_cast<const tx_aware_value_type*>(iter_.operator->());
}

template<typename LeafIterator>
inline auto basic_tx_aware_tree::tx_object::iterator_<LeafIterator>::ptr() const -> cycle_ptr::cycle_gptr<const tx_aware_value_type> {
  return boost::polymorphic_pointer_downcast<const tx_aware_value_type>(iter_.ptr());
}

template<typename LeafIterator>
inline auto basic_tx_aware_tree::tx_object::iterator_<LeafIterator>::is_sentinel() const -> bool {
  return iter_->is_sentinel();
}

template<typename LeafIterator>
inline void basic_tx_aware_tree::tx_object::iterator_<LeafIterator>::seek_forward_until_valid_() {
  while (!is_sentinel() && !txo_->tx.visible(ptr()))
    ++iter_;
}

template<typename LeafIterator>
inline void basic_tx_aware_tree::tx_object::iterator_<LeafIterator>::seek_backward_until_valid_() {
  while (!is_sentinel() && !txo_->tx.visible(ptr()))
    --iter_;
}


template<typename KeyType, typename ValueType, typename... Augments>
inline tx_aware_tree<KeyType, ValueType, Augments...>::tx_aware_tree(std::shared_ptr<class db> db, txfile::transaction::offset_type offset, std::shared_ptr<const tx_aware_loader<KeyType, ValueType, Augments...>> loader)
: impl_(*this, cycle_ptr::allocate_cycle<basic_tree>(std::move(db), offset, std::move(loader)))
{}


template<typename ValueType, typename Iterator>
inline tx_aware_tree_iterator<ValueType, Iterator>::tx_aware_tree_iterator(const tx_aware_tree_iterator<ValueType, leaf_iterator>& iter) noexcept
: iter_(iter)
{}

template<typename ValueType, typename Iterator>
inline tx_aware_tree_iterator<ValueType, Iterator>::tx_aware_tree_iterator(const tx_aware_tree_iterator<ValueType, reverse_leaf_iterator>& iter) noexcept
: iter_(iter)
{}

template<typename ValueType, typename Iterator>
inline tx_aware_tree_iterator<ValueType, Iterator>::tx_aware_tree_iterator(tx_aware_tree_iterator<ValueType, leaf_iterator>&& iter) noexcept
: iter_(std::move(iter))
{}

template<typename ValueType, typename Iterator>
inline tx_aware_tree_iterator<ValueType, Iterator>::tx_aware_tree_iterator(tx_aware_tree_iterator<ValueType, reverse_leaf_iterator>&& iter) noexcept
: iter_(std::move(iter))
{}

template<typename ValueType, typename Iterator>
inline tx_aware_tree_iterator<ValueType, Iterator>::tx_aware_tree_iterator(const Iterator& iter) noexcept
: iter_(iter)
{}

template<typename ValueType, typename Iterator>
inline tx_aware_tree_iterator<ValueType, Iterator>::tx_aware_tree_iterator(Iterator&& iter) noexcept
: iter_(std::move(iter))
{}

template<typename ValueType, typename Iterator>
inline auto tx_aware_tree_iterator<ValueType, Iterator>::operator++(int) -> tx_aware_tree_iterator {
  return tx_aware_tree_iterator(iter_++);
}

template<typename ValueType, typename Iterator>
inline auto tx_aware_tree_iterator<ValueType, Iterator>::operator--(int) -> tx_aware_tree_iterator {
  return tx_aware_tree_iterator(iter_--);
}

template<typename ValueType, typename Iterator>
inline auto tx_aware_tree_iterator<ValueType, Iterator>::operator++() -> tx_aware_tree_iterator& {
  ++iter_;
  return *this;
}

template<typename ValueType, typename Iterator>
inline auto tx_aware_tree_iterator<ValueType, Iterator>::operator--() -> tx_aware_tree_iterator& {
  --iter_;
  return *this;
}

template<typename ValueType, typename Iterator>
inline auto tx_aware_tree_iterator<ValueType, Iterator>::operator==(const tx_aware_tree_iterator& y) const -> bool {
  return iter_ == y.iter_;
}

template<typename ValueType, typename Iterator>
inline auto tx_aware_tree_iterator<ValueType, Iterator>::operator!=(const tx_aware_tree_iterator& y) const -> bool {
  return iter_ != y.iter_;
}

template<typename ValueType, typename Iterator>
inline auto tx_aware_tree_iterator<ValueType, Iterator>::operator*() const -> reference {
  return boost::polymorphic_downcast<const tx_aware_value_type_impl<ValueType>*>(&iter_.operator*())->value;
}

template<typename ValueType, typename Iterator>
inline auto tx_aware_tree_iterator<ValueType, Iterator>::operator->() const -> pointer {
  return std::addressof(boost::polymorphic_downcast<const tx_aware_value_type_impl<ValueType>*>(&iter_.operator*())->value);
}

template<typename ValueType, typename Iterator>
inline auto tx_aware_tree_iterator<ValueType, Iterator>::ptr() const -> cycle_ptr::cycle_gptr<const tx_aware_value_type_impl<ValueType>> {
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

template<typename KeyType, typename ValueType, typename... Augments>
inline auto tx_aware_tree<KeyType, ValueType, Augments...>::tx_object::rbegin() const -> reverse_iterator {
  return iterator(impl_->rbegin());
}

template<typename KeyType, typename ValueType, typename... Augments>
inline auto tx_aware_tree<KeyType, ValueType, Augments...>::tx_object::rend() const -> reverse_iterator {
  return iterator(impl_->rend());
}


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_TREE_INL_H */
