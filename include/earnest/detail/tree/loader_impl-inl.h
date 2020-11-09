#ifndef EARNEST_DETAIL_TREE_LOADER_IMPL_INL_H
#define EARNEST_DETAIL_TREE_LOADER_IMPL_INL_H

#include <earnest/detail/tree/leaf.h>
#include <earnest/detail/tree/branch.h>
#include <boost/polymorphic_cast.hpp>
#include <numeric>

namespace earnest::detail::tree {


template<typename KeyType, typename ValueType, typename... Augments>
auto tx_aware_loader<KeyType, ValueType, Augments...>::allocate_elem(db_cache::allocator_type alloc) const
-> cycle_ptr::cycle_gptr<earnest::detail::tree::value_type> {
  return cycle_ptr::allocate_cycle<value_type>(alloc, alloc);
}

template<typename KeyType, typename ValueType, typename... Augments>
auto tx_aware_loader<KeyType, ValueType, Augments...>::allocate_key(db_cache::allocator_type alloc) const
-> cycle_ptr::cycle_gptr<earnest::detail::tree::key_type> {
  return cycle_ptr::allocate_cycle<key_type>(alloc, alloc);
}

template<typename KeyType, typename ValueType, typename... Augments>
auto tx_aware_loader<KeyType, ValueType, Augments...>::allocate_key(const earnest::detail::tree::value_type& value, db_cache::allocator_type alloc) const
-> cycle_ptr::cycle_gptr<earnest::detail::tree::key_type> {
  return cycle_ptr::allocate_cycle<key_type>(alloc, KeyType(boost::polymorphic_downcast<const value_type*>(&value)->value), alloc);
}

template<typename KeyType, typename ValueType, typename... Augments>
auto tx_aware_loader<KeyType, ValueType, Augments...>::allocate_augmented_page_ref(db_cache::allocator_type alloc) const
-> cycle_ptr::cycle_gptr<augmented_page_ref> {
  return cycle_ptr::allocate_cycle<augment_type>(alloc, std::allocator_arg, alloc);
}

template<typename KeyType, typename ValueType, typename... Augments>
auto tx_aware_loader<KeyType, ValueType, Augments...>::key_bytes() const noexcept -> std::size_t {
  return KeyType::SIZE;
}

template<typename KeyType, typename ValueType, typename... Augments>
auto tx_aware_loader<KeyType, ValueType, Augments...>::val_bytes() const noexcept -> std::size_t {
  return ValueType::SIZE;
}

template<typename KeyType, typename ValueType, typename... Augments>
auto tx_aware_loader<KeyType, ValueType, Augments...>::augment_bytes() const noexcept -> std::size_t {
  std::initializer_list<std::size_t> il{ Augments::SIZE... };
  return std::accumulate(il.begin(), il.end(), std::size_t(0));
}

template<typename KeyType, typename ValueType, typename... Augments>
auto tx_aware_loader<KeyType, ValueType, Augments...>::allocate_augmented_page_ref(const leaf& leaf, db_cache::allocator_type alloc) const
-> cycle_ptr::cycle_gptr<augmented_page_ref> {
  cycle_ptr::cycle_gptr<augment_type> pgref = cycle_ptr::allocate_cycle<augment_type>(alloc, std::allocator_arg, alloc);

  if constexpr(sizeof...(Augments) != 0) {
    earnest::detail::tree::value_type::shared_lock_ptr elem(leaf.head_sentinel_->succ_);
    if (elem.mutex() != leaf.tail_sentinel_) {
      {
        const auto elem_ptr = boost::polymorphic_downcast<const value_type*>(elem.mutex().get());
        pgref->augments = std::make_tuple(Augments(elem_ptr->value)...);
      }

      for (elem = earnest::detail::tree::value_type::shared_lock_ptr(elem->succ_);
          elem.mutex() != leaf.tail_sentinel_;
          elem = earnest::detail::tree::value_type::shared_lock_ptr(elem->succ_)) {
        const auto elem_ptr = boost::polymorphic_downcast<const value_type*>(elem.mutex().get());
        pgref->augments = reduce_(std::move(pgref->augments), std::make_tuple(Augments(elem_ptr->value)...));
      }
    }
  }

  return pgref;
}

template<typename KeyType, typename ValueType, typename... Augments>
auto tx_aware_loader<KeyType, ValueType, Augments...>::allocate_augmented_page_ref(const branch& branch, db_cache::allocator_type alloc) const
-> cycle_ptr::cycle_gptr<augmented_page_ref> {
  cycle_ptr::cycle_gptr<augment_type> pgref = cycle_ptr::allocate_cycle<augment_type>(alloc, std::allocator_arg, alloc);

  if constexpr(sizeof...(Augments) != 0) {
    auto pages_iter = branch.pages_.begin();
    if (pages_iter != branch.pages_.end()) {
      {
        const auto elem_ptr = boost::polymorphic_downcast<const augment_type*>(pages_iter->get());
        pgref->augments = elem_ptr->augments;
      }

      for (++pages_iter; pages_iter != branch.pages_.end(); ++pages_iter) {
        const auto elem_ptr = boost::polymorphic_downcast<const augment_type*>(pages_iter->get());
        pgref->augments = reduce_(std::move(pgref->augments), elem_ptr->augments);
      }
    }
  }

  return pgref;
}

template<typename KeyType, typename ValueType, typename... Augments>
auto tx_aware_loader<KeyType, ValueType, Augments...>::reduce_(std::tuple<Augments...>&& x, const std::tuple<Augments...>& y)
-> std::tuple<Augments...> {
  return reduce_(std::move(x), y, std::index_sequence_for<Augments...>());
}

template<typename KeyType, typename ValueType, typename... Augments>
auto tx_aware_loader<KeyType, ValueType, Augments...>::reduce_(std::tuple<Augments...>&& x, std::tuple<Augments...>&& y)
-> std::tuple<Augments...> {
  return reduce_(std::move(x), std::move(y), std::index_sequence_for<Augments...>());
}

template<typename KeyType, typename ValueType, typename... Augments>
template<std::size_t... Idx>
auto tx_aware_loader<KeyType, ValueType, Augments...>::reduce_(std::tuple<Augments...>&& x, const std::tuple<Augments...>& y, [[maybe_unused]] std::index_sequence<Idx...> idx_seq)
-> std::tuple<Augments...> {
  return std::make_tuple(Augments::merge(std::get<Idx>(std::move(x)), std::get<Idx>(y))...);
}

template<typename KeyType, typename ValueType, typename... Augments>
template<std::size_t... Idx>
auto tx_aware_loader<KeyType, ValueType, Augments...>::reduce_(std::tuple<Augments...>&& x, std::tuple<Augments...>&& y, [[maybe_unused]] std::index_sequence<Idx...> idx_seq)
-> std::tuple<Augments...> {
  return std::make_tuple(Augments::merge(std::get<Idx>(std::move(x)), std::get<Idx>(std::move(y)))...);
}


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_LOADER_IMPL_INL_H */
