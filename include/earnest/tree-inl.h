#ifndef EARNEST_TREE_INL_H
#define EARNEST_TREE_INL_H

#include <boost/polymorphic_pointer_cast.hpp>

namespace earnest {


template<typename Key, typename Val, typename Less, typename... Augments>
inline tree<Key, Val, Less, Augments...>::tx_object::tx_object(db::transaction& tx, cycle_ptr::cycle_gptr<tree> self)
: abstract_tree::abstract_tx_object(tx, std::move(self))
{}

template<typename Key, typename Val, typename Less, typename... Augments>
inline auto tree<Key, Val, Less, Augments...>::tx_object::self() const noexcept -> cycle_ptr::cycle_gptr<tree> {
  return boost::polymorphic_pointer_downcast<tree>(this->abstract_tx_object::self());
}


} /* namespace earnest */

#endif /* EARNEST_TREE_INL_H */
