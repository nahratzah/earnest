#ifndef EARNEST_DETAIL_TREE_FWD_H
#define EARNEST_DETAIL_TREE_FWD_H

#include <cstddef>
#include <cstdint>

namespace earnest::detail::tree {


using index_type = std::size_t;
struct ops;
struct cfg;
class loader;
class basic_tree;
class abstract_page;
class leaf;
class branch;
class key_type;
class value_type;
class tx_aware_value_type;
class augmented_page_ref;
class leaf_iterator;
class reverse_leaf_iterator;
template<typename KeyType, typename ValueType, typename... Augments> class tx_aware_loader;

using tree_size_type = std::uint64_t;
using tree_difference_type = std::int64_t;


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_FWD_H */
