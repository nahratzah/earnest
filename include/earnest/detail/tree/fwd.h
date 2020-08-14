#ifndef EARNEST_DETAIL_TREE_FWD_H
#define EARNEST_DETAIL_TREE_FWD_H

#include <cstddef>

namespace earnest::detail::tree {


using index_type = std::size_t;
class cfg;
class loader;
class abstract_tree;
class abstract_page;
class leaf;
class key_type;
class value_type;
class tx_aware_value_type;
class leaf_iterator;


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_FWD_H */
