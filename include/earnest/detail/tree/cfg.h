#ifndef EARNEST_DETAIL_TREE_CFG_H
#define EARNEST_DETAIL_TREE_CFG_H

#include <boost/asio/buffer.hpp>
#include <cstdint>
#include <cycle_ptr/cycle_ptr.h>
#include <earnest/detail/export_.h>
#include <earnest/detail/db_cache.h>
#include <earnest/detail/tree/fwd.h>

namespace earnest::detail::tree {


struct cfg {
  static constexpr std::size_t SIZE = 20u;

  ///\brief Number of items per leaf page.
  std::uint32_t items_per_leaf_page;
  ///\brief Number of items per branch page.
  std::uint32_t items_per_node_page;
  ///\brief Size of the key type.
  std::uint32_t key_bytes;
  ///\brief Size of the value type.
  ///\note A value_type for a map, is both the key, and the mapped type.
  std::uint32_t val_bytes;
  ///\brief Size of the augmentation.
  std::uint32_t augment_bytes;

  earnest_export_ void encode(boost::asio::mutable_buffer buf) const;
  earnest_export_ void decode(boost::asio::const_buffer buf);
};


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_CFG_H */
