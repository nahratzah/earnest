#ifndef EARNEST_DETAIL_TREE_CFG_H
#define EARNEST_DETAIL_TREE_CFG_H

#include <cstdint>
#include <boost/asio/buffer.hpp>

namespace earnest::detail {


struct tree_cfg {
  static constexpr std::size_t SIZE = 20u;

  std::uint32_t items_per_leaf_page, items_per_node_page;
  std::uint32_t key_bytes, val_bytes, augment_bytes;

  void encode(boost::asio::mutable_buffer buf) const;
  void decode(boost::asio::const_buffer buf);
};


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_TREE_CFG_H */
