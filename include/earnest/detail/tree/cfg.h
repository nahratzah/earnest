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

  std::uint32_t items_per_leaf_page, items_per_node_page;
  std::uint32_t key_bytes, val_bytes, augment_bytes;

  earnest_export_ void encode(boost::asio::mutable_buffer buf) const;
  earnest_export_ void decode(boost::asio::const_buffer buf);

  cfg() = default;
  virtual ~cfg() noexcept;

  virtual auto allocate_elem(db_cache::allocator_type alloc) const -> cycle_ptr::cycle_gptr<value_type> = 0;
  virtual auto allocate_key(db_cache::allocator_type alloc) const -> cycle_ptr::cycle_gptr<key_type> = 0;
};


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_CFG_H */
