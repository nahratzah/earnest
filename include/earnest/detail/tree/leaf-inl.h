#ifndef EARNEST_DETAIL_TREE_LEAF_INL_H
#define EARNEST_DETAIL_TREE_LEAF_INL_H

#include <boost/endian/conversion.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/error.hpp>

#include <earnest/detail/tree/cfg.h>

namespace earnest::detail::tree {


inline void leaf::header::native_to_big_endian() noexcept {
  static_assert(sizeof(header) == header::SIZE);

  boost::endian::native_to_big_inplace(magic);
  boost::endian::native_to_big_inplace(flags);
  boost::endian::native_to_big_inplace(parent_off);
  boost::endian::native_to_big_inplace(next_sibling_off);
  boost::endian::native_to_big_inplace(prev_sibling_off);
}

inline void leaf::header::big_to_native_endian() noexcept {
  static_assert(sizeof(header) == header::SIZE);

  boost::endian::big_to_native_inplace(magic);
  boost::endian::big_to_native_inplace(flags);
  boost::endian::big_to_native_inplace(parent_off);
  boost::endian::big_to_native_inplace(next_sibling_off);
  boost::endian::big_to_native_inplace(prev_sibling_off);
}

template<typename SyncReadStream>
inline void leaf::header::decode(SyncReadStream& stream, boost::system::error_code& ec) {
  boost::asio::read(stream, boost::asio::buffer(this, sizeof(*this)), ec);
  if (ec) return;

  big_to_native_endian();
  if (magic != leaf::magic) ec = boost::asio::error::operation_not_supported;
}


inline auto leaf::max_size() const -> size_type {
  return cfg->items_per_leaf_page;
}

inline auto leaf::key() const -> cycle_ptr::cycle_gptr<const key_type> {
  return key_;
}

inline auto leaf::bytes_per_key_() const noexcept -> std::size_t {
  return cfg->key_bytes;
}

inline auto leaf::bytes_per_val_() const noexcept -> std::size_t {
  return cfg->val_bytes;
}

inline auto leaf::bytes_per_page_() const noexcept -> std::size_t {
  return header::SIZE + bytes_per_key_() + cfg->items_per_leaf_page * bytes_per_val_();
}

inline auto leaf::offset_for_idx_(index_type idx) const noexcept -> offset_type {
  auto rel_off = header::SIZE + bytes_per_key_() + idx * bytes_per_val_();
  return offset + rel_off;
}


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_LEAF_INL_H */
