#ifndef EARNEST_DETAIL_TREE_BRANCH_INL_H
#define EARNEST_DETAIL_TREE_BRANCH_INL_H

#include <boost/endian/conversion.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/error.hpp>

#include <earnest/detail/tree/cfg.h>

namespace earnest::detail::tree {


inline void branch::header::native_to_big_endian() noexcept {
  static_assert(sizeof(header) == header::SIZE);

  boost::endian::native_to_big_inplace(magic);
  boost::endian::native_to_big_inplace(size);
  boost::endian::native_to_big_inplace(parent_off);
}

inline void branch::header::big_to_native_endian() noexcept {
  static_assert(sizeof(header) == header::SIZE);

  boost::endian::big_to_native_inplace(magic);
  boost::endian::big_to_native_inplace(size);
  boost::endian::big_to_native_inplace(parent_off);
}

template<typename SyncReadStream>
inline void branch::header::decode(SyncReadStream& stream, boost::system::error_code& ec) {
  boost::asio::read(stream, boost::asio::buffer(this, sizeof(*this)), ec);
  if (ec) return;

  big_to_native_endian();
  if (magic != branch::magic) ec = boost::asio::error::operation_not_supported;
}


inline auto branch::find_slot(const shared_lock_ptr& self, offset_type page) -> std::optional<size_type> {
  return self->find_slot_(page);
}

inline auto branch::find_slot(const unique_lock_ptr& self, offset_type page) -> std::optional<size_type> {
  return self->find_slot_(page);
}

inline auto branch::max_size() const -> size_type {
  return cfg->items_per_node_page;
}

inline auto branch::bytes_per_key_() const noexcept -> std::size_t {
  return cfg->key_bytes;
}

inline auto branch::bytes_per_augmented_page_ref_() const noexcept -> std::size_t {
  return sizeof(std::uint64_t) + cfg->augment_bytes;
}

inline auto branch::bytes_per_page_() const noexcept -> std::size_t {
  return header::SIZE
      + max_size() * bytes_per_augmented_page_ref_()
      + (max_size() - 1u) * bytes_per_key_();
}


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_BRANCH_INL_H */
