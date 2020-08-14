#include <earnest/detail/tree/cfg.h>
#include <cassert>
#include <boost/endian/conversion.hpp>

namespace earnest::detail::tree {


void cfg::encode(boost::asio::mutable_buffer buf) const {
  assert(buf.size() >= SIZE);

  auto items_per_leaf_page = boost::endian::native_to_big(this->items_per_leaf_page);
  auto items_per_node_page = boost::endian::native_to_big(this->items_per_node_page);
  auto key_bytes = boost::endian::native_to_big(this->key_bytes);
  auto val_bytes = boost::endian::native_to_big(this->val_bytes);
  auto augment_bytes = boost::endian::native_to_big(this->augment_bytes);

  boost::asio::buffer_copy(
      buf,
      std::array<boost::asio::const_buffer, 5>{
        boost::asio::buffer(&items_per_leaf_page, sizeof(items_per_leaf_page)),
        boost::asio::buffer(&items_per_node_page, sizeof(items_per_node_page)),
        boost::asio::buffer(&key_bytes, sizeof(key_bytes)),
        boost::asio::buffer(&val_bytes, sizeof(val_bytes)),
        boost::asio::buffer(&augment_bytes, sizeof(augment_bytes))
      });
}

void cfg::decode(boost::asio::const_buffer buf) {
  assert(buf.size() >= SIZE);

  boost::asio::buffer_copy(boost::asio::buffer(this, sizeof(*this)), buf);
  boost::endian::big_to_native_inplace(items_per_leaf_page);
  boost::endian::big_to_native_inplace(items_per_node_page);
  boost::endian::big_to_native_inplace(key_bytes);
  boost::endian::big_to_native_inplace(val_bytes);
  boost::endian::big_to_native_inplace(augment_bytes);
}

cfg::~cfg() noexcept = default;


} /* namespace earnest::detail::tree */
