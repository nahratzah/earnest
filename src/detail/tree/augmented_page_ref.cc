#include <earnest/detail/tree/augmented_page_ref.h>
#include <boost/endian/conversion.hpp>
#include <cassert>

namespace earnest::detail::tree {


augmented_page_ref::~augmented_page_ref() noexcept = default;

void augmented_page_ref::encode(boost::asio::mutable_buffer buf) const {
  assert(buf.size() >= sizeof(offset_type));

  offset_type offset_be = offset_;
  boost::endian::native_to_big_inplace(offset_be);
  boost::asio::buffer_copy(
      buf,
      boost::asio::buffer(&offset_be, sizeof(offset_be)));

  do_encode(buf + sizeof(offset_be));
}

void augmented_page_ref::decode(boost::asio::const_buffer buf) {
  assert(buf.size() >= sizeof(offset_type));

  boost::asio::buffer_copy(
      boost::asio::buffer(&offset_, sizeof(offset_)),
      buf);
  boost::endian::big_to_native_inplace(offset_);

  do_decode(buf + sizeof(offset_));
}


} /* namespace earnest::detail::tree */
