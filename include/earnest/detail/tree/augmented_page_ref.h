#ifndef EARNEST_DETAIL_TREE_AUGMENTED_PAGE_REF_H
#define EARNEST_DETAIL_TREE_AUGMENTED_PAGE_REF_H

#include <earnest/detail/export_.h>
#include <earnest/detail/tree/fwd.h>
#include <cstdint>
#include <boost/asio/buffer.hpp>

namespace earnest::detail::tree {


class earnest_export_ augmented_page_ref {
  friend ops;

  public:
  using offset_type = std::uint64_t;

  explicit augmented_page_ref(offset_type offset = 0) : offset_(offset) {}
  virtual ~augmented_page_ref() noexcept;

  ///\brief Encode this key value element.
  void encode(boost::asio::mutable_buffer buf) const;
  ///\brief Decode this key value element.
  void decode(boost::asio::const_buffer buf);

  ///\brief Read the offset of the child page.
  auto offset() const noexcept -> offset_type { return offset_; }

  private:
  ///\brief Encode this key value element.
  virtual void do_encode(boost::asio::mutable_buffer buf) const = 0;
  ///\brief Decode this key value element.
  virtual void do_decode(boost::asio::const_buffer buf) = 0;

  offset_type offset_;
};


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_AUGMENTED_PAGE_REF_H */
