#ifndef EARNEST_DETAIL_TREE_KEY_TYP_H
#define EARNEST_DETAIL_TREE_KEY_TYP_H

#include <earnest/detail/tree/fwd.h>
#include <earnest/detail/export_.h>
#include <earnest/detail/db_cache.h>
#include <boost/asio/buffer.hpp>
#include <optional>

namespace earnest::detail::tree {


class earnest_export_ key_type {
  public:
  using allocator_type = db_cache::allocator_type;

  explicit key_type(allocator_type alloc) noexcept : alloc_(std::move(alloc)) {}
  virtual ~key_type() noexcept = 0;

  ///\brief Encode this key value element.
  virtual void encode(boost::asio::mutable_buffer buf) const = 0;
  ///\brief Decode this key value element.
  virtual void decode(boost::asio::const_buffer buf) = 0;

  auto get_allocator() const -> allocator_type { return alloc_; }

  /**
   * \brief Polymorphic before test.
   * \returns An optional containing true/false, which is the result of less-than comparison.
   * Or an empty optional if the key types are a mismatch.
   */
  virtual auto before(const key_type& y) const -> std::optional<bool> = 0;

  private:
  ///\brief Allocator.
  allocator_type alloc_;
};


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_KEY_TYP_H */
