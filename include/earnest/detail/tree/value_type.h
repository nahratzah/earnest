#ifndef EARNEST_DETAIL_TREE_VALUE_TYPE_H
#define EARNEST_DETAIL_TREE_VALUE_TYPE_H

#include <cstdint>
#include <cycle_ptr/cycle_ptr.h>
#include <earnest/detail/export_.h>
#include <earnest/detail/locked_ptr.h>
#include <earnest/detail/tree/fwd.h>
#include <earnest/detail/db_cache.h>
#include <tuple>
#include <vector>
#include <boost/asio/buffer.hpp>

namespace earnest::detail::tree {


class earnest_export_ value_type
: protected cycle_ptr::cycle_base
{
  friend leaf;
  friend tx_aware_value_type;
  friend leaf_iterator;
  template<typename KeyType, typename ValueType, typename... Augments> friend class tx_aware_loader;

  public:
  using allocator_type = db_cache::allocator_type;
  using shared_lock_ptr = earnest::detail::shared_lock_ptr<cycle_ptr::cycle_gptr<const value_type>>;
  using unique_lock_ptr = earnest::detail::unique_lock_ptr<cycle_ptr::cycle_gptr<value_type>>;

  value_type(index_type slot, allocator_type alloc)
  : parent_(*this),
    slot_(slot),
    alloc_(std::move(alloc))
  {}

  value_type(allocator_type alloc)
  : value_type(0, alloc)
  {}

  virtual ~value_type() noexcept;

  virtual void lock() = 0;
  virtual auto try_lock() -> bool = 0;
  virtual void unlock() = 0;
  virtual void lock_shared() const = 0;
  virtual auto try_lock_shared() const -> bool = 0;
  virtual void unlock_shared() const = 0;

  virtual auto is_never_visible() const noexcept -> bool = 0;
  virtual auto is_sentinel() const noexcept -> bool;

  static auto parent_page_for_read(shared_lock_ptr& vptr)
  -> std::tuple<
      earnest::detail::shared_lock_ptr<cycle_ptr::cycle_gptr<const leaf>>,
      bool>;
  static auto parent_page_for_read(unique_lock_ptr& vptr)
  -> std::tuple<
      earnest::detail::shared_lock_ptr<cycle_ptr::cycle_gptr<const leaf>>,
      bool>;
  static auto parent_page_for_write(shared_lock_ptr& vptr)
  -> std::tuple<
      earnest::detail::unique_lock_ptr<cycle_ptr::cycle_gptr<leaf>>,
      bool>;
  static auto parent_page_for_write(unique_lock_ptr& vptr)
  -> std::tuple<
      earnest::detail::unique_lock_ptr<cycle_ptr::cycle_gptr<leaf>>,
      bool>;

  auto get_allocator() const -> allocator_type { return alloc_; }

  private:
  // Internal only: implementations of parent_page_for_read and parent_page_for_write.
  template<typename LPtr, typename VPtr>
  static earnest_local_ auto parent_page_for_x_(VPtr& vptr) -> std::tuple<LPtr, bool>;

  ///\brief Encode this key value element.
  virtual void encode(boost::asio::mutable_buffer buf) const = 0;
  ///\brief Decode this key value element.
  virtual void decode(boost::asio::const_buffer buf) = 0;

  ///\brief Sibling pointers.
  ///\note Protected by locking this.
  ///\note Can only be changed with parent locked for write.
  cycle_ptr::cycle_member_ptr<value_type> pred_{ *this }, succ_{ *this };
  ///\brief Parent pointer.
  ///\note Protected by locking this.
  ///\note Can only be changed with (all involved) parents locked for write.
  cycle_ptr::cycle_member_ptr<leaf> parent_{ *this };
  ///\brief Slot index.
  ///\note Protected by locking this.
  ///\note Can only be changed with (all involved) parents locked for write.
  index_type slot_;
  ///\brief Allocator.
  allocator_type alloc_;
};


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_VALUE_TYPE_H */
