#include <earnest/detail/tree/value_type.h>
#include <earnest/detail/tree/leaf.h>
#include <algorithm>
#include <cassert>
#include <deque>
#include <memory>
#include <vector>
#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/write_at.hpp>
#include <boost/asio/completion_condition.hpp>

namespace earnest::detail::tree {


value_type::~value_type() noexcept = default;

template<typename LPtr, typename VPtr>
inline earnest_local_ auto value_type::parent_page_for_x_(VPtr& vptr) -> std::tuple<LPtr, bool> {
  if (!vptr.owns_lock()) throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));

  auto parent = LPtr(vptr->parent_, std::try_to_lock);
  bool relocked = false;
  while (!parent.owns_lock()) {
    relocked = true;
    vptr.unlock();
    parent.lock();
    vptr.lock();

    if (parent.mutex() != vptr->parent_) {
      parent.unlock();
      parent = LPtr(vptr->parent_, std::try_to_lock);
    }
  }

  assert(parent.owns_lock() && vptr.owns_lock());
  assert(parent.mutex() == vptr->parent_);
  return std::make_tuple(std::move(parent), std::move(relocked));
}

auto value_type::is_sentinel() const noexcept -> bool {
  return false;
}

auto value_type::parent_page_for_read(shared_lock_ptr& vptr)
-> std::tuple<leaf::shared_lock_ptr, bool> {
  return parent_page_for_x_<leaf::shared_lock_ptr>(vptr);
}

auto value_type::parent_page_for_read(unique_lock_ptr& vptr)
-> std::tuple<leaf::shared_lock_ptr, bool> {
  return parent_page_for_x_<leaf::shared_lock_ptr>(vptr);
}

auto value_type::parent_page_for_write(shared_lock_ptr& vptr)
-> std::tuple<leaf::unique_lock_ptr, bool> {
  return parent_page_for_x_<leaf::unique_lock_ptr>(vptr);
}

auto value_type::parent_page_for_write(unique_lock_ptr& vptr)
-> std::tuple<leaf::unique_lock_ptr, bool> {
  return parent_page_for_x_<leaf::unique_lock_ptr>(vptr);
}


} /* namespace earnest::detail::tree */
