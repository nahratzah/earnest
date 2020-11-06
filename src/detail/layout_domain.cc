#include <earnest/detail/layout_domain.h>

namespace earnest::detail {


layout_obj::~layout_obj() noexcept = default;


layout_domain::~layout_domain() noexcept = default;


void layout_lock::lock() {
  if (locked_) throw std::system_error(std::make_error_code(std::errc::resource_deadlock_would_occur));
  if (lobj_ == nullptr) throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));

  lobj_->lock_layout();
  locked_ = true;
}

bool layout_lock::try_lock() {
  if (locked_) throw std::system_error(std::make_error_code(std::errc::resource_deadlock_would_occur));
  if (lobj_ == nullptr) throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));

  return locked_ = lobj_->try_lock_layout();
}

void layout_lock::unlock() {
  if (lobj_ == nullptr || !locked_) throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));

  lobj_->unlock_layout();
  locked_ = false;
}


} /* namespace earnest::detail */
