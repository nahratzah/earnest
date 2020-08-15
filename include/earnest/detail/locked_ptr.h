#ifndef EARNEST_DETAIL_LOCKED_PTR_H
#define EARNEST_DETAIL_LOCKED_PTR_H

#include <system_error>
#include <type_traits>
#include <utility>
#include <mutex>

namespace earnest::detail {


template<typename Ptr>
class shared_lock_ptr {
  public:
  using element_type = typename std::pointer_traits<Ptr>::element_type;

  shared_lock_ptr(const shared_lock_ptr&) = delete;
  shared_lock_ptr& operator=(const shared_lock_ptr&) = delete;

  shared_lock_ptr() = default;

  shared_lock_ptr(shared_lock_ptr&& o) noexcept(std::is_nothrow_move_constructible_v<Ptr>)
  : ptr_(std::move(o.ptr_)),
    locked_(std::exchange(o.locked_, false))
  {}

  shared_lock_ptr& operator=(shared_lock_ptr&& o) noexcept(std::is_nothrow_move_assignable_v<Ptr>) {
    if (&o != this) {
      o.ensure_unlocked_();
      ptr_ = std::move(o.ptr_);
      locked_ = std::exchange(o.locked_, false);
    }
    return *this;
  }

  explicit shared_lock_ptr(const Ptr& p)
  : shared_lock_ptr(p, std::defer_lock)
  {
    lock();
  }

  explicit shared_lock_ptr(Ptr&& p)
  : shared_lock_ptr(std::move(p), std::defer_lock)
  {
    lock();
  }

  shared_lock_ptr(const Ptr& p, [[maybe_unused]] std::defer_lock_t t) noexcept(std::is_nothrow_copy_constructible_v<Ptr>)
  : ptr_(p)
  {}

  shared_lock_ptr(Ptr&& p, [[maybe_unused]] std::defer_lock_t t) noexcept(std::is_nothrow_move_constructible_v<Ptr>)
  : ptr_(std::move(p))
  {}

  shared_lock_ptr(const Ptr& p, [[maybe_unused]] std::try_to_lock_t t)
  : shared_lock_ptr(p, std::defer_lock)
  {
    try_lock();
  }

  shared_lock_ptr(Ptr&& p, [[maybe_unused]] std::try_to_lock_t t)
  : shared_lock_ptr(std::move(p), std::defer_lock)
  {
    try_lock();
  }

  shared_lock_ptr(const Ptr& p, [[maybe_unused]] std::adopt_lock_t t)
  : ptr_(p),
    locked_(true)
  {
    if (ptr_ == nullptr) throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));
  }

  shared_lock_ptr(Ptr&& p, [[maybe_unused]] std::adopt_lock_t t)
  : ptr_(std::move(p)),
    locked_(true)
  {
    if (ptr_ == nullptr) throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));
  }

  ~shared_lock_ptr() noexcept {
    ensure_unlocked_();
  }

  void swap(shared_lock_ptr& o) noexcept(std::is_nothrow_swappable_v<Ptr>) {
    if constexpr(std::is_nothrow_swappable_v<Ptr>) {
      using std::swap;

      swap(ptr_, o.ptr_);
      swap(locked_, o.locked_);
    } else {
      // Use move operations, to maintain locked_ invariant.
      shared_lock_ptr tmp = std::move(o);
      o = std::move(*this);
      *this = std::move(tmp);
    }
  }

  auto mutex() const noexcept -> const Ptr& { return ptr_; }
  bool owns_lock() const noexcept { return locked_; }
  explicit operator bool() const noexcept { return owns_lock(); }

  auto operator*() const noexcept(noexcept(*std::declval<const Ptr&>())) -> element_type& {
    return *ptr_;
  }

  auto operator->() const noexcept(noexcept(std::declval<const Ptr&>().operator->())) -> decltype(std::declval<const Ptr&>().operator->()) {
    return ptr_.operator->();
  }

  void lock() {
    if (ptr_ == nullptr) throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));
    if (locked_) throw std::system_error(std::make_error_code(std::errc::resource_deadlock_would_occur));

    ptr_->lock_shared();
    locked_ = true;
  }

  auto try_lock() -> bool {
    if (ptr_ == nullptr) throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));
    if (locked_) throw std::system_error(std::make_error_code(std::errc::resource_deadlock_would_occur));

    return locked_ = ptr_->try_lock_shared();
  }

  void unlock() {
    if (ptr_ == nullptr) throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));
    if (!locked_) throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));

    ptr_->unlock_shared();
    locked_ = false;
  }

  auto release() noexcept(std::is_nothrow_move_constructible_v<Ptr> || std::is_nothrow_copy_constructible_v<Ptr>) -> Ptr {
    Ptr ptr = std::move_if_noexcept(ptr_);
    locked_ = false;
    return ptr;
  }

  private:
  void ensure_unlocked_() noexcept(noexcept(std::declval<Ptr&>()->unlock_shared())) {
    if (locked_) {
      ptr_->unlock_shared();
      locked_ = false;
    }
  }

  Ptr ptr_ = nullptr;
  bool locked_ = false;
};


template<typename Ptr>
class unique_lock_ptr {
  public:
  using element_type = typename std::pointer_traits<Ptr>::element_type;

  unique_lock_ptr(const unique_lock_ptr&) = delete;
  unique_lock_ptr& operator=(const unique_lock_ptr&) = delete;

  unique_lock_ptr() = default;

  unique_lock_ptr(unique_lock_ptr&& o) noexcept(std::is_nothrow_move_constructible_v<Ptr>)
  : ptr_(std::move(o.ptr_)),
    locked_(std::exchange(o.locked_, false))
  {}

  unique_lock_ptr& operator=(unique_lock_ptr&& o) noexcept(std::is_nothrow_move_assignable_v<Ptr>) {
    if (&o != this) {
      o.ensure_unlocked_();
      ptr_ = std::move(o.ptr_);
      locked_ = std::exchange(o.locked_, false);
    }
    return *this;
  }

  explicit unique_lock_ptr(const Ptr& p)
  : unique_lock_ptr(p, std::defer_lock)
  {
    lock();
  }

  explicit unique_lock_ptr(Ptr&& p)
  : unique_lock_ptr(std::move(p), std::defer_lock)
  {
    lock();
  }

  unique_lock_ptr(const Ptr& p, [[maybe_unused]] std::defer_lock_t t) noexcept(std::is_nothrow_copy_constructible_v<Ptr>)
  : ptr_(p)
  {}

  unique_lock_ptr(Ptr&& p, [[maybe_unused]] std::defer_lock_t t) noexcept(std::is_nothrow_move_constructible_v<Ptr>)
  : ptr_(std::move(p))
  {}

  unique_lock_ptr(const Ptr& p, [[maybe_unused]] std::try_to_lock_t t)
  : unique_lock_ptr(p, std::defer_lock)
  {
    try_lock();
  }

  unique_lock_ptr(Ptr&& p, [[maybe_unused]] std::try_to_lock_t t)
  : unique_lock_ptr(std::move(p), std::defer_lock)
  {
    try_lock();
  }

  unique_lock_ptr(const Ptr& p, [[maybe_unused]] std::adopt_lock_t t)
  : ptr_(p),
    locked_(true)
  {
    if (ptr_ == nullptr) throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));
  }

  unique_lock_ptr(Ptr&& p, [[maybe_unused]] std::adopt_lock_t t)
  : ptr_(std::move(p)),
    locked_(true)
  {
    if (ptr_ == nullptr) throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));
  }

  ~unique_lock_ptr() noexcept {
    ensure_unlocked_();
  }

  void swap(unique_lock_ptr& o) noexcept(std::is_nothrow_swappable_v<Ptr>) {
    if constexpr(std::is_nothrow_swappable_v<Ptr>) {
      using std::swap;

      swap(ptr_, o.ptr_);
      swap(locked_, o.locked_);
    } else {
      // Use move operations, to maintain locked_ invariant.
      unique_lock_ptr tmp = std::move(o);
      o = std::move(*this);
      *this = std::move(tmp);
    }
  }

  auto mutex() const noexcept -> const Ptr& { return ptr_; }
  bool owns_lock() const noexcept { return locked_; }
  explicit operator bool() const noexcept { return owns_lock(); }

  auto operator*() const noexcept(noexcept(*std::declval<const Ptr&>())) -> element_type& {
    return *ptr_;
  }

  auto operator->() const noexcept(noexcept(std::declval<const Ptr&>().operator->())) -> decltype(std::declval<const Ptr&>().operator->()) {
    return ptr_.operator->();
  }

  void lock() {
    if (ptr_ == nullptr) throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));
    if (locked_) throw std::system_error(std::make_error_code(std::errc::resource_deadlock_would_occur));

    ptr_->lock();
    locked_ = true;
  }

  auto try_lock() -> bool {
    if (ptr_ == nullptr) throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));
    if (locked_) throw std::system_error(std::make_error_code(std::errc::resource_deadlock_would_occur));

    return locked_ = ptr_->try_lock();
  }

  void unlock() {
    if (ptr_ == nullptr) throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));
    if (!locked_) throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));

    ptr_->unlock();
    locked_ = false;
  }

  auto release() noexcept(std::is_nothrow_move_constructible_v<Ptr> || std::is_nothrow_copy_constructible_v<Ptr>) -> Ptr {
    Ptr ptr = std::move_if_noexcept(ptr_);
    locked_ = false;
    return ptr;
  }

  private:
  void ensure_unlocked_() noexcept(noexcept(std::declval<Ptr&>()->unlock())) {
    if (locked_) {
      ptr_->unlock();
      locked_ = false;
    }
  }

  Ptr ptr_ = nullptr;
  bool locked_ = false;
};


template<typename Ptr>
inline void swap(shared_lock_ptr<Ptr>& x, shared_lock_ptr<Ptr>& y) noexcept(noexcept(std::declval<Ptr&>().swap(std::declval<Ptr&>()))) {
  x.swap(y);
}

template<typename Ptr>
inline void swap(unique_lock_ptr<Ptr>& x, unique_lock_ptr<Ptr>& y) noexcept(noexcept(std::declval<Ptr&>().swap(std::declval<Ptr&>()))) {
  x.swap(y);
}


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_LOCKED_PTR_H */
