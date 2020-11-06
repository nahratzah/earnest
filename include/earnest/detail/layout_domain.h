#ifndef EARNEST_DETAIL_LAYOUT_DOMAIN_H
#define EARNEST_DETAIL_LAYOUT_DOMAIN_H

#include <earnest/detail/export_.h>
#include <utility>
#include <mutex>
#include <system_error>

namespace earnest {
class db;
} /* namespace earnest */
namespace earnest::detail {


class layout_domain;
class layout_lock;

class earnest_export_ layout_obj {
  friend earnest::db;
  friend layout_lock;

  public:
  virtual ~layout_obj() noexcept = 0;

  protected:
  virtual auto get_layout_domain() const noexcept -> const layout_domain& = 0;

  private:
  virtual void lock_layout() const = 0;
  virtual bool try_lock_layout() const = 0;
  virtual void unlock_layout() const = 0;
};

class earnest_export_ layout_domain {
  protected:
  layout_domain() noexcept = default;
  virtual ~layout_domain() noexcept;

  public:
  virtual auto less_compare(const layout_obj& x, const layout_obj& y) const -> bool = 0;
};

class earnest_export_ layout_lock {
  public:
  constexpr layout_lock() noexcept
  : lobj_(nullptr),
    locked_(false)
  {}

  explicit layout_lock(const layout_obj& lobj)
  : lobj_(&lobj),
    locked_(true)
  {
    lobj_->lock_layout();
  }

  constexpr layout_lock(const layout_obj& lobj, [[maybe_unused]] std::defer_lock_t lock_how) noexcept
  : lobj_(&lobj),
    locked_(false)
  {}

  layout_lock(const layout_obj& lobj, [[maybe_unused]] std::try_to_lock_t lock_how)
  : lobj_(&lobj),
    locked_(lobj.try_lock_layout())
  {}

  constexpr layout_lock(const layout_obj& lobj, [[maybe_unused]] std::adopt_lock_t lock_how) noexcept
  : lobj_(&lobj),
    locked_(true)
  {}

  layout_lock(const layout_lock&) = delete;
  layout_lock& operator=(const layout_lock&) = delete;

  layout_lock(layout_lock&& y) noexcept
  : lobj_(std::exchange(y.lobj_, nullptr)),
    locked_(std::exchange(y.locked_, false))
  {}

  auto operator=(layout_lock&& y) noexcept -> layout_lock& {
    if (&y != this) {
      if (locked_) lobj_->unlock_layout();
      lobj_ = std::exchange(y.lobj_, nullptr);
      locked_ = std::exchange(y.locked_, false);
    }
    return *this;
  }

  ~layout_lock() {
    if (locked_) lobj_->unlock_layout();
  }

  constexpr auto owns_lock() const noexcept -> bool { return locked_; }
  constexpr explicit operator bool() const noexcept { return locked_; }
  constexpr auto mutex() const noexcept -> const layout_obj* { return lobj_; }

  void lock();
  bool try_lock();
  void unlock();

  friend void swap(layout_lock& x, layout_lock& y) noexcept {
    using std::swap;

    swap(x.lobj_, y.lobj_);
    swap(x.locked_, y.locked_);
  }

  private:
  const layout_obj* lobj_;
  bool locked_;
};


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_LAYOUT_DOMAIN_H */
