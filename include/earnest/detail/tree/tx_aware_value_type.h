#ifndef EARNEST_DETAIL_TREE_TX_AWARE_VALUE_TYPE_H
#define EARNEST_DETAIL_TREE_TX_AWARE_VALUE_TYPE_H

#include <earnest/detail/tree/fwd.h>
#include <earnest/detail/tree/value_type.h>
#include <earnest/tx_aware_data.h>
#include <cstdint>

namespace earnest::detail::tree {


class earnest_export_ tx_aware_value_type
: public value_type,
  public tx_aware_data
{
  public:
  using value_type::value_type;

  ~tx_aware_value_type() noexcept override;

  void lock() override final { mtx_.lock(); }
  auto try_lock() -> bool override final { return mtx_.try_lock(); }
  void unlock() override final { mtx_.unlock(); }
  void lock_shared() const override final { mtx_.lock_shared(); }
  auto try_lock_shared() const -> bool override final { return mtx_.try_lock_shared(); }
  void unlock_shared() const override final { mtx_.unlock_shared(); }

  auto is_never_visible() const noexcept -> bool override final;

  private:
  auto offset() const -> std::uint64_t override final;
};


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_TX_AWARE_VALUE_TYPE_H */
