#include <earnest/detail/tree/tx_aware_value_type.h>
#include <earnest/detail/tree/leaf.h>

namespace earnest::detail::tree {


tx_aware_value_type::~tx_aware_value_type() noexcept = default;

auto tx_aware_value_type::is_never_visible() const noexcept -> bool {
  return this->tx_aware_data::is_never_visible();
}

auto tx_aware_value_type::offset() const -> std::uint64_t {
  return parent_->offset_for_idx_(slot_);
}


} /* namespace earnest::detail::tree */
