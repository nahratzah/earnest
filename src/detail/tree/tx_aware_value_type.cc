#include <earnest/detail/tree/tx_aware_value_type.h>
#include <earnest/detail/tree/leaf.h>

namespace earnest::detail::tree {


tx_aware_value_type::~tx_aware_value_type() noexcept = default;

void tx_aware_value_type::encode(boost::asio::mutable_buffer buf) const {
  encode_tx_aware(boost::asio::buffer(buf, tx_aware_data::TX_AWARE_SIZE));
  encode_value(buf + tx_aware_data::TX_AWARE_SIZE);
}

void tx_aware_value_type::decode(boost::asio::const_buffer buf) {
  decode_tx_aware(boost::asio::buffer(buf, tx_aware_data::TX_AWARE_SIZE));
  decode_value(buf + tx_aware_data::TX_AWARE_SIZE);
}

auto tx_aware_value_type::is_never_visible() const noexcept -> bool {
  return this->tx_aware_data::is_never_visible();
}

auto tx_aware_value_type::offset() const -> std::uint64_t {
  return parent_->offset_for_idx_(slot_);
}

auto tx_aware_value_type::get_container_for_layout() const -> cycle_ptr::cycle_gptr<const detail::layout_obj> {
  std::shared_lock<const tx_aware_value_type> lck{ *this };
  return parent_;
}


} /* namespace earnest::detail::tree */
