#include <earnest/detail/tree/loader_impl.h>

namespace earnest::detail::tree {


augment_impl<>::~augment_impl() noexcept = default;

void augment_impl<>::do_encode(boost::asio::mutable_buffer buf) const {}
void augment_impl<>::do_decode(boost::asio::const_buffer buf) {}


} /* namespace earnest::detail::tree */
