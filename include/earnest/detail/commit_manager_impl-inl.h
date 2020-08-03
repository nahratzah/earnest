#ifndef EARNEST_DETAIL_COMMIT_MANAGER_IMPL_INL_H
#define EARNEST_DETAIL_COMMIT_MANAGER_IMPL_INL_H

#include <utility>

namespace earnest::detail {


inline commit_manager_impl::write_id_state_impl_::write_id_state_impl_(commit_id seq, txfile::transaction&& tx) noexcept
: write_id_state_(std::move(seq), std::move(tx))
{}


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_COMMIT_MANAGER_IMPL_INL_H */
