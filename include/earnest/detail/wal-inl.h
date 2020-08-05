#ifndef EARNEST_DETAIL_WAL_INL_H
#define EARNEST_DETAIL_WAL_INL_H

namespace earnest::detail {


template<typename CommitFn>
inline auto wal_region::tx::on_commit(CommitFn&& commit_fn) -> tx& {
  ops_.on_commit(std::forward<CommitFn>(commit_fn));
  return *this;
}

template<typename RollbackFn>
inline auto wal_region::tx::on_rollback(RollbackFn&& rollback_fn) -> tx& {
  ops_.on_rollback(std::forward<RollbackFn>(rollback_fn));
  return *this;
}

template<typename CommitFn, typename RollbackFn>
inline auto wal_region::tx::on_complete(CommitFn&& commit_fn, RollbackFn&& rollback_fn) -> tx& {
  ops_.on_complete(std::forward<CommitFn>(commit_fn), std::forward<RollbackFn>(rollback_fn));
  return *this;
}

inline auto wal_region::tx::operator+=(earnest::detail::tx_op_collection&& new_ops) -> tx& {
  ops_ += std::move(new_ops);
  return *this;
}


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_WAL_INL_H */
