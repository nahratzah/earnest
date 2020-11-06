#ifndef EARNEST_DB_INL_H
#define EARNEST_DB_INL_H

#include <utility>
#include <boost/polymorphic_pointer_cast.hpp>

namespace earnest {


inline db::transaction::transaction(transaction&& other) noexcept
: seq_(std::move(other.seq_)),
  read_only_(std::move(other.read_only_)),
  active_(std::exchange(other.active_, false)),
  callbacks_(std::move(other.callbacks_)),
  self_(std::move(other.self_)),
  deleted_set_(std::move(other.deleted_set_)),
  created_set_(std::move(other.created_set_)),
  require_set_(std::move(other.require_set_)),
  ops_(std::move(other.ops_))
{}

inline auto db::transaction::operator=(transaction&& other) noexcept -> transaction& {
  if (active_) rollback();

  seq_ = std::move(other.seq_);
  read_only_ = std::move(other.read_only_);
  active_ = std::exchange(other.active_, false);
  callbacks_ = std::move(other.callbacks_);
  self_ = std::move(other.self_);
  deleted_set_ = std::move(other.deleted_set_);
  created_set_ = std::move(other.created_set_);
  require_set_ = std::move(other.require_set_);
  ops_ = std::move(other.ops_);
  return *this;
}

inline db::transaction::~transaction() noexcept {
  if (active_) rollback();
}

template<typename T>
inline auto db::transaction::on(cycle_ptr::cycle_gptr<T> v) -> cycle_ptr::cycle_gptr<typename T::tx_object> {
  auto& txo = callbacks_[v];
  if (txo == nullptr) txo = cycle_ptr::make_cycle<typename T::tx_object>(*this, std::move(v));
  return boost::polymorphic_pointer_downcast<typename T::tx_object>(txo);
}

inline db::transaction::transaction(allocator_type alloc)
: callbacks_(4, alloc),
  deleted_set_(alloc),
  created_set_(alloc),
  require_set_(alloc),
  ops_(alloc)
{}

inline db::transaction::transaction(detail::commit_manager::commit_id seq, bool read_only, db& self, allocator_type alloc)
: seq_(seq),
  read_only_(read_only),
  active_(true),
  callbacks_(4, alloc),
  self_(self.shared_from_this()),
  deleted_set_(alloc),
  created_set_(alloc),
  require_set_(alloc),
  ops_(alloc)
{}

inline auto db::transaction::before(const transaction& other) const noexcept -> bool {
  return seq_ < other.seq_;
}

inline auto db::transaction::after(const transaction& other) const noexcept -> bool {
  return seq_ > other.seq_;
}

template<typename CommitFn>
inline auto db::transaction::on_commit(CommitFn&& commit_fn) -> transaction& {
  ops_.on_commit(std::forward<CommitFn>(commit_fn));
  return *this;
}

template<typename RollbackFn>
inline auto db::transaction::on_rollback(RollbackFn&& rollback_fn) -> transaction& {
  ops_.on_rollback(std::forward<RollbackFn>(rollback_fn));
  return *this;
}

template<typename CommitFn, typename RollbackFn>
inline auto db::transaction::on_complete(CommitFn&& commit_fn, RollbackFn&& rollback_fn) -> transaction& {
  ops_.on_complete(std::forward<CommitFn>(commit_fn), std::forward<RollbackFn>(rollback_fn));
  return *this;
}

inline auto db::transaction::operator+=(earnest::detail::tx_op_collection&& new_ops) -> transaction& {
  ops_ += std::move(new_ops);
  return *this;
}

inline auto db::transaction::get_allocator() const -> allocator_type {
  // Any of the collection types would yield the correct allocator.
  return ops_.get_allocator();
}


inline db::db_obj::db_obj(std::shared_ptr<class db> db)
: db_(db)
{}


} /* namespace earnest */

#endif /* EARNEST_DB_INL_H */
