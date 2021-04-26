#ifndef EARNEST_DETAIL_CLOSEABLE_TRANSACTION_H
#define EARNEST_DETAIL_CLOSEABLE_TRANSACTION_H

#include <utility>

namespace earnest::detail {


template<typename Closeable>
class closeable_transaction {
  public:
  constexpr closeable_transaction() noexcept
  : closeable_(nullptr)
  {}

  explicit closeable_transaction(Closeable& fd) noexcept
  : closeable_(&fd)
  {}

  closeable_transaction(const closeable_transaction&) = delete;

  closeable_transaction(closeable_transaction&& o) noexcept
  : closeable_(std::exchange(o.closeable_, nullptr))
  {}

  ~closeable_transaction() {
    if (closeable_ != nullptr) closeable_->close();
  }

  void commit() { closeable_ = nullptr; }

  private:
  Closeable* closeable_;
};


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_CLOSEABLE_TRANSACTION_H */
