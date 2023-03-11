#pragma once

#include <cycle_ptr.h>

#include <asio/async_result.hpp>

#include <earnest/raw_db.h>
#include <earnest/db_address.h>
#include <earnest/db_error.h>
#include <earnest/file_id.h>

namespace earnest::detail {


template<typename Executor, typename Allocator>
class file_grow_allocator {
  public:
  using raw_db_type = raw_db<Executor, Allocator>;
  using executor_type = typename raw_db_type::executor_type;
  using allocator_type = typename raw_db_type::allocator_type;

  file_grow_allocator(const cycle_ptr::cycle_gptr<raw_db_type>& raw_db, file_id id)
  : raw_db_(raw_db),
    id_(std::move(id)),
    ex_(raw_db->get_executor()),
    alloc_(raw_db->get_allocator()),
    lock_(raw_db->get_executor(), raw_db->get_allocator())
  {}

  auto id() const noexcept -> const file_id& {
    return id_;
  }

  auto get_executor() const -> executor_type {
    return ex_;
  }

  auto get_allocator() const -> allocator_type {
    return alloc_;
  }

  template<typename FdbTxAlloc, typename CompletionToken>
  auto async_allocate(typename raw_db_type::template fdb_transaction<FdbTxAlloc> fdb_tx, std::size_t bytes, CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(std::error_code, db_address)>(
        [](auto handler, typename raw_db_type::template fdb_transaction<FdbTxAlloc> fdb_tx, file_id id, monitor<executor_type, allocator_type> lock, cycle_ptr::cycle_weak_ptr<raw_db_type> weak_raw_db, std::size_t bytes, executor_type ex) -> void {
          cycle_ptr::cycle_gptr<raw_db_type> raw_db = weak_raw_db.lock();
          if (raw_db == nullptr) [[unlikely]] {
            std::invoke(
                completion_handler_fun(std::move(handler), std::move(ex)),
                make_error_code(db_errc::data_expired),
                db_address());
            return;
          }

          allocation_op_(std::move(fdb_tx), std::move(id), std::move(lock), std::move(bytes))
          | completion_handler_fun(std::move(handler), std::move(ex));
        },
        token, std::move(fdb_tx), id_, lock_, raw_db_, std::move(bytes), get_executor());
  }

  private:
  template<typename FdbTxAlloc>
  static auto allocation_op_(typename raw_db_type::template fdb_transaction<FdbTxAlloc> fdb_tx, file_id id, monitor<executor_type, allocator_type> lock, std::size_t bytes) {
    return lock.dispatch_exclusive(asio::deferred)
    | asio::deferred(
        [fdb_tx, id](typename monitor<executor_type, allocator_type>::exclusive_lock lock) mutable {
          return fdb_tx[id].async_file_size(asio::append(asio::deferred, std::move(lock)));
        })
    | asio::deferred(
        [fdb_tx, id, bytes](std::error_code ec, auto file_size_bytes, typename monitor<executor_type, allocator_type>::exclusive_lock lock) mutable {
          return asio::deferred.when(!ec)
              .then(fdb_tx[id].async_truncate(file_size_bytes + bytes, asio::append(asio::deferred, file_size_bytes, std::move(lock))))
              .otherwise(asio::deferred.values(ec, file_size_bytes, typename monitor<executor_type, allocator_type>::exclusive_lock{}));
        })
    | asio::deferred(
        [fdb_tx, id](std::error_code ec, auto offset, typename monitor<executor_type, allocator_type>::exclusive_lock lock) mutable {
          if (!ec) {
            // We want the lock to be maintained until the transaction has commited.
            fdb_tx.on_commit(
                [lock]() mutable {
                  lock.reset();
                });
          }

          return asio::deferred.values(ec, db_address(id, offset));
        });
  }

  cycle_ptr::cycle_weak_ptr<raw_db_type> raw_db_;
  file_id id_;
  executor_type ex_;
  allocator_type alloc_;
  monitor<executor_type, allocator_type> lock_;
};


} /* namespace earnest::detail */
