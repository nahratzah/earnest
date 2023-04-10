#pragma once

#include <cycle_ptr.h>

#include <asio/async_result.hpp>
#include <spdlog/spdlog.h>

#include <earnest/raw_db.h>
#include <earnest/db_address.h>
#include <earnest/db_error.h>
#include <earnest/file_id.h>

namespace earnest::detail {


template<typename RawDbType>
class file_grow_allocator {
  public:
  using raw_db_type = RawDbType;
  using executor_type = typename raw_db_type::executor_type;
  using allocator_type = typename raw_db_type::allocator_type;

  file_grow_allocator(const cycle_ptr::cycle_gptr<raw_db_type>& raw_db, const file_id& id)
  : raw_db_(raw_db),
    id_(id),
    ex_(raw_db->get_executor()),
    alloc_(raw_db->get_allocator()),
    lock_(raw_db->get_executor(), "file_grow_allocator{" + id.to_string() + "}", raw_db->get_allocator()),
    logger(get_logger())
  {}

  private:
  static auto get_logger() -> std::shared_ptr<spdlog::logger> {
    std::shared_ptr<spdlog::logger> logger = spdlog::get("earnest.file_grow_allocator");
    if (!logger) logger = std::make_shared<spdlog::logger>("earnest.file_grow_allocator", std::make_shared<spdlog::sinks::null_sink_mt>());
    return logger;
  }

  public:
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
        [](auto handler, typename raw_db_type::template fdb_transaction<FdbTxAlloc> fdb_tx, file_id id, monitor<executor_type, allocator_type> lock, cycle_ptr::cycle_weak_ptr<raw_db_type> weak_raw_db, std::size_t bytes, executor_type ex, std::shared_ptr<spdlog::logger> logger) -> void {
          if (fdb_tx.get_isolation() != isolation::read_commited) {
            logger->error("cannot use {} transaction level when calling file_grow_allocator: would not read the correct file size", fdb_tx.get_isolation());
            throw std::logic_error("must use read-commited transaction level");
          }

          cycle_ptr::cycle_gptr<raw_db_type> raw_db = weak_raw_db.lock();
          if (raw_db == nullptr) [[unlikely]] {
            std::invoke(
                completion_handler_fun(std::move(handler), std::move(ex)),
                make_error_code(db_errc::data_expired),
                db_address());
            return;
          }

          allocation_op_(std::move(fdb_tx), std::move(id), std::move(lock), std::move(bytes), logger)
          | completion_handler_fun(std::move(handler), std::move(ex));
        },
        token, std::move(fdb_tx), id_, lock_, raw_db_, std::move(bytes), get_executor(), logger);
  }

  private:
  template<typename FdbTxAlloc>
  static auto allocation_op_(typename raw_db_type::template fdb_transaction<FdbTxAlloc> fdb_tx, file_id id, monitor<executor_type, allocator_type> lock, std::size_t bytes, std::shared_ptr<spdlog::logger> logger) {
    return lock.dispatch_exclusive(asio::deferred)
    | asio::deferred(
        [logger, fdb_tx, id, bytes](typename monitor<executor_type, allocator_type>::exclusive_lock lock) mutable {
          logger->debug("{}: new allocation for {} bytes", id, bytes);
          return fdb_tx[id].async_file_size(asio::append(asio::deferred, std::move(lock)));
        })
    | asio::deferred(
        [logger, fdb_tx, id, bytes](std::error_code ec, auto file_size_bytes, typename monitor<executor_type, allocator_type>::exclusive_lock lock) mutable {
          logger->debug("{}: assigning offset {}", id, file_size_bytes);
          return asio::deferred.when(!ec)
              .then(fdb_tx[id].async_truncate(file_size_bytes + bytes, asio::append(asio::deferred, file_size_bytes, std::move(lock))))
              .otherwise(asio::deferred.values(ec, file_size_bytes, typename monitor<executor_type, allocator_type>::exclusive_lock{}));
        })
    | asio::deferred(
        [logger, fdb_tx, id](std::error_code ec, auto offset, typename monitor<executor_type, allocator_type>::exclusive_lock lock) mutable {
          if (!ec) {
            // We want the lock to be maintained until the transaction has commited.
            fdb_tx.on_commit(
                [logger, lock, offset, id]() mutable {
                  logger->debug("{}: releasing lock, for allocation offset {}", id, offset);
                  assert(lock.is_locked());
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
  const std::shared_ptr<spdlog::logger> logger;
};


} /* namespace earnest::detail */
