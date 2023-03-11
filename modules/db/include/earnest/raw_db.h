#pragma once

#include <memory>
#include <utility>

#include <cycle_ptr.h>

#include <earnest/db_cache.h>
#include <earnest/dir.h>
#include <earnest/file_db.h>

namespace earnest {


template<typename Executor, typename Allocator = std::allocator<std::byte>>
class raw_db
: private cycle_ptr::cycle_base
{
  public:
  using executor_type = Executor;
  using allocator_type = Allocator;
  using file_db_type = file_db<executor_type, allocator_type>;
  using cache_type = db_cache<executor_type, allocator_type>;
  using cache_allocator_type = typename cache_type::allocator_type;
  template<typename T> using key_type = typename db_cache<executor_type, allocator_type>::template key_type<T>;
  template<typename TxAllocator> using fdb_transaction = transaction<file_db_type, TxAllocator>;

  explicit raw_db(executor_type ex, allocator_type alloc = allocator_type())
  : fdb_(std::allocate_shared<file_db_type>(alloc, ex, alloc)),
    cache_(*this, ex, alloc)
  {}

  template<typename CompletionToken>
  auto async_create(dir d, CompletionToken&& token) {
    return fdb_->async_create(std::move(d), std::forward<CompletionToken>(token));
  }

  template<typename CompletionToken>
  auto async_open(dir d, CompletionToken&& token) {
    return fdb_->async_open(std::move(d), std::forward<CompletionToken>(token));
  }

  auto get_executor() const -> executor_type { return cache_.get_executor(); }
  auto get_allocator() const -> allocator_type { return fdb_->get_allocator(); }
  auto get_cache_allocator() const -> cache_allocator_type { return cache_.get_allocator(); }
  auto cache_max_mem() const noexcept -> std::size_t { return cache_.max_mem(); }
  auto cache_max_mem(std::size_t new_max_mem) noexcept -> std::size_t { return cache_.max_mem(new_max_mem); }

  template<typename Alloc, typename T, typename CompletionToken, typename Fn, typename AcceptanceFn, typename... Args>
  auto async_get([[maybe_unused]] std::allocator_arg_t aa, Alloc alloc, key_type<T> k, CompletionToken&& token, Fn&& fn, AcceptanceFn&& acceptance_fn, Args&&... args) {
    return asio::async_initiate<CompletionToken, void(std::error_code, cycle_ptr::cycle_gptr<T>)>(
        []<typename... LambdaArgs>(auto handler, cache_type cache, std::shared_ptr<file_db_type> fdb, key_type<T> k, Alloc alloc, auto fn, auto acceptance_fn, LambdaArgs&&... args) {
          return cache.async_get(
              k,
              std::move(handler),
              [fdb, k, alloc]<typename... LambdaLambdaArgs>(auto fn, auto acceptance_fn, LambdaLambdaArgs&&... args) {
                auto file = fdb->tx_begin(isolation::read_commited, tx_mode::read_only, alloc)[k.file];
                using stream_type = detail::positional_stream_adapter<decltype(file)>;

                return std::invoke(fn, stream_type(std::move(file), k.offset), std::forward<LambdaLambdaArgs>(args)...)
                | asio::deferred(
                    [acceptance_fn=std::move(acceptance_fn)](std::error_code ec, cycle_ptr::cycle_gptr<T> new_obj) {
                      return std::invoke(acceptance_fn)
                      | asio::deferred(
                          [ lookup_ec=std::move(ec),
                            new_obj=std::move(new_obj)
                          ](std::error_code acceptance_ec) {
                            // Acceptance errors override any regular errors.
                            std::error_code ec = (!acceptance_ec ? acceptance_ec : lookup_ec);
                            return asio::deferred.values(std::move(ec), new_obj);
                          });
                    });
              },
              std::move(fn), std::move(acceptance_fn), std::forward<LambdaArgs>(args)...);
        },
        token, this->cache_, this->fdb_, std::move(k), std::move(alloc), std::forward<Fn>(fn), std::forward<AcceptanceFn>(acceptance_fn), std::forward<Args>(args)...);
  }

  template<typename T, typename CompletionToken, typename Fn, typename AcceptanceFn, typename... Args>
  auto async_get(key_type<T> k, CompletionToken&& token, Fn&& fn, AcceptanceFn&& acceptance_fn, Args&&... args) {
    return async_get(std::allocator_arg, std::allocator<std::byte>(), std::move(k), std::forward<CompletionToken>(token), std::forward<Fn>(fn), std::forward<AcceptanceFn>(acceptance_fn), std::forward<Args>(args)...);
  }

  template<typename T>
  auto emplace(key_type<T> k, cycle_ptr::cycle_gptr<T> v) -> void {
    cache_.emplace(std::move(k), std::move(v));
  }

  template<typename... Args>
  auto fdb_tx_begin(Args&&... args) {
    return fdb_->tx_begin(std::forward<Args>(args)...);
  }

  private:
  std::shared_ptr<file_db_type> fdb_;
  cache_type cache_;
};


} /* namespace earnest */
