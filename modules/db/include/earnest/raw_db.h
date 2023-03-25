#pragma once

#include <cstdint>
#include <memory>
#include <utility>

#include <cycle_ptr.h>
#include <prometheus/registry.h>

#include <earnest/db_cache.h>
#include <earnest/db_error.h>
#include <earnest/detail/completion_handler_fun.h>
#include <earnest/detail/completion_wrapper.h>
#include <earnest/detail/handler_traits.h>
#include <earnest/detail/move_only_function.h>
#include <earnest/dir.h>
#include <earnest/file_db.h>

namespace earnest {


template<typename Executor, typename Allocator = std::allocator<std::byte>>
class raw_db
: protected cycle_ptr::cycle_base
{
  public:
  // Database session number is incremented, each time the database is opened.
  // The number will wrap around once it reaches its maximum.
  using session_number = std::uint32_t;

  using executor_type = Executor;
  using allocator_type = detail::prom_allocator<Allocator>;
  using file_db_type = file_db<executor_type, allocator_type>;
  using cache_type = db_cache<executor_type, Allocator>;
  using cache_allocator_type = typename cache_type::allocator_type;
  template<typename T> using key_type = typename cache_type::template key_type<T>;
  template<typename TxAllocator> using fdb_transaction = transaction<file_db_type, TxAllocator>;

  private:
  explicit raw_db(executor_type ex, std::shared_ptr<prometheus::Registry> prom_registry, std::string_view prom_db_name, Allocator alloc, allocator_type prom_alloc)
  : fdb_(std::allocate_shared<file_db_type>(prom_alloc, ex, prom_alloc)),
    cache_(*this, prom_registry, prom_db_name, ex, alloc)
  {}

  public:
  explicit raw_db(executor_type ex, std::shared_ptr<prometheus::Registry> prom_registry, std::string_view prom_db_name, Allocator alloc = Allocator())
  : raw_db(
      std::move(ex), prom_registry, prom_db_name, alloc,
      allocator_type(prom_registry, "earnest_db_memory", prometheus::Labels{{"db_name", std::string(prom_db_name)}}, alloc))
  {}

  explicit raw_db(executor_type ex, Allocator alloc = Allocator())
  : raw_db(std::move(ex), nullptr, std::string_view(), std::move(alloc))
  {}

  static auto session_number_fileid() -> file_id {
    return file_id("", "session_number");
  }

  template<typename CompletionToken>
  auto async_create(dir d, CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, cycle_ptr::cycle_gptr<raw_db> self, dir d) -> void {
          if constexpr(detail::handler_has_executor_v<decltype(handler)>)
            self->async_create_impl_(std::move(d), detail::completion_handler_fun(std::move(handler)));
          else
            self->async_create_impl_(std::move(d), std::move(handler));
        },
        token, this->shared_from_this(this), std::move(d));
  }

  private:
  auto async_create_impl_(dir d, detail::move_only_function<void(std::error_code)> handler) -> void {
    fdb_->async_create(
        std::move(d),
        detail::completion_wrapper<void(std::error_code)>(
            std::move(handler),
            [self=this->shared_from_this(this)](auto handler, std::error_code ec) -> void {
              if (ec) {
                std::invoke(handler, ec);
                return;
              }

              struct state {
                state(const cycle_ptr::cycle_gptr<raw_db>& self)
                : tx(self->fdb_tx_begin(isolation::read_commited, tx_mode::read_write, self->get_allocator())),
                  session_number_file(this->tx[session_number_fileid()])
                {}

                fdb_transaction<allocator_type> tx;
                typename fdb_transaction<allocator_type>::file session_number_file;
              };

              self->session_number_ = 0;
              std::shared_ptr<state> state_ptr = std::allocate_shared<state>(self->get_allocator(), self);
              state_ptr->session_number_file.async_create(asio::append(asio::deferred, state_ptr))
              | asio::deferred(
                  [](std::error_code ec, std::shared_ptr<state> state_ptr) {
                    return asio::deferred.when(!ec)
                        .then(state_ptr->session_number_file.async_truncate(sizeof(session_number), asio::append(asio::deferred, state_ptr)))
                        .otherwise(asio::deferred.values(ec, state_ptr));
                  })
              | asio::deferred(
                  [](std::error_code ec, std::shared_ptr<state> state_ptr) {
                    return asio::deferred.when(!ec)
                        .then(state_ptr->tx.async_commit(asio::deferred))
                        .otherwise(asio::deferred.values(ec));
                  })
              | std::move(handler);
            }));
  }

  public:
  template<typename CompletionToken>
  auto async_open(dir d, CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, cycle_ptr::cycle_gptr<raw_db> self, dir d) -> void {
          if constexpr(detail::handler_has_executor_v<decltype(handler)>)
            self->async_open_impl_(std::move(d), detail::completion_handler_fun(std::move(handler)));
          else
            self->async_open_impl_(std::move(d), std::move(handler));
        },
        token, this->shared_from_this(this), std::move(d));
  }

  private:
  auto async_open_impl_(dir d, detail::move_only_function<void(std::error_code)> handler) -> void {
    fdb_->async_open(
        std::move(d),
        detail::completion_wrapper<void(std::error_code)>(
            std::move(handler),
            [self=this->shared_from_this(this)](auto handler, std::error_code ec) {
              if (ec) {
                std::invoke(handler, ec);
                return;
              }

              struct state {
                state(const cycle_ptr::cycle_gptr<raw_db>& self)
                : self(self),
                  tx(self->fdb_tx_begin(isolation::read_commited, tx_mode::read_write, self->get_allocator())),
                  session_number_file(this->tx[session_number_fileid()])
                {}

                cycle_ptr::cycle_gptr<raw_db> self;
                fdb_transaction<allocator_type> tx;
                typename fdb_transaction<allocator_type>::file session_number_file;
                std::uint64_t new_session_number_be;
              };

              std::shared_ptr<state> state_ptr = std::allocate_shared<state>(self->get_allocator(), self);
              state_ptr->session_number_file.async_file_size(asio::append(asio::deferred, state_ptr))
              | asio::deferred(
                  [](std::error_code ec, std::uint64_t filesize, std::shared_ptr<state> state_ptr) {
                    if (!ec && filesize != sizeof(session_number))
                      ec = make_error_code(db_errc::bad_database);
                    return asio::deferred.values(ec, state_ptr);
                  })
              | asio::deferred(
                  [](std::error_code ec, std::shared_ptr<state> state_ptr) {
                    return asio::deferred.when(!ec)
                        .then(asio::async_read_at(state_ptr->session_number_file, 0u, asio::buffer(&state_ptr->self->session_number), asio::append(asio::deferred, state_ptr)))
                        .otherwise(asio::deferred.values(ec, state_ptr));
                  })
              | asio::deferred(
                  [](std::error_code ec, std::shared_ptr<state> state_ptr) {
                    if (!ec) boost::endian::big_to_native_inplace(state_ptr->self->session_number);
                    return asio::deferred.values(ec, state_ptr);
                  })
              | asio::deferred(
                  [](std::error_code ec, std::shared_ptr<state> state_ptr) {
                    state_ptr->new_session_number_be = state_ptr->self->session_number + 1u;
                    boost::endian::native_to_big_inplace(state_ptr->new_session_number_be);

                    return asio::deferred.when(!ec)
                        .then(asio::async_write_at(state_ptr->session_number_file, 0, asio::buffer(&state_ptr->new_session_number_be), asio::append(asio::deferred, state_ptr)))
                        .otherwise(asio::deferred.values(ec, state_ptr));
                  })
              | asio::deferred(
                  [](std::error_code ec, std::shared_ptr<state> state_ptr) {
                    return asio::deferred.when(!ec)
                        .then(state_ptr->tx.async_commit(asio::deferred))
                        .otherwise(asio::deferred.values(ec));
                  })
              | asio::deferred(
                  [](std::error_code ec, std::shared_ptr<state> state_ptr) {
                    if (!ec) std::clog << "DB opened, previous session << " << state_ptr->self->session_number << ", new session " << boost::endian::big_to_native(state_ptr->new_session_number_be) << "\n";
                    return asio::deferred.values(ec);
                  })
              | std::move(handler);
            }));
  }

  public:
  auto get_executor() const -> executor_type { return cache_.get_executor(); }
  auto get_allocator() const -> allocator_type { return fdb_->get_allocator(); }
  auto get_cache_allocator() const -> cache_allocator_type { return cache_.get_allocator(); }
  auto cache_max_mem() const noexcept -> std::size_t { return cache_.max_mem(); }
  auto cache_max_mem(std::size_t new_max_mem) noexcept -> std::size_t { return cache_.max_mem(new_max_mem); }
  auto get_session_number() const noexcept -> session_number { return session_number_; }

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
  session_number session_number_ = 0;
};


} /* namespace earnest */
