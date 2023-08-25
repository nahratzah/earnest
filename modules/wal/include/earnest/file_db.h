#pragma once

#include <cassert>
#include <functional>
#include <initializer_list>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <asio/append.hpp>
#include <asio/async_result.hpp>
#include <asio/bind_executor.hpp>
#include <asio/completion_condition.hpp>
#include <asio/consign.hpp>
#include <asio/deferred.hpp>
#include <asio/prepend.hpp>
#include <asio/strand.hpp>
#include <asio/write.hpp>
#include <asio/write_at.hpp>
#include <prometheus/counter.h>
#include <prometheus/registry.h>

#include <earnest/detail/byte_positional_stream.h>
#include <earnest/detail/completion_handler_fun.h>
#include <earnest/detail/completion_wrapper.h>
#include <earnest/detail/deferred_on_executor.h>
#include <earnest/detail/file_recover_state.h>
#include <earnest/detail/forward_deferred.h>
#include <earnest/detail/handler_traits.h>
#include <earnest/detail/monitor.h>
#include <earnest/detail/move_only_function.h>
#include <earnest/detail/namespace_map.h>
#include <earnest/detail/positional_stream_adapter.h>
#include <earnest/detail/replacement_map.h>
#include <earnest/detail/replacement_map_reader.h>
#include <earnest/detail/wal_file.h>
#include <earnest/detail/wal_records.h>
#include <earnest/file_db_error.h>
#include <earnest/file_id.h>
#include <earnest/isolation.h>
#include <earnest/tx_mode.h>

namespace earnest::detail {


template<typename Alloc>
class on_commit_events {
  private:
  using function_type = move_only_function<void()>;

  public:
  using allocator_type = Alloc;

  explicit on_commit_events(allocator_type alloc)
  : events_(std::move(alloc))
  {}

  auto get_allocator() const -> allocator_type {
    return events_.get_allocator();
  }

  template<typename Fn>
  auto add(Fn&& fn) -> void {
    std::lock_guard lck{mtx_};
    events_.emplace_back(std::forward<Fn>(fn));
  }

  auto run() {
    std::lock_guard lck{mtx_};
    std::for_each(
        events_.begin(), events_.end(),
        [](const auto& event_fn) {
          std::invoke(event_fn);
        });
    events_.clear();
  }

  private:
  std::mutex mtx_;
  std::vector<std::function<void()>, typename std::allocator_traits<Alloc>::template rebind_alloc<std::function<void()>>> events_;
};


} /* namespace earnest::detail */

namespace earnest {


template<typename FileDB, typename Allocator = std::allocator<std::byte>>
class transaction;


/**
 * A database that allows for transactional access to files.
 *
 * Supported transaction isolation levels:
 * - read-committed
 * - repeatable-read
 */
template<typename Executor, typename Allocator = std::allocator<std::byte>>
class file_db
: public std::enable_shared_from_this<file_db<Executor, Allocator>>
{
  template<typename, typename> friend class transaction;

  public:
  using executor_type = Executor;
  using allocator_type = Allocator;
  static inline const std::string_view namespaces_filename = "namespaces.fdb";

  private:
  template<typename T> using rebind_alloc = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;
  using wal_type = detail::wal_file<executor_type, allocator_type>;
  using fd_type = fd<executor_type>;
  using variant_type = typename wal_type::variant_type;
  using write_variant_type = typename wal_type::write_variant_type;

  struct ns {
    dir d;
    std::filesystem::path dirname;
  };

  struct file {
    file(file_db& fdb, [[maybe_unused]] const file_id& id, bool exists_logically = false)
    : fd(std::allocate_shared<fd_type>(fdb.get_allocator(), fdb.get_executor())),
      exists_logically(exists_logically)
    {}

    std::shared_ptr<fd_type> fd;
    bool exists_logically = false;
    std::uint64_t file_size = 0;
  };

  public:
  explicit file_db(executor_type ex, std::shared_ptr<prometheus::Registry> prom_registry, std::string_view db_name, allocator_type alloc = allocator_type())
  : alloc_(alloc),
    wal(),
    namespaces(alloc),
    files(alloc),
    strand_(std::move(ex)),
    successful_commits_(build_commit_metric_(prom_registry, db_name, "success")),
    failed_commits_(build_commit_metric_(prom_registry, db_name, "fail"))
  {}

  explicit file_db(executor_type ex, allocator_type alloc = allocator_type())
  : file_db(std::move(ex), nullptr, std::string_view(), std::move(alloc))
  {}

  auto get_executor() const -> executor_type { return strand_.get_inner_executor(); }
  auto get_allocator() const -> allocator_type { return alloc_; }

  private:
  auto async_create_impl_(dir d, detail::move_only_function<void(std::error_code)> handler) -> void {
    using ::earnest::detail::completion_handler_fun;
    using ::earnest::detail::completion_wrapper;

    detail::deferred_on_executor<void(std::error_code, std::shared_ptr<file_db>)>(
        [](std::shared_ptr<file_db> fdb, dir d) {
          assert(fdb->strand_.running_in_this_thread());

          const bool wal_is_nil = (fdb->wal == nullptr);
          if (wal_is_nil) fdb->wal = std::allocate_shared<wal_type>(fdb->get_allocator(), fdb->get_executor(), fdb->get_allocator());
          return asio::deferred.when(wal_is_nil)
              .then(
                  fdb->wal->async_create(
                      std::move(d),
                      asio::append(asio::deferred, fdb)))
              .otherwise(asio::deferred.values(make_error_code(wal_errc::bad_state), fdb));
        },
        strand_, this->shared_from_this(), std::move(d))
    | asio::deferred(
        [](std::error_code ec, std::shared_ptr<file_db> fdb) {
          return asio::deferred.when(!ec)
              .then(fdb->create_ns_file_op_() | asio::append(asio::deferred, fdb))
              .otherwise(asio::deferred.values(ec, fdb));
        })
    | asio::bind_executor(
        strand_,
        asio::deferred(
            [](std::error_code ec, std::shared_ptr<file_db> fdb) {
              return asio::deferred.when(!ec)
                  .then(fdb->recover_namespaces_op_())
                  .otherwise(asio::deferred.values(ec));
            }))
    | asio::deferred(
        [fdb=this->shared_from_this()](std::error_code ec) {
          if (!ec) fdb->install_apply_();
          return asio::deferred.values(ec);
        })
    | undo_on_fail_op_()
    | std::move(handler);
  }

  public:
  template<typename CompletionToken>
  auto async_create(dir d, CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, std::shared_ptr<file_db> self, dir d) -> void {
          if constexpr(detail::handler_has_executor_v<decltype(handler)>) {
            self->async_create_impl_(std::move(d), completion_handler_fun(std::move(handler), self->get_executor()));
          } else {
            self->async_create_impl_(std::move(d), std::move(handler));
          }
        },
        token, this->shared_from_this(), std::move(d));
  }

  private:
  auto async_open_impl_(dir d, detail::move_only_function<void(std::error_code)> handler) -> void {
    using ::earnest::detail::completion_handler_fun;
    using ::earnest::detail::completion_wrapper;

    return detail::deferred_on_executor<void(std::error_code, std::shared_ptr<file_db>)>(
        [](std::shared_ptr<file_db> fdb, dir d) {
          assert(fdb->strand_.running_in_this_thread());

          const bool wal_is_nil = (fdb->wal == nullptr);
          if (wal_is_nil) fdb->wal = std::allocate_shared<wal_type>(fdb->get_allocator(), fdb->get_executor(), fdb->get_allocator());
          return asio::deferred.when(wal_is_nil)
              .then(
                  fdb->wal->async_open(
                      std::move(d),
                      asio::append(asio::deferred, fdb)))
              .otherwise(asio::deferred.values(make_error_code(wal_errc::bad_state), fdb));
        },
        strand_, this->shared_from_this(), std::move(d))
    | asio::bind_executor(
        strand_,
        asio::deferred(
            [](std::error_code ec, std::shared_ptr<file_db> fdb) {
              return asio::deferred.when(!ec)
                  .then(fdb->recover_namespaces_op_())
                  .otherwise(asio::deferred.values(ec));
            }))
    | asio::deferred(
        [fdb=this->shared_from_this()](std::error_code ec) {
          if (!ec) fdb->install_apply_();
          return asio::deferred.values(ec);
        })
    | undo_on_fail_op_()
    | std::move(handler);
  }

  public:
  template<typename CompletionToken>
  auto async_open(dir d, CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, std::shared_ptr<file_db> self, dir d) -> void {
          if constexpr(detail::handler_has_executor_v<decltype(handler)>) {
            self->async_open_impl_(std::move(d), completion_handler_fun(std::move(handler), self->get_executor()));
          } else {
            self->async_open_impl_(std::move(d), std::move(handler));
          }
        },
        token, this->shared_from_this(), std::move(d));
  }

  template<typename Acceptor, typename CompletionToken>
  requires std::invocable<Acceptor, typename wal_type::record_type>
  auto async_records(Acceptor&& acceptor, CompletionToken&& token) const {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, std::shared_ptr<const file_db> fdb, auto acceptor) {
          fdb->strand_.dispatch(
              [fdb, acceptor=std::move(acceptor), handler=std::move(handler)]() mutable -> void {
                if (fdb->wal == nullptr) [[unlikely]] {
                  std::invoke(handler, make_error_code(wal_errc::bad_state));
                  return;
                }

                fdb->wal->async_records(std::move(acceptor), std::move(handler));
              },
              fdb->get_allocator());
        },
        token, this->shared_from_this(), std::forward<Acceptor>(acceptor));
  }

  template<typename TxAllocator = std::allocator<std::byte>>
  auto tx_begin(isolation i, tx_mode m = tx_mode::read_write, TxAllocator alloc = TxAllocator()) -> transaction<file_db, TxAllocator> {
    return transaction<file_db, TxAllocator>(
        this->shared_from_this(),
        i,
        m,
        std::move(alloc));
  }

  template<typename CompletionToken>
  auto async_wal_rollover(CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, std::shared_ptr<file_db> fdb) {
          asio::dispatch(
              completion_wrapper<void()>(
                  detail::completion_handler_fun(std::move(handler), fdb->get_executor(), fdb->strand_, fdb->get_allocator()),
                  [fdb](auto handler) {
                    fdb->wal->async_rollover(std::move(handler).as_unbound_handler());
                  }));
        },
        token, this->shared_from_this());
  }

  auto async_wal_rollover() -> void {
    async_wal_rollover(
        []([[maybe_unused]] std::error_code ec) {
          // skip
        });
  }

  auto auto_rollover_size(std::size_t new_size) const -> void {
    wal->auto_rollover_size(new_size);
  }

  private:
  auto undo_on_fail_op_() {
    return asio::deferred(
        [fdb=this->shared_from_this()](std::error_code ec) {
          return asio::deferred.when(!ec)
              .then(asio::deferred.values(ec))
              .otherwise(
                  detail::deferred_on_executor<void(std::error_code)>(
                      [fdb, ec]() {
                        fdb->wal.reset();
                        fdb->namespaces.clear();
                        fdb->files.clear();
                        return asio::deferred.values(ec);
                      },
                      fdb->strand_));
        });
  }

  auto create_ns_file_op_() {
    using ::earnest::detail::namespace_map;
    using ::earnest::detail::wal_record_create_file;
    using ::earnest::detail::wal_record_modify_file_write32;
    using ::earnest::detail::wal_record_truncate_file;
    using byte_vector = std::vector<std::byte>;

    return asio::deferred.values()
    | asio::deferred(
        [fdb=this->shared_from_this()]() mutable {
          return asio::async_initiate<decltype(asio::deferred), void(std::error_code, byte_vector)>(
              [](auto handler, std::shared_ptr<file_db> fdb) {
                auto stream_ptr = std::make_unique<byte_stream<executor_type>>(fdb->get_executor());
                auto& stream_ref = *stream_ptr;
                async_write(
                    stream_ref,
                    xdr_writer<>() & xdr_constant(namespace_map<>{ .files={ file_id("", std::string(namespaces_filename)) } }),
                    ::earnest::detail::completion_wrapper<void(std::error_code)>(
                        std::move(handler),
                        [stream_ptr=std::move(stream_ptr)](auto handler, std::error_code ec) {
                          std::invoke(handler, ec, std::move(*stream_ptr).data());
                        }));
              },
              asio::deferred, std::move(fdb));
        })
    | asio::bind_executor(
        strand_,
        asio::deferred(
            [fdb=this->shared_from_this()](std::error_code ec, byte_vector ns_bytes) {
              assert(fdb->strand_.running_in_this_thread());

              const auto ns_file_size = ns_bytes.size();
              const auto ns_file_id = file_id("", std::string(namespaces_filename));
              return asio::deferred.when(!ec)
                  .then(
                      fdb->wal->async_append(
                          std::initializer_list<write_variant_type>{
                          wal_record_create_file{
                            .file=ns_file_id
                          },
                          wal_record_truncate_file{
                            .file=ns_file_id,
                            .new_size=ns_file_size
                          },
                          wal_record_modify_file_write32{
                            .file=ns_file_id,
                            .file_offset=0,
                            .data=std::move(ns_bytes)
                          }
                        },
                        asio::deferred))
                  .otherwise(asio::deferred.values(ec));
            }));
  }

  struct namespace_recover_state
  : public detail::file_recover_state<typename wal_type::fd_type, allocator_type> {
    namespace_recover_state(executor_type ex, const dir& d, const std::filesystem::path filename, open_mode m = open_mode::READ_WRITE, allocator_type alloc = allocator_type())
    : detail::file_recover_state<typename wal_type::fd_type, allocator_type>(std::move(alloc)),
      actual_file(std::move(ex))
    {
      std::error_code ec;
      actual_file.open(d, filename, m, ec);
      if (ec == make_error_code(std::errc::no_such_file_or_directory)) {
        // skip
      } else if (ec) {
        throw std::system_error(ec, "file_db::namespace_recover_state::open");
      }
    }

    fd_type actual_file;
  };

  auto read_namespace_replacements_op_() {
    using namespace std::string_view_literals;
    using ::earnest::detail::completion_handler_fun;
    using ::earnest::detail::completion_wrapper;

    return asio::async_initiate<decltype(asio::deferred), void(std::error_code, std::unique_ptr<namespace_recover_state>)>(
        [](auto handler, std::shared_ptr<file_db> fdb) {
          asio::dispatch(
              completion_wrapper<void()>(
                  completion_handler_fun(std::move(handler), fdb->get_executor(), fdb->strand_, fdb->get_allocator()),
                  [fdb](auto handler) {
                    assert(fdb->strand_.running_in_this_thread());
                    assert(fdb->wal != nullptr);

                    auto state_ptr = std::make_unique<namespace_recover_state>(
                        fdb->get_executor(),
                        fdb->wal->get_dir(),
                        namespaces_filename,
                        open_mode::READ_ONLY,
                        fdb->get_allocator());
                    auto& state_ref = *state_ptr;

                    // Ensure no-one else writes to the namespaces file while we read it.
                    try {
                      if (state_ref.actual_file.is_open()) {
                        if (!state_ref.actual_file.ftrylock_shared())
                          throw std::system_error(make_error_code(file_db_errc::lock_failure), "file is locked");
                      }
                    } catch (const std::system_error& ex) {
                      std::clog << "File-DB: unable to lock namespaces file (" << ex.what() << ")\n";
                      std::invoke(handler, ex.code(), std::move(state_ptr));
                      return;
                    }

                    fdb->async_records(
                        [&state_ref](const auto& record) -> std::error_code {
                          return std::visit(
                              [&state_ref](const auto& r) -> std::error_code {
                                if (r.file.ns == ""sv && r.file.filename == namespaces_filename)
                                  return state_ref.apply(r);
                                else
                                  return {};
                              },
                              record);
                        },
                        asio::append(std::move(handler), std::move(state_ptr)));
                  }));
        },
        asio::deferred, this->shared_from_this());
  }

  auto read_namespaces_op_(namespace_recover_state state) {
    using ::earnest::detail::namespace_map;
    using ::earnest::detail::positional_stream_adapter;
    using ::earnest::detail::replacement_map;
    using ::earnest::detail::replacement_map_reader;

    using reader_type = replacement_map_reader<
        fd_type,
        typename wal_type::fd_type,
        rebind_alloc<std::byte>>;
    struct state_t {
      explicit state_t(reader_type&& r)
      : stream(std::move(r))
      {}

      positional_stream_adapter<reader_type> stream;
      namespace_map<> ns_map;
    };

    auto state_ptr = std::make_unique<state_t>(
        reader_type(std::allocate_shared<fd_type>(get_allocator(), std::move(state.actual_file)), std::allocate_shared<replacement_map<typename wal_type::fd_type, rebind_alloc<std::byte>>>(get_allocator(), std::move(state.replacements)), std::move(state.file_size.value_or(0))));
    auto& state_ref = *state_ptr;
    return async_read(
        state_ref.stream,
        xdr_reader<>() & state_ref.ns_map,
        asio::append(asio::deferred, std::move(state_ptr)))
    | asio::deferred(
        [](std::error_code ec, std::unique_ptr<state_t> state_ptr) {
          return asio::deferred.values(ec, std::move(state_ptr->ns_map));
        });
  }

  auto recover_namespaces_op_() {
    using ::earnest::detail::namespace_map;

    return read_namespace_replacements_op_()
    | asio::append(asio::deferred, this->shared_from_this())
    | asio::bind_executor(
        this->strand_,
        asio::deferred(
            [](std::error_code ec, std::unique_ptr<namespace_recover_state> state_ptr, std::shared_ptr<file_db> fdb) {
              assert(fdb->strand_.running_in_this_thread());

              if (!ec) {
                // If nothing declared a state on the namespaces file,
                // then propagate the filesystem state.
                if (!state_ptr->exists.has_value()) state_ptr->exists = state_ptr->actual_file.is_open();
                if (!state_ptr->file_size.has_value()) state_ptr->file_size = (state_ptr->exists.value() ? state_ptr->actual_file.size() : 0);
                if (!state_ptr->exists.value()) {
                  std::clog << "File-DB: namespace file does not exist\n";
                  ec = make_error_code(file_db_errc::unrecoverable);
                }
              }

              return asio::deferred.when(!ec)
                  .then(fdb->read_namespaces_op_(std::move(*state_ptr)))
                  .otherwise(asio::deferred.values(ec, namespace_map()));
            }))
    | asio::bind_executor(
        this->strand_,
        asio::deferred(
            [fdb=this->shared_from_this()](std::error_code ec, namespace_map<> ns_map) {
              assert(fdb->strand_.running_in_this_thread());

              if (!ec) {
                try {
                  std::transform(
                      ns_map.namespaces.begin(), ns_map.namespaces.end(),
                      std::inserter(fdb->namespaces, fdb->namespaces.end()),
                      [](const std::pair<const std::string, std::string>& ns_entry) {
                        return std::make_pair(ns_entry.first, ns{ .d{ns_entry.second}, .dirname=ns_entry.second });
                      });
                  std::transform(
                      ns_map.files.begin(), ns_map.files.end(),
                      std::inserter(fdb->files, fdb->files.end()),
                      [fdb](const file_id& id) {
                        auto result = std::make_pair(
                            id,
                            file(*fdb, id, true));

                        if (id.ns.empty()) {
                          try {
                            result.second.fd->open(fdb->wal->get_dir(), id.filename, open_mode::READ_WRITE);
                          } catch (const std::system_error& ex) {
                            // Recovery will deal with it.
                          }
                        } else {
                          const auto ns_iter = fdb->namespaces.find(id.ns);
                          if (ns_iter == fdb->namespaces.end())
                            throw std::system_error(std::make_error_code(std::errc::no_such_file_or_directory));
                          try {
                            result.second.fd->open(ns_iter->second.d, id.filename, open_mode::READ_WRITE);
                          } catch (const std::system_error& ex) {
                            // Recovery will deal with it.
                          }
                        }

                        if (result.second.fd->is_open())
                          result.second.file_size = result.second.fd->size();
                        return result;
                      });
                } catch (const std::system_error& ex) {
                  ec = ex.code();
                }
              }

              return asio::deferred.values(ec);
            }));
  }

  template<typename CompletionToken>
  auto async_tx(CompletionToken&& token) const {
    return asio::async_initiate<CompletionToken, void(typename wal_type::tx_lock, std::shared_ptr<const typename wal_type::file_replacements>)>(
        [](auto handler, std::shared_ptr<const file_db> fdb) -> void {
          fdb->strand_.dispatch(
              detail::completion_wrapper<void()>(
                  std::move(handler),
                  [wal=fdb->wal](auto handler) -> void {
                    wal->async_tx(std::move(handler));
                  }),
              fdb->get_allocator());
        },
        token, this->shared_from_this());
  }

  auto install_apply_() -> void {
    wal->set_apply_callback(
        [ weak_fdb=this->weak_from_this(),
          ex=get_executor()
        ]<typename CompletionToken>(std::shared_ptr<typename wal_type::entry> e, CompletionToken&& token) {
          return asio::async_initiate<CompletionToken, void(std::error_code)>(
              [](auto handler, std::weak_ptr<file_db> weak_fdb, auto ex, std::shared_ptr<typename wal_type::entry> e) {
                std::shared_ptr<file_db> fdb;
                try {
                  fdb = std::shared_ptr<file_db>(weak_fdb);
                } catch (const std::bad_weak_ptr&) {
                  std::invoke(completion_handler_fun(std::move(handler), ex), make_error_code(file_db_errc::closing));
                  return;
                }

                fdb->apply_wal_entry_(std::move(e), std::move(handler));
              },
              token, weak_fdb, ex, std::move(e));
        });
  }

  template<typename CompletionToken>
  auto apply_wal_entry_(std::shared_ptr<typename wal_type::entry> e, CompletionToken&& token) {
    using update_map = std::unordered_map<
        file_id, detail::file_recover_state<typename wal_type::fd_type, allocator_type>,
        std::hash<file_id>,
        std::equal_to<file_id>,
        rebind_alloc<std::pair<const file_id, detail::file_recover_state<typename wal_type::fd_type, allocator_type>>>>;

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, std::shared_ptr<file_db> fdb, std::shared_ptr<typename wal_type::entry> e) {
          auto update_map_ptr = std::allocate_shared<update_map>(fdb->get_allocator(), fdb->get_allocator());
          e->async_records(
              [fdb, update_map_ptr](typename wal_type::entry::nb_variant_type v) -> std::error_code {
                const file_id& id = std::visit(
                    [](const auto& record) -> const file_id& {
                      return record.file;
                    },
                    v);

                auto update_iter = update_map_ptr->find(id);
                if (update_iter == update_map_ptr->end()) {
                  bool inserted;
                  std::tie(update_iter, inserted) = update_map_ptr->emplace(id, fdb->get_allocator());
                  assert(inserted);
                }

                std::visit(
                    [update_iter]<typename Record>(Record&& record) {
                      update_iter->second.apply(std::forward<Record>(record));
                    },
                    std::move(v));
                return {};
              },
              completion_wrapper<void(std::error_code)>(
                  completion_handler_fun(std::move(handler), fdb->get_executor(), fdb->strand_, fdb->get_allocator()),
                  [fdb, update_map_ptr](auto handler, std::error_code) {
                    assert(fdb->strand_.running_in_this_thread());
                    auto barrier = detail::make_completion_barrier(std::move(handler).as_unbound_handler(), fdb->get_executor());

                    bool need_ns_update = false;
                    for (const auto& [id, update] : *update_map_ptr) {
                      if (fdb->apply_file_recovery_(id, update, barrier))
                        need_ns_update = true;
                    }
                    if (need_ns_update) fdb->write_ns_op_() | ++barrier;

                    std::invoke(barrier, std::error_code());
                  }));
        },
        token, this->shared_from_this(), std::move(e));
  }

  template<typename BarrierHandler, typename BarrierExecutor>
  [[nodiscard]]
  auto apply_file_recovery_(const file_id& id, const detail::file_recover_state<typename wal_type::fd_type, allocator_type>& update, detail::completion_barrier<BarrierHandler, BarrierExecutor>& barrier) -> bool {
    assert(strand_.running_in_this_thread());

    auto sync_barrier = detail::fanout_barrier<executor_type, allocator_type>(get_executor(), get_allocator());
    bool need_namespaces_update = false;
    bool need_fsync = false;
    bool need_fdatasync = false;
    auto files_iter = files.find(id);
    bool files_found = (files_iter != files.end());
    bool files_open = (files_found && files_iter->second.fd->is_open());

    try {
      if (update.exists.has_value()) {
        if (update.exists.value()) {
          if (!files_found) {
            bool inserted;
            std::tie(files_iter, inserted) = files.emplace(id, file(*this, id));
            assert(inserted);
            files_found = true;
          }
          if (!files_open) {
            dir d;
            if (id.ns.empty()) {
              d = wal->get_dir();
            } else {
              auto ns_iter = namespaces.find(id.ns);
              if (ns_iter == namespaces.end())
                throw std::system_error(make_error_code(file_db_errc::unrecoverable), "missing namespace");
              d = ns_iter->second.d;
            }

            files_iter->second.fd->create(d, id.filename);
            need_namespaces_update = true;
            need_fsync = true;
            files_open = true;
          }
        } else {
          if (files_open) {
            dir d;
            if (id.ns.empty()) {
              d = wal->get_dir();
            } else {
              auto ns_iter = namespaces.find(id.ns);
              if (ns_iter == namespaces.end())
                throw std::system_error(make_error_code(file_db_errc::unrecoverable), "missing namespace");
              d = ns_iter->second.d;
            }

            files_iter->second.fd->close();
            files_open = false;
            d.erase(id.filename);
          }
          if (files_found) {
            files.erase(files_iter);
            files_iter = files.end();
            files_found = false;
            need_namespaces_update = true;
          }
        }
      }

      if (update.file_size.has_value()) {
        if (!files_found)
          throw std::system_error(make_error_code(file_db_errc::unrecoverable), "cannot truncate, because there's no record of this file");
        if (!files_open)
          throw std::system_error(make_error_code(file_db_errc::unrecoverable), "cannot truncate, because this file is closed");

        files_iter->second.fd->truncate(update.file_size.value());
        need_fdatasync = true;
      }
    } catch (const std::system_error& ex) {
      std::clog << "File-DB " << id.ns << "/" << id.filename << ": unable to apply WAL records: " << ex.what() << "\n";
      std::invoke(++barrier, ex.code());
      return need_namespaces_update;
    }

    for (const auto& elem : update.replacements) {
      if (!files_open) {
        std::clog << "File-DB " << id.ns << "/" << id.filename << ": unable to apply WAL records: modified file is not opened\n";
        std::invoke(++barrier, make_error_code(file_db_errc::unrecoverable));
        return need_namespaces_update;
      }

      auto tmp_ptr = std::allocate_shared<std::vector<std::byte, rebind_alloc<std::byte>>>(get_allocator(), elem.size(), get_allocator());

      std::shared_ptr<fd_type> fd_ptr = files_iter->second.fd;
      asio::async_read_at(
          elem.wal_file(),
          elem.wal_offset(),
          asio::buffer(*tmp_ptr),
          asio::transfer_all(),
          asio::deferred)
      | asio::deferred(
          [fd_ptr, elem, tmp_ptr](std::error_code ec, std::size_t nbytes) {
            return asio::deferred.when(!ec)
                .then(
                    asio::async_write_at(
                        *fd_ptr,
                        elem.offset(),
                        asio::buffer(*tmp_ptr),
                        asio::transfer_all(),
                        asio::deferred))
                .otherwise(asio::deferred.values(ec, nbytes));
          })
      | asio::deferred(
          [fd_ptr, elem, tmp_ptr](std::error_code ec, [[maybe_unused]] std::size_t nbytes) {
            return asio::deferred.values(ec);
          })
      | ++sync_barrier;
      need_fdatasync = true;
    }

    if (need_fsync || need_fdatasync) {
      std::invoke(sync_barrier, std::error_code());
      sync_barrier.async_on_ready(asio::deferred)
      | asio::deferred(
          [fd_ptr=files_iter->second.fd, need_fsync](std::error_code ec) {
            return asio::deferred.when(!ec)
                .then(fd_ptr->async_flush(!need_fsync, asio::deferred))
                .otherwise(asio::deferred.values(ec));
          })
      | asio::deferred(
          [fd_ptr=files_iter->second.fd](std::error_code ec) mutable {
            fd_ptr.reset();
            return asio::deferred.values(ec);
          })
      | ++barrier;
    }

    return need_namespaces_update;
  }

  auto write_ns_op_() {
    using detail::wal_record_truncate_file;
    using detail::wal_record_modify_file_write32;

    assert(strand_.running_in_this_thread());

    detail::namespace_map<> ns_map;
    std::transform(
        namespaces.cbegin(), namespaces.cend(),
        std::inserter(ns_map.namespaces, ns_map.namespaces.end()),
        [](const auto& ns_pair) {
          return std::make_pair(ns_pair.first, ns_pair.second.dirname.generic_string());
        });
    std::transform(
        files.cbegin(), files.cend(),
        std::inserter(ns_map.files, ns_map.files.end()),
        [](const auto& f_pair) -> const file_id& {
          return f_pair.first;
        });

    auto stream = std::allocate_shared<byte_stream<executor_type>, rebind_alloc<std::byte>>(get_allocator(), get_executor(), get_allocator());
    return async_write(
        *stream,
        xdr_writer<>() & xdr_constant(std::move(ns_map)),
        asio::deferred)
    | asio::deferred(
        [wal=this->wal, stream](std::error_code ec) {
          const auto data_len = stream->cdata().size();
          return asio::deferred.when(!ec)
              .then(
                  wal->async_append(
                      std::initializer_list<typename wal_type::write_variant_type>{
                        wal_record_truncate_file{
                          .file{"", std::string(namespaces_filename)},
                          .new_size=data_len,
                        },
                        wal_record_modify_file_write32{
                          .file{"", std::string(namespaces_filename)},
                          .file_offset=0,
                          .data=std::move(*stream).data(),
                        },
                      },
                      asio::deferred)
                  )
              .otherwise(asio::deferred.values(ec));
        });
  }

  static auto build_commit_metric_(std::shared_ptr<prometheus::Registry> prom_registry, std::string_view db_name, std::string_view success_or_fail) -> std::shared_ptr<prometheus::Counter> {
    if (prom_registry == nullptr) return std::make_shared<prometheus::Counter>();

    return std::shared_ptr<prometheus::Counter>(
        prom_registry,
        &prometheus::BuildCounter()
            .Name("earnest_file_db_commits")
            .Help("Count the number of file-db commits")
            .Register(*prom_registry)
            .Add({
                  {"db_name", std::string(db_name)},
                  {"success", std::string(success_or_fail)},
                }));
  }

  allocator_type alloc_;

  public:
  std::shared_ptr<wal_type> wal;
  std::unordered_map<std::string, ns, std::hash<std::string>, std::equal_to<std::string>, rebind_alloc<std::pair<const std::string, ns>>> namespaces;
  std::unordered_map<file_id, file, std::hash<file_id>, std::equal_to<file_id>, rebind_alloc<std::pair<const file_id, file>>> files;

  private:
  asio::strand<executor_type> strand_;
  std::shared_ptr<prometheus::Counter> successful_commits_;
  std::shared_ptr<prometheus::Counter> failed_commits_;
};


template<typename FileDB, typename Allocator>
class transaction {
  template<typename, typename> friend class ::earnest::file_db;

  private:
  using file_db = FileDB;

  public:
  using executor_type = typename file_db::executor_type;
  using allocator_type = Allocator;

  private:
  template<typename T>
  using rebind_alloc = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;

  // Writes done by this transaction.
  // The actual writes are tracked in a replacement map.
  // But the replacement map requires a positional stream, so we have to create one.
  using writes_buffer_type = detail::byte_positional_stream<executor_type, rebind_alloc<std::byte>>;

  struct writes_map_element {
    writes_map_element(allocator_type alloc)
    : replacements(alloc)
    {}

    detail::replacement_map<writes_buffer_type, allocator_type> replacements;
    std::optional<std::uint64_t> file_size = std::nullopt;
    bool created = false, erased = false;
  };

  using writes_map = std::unordered_map<
      file_id, std::shared_ptr<writes_map_element>,
      std::hash<file_id>,
      std::equal_to<file_id>,
      typename std::allocator_traits<allocator_type>::template rebind_alloc<std::pair<const file_id, std::shared_ptr<writes_map_element>>>>;

  using file_replacements = typename file_db::wal_type::file_replacements;

  struct locked_file_replacements {
    explicit locked_file_replacements(std::shared_ptr<const file_replacements> fr, typename file_db::wal_type::tx_lock lock)
    : fr(std::move(fr)),
      lock(std::move(lock))
    {}

    std::shared_ptr<const file_replacements> fr;
    typename file_db::wal_type::tx_lock lock;
  };

  struct locked_file_recover_state {
    explicit locked_file_recover_state (allocator_type allocator)
    : frs(std::move(allocator))
    {}

    typename file_replacements::mapped_type frs;
    typename file_db::wal_type::tx_lock lock;
  };

  using raw_reader_type = detail::replacement_map_reader<
      const typename file_db::fd_type,
      typename file_replacements::mapped_type::fd_type,
      typename file_db::allocator_type>;

  using reader_type = detail::replacement_map_reader<raw_reader_type, writes_buffer_type, allocator_type>;
  using monitor_type = detail::monitor<executor_type, allocator_type>;

  struct impl_type {
    impl_type() = delete;
    impl_type(const impl_type&) = delete;
    impl_type(impl_type&&) = delete;

    impl_type(std::shared_ptr<FileDB> fdb, isolation i, tx_mode m, allocator_type allocator)
    : fdb_(fdb),
      alloc_(allocator),
      i_(i),
      m_(m),
      writes_map_(std::allocate_shared<writes_map>(allocator, allocator)),
      read_barrier_(fdb->get_executor(), allocator),
      writes_mon_(fdb->get_executor(), "file_db::transaction", allocator),
      wait_for_writes_(fdb->get_executor(), allocator),
      on_commit_events_(std::allocate_shared<detail::on_commit_events<allocator_type>>(allocator, allocator))
    {
      switch (i_) {
        default:
          {
            std::ostringstream s;
            s << "isolation " << i_ << " is not supported\n";
            throw std::invalid_argument(std::move(s).str());
          }
          break;
        case isolation::read_commited:
          std::invoke(read_barrier_, nullptr);
          break;
        case isolation::repeatable_read:
          compute_replacements_map_op_(fdb, allocator) | read_barrier_;
          break;
      }
    }

    std::shared_ptr<FileDB> fdb_;
    allocator_type alloc_;
    isolation i_;
    tx_mode m_;
    std::shared_ptr<writes_map> writes_map_;
    mutable detail::fanout<executor_type, void(std::shared_ptr<const locked_file_replacements>), allocator_type> read_barrier_; // Barrier to fill in reads.
    mutable monitor_type writes_mon_;
    detail::fanout_barrier<executor_type, allocator_type> wait_for_writes_;
    std::shared_ptr<detail::on_commit_events<allocator_type>> on_commit_events_;
  };

  public:
  class file;

  constexpr transaction() noexcept = default;

  private:
  transaction(std::shared_ptr<FileDB> fdb, isolation i, tx_mode m, allocator_type allocator = allocator_type())
  : pimpl_(std::allocate_shared<impl_type>(allocator, std::move(fdb), i, m, allocator))
  {}

  public:
  auto get_isolation() const noexcept -> isolation { return pimpl_->i_; }
  auto get_tx_mode() const noexcept -> tx_mode { return pimpl_->m_; }

  template<typename Alloc, typename CompletionToken>
  auto async_file_contents(file_id id, Alloc alloc, CompletionToken&& token) const {
    return async_file_contents_op_(std::move(id), std::move(alloc)) | std::forward<CompletionToken>(token);
  }

  template<typename CompletionToken>
  auto async_file_contents(file_id id, CompletionToken&& token) {
    return async_file_contents(std::move(id), std::allocator<std::byte>(), std::forward<CompletionToken>(token));
  }

  template<typename CompletionToken>
  auto async_commit(CompletionToken&& token, bool delay_sync = false) {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, transaction self, bool delay_sync) {
          if constexpr(detail::handler_has_executor_v<decltype(handler)>)
            self.commit_op_(detail::completion_handler_fun(std::move(handler), self.get_executor()), delay_sync);
          else
            self.commit_op_(std::move(handler), delay_sync);
        },
        token, *this, delay_sync);
  }

  template<typename Fn>
  auto on_commit(Fn&& fn) {
    return pimpl_->on_commit_events_->add(std::forward<Fn>(fn));
  }

  auto get_allocator() const -> allocator_type { return pimpl_->alloc_; }
  auto get_executor() const -> executor_type { return pimpl_->fdb_->get_executor(); }

  auto operator[](file_id id) -> file {
    return file(*this, std::move(id));
  }

  private:
  static auto compute_replacements_map_op_(std::shared_ptr<file_db> fdb, allocator_type allocator) {
    return fdb->async_tx(asio::deferred)
    | asio::deferred(
        [allocator](typename file_db::wal_type::tx_lock lock, std::shared_ptr<const file_replacements> fr) {
          return asio::deferred.values(std::allocate_shared<locked_file_replacements>(allocator, fr, std::move(lock)));
        });
  }

  auto compute_replacements_map_1_op_(file_id id) const {
    using result_map_ptr = std::shared_ptr<const typename file_replacements::mapped_type>;

    return pimpl_->read_barrier_.async_on_ready(asio::deferred)
    | asio::deferred(
        [impl=pimpl_](std::shared_ptr<const locked_file_replacements> repl_maps) {
          return asio::deferred.when(repl_maps != nullptr)
              .then(asio::deferred.values(repl_maps))
              .otherwise(compute_replacements_map_op_(impl->fdb_, impl->alloc_));
        })
    | asio::deferred(
        [impl=pimpl_, id](std::shared_ptr<const locked_file_replacements> repl_maps) mutable {
          return asio::async_initiate<decltype(asio::deferred), void(std::error_code, result_map_ptr)>(
              [](auto handler, std::shared_ptr<impl_type> impl, file_id id, std::shared_ptr<const locked_file_replacements> repl_maps) {
                auto wrapped_handler = detail::completion_handler_fun(std::move(handler), impl->fdb_->get_executor());

                assert(repl_maps != nullptr);
                std::error_code ec;
                result_map_ptr result;
                auto iter = repl_maps->fr->find(id);
                if (iter == repl_maps->fr->end()) {
                  result = std::allocate_shared<typename file_replacements::mapped_type>(impl->alloc_, impl->fdb_->get_allocator());
                } else {
                  result = result_map_ptr(repl_maps, &iter->second); // We alias repl_maps, because we want to keep the lock valid.
                  if (!iter->second.exists.value_or(true))
                    ec = make_error_code(std::errc::no_such_file_or_directory);
                }
                std::invoke(wrapped_handler, std::move(ec), std::move(result));
              },
              asio::deferred, std::move(impl), std::move(id), std::move(repl_maps));
        });
  }

  auto get_raw_reader_op_(file_id id) const {
    using underlying_fd = typename file_db::fd_type;

    return compute_replacements_map_1_op_(id)
    | asio::deferred(
        [impl=pimpl_, id](std::error_code ec, std::shared_ptr<const typename file_replacements::mapped_type> state) mutable {
          return detail::deferred_on_executor<void(std::error_code, std::shared_ptr<const underlying_fd>, std::shared_ptr<const typename file_replacements::mapped_type>)>(
              [](std::error_code ec, std::shared_ptr<impl_type> impl, std::shared_ptr<const typename file_replacements::mapped_type> state, const file_id id) {
                assert(impl->fdb_->strand_.running_in_this_thread());
                assert(!!ec || state != nullptr);

                if (!ec && state != nullptr && !state->exists.value_or(true)) [[unlikely]]
                  ec = std::make_error_code(std::errc::no_such_file_or_directory);

                std::shared_ptr<const underlying_fd> ufd;
                if (!ec) [[likely]] {
                  auto files_iter = impl->fdb_->files.find(id);
                  if (files_iter != impl->fdb_->files.end())
                    ufd = files_iter->second.fd;
                  else
                    ufd = std::allocate_shared<underlying_fd>(impl->alloc_, impl->fdb_->get_executor());
                }
                return asio::deferred.values(ec, std::move(ufd), std::move(state));
              },
              impl->fdb_->strand_, ec, impl, std::move(state), std::move(id));
        })
    | asio::deferred(
        [](std::error_code ec, std::shared_ptr<const underlying_fd> ufd, std::shared_ptr<const typename file_replacements::mapped_type> state) {
          typename raw_reader_type::size_type filesize = 0u;
          std::shared_ptr<const detail::replacement_map<typename file_replacements::mapped_type::fd_type, typename file_db::allocator_type>> replacements;

          if (!ec) {
            assert(ufd != nullptr);
            assert(state != nullptr);

            const bool exists = state->exists.value_or(ufd->is_open());
            filesize = state->file_size.value_or(ufd->is_open() ? ufd->size() : 0u);
            replacements = std::shared_ptr<const detail::replacement_map<typename file_replacements::mapped_type::fd_type, typename file_db::allocator_type>>(state, &state->replacements);

            if (!exists) ec = make_error_code(file_db_errc::file_erased);
          }

          return asio::deferred.values(
              ec,
              raw_reader_type(ufd, std::move(replacements), filesize));
        });
  }

  auto get_reader_op_(file_id id) const {
    return asio::deferred.values(read_permitted(pimpl_->m_) ? std::error_code{} : make_error_code(file_db_errc::read_not_permitted))
    | asio::deferred(
        [self=*this, id](std::error_code ec) mutable {
          return asio::deferred.when(!ec)
              .then(self.get_raw_reader_op_(id))
              .otherwise(asio::deferred.values(ec, raw_reader_type{}));
        })
    | asio::deferred(
        [impl=pimpl_](std::error_code ec, raw_reader_type raw_reader) mutable {
          return asio::deferred.when(!ec || ec == make_error_code(file_db_errc::file_erased))
              .then(impl->writes_mon_.async_shared(asio::append(asio::deferred, ec, std::move(raw_reader)), __FILE__, __LINE__))
              .otherwise(asio::deferred.values(typename monitor_type::shared_lock(), ec, raw_reader_type()));
        })
    | asio::deferred(
        [impl=pimpl_, id](typename monitor_type::shared_lock lock, std::error_code ec, raw_reader_type raw_reader) {
          std::shared_ptr<writes_map_element> tx_writes;
          typename reader_type::size_type filesize = 0;
          if (!ec || ec == make_error_code(file_db_errc::file_erased)) {
            const auto writes_iter = impl->writes_map_->find(id);
            if (writes_iter != impl->writes_map_->end()) {
              tx_writes = writes_iter->second;
              if (tx_writes->erased)
                ec = make_error_code(file_db_errc::file_erased);
              else if (tx_writes->created)
                ec.clear();
              filesize = tx_writes->file_size.value_or(raw_reader.size());
            } else {
              filesize = raw_reader.size();
            }
          }

          return asio::deferred.when(!ec)
              .then(
                  asio::deferred.values(
                      std::move(lock),
                      ec,
                      reader_type(
                          std::allocate_shared<raw_reader_type>(impl->alloc_, std::move(raw_reader)),
                          std::shared_ptr<detail::replacement_map<writes_buffer_type, allocator_type>>(tx_writes, &tx_writes->replacements),
                          filesize)))
              .otherwise(
                  asio::deferred.values(
                      typename monitor_type::shared_lock(),
                      ec,
                      reader_type()));
        });
  }

  auto get_writer_op_(file_id id) {
    return asio::deferred.values(write_permitted(pimpl_->m_) ? std::error_code{} : make_error_code(file_db_errc::read_not_permitted))
    | asio::deferred(
        [self=*this, id](std::error_code ec) mutable {
          return asio::deferred.when(!ec)
              .then(self.get_raw_reader_op_(id))
              .otherwise(asio::deferred.values(ec, raw_reader_type{}));
        })
    | asio::deferred(
        [impl=pimpl_](std::error_code ec, raw_reader_type raw_reader) mutable {
          return asio::deferred.when(!ec || ec == make_error_code(file_db_errc::file_erased))
              .then(impl->writes_mon_.async_exclusive(asio::append(asio::deferred, ec, std::move(raw_reader)), __FILE__, __LINE__))
              .otherwise(asio::deferred.values(typename monitor_type::exclusive_lock(), ec, raw_reader_type()));
        })
    | asio::deferred(
        [impl=pimpl_, id](typename monitor_type::exclusive_lock lock, std::error_code ec, raw_reader_type raw_reader) {
          std::shared_ptr<writes_map_element> tx_writes;
          typename reader_type::size_type filesize = 0;
          if (!ec || ec == make_error_code(file_db_errc::file_erased)) {
            auto writes_iter = impl->writes_map_->find(id);
            if (writes_iter == impl->writes_map_->end()) {
              bool inserted;
              std::tie(writes_iter, inserted) = impl->writes_map_->emplace(
                  std::move(id),
                  std::allocate_shared<writes_map_element>(impl->alloc_, impl->alloc_));
              assert(inserted);
            }

            tx_writes = writes_iter->second;
            if (tx_writes->erased)
              ec = make_error_code(file_db_errc::file_erased);
            else if (tx_writes->created)
              ec.clear();
            filesize = tx_writes->file_size.value_or(raw_reader.size());
          }

          return asio::deferred.when(!ec || ec == make_error_code(file_db_errc::file_erased))
              .then(
                  asio::deferred.values(
                      std::move(lock),
                      ec,
                      std::move(tx_writes),
                      filesize))
              .otherwise(
                  asio::deferred.values(
                      typename monitor_type::exclusive_lock(),
                      ec,
                      std::shared_ptr<writes_map_element>(),
                      filesize));
        });
  }

  template<typename Alloc>
  auto async_file_contents_op_(file_id id, Alloc alloc) const {
    using bytes_vec = std::vector<std::byte, typename std::allocator_traits<Alloc>::template rebind_alloc<std::byte>>;

    return asio::async_initiate<decltype(asio::deferred), void(std::error_code, bytes_vec)>(
        [](auto handler, transaction self, file_id id, Alloc alloc) mutable -> void {
          if constexpr(detail::handler_has_executor_v<decltype(handler)>)
            self.async_file_contents_op_impl_(std::move(id), std::move(alloc), detail::completion_handler_fun(std::move(handler), self.get_executor()));
          else
            self.async_file_contents_op_impl_(std::move(id), std::move(alloc), std::move(handler));
        },
        asio::deferred, *this, std::move(id), std::move(alloc));
  }

  template<typename Alloc>
  auto async_file_contents_op_impl_(file_id id, Alloc alloc, detail::move_only_function<void(std::error_code, std::vector<std::byte, typename std::allocator_traits<Alloc>::template rebind_alloc<std::byte>>)> handler) const -> void {
    using bytes_vec = std::vector<std::byte, typename std::allocator_traits<Alloc>::template rebind_alloc<std::byte>>;

    return get_reader_op_(std::move(id))
    | completion_wrapper<void(typename monitor_type::shared_lock, std::error_code, reader_type)>(
        std::move(handler),
        [alloc=std::move(alloc)](auto handler, typename monitor_type::shared_lock lock, std::error_code ec, reader_type r) mutable {
          if (ec) {
            std::invoke(handler, ec, bytes_vec(std::move(alloc)));
            return;
          }

          struct state {
            state(reader_type&& r, Alloc alloc)
            : r(std::move(r)),
              bytes(std::move(alloc))
            {
              this->bytes.resize(this->r.size());
            }

            reader_type r;
            bytes_vec bytes;
          };

          auto state_ptr = std::make_unique<state>(std::move(r), alloc);
          auto& state_ref = *state_ptr;

          asio::async_read_at(
              state_ref.r,
              0,
              asio::buffer(state_ref.bytes),
              detail::completion_wrapper<void(std::error_code, std::size_t)>(
                  std::move(handler),
                  [lock, state_ptr=std::move(state_ptr)](auto handler, std::error_code ec, [[maybe_unused]] std::size_t nbytes) mutable {
                    assert(ec || nbytes == state_ptr->bytes.size());
                    lock.reset(); // No longer need the lock.
                                  // And we want the compiler to not complain, so we must use the variable.
                    std::invoke(handler, ec, std::move(state_ptr->bytes));
                  }));
        });
  }

  template<typename MB>
  auto async_file_read_some_op_(file_id id, std::uint64_t offset, MB&& buffers) const {
    auto buffer_vec = std::vector<asio::mutable_buffer, rebind_alloc<asio::mutable_buffer>>(
        asio::buffer_sequence_begin(buffers),
        asio::buffer_sequence_end(buffers),
        get_allocator());

    return asio::async_initiate<decltype(asio::deferred), void(std::error_code, std::size_t)>(
        [](auto handler, transaction self, file_id id, std::uint64_t offset, std::vector<asio::mutable_buffer, rebind_alloc<asio::mutable_buffer>> buffers) -> void {
          if constexpr(detail::handler_has_executor_v<decltype(handler)>)
            self.async_file_read_some_op_impl_(std::move(id), std::move(offset), std::move(buffers), detail::completion_handler_fun(std::move(handler), self.get_executor()));
          else
            self.async_file_read_some_op_impl_(std::move(id), std::move(offset), std::move(buffers), std::move(handler));
        },
        asio::deferred, *this, std::move(id), std::move(offset), std::move(buffer_vec));
  }

  auto async_file_read_some_op_impl_(file_id id, std::uint64_t offset, std::vector<asio::mutable_buffer, rebind_alloc<asio::mutable_buffer>> buffers, detail::move_only_function<void(std::error_code, std::size_t)> handler) const -> void {
    return get_reader_op_(std::move(id))
    | asio::deferred(
        [ impl=pimpl_,
          buffers=std::move(buffers),
          offset
        ](typename monitor_type::shared_lock lock, std::error_code ec, reader_type r) mutable {
          return asio::async_initiate<decltype(asio::deferred), void(std::error_code, std::size_t)>(
              [](auto handler, std::error_code input_ec, std::uint64_t offset, auto mb, typename monitor_type::shared_lock lock, reader_type r, auto fdb_ex) {
                if (input_ec) {
                  std::invoke(detail::completion_handler_fun(std::move(handler), std::move(fdb_ex)), input_ec, std::size_t(0));
                  return;
                }

                auto r_ptr = std::make_unique<reader_type>(std::move(r));
                auto& r_ref = *r_ptr;

                r_ref.async_read_some_at(offset, std::move(mb),
                    detail::completion_wrapper<void(std::error_code, std::size_t)>(
                        std::move(handler),
                        [lock=std::move(lock), r_ptr=std::move(r_ptr)](auto handler, std::error_code ec, std::size_t nbytes) mutable {
                          lock.reset();
                          r_ptr.reset();
                          std::invoke(handler, ec, nbytes);
                        }));
              },
              asio::deferred, ec, offset, std::move(buffers), std::move(lock), std::move(r), impl->fdb_->get_executor());
        })
    | std::move(handler);
  }

  template<typename MB>
  auto async_file_write_some_op_(file_id id, std::uint64_t offset, MB&& buffers) {
    auto buffer = std::allocate_shared<writes_buffer_type>(pimpl_->alloc_, pimpl_->fdb_->get_executor(), pimpl_->alloc_);
    std::error_code ec;
    asio::write_at(*buffer, 0, std::forward<MB>(buffers), asio::transfer_all(), ec);

    return asio::async_initiate<decltype(asio::deferred), void(std::error_code, std::size_t)>(
        [](auto handler, transaction self, file_id id, std::uint64_t offset, std::error_code ec, std::shared_ptr<writes_buffer_type> buffer) mutable -> void {
          if constexpr(detail::handler_has_executor_v<decltype(handler)>)
            self.async_file_write_some_op_impl_(std::move(id), std::move(offset), std::move(ec), std::move(buffer), detail::completion_handler_fun(std::move(handler), self.get_executor()));
          else
            self.async_file_write_some_op_impl_(std::move(id), std::move(offset), std::move(ec), std::move(buffer), std::move(handler));
        },
        asio::deferred, *this, std::move(id), std::move(offset), std::move(ec), std::move(buffer));
  }

  auto async_file_write_some_op_impl_(file_id id, std::uint64_t offset, std::error_code write_ec, std::shared_ptr<writes_buffer_type>&& buffer, detail::move_only_function<void(std::error_code, std::size_t)> handler) -> void {
    return get_writer_op_(std::move(id))
    | asio::deferred(
        [ buffer=std::move(buffer),
          write_ec, offset
        ](typename monitor_type::exclusive_lock lock, std::error_code ec, std::shared_ptr<writes_map_element> replacements, auto filesize) mutable {
          if (!ec) ec = write_ec;

          const std::size_t buffer_size = buffer->cdata().size();
          if (!ec) {
            // We don't allow writes past the end of a file.
            if (offset > replacements->file_size.value_or(filesize) ||
                buffer_size > replacements->file_size.value_or(filesize) - offset) {
              ec = make_error_code(file_db_errc::write_past_eof);
            } else {
              replacements->replacements.insert(offset, 0, buffer_size, std::move(buffer));
            }
          }

          lock.reset();
          return asio::deferred.values(ec, buffer_size);
        })
    | asio::deferred(
        [wfw=++pimpl_->wait_for_writes_](std::error_code ec, std::size_t nbytes) mutable {
          std::invoke(wfw, ec);
          return asio::deferred.values(ec, nbytes);
        })
    | std::move(handler);
  }

  auto async_file_size_op_(file_id id) const {
    return get_raw_reader_op_(std::move(id))
    | asio::deferred(
        [](std::error_code ec, raw_reader_type raw_reader) {
          const typename raw_reader_type::size_type file_size = (!ec ? raw_reader.size() : 0u);
          return asio::deferred.values(ec, file_size);
        })
    | detail::forward_deferred(get_executor());
  }

  auto async_truncate_op_(file_id id, std::uint64_t new_size) {
    return get_writer_op_(std::move(id))
    | asio::deferred(
        [ impl=pimpl_,
          new_size
        ](typename monitor_type::exclusive_lock lock, std::error_code ec, std::shared_ptr<writes_map_element> replacements, auto filesize) mutable {
          if (!ec) {
            if (new_size > filesize) {
              auto buffer = std::allocate_shared<writes_buffer_type>(impl->alloc_, impl->fdb_->get_executor(), impl->alloc_);
              buffer->data().resize(new_size - filesize);
              replacements->replacements.insert(filesize, 0, new_size - filesize, std::move(buffer));
            } else if (new_size < filesize) {
              replacements->replacements.truncate(new_size);
            }
            replacements->file_size = new_size;
          }

          lock.reset();
          return asio::deferred.values(ec);
        })
    | asio::deferred(
        [wfw=++pimpl_->wait_for_writes_](std::error_code ec) mutable {
          std::invoke(wfw, ec);
          return asio::deferred.values(ec);
        })
    | detail::forward_deferred(get_executor());
  }

  auto async_create_file_op_(file_id id) {
    return get_writer_op_(id)
    | asio::deferred(
        [id](typename monitor_type::exclusive_lock lock, std::error_code ec, std::shared_ptr<writes_map_element> replacements, [[maybe_unused]] auto filesize) mutable {
          if (ec == make_error_code(file_db_errc::file_erased)) {
            assert(replacements->replacements.empty());
            ec.clear();
            if (replacements->erased)
              replacements->erased = false;
            else
              replacements->created = true;
            replacements->file_size = 0u;
          } else if (!ec) {
            ec = make_error_code(file_db_errc::file_exists);
          }

          lock.reset();
          return asio::deferred.values(ec);
        })
    | asio::deferred(
        [wfw=++pimpl_->wait_for_writes_](std::error_code ec) mutable {
          std::invoke(wfw, ec);
          return asio::deferred.values(ec);
        })
    | detail::forward_deferred(get_executor());
  }

  auto async_erase_file_op_(file_id id) {
    return get_writer_op_(id)
    | asio::deferred(
        [id](typename monitor_type::exclusive_lock lock, std::error_code ec, std::shared_ptr<writes_map_element> replacements, [[maybe_unused]] auto filesize) mutable {
          if (!ec) {
            replacements->replacements.clear();
            if (replacements->created) {
              replacements->created = false;
              replacements->file_size = std::nullopt;
            } else {
              replacements->file_size = std::nullopt;
              replacements->erased = true;
            }
          }

          lock.reset();
          return asio::deferred.values(ec);
        })
    | asio::deferred(
        [wfw=++pimpl_->wait_for_writes_](std::error_code ec) mutable {
          std::invoke(wfw, ec);
          return asio::deferred.values(ec);
        })
    | detail::forward_deferred(get_executor());
  }

  auto commit_op_(detail::move_only_function<void(std::error_code)> handler, bool delay_sync = false) {
    std::invoke(pimpl_->wait_for_writes_, std::error_code()); // Complete the writes (further write attempts will now throw an exception).
    return pimpl_->wait_for_writes_.async_on_ready(asio::deferred)
    | asio::deferred(
        [impl=pimpl_](std::error_code ec) mutable {
          return asio::deferred.when(!ec)
              .then(impl->writes_mon_.async_shared(asio::prepend(asio::deferred, ec), __FILE__, __LINE__))
              .otherwise(asio::deferred.values(ec, typename monitor_type::shared_lock{}));
        })
    | asio::deferred(
        [ impl=pimpl_,
          delay_sync
        ](std::error_code ec, typename monitor_type::shared_lock lock) {
          auto writes = std::vector<typename file_db::wal_type::write_variant_type, rebind_alloc<typename file_db::wal_type::write_variant_type>>(impl->alloc_);
          if (!ec) {
            std::for_each(
                impl->writes_map_->cbegin(), impl->writes_map_->cend(),
                [&writes](const auto& wmap_pair) {
                  commit_elems_(wmap_pair.first, *wmap_pair.second, std::back_inserter(writes));
                });
          }

          return asio::deferred.when(!ec)
              .then(
                  impl->fdb_->wal->async_append(
                      std::move(writes),
                      asio::consign(asio::deferred, lock),
                      can_commit_op_(impl->fdb_, impl->writes_map_, impl->alloc_),
                      delay_sync))
              .otherwise(asio::deferred.values(ec));
        })
    | asio::deferred(
        [impl=pimpl_](std::error_code ec) {
          if (!ec) impl->on_commit_events_->run();
          return asio::deferred.values(ec);
        })
    | asio::deferred(
        [impl=pimpl_](std::error_code ec) {
          if (!ec)
            impl->fdb_->successful_commits_->Increment();
          else
            impl->fdb_->failed_commits_->Increment();
          return asio::deferred.values(ec);
        })
    | std::move(handler);
  }

  static auto can_commit_op_(std::shared_ptr<file_db> fdb, std::shared_ptr<const writes_map> wmap, allocator_type allocator) {
    return compute_replacements_map_op_(fdb, allocator)
    | asio::deferred(
        [fdb, wmap](std::shared_ptr<const locked_file_replacements> fr_map) mutable {
          return detail::deferred_on_executor<void(std::error_code)>(
              [](std::shared_ptr<const locked_file_replacements> fr_map, std::shared_ptr<file_db> fdb, std::shared_ptr<const writes_map> wmap) {
                std::error_code ec;
                const bool writes_are_valid = std::all_of(
                    wmap->begin(), wmap->end(),
                    [&](const auto& wmap_entry) {
                      const auto files_iter = fdb->files.find(wmap_entry.first);
                      const bool files_iter_is_end = (files_iter == fdb->files.end());
                      const bool files_iter_exists = (files_iter_is_end ? false : files_iter->second.fd->is_open());
                      const auto files_iter_f_size = (files_iter_is_end || !files_iter_exists ? 0u : files_iter->second.fd->size());

                      const auto fr_iter = fr_map->fr->find(wmap_entry.first);
                      const auto fr_iter_is_end = (fr_iter == fr_map->fr->end());
                      const bool exists = (fr_iter_is_end ? files_iter_exists : fr_iter->second.exists.value_or(files_iter_exists));
                      const auto f_size = (fr_iter_is_end ? files_iter_f_size : fr_iter->second.file_size.value_or(files_iter_f_size));

                      if (wmap_entry.second->created && exists) return false; // Double file-creation.
                      if (wmap_entry.second->erased && !exists) return false; // Double file-erasure.

                      if (wmap_entry.second->file_size.has_value()) return true; // All writes must meet the resize constraint.
                      if (wmap_entry.second->replacements.empty()) return true; // No writes are outside the file-size.
                      if (std::prev(wmap_entry.second->replacements.end())->end_offset() > f_size) return false; // Write past EOF.

                      return true;
                    });
                if (!writes_are_valid) ec = make_error_code(file_db_errc::cannot_commit);

                return asio::deferred.values(ec);
              },
              fdb->strand_, std::move(fr_map), std::move(fdb), std::move(wmap));
        });
  }

  template<typename InsertIter>
  static auto commit_elems_(const file_id& id, const writes_map_element& elems, InsertIter insert_iter) -> void {
    using detail::wal_record_erase_file;
    using detail::wal_record_create_file;
    using detail::wal_record_truncate_file;
    using detail::wal_record_modify_file_write32;

    if (elems.erased)
      insert_iter++ = wal_record_erase_file{ .file=id };
    if (elems.created)
      insert_iter++ = wal_record_create_file{ .file=id };
    if (elems.file_size.has_value())
      insert_iter++ = wal_record_truncate_file{ .file=id, .new_size=elems.file_size.value() };

    for (const auto& replacement : elems.replacements) {
      insert_iter++ = wal_record_modify_file_write32{
        .file=id,
        .file_offset=replacement.offset(),
        .data{
            replacement.wal_file().cdata().begin() + replacement.wal_offset(),
            replacement.wal_file().cdata().begin() + replacement.wal_offset() + replacement.size()
        }
      };
    }
  }

  std::shared_ptr<impl_type> pimpl_;
};


template<typename FileDB, typename Allocator>
class transaction<FileDB, Allocator>::file {
  template<typename, typename> friend class transaction;

  public:
  using executor_type = typename transaction::executor_type;
  using allocator_type = typename transaction::allocator_type;

  private:
  file(transaction<FileDB, Allocator> tx, file_id id)
  : tx_(std::move(tx)),
    id_(std::move(id))
  {}

  public:
  auto get_executor() const -> executor_type { return tx_.get_executor(); }
  auto get_allocator() const -> allocator_type { return tx_.get_allocator(); }
  auto id() const noexcept -> const file_id& { return id_; }

  template<typename CompletionToken>
  auto async_create(CompletionToken&& token) {
    return tx_.async_create_file_op_(id_) | std::forward<CompletionToken>(token);
  }

  template<typename CompletionToken>
  auto async_erase(CompletionToken&& token) {
    return tx_.async_erase_file_op_(id_) | std::forward<CompletionToken>(token);
  }

  template<typename CompletionToken>
  auto async_truncate(std::uint64_t new_size, CompletionToken&& token) {
    return tx_.async_truncate_op_(id_, new_size) | std::forward<CompletionToken>(token);
  }

  template<typename MB, typename CompletionToken>
  auto async_read_some_at(std::uint64_t offset, MB&& mb, CompletionToken&& token) const {
    return tx_.async_file_read_some_op_(id_, offset, std::forward<MB>(mb)) | std::forward<CompletionToken>(token);
  }

  template<typename MB, typename CompletionToken>
  auto async_write_some_at(std::uint64_t offset, MB&& mb, CompletionToken&& token) {
    return tx_.async_file_write_some_op_(id_, offset, std::forward<MB>(mb)) | std::forward<CompletionToken>(token);
  }

  template<typename CompletionToken>
  auto async_file_size(CompletionToken&& token) const {
    return tx_.async_file_size_op_(id_) | std::forward<CompletionToken>(token);
  }

  private:
  transaction<FileDB, Allocator> tx_;
  file_id id_;
};


} /* namespace earnest */
