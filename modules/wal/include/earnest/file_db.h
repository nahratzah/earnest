#pragma once

#include <cassert>
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
#include <asio/deferred.hpp>
#include <asio/strand.hpp>
#include <asio/write.hpp>
#include <asio/write_at.hpp>

#include <earnest/detail/byte_positional_stream.h>
#include <earnest/detail/completion_handler_fun.h>
#include <earnest/detail/completion_wrapper.h>
#include <earnest/detail/deferred_on_executor.h>
#include <earnest/detail/file_recover_state.h>
#include <earnest/detail/monitor.h>
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
  };

  struct file {
    std::shared_ptr<fd_type> fd;
    bool exists_logically = false;
    std::uint64_t file_size = 0;
  };

  public:
  explicit file_db(executor_type ex, allocator_type alloc = allocator_type())
  : alloc_(alloc),
    wal(),
    namespaces(alloc),
    files(alloc),
    strand_(std::move(ex))
  {}

  auto get_executor() const -> executor_type { return strand_.get_inner_executor(); }
  auto get_allocator() const -> allocator_type { return alloc_; }

  template<typename CompletionToken>
  auto async_create(dir d, CompletionToken&& token) {
    using ::earnest::detail::completion_handler_fun;
    using ::earnest::detail::completion_wrapper;

    return detail::deferred_on_executor<void(std::error_code, std::shared_ptr<file_db>)>(
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
    | undo_on_fail_op_()
    | std::forward<CompletionToken>(token);
  }

  template<typename CompletionToken>
  auto async_open(dir d, CompletionToken&& token) {
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
    | undo_on_fail_op_()
    | std::forward<CompletionToken>(token);
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
        std::nullopt,
        std::move(alloc));
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
    using byte_vector = std::vector<std::byte, rebind_alloc<std::byte>>;

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
                        return std::make_pair(ns_entry.first, ns{ .d{ns_entry.second} });
                      });
                  std::transform(
                      ns_map.files.begin(), ns_map.files.end(),
                      std::inserter(fdb->files, fdb->files.end()),
                      [fdb](const file_id& id) {
                        auto result = std::make_pair(
                            id,
                            file{
                              .fd=std::allocate_shared<fd_type>(fdb->get_allocator(), fdb->get_executor()),
                              .exists_logically=true,
                            });

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

  template<typename Acceptor, typename CompletionToken>
  requires std::invocable<Acceptor, typename wal_type::record_type>
  auto async_tx(Acceptor&& acceptor, CompletionToken&& token) const {
    return asio::async_initiate<CompletionToken, void(std::error_code, typename wal_type::tx_lock)>(
        [](auto handler, std::shared_ptr<const file_db> fdb, auto acceptor) {
          fdb->strand_.dispatch(
              [fdb, acceptor=std::move(acceptor), handler=std::move(handler)]() mutable -> void {
                if (fdb->wal == nullptr) [[unlikely]] {
                  std::invoke(handler, make_error_code(wal_errc::bad_state), typename wal_type::tx_lock{});
                  return;
                }

                fdb->wal->async_tx(std::move(acceptor), std::move(handler));
              },
              fdb->get_allocator());
        },
        token, this->shared_from_this(), std::forward<Acceptor>(acceptor));
  }

  allocator_type alloc_;

  public:
  std::shared_ptr<wal_type> wal;
  std::unordered_map<std::string, ns, std::hash<std::string>, std::equal_to<std::string>, rebind_alloc<std::pair<const std::string, ns>>> namespaces;
  std::unordered_map<file_id, file, std::hash<file_id>, std::equal_to<file_id>, rebind_alloc<std::pair<const file_id, file>>> files;

  private:
  asio::strand<executor_type> strand_;
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
  using writes_buffer_type = detail::byte_positional_stream<executor_type, allocator_type>;

  struct writes_map_element {
    writes_map_element(allocator_type alloc)
    : replacements(alloc)
    {}

    detail::replacement_map<writes_buffer_type, allocator_type> replacements;
    std::optional<std::uint64_t> file_size = std::nullopt;
  };

  using writes_map = std::unordered_map<
      file_id, std::shared_ptr<writes_map_element>,
      std::hash<file_id>,
      std::equal_to<file_id>,
      typename std::allocator_traits<allocator_type>::template rebind_alloc<std::pair<const file_id, std::shared_ptr<writes_map_element>>>>;

  using file_replacements = std::unordered_map<
      file_id,
      detail::file_recover_state<typename file_db::wal_type::fd_type, allocator_type>,
      std::hash<file_id>,
      std::equal_to<file_id>,
      rebind_alloc<std::pair<const file_id, detail::file_recover_state<typename file_db::wal_type::fd_type, allocator_type>>>>;

  struct locked_file_replacements {
    explicit locked_file_replacements(allocator_type allocator)
    : fr(std::move(allocator))
    {}

    file_replacements fr;
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
      allocator_type>;

  using reader_type = detail::replacement_map_reader<raw_reader_type, writes_buffer_type, allocator_type>;
  using monitor_type = detail::monitor<executor_type, allocator_type>;
  class file;

  public:
  transaction(const transaction&) = default;
  transaction(transaction&&) = default;

  private:
  transaction(std::shared_ptr<FileDB> fdb, isolation i, tx_mode m, std::optional<std::unordered_set<file_id>> fileset = std::nullopt, allocator_type allocator = allocator_type())
  : fdb_(fdb),
    alloc_(allocator),
    i_(i),
    m_(m),
    writes_map_(std::allocate_shared<writes_map>(allocator, allocator)),
    read_barrier_(fdb->get_executor(), allocator),
    writes_mon_(fdb->get_executor(), allocator)
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
        std::invoke(read_barrier_, std::error_code(), nullptr);
        break;
      case isolation::repeatable_read:
        compute_replacements_map_op_(fdb, std::move(fileset), allocator) | read_barrier_;
        break;
    }
  }

  public:
  template<typename Alloc, typename CompletionToken>
  auto async_file_contents(file_id id, Alloc alloc, CompletionToken&& token) const {
    return async_file_contents_op_(std::move(id), std::move(alloc)) | std::forward<CompletionToken>(token);
  }

  template<typename CompletionToken>
  auto async_file_contents(file_id id, CompletionToken&& token) {
    return async_file_contents(std::move(id), std::allocator<std::byte>(), std::forward<CompletionToken>(token));
  }

  auto get_allocator() const -> allocator_type { return alloc_; }
  auto get_executor() const -> executor_type { return fdb_->get_executor(); }

  auto operator[](file_id id) -> file {
    return file(*this, std::move(id));
  }

  private:
  auto compute_replacements_map_op_(std::shared_ptr<file_db> fdb, std::optional<std::unordered_set<file_id>> fileset, allocator_type allocator) {
    auto repl_map = std::allocate_shared<locked_file_replacements>(allocator, allocator);
    return fdb->async_tx(
        [fdb, repl_map, fileset=std::move(fileset), ex=fdb->get_executor(), allocator](const auto& record) -> std::error_code {
          return std::visit(
              [&](const auto& r) -> std::error_code {
                if (!fileset.has_value() || fileset->contains(r.file)) {
                  auto [iter, inserted] = repl_map->fr.try_emplace(r.file, allocator);
                  return iter->second.apply(r);
                } else {
                  return {};
                }
              },
              record);
        },
        asio::deferred)
    | asio::deferred(
        [repl_map](std::error_code ec, typename file_db::wal_type::tx_lock lock) mutable {
          repl_map->lock = std::move(lock);
          return asio::deferred.values(ec, std::move(repl_map));
        });
  }

  auto compute_replacements_map_1_op_(file_id id) const {
    using result_map_ptr = std::shared_ptr<const typename file_replacements::mapped_type>;

    return read_barrier_.async_on_ready(asio::deferred)
    | asio::deferred(
        [fdb=this->fdb_, id, allocator=this->alloc_](std::error_code input_ec, std::shared_ptr<const locked_file_replacements> repl_maps) {
          return asio::async_initiate<decltype(asio::deferred), void(std::error_code, result_map_ptr)>(
              [id, repl_maps, fdb, allocator](auto handler, std::error_code input_ec) {
                auto wrapped_handler = detail::completion_handler_fun(std::move(handler), fdb->get_executor());
                if (input_ec) {
                  std::invoke(wrapped_handler, input_ec, nullptr);
                  return;
                }

                if (repl_maps == nullptr) {
                  auto repl_map = std::allocate_shared<locked_file_recover_state>(allocator, allocator);
                  fdb->async_tx(
                      [repl_map, id=std::move(id)](const auto& record) -> std::error_code {
                        return std::visit(
                            [&](const auto& r) -> std::error_code {
                              if (r.file == id)
                                return repl_map->frs.apply(r);
                              return {};
                            },
                            record);
                      },
                      completion_wrapper<void(std::error_code, typename file_db::wal_type::tx_lock)>(
                          std::move(wrapped_handler),
                          [repl_map](auto handler, std::error_code ec, typename file_db::wal_type::tx_lock lock) {
                            if (!ec && !repl_map->frs.exists.value_or(true))
                              ec = make_error_code(std::errc::no_such_file_or_directory);
                            repl_map->lock = std::move(lock);
                            std::invoke(handler, ec, result_map_ptr(repl_map, &repl_map->frs));
                          }));
                } else {
                  std::error_code ec;
                  result_map_ptr result;
                  auto iter = repl_maps->fr.find(id);
                  if (iter == repl_maps->fr.end() || !iter->second.exists.value_or(true))
                    ec = make_error_code(std::errc::no_such_file_or_directory);
                  else
                    result = result_map_ptr(repl_maps, &iter->second);
                  std::invoke(wrapped_handler, std::move(ec), std::move(result));
                }
              },
              asio::deferred, std::move(input_ec));
        });
  }

  auto get_raw_reader_op_(file_id id) const {
    using underlying_fd = typename file_db::fd_type;

    return compute_replacements_map_1_op_(id)
    | asio::deferred(
        [fdb=this->fdb_, id, allocator=this->alloc_](std::error_code ec, std::shared_ptr<const typename file_replacements::mapped_type> state) mutable {
          return detail::deferred_on_executor<void(std::error_code, std::shared_ptr<const underlying_fd>, std::shared_ptr<const typename file_replacements::mapped_type>)>(
              [id](std::error_code ec, std::shared_ptr<const file_db> fdb, std::shared_ptr<const typename file_replacements::mapped_type> state, auto allocator) {
                assert(fdb->strand_.running_in_this_thread());

                if (!ec && state->exists.has_value() && !state->exists.value()) [[unlikely]]
                  ec = std::make_error_code(std::errc::no_such_file_or_directory);

                std::shared_ptr<const underlying_fd> ufd;
                if (!ec) [[likely]] {
                  auto files_iter = fdb->files.find(id);
                  if (files_iter != fdb->files.end())
                    ufd = files_iter->second.fd;
                  else
                    ufd = std::allocate_shared<underlying_fd>(allocator, fdb->get_executor());
                }
                return asio::deferred.values(ec, std::move(ufd), std::move(state));
              },
              fdb->strand_, ec, std::move(fdb), std::move(state), std::move(allocator));
        })
    | asio::deferred(
        [](std::error_code ec, std::shared_ptr<const underlying_fd> ufd, std::shared_ptr<const typename file_replacements::mapped_type> state) {
          typename raw_reader_type::size_type filesize = 0;
          std::shared_ptr<const detail::replacement_map<typename file_replacements::mapped_type::fd_type, allocator_type>> replacements;
          if (state != nullptr) [[likely]] {
            filesize = state->file_size.value_or(ufd != nullptr && ufd->is_open() ? ufd->size() : 0u);
            replacements = std::shared_ptr<const detail::replacement_map<typename file_replacements::mapped_type::fd_type, allocator_type>>(state, &state->replacements);
          }

          return asio::deferred.values(
              ec,
              raw_reader_type(ufd, std::move(replacements), filesize));
        });
  }

  auto get_reader_op_(file_id id) const {
    return get_raw_reader_op_(id)
    | asio::deferred(
        [ writes_mon=this->writes_mon_,
          wmap=this->writes_map_,
          allocator=this->alloc_,
          m=this->m_,
          id
        ](std::error_code ec, raw_reader_type raw_reader) mutable {
          return asio::async_initiate<decltype(asio::deferred), void(typename monitor_type::shared_lock, std::error_code, reader_type)>(
              [allocator, m](auto handler, std::error_code ec, raw_reader_type raw_reader, auto writes_mon, std::shared_ptr<const writes_map> wmap, file_id id) {
                if (!read_permitted(m)) ec = make_error_code(file_db_errc::read_not_permitted);

                writes_mon.async_shared(
                    detail::completion_wrapper<void(typename monitor_type::shared_lock)>(
                        std::move(handler),
                        [ ec, allocator, wmap,
                          id=std::move(id),
                          raw_reader=std::move(raw_reader)
                        ](auto handler, typename monitor_type::shared_lock lock) mutable {
                          std::shared_ptr<writes_map_element> tx_writes;
                          auto writes_iter = wmap->find(id);
                          if (writes_iter != wmap->end()) tx_writes = writes_iter->second;

                          auto file_size = tx_writes == nullptr ? raw_reader.size() : tx_writes->file_size.value_or(raw_reader.size());
                          std::invoke(handler,
                              std::move(lock),
                              ec,
                              reader_type(
                                  std::allocate_shared<raw_reader_type>(allocator, std::move(raw_reader)),
                                  std::shared_ptr<detail::replacement_map<writes_buffer_type, allocator_type>>(tx_writes, &tx_writes->replacements),
                                  file_size));
                        }));
              },
              asio::deferred, ec, std::move(raw_reader), std::move(writes_mon), std::move(wmap), std::move(id));
        });
  }

  auto get_writer_op_(file_id id) {
    return get_raw_reader_op_(id)
    | asio::deferred(
        [ writes_mon=this->writes_mon_,
          wmap=this->writes_map_,
          allocator=this->alloc_,
          m=this->m_,
          id
        ](std::error_code ec, raw_reader_type raw_reader) mutable {
          return asio::async_initiate<decltype(asio::deferred), void(typename monitor_type::exclusive_lock, std::error_code, std::shared_ptr<writes_map_element>, typename raw_reader_type::size_type)>(
              [allocator, m](auto handler, std::error_code ec, raw_reader_type raw_reader, auto writes_mon, std::shared_ptr<writes_map> wmap, file_id id) {
                if (!write_permitted(m)) ec = make_error_code(file_db_errc::write_not_permitted);

                writes_mon.async_exclusive(
                    detail::completion_wrapper<void(typename monitor_type::exclusive_lock)>(
                        std::move(handler),
                        [ ec, allocator, wmap,
                          id=std::move(id),
                          raw_reader=std::move(raw_reader)
                        ](auto handler, typename monitor_type::exclusive_lock lock) mutable {
                          std::shared_ptr<writes_map_element> tx_writes;
                          auto writes_iter = wmap->find(id);
                          if (writes_iter == wmap->end()) {
                            bool inserted;
                            std::tie(writes_iter, inserted) = wmap->emplace(
                                std::move(id),
                                std::allocate_shared<writes_map_element>(allocator, allocator));
                            assert(inserted);
                          }
                          tx_writes = writes_iter->second;

                          auto file_size = tx_writes == nullptr ? raw_reader.size() : tx_writes->file_size.value_or(raw_reader.size());
                          std::invoke(handler,
                              std::move(lock),
                              ec,
                              std::move(tx_writes),
                              file_size);
                        }));
              },
              asio::deferred, ec, std::move(raw_reader), std::move(writes_mon), std::move(wmap), std::move(id));
        });
  }

  template<typename Alloc>
  auto async_file_contents_op_(file_id id, Alloc alloc) const {
    using bytes_vec = std::vector<std::byte, typename std::allocator_traits<Alloc>::template rebind_alloc<std::byte>>;

    return get_reader_op_(std::move(id))
    | asio::deferred(
        [alloc=std::move(alloc), fdb=this->fdb_](typename monitor_type::shared_lock lock, std::error_code ec, reader_type r) mutable {
          return asio::async_initiate<decltype(asio::deferred), void(std::error_code, bytes_vec)>(
              [](auto handler, auto alloc, std::error_code ec, reader_type r, auto fdb_ex, typename monitor_type::shared_lock lock) {
                if (ec) {
                  std::invoke(
                      detail::completion_handler_fun(handler, fdb_ex),
                      ec, bytes_vec(std::move(alloc)));
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
              },
              asio::deferred, std::move(alloc), std::move(ec), std::move(r), fdb->get_executor(), lock);
        });
  }

  template<typename MB>
  auto async_file_read_some_op_(file_id id, std::uint64_t offset, MB&& buffers) const {
    return get_reader_op_(std::move(id))
    | asio::deferred(
        [ fdb=this->fdb_,
          buffers=std::forward<MB>(buffers),
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
              asio::deferred, ec, offset, std::move(buffers), std::move(lock), std::move(r), fdb->get_executor());
        });
  }

  template<typename MB>
  auto async_file_write_some_op_(file_id id, std::uint64_t offset, MB&& buffers) {
    auto buffer = std::allocate_shared<writes_buffer_type>(alloc_, fdb_->get_executor(), alloc_);
    std::error_code ec;
    asio::write_at(*buffer, 0, std::forward<MB>(buffers), asio::transfer_all(), ec);

    return get_writer_op_(std::move(id))
    | asio::deferred(
        [ fdb=this->fdb_,
          buffer=std::move(buffer),
          write_ec=std::move(ec),
          offset
        ](typename monitor_type::exclusive_lock lock, std::error_code ec, std::shared_ptr<writes_map_element> replacements, auto filesize) mutable {
          return asio::async_initiate<decltype(asio::deferred), void(std::error_code, std::size_t)>(
              [write_ec, offset, fdb=std::move(fdb), buffer=std::move(buffer)](auto handler, typename monitor_type::exclusive_lock lock, std::error_code ec, std::shared_ptr<writes_map_element> replacements, auto filesize) {
                auto wrapped_handler = detail::completion_handler_fun(std::move(handler), std::move(fdb->get_executor()));
                if (ec) {
                  std::invoke(wrapped_handler, ec, 0);
                  return;
                }
                if (write_ec) {
                  std::invoke(wrapped_handler, write_ec, 0);
                  return;
                }

                const auto buffer_size = buffer->cdata().size();
                // We don't allow writes past the end of a file.
                if (offset > replacements->file_size.value_or(filesize) ||
                    buffer_size > replacements->file_size.value_or(filesize) - offset) {
                  std::invoke(wrapped_handler, make_error_code(file_db_errc::write_past_eof), 0);
                  return;
                }

                replacements->replacements.insert(offset, 0, buffer_size, std::move(buffer));
                lock.reset();
                std::invoke(wrapped_handler, std::error_code(), buffer_size);
              },
              asio::deferred, std::move(lock), ec, std::move(replacements), std::move(filesize));
        });
  }

  std::shared_ptr<FileDB> fdb_;
  allocator_type alloc_;
  isolation i_;
  tx_mode m_;
  std::shared_ptr<writes_map> writes_map_;
  mutable detail::fanout<executor_type, void(std::error_code, std::shared_ptr<const locked_file_replacements>), allocator_type> read_barrier_; // Barrier to fill in reads.
  mutable monitor_type writes_mon_;
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
  auto async_truncate(std::uint64_t new_size, CompletionToken&& token) {
    return tx_.async_truncate_op_(new_size) | std::forward<CompletionToken>(token);
  }

  template<typename MB, typename CompletionToken>
  auto async_read_some_at(std::uint64_t offset, MB&& mb, CompletionToken&& token) const {
    return tx_.async_file_read_some_op_(id_, offset, std::forward<MB>(mb)) | std::forward<CompletionToken>(token);
  }

  template<typename MB, typename CompletionToken>
  auto async_write_some_at(std::uint64_t offset, MB&& mb, CompletionToken&& token) {
    return tx_.async_file_write_some_op_(id_, offset, std::forward<MB>(mb)) | std::forward<CompletionToken>(token);
  }

  private:
  transaction<FileDB, Allocator> tx_;
  file_id id_;
};


} /* namespace earnest */
