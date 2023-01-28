#pragma once

#include <cassert>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <asio/append.hpp>
#include <asio/async_result.hpp>
#include <asio/bind_executor.hpp>
#include <asio/deferred.hpp>
#include <asio/strand.hpp>
#include <asio/write.hpp>

#include <earnest/detail/completion_handler_fun.h>
#include <earnest/detail/completion_wrapper.h>
#include <earnest/detail/deferred_on_executor.h>
#include <earnest/detail/file_recover_state.h>
#include <earnest/detail/namespace_map.h>
#include <earnest/detail/positional_stream_adapter.h>
#include <earnest/detail/replacement_map.h>
#include <earnest/detail/replacement_map_reader.h>
#include <earnest/detail/wal_file.h>
#include <earnest/detail/wal_records.h>
#include <earnest/file_db_error.h>
#include <earnest/file_id.h>
#include <earnest/isolation.h>

namespace earnest {


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
  public:
  using executor_type = Executor;
  using allocator_type = Allocator;
  static inline const std::string namespaces_filename = "namespaces.fdb";

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
    fd_type fd;
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
  auto async_records(Acceptor&& acceptor, CompletionToken&& token) const {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, std::shared_ptr<const file_db> fdb, auto acceptor) {
          asio::dispatch(
              completion_handler_fun(
                  completion_wrapper<void()>(
                      std::move(handler),
                      [fdb, acceptor=std::move(acceptor)](auto handler) mutable -> void {
                        if (fdb->wal == nullptr) [[unlikely]] {
                          std::invoke(handler, make_error_code(wal_errc::bad_state));
                          return;
                        }

                        fdb->wal->async_records(
                            [acceptor=std::move(acceptor)](variant_type v) mutable -> std::error_code {
                              if (wal_record_is_bookkeeping(v)) return {};
                              return std::invoke(acceptor, make_wal_record_no_bookeeping(std::move(v)));
                            },
                            std::move(handler));
                      }),
                  fdb->get_executor()));
        },
        token, this->shared_from_this(), std::forward<Acceptor>(acceptor));
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
                    xdr_writer<>() & xdr_constant(namespace_map<>{ .files={ file_id("", namespaces_filename) } }),
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
              const auto ns_file_id = file_id("", namespaces_filename);
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

  auto read_namespace_replacements_op_() {
    using ::earnest::detail::completion_handler_fun;
    using ::earnest::detail::completion_wrapper;

    return asio::async_initiate<decltype(asio::deferred), void(std::error_code, std::unique_ptr<detail::file_recover_state<typename wal_type::fd_type, allocator_type>>)>(
        [](auto handler, std::shared_ptr<file_db> fdb) {
          asio::dispatch(
              completion_wrapper<void()>(
                  completion_handler_fun(std::move(handler), fdb->get_executor(), fdb->strand_, fdb->get_allocator()),
                  [fdb](auto handler) {
                    assert(fdb->strand_.running_in_this_thread());
                    assert(fdb->wal != nullptr);

                    auto state_ptr = std::make_unique<detail::file_recover_state<typename wal_type::fd_type, allocator_type>>(
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
                          return state_ref.apply(record);
                        },
                        asio::append(std::move(handler), std::move(state_ptr)));
                  }));
        },
        asio::deferred, this->shared_from_this());
  }

  auto read_namespaces_op_(detail::file_recover_state<typename wal_type::fd_type, allocator_type> state) {
    using ::earnest::detail::namespace_map;
    using ::earnest::detail::positional_stream_adapter;
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
        reader_type(std::move(state.actual_file), std::move(state.replacements), std::move(state.file_size)));
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
            [](std::error_code ec, std::unique_ptr<detail::file_recover_state<typename wal_type::fd_type, allocator_type>> state_ptr, std::shared_ptr<file_db> fdb) {
              assert(fdb->strand_.running_in_this_thread());

              if (!ec) {
                // If nothing declared a state on the namespaces file,
                // then propagate the filesystem state.
                if (!state_ptr->exists.has_value()) state_ptr->exists = state_ptr->actual_file.is_open();
                if (!state_ptr->exists.value()) {
                  std::clog << "File-DB: namespace file does not exist\n";
                  ec = make_error_code(file_db_errc::unrecoverable);
                }
              }

              return asio::deferred.when(!ec)
                  .then(fdb->read_namespaces_op_(std::move(*state_ptr)))
                  .otherwise(asio::deferred.values(ec, namespace_map()));
            }))
    | asio::append(asio::deferred, this->shared_from_this())
    | asio::bind_executor(
        this->strand_,
        asio::deferred(
            [](std::error_code ec, namespace_map<> ns_map, std::shared_ptr<file_db> fdb) {
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
                              .fd{fdb->get_executor()},
                              .exists_logically=true,
                            });

                        if (id.ns.empty()) {
                          try {
                            result.second.fd.open(fdb->wal->get_dir(), id.filename, open_mode::READ_WRITE);
                          } catch (const std::system_error& ex) {
                            // Recovery will deal with it.
                          }
                        } else {
                          const auto ns_iter = fdb->namespaces.find(id.ns);
                          if (ns_iter == fdb->namespaces.end())
                            throw std::system_error(std::make_error_code(std::errc::no_such_file_or_directory));
                          try {
                            result.second.fd.open(ns_iter->second.d, id.filename, open_mode::READ_WRITE);
                          } catch (const std::system_error& ex) {
                            // Recovery will deal with it.
                          }
                        }

                        if (result.second.fd.is_open())
                          result.second.file_size = result.second.fd.size();
                        return result;
                      });
                } catch (const std::system_error& ex) {
                  ec = ex.code();
                }
              }

              return asio::deferred.values(ec);
            }));
  }

  allocator_type alloc_;

  public:
  std::shared_ptr<wal_type> wal;
  std::unordered_map<std::string, ns, std::hash<std::string>, std::equal_to<std::string>, rebind_alloc<std::pair<const std::string, ns>>> namespaces;
  std::unordered_map<file_id, file, std::hash<file_id>, std::equal_to<file_id>, rebind_alloc<std::pair<const file_id, file>>> files;

  private:
  asio::strand<executor_type> strand_;
};


} /* namespace earnest */
