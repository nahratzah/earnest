#pragma once

#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <earnest/detail/completion_handler_fun.h>
#include <earnest/detail/completion_wrapper.h>
#include <earnest/detail/namespace_map.h>
#include <earnest/detail/positional_stream_adapter.h>
#include <earnest/detail/replacement_map.h>
#include <earnest/detail/replacement_map_reader.h>
#include <earnest/detail/wal_file.h>
#include <earnest/detail/wal_records.h>
#include <earnest/file_db_error.h>
#include <earnest/file_id.h>

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
    using ::earnest::detail::wal_record_create_file;
    using ::earnest::detail::wal_record_modify_file_write32;
    using ::earnest::detail::wal_record_truncate_file;
    using ::earnest::detail::record_write_type_t;
    using ::earnest::detail::wal_record_variant;
    using ::earnest::detail::namespace_map;

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto completion_handler, std::shared_ptr<file_db> fdb, dir d) {
          asio::dispatch(
              completion_wrapper<void()>(
                  completion_handler_fun(std::move(completion_handler), fdb->get_executor(), fdb->strand_, fdb->get_allocator()),
                  [d, fdb](auto handler) mutable {
                    if (fdb->wal != nullptr) {
                      invoke(handler, make_error_code(wal_errc::bad_state));
                      return;
                    }

                    // Wrap the handler with a cleanup-function that executes on errors.
                    auto wrapped_handler = completion_wrapper<void(std::error_code)>(
                        std::move(handler),
                        [fdb](auto handler, std::error_code ec) {
                          if (ec) {
                            fdb->wal.reset();
                            fdb->namespaces.clear();
                            fdb->files.clear();
                          }
                          std::invoke(handler, ec);
                        });

                    fdb->wal = std::allocate_shared<wal_type>(fdb->get_allocator(), fdb->get_executor(), fdb->get_allocator());
                    fdb->wal->async_create(
                        std::move(d),
                        completion_wrapper<void(std::error_code)>(
                            std::move(wrapped_handler),
                            [fdb](auto handler, std::error_code ec) {
                              if (ec) {
                                std::invoke(handler, ec);
                                return;
                              }

                              auto stream_ptr = std::make_unique<byte_stream<executor_type>>(fdb->get_executor());
                              auto& stream = *stream_ptr;
                              async_write(
                                  stream,
                                  xdr_writer<>() & xdr_constant(namespace_map<>{ .files={ file_id("", namespaces_filename) } }),
                                  completion_wrapper<void(std::error_code)>(
                                      std::move(handler),
                                      [fdb, stream_ptr=std::move(stream_ptr)](auto handler, std::error_code ec) {
                                        assert(asio::get_associated_executor(handler) == fdb->strand_);
                                        if (ec) {
                                          std::invoke(handler, ec);
                                          return;
                                        }

                                        const auto ns_file_size = stream_ptr->data().size();
                                        const auto ns_file_id = file_id("", namespaces_filename);
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
                                                .data=std::move(*stream_ptr).data()
                                              }
                                            },
                                            completion_wrapper<void(std::error_code)>(
                                                std::move(handler),
                                                [fdb](auto handler, std::error_code ec) {
                                                  if (ec)
                                                    std::invoke(handler, ec);
                                                  else
                                                    fdb->recover_namespaces_(std::move(handler));
                                                }));
                                      }));
                            }));
                  }));
        },
        token, this->shared_from_this(), std::move(d));
  }

  private:
  template<typename Handler>
  auto recover_namespaces_(Handler&& handler) {
    assert(strand_.running_in_this_thread());
    assert(wal != nullptr);

    struct state_t {
      explicit state_t(executor_type ex, rebind_alloc<std::byte> alloc)
      : replacements(std::move(alloc)),
        actual_file(std::move(ex))
      {}

      detail::replacement_map<typename wal_type::fd_type, rebind_alloc<std::byte>> replacements;
      fd<executor_type> actual_file;
      bool exists = false;
      std::uint64_t file_size = 0;
    };

    auto state_ptr = std::make_unique<state_t>(get_executor(), get_allocator());
    auto& state_ref = *state_ptr;
    {
      std::error_code open_ec;
      state_ptr->actual_file.open(wal->get_dir(), namespaces_filename, open_mode::READ_ONLY, open_ec);
      if (open_ec == make_error_code(std::errc::no_such_file_or_directory)) {
        state_ptr->exists = false;
      } else if (open_ec) {
        std::invoke(handler, open_ec);
        return;
      } else {
        state_ptr->exists = true;
        state_ptr->file_size = state_ptr->actual_file.size();
      }
    }

    wal->async_records(
        [&state_ref]<typename E, typename R>(const detail::wal_record_variant<E, R>& record) {
          auto ec = std::visit(
              [ &state_ref,
                ns_file_id=file_id("", namespaces_filename)
              ](const auto& record) {
                using record_type = std::remove_cvref_t<decltype(record)>;

                if constexpr(std::is_same_v<record_type, detail::wal_record_create_file>) {
                  if (record.file == ns_file_id) {
                    state_ref.exists = true;
                    state_ref.file_size = 0;
                    state_ref.replacements.clear();
                  }
                } else if constexpr(std::is_same_v<record_type, detail::wal_record_erase_file>) {
                  if (record.file == ns_file_id) {
                    state_ref.exists = false;
                    state_ref.file_size = 0;
                    state_ref.replacements.clear();
                  }
                } else if constexpr(std::is_same_v<record_type, detail::wal_record_truncate_file>) {
                  if (record.file == ns_file_id) {
                    if (!state_ref.exists) {
                      std::clog << "File-DB: namespaces file was truncated, but logically doesn't exist\n";
                      return make_error_code(file_db_errc::unrecoverable);
                    }
                    state_ref.file_size = record.new_size;
                    state_ref.replacements.truncate(record.new_size);
                  }
                } else if constexpr(std::is_same_v<record_type, detail::wal_record_modify_file32<E, R>>) {
                  if (record.file == ns_file_id) {
                    if (!state_ref.exists) {
                      std::clog << "File-DB: namespaces file was written to, but logically doesn't exist\n";
                      return make_error_code(file_db_errc::unrecoverable);
                    }
                    state_ref.replacements.insert(record);
                  }
                }
                return std::error_code();
              },
              record);
          return ec;
        },
        completion_wrapper<void(std::error_code)>(
            std::forward<Handler>(handler),
            [ fdb=this->shared_from_this(),
              state_ptr=std::move(state_ptr)
            ](auto handler, std::error_code ec) {
              assert(fdb->strand_.running_in_this_thread());

              if (!ec && !state_ptr->exists) {
                std::clog << "File-DB: namespace file does not exist\n";
                ec = make_error_code(file_db_errc::unrecoverable);
              }
              if (ec) [[unlikely]] {
                std::invoke(handler, ec);
                return;
              }

              if (!state_ptr->exists) state_ptr->actual_file.close();

              using reader_type = detail::replacement_map_reader<
                  fd_type,
                  typename wal_type::fd_type,
                  rebind_alloc<std::byte>>;
              auto reader_ptr = std::make_unique<detail::positional_stream_adapter<reader_type>>(reader_type(std::move(state_ptr->actual_file), std::move(state_ptr->replacements), state_ptr->file_size));
              auto& reader_ref = *reader_ptr;
              auto namespaces_map_ptr = std::make_unique<detail::namespace_map<>>();
              auto& namespaces_map_ref = *namespaces_map_ptr;
              async_read(
                  reader_ref,
                  xdr_reader<>() & namespaces_map_ref,
                  completion_wrapper<void(std::error_code)>(
                      std::move(handler),
                      [ reader_ptr=std::move(reader_ptr),
                        namespaces_map_ptr=std::move(namespaces_map_ptr),
                        fdb
                      ](auto handler, std::error_code ec) mutable {
                        assert(fdb->strand_.running_in_this_thread());

                        if (ec) {
                          std::invoke(handler, ec);
                          return;
                        }

                        reader_ptr.reset();
                        try {
                          std::transform(
                              namespaces_map_ptr->namespaces.begin(), namespaces_map_ptr->namespaces.end(),
                              std::inserter(fdb->namespaces, fdb->namespaces.end()),
                              [](const std::pair<const std::string, std::string>& ns_entry) {
                                return std::make_pair(ns_entry.first, ns{ .d{ns_entry.second} });
                              });
                          std::transform(
                              namespaces_map_ptr->files.begin(), namespaces_map_ptr->files.end(),
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
                          std::invoke(handler, ex.code());
                          return;
                        }

                        std::invoke(handler, std::error_code());
                      }));
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
