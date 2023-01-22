#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <utility>

#include <earnest/detail/completion_handler_fun.h>
#include <earnest/detail/completion_wrapper.h>
#include <earnest/detail/namespace_map.h>
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
    using wal_record_modify_file32 = std::variant_alternative<35, variant_type>;
    using wal_record_create_file = detail::wal_record_create_file;
    using wal_record_erase_file = detail::wal_record_erase_file;
    using wal_record_truncate_file = detail::wal_record_truncate_file;

    assert(strand_.running_in_this_thread());
    assert(wal != nullptr);

#if 0
    fdb->files.emplace(
        file_id(std::string(), namespaces_filename),
        file{ .fd{fdb->get_executor()}, .exists_logically{true} });
#endif

    wal->async_records(
        completion_wrapper<void(std::error_code, typename wal_type::records_vector)>(
            std::forward<Handler>(handler),
            [fdb=this->shared_from_this()](auto handler, std::error_code ec, typename wal_type::records_vector records) {
              assert(fdb->strand_.running_in_this_thread());

              if (ec) {
                std::invoke(handler, ec);
                return;
              }

              file namespaces_file{ .fd{fdb->get_executor()}, .exists_logically{true} };
              {
                std::error_code open_ec;
                namespaces_file.fd.open(fdb->wal->get_dir(), namespaces_filename, open_mode::READ_WRITE, open_ec);
                if (open_ec == make_error_code(std::errc::no_such_file_or_directory)) {
                  namespaces_file.exists_logically = false;
                } else if (open_ec) {
                  std::invoke(handler, open_ec);
                  return;
                } else {
                  namespaces_file.file_size = namespaces_file.fd.size();
                }
              }

              std::vector<wal_record_modify_file32, rebind_alloc<wal_record_modify_file32>> overwrites(fdb->get_allocator());

              for (const auto& record : records) {
                std::error_code visit_ec = std::visit(
                    [&](const auto& record) {
                      using record_type = std::remove_cvref_t<decltype(record)>;

                      if constexpr(std::is_same_v<record_type, wal_record_create_file>) {
                        if (record.file == file_id("", namespaces_filename)) {
                          namespaces_file.exists_logically = true;
                          namespaces_file.file_size = 0;
                        }
                      } else if constexpr(std::is_same_v<record_type, wal_record_erase_file>) {
                        if (record.file == file_id("", namespaces_filename)) {
                          namespaces_file.exists_logically = false;
                          namespaces_file.file_size = 0;
                          overwrites.clear();
                        }
                      } else if constexpr(std::is_same_v<record_type, wal_record_truncate_file>) {
                        if (record.file == file_id("", namespaces_filename)) {
                          if (!namespaces_file.exists_logically) {
                            std::clog << "File-DB: namespaces file was truncated, but logically doesn't exist\n";
                            return make_error_code(file_db_errc::unrecoverable);
                          }
                          namespaces_file.file_size = record.new_size;
                        }
                      } else if constexpr(std::is_same_v<record_type, wal_record_modify_file32>) {
                        if (record.file == file_id("", namespaces_filename)) {
                          if (!namespaces_file.exists_logically) {
                            std::clog << "File-DB: namespaces file was written to, but logically doesn't exist\n";
                            return make_error_code(file_db_errc::unrecoverable);
                          }
                          overwrites.push_back(record);
                        }
                      }
                      return std::error_code();
                    },
                    record);
                if (visit_ec) {
                  std::invoke(handler, visit_ec);
                  return;
                }
              }

              fdb->files.emplace(
                  file_id("", namespaces_filename),
                  std::move(namespaces_file));

              // XXX use the overwrites
              std::invoke(handler, std::error_code());
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
