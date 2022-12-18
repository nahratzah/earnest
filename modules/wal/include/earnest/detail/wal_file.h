#ifndef EARNEST_DETAIL_WAL_FILE_H
#define EARNEST_DETAIL_WAL_FILE_H

#include <cassert>
#include <type_traits>
#include <memory>
#include <filesystem>

#include <asio/async_result.hpp>
#include <asio/executor.hpp>
#include <asio/strand.hpp>

#include <earnest/dir.h>
#include <earnest/fd.h>

#include <earnest/detail/replacement_map.h>

namespace earnest::detail {


template<typename Executor>
class wal_file_entry {
  public:
  static inline constexpr std::uint_fast32_t max_version = 0;
  static inline constexpr std::size_t read_buffer_size = 2u * 1024u * 1024u;
  using executor_type = Executor;

  wal_file_entry(const executor_type& ex);
  auto get_executor() const -> executor_type { return file.get_executor(); }

  template<typename CompletionToken>
  auto async_open(const dir& d, const std::filesystem::path& name, CompletionToken&& token);

  private:
  auto header_reader_();
  auto header_writer_() const;

  public:
  std::filesystem::path name;
  fd<executor_type> file;
  std::uint_fast32_t version;
  std::uint_fast64_t sequence;
};


template<typename Executor = asio::executor, typename Alloc = std::allocator<std::byte>>
class wal_file
: public std::enable_shared_from_this<wal_file<Executor, Alloc>>
{
  public:
  using allocator_type = Alloc;
  using executor_type = Executor;

  private:
  template<typename T>
  using rebind_alloc = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;

  using file_entry = wal_file_entry<executor_type>;
  using files_list = std::list<file_entry, rebind_alloc<file_entry>>;

  public:
  class tx;

  wal_file(const wal_file&) = delete;
  explicit wal_file(const executor_type& ex, allocator_type alloc = allocator_type());

  template<typename CompletionToken = typename asio::default_completion_token<executor_type>::type>
  auto async_open(const std::filesystem::path& dirname, CompletionToken&& token = typename asio::default_completion_token<executor_type>::type{})
  -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code)>::return_type;

  auto get_allocator() const -> allocator_type;
  auto get_executor() const -> executor_type;

  private:
  template<typename Callback>
  void async_populate_wal_files_(Callback&& callback);
  template<typename Callback>
  void async_process_wal_files_(Callback&& callback);

  dir d_;
  asio::strand<executor_type> strand_;
  files_list files_;
  replacement_map<allocator_type> pending_writes_;
};


template<typename Executor, typename Alloc>
class wal_file<Executor, Alloc>::tx {
  public:
  using read_offset = typename replacement_map<allocator_type>::offset_type;
  using read_size = typename replacement_map<allocator_type>::size_type;

  tx(wal_file& wf);

  private:
  std::shared_ptr<wal_file> wf_;
  replacement_map<allocator_type> pending_writes_;
  std::map<read_offset, read_size, std::less<read_offset>, rebind_alloc<std::pair<const read_offset, read_size>>> read_ranges_;
};


} /* namespace earnest::detail */

#include "wal_file.ii"

#endif /* EARNEST_DETAIL_WAL_FILE_H */
