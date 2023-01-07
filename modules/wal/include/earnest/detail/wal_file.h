#ifndef EARNEST_DETAIL_WAL_FILE_H
#define EARNEST_DETAIL_WAL_FILE_H

#include <cassert>
#include <filesystem>
#include <list>
#include <map>
#include <memory>
#include <ranges>
#include <scoped_allocator>
#include <tuple>
#include <type_traits>
#include <variant>

#include <asio/async_result.hpp>
#include <asio/executor.hpp>
#include <asio/strand.hpp>

#include <earnest/dir.h>
#include <earnest/fd.h>

#include <earnest/detail/fanout.h>
#include <earnest/detail/replacement_map.h>
#include <earnest/xdr.h>

namespace earnest::detail {


template<typename> struct record_write_type_;
template<> struct record_write_type_<std::monostate> { using type = std::monostate; };

template<typename T>
using record_write_type_t = typename record_write_type_<T>::type;

template<typename... T>
struct record_write_type_<std::variant<T...>> {
  using type = std::variant<record_write_type_t<T>...>;
};

template<typename... T>
struct record_write_type_<std::tuple<T...>> {
  using type = std::tuple<record_write_type_t<T>...>;
};


struct wal_record_noop {};
template<> struct record_write_type_<wal_record_noop> { using type = wal_record_noop; };

template<typename X>
inline auto operator&(xdr<X>&& x, const wal_record_noop& noop) noexcept -> xdr<X>&&;


struct wal_record_skip32 {
  std::uint32_t bytes;
};
template<> struct record_write_type_<wal_record_skip32> { using type = wal_record_skip32; };

struct wal_record_skip64 {
  std::uint64_t bytes;
};
template<> struct record_write_type_<wal_record_skip64> { using type = wal_record_skip64; };

template<typename... X>
inline auto operator&(::earnest::xdr_reader<X...>&& x, wal_record_skip32& skip);
template<typename... X>
inline auto operator&(::earnest::xdr_writer<X...>&& x, const wal_record_skip32& skip);

template<typename... X>
inline auto operator&(::earnest::xdr_reader<X...>&& x, wal_record_skip64& skip);
template<typename... X>
inline auto operator&(::earnest::xdr_writer<X...>&& x, const wal_record_skip64& skip);


// XXX this will probably become a template
using wal_record_variant = std::variant<std::monostate, wal_record_noop, wal_record_skip32, wal_record_skip64>;


enum class wal_file_entry_state {
  uninitialized,
  opening,
  ready,
  failed = -1
};

auto operator<<(std::ostream& out, wal_file_entry_state state) -> std::ostream&;


template<typename Executor, typename Allocator>
class wal_file_entry {
  public:
  static inline constexpr std::uint_fast32_t max_version = 0;
  static inline constexpr std::size_t read_buffer_size = 2u * 1024u * 1024u;
  using executor_type = Executor;
  using allocator_type = Allocator;

  private:
  template<typename T>
  using rebind_alloc = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;

  public:
  using variant_type = wal_record_variant;
  using write_variant_type = record_write_type_t<wal_record_variant>;
  using records_vector = std::vector<variant_type, rebind_alloc<variant_type>>;

  private:
  using write_records_vector = std::vector<write_variant_type, std::scoped_allocator_adaptor<rebind_alloc<write_variant_type>>>;

  public:
  wal_file_entry(const executor_type& ex, allocator_type alloc);

  wal_file_entry(const wal_file_entry&) = delete;
  wal_file_entry(wal_file_entry&&) = delete;
  wal_file_entry& operator=(const wal_file_entry&) = delete;
  wal_file_entry& operator=(wal_file_entry&&) = delete;

  auto get_executor() const -> executor_type { return file.get_executor(); }
  auto get_allocator() const -> allocator_type { return records_.get_allocator(); }
  auto state() const noexcept -> wal_file_entry_state { return state_; }

  private:
  auto header_reader_();
  auto header_writer_() const;

  template<typename Stream, typename Callback>
  auto read_records_(Stream& stream, Callback callback) -> void;
  template<typename CompletionToken>
  auto write_records_to_buffer_(write_records_vector&& records, CompletionToken&& token) const;
  template<typename State>
  static auto write_records_to_buffer_iter_(std::unique_ptr<State> state_ptr, typename write_records_vector::const_iterator b, typename write_records_vector::const_iterator e, std::error_code ec) -> void;

  public:
  template<typename CompletionToken>
  auto async_open(const dir& d, const std::filesystem::path& name, CompletionToken&& token);
  template<typename CompletionToken>
  auto async_create(const dir& d, const std::filesystem::path& name, std::uint_fast64_t sequence, CompletionToken&& token);

  template<typename Range, typename CompletionToken>
#if __cpp_concepts >= 201907L
  requires std::ranges::input_range<std::remove_reference_t<Range>>
#endif
  auto async_append(Range&& records, CompletionToken&& token);

  auto write_offset() const noexcept -> typename fd<executor_type>::offset_type;
  auto link_offset() const noexcept -> typename fd<executor_type>::offset_type;
  auto records() const noexcept -> const records_vector& { return records_; }
  auto has_unlinked_data() const -> bool;

  private:
  template<typename CompletionToken>
  auto append_bytes_(std::vector<std::byte, rebind_alloc<std::byte>>&& bytes, CompletionToken&& token, std::error_code ec);

  template<typename CompletionToken, typename Barrier, typename Fanout>
  auto append_bytes_at_(
      typename fd<executor_type>::offset_type link_offset,
      typename fd<executor_type>::offset_type write_offset,
      std::vector<std::byte, rebind_alloc<std::byte>>&& bytes,
      CompletionToken&& token,
      Barrier&& barrier, Fanout&& f);

  template<typename CompletionToken>
  auto write_skip_record_(
      typename fd<executor_type>::offset_type link_offset,
      typename fd<executor_type>::offset_type write_offset,
      std::size_t bytes, CompletionToken&& token);

  template<typename CompletionToken>
  auto write_link_(
      typename fd<executor_type>::offset_type link_offset,
      std::array<std::byte, 4> bytes, CompletionToken&& token);

  public:
  std::filesystem::path name;
  fd<executor_type> file;
  std::uint_fast32_t version;
  std::uint_fast64_t sequence;

  private:
  typename fd<executor_type>::offset_type write_offset_;
  typename fd<executor_type>::offset_type link_offset_;
  records_vector records_;
  asio::strand<executor_type> strand_;
  wal_file_entry_state state_ = wal_file_entry_state::uninitialized;
  fanout<executor_type, void(std::error_code), allocator_type> link_done_event_;
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

  using file_entry = wal_file_entry<executor_type, allocator_type>;
  using files_list = std::list<file_entry, std::scoped_allocator_adaptor<rebind_alloc<file_entry>>>;

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
