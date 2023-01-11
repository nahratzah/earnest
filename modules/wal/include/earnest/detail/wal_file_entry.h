#pragma once

#include <concepts>
#include <filesystem>
#include <memory>
#include <ranges>
#include <scoped_allocator>
#include <tuple>
#include <type_traits>
#include <variant>

#include <asio/strand.hpp>

#include <earnest/detail/fanout.h>
#include <earnest/detail/fanout_barrier.h>
#include <earnest/detail/wal_flusher.h>
#include <earnest/detail/wal_records.h>
#include <earnest/dir.h>
#include <earnest/fd.h>
#include <earnest/xdr.h>

namespace earnest::detail {


// XXX this will probably become a template
using wal_record_variant = std::variant<
    std::monostate, // 0
    wal_record_noop, // 1
    wal_record_skip32, // 2
    wal_record_skip64, // 3
    wal_record_seal, // 4
    wal_record_wal_archived, // 5
    wal_record_reserved<6>,
    wal_record_reserved<7>,
    wal_record_reserved<8>,
    wal_record_reserved<9>,
    wal_record_reserved<10>,
    wal_record_reserved<11>,
    wal_record_reserved<12>,
    wal_record_reserved<13>,
    wal_record_reserved<14>,
    wal_record_reserved<15>,
    wal_record_reserved<16>,
    wal_record_reserved<17>,
    wal_record_reserved<18>,
    wal_record_reserved<19>,
    wal_record_reserved<20>,
    wal_record_reserved<21>,
    wal_record_reserved<22>,
    wal_record_reserved<23>,
    wal_record_reserved<24>,
    wal_record_reserved<25>,
    wal_record_reserved<26>,
    wal_record_reserved<27>,
    wal_record_reserved<28>,
    wal_record_reserved<29>,
    wal_record_reserved<30>,
    wal_record_reserved<31>,
    wal_record_create_file, // 32
    wal_record_erase_file // 33
    >;

// Indices 0..31 (inclusive) are reserved for bookkeeping of the WAL.
inline constexpr auto wal_record_is_bookkeeping(std::size_t idx) noexcept -> bool {
  return idx < 32;
}

inline auto wal_record_is_bookkeeping(const wal_record_variant& r) noexcept -> bool {
  return wal_record_is_bookkeeping(r.index());
}


enum class wal_file_entry_state {
  uninitialized,
  opening,
  ready,
  sealing,
  sealed,
  failed = -1
};

auto operator<<(std::ostream& out, wal_file_entry_state state) -> std::ostream&;


template<typename Executor, typename Allocator>
class wal_file_entry
: public std::enable_shared_from_this<wal_file_entry<Executor, Allocator>>
{
  public:
  static inline constexpr std::uint_fast32_t max_version = 0;
  static inline constexpr std::size_t read_buffer_size = 2u * 1024u * 1024u;
  using executor_type = Executor;
  using allocator_type = Allocator;
  using link_done_event_type = fanout<executor_type, void(std::error_code), allocator_type>;

  private:
  template<typename T>
  using rebind_alloc = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;

  public:
  using variant_type = wal_record_variant;
  using write_variant_type = record_write_type_t<wal_record_variant>;
  using records_vector = std::vector<variant_type, rebind_alloc<variant_type>>;
  using write_records_vector = std::vector<write_variant_type, std::scoped_allocator_adaptor<rebind_alloc<write_variant_type>>>;

  wal_file_entry(const executor_type& ex, allocator_type alloc);

  wal_file_entry(const wal_file_entry&) = delete;
  wal_file_entry(wal_file_entry&&) = delete;
  wal_file_entry& operator=(const wal_file_entry&) = delete;
  wal_file_entry& operator=(wal_file_entry&&) = delete;

  auto get_executor() const -> executor_type { return file.get_executor(); }
  auto get_allocator() const -> allocator_type { return alloc_; }
  auto state() const noexcept -> wal_file_entry_state { return state_; }

  private:
  auto header_reader_();
  auto header_writer_() const;

  template<typename Stream, typename Callback>
  auto read_records_(Stream& stream, Callback callback, std::unique_ptr<records_vector> records) -> void; // Has side-effects.
  template<typename Stream, typename Callback>
  auto read_records_until_(Stream& stream, Callback callback, typename fd<executor_type>::offset_type end_offset, std::unique_ptr<records_vector> records) const -> void;
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

  template<typename CompletionToken>
  auto async_append(write_records_vector records, CompletionToken&& token);

  template<typename CompletionToken>
  auto async_seal(CompletionToken&& token);

  template<typename CompletionToken>
  auto async_discard_all(CompletionToken&& token);

  auto write_offset() const noexcept -> typename fd<executor_type>::offset_type;
  auto link_offset() const noexcept -> typename fd<executor_type>::offset_type;
  auto has_unlinked_data() const -> bool;

  template<typename CompletionToken> auto async_records(CompletionToken&& token) const;

  private:
  template<typename CompletionToken, typename OnSpaceAssigned>
  auto append_bytes_(std::vector<std::byte, rebind_alloc<std::byte>>&& bytes, CompletionToken&& token, OnSpaceAssigned&& space_assigned_event, std::error_code ec, wal_file_entry_state expected_state = wal_file_entry_state::ready);

  template<typename CompletionToken, typename Barrier, typename Fanout>
  auto append_bytes_at_(
      typename fd<executor_type>::offset_type write_offset,
      std::vector<std::byte, rebind_alloc<std::byte>>&& bytes,
      CompletionToken&& token,
      Barrier&& barrier, Fanout&& f);

  template<typename CompletionToken>
  auto write_skip_record_(
      typename fd<executor_type>::offset_type write_offset,
      std::size_t bytes, CompletionToken&& token);

  template<typename CompletionToken>
  auto write_link_(
      typename fd<executor_type>::offset_type write_offset,
      std::array<std::byte, 4> bytes, CompletionToken&& token);

  public:
  std::filesystem::path name;
  fd<executor_type> file;
  std::uint_fast32_t version;
  std::uint_fast64_t sequence;

  private:
  allocator_type alloc_;
  typename fd<executor_type>::offset_type write_offset_;
  typename fd<executor_type>::offset_type link_offset_;
  asio::strand<executor_type> strand_;
  wal_file_entry_state state_ = wal_file_entry_state::uninitialized;
  link_done_event_type link_done_event_;
  wal_flusher<fd<executor_type>&, allocator_type> wal_flusher_;

  // Seal barrier becomes ready when:
  // 1. all pending writes have their space allocated.
  // 2. a seal has been requested.
  fanout_barrier<executor_type, allocator_type> seal_barrier_;
};


} /* namespace earnest::detail */

#include "wal_file_entry.ii"
