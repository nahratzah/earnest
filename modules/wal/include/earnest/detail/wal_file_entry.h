#pragma once

#include <concepts>
#include <filesystem>
#include <memory>
#include <ranges>
#include <scoped_allocator>
#include <tuple>
#include <type_traits>

#include <asio/strand.hpp>

#include <earnest/detail/fanout.h>
#include <earnest/detail/fanout_barrier.h>
#include <earnest/detail/move_only_function.h>
#include <earnest/detail/wal_flusher.h>
#include <earnest/detail/wal_records.h>
#include <earnest/dir.h>
#include <earnest/fd.h>
#include <earnest/xdr.h>

namespace earnest::detail {


enum class wal_file_entry_state {
  uninitialized,
  opening,
  ready,
  sealing,
  sealed,
  failed = -1
};

auto operator<<(std::ostream& out, wal_file_entry_state state) -> std::ostream&;


struct no_transaction_validation {};


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
  using variant_type = wal_record_variant<Executor>;
  using write_variant_type = record_write_type_t<variant_type>;
  using records_vector = std::vector<variant_type, rebind_alloc<variant_type>>;
  using write_records_vector = std::vector<write_variant_type, std::scoped_allocator_adaptor<rebind_alloc<write_variant_type>>>;
  using fd_type = fd<executor_type>;

  private:
  struct write_record_with_offset {
    write_variant_type record;
    std::uint64_t offset;
  };
  using write_record_with_offset_vector = std::vector<write_record_with_offset, std::scoped_allocator_adaptor<rebind_alloc<write_record_with_offset>>>;

  public:
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
  auto read_records_(Stream& stream, Callback callback, std::unique_ptr<variant_type> ptr) -> void; // Has side-effects.

  template<typename Stream, typename Acceptor, typename Callback>
  auto read_records_until_(Stream& stream, Acceptor&& acceptor, Callback callback, typename fd_type::offset_type end_offset) const -> void;

  auto decorate_(variant_type& v) const -> void;

  template<typename CompletionToken>
  auto write_records_to_buffer_(write_records_vector&& records, CompletionToken&& token) const;
  template<typename State>
  static auto write_records_to_buffer_iter_(std::unique_ptr<State> state_ptr, typename write_records_vector::const_iterator b, typename write_records_vector::const_iterator e, std::error_code ec) -> void;

  public:
  template<typename CompletionToken>
  auto async_open(const dir& d, const std::filesystem::path& name, CompletionToken&& token);
  template<typename CompletionToken>
  auto async_create(const dir& d, const std::filesystem::path& name, std::uint_fast64_t sequence, CompletionToken&& token);

  template<typename Range, typename CompletionToken, typename TransactionValidator = no_transaction_validation>
#if __cpp_concepts >= 201907L
  requires std::ranges::input_range<std::remove_reference_t<Range>>
#endif
  auto async_append(Range&& records, CompletionToken&& token, TransactionValidator&& transaction_validator = no_transaction_validation(), move_only_function<void(records_vector)> on_successful_write_callback = move_only_function<void(records_vector)>());

  template<typename CompletionToken, typename TransactionValidator = no_transaction_validation>
  auto async_append(write_records_vector records, CompletionToken&& token, TransactionValidator&& transaction_validator = no_transaction_validation(), move_only_function<void(records_vector)> on_successful_write_callback = move_only_function<void(records_vector)>());

  private:
  template<typename OnSpaceAssigned, typename TransactionValidator>
  auto async_append_impl_(write_records_vector records, move_only_function<void(std::error_code)>&& completion_handler, OnSpaceAssigned&& on_space_assigned, TransactionValidator&& transaction_validator, move_only_function<void(records_vector)> on_successful_write_callback) -> void;

  public:
  template<typename CompletionToken>
  auto async_seal(CompletionToken&& token);

  template<typename CompletionToken>
  auto async_discard_all(CompletionToken&& token);

  auto write_offset() const noexcept -> typename fd_type::offset_type;
  auto link_offset() const noexcept -> typename fd_type::offset_type;
  auto has_unlinked_data() const -> bool;

  template<typename Acceptor, typename CompletionToken>
  auto async_records(Acceptor&& acceptor, CompletionToken&& token) const;

  template<typename CompletionToken>
  [[deprecated]]
  auto async_records(CompletionToken&& token) const;

  private:
  auto async_records_impl_(move_only_function<std::error_code(variant_type)> acceptor, move_only_function<void(std::error_code)> completion_handler) const;

  template<typename CompletionToken, typename OnSpaceAssigned, typename TransactionValidator>
  auto append_bytes_(std::vector<std::byte, rebind_alloc<std::byte>>&& bytes, CompletionToken&& token, OnSpaceAssigned&& space_assigned_event, std::error_code ec, wal_file_entry_state expected_state, TransactionValidator&& transaction_validator, move_only_function<void(records_vector)> on_successful_write_callback, write_record_with_offset_vector records);

  template<typename CompletionToken, typename Barrier, typename Fanout, typename DeferredTransactionValidator>
  auto append_bytes_at_(
      typename fd_type::offset_type write_offset,
      std::vector<std::byte, rebind_alloc<std::byte>>&& bytes,
      CompletionToken&& token,
      Barrier&& barrier, Fanout&& f,
      DeferredTransactionValidator&& deferred_transaction_validator,
      move_only_function<void(records_vector)> on_successful_write_callback,
      write_record_with_offset_vector records);

  template<typename CompletionToken>
  auto write_skip_record_(
      typename fd_type::offset_type write_offset,
      std::size_t bytes, CompletionToken&& token);

  template<typename CompletionToken>
  auto write_link_(
      typename fd_type::offset_type write_offset,
      std::array<std::byte, 4> bytes, CompletionToken&& token);

  public:
  std::filesystem::path name;
  fd_type file;
  std::uint_fast32_t version;
  std::uint_fast64_t sequence;

  private:
  allocator_type alloc_;
  typename fd_type::offset_type write_offset_;
  typename fd_type::offset_type link_offset_;
  asio::strand<executor_type> strand_;
  wal_file_entry_state state_ = wal_file_entry_state::uninitialized;
  link_done_event_type link_done_event_;
  wal_flusher<fd_type&, allocator_type> wal_flusher_;

  // Seal barrier becomes ready when:
  // 1. all pending writes have their space allocated.
  // 2. a seal has been requested.
  fanout_barrier<executor_type, allocator_type> seal_barrier_;
};


} /* namespace earnest::detail */

#include "wal_file_entry.ii"
