#pragma once

#include <algorithm>
#include <concepts>
#include <filesystem>
#include <memory>
#include <ranges>
#include <scoped_allocator>
#include <tuple>
#include <type_traits>

#include <asio/buffer.hpp>
#include <asio/strand.hpp>

#include <earnest/detail/fanout.h>
#include <earnest/detail/fanout_barrier.h>
#include <earnest/detail/move_only_function.h>
#include <earnest/detail/overload.h>
#include <earnest/detail/positional_stream_adapter.h>
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

  struct unwritten_link {
    unwritten_link(executor_type ex, std::uint64_t offset, std::array<std::byte, 4> data, allocator_type alloc)
    : offset(offset),
      data(data),
      done(ex, alloc)
    {}

    std::uint64_t offset;
    std::array<std::byte, 4> data;
    fanout<executor_type, void(std::error_code), allocator_type> done;
  };
  using unwritten_link_vector = std::vector<unwritten_link, rebind_alloc<unwritten_link>>;

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
  auto async_append(Range&& records, CompletionToken&& token, TransactionValidator&& transaction_validator = no_transaction_validation(), move_only_function<void(records_vector)> on_successful_write_callback = move_only_function<void(records_vector)>(), bool delay_flush = false);

  template<typename CompletionToken, typename TransactionValidator = no_transaction_validation>
  auto async_append(write_records_vector records, CompletionToken&& token, TransactionValidator&& transaction_validator = no_transaction_validation(), move_only_function<void(records_vector)> on_successful_write_callback = move_only_function<void(records_vector)>(), bool delay_flush = false);

  private:
  template<typename OnSpaceAssigned, typename TransactionValidator>
  auto async_append_impl_(write_records_vector records, move_only_function<void(std::error_code)>&& completion_handler, OnSpaceAssigned&& on_space_assigned, TransactionValidator&& transaction_validator, move_only_function<void(records_vector)> on_successful_write_callback, bool delay_flush = false) -> void;

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
  class async_records_reader
  : private positional_stream_adapter<const fd_type&>
  {
    private:
    static constexpr auto begin_offset = std::remove_cvref_t<decltype(std::declval<wal_file_entry>().header_reader_())>::bytes.value();

    public:
    using offset_type = typename positional_stream_adapter<const fd_type&>::offset_type;
    using executor_type = typename positional_stream_adapter<const fd_type&>::executor_type;

    explicit async_records_reader(std::shared_ptr<const wal_file_entry> wf)
    : positional_stream_adapter<const fd_type&>(wf->file, begin_offset),
      wf_(std::move(wf))
    {}

    using positional_stream_adapter<const fd_type&>::position;
    using positional_stream_adapter<const fd_type&>::get_executor;
    using positional_stream_adapter<const fd_type&>::skip;

    template<typename MutableBufferSequence, typename CompletionToken>
    auto async_read_some(const MutableBufferSequence& buffers, CompletionToken&& token) {
      return asio::async_initiate<CompletionToken, void(std::error_code, std::size_t)>(
          [this](auto completion_handler, auto buffers) {
            asio::dispatch(
                completion_wrapper<void()>(
                    completion_handler_fun(std::move(completion_handler), wf_->get_executor(), wf_->strand_),
                    [this, buffers](auto handler) mutable -> void {
                      assert(wf_->strand_.running_in_this_thread());

                      auto next = std::upper_bound(wf_->unwritten_links_.cbegin(), wf_->unwritten_links_.cend(), this->position(),
                          overload(
                              [](offset_type x, const unwritten_link& y) {
                                return x < y.offset + y.data.size();
                              },
                              [](const unwritten_link& x, offset_type y) {
                                return x.offset + x.data.size() < y;
                              }));

                      if (next == wf_->unwritten_links_.cend()) {
                        this->positional_stream_adapter<const fd_type&>::async_read_some(std::move(buffers), std::move(handler).inner_handler());
                        return;
                      }

                      assert(next->offset + next->data.size() > this->position());
                      if (next->offset <= this->position()) {
                        asio::const_buffer source_buffer = asio::buffer(next->data) + (this->position() - next->offset);
                        auto sz = asio::buffer_copy(buffers, source_buffer);
                        this->skip(sz);
                        std::invoke(std::move(handler), std::error_code{}, sz);
                        return;
                      }

                      auto next_avail = next->offset - this->position();
                      auto buf_end_iter = buffers.begin();
                      while (next_avail > 0 && buf_end_iter != buffers.end()) {
                        *buf_end_iter = asio::buffer(*buf_end_iter, next_avail);
                        next_avail -= buf_end_iter->size();
                        ++buf_end_iter;
                      }
                      buffers.erase(buf_end_iter, buffers.end());
                      assert(buffer_size(buffers) <= next_avail);
                      this->positional_stream_adapter<const fd_type&>::async_read_some(std::move(buffers), std::move(handler).inner_handler());
                    }));
          },
          token, std::vector<asio::mutable_buffer>(asio::buffer_sequence_begin(buffers), asio::buffer_sequence_end(buffers)));
    }

    private:
    std::shared_ptr<const wal_file_entry> wf_;
  };

  auto async_records_impl_(move_only_function<std::error_code(variant_type)> acceptor, move_only_function<void(std::error_code)> completion_handler) const;

  template<typename CompletionToken, typename OnSpaceAssigned, typename TransactionValidator>
  auto append_bytes_(std::vector<std::byte, rebind_alloc<std::byte>>&& bytes, CompletionToken&& token, OnSpaceAssigned&& space_assigned_event, std::error_code ec, wal_file_entry_state expected_state, TransactionValidator&& transaction_validator, move_only_function<void(records_vector)> on_successful_write_callback, write_record_with_offset_vector records, bool delay_flush = false);

  template<typename CompletionToken, typename Barrier, typename Fanout, typename DeferredTransactionValidator>
  auto append_bytes_at_(
      typename fd_type::offset_type write_offset,
      std::vector<std::byte, rebind_alloc<std::byte>>&& bytes,
      CompletionToken&& token,
      Barrier&& barrier, Fanout&& f,
      DeferredTransactionValidator&& deferred_transaction_validator,
      std::shared_ptr<records_vector> converted_records,
      bool delay_flush = false);

  template<typename CompletionToken>
  auto write_skip_record_(
      typename fd_type::offset_type write_offset,
      std::size_t bytes, CompletionToken&& token);

  template<typename CompletionToken>
  auto write_link_(
      typename fd_type::offset_type write_offset,
      std::array<std::byte, 4> bytes, CompletionToken&& token,
      bool delay_flush = false);

  public:
  std::filesystem::path name;
  fd_type file;
  std::uint_fast32_t version;
  std::uint_fast64_t sequence;

  private:
  allocator_type alloc_;
  typename fd_type::offset_type write_offset_;
  typename fd_type::offset_type link_offset_;
  typename fd_type::offset_type must_flush_offset_ = 0;
  asio::strand<executor_type> strand_;
  wal_file_entry_state state_ = wal_file_entry_state::uninitialized;
  link_done_event_type link_done_event_;
  wal_flusher<fd_type&, allocator_type> wal_flusher_;
  unwritten_link_vector unwritten_links_;

  // Seal barrier becomes ready when:
  // 1. all pending writes have their space allocated.
  // 2. a seal has been requested.
  fanout_barrier<executor_type, allocator_type> seal_barrier_;
};


} /* namespace earnest::detail */

#include "wal_file_entry.ii"
