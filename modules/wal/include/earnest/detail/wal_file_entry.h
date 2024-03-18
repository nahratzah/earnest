#pragma once

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <optional>
#include <ranges>
#include <scoped_allocator>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <vector>

#include <gsl/gsl>
#include <spdlog/spdlog.h>

#include <earnest/detail/buffered_readstream_adapter.h>
#include <earnest/detail/byte_stream.h>
#include <earnest/detail/logger.h>
#include <earnest/detail/positional_stream_adapter.h>
#include <earnest/detail/prom_allocator.h>
#include <earnest/detail/wal_flusher.h>
#include <earnest/detail/wal_records.h>
#include <earnest/dir.h>
#include <earnest/execution.h>
#include <earnest/execution_util.h>
#include <earnest/fd.h>
#include <earnest/move_only_function.h>
#include <earnest/strand.h>
#include <earnest/wal_error.h>
#include <earnest/xdr_v2.h>

namespace earnest::detail {


enum class wal_file_entry_state {
  ready,
  sealing,
  sealed,
  failed = -1
};

inline auto operator<<(std::ostream& out, wal_file_entry_state state) -> std::ostream& {
  using namespace std::literals;

  switch (state) {
    default:
      out << "wal_file_entry_state{"sv << static_cast<std::underlying_type_t<wal_file_entry_state>>(state) << "}"sv;
      break;
    case wal_file_entry_state::ready:
      out << "wal_file_entry_state{ready}"sv;
      break;
    case wal_file_entry_state::sealing:
      out << "wal_file_entry_state{sealing}"sv;
      break;
    case wal_file_entry_state::sealed:
      out << "wal_file_entry_state{sealed}"sv;
      break;
    case wal_file_entry_state::failed:
      out << "wal_file_entry_state{failed}"sv;
      break;
  }
  return out;
}


struct no_transaction_validation {};

using wal_allocator_type = prom_allocator<std::allocator<std::byte>>;


// Buffers both records and their on-disk representation.
//
// The records vector has the offset attached.
class write_records_buffer {
  private:
  template<typename T>
  using allocator_for = std::allocator<T>;

  struct xdr_invocation_;

  public:
  using variant_type = wal_record_variant;
  using write_variant_type = record_write_type_t<variant_type>;
  using allocator_type = allocator_for<std::byte>;
  using byte_vector = std::vector<std::byte, allocator_for<std::byte>>;

  private:
  using byte_stream = byte_stream<allocator_for<std::byte>>;

  public:
  struct write_record_with_offset {
    write_variant_type record;
    std::uint64_t offset;
  };
  using write_record_with_offset_vector = std::vector<write_record_with_offset, allocator_for<write_record_with_offset>>;

  explicit write_records_buffer(allocator_type alloc = allocator_type())
  : bytes_(alloc),
    records_(alloc)
  {}

  explicit write_records_buffer(std::initializer_list<write_variant_type> il, allocator_type alloc = allocator_type())
  : write_records_buffer(std::move(alloc))
  {
    std::for_each(il.begin(), il.end(),
        [this](write_variant_type v) {
          this->push_back(v);
        });
  }

  auto get_allocator() const -> allocator_type {
    return bytes_.get_allocator();
  }

  template<typename Record>
  auto push_back(const Record& v) -> void;
  template<std::ranges::input_range Range>
  auto push_back_range(Range&& range) -> void;

  auto empty() const noexcept -> bool {
    return records_.empty();
  }

  auto bytes() const & noexcept -> const byte_vector& {
    return bytes_.data();
  }

  auto bytes() & noexcept -> byte_vector& {
    return bytes_.data();
  }

  auto bytes() && noexcept -> byte_vector&& {
    return std::move(bytes_).data();
  }

  auto write_records_with_offset() const & noexcept -> const write_record_with_offset_vector& {
    return records_;
  }

  auto write_records_with_offset() & noexcept -> write_record_with_offset_vector& {
    return records_;
  }

  auto write_records_with_offset() && noexcept -> write_record_with_offset_vector&& {
    return std::move(records_);
  }

  auto converted_records(gsl::not_null<std::shared_ptr<execution::io::type_erased_at_readable>> fd, execution::io::offset_type offset) const
  -> std::vector<variant_type, allocator_for<variant_type>> {
    std::vector<variant_type, allocator_for<variant_type>> converted_records(get_allocator());
    converted_records.reserve(write_records_with_offset().size());

    std::ranges::copy(
        write_records_with_offset()
        | std::ranges::views::transform(
            [&](const auto& r) {
              wal_record_write_to_read_converter<variant_type> convert(fd, offset + r.offset);
              return convert(r.record);
            }),
        std::back_inserter(converted_records));

    return converted_records;
  }

  auto clear() {
    bytes_.clear();
    records_.clear();
  }

  private:
  byte_stream bytes_;
  write_record_with_offset_vector records_;
};


// Write record operation for the write-records-buffer.
//
// For each record that is written, it records the offset and the converted record.
struct write_records_buffer::xdr_invocation_ {
  explicit xdr_invocation_(write_record_with_offset_vector& records)
  : wrwo_vec(records)
  {}

  auto write(const write_variant_type& r) const {
    return xdr_v2::manual.write(
        [&r, self=*this](auto& stream) {
          self.update_wrwo_vec(stream, r);
          return execution::just(std::move(stream));
        })
    | xdr_v2::identity.write(r);
  }

  template<typename Record>
  requires requires {
    { std::bool_constant<(Record::opcode, true)>{} }; // Must have an opcode.
  }
  auto write(const Record& r) const {
    return xdr_v2::manual.write(
        [&r, self=*this](auto& stream) {
          self.update_wrwo_vec(stream, r);
          return execution::just(std::move(stream));
        })
    | xdr_v2::constant.write(Record::opcode, xdr_v2::uint32)
    | xdr_v2::identity.write(r);
  }

  private:
  auto update_wrwo_vec(const byte_stream& stream, const write_variant_type& r) const -> void {
    wrwo_vec.push_back({
          .record=r,
          .offset=stream.data().size(),
        });
  }

  write_record_with_offset_vector& wrwo_vec;
};

template<typename Record>
inline auto write_records_buffer::push_back(const Record& v) -> void {
  execution::sync_wait(
      xdr_v2::write(std::ref(bytes_), v, xdr_invocation_(records_))
      | execution::io::ec_to_exception());
}

template<std::ranges::input_range Range>
inline auto write_records_buffer::push_back_range(Range&& range) -> void {
  execution::sync_wait(
      xdr_v2::write(std::ref(bytes_), std::forward<Range>(range), xdr_v2::fixed_collection(xdr_invocation_(records_)))
      | execution::io::ec_to_exception());
}


static auto make_link_sender(execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>> input) -> execution::sender_of<> auto {
  using namespace execution;

  // We _must_ start the chain:
  // it'll contain shared-pointer to this.
  // And the link-sender will be installed on this.
  // So in order to ensure the links don't start keeping this alive, we must immediately start the operation, and chain-break it.
  return split(ensure_started(std::move(input) | chain_breaker()));
}

using link_sender = decltype(make_link_sender(std::declval<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>>()));


// A single WAL file.
//
// This class manages the read and write operations.
// - any read is atomic
// - any write is append-only and atomic
//
// The invariant is that at the end of the file, holds a wal_record_end_of_records.
// Note that the end-of-file is tracked with end_offset, not with the file-size.
class wal_file_entry_file {
  public:
  using allocator_type = wal_allocator_type;
  using offset_type = execution::io::offset_type;

  using callback = earnest::move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>, true>()>;

  private:
  template<typename T> using allocator_for = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;
  using fd_type = fd;
  using commit_space_late_binding_t = execution::late_binding_t<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>;

  public:
  class prepared_space;

  wal_file_entry_file(
      fd_type&& file, offset_type end_offset,
      link_sender initial_link,
      allocator_type alloc = allocator_type())
  : file(std::move(file)),
    end_offset(end_offset),
    sync_offset(end_offset - 4u),
    link_offset(end_offset - 4u),
    wal_flusher_(this->file, alloc),
    link(std::move(initial_link)),
    alloc_(alloc)
  {
    if (end_offset < 4) throw std::range_error("bug: wal file end-offset too low");
  }

  auto close() -> void {
    file.close();
  }

  auto is_open() const noexcept -> bool {
    return file.is_open();
  }

  auto get_end_offset() const noexcept -> offset_type { return end_offset; }
  auto get_link_offset() const noexcept -> offset_type { return link_offset; }
  auto get_allocator() const -> allocator_type { return alloc_; }

  private:
  template<typename T>
  auto recovery_record_bytes_(T record) -> std::vector<std::byte, allocator_for<std::byte>> {
    auto [stream] = execution::sync_wait(
        xdr_v2::write(
            byte_stream<allocator_type>(get_allocator()),
            std::make_tuple(T::opcode, record),
            xdr_v2::constant(xdr_v2::tuple(xdr_v2::uint32, xdr_v2::identity)))
    ).value();
    return std::move(stream).data();
  }

  // XXX It would be nice to only encode the actual data, but not the skipped data.
  // That way, the vector would be at most 12 bytes,
  // and we could go find a short-string-optimization style vector.
  auto recovery_record_(std::size_t write_len) -> std::vector<std::byte, allocator_for<std::byte>> {
    assert(write_len >= 4u);
    if (write_len == 4u) {
      return recovery_record_bytes_(wal_record_noop{});
    } else if (write_len - 8u <= 0xffff'ffffu) {
      assert(write_len >= 8u);
      return recovery_record_bytes_(wal_record_skip32{static_cast<std::uint32_t>(write_len - 8u)});
    } else {
      assert(write_len - 12u <= 0xffff'ffff'ffff'ffffull);
      assert(write_len >= 12u);
      return recovery_record_bytes_(wal_record_skip64{static_cast<std::uint64_t>(write_len - 12u)});
    }
  }

  public:
  // Reserve space for a write operation.
  //
  // When this sender completes, the space allocated will be used.
  // If the prepared-space isn't written to (or the write fails),
  // a skip record will be placed.
  //
  // Note that, when this function returns, the space allocated will be used.
  // If the acceptor isn't completed successfully, a recovery-operation will write
  // a skip record to skip the space when reading it back.
  static auto prepare_space(gsl::not_null<std::shared_ptr<wal_file_entry_file>> self, execution::strand<allocator_type> strand, std::size_t write_len) -> prepared_space;

  // Discard all data in the file.
  // Note that this is not an atomic operation.
  static auto discard_all(gsl::not_null<std::shared_ptr<wal_file_entry_file>> self, execution::strand<allocator_type> strand) -> execution::sender_of<> auto;

  static auto read_some_at(gsl::not_null<std::shared_ptr<const wal_file_entry_file>> self, offset_type offset, std::span<std::byte> buf) -> execution::sender_of<std::size_t> auto {
    using namespace execution;

    return just(self, offset, buf)
    | lazy_validation(
        [](gsl::not_null<std::shared_ptr<const wal_file_entry_file>> self, offset_type offset, std::span<std::byte> buf) -> std::optional<std::error_code> {
          if (offset >= self->link_offset && !buf.empty())
            return make_error_code(execution::io::errc::eof);
          else
            return std::nullopt;
        })
    | lazy_let_value(
        [](gsl::not_null<std::shared_ptr<const wal_file_entry_file>> self, offset_type offset, std::span<std::byte> buf) {
          if (buf.size() > self->end_offset - offset) buf = buf.subspan(0, self->end_offset - offset);
          return io::read_some_at_ec(self->file, offset, buf);
        });
  }

  auto file_ref() const & -> const fd_type& {
    return file;
  }

  auto file_ref() & -> fd_type& {
    return file;
  }

  private:
  auto delay_flush(offset_type write_offset, bool delay_flush_requested) const noexcept -> bool {
    return delay_flush_requested && (write_offset > sync_offset);
  }

  fd_type file;
  offset_type end_offset; // End of the file. New appends happen at end_offset-4.
  offset_type sync_offset; // Highest write-offset for which a sync is requested.
  offset_type link_offset; // The end of fully-written data.
  wal_flusher<fd_type&, allocator_type> wal_flusher_;
  link_sender link;
  [[no_unique_address]] allocator_type alloc_;
};


class wal_file_entry_unwritten_data {
  public:
  using allocator_type = wal_allocator_type;
  using offset_type = execution::io::offset_type;

  private:
  template<typename T> using allocator_for = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;
  using byte_span = std::span<const std::byte>;
  using byte_vector = std::vector<std::byte, allocator_for<std::byte>>;

  struct record {
    using allocator_type = wal_file_entry_unwritten_data::allocator_type;

    record() = default;

    explicit record(allocator_type alloc)
    : bytes(alloc)
    {}

    // Create a new record.
    // Bytes are copied.
    record(offset_type offset, std::span<const std::byte> bytes, allocator_type alloc)
    : offset(offset),
      bytes(bytes.begin(), bytes.end(), alloc)
    {}

    record(record&& other, allocator_type alloc)
    : offset(other.offset),
      bytes(std::move(other.bytes), alloc)
    {
      assert(other.bytes.get_allocator() == alloc); // Required, so that the move call will use a move-constructor.
    }

    // Moveable only, because we require that the byte_vector data remains at a constant memory address.
    record(const record&) = delete;
    record(record&&) = default;
    record& operator=(const record&) = delete;
    record& operator=(record&&) = default;

    auto end_offset() const noexcept -> offset_type {
      return offset + bytes.size();
    }

    auto get_allocator() const -> allocator_type {
      return bytes.get_allocator();
    }

    offset_type offset;
    byte_vector bytes;
  };
  static_assert(std::uses_allocator_v<record, std::scoped_allocator_adaptor<allocator_for<record>>>);
  using record_vector = std::vector<record, std::scoped_allocator_adaptor<allocator_for<record>>>;

  // Less-compare that checks offsets.
  struct offset_compare {
    auto operator()(offset_type x, offset_type y) const noexcept -> bool {
      return x < y;
    }

    auto operator()(const record& x, const record& y) const noexcept -> bool {
      return (*this)(x.offset, y.offset);
    }

    auto operator()(offset_type x, const record& y) const noexcept -> bool {
      return (*this)(x, y.offset);
    }

    auto operator()(const record& x, offset_type y) const noexcept -> bool {
      return (*this)(x.offset, y);
    }
  };

  // End-offset-compare.
  // Compares an offset to the end of a record.
  struct search_compare {
    auto operator()(offset_type x, const record& y) const noexcept -> bool {
      return x < y.end_offset();
    }

    auto operator()(const record& x, offset_type y) const noexcept -> bool {
      return x.end_offset() <= y;
    }
  };

  public:
  explicit wal_file_entry_unwritten_data(allocator_type alloc = allocator_type())
  : records(alloc)
  {
    assert(invariant());
  }

  auto empty() const noexcept -> bool {
    return records.empty();
  }

  // Add bytes to the unwritten-data.
  // Bytes are copied.
  auto add(offset_type offset, std::span<const std::byte> bytes) -> byte_span {
    if (bytes.empty()) throw std::range_error("no data to record");

    record_vector::const_iterator insert_position;
    if (records.empty() || records.back().offset < offset)
      insert_position = records.end();
    else
      insert_position = std::upper_bound(records.cbegin(), records.cend(), offset, offset_compare{});

    // Check that we don't overlap our ranges.
    if (insert_position != records.begin()) {
      const record& prev = *std::prev(insert_position);
      if (!(prev.end_offset() <= offset))
        throw std::range_error("bug: data overlaps with preceding record");
    }
    if (insert_position != records.end() && !(offset + bytes.size() <= insert_position->offset))
      throw std::range_error("bug: data overlaps with successive record");

    record_vector::const_iterator inserted = records.emplace(insert_position, offset, bytes);
    assert(invariant());
    return inserted->bytes;
  }

  void remove(offset_type offset, byte_span bytes) {
    typename record_vector::const_iterator erase_position;
    if (!records.empty() && records.front().offset == offset) {
      erase_position = records.cbegin();
    } else {
      erase_position = std::lower_bound(records.cbegin(), records.cend(), offset, offset_compare{});
      if (erase_position->offset != offset) erase_position = records.cend();
    }

    // Check that this is indeed the record to be erased.
    if (erase_position == records.cend())
      throw std::range_error("record not found");
    if (erase_position->bytes.data() != bytes.data() || erase_position->bytes.size() != bytes.size())
      throw std::range_error("record data mismatch");

    records.erase(erase_position);
    assert(invariant());
  }

  // Returns a sender for a read operation.
  // Note: this function is non-lazy.
  //
  // - offset: the offset at which the read is to happen
  // - buf: the buffer into which the read data is to be copied
  // - overflow_read: a read function that'll be invoked, if there's no data available at the offset.
  //   Must return a sender_of<std::size_t>.
  template<std::invocable<offset_type, std::span<std::byte>> OverflowFn>
  auto read_some_at(offset_type offset, std::span<std::byte> buf, OverflowFn&& overflow_read) const -> execution::sender_of<std::size_t> auto {
    // Figure out the return types used.
    using self_type = decltype(execution::just(std::declval<std::size_t>()));
    using overflow_type = std::invoke_result_t<OverflowFn, offset_type, std::span<std::byte>>;
    constexpr bool same_types = std::is_same_v<self_type, overflow_type>;
    // Figure out the variant type.
    using variant_type = std::conditional_t<same_types, self_type, std::variant<self_type, overflow_type>>;

    // A small wrapper that'll turn an lvalue-reference into an rvalue-reference.
    auto move_variant_type = [](variant_type& v) -> variant_type&& { return std::move(v); };
    // A small wrapper that wraps the return-sender into a variant-sender.
    auto make_return_value = [move_variant_type](auto return_sender) -> execution::sender_of<std::size_t> auto {
      if constexpr(same_types) {
        return return_sender;
      } else {
        return execution::just(variant_type(std::move(return_sender)))
        | execution::let_variant(move_variant_type);
      }
    };

    // Find the element that ends after the offset.
    typename record_vector::const_iterator elem = std::upper_bound(records.cbegin(), records.cend(), offset, search_compare{});
    if (elem == records.end()) {
      // There are no more records at/after the offset.
      // So we forward all calls to the overflow reader.
      return make_return_value(std::invoke(std::forward<OverflowFn>(overflow_read), offset, buf));
    } else if (elem->offset <= offset) {
      // We have local data for the given offset.
      // So we'll handle it locally.
      assert(offset < elem->end_offset());

      // Clamp the read-range.
      std::span<std::byte> destination = buf;
      if (destination.size() > elem->end_offset() - offset)
        destination = destination.subspan(0, elem->end_offset() - offset);
      // Figure out the source range.
      // We make sure it's equal-length to the destination-range.
      auto origin_span = std::span<const std::byte>(elem->bytes).subspan(offset - elem->offset, destination.size());
      // Now copy the bytes.
      assert(origin_span.size() == destination.size());
      std::copy(origin_span.begin(), origin_span.end(), destination.begin());
      // Record that we managed to read data.
      return make_return_value(execution::just(destination.size()));
    } else {
      // We have an element after the read.
      // So we'll have to pass it on to the overflow read,
      // but we want to make sure the overflow read won't read
      // the data we are to provide.
      assert(offset < elem->offset);

      // Clamp the buf...
      if (buf.size() > elem->offset - offset)
        buf = buf.subspan(elem->offset - offset);
      // ... and then we let the overflow deal with it.
      return make_return_value(std::invoke(std::forward<OverflowFn>(overflow_read), offset, buf));
    }
  }

  private:
  auto invariant() const noexcept -> bool {
    // Confirm that none of the ranges is empty.
    if (std::any_of(records.cbegin(), records.cend(),
            [](const record& r) noexcept -> bool {
              return r.bytes.empty();
            }))
      return false;

    // Confirm the range is sorted.
    if (!std::is_sorted(records.cbegin(), records.cend(), offset_compare{}))
      return false;

    // Confirm that no two successive elements overlap.
    offset_type preceding_end_offset = 0;
    for (const record& r : records) {
      if (r.offset < preceding_end_offset)
        return false;
      preceding_end_offset = r.offset + r.bytes.size();
    }

    // Confirm none of the offset+bytes.size() overflows.
    for (const record& r : records) {
      if (std::numeric_limits<offset_type>::max() - r.offset < r.bytes.size())
        return false;
    }

    return true;
  }

  // All the records for unwritten data.
  // They're kept in ascending order.
  record_vector records;
};


class wal_file_entry
: public std::enable_shared_from_this<wal_file_entry>,
  with_logger
{
  friend wal_file_entry_file; // Needs access to xdr_header_tuple and related types.

  public:
  static inline constexpr std::uint_fast32_t max_version = 0;
  static inline constexpr std::size_t read_buffer_size = 2u * 1024u * 1024u;
  using allocator_type = wal_allocator_type;
  using link_done_event_type = execution::late_binding_t<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>::acceptor;
  using write_records_buffer_t = write_records_buffer;

  private:
  using link_done_event_sender = execution::late_binding_t<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>::sender;

  template<typename T>
  using rebind_alloc = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;

  using xdr_header_tuple = std::tuple<std::uint32_t /*version*/, std::uint64_t /*sequence*/>;
  struct xdr_header_invocation;

  public:
  using variant_type = wal_record_variant;
  using write_variant_type = record_write_type_t<variant_type>;
  using records_vector = std::vector<variant_type>;
  using write_records_vector = std::vector<write_variant_type>;
  using fd_type = fd;

  private:
  struct write_record_with_offset {
    write_variant_type record;
    std::uint64_t offset;
  };
  using write_record_with_offset_vector = std::vector<write_record_with_offset, std::scoped_allocator_adaptor<rebind_alloc<write_record_with_offset>>>;

  struct unwritten_data {
    using allocator_type = rebind_alloc<std::byte>;

    template<std::ranges::range Bytes>
    requires std::convertible_to<std::ranges::range_reference_t<Bytes>, std::byte>
    unwritten_data(std::uint64_t offset, Bytes&& data, allocator_type alloc)
    : offset(offset),
      data(std::ranges::begin(data), std::ranges::end(data), std::move(alloc))
    {
      if (this->data.size() < 4u)
        throw std::range_error("bug: unwritten_data too short");
      if (this->data.size() % 4u)
        throw std::range_error("bug: unwritten_data length should be a multiple of 4"); // XDR format always uses multiples of 4-byte
    }

    std::uint64_t offset;
    std::vector<std::byte, allocator_type> data;
  };
  using unwritten_data_vector = std::vector<unwritten_data, rebind_alloc<unwritten_data>>;

  public:
  wal_file_entry(fd_type fd, std::filesystem::path name, xdr_header_tuple hdr, execution::io::offset_type end_offset, link_sender link, bool sealed, gsl::not_null<std::shared_ptr<spdlog::logger>> logger, allocator_type alloc)
  : with_logger(logger),
    name(name),
    version(std::get<0>(hdr)),
    sequence(std::get<1>(hdr)),
    alloc_(alloc),
    state_(sealed ? wal_file_entry_state::sealed : wal_file_entry_state::ready),
    file(std::move(fd), end_offset, link, alloc),
    unwritten_data(alloc),
    fake_link(link),
    strand_(alloc)
  {}

  wal_file_entry(const wal_file_entry&) = delete;
  wal_file_entry(wal_file_entry&&) = delete;
  wal_file_entry& operator=(const wal_file_entry&) = delete;
  wal_file_entry& operator=(wal_file_entry&&) = delete;

  auto get_allocator() const -> allocator_type { return alloc_; }

  auto close() -> void {
    std::lock_guard lck{strand_};
    file.close();
  }

  auto state() const noexcept -> wal_file_entry_state {
    std::lock_guard lck{strand_};
    return state_;
  }

  auto is_open() const noexcept -> bool {
    std::lock_guard lck{strand_};
    return file.is_open();
  }

  [[nodiscard]]
  static auto open(dir d, std::filesystem::path name, allocator_type alloc)
  -> execution::type_erased_sender<
      std::variant<std::tuple<gsl::not_null<std::shared_ptr<wal_file_entry>>>>,
      std::variant<std::exception_ptr, std::error_code>,
      false>;

  [[nodiscard]]
  static auto create(dir d, const std::filesystem::path& name, std::uint_fast64_t sequence, allocator_type alloc)
  -> execution::type_erased_sender<
      std::variant<std::tuple<
          gsl::not_null<std::shared_ptr<wal_file_entry>>,
          execution::late_binding_t<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>::acceptor>>,
      std::variant<std::exception_ptr, std::error_code>,
      false>;

  [[nodiscard]]
  static auto open(dir d, std::filesystem::path name)
  -> execution::type_erased_sender<
      std::variant<std::tuple<gsl::not_null<std::shared_ptr<wal_file_entry>>>>,
      std::variant<std::exception_ptr, std::error_code>,
      false> {
    return open(std::move(d), name, allocator_type(nullptr, "", prometheus::Labels{}));
  }

  [[nodiscard]]
  static auto create(dir d, const std::filesystem::path& name, std::uint_fast64_t sequence)
  -> execution::type_erased_sender<
      std::variant<std::tuple<
          gsl::not_null<std::shared_ptr<wal_file_entry>>,
          execution::late_binding_t<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>::acceptor>>,
      std::variant<std::exception_ptr, std::error_code>,
      false> {
    return create(std::move(d), name, sequence, allocator_type(nullptr, "", prometheus::Labels{}));
  }

  private:
  // We only implement this in the .cc file, because that's the only place it's used.
  // Saves some parsing for files including this.
  static auto durable_append_(
      gsl::not_null<std::shared_ptr<wal_file_entry>> wf,
      write_records_buffer_t&& records,
      typename wal_file_entry_file::callback transaction_validation,
      earnest::move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>, false>(records_vector)> on_successful_write_callback,
      wal_file_entry_file::prepared_space&& space)
  -> execution::sender_of<> auto;

  // We only implement this in the .cc file, because that's the only place it's used.
  // Saves some parsing for files including this.
  static auto non_durable_append_(
      gsl::not_null<std::shared_ptr<wal_file_entry>> wf,
      write_records_buffer_t&& records,
      typename wal_file_entry_file::callback transaction_validation,
      earnest::move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>, false>(records_vector)> on_successful_write_callback,
      wal_file_entry_file::prepared_space&& space)
  -> execution::sender_of<> auto;

  public:
  class append_operation {
    public:
    explicit append_operation(
        execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>> sender_impl,
        execution::io::offset_type file_size)
    : sender_impl(std::move(sender_impl)),
      file_size(file_size)
    {}

    append_operation(const append_operation&) = delete;
    append_operation(append_operation&&) noexcept = default;

    auto operator()() && -> execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>&& {
      return std::move(sender_impl);
    }

    private:
    execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>> sender_impl;

    public:
    execution::io::offset_type file_size;
  };

  [[nodiscard]]
  auto append(
      write_records_buffer_t records,
      typename wal_file_entry_file::callback transaction_validation = nullptr,
      earnest::move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>, false>(records_vector)> on_successful_write_callback = nullptr,
      bool delay_flush = false)
  -> execution::type_erased_sender<std::variant<std::tuple<append_operation>>, std::variant<std::exception_ptr, std::error_code>>;

  [[nodiscard]]
  auto seal()
  -> execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>;

  [[nodiscard]]
  auto discard_all(write_records_buffer_t instead = write_records_buffer_t{}, bool seal = true)
  -> execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>;

  auto end_offset() const noexcept {
    std::lock_guard lck{strand_};
    return file.get_end_offset();
  }

  auto link_offset() const noexcept {
    std::lock_guard lck{strand_};
    return file.get_link_offset();
  }

  [[nodiscard]]
  auto records(move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>(variant_type)> acceptor) const
  -> execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>;

  [[nodiscard]]
  auto records() const -> execution::type_erased_sender<std::variant<std::tuple<records_vector>>, std::variant<std::exception_ptr, std::error_code>>;

  private:
  // Adapter class.
  // Provides a readable interface for the wal-file-entry.
  class reader_impl_ {
    public:
    explicit reader_impl_(gsl::not_null<std::shared_ptr<const wal_file_entry>> wf)
    : wf(wf)
    {}

    friend auto tag_invoke([[maybe_unused]] execution::io::lazy_read_some_at_ec_t tag, const reader_impl_& self, execution::io::offset_type offset, std::span<std::byte> buffer) -> execution::sender_of<std::size_t> auto {
      using namespace execution;
      using namespace execution::io;

      return just(self.wf, offset, buffer)
      | lazy_let_value(
          [](gsl::not_null<std::shared_ptr<const wal_file_entry>> wf, offset_type offset, std::span<std::byte> buffer) {
            assert(wf->strand_.running_in_this_thread());

            auto wff = std::shared_ptr<const wal_file_entry_file>(wf.get(), &wf->file);
            return wf->unwritten_data.read_some_at(
                offset, buffer,
                [wff](offset_type offset, std::span<std::byte> buffer) {
                  return wal_file_entry_file::read_some_at(wff, offset, buffer);
                });
          });
    }

    auto get_strand() { return wf->strand_; }

    private:
    gsl::not_null<std::shared_ptr<const wal_file_entry>> wf;
  };

  // Reader that we pass to records.
  // It ensures the read starts on the strand, to ensure the file descriptor won't be invalidated while the call is set up.
  class record_reader {
    public:
    explicit record_reader(gsl::not_null<std::shared_ptr<const wal_file_entry>> wf)
    : impl(wf),
      strand(wf->strand_)
    {}

    template<execution::io::mutable_buffers Buffer>
    friend auto tag_invoke(execution::io::lazy_read_some_at_ec_t tag, const record_reader& self, execution::io::offset_type offset, Buffer&& buffer) {
      return execution::on(self.strand, tag(self.impl, offset, std::forward<Buffer>(buffer)));
    }

    template<execution::io::mutable_buffers Buffer>
    friend auto tag_invoke(execution::io::lazy_read_at_ec_t tag, const record_reader& self, execution::io::offset_type offset, Buffer&& buffer, std::optional<std::size_t> minbytes) {
      return execution::on(self.strand, tag(self.impl, offset, std::forward<Buffer>(buffer), minbytes));
    }

    private:
    reader_impl_ impl;
    execution::strand<allocator_type> strand;
  };

  template<typename StreamAllocator>
  class async_records_reader
  : public buffered_readstream_adapter<positional_stream_adapter<reader_impl_>, StreamAllocator>
  {
    private:
    using parent_type = buffered_readstream_adapter<positional_stream_adapter<reader_impl_>, StreamAllocator>;

    public:
    explicit async_records_reader(gsl::not_null<std::shared_ptr<const wal_file_entry>> wf)
    : parent_type(typename parent_type::next_layer_type(reader_impl_(wf)))
    {}

    explicit async_records_reader(gsl::not_null<std::shared_ptr<const wal_file_entry>> wf, StreamAllocator alloc)
    : parent_type(typename parent_type::next_layer_type(reader_impl_(wf)), alloc)
    {}
  };

  public:
  const std::filesystem::path name;
  const std::uint_fast32_t version;
  const std::uint_fast64_t sequence;

  private:
  [[no_unique_address]] allocator_type alloc_;
  wal_file_entry_state state_ = wal_file_entry_state::failed; // Must be initialized.
  wal_file_entry_file file;
  wal_file_entry_unwritten_data unwritten_data;
  link_sender fake_link;
  mutable execution::strand<allocator_type> strand_;

  // Wal-file needs some data.
  // In order to not require two allocations, we embed it here.
  // But we only provide it, we don't manage it.
  struct wal_file_data_ {
    std::size_t locks = 0; // Number of transactions depending on this file.
    std::size_t read_locks = 0; // Number of transactions depending on being able to read this file.
    bool rollover_running = false; // If set, a rollover task is running on this file.
    bool post_processing_done = false; // If set, the file has had its post-processing done.

    // Successor data.
    // Used half-way a rollover operation.
    std::optional<
        std::tuple<
            gsl::not_null<std::shared_ptr<wal_file_entry>>,
            execution::late_binding_t<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>::acceptor
            >
        > successor;
  };

  public:
  wal_file_data_ wal_file_data;
};

struct wal_file_entry::xdr_header_invocation {
  private:
  static constexpr auto magic() noexcept -> std::array<char, 13> {
    return std::array<char, 13>{ '\013', '\013', 'e', 'a', 'r', 'n', 'e', 's', 't', '.', 'w', 'a', 'l' };
  }

  public:
  auto read(xdr_header_tuple& tpl) const {
    return xdr_v2::constant.read(magic(), xdr_v2::fixed_byte_string)
    | xdr_v2::uint32.read(std::get<0>(tpl))
    | xdr_v2::validation.read(
        [&tpl]() -> std::optional<std::error_code> {
          if (std::get<0>(tpl) <= max_version)
            return std::nullopt;
          else
            return make_error_code(wal_errc::bad_version);
        })
    | xdr_v2::uint64.read(std::get<1>(tpl));
  }

  auto write(const xdr_header_tuple& tpl) const {
    return xdr_v2::constant.write(magic(), xdr_v2::fixed_byte_string)
    | xdr_v2::validation.write(
        [&tpl]() -> std::optional<std::error_code> {
          if (std::get<0>(tpl) <= max_version)
            return std::nullopt;
          else
            return make_error_code(wal_errc::bad_version);
        })
    | xdr_v2::uint32.write(std::get<0>(tpl))
    | xdr_v2::uint64.write(std::get<1>(tpl));
  }
};


} /* namespace earnest::detail */

namespace fmt {


template<>
struct formatter<earnest::detail::wal_file_entry_state>
: formatter<std::string>
{
  auto format(earnest::detail::wal_file_entry_state state, format_context& ctx) -> decltype(ctx.out()) {
    using namespace std::literals;

    switch (state) {
      default:
        return fmt::format_to(ctx.out(), "wal_file_entry_state({})", static_cast<std::underlying_type_t<earnest::detail::wal_file_entry_state>>(state));
      case earnest::detail::wal_file_entry_state::ready:
        return fmt::format_to(ctx.out(), "ready");
      case earnest::detail::wal_file_entry_state::sealing:
        return fmt::format_to(ctx.out(), "sealing");
      case earnest::detail::wal_file_entry_state::sealed:
        return fmt::format_to(ctx.out(), "sealed");
      case earnest::detail::wal_file_entry_state::failed:
        return fmt::format_to(ctx.out(), "failed");
    }
  }
};


} /* namespace fmt */
