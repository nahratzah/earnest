#ifndef EARNEST_DETAIL_WAL2_H
#define EARNEST_DETAIL_WAL2_H

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <system_error>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <earnest/fd.h>

#include <earnest/detail/allocated_object.h>
#include <earnest/detail/buffered_read_stream_at.h>
#include <earnest/detail/completion_barrier.h>
#include <earnest/detail/replacement_map.h>

#include <asio/async_result.hpp>
#include <asio/strand.hpp>
#include <asio/associated_executor.hpp>
#include <asio/associated_allocator.hpp>

namespace earnest::detail {


enum class wal_errc {
  already_open,
  invalid,
  bad_opcode,
};

auto wal_error_category() -> const std::error_category&;
auto make_error_code(wal_errc e) noexcept -> std::error_code;


enum class wal_tx_errc {
  bad_tx,
  write_past_eof,
};

auto wal_tx_error_category() -> const std::error_category&;
auto make_error_code(wal_tx_errc e) noexcept -> std::error_code;


} /* namespace earnest::detail */

namespace std {


template<>
struct is_error_code_enum<earnest::detail::wal_errc>
: std::true_type
{};

template<>
struct is_error_code_enum<earnest::detail::wal_tx_errc>
: std::true_type
{};


} /* namespace std */

namespace earnest::detail {


enum class wal_entry : std::uint32_t {
  end = 0,
  noop = 1,
  skip = 2,
  checkpoint = 3,
  resize = 8,
  write = 9,
  write_many = 10,
  memset = 11
};


template<typename AsyncReadStream, typename CompletionToken>
auto async_wal_entry_read(AsyncReadStream& reader, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, wal_entry)>::return_type;


class wal_header_t {
  public:
  static constexpr std::size_t BYTES = 24;
  static constexpr std::array<char, 8> MAGIC = { 'e', 'a', 'r', 'n', '-', 'W', 'A', 'L' };

  std::array<char, 8> magic = MAGIC;
  std::uint64_t foff;
  std::uint64_t fsize;

  constexpr wal_header_t() noexcept;
  constexpr wal_header_t(std::uint64_t foff, std::uint64_t fsize) noexcept;

  template<typename AsyncReadStream, typename CompletionToken>
  static auto async_read(AsyncReadStream& reader, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, wal_header_t)>::return_type;
};


template<wal_entry> struct wal_record;

template<>
struct wal_record<wal_entry::end> {};
template<>
struct wal_record<wal_entry::noop> {};
template<>
struct wal_record<wal_entry::skip> {
  static constexpr std::size_t BYTES = 4;

  std::uint32_t nbytes;

  template<typename AsyncReadStream, typename CompletionToken>
  static auto async_read(AsyncReadStream& reader, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, wal_record)>::return_type;
};
template<>
struct wal_record<wal_entry::checkpoint> {};
template<>
struct wal_record<wal_entry::resize> {
  static constexpr std::size_t BYTES = 8;

  std::uint64_t size;

  template<typename AsyncReadStream, typename CompletionToken>
  static auto async_read(AsyncReadStream& reader, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, wal_record)>::return_type;
};
template<>
struct wal_record<wal_entry::write> {
  static constexpr std::size_t BYTES = 12;

  std::uint64_t offset;
  std::uint32_t nbytes;

  auto skip_bytes() const noexcept -> std::uint64_t {
    return (std::uint64_t(nbytes) + 3u) & ~std::uint64_t(0x0000'0003U);
  }

  template<typename AsyncReadStream, typename CompletionToken>
  static auto async_read(AsyncReadStream& reader, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, wal_record)>::return_type;
};
template<>
struct wal_record<wal_entry::write_many> {
  std::vector<std::uint64_t> offsets;
  std::uint32_t nbytes;

  auto skip_bytes() const noexcept -> std::uint64_t {
    return (std::uint64_t(nbytes) + 3u) & ~std::uint64_t(0x0000'0003U);
  }

  template<typename AsyncReadStream, typename CompletionToken>
  static auto async_read(AsyncReadStream& reader, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, wal_record)>::return_type;
};
template<>
struct wal_record<wal_entry::memset> {
  static constexpr std::size_t BYTES = 16;

  std::uint64_t offset;
  std::uint32_t fill_value_;
  std::uint32_t nbytes;

  auto fill_value() const -> std::uint8_t {
    return fill_value_ & 0xffU;
  }

  template<typename AsyncReadStream, typename CompletionToken>
  static auto async_read(AsyncReadStream& reader, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, wal_record)>::return_type;
};


template<wal_entry> class wal_tx_record;

template<>
class wal_tx_record<wal_entry::write> {
  public:
  static constexpr wal_entry entry_type = wal_entry::write;

  wal_tx_record() noexcept = default;
  wal_tx_record(std::uint64_t offset, std::shared_ptr<const void>(buf), std::size_t bufsiz);

  auto buffers() const noexcept -> std::array<asio::const_buffer, 5>;
  auto shared_data() const noexcept -> const std::shared_ptr<const void>& { return buf_; }
  auto shared_data() noexcept -> std::shared_ptr<const void>& { return buf_; }

  private:
  std::shared_ptr<const void> buf_;
  wal_record<wal_entry::write> data_; // Stored in big-endian.
};
template<>
class wal_tx_record<wal_entry::write_many> {
  public:
  static constexpr wal_entry entry_type = wal_entry::write_many;

  wal_tx_record() noexcept = default;
  template<typename OffsetIter>
  wal_tx_record(OffsetIter b, OffsetIter e, std::shared_ptr<const void>(buf), std::size_t bufsiz);

  auto buffers() const noexcept -> std::array<asio::const_buffer, 6>;
  auto shared_data() const noexcept -> const std::shared_ptr<const void>& { return buf_; }
  auto shared_data() noexcept -> std::shared_ptr<const void>& { return buf_; }

  private:
  std::shared_ptr<const void> buf_;
  wal_record<wal_entry::write_many> data_; // Stored in big-endian.
  std::uint32_t noffsets_; // Stored in big-endian.
};
template<>
class wal_tx_record<wal_entry::memset> {
  public:
  static constexpr wal_entry entry_type = wal_entry::memset;

  wal_tx_record() noexcept = default;
  wal_tx_record(std::uint64_t offset, std::uint32_t nbytes, std::uint8_t fill_value) noexcept;

  auto buffers() const noexcept -> std::array<asio::const_buffer, 2>;

  private:
  wal_record<wal_entry::memset> data_; // Stored in big-endian.
};
template<>
class wal_tx_record<wal_entry::resize> {
  public:
  static constexpr wal_entry entry_type = wal_entry::resize;

  wal_tx_record() noexcept = default;
  wal_tx_record(std::uint64_t newsize) noexcept;

  auto buffers() const noexcept -> std::array<asio::const_buffer, 2>;

  private:
  wal_record<wal_entry::resize> data_; // Stored in big-endian.
};


template<typename AsyncReadStream, typename Allocator>
class async_decode_offset_vector_ {
  public:
  using vector_type = std::vector<std::uint64_t>;
  using elem_type = std::tuple<std::uint32_t, vector_type>;
  using alloc_type = typename std::allocator_traits<Allocator>::template rebind_alloc<elem_type>;

  enum class state {
    init,
    expect_nelems,
    expect_offsets,
  };

  explicit async_decode_offset_vector_(AsyncReadStream& reader, alloc_type alloc);
  template<typename Self> void operator()(Self& self, std::error_code = {}, std::size_t bytes = 0);

  private:
  state state_ = state::init;
  AsyncReadStream& reader_;
  allocated_object<elem_type, alloc_type> elem_;
};

template<typename AsyncReadStream, typename CompletionToken>
auto async_decode_offset_vector(AsyncReadStream& reader, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, std::vector<std::uint64_t>)>::return_type;


template<typename Executor = fd<>::executor_type, typename Allocator = std::allocator<std::byte>>
class wal
: public std::enable_shared_from_this<wal<Executor, Allocator>>
{
  public:
  using allocator_type = typename std::allocator_traits<Allocator>::template rebind_alloc<std::byte>;
  using fd_type = ::earnest::fd<Executor>;
  using size_type = typename fd_type::size_type;
  using offset_type = typename fd_type::offset_type;
  using executor_type = typename fd_type::executor_type;

  template<typename TxAllocator = allocator_type> class tx;

  private:
  using strand_type = asio::strand<executor_type>;

  public:
  wal(const wal&) = delete; // Prevent copy/move construction/assignment.
  explicit wal(const executor_type& e, allocator_type alloc = allocator_type());
  wal(fd_type&& fd, offset_type off, allocator_type alloc = allocator_type());
  wal(fd_type&& fd, offset_type off, std::error_code& ec, allocator_type alloc = allocator_type());
  ~wal();

  auto get_executor() const -> executor_type;
  auto get_allocator() const -> allocator_type;

  [[deprecated]]
  void open(const std::filesystem::path& filename, offset_type off);
  [[deprecated]]
  void open(const std::filesystem::path& filename, offset_type off, std::error_code& ec);
  [[deprecated]]
  void open(fd_type&& fd, offset_type off);
  [[deprecated]]
  void open(fd_type&& fd, offset_type off, std::error_code& ec);
  void close();
  void close(std::error_code& ec);

  template<typename CompletionToken>
  auto async_open(const std::filesystem::path& filename, offset_type off, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code)>::return_type;

  template<typename CompletionToken>
  auto async_open(fd_type&& fd, offset_type off, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code)>::return_type;

  private:
  template<typename CompletionToken, typename MapAlloc>
  auto async_recover_(CompletionToken&& token, MapAlloc&& map_alloc) -> typename asio::async_result<
      std::decay_t<CompletionToken>,
      void(
          std::error_code /* ec */,
          wal_header_t /* header */,
          replacement_map<std::decay_t<MapAlloc>> /* wmap */,
          std::uint64_t /* next_write_off */,
          bool /* checkpoint_seen */
      )>::return_type;

  template<typename CompletionToken>
  auto async_recover_(CompletionToken&& token) -> typename asio::async_result<
      std::decay_t<CompletionToken>,
      void(
          std::error_code /* ec */,
          wal_header_t /* header */,
          replacement_map<allocator_type> /* wmap */,
          std::uint64_t /* next_write_off */,
          bool /* checkpoint_seen */
      )>::return_type;

  template<typename ReaderAllocator, typename MapAllocator, typename Handler> class async_recover_reader;

  fd_type fd_;
  offset_type off_;
  strand_type strand_; // WAL internals need certain actions to happen on a single thread.
  allocator_type alloc_;

  std::uint64_t fsize_ = 0; // WAL models a file with the given size.
  std::uint64_t foff_ = 0; // WAL models a file, starting at this offset in `fd_`.
  std::uint64_t next_write_off_; // offset to end-of-the-wal.
  std::uint64_t wal_end_record_off_; // offset to the active end-of-wal record.
  replacement_map<allocator_type> wmap_; // writes that are present in the wal, but not yet written to the actual files.
  bool checkpointed_ = false; // If set, some of the writes in this WAL have been flushed to their underlying files.
};


template<typename Executor, typename WalAllocator>
template<typename Allocator>
class wal<Executor, WalAllocator>::tx {
  private:
  using wal_message = std::variant<
      wal_tx_record<wal_entry::write>,
      wal_tx_record<wal_entry::write_many>,
      wal_tx_record<wal_entry::memset>,
      wal_tx_record<wal_entry::resize>>;
  using wal_message_vector = std::vector<
      wal_message,
      typename std::allocator_traits<Allocator>::template rebind_alloc<wal_message>>;

  class wal_message_inserter_;

  public:
  using allocator_type = typename std::allocator_traits<Allocator>::template rebind_alloc<std::byte>;
  using offset_type = wal::offset_type;
  using size_type = wal::size_type;

  explicit tx(allocator_type alloc = allocator_type());
  explicit tx(wal& w, allocator_type alloc = allocator_type());
  tx(const tx&) = delete;
  tx(tx&& other) noexcept(std::is_nothrow_move_constructible_v<replacement_map<allocator_type>>);

  auto get_allocator() const -> allocator_type {
    return wmap_.get_allocator();
  }

  ///\brief Write data.
  void write(offset_type off, asio::const_buffer buf, std::error_code& ec);
  ///\brief Write the same data at multiple offsets.
  template<typename OffsetIter>
  void write_many(OffsetIter b, OffsetIter e, asio::const_buffer buf, std::error_code& ec);
  ///\brief Write a byte pattern at the given address range.
  void memset(offset_type off, size_type bytes, std::uint8_t value, std::error_code& ec);
  ///\brief Zero the file at the given address range.
  void memset(offset_type off, size_type bytes, std::error_code& ec);
  /**
   * \brief Resize the file.
   * \attention
   * Transactions with a resize will only succeed if no other commited
   * transactions have resized the file in the meantime.
   */
  void resize(size_type new_size, std::error_code& ec);

  ///\brief Write data.
  void write(offset_type off, asio::const_buffer buf);
  ///\brief Write the same data at multiple offsets.
  template<typename OffsetIter>
  void write_many(OffsetIter b, OffsetIter e, asio::const_buffer buf);
  ///\brief Write a byte pattern at the given address range.
  void memset(offset_type off, size_type bytes, std::uint8_t value);
  ///\brief Zero the file at the given address range.
  void memset(offset_type off, size_type bytes);
  /**
   * \brief Resize the file.
   * \attention
   * Transactions with a resize will only succeed if no other commited
   * transactions have resized the file in the meantime.
   */
  void resize(size_type new_size);

  ///\brief The size of the file.
  auto size() const noexcept -> size_type { return file_end_; }

  ///\brief Cancel the transaction.
  ///\note It's safe to rollback when not in the transaction state.
  void rollback() noexcept;
  /**
   * \brief Perform a soft commit.
   * \details
   * A soft commit guarantees atomicity, but does not guarantee persistence.
   * It is used for transactional changes, where the logical before and after
   * states are unaltered.
   *
   * The WAL guarantees atomicity and ordering.
   *
   * In the case of failure, the WAL guarantees that if the transaction is
   * not recovered, then neither will any subsequent transactions be recovered.
   * In other words, it guarantees ordering will be maintained.
   *
   * A soft commit is faster than a hard commit, since it foregoes the need
   * for fsync calls.
   *
   * \attention
   * This call transitions the transaction to the not-a-transaction state.
   *
   * \tparam CompletionToken
   * Asio completion token that is signaled when the commit completes.
   * Must accept a single `std::error_code` argument.
   */
  template<typename CompletionToken>
  auto async_soft_commit(CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code)>::return_type; // XXX implement
  /**
   * \brief Perform a hard commit.
   * \details
   * A hard commit guarantees atomicity, and persistence.
   * It is used for transactional changes, where the logical before and after
   * states are altered.
   *
   * The WAL guarantees atomicity and ordering.
   *
   * In the case of failure, the WAL guarantees that the transaction is
   * recovered, and all preceding transactions will be recovered as well.
   *
   * A hard commit is slower than a soft commit, because it needs to wait
   * for fsync calls to complete.
   *
   * \attention
   * This call transitions the transaction to the not-a-transaction state.
   *
   * \tparam CompletionToken
   * Asio completion token that is signaled when the commit completes.
   * Must accept a single `std::error_code` argument.
   */
  template<typename CompletionToken>
  auto async_commit(CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code)>::return_type; // XXX implement

  private:
  template<typename Alloc>
  auto make_buffer_vector_(Alloc alloc) const
  -> std::vector<
      asio::const_buffer,
      typename std::allocator_traits<Alloc>::template rebind_alloc<asio::const_buffer>>;

  auto make_buffer_vector_() const
  -> std::vector<
      asio::const_buffer,
      typename std::allocator_traits<allocator_type>::template rebind_alloc<asio::const_buffer>>;

  std::weak_ptr<wal> wal_;
  offset_type file_end_;
  bool file_end_changed_ = false; // Set if the transaction changed the file size.
  replacement_map<allocator_type> wmap_;
  wal_message_vector messages_;
};


template<typename Executor, typename WalAllocator>
template<typename Allocator>
class wal<Executor, WalAllocator>::tx<Allocator>::wal_message_inserter_ {
  public:
  constexpr wal_message_inserter_() noexcept;
  wal_message_inserter_(const wal_message_inserter_& other) = delete;
  wal_message_inserter_(wal_message_inserter_&& other) noexcept;
  template<typename... Args> explicit wal_message_inserter_(wal_message_vector& v, Args&&... args);
  ~wal_message_inserter_();

  wal_message_inserter_& operator=(const wal_message_inserter_& other) = delete;
  auto operator=(wal_message_inserter_&& other) noexcept -> wal_message_inserter_&;

  void commit() noexcept;
  void rollback() noexcept;

  auto operator*() const noexcept -> wal_message&;
  auto operator->() const noexcept -> wal_message*;

  private:
  wal_message_vector* vector_;
};


template<typename Executor, typename WalAllocator>
template<typename Allocator, typename MapAlloc, typename Handler>
class wal<Executor, WalAllocator>::async_recover_reader {
  private:
  enum class state {
    init,
    parse_header,
    decode_entry_opcode,
    decode_entry,
    invalid
  };

  public:
  using allocator_type = Allocator;

  private:
  class barrier_handler_;
  using reader_type = buffered_read_stream_at<fd_type, asio::strand<executor_type>, allocator_type>;

  struct impl_type {
    impl_type(fd_type& fd, offset_type wal_off, allocator_type alloc, MapAlloc map_alloc)
    : reader(asio::strand<executor_type>(fd.get_executor()), fd, wal_off, reader_type::max_filelen, alloc),
      rmap(std::move(map_alloc))
    {}

    reader_type reader;
    // Outputs.
    wal_header_t header;
    replacement_map<MapAlloc> rmap;
    std::uint64_t wal_end_offset;
    bool checkpoint_seen = false;
  };

  public:
  async_recover_reader(fd_type& fd, offset_type wal_off, allocator_type alloc, MapAlloc map_alloc, Handler&& handler);
  async_recover_reader(async_recover_reader&&) = default;

  void operator()();
  void operator()(std::error_code ec, wal_header_t header);
  void operator()(std::error_code ec, wal_entry opcode);
  template<wal_entry Entry> void operator()(std::error_code ec, const wal_record<Entry>& opcode);

  auto get_allocator() const -> allocator_type;

  private:
  void fin_(std::error_code ec);

  state state_ = state::init;
  std::shared_ptr<impl_type> impl_;
  completion_barrier<barrier_handler_, executor_type> barrier_;
};


template<typename Executor, typename WalAllocator>
template<typename Allocator, typename MapAlloc, typename Handler>
class wal<Executor, WalAllocator>::async_recover_reader<Allocator, MapAlloc, Handler>::barrier_handler_ {
  public:
  using executor_type = typename asio::associated_executor<Handler, wal::executor_type>::type;
  using allocator_type = typename asio::associated_allocator<Handler>::type;

  barrier_handler_(Handler&& handler, std::shared_ptr<impl_type> impl, const wal::executor_type& ex);
  void operator()(std::error_code ec);
  auto get_executor() const -> executor_type;
  auto get_allocator() const -> allocator_type;

  private:
  Handler handler_;
  std::shared_ptr<impl_type> impl_;
  executor_type ex_;
};


extern template class wal<>;


} /* namespace earnest::detail */

#include "wal2-inl.h"

#endif /* EARNEST_DETAIL_WAL2_H */
