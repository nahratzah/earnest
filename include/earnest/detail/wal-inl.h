#ifndef EARNEST_DETAIL_WAL2_INL_H
#define EARNEST_DETAIL_WAL2_INL_H

#include <algorithm>
#include <cassert>
#include <tuple>

#include <earnest/detail/allocated_object.h>
#include <earnest/detail/completion_wrapper.h>

#include <asio/associated_allocator.hpp>
#include <asio/associated_executor.hpp>
#include <asio/bind_executor.hpp>
#include <asio/buffer.hpp>
#include <asio/compose.hpp>
#include <asio/executor_work_guard.hpp>
#include <asio/read.hpp>
#include <asio/read_at.hpp>
#include <asio/use_future.hpp>
#include <boost/endian/conversion.hpp>

namespace earnest::detail {


inline auto make_error_code(wal_errc e) noexcept -> std::error_code {
  return std::error_code(static_cast<int>(e), wal_error_category());
}

inline auto make_error_code(wal_tx_errc e) noexcept -> std::error_code {
  return std::error_code(static_cast<int>(e), wal_tx_error_category());
}


template<typename AsyncReadStream, typename CompletionToken>
auto async_wal_entry_read(AsyncReadStream& reader, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, wal_entry)>::return_type {
  using init_type = asio::async_completion<CompletionToken, void(std::error_code, wal_entry)>;
  using associated_allocator_type = asio::associated_allocator<typename init_type::completion_handler_type>;
  using raw_entry_t = std::underlying_type_t<wal_entry>;
  using allocated_raw_entry = allocated_object<raw_entry_t, typename std::allocator_traits<typename associated_allocator_type::type>::template rebind_alloc<raw_entry_t>>;

  static_assert(sizeof(raw_entry_t) % sizeof(std::uint32_t) == 0,
      "entry type must be 32-bit aligned type");

  init_type init(token);
  allocated_raw_entry opcode = allocated_raw_entry::allocate(associated_allocator_type::get(init.completion_handler));
  auto buf = asio::buffer(opcode.get(), sizeof(raw_entry_t));

  asio::async_read(
      reader, buf,
      completion_wrapper<void(std::error_code, std::size_t)>(
          std::move(init.completion_handler),
          [opcode=std::move(opcode)](auto handler, std::error_code ec, std::size_t bytes) {
            if (!ec) assert(bytes == sizeof(raw_entry_t));

            boost::endian::big_to_native_inplace(*opcode);
            std::invoke(handler, ec, static_cast<wal_entry>(*opcode));
          }));

  return init.result.get();
}


constexpr wal_header_t::wal_header_t() noexcept
: wal_header_t(0, 0)
{}

constexpr wal_header_t::wal_header_t(std::uint64_t foff, std::uint64_t fsize) noexcept
: foff(foff),
  fsize(fsize)
{}

template<typename AsyncReadStream, typename CompletionToken>
auto wal_header_t::async_read(AsyncReadStream& reader, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, wal_header_t)>::return_type {
  using init_type = asio::async_completion<CompletionToken, void(std::error_code, std::size_t)>;
  using associated_allocator_type = asio::associated_allocator<typename init_type::completion_handler_type>;
  using wal_header_alloc = typename std::allocator_traits<typename associated_allocator_type::type>::template rebind_alloc<wal_header_t>;

  static_assert(
      std::has_unique_object_representations_v<wal_header_t> && std::is_standard_layout_v<wal_header_t>,
      "the read operation depends on wal_header_t mirroring the on-disk layout of the data");
  static_assert(sizeof(wal_header_t) == BYTES);

  init_type init(token);
  auto header = allocated_object<wal_header_t, wal_header_alloc>::allocate(
      associated_allocator_type::get(init.completion_handler));
  auto buf = asio::buffer(header.get(), BYTES);

  asio::async_read(
      reader, buf,
      completion_wrapper<void(std::error_code, std::size_t)>(
          std::move(init.completion_handler),
          [header=std::move(header)](auto handler, std::error_code ec, std::size_t bytes) {
            if (!ec) {
              assert(bytes == BYTES);
              if (header->magic != MAGIC) ec = wal_errc::invalid;
            }

            boost::endian::big_to_native_inplace(header->foff);
            boost::endian::big_to_native_inplace(header->fsize);
            std::invoke(handler, ec, *header);
          }));

  return init.result.get();
}


template<typename AsyncReadStream, typename CompletionToken>
auto wal_record<wal_entry::skip>::async_read(AsyncReadStream& reader, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, wal_record)>::return_type {
  using init_type = asio::async_completion<CompletionToken, void(std::error_code, wal_record)>;
  using associated_allocator_type = asio::associated_allocator<typename init_type::completion_handler_type>;
  using alloc = typename std::allocator_traits<typename associated_allocator_type::type>::template rebind_alloc<wal_record>;

  static_assert(
      std::has_unique_object_representations_v<wal_record> && std::is_standard_layout_v<wal_record>,
      "the read operation depends on wal_record mirroring the on-disk layout of the data");
  static_assert(sizeof(wal_record) == BYTES);

  init_type init(token);
  auto entry = allocated_object<wal_record, alloc>::allocate(
      associated_allocator_type::get(init.completion_handler));
  auto buf = asio::buffer(entry.get(), BYTES);

  asio::async_read(
      reader, buf,
      completion_wrapper<void(std::error_code, std::size_t)>(
          std::move(init.completion_handler),
          [entry=std::move(entry)](auto handler, std::error_code ec, std::size_t bytes) {
            if (!ec) assert(bytes == BYTES);

            boost::endian::big_to_native_inplace(entry->nbytes);
            std::invoke(handler, ec, *entry);
          }));

  return init.result.get();
}


template<typename AsyncReadStream, typename CompletionToken>
auto wal_record<wal_entry::resize>::async_read(AsyncReadStream& reader, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, wal_record)>::return_type {
  using init_type = asio::async_completion<CompletionToken, void(std::error_code, wal_record)>;
  using associated_allocator_type = asio::associated_allocator<typename init_type::completion_handler_type>;
  using alloc = typename std::allocator_traits<typename associated_allocator_type::type>::template rebind_alloc<wal_record>;

  static_assert(
      std::has_unique_object_representations_v<wal_record> && std::is_standard_layout_v<wal_record>,
      "the read operation depends on wal_record mirroring the on-disk layout of the data");
  static_assert(sizeof(wal_record) == BYTES);

  init_type init(token);
  auto entry = allocated_object<wal_record, alloc>::allocate(
      associated_allocator_type::get(init.completion_handler));
  auto buf = asio::buffer(entry.get(), BYTES);

  asio::async_read(
      reader, buf,
      completion_wrapper<void(std::error_code, std::size_t)>(
          std::move(init.completion_handler),
          [entry=std::move(entry)](auto handler, std::error_code ec, std::size_t bytes) {
            if (!ec) assert(bytes == BYTES);

            boost::endian::big_to_native_inplace(entry->size);
            std::invoke(handler, ec, *entry);
          }));

  return init.result.get();
}


template<typename AsyncReadStream, typename CompletionToken>
auto wal_record<wal_entry::write>::async_read(AsyncReadStream& reader, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, wal_record)>::return_type {
  using init_type = asio::async_completion<CompletionToken, void(std::error_code, wal_record)>;
  using associated_allocator_type = asio::associated_allocator<typename init_type::completion_handler_type>;
  using alloc = typename std::allocator_traits<typename associated_allocator_type::type>::template rebind_alloc<wal_record>;

  init_type init(token);
  auto entry = allocated_object<wal_record, alloc>::allocate(
      associated_allocator_type::get(init.completion_handler));
  std::array<asio::mutable_buffer, 2> bufs = {
      asio::buffer(&entry->offset, sizeof(entry->offset)),
      asio::buffer(&entry->nbytes, sizeof(entry->nbytes))
  };
  assert(asio::buffer_size(bufs) == BYTES);

  asio::async_read(
      reader, bufs,
      completion_wrapper<void(std::error_code, std::size_t)>(
          std::move(init.completion_handler),
          [entry=std::move(entry)](auto handler, std::error_code ec, std::size_t bytes) {
            if (!ec) assert(bytes == BYTES);

            boost::endian::big_to_native_inplace(entry->offset);
            boost::endian::big_to_native_inplace(entry->nbytes);
            std::invoke(handler, ec, *entry);
          }));

  return init.result.get();
}


template<typename AsyncReadStream, typename CompletionToken>
auto wal_record<wal_entry::write_many>::async_read(AsyncReadStream& reader, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, wal_record)>::return_type {
  using init_type = asio::async_completion<CompletionToken, void(std::error_code, wal_record)>;
  using associated_allocator_type = asio::associated_allocator<typename init_type::completion_handler_type>;
  using alloc = typename std::allocator_traits<typename associated_allocator_type::type>::template rebind_alloc<wal_record>;

  init_type init(token);
  auto entry = allocated_object<wal_record, alloc>::allocate(
      associated_allocator_type::get(init.completion_handler));

  async_decode_offset_vector(reader,
      completion_wrapper<void(std::error_code, std::vector<std::uint64_t>)>(
          std::move(init.completion_handler),
          [entry=std::move(entry), &reader](auto&& handler, std::error_code ec, std::vector<std::uint64_t> offsets) mutable {
            if (ec) {
              handler(ec, wal_record());
              return;
            }

            entry->offsets = std::move(offsets);
            auto buf = asio::buffer(&entry->nbytes, sizeof(entry->nbytes));
            asio::async_read(
                reader,
                buf,
                completion_wrapper<void(std::error_code, std::size_t)>(
                    std::move(handler),
                    [entry=std::move(entry)](auto&& handler, std::error_code ec, std::size_t bytes) {
                      assert(bytes == 4); // expect length (4 byte)

                      boost::endian::big_to_native_inplace(entry->nbytes);
                      handler(ec, *entry);
                    }));
          }));

  return init.result.get();
}


template<typename AsyncReadStream, typename CompletionToken>
auto wal_record<wal_entry::memset>::async_read(AsyncReadStream& reader, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, wal_record)>::return_type {
  using init_type = asio::async_completion<CompletionToken, void(std::error_code, wal_record)>;
  using associated_allocator_type = asio::associated_allocator<typename init_type::completion_handler_type>;
  using alloc = typename std::allocator_traits<typename associated_allocator_type::type>::template rebind_alloc<wal_record>;

  static_assert(
      std::has_unique_object_representations_v<wal_record> && std::is_standard_layout_v<wal_record>,
      "the read operation depends on wal_record mirroring the on-disk layout of the data");
  static_assert(sizeof(wal_record) == BYTES);

  init_type init(token);
  auto entry = allocated_object<wal_record, alloc>::allocate(
      associated_allocator_type::get(init.completion_handler));
  auto buf = asio::buffer(entry.get(), BYTES);

  asio::async_read(
      reader, buf,
      completion_wrapper<void(std::error_code, std::size_t)>(
          std::move(init.completion_handler),
          [entry=std::move(entry)](auto handler, std::error_code ec, std::size_t bytes) {
            if (!ec) assert(bytes == BYTES);

            boost::endian::big_to_native_inplace(entry->offset);
            boost::endian::big_to_native_inplace(entry->fill_value_);
            boost::endian::big_to_native_inplace(entry->nbytes);
            std::invoke(handler, ec, *entry);
          }));

  return init.result.get();
}


inline wal_tx_record<wal_entry::write>::wal_tx_record(std::uint64_t offset, std::shared_ptr<const void> buf, std::size_t bufsiz)
: buf_(std::move(buf)),
  data_{ offset, std::uint32_t(bufsiz) }
{
  if (bufsiz > std::numeric_limits<std::uint32_t>::max())
    throw std::length_error("too many bytes");

  boost::endian::native_to_big_inplace(data_.nbytes);
  boost::endian::native_to_big_inplace(data_.offset);
}

inline auto wal_tx_record<wal_entry::write>::buffers() const noexcept -> std::array<asio::const_buffer, 5> {
  static const std::uint32_t opcode = boost::endian::native_to_big(static_cast<std::uint32_t>(entry_type));
  static const std::array<std::uint8_t, 3> padding{ 0u, 0u, 0u };

  asio::const_buffer padding_buf = asio::buffer(padding);
  switch (boost::endian::big_to_native(data_.nbytes) % 4u) {
    case 0u:
      padding_buf = asio::buffer(padding_buf, 0);
      break;
    case 1u:
      padding_buf = asio::buffer(padding_buf, 3);
      break;
    case 2u:
      padding_buf = asio::buffer(padding_buf, 2);
      break;
    case 3u:
      padding_buf = asio::buffer(padding_buf, 1);
      break;
  }

  auto result = std::array<asio::const_buffer, 5>{
    asio::buffer(&opcode, sizeof(opcode)),
    asio::buffer(&data_.offset, sizeof(data_.offset)),
    asio::buffer(&data_.nbytes, sizeof(data_.nbytes)),
    asio::buffer(buf_.get(), boost::endian::big_to_native(data_.nbytes)),
    padding_buf
  };
  assert(asio::buffer_size(result) % 4u == 0u);
  return result;
}


template<typename OffsetIter>
inline wal_tx_record<wal_entry::write_many>::wal_tx_record(OffsetIter b, OffsetIter e, std::shared_ptr<const void> buf, std::size_t bufsiz)
: buf_(std::move(buf)),
  data_{ std::vector<std::uint64_t>(b, e), std::uint32_t(bufsiz) },
  noffsets_(data_.offsets.size())
{
  if (data_.offsets.size() > std::numeric_limits<std::uint32_t>::max())
    throw std::length_error("too many offsets");
  if (bufsiz > std::numeric_limits<std::uint32_t>::max())
    throw std::length_error("too many bytes");

  boost::endian::native_to_big_inplace(data_.nbytes);
  boost::endian::native_to_big_inplace(noffsets_);
  std::for_each(data_.offsets.begin(), data_.offsets.end(),
      [](std::uint64_t& v) {
        boost::endian::native_to_big_inplace(v);
      });
}

inline auto wal_tx_record<wal_entry::write_many>::buffers() const noexcept -> std::array<asio::const_buffer, 6> {
  static const std::uint32_t opcode = boost::endian::native_to_big(static_cast<std::uint32_t>(entry_type));
  static const std::array<std::uint8_t, 3> padding{ 0u, 0u, 0u };

  asio::const_buffer padding_buf = asio::buffer(padding);
  switch (boost::endian::big_to_native(data_.nbytes) % 4u) {
    case 0u:
      padding_buf = asio::buffer(padding_buf, 0);
      break;
    case 1u:
      padding_buf = asio::buffer(padding_buf, 3);
      break;
    case 2u:
      padding_buf = asio::buffer(padding_buf, 2);
      break;
    case 3u:
      padding_buf = asio::buffer(padding_buf, 1);
      break;
  }

  auto result = std::array<asio::const_buffer, 6>{
    asio::buffer(&opcode, sizeof(opcode)),
    asio::buffer(&noffsets_, sizeof(noffsets_)),
    asio::buffer(data_.offsets.data(), data_.offsets.size() * sizeof(std::uint32_t)),
    asio::buffer(&data_.nbytes, sizeof(data_.nbytes)),
    asio::buffer(buf_.get(), boost::endian::big_to_native(data_.nbytes)),
    padding_buf
  };
  assert(asio::buffer_size(result) % 4u == 0u);
  return result;
}


inline wal_tx_record<wal_entry::memset>::wal_tx_record(std::uint64_t offset, std::uint32_t nbytes, std::uint8_t fill_value) noexcept
: data_{ offset, std::uint32_t(fill_value), nbytes }
{
  boost::endian::native_to_big_inplace(data_.offset);
  boost::endian::native_to_big_inplace(data_.fill_value_);
  boost::endian::native_to_big_inplace(data_.nbytes);
}

inline auto wal_tx_record<wal_entry::memset>::buffers() const noexcept -> std::array<asio::const_buffer, 2> {
  static const std::uint32_t opcode = boost::endian::native_to_big(static_cast<std::uint32_t>(entry_type));
  return std::array<asio::const_buffer, 2>{
    asio::buffer(&opcode, sizeof(opcode)),
    asio::buffer(&data_, sizeof(data_))
  };
}


inline wal_tx_record<wal_entry::resize>::wal_tx_record(std::uint64_t newsize) noexcept
: data_{ newsize }
{
  boost::endian::native_to_big_inplace(data_.size);
}

inline auto wal_tx_record<wal_entry::resize>::buffers() const noexcept -> std::array<asio::const_buffer, 2> {
  static const std::uint32_t opcode = boost::endian::native_to_big(static_cast<std::uint32_t>(entry_type));
  return std::array<asio::const_buffer, 2>{
    asio::buffer(&opcode, sizeof(opcode)),
    asio::buffer(&data_, sizeof(data_))
  };
}


template<typename AsyncReadStream, typename Allocator>
async_decode_offset_vector_<AsyncReadStream, Allocator>::async_decode_offset_vector_(AsyncReadStream& reader, alloc_type alloc)
: reader_(reader),
  elem_(allocated_object<elem_type, alloc_type>::allocate(alloc))
{}

template<typename AsyncReadStream, typename CompletionToken>
template<typename Self>
void async_decode_offset_vector_<AsyncReadStream, CompletionToken>::operator()(Self& self, std::error_code ec, std::size_t bytes) {
  if (ec) {
    self.complete(ec, std::get<1>(std::move(*elem_)));
    return;
  }

  switch (state_) {
    case state::init:
      state_ = state::expect_nelems;
      asio::async_read(reader_,
          asio::buffer(&std::get<0>(*elem_), sizeof(std::get<0>(*elem_))),
          std::move(self));
      break;
    case state::expect_nelems:
      assert(bytes == 4); // expect 4 bytes counter

      boost::endian::big_to_native_inplace(std::get<0>(*elem_));
      if (std::get<0>(*elem_) == 0) {
        self.complete(ec, std::get<1>(std::move(*elem_)));
        return;
      }

      std::get<1>(*elem_).resize(std::get<0>(*elem_));

      state_ = state::expect_offsets;
      asio::async_read(reader_,
          asio::buffer(std::get<1>(*elem_).data(), std::get<1>(*elem_).size() * sizeof(vector_type::value_type)),
          std::move(self));
      break;
    case state::expect_offsets:
      assert(bytes == 8 * std::get<1>(*elem_).size()); // expect repeated 8 bytes offset

      std::for_each(
          std::get<1>(*elem_).begin(), std::get<1>(*elem_).end(),
          [](vector_type::reference v) {
            boost::endian::big_to_native_inplace(v);
          });
      self.complete(ec, std::get<1>(std::move(*elem_)));
      break;
  }
}

template<typename AsyncReadStream, typename CompletionToken>
auto async_decode_offset_vector(AsyncReadStream& reader, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, std::vector<std::uint64_t>)>::return_type {
  using vector_type = std::vector<std::uint64_t>;
  using associated_allocator_type = asio::associated_allocator<std::decay_t<CompletionToken>>;

  auto alloc = associated_allocator_type::get(token);
  return asio::async_compose<CompletionToken, void(std::error_code, vector_type)>(
      async_decode_offset_vector_<AsyncReadStream, typename associated_allocator_type::type>(reader, std::move(alloc)),
      token,
      reader);
}


template<typename Executor, typename Allocator>
wal<Executor, Allocator>::wal(const executor_type& e, allocator_type alloc)
: fd_(e),
  strand_(e)
{}

template<typename Executor, typename Allocator>
wal<Executor, Allocator>::wal(fd_type&& fd, offset_type off, allocator_type alloc)
: wal(fd.get_executor(), alloc)
{
  open(std::move(fd), off);
}

template<typename Executor, typename Allocator>
wal<Executor, Allocator>::wal(fd_type&& fd, offset_type off, std::error_code& ec, allocator_type alloc)
: wal(fd.get_executor(), alloc)
{
  open(std::move(fd), off, ec);
}

template<typename Executor, typename Allocator>
wal<Executor, Allocator>::~wal() = default;

template<typename Executor, typename Allocator>
auto wal<Executor, Allocator>::get_executor() const -> executor_type {
  return strand_.get_inner_executor();
}

template<typename Executor, typename Allocator>
auto wal<Executor, Allocator>::get_allocator() const -> allocator_type {
  // work around allocator<non-void> --> allocator<void> conversion being broken
  if constexpr(std::is_same_v<std::allocator<void>, allocator_type>)
    return allocator_type();
  else
    return alloc_;
}

template<typename Executor, typename Allocator>
void wal<Executor, Allocator>::open(const std::filesystem::path& filename, offset_type off) {
  auto fut = async_open(
      filename, off,
      asio::use_future(
          [](std::error_code ec) {
            if (ec) throw std::system_error(ec, "wal::open");
          }));
  // Wait for future completion.
  fut.get();
}

template<typename Executor, typename Allocator>
void wal<Executor, Allocator>::open(const std::filesystem::path& filename, offset_type off, std::error_code& ec) {
  auto fut = async_open(
      filename, off,
      asio::use_future(
          [&ec](std::error_code ec_result) {
            ec = ec_result;
          }));
  // Wait for future completion.
  fut.get();
}

template<typename Executor, typename Allocator>
void wal<Executor, Allocator>::open(fd_type&& fd, offset_type off) {
  auto fut = async_open(
      std::move(fd), off,
      asio::use_future(
          [](std::error_code ec) {
            if (ec) throw std::system_error(ec, "wal::open");
          }));
  // Wait for future completion.
  fut.get();
}

template<typename Executor, typename Allocator>
void wal<Executor, Allocator>::open(fd_type&& fd, offset_type off, std::error_code& ec) {
  auto fut = async_open(
      std::move(fd), off,
      asio::use_future(
          [&ec](std::error_code ec_result) {
            ec = ec_result;
          }));
  // Wait for future completion.
  fut.get();
}

template<typename Executor, typename Allocator>
template<typename CompletionToken>
auto wal<Executor, Allocator>::async_open(const std::filesystem::path& filename, offset_type off, CompletionToken&& token)
-> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code)>::return_type {
  using init_type = asio::async_completion<CompletionToken, void(std::error_code)>;
  using associated_allocator_type = asio::associated_allocator<typename init_type::completion_handler_type>;
  using associated_executor_type = asio::associated_executor<typename init_type::completion_handler_type, executor_type>;

  init_type init(token);
  auto ex = associated_executor_type::get(init.completion_handler, get_executor());
  if (fd_.is_open()) {
    auto alloc = associated_allocator_type::get(init.completion_handler);
    ex.post(
        [h=std::move(init.completion_handler)]() mutable {
          h(wal_errc::already_open);
        },
        alloc);
    return init.result.get();
  }

  std::error_code ec;
  fd_.open(filename, fd_type::open_mode::READ_WRITE, ec);
  if (ec) {
    auto alloc = associated_allocator_type::get(init.completion_handler);
    ex.post(
        [h=std::move(init.completion_handler), ec]() mutable {
          h(ec);
        },
        alloc);
  } else {
    off_ = off;
    async_recover_(
        completion_wrapper<void(std::error_code, wal_header_t, replacement_map<allocator_type>, std::uint64_t, bool)>(
            std::move(init.completion_handler),
            asio::bind_executor(
                strand_,
                [this, ex=asio::make_work_guard(ex)](auto handler, std::error_code ec, wal_header_t wal_header, replacement_map<allocator_type> rmap, std::uint64_t wal_end, bool checkpoint_seen) {
                  assert(strand_.running_in_this_thread());

                  this->foff_ = wal_header.foff;
                  this->fsize_ = wal_header.fsize;
                  this->next_write_off_ = wal_end;
                  this->wal_end_record_off_ = wal_end - sizeof(std::underlying_type_t<wal_entry>);
                  this->wmap_ = std::move(rmap);
                  this->checkpointed_ = checkpoint_seen;

                  auto alloc = associated_allocator_type::get(handler);
                  ex.get_executor().dispatch(
                      [h=std::move(handler), ec]() mutable {
                        h(ec);
                      },
                      alloc);
                })),
        get_allocator());
  }

  return init.result.get();
}

template<typename Executor, typename Allocator>
template<typename CompletionToken>
auto wal<Executor, Allocator>::async_open(fd_type&& fd, offset_type off, CompletionToken&& token)
-> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code)>::return_type {
  using init_type = asio::async_completion<CompletionToken, void(std::error_code)>;
  using associated_allocator_type = asio::associated_allocator<typename init_type::completion_handler_type>;
  using associated_executor_type = asio::associated_executor<typename init_type::completion_handler_type, executor_type>;

  init_type init(token);
  auto ex = associated_executor_type::get(init.completion_handler, get_executor());
  if (fd_.is_open()) {
    auto alloc = associated_allocator_type::get(init.completion_handler);
    ex.post(
        [h=std::move(init.completion_handler)]() mutable {
          h(wal_errc::already_open);
        },
        alloc);
    return init.result.get();
  }

  fd_ = std::move(fd);
  off_ = off;
  if (!fd_.is_open()) { // closed file is instantanious success
    auto alloc = associated_allocator_type::get(init.completion_handler);
    ex.post(
        [h=std::move(init.completion_handler)]() mutable {
          h(std::error_code());
        },
        alloc);
  } else {
    async_recover_(
        completion_wrapper<void(std::error_code, wal_header_t, replacement_map<allocator_type>, std::uint64_t, bool)>(
            std::move(init.completion_handler),
            asio::bind_executor(
                strand_,
                [this, ex=asio::make_work_guard(ex)](auto handler, std::error_code ec, wal_header_t wal_header, replacement_map<allocator_type> rmap, std::uint64_t wal_end, bool checkpoint_seen) {
                  assert(strand_.running_in_this_thread());

                  this->foff_ = wal_header.foff;
                  this->fsize_ = wal_header.fsize;
                  this->next_write_off_ = wal_end;
                  this->wal_end_record_off_ = wal_end - sizeof(std::underlying_type_t<wal_entry>);
                  this->wmap_ = std::move(rmap);
                  this->checkpointed_ = checkpoint_seen;

                  auto alloc = associated_allocator_type::get(handler);
                  ex.get_executor().dispatch(
                      [h=std::move(handler), ec]() mutable {
                        h(ec);
                      },
                      alloc);
                })),
        get_allocator());
  }

  return init.result.get();
}

template<typename Executor, typename Allocator>
void wal<Executor, Allocator>::close() {
  std::error_code ec;
  close(ec);
  if (ec) throw std::system_error(ec, "wal::close");
}

template<typename Executor, typename Allocator>
void wal<Executor, Allocator>::close(std::error_code& ec) {
  fd_.close(ec);
}

template<typename Executor, typename Allocator>
template<typename CompletionToken, typename MapAlloc>
auto wal<Executor, Allocator>::async_recover_(CompletionToken&& token, MapAlloc&& map_alloc) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, wal_header_t, replacement_map<std::decay_t<MapAlloc>>, std::uint64_t, bool)>::return_type {
  using init_type = asio::async_completion<CompletionToken, void(std::error_code, wal_header_t, replacement_map<std::decay_t<MapAlloc>>, std::uint64_t, bool)>;
  using completion_handler_t = typename init_type::completion_handler_type;
  using associated_allocator_t = asio::associated_allocator<completion_handler_t, allocator_type>;

  init_type init(token);
  typename associated_allocator_t::type ch_allocator = associated_allocator_t::get(init.completion_handler, get_allocator());
  get_executor().post(
      async_recover_reader<typename associated_allocator_t::type, std::decay_t<MapAlloc>, completion_handler_t>(fd_, off_, ch_allocator, std::forward<MapAlloc>(map_alloc), std::move(init.completion_handler)),
      ch_allocator);
  return init.result.get();
}

template<typename Executor, typename Allocator>
template<typename CompletionToken>
auto wal<Executor, Allocator>::async_recover_(CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, wal_header_t, replacement_map<allocator_type>, std::uint64_t, bool)>::return_type {
  return async_recover_(std::forward<CompletionToken>(token), get_allocator());
}


template<typename Executor, typename WalAllocator>
template<typename Allocator>
wal<Executor, WalAllocator>::tx<Allocator>::tx(allocator_type alloc)
: wmap_(alloc),
  messages_(alloc)
{}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
wal<Executor, WalAllocator>::tx<Allocator>::tx(wal& w, allocator_type alloc)
: wal_(w.weak_from_this()),
  file_end_(w.fsize_),
  wmap_(alloc),
  messages_(alloc)
{}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
wal<Executor, WalAllocator>::tx<Allocator>::tx(tx&& other) noexcept(std::is_nothrow_move_constructible_v<replacement_map<allocator_type>>)
: wal_(std::move(other.wal_)),
  file_end_(std::move(other.file_end_)),
  file_end_changed_(std::move(other.file_end_changed_)),
  wmap_(std::move(other.wmap_)),
  messages_(std::move(other.messages_))
{}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
void wal<Executor, WalAllocator>::tx<Allocator>::write(offset_type off, asio::const_buffer buf, std::error_code& ec) {
  if (wal_.expired()) [[unlikely]] {
    ec = wal_tx_errc::bad_tx;
    return;
  }
  if (off > file_end_ || buf.size() > file_end_ - off) [[unlikely]] {
    ec = wal_tx_errc::write_past_eof;
    return;
  }
  if (buf.size() == 0) [[unlikely]] {
    ec.clear();
    return;
  }

  wal_message_inserter_ msg_inserter(
      messages_,
      std::in_place_type<wal_tx_record<wal_entry::write>>, off, nullptr, buf.size());

  ec.clear();
  auto ins = wmap_.insert(off, buf);
  std::get<wal_tx_record<wal_entry::write>>(*msg_inserter).shared_data() = ins.shared_data();

  msg_inserter.commit();
}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
template<typename OffsetIter>
void wal<Executor, WalAllocator>::tx<Allocator>::write_many(OffsetIter b, OffsetIter e, asio::const_buffer buf, std::error_code& ec) {
  if (wal_.expired()) [[unlikely]] {
    ec = wal_tx_errc::bad_tx;
    return;
  }
  if (b == e) [[unlikely]] {
    ec.clear();
    return;
  } else {
    const std::uint64_t off = *std::max_element(b, e);
    if (off > file_end_ || buf.size() > file_end_ - off) [[unlikely]] {
      ec = wal_tx_errc::write_past_eof;
      return;
    }
  }
  if (buf.size() == 0) [[unlikely]] {
    ec.clear();
    return;
  }

  wal_message_inserter_ msg_inserter(
      messages_,
      std::in_place_type<wal_tx_record<wal_entry::write_many>>, b, e, nullptr, buf.size());

  ec.clear();
  auto ins_shared_data = wmap_.insert_many(b, e, buf);
  std::get<wal_tx_record<wal_entry::write>>(*msg_inserter).shared_data() = std::move(ins_shared_data);

  msg_inserter.commit();
}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
void wal<Executor, WalAllocator>::tx<Allocator>::memset(offset_type off, size_type bytes, std::uint8_t value, std::error_code& ec) {
  if (wal_.expired()) [[unlikely]] {
    ec = wal_tx_errc::bad_tx;
    return;
  }
  if (off > file_end_ || bytes > file_end_ - off) [[unlikely]] {
    ec = wal_tx_errc::write_past_eof;
    return;
  }
  if (bytes == 0) [[unlikely]] {
    ec.clear();
    return;
  }

  wal_message_inserter_ msg_inserter(
      messages_,
      std::in_place_type<wal_tx_record<wal_entry::memset>>, off, bytes, value);

  ec.clear();
  auto ins = wmap_.insert(off, bytes);
  std::memset(ins.second.data(), value, ins.second.size());

  msg_inserter.commit();
}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
void wal<Executor, WalAllocator>::tx<Allocator>::memset(offset_type off, size_type bytes, std::error_code& ec) {
  memset(off, bytes, 0u, ec);
}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
void wal<Executor, WalAllocator>::tx<Allocator>::resize(size_type new_size, std::error_code& ec) {
  if (wal_.expired()) [[unlikely]] {
    ec = wal_tx_errc::bad_tx;
    return;
  }

  wal_message_inserter_ msg_inserter(
      messages_,
      std::in_place_type<wal_tx_record<wal_entry::resize>>, new_size);

  wmap_.truncate(new_size);
  if (file_end_ < new_size) {
    auto ins = wmap_.insert(file_end_, new_size - file_end_);
    std::memset(ins.second.data(), 0, ins.second.size());
  }

  file_end_ = new_size;
  file_end_changed_ = true;

  msg_inserter.commit();
}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
void wal<Executor, WalAllocator>::tx<Allocator>::write(offset_type off, asio::const_buffer buf) {
  std::error_code ec;
  write(off, buf, ec);
  if (ec) [[unlikely]] throw std::system_error(ec);
}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
template<typename OffsetIter>
void wal<Executor, WalAllocator>::tx<Allocator>::write_many(OffsetIter b, OffsetIter e, asio::const_buffer buf) {
  std::error_code ec;
  write_many(std::forward<OffsetIter>(b), std::forward<OffsetIter>(e), buf, ec);
  if (ec) [[unlikely]] throw std::system_error(ec);
}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
void wal<Executor, WalAllocator>::tx<Allocator>::memset(offset_type off, size_type bytes, std::uint8_t value) {
  std::error_code ec;
  memset(off, bytes, value, ec);
  if (ec) [[unlikely]] throw std::system_error(ec);
}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
void wal<Executor, WalAllocator>::tx<Allocator>::memset(offset_type off, size_type bytes) {
  std::error_code ec;
  memset(off, bytes, ec);
  if (ec) [[unlikely]] throw std::system_error(ec);
}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
void wal<Executor, WalAllocator>::tx<Allocator>::resize(size_type new_size) {
  std::error_code ec;
  resize(new_size, ec);
  if (ec) [[unlikely]] throw std::system_error(ec);
}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
void wal<Executor, WalAllocator>::tx<Allocator>::rollback() noexcept {
  wal_ = nullptr;
  wmap_.clear();
  messages_.clear();
  file_end_changed_ = false;
}

template<typename Executor, typename WalAllocator>
template<typename TxAllocator>
template<typename Alloc>
auto wal<Executor, WalAllocator>::tx<TxAllocator>::make_buffer_vector_(Alloc alloc) const
-> std::vector<asio::const_buffer, typename std::allocator_traits<Alloc>::template rebind_alloc<asio::const_buffer>> {
  std::vector<asio::const_buffer, typename std::allocator_traits<Alloc>::template rebind_alloc<asio::const_buffer>> result(alloc);

  std::for_each(
      messages_.begin(), messages_.end(),
      [&result](const wal_message& msg) noexcept {
        std::visit(
            [&result](const auto& m) {
              const auto buffers = m.buffers();
              result.insert(
                  result.end(),
                  asio::buffer_sequence_begin(buffers),
                  asio::buffer_sequence_end(buffers));
            },
            msg);
      });
  return result;
}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
auto wal<Executor, WalAllocator>::tx<Allocator>::make_buffer_vector_() const
-> std::vector<asio::const_buffer, typename std::allocator_traits<allocator_type>::template rebind_alloc<asio::const_buffer>> {
  return make_buffer_vector_(get_allocator());
}


template<typename Executor, typename WalAllocator>
template<typename Allocator>
constexpr wal<Executor, WalAllocator>::tx<Allocator>::wal_message_inserter_::wal_message_inserter_() noexcept
: vector_(nullptr)
{}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
wal<Executor, WalAllocator>::tx<Allocator>::wal_message_inserter_::wal_message_inserter_(wal_message_inserter_&& other) noexcept
: vector_(std::exchange(other.vector_, nullptr))
{}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
template<typename... Args>
wal<Executor, WalAllocator>::tx<Allocator>::wal_message_inserter_::wal_message_inserter_(wal_message_vector& v, Args&&... args)
: vector_(&v)
{
  vector_->emplace_back(std::forward<Args>(args)...);
}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
wal<Executor, WalAllocator>::tx<Allocator>::wal_message_inserter_::~wal_message_inserter_() {
  if (vector_ != nullptr) rollback();
}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
auto wal<Executor, WalAllocator>::tx<Allocator>::wal_message_inserter_::operator=(wal_message_inserter_&& other) noexcept -> wal_message_inserter_& {
  if (&other != this) [[likely]] {
    if (vector_ != nullptr) rollback();
    vector_ = std::exchange(other.vector_, nullptr);
  }
  return *this;
}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
void wal<Executor, WalAllocator>::tx<Allocator>::wal_message_inserter_::commit() noexcept {
  assert(vector_ != nullptr); // undefined behaviour
  vector_ = nullptr;
}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
void wal<Executor, WalAllocator>::tx<Allocator>::wal_message_inserter_::rollback() noexcept {
  assert(vector_ != nullptr); // undefined behaviour
  vector_->pop_back();
  vector_ = nullptr;
}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
auto wal<Executor, WalAllocator>::tx<Allocator>::wal_message_inserter_::operator*() const noexcept -> wal_message& {
  assert(vector_ != nullptr); // undefined behaviour
  return vector_->back();
}

template<typename Executor, typename WalAllocator>
template<typename Allocator>
auto wal<Executor, WalAllocator>::tx<Allocator>::wal_message_inserter_::operator->() const noexcept -> wal_message* {
  assert(vector_ != nullptr); // undefined behaviour
  return &vector_->back();
}


template<typename Executor, typename WalAllocator>
template<typename Allocator, typename MapAlloc, typename Handler>
wal<Executor, WalAllocator>::async_recover_reader<Allocator, MapAlloc, Handler>::async_recover_reader(fd_type& fd, offset_type wal_off, allocator_type alloc, MapAlloc map_alloc, Handler&& handler)
: impl_(std::allocate_shared<impl_type>(alloc, fd, wal_off, alloc, std::move(map_alloc))),
  barrier_(barrier_handler_(std::move(handler), impl_, fd.get_executor()), fd.get_executor())
{}

template<typename Executor, typename WalAllocator>
template<typename Allocator, typename MapAlloc, typename Handler>
void wal<Executor, WalAllocator>::async_recover_reader<Allocator, MapAlloc, Handler>::operator()() {
  switch (state_) {
    default:
      assert(false);
      break;
    case state::init:
      state_ = state::parse_header;
      wal_header_t::async_read(impl_->reader, std::move(*this));
      return;
  }
}

template<typename Executor, typename WalAllocator>
template<typename Allocator, typename MapAlloc, typename Handler>
void wal<Executor, WalAllocator>::async_recover_reader<Allocator, MapAlloc, Handler>::operator()(std::error_code ec, wal_header_t header) {
  switch (state_) {
    default:
      assert(false);
      break;
    case state::parse_header:
      if (ec) {
        fin_(ec);
        return;
      }

      impl_->header = std::move(header);
      impl_->reader.max_offset(impl_->header.foff); // Ensure we won't read outside bounds.

      state_ = state::decode_entry_opcode;
      async_wal_entry_read(impl_->reader, std::move(*this));
      return;
  }
}

template<typename Executor, typename WalAllocator>
template<typename Allocator, typename MapAlloc, typename Handler>
void wal<Executor, WalAllocator>::async_recover_reader<Allocator, MapAlloc, Handler>::operator()(std::error_code ec, wal_entry opcode) {
  switch (state_) {
    default:
      assert(false);
      break;
    case state::decode_entry_opcode:
      if (ec) {
        fin_(ec);
        return;
      }

      switch (opcode) {
        default:
          fin_(wal_errc::bad_opcode);
          return;
        case wal_entry::end: // end. Report on completed wal.
          static_assert(std::is_empty_v<wal_record<wal_entry::end>>);
          state_ = state::decode_entry;
          (*this)(ec, wal_record<wal_entry::end>());
          break;
        case wal_entry::noop: // noop. Continue reading the next opcode.
          static_assert(std::is_empty_v<wal_record<wal_entry::noop>>);
          state_ = state::decode_entry;
          (*this)(ec, wal_record<wal_entry::noop>());
          break;
        case wal_entry::skip:
          state_ = state::decode_entry;
          wal_record<wal_entry::skip>::async_read(impl_->reader, std::move(*this));
          break;
        case wal_entry::checkpoint: // checkpoint. All changes up to this point have been confirmed to be synced to their underlying files.
          static_assert(std::is_empty_v<wal_record<wal_entry::checkpoint>>);
          state_ = state::decode_entry;
          (*this)(ec, wal_record<wal_entry::checkpoint>());
          break;
        case wal_entry::resize:
          state_ = state::decode_entry;
          wal_record<wal_entry::resize>::async_read(impl_->reader, std::move(*this));
          break;
        case wal_entry::write:
          state_ = state::decode_entry;
          wal_record<wal_entry::write>::async_read(impl_->reader, std::move(*this));
          break;
        case wal_entry::write_many:
          state_ = state::decode_entry;
          wal_record<wal_entry::write_many>::async_read(impl_->reader, std::move(*this));
          break;
        case wal_entry::memset:
          state_ = state::decode_entry;
          wal_record<wal_entry::memset>::async_read(impl_->reader, std::move(*this));
          break;
      }
      break;
  }
}

template<typename Executor, typename WalAllocator>
template<typename Allocator, typename MapAlloc, typename Handler>
template<wal_entry Entry>
void wal<Executor, WalAllocator>::async_recover_reader<Allocator, MapAlloc, Handler>::operator()(std::error_code ec, const wal_record<Entry>& opcode) {
  switch (state_) {
    default:
      assert(false);
      break;
    case state::decode_entry:
      if (ec) {
        fin_(ec);
        return;
      }

      if constexpr(Entry == wal_entry::end) {
        fin_(std::error_code());
        return;
      } else if constexpr(Entry == wal_entry::noop) {
        // skip
      } else if constexpr(Entry == wal_entry::skip) {
        impl_->reader.advance(opcode.nbytes, ec);
        if (ec) {
          fin_(ec);
          return;
        }
      } else if constexpr(Entry == wal_entry::checkpoint) {
        // Reset the pending writes.
        impl_->rmap.clear();
        impl_->checkpoint_seen = true;
      } else if constexpr(Entry == wal_entry::resize) {
        // If the file grows, zero out the new memory.
        if (impl_->header.fsize < opcode.size) {
          auto ins = impl_->rmap.insert(impl_->header.fsize, opcode.size - impl_->header.fsize);
          std::memset(ins.second.data(), 0, ins.second.size());
        }

        impl_->header.fsize = opcode.size;
        impl_->rmap.truncate(opcode.size);
      } else if constexpr(Entry == wal_entry::write) {
        auto ins = impl_->rmap.insert(opcode.offset, opcode.nbytes);
        const auto peek_bytes = asio::buffer_copy(ins.second, impl_->reader.peek());
        ins.second += peek_bytes;
        const auto read_at_off = impl_->reader.offset() + peek_bytes;

        if (ins.second.size() > 0) {
          ++barrier_;
          async_read_at(impl_->reader.underlying_source(), read_at_off, ins.second,
              completion_wrapper<void(std::error_code, std::size_t)>(
                  barrier_,
                  [vptr=ins.first->shared_data()](auto handler, std::error_code ec, [[maybe_unused]] std::size_t bytes) {
                    handler(ec);
                  }));
        }

        impl_->reader.advance(opcode.skip_bytes(), ec);
        if (ec) {
          fin_(ec);
          return;
        }
      } else if constexpr(Entry == wal_entry::write_many) {
        std::shared_ptr<const void> vptr;
        asio::mutable_buffer buf;
        std::tie(vptr, buf) = impl_->rmap.insert_many(opcode.offsets.begin(), opcode.offsets.end(), opcode.nbytes);
        const auto peek_bytes = asio::buffer_copy(buf, impl_->reader.peek());
        buf += peek_bytes;
        const auto read_at_off = impl_->reader.offset() + peek_bytes;

        if (buf.size() > 0) {
          ++barrier_;
          async_read_at(impl_->reader.underlying_source(), read_at_off, buf,
              completion_wrapper<void(std::error_code, std::size_t)>(
                  barrier_,
                  [vptr](auto handler, std::error_code ec, [[maybe_unused]] std::size_t bytes) {
                    handler(ec);
                  }));
        }

        impl_->reader.advance(opcode.skip_bytes(), ec);
        if (ec) {
          fin_(ec);
          return;
        }
      } else if constexpr(Entry == wal_entry::memset) {
        asio::mutable_buffer buf;
        std::tie(std::ignore, buf) = impl_->rmap.insert(opcode.offset, opcode.nbytes);
        std::memset(buf.data(), opcode.fill_value(), buf.size());
      } else {
        fin_(wal_errc::bad_opcode);
        return;
      }

      // Continue reading.
      assert(state_ != state::invalid);
      state_ = state::decode_entry_opcode;
      async_wal_entry_read(impl_->reader, std::move(*this));
      break;
  }
}

template<typename Executor, typename WalAllocator>
template<typename Allocator, typename MapAlloc, typename Handler>
auto wal<Executor, WalAllocator>::async_recover_reader<Allocator, MapAlloc, Handler>::get_allocator() const -> allocator_type {
  // work around allocator<non-void> --> allocator<void> conversion being broken
  if constexpr(std::is_same_v<std::allocator<void>, allocator_type>)
    return allocator_type();
  else
    return impl_->reader.get_allocator();
}

template<typename Executor, typename WalAllocator>
template<typename Allocator, typename MapAlloc, typename Handler>
void wal<Executor, WalAllocator>::async_recover_reader<Allocator, MapAlloc, Handler>::fin_(std::error_code ec) {
  state_ = state::invalid;
  barrier_(ec);
}


template<typename Executor, typename WalAllocator>
template<typename Allocator, typename MapAlloc, typename Handler>
wal<Executor, WalAllocator>::async_recover_reader<Allocator, MapAlloc, Handler>::barrier_handler_::barrier_handler_(Handler&& handler, std::shared_ptr<impl_type> impl, const wal::executor_type& ex)
: handler_(std::move(handler)),
  impl_(std::move(impl)),
  ex_(asio::associated_executor<Handler, wal::executor_type>::get(handler_, ex))
{}

template<typename Executor, typename WalAllocator>
template<typename Allocator, typename MapAlloc, typename Handler>
void wal<Executor, WalAllocator>::async_recover_reader<Allocator, MapAlloc, Handler>::barrier_handler_::operator()(std::error_code ec) {
  handler_(ec, impl_->header, std::move(impl_->rmap), impl_->wal_end_offset, impl_->checkpoint_seen);
}

template<typename Executor, typename WalAllocator>
template<typename Allocator, typename MapAlloc, typename Handler>
auto wal<Executor, WalAllocator>::async_recover_reader<Allocator, MapAlloc, Handler>::barrier_handler_::get_executor() const -> executor_type {
  return ex_;
}

template<typename Executor, typename WalAllocator>
template<typename Allocator, typename MapAlloc, typename Handler>
auto wal<Executor, WalAllocator>::async_recover_reader<Allocator, MapAlloc, Handler>::barrier_handler_::get_allocator() const -> allocator_type {
  return asio::associated_allocator<Handler>::get(handler_);
}


template class wal<>;


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_WAL2_INL_H */
