#include <earnest/detail/wal.h>
#include <algorithm>
#include <array>
#include <cassert>
#include <functional>
#include <iostream>
#include <iterator>
#include <tuple>
#include <unordered_map>
#include <objpipe/of.h>
#include <boost/endian/conversion.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/read_at.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/write_at.hpp>
#include <boost/asio/completion_condition.hpp>
#include <boost/system/system_error.hpp>
#include <earnest/fd.h>
#include <earnest/detail/buffered_read_stream_at.h>
#include <earnest/detail/buffered_write_stream_at.h>

namespace earnest::detail {
namespace {


template<typename DynBuffer>
auto dyn_grow(DynBuffer& buffer, std::size_t n)
-> std::enable_if_t<boost::asio::is_dynamic_buffer_v2<DynBuffer>::value , typename DynBuffer::mutable_buffers_type> {
  const auto off = buffer.size();
  buffer.grow(n);
  return buffer.data(off, n);
}


} /* namespace earnest::detail::<unnamed> */


struct wal_region::wal_header {
  static constexpr std::size_t XDR_SIZE = 12u;

  wal_header() noexcept = default;

  wal_header(std::uint32_t seq, std::uint64_t file_size) noexcept
  : file_size(file_size),
    seq(seq)
  {}

  void write(boost::asio::dynamic_string_buffer<char, std::char_traits<char>, std::allocator<char>> out) const {
    std::uint32_t enc_seq = seq;
    boost::endian::native_to_big_inplace(enc_seq);
    std::uint64_t enc_fsize = file_size;
    boost::endian::native_to_big_inplace(enc_fsize);

    boost::asio::buffer_copy(
        dyn_grow(out, XDR_SIZE),
        std::array<boost::asio::const_buffer, 2>{
          boost::asio::const_buffer(&enc_seq, sizeof(enc_seq)),
          boost::asio::const_buffer(&enc_fsize, sizeof(enc_fsize))
        });
  }

  static auto read(buffered_read_stream_at<class fd>& in) -> wal_header {
    std::uint32_t seq;
    std::uint64_t file_size;

    boost::asio::read(
        in,
        std::array<boost::asio::mutable_buffer, 2>{
          boost::asio::mutable_buffer(&seq, sizeof(seq)),
          boost::asio::mutable_buffer(&file_size, sizeof(file_size))
        },
        boost::asio::transfer_all());

    boost::endian::big_to_native_inplace(seq);
    boost::endian::big_to_native_inplace(file_size);
    return wal_header(seq, file_size);
  }

  std::uint64_t file_size = 0u;
  std::uint32_t seq = 0u;
};


class wal_record_end
: public wal_record
{
  public:
  static constexpr std::size_t XDR_SIZE = 4u;

  wal_record_end() noexcept
  : wal_record(0u)
  {}

  ~wal_record_end() noexcept override = default;
  auto get_wal_entry() const noexcept -> wal_entry override { return wal_entry::end; }
  private:
  auto do_write([[maybe_unused]] boost::asio::dynamic_string_buffer<char, std::char_traits<char>, std::allocator<char>> out) const -> void override {}
  auto do_apply([[maybe_unused]] wal_region& wal) const -> void override {}
};


class wal_record_commit
: public wal_record
{
  public:
  using wal_record::wal_record;

  ~wal_record_commit() noexcept override = default;
  auto get_wal_entry() const noexcept -> wal_entry override { return wal_entry::commit; }
  private:
  auto do_write([[maybe_unused]] boost::asio::dynamic_string_buffer<char, std::char_traits<char>, std::allocator<char>> out) const -> void override {}
  auto do_apply([[maybe_unused]] wal_region& wal) const -> void override {}
};


class wal_record_write
: public wal_record
{
  public:
  wal_record_write() = delete;

  wal_record_write(tx_id_type tx_id, std::uint64_t offset, std::vector<uint8_t> data)
  : wal_record(tx_id),
    offset(offset),
    data(std::move(data))
  {}

  wal_record_write(tx_id_type tx_id, std::uint64_t offset, const void* buf, std::size_t len)
  : wal_record_write(
      tx_id,
      offset,
      std::vector<std::uint8_t>(
          reinterpret_cast<const std::uint8_t*>(buf),
          reinterpret_cast<const std::uint8_t*>(buf) + len))
  {}

  ~wal_record_write() noexcept override = default;

  static auto from_stream(tx_id_type tx_id, buffered_read_stream_at<fd>& in)
  -> std::unique_ptr<wal_record_write> {
    std::uint64_t offset;
    std::uint32_t len;
    boost::asio::read(
        in,
        std::array<boost::asio::mutable_buffer, 2>{
          boost::asio::mutable_buffer(&offset, sizeof(offset)),
          boost::asio::mutable_buffer(&len, sizeof(len))
        },
        boost::asio::transfer_all());

    boost::endian::big_to_native_inplace(offset);
    boost::endian::big_to_native_inplace(len);

    std::vector<std::uint8_t> bytes;
    bytes.resize((len + 3u) / 4u * 4u); // Rounded up.
    boost::asio::read(
        in,
        boost::asio::mutable_buffer(bytes.data(), bytes.size()),
        boost::asio::transfer_all());
    bytes.resize(len);

    return std::make_unique<wal_record_write>(tx_id, offset, bytes);
  }

  auto get_wal_entry() const noexcept -> wal_entry override { return wal_entry::write; }

  static void to_stream(boost::asio::dynamic_string_buffer<char, std::char_traits<char>, std::allocator<char>> out, tx_id_type tx_id, std::uint64_t offset, const void* buf, std::size_t len) {
    wal_record::to_stream(out, wal_entry::write, tx_id);
    to_stream_internal_(out, offset, buf, len);
  }

  private:
  static auto to_stream_internal_(boost::asio::dynamic_string_buffer<char, std::char_traits<char>, std::allocator<char>> out, std::uint64_t offset, const void* buf, std::uint32_t len) -> void {
    boost::endian::native_to_big_inplace(offset);
    std::uint32_t enc_len = len;
    boost::endian::native_to_big_inplace(enc_len);

    // Padding for the byte vector.
    const std::array<char, 4> zpad{ '\0', '\0', '\0', '\0' };
    const auto zpad_len = (4u - (len % 4u)) % 4u;

    boost::asio::buffer_copy(
        dyn_grow(out, sizeof(offset) + sizeof(enc_len) + len + zpad_len),
        std::array<boost::asio::const_buffer, 4>{
          boost::asio::const_buffer(&offset, sizeof(offset)),
          boost::asio::const_buffer(&enc_len, sizeof(enc_len)),
          boost::asio::const_buffer(buf, len),
          boost::asio::const_buffer(&zpad, zpad_len)
        });
  }

  auto do_write(boost::asio::dynamic_string_buffer<char, std::char_traits<char>, std::allocator<char>> out) const -> void override {
    to_stream_internal_(std::move(out), offset, data.data(), data.size());
  }

  auto do_apply(wal_region& wal) const -> void override {
    wal.repl_.write_at(offset, data.data(), data.size()).commit();
  }

  std::uint64_t offset;
  std::vector<std::uint8_t> data;
};


class wal_record_resize
: public wal_record
{
  public:
  wal_record_resize() = delete;

  wal_record_resize(tx_id_type tx_id, std::uint64_t new_size)
  : wal_record(tx_id),
    new_size(new_size)
  {}

  ~wal_record_resize() noexcept override = default;

  static auto from_stream(tx_id_type tx_id, buffered_read_stream_at<fd>& in)
  -> std::unique_ptr<wal_record_resize> {
    std::uint64_t new_size;
    boost::asio::read(
        in,
        boost::asio::mutable_buffer(&new_size, sizeof(new_size)),
        boost::asio::transfer_all());

    boost::endian::big_to_native_inplace(new_size);
    return std::make_unique<wal_record_resize>(tx_id, new_size);
  }

  auto get_wal_entry() const noexcept -> wal_entry override { return wal_entry::resize; }

  private:
  auto do_write(boost::asio::dynamic_string_buffer<char, std::char_traits<char>, std::allocator<char>> out) const -> void override {
    std::uint64_t s = new_size;
    boost::endian::native_to_big_inplace(s);
    boost::asio::buffer_copy(
        dyn_grow(out, sizeof(s)),
        boost::asio::const_buffer(&s, sizeof(s)));
  }

  auto do_apply(wal_region& wal) const -> void override {
    wal.fd_size_ = new_size;
  }

  std::uint64_t new_size;
};


class wal_record_write_many
: public wal_record
{
  public:
  wal_record_write_many() = delete;

  wal_record_write_many(tx_id_type tx_id, std::vector<std::uint64_t> offsets, std::vector<uint8_t> data)
  : wal_record(tx_id),
    offsets(std::move(offsets)),
    data(std::move(data))
  {}

  wal_record_write_many(tx_id_type tx_id, std::vector<std::uint64_t> offsets, const void* buf, std::size_t len)
  : wal_record_write_many(
      tx_id,
      std::move(offsets),
      std::vector<std::uint8_t>(
          reinterpret_cast<const std::uint8_t*>(buf),
          reinterpret_cast<const std::uint8_t*>(buf) + len))
  {}

  ~wal_record_write_many() noexcept override = default;

  static auto from_stream(tx_id_type tx_id, buffered_read_stream_at<fd>& in)
  -> std::unique_ptr<wal_record_write_many> {
    // Decode array of offsets.
    std::uint32_t offsets_len;
    boost::asio::read(
        in,
        boost::asio::mutable_buffer(&offsets_len, sizeof(offsets_len)),
        boost::asio::transfer_all());

    std::vector<std::uint64_t> offsets;
    offsets.resize(offsets_len);
    boost::asio::read(
        in,
        boost::asio::mutable_buffer(offsets.data(), offsets.size() * sizeof(std::uint64_t)),
        boost::asio::transfer_all());
    std::for_each(offsets.begin(), offsets.end(),
        [](std::uint64_t v) {
          boost::endian::big_to_native_inplace(v);
        });

    // Decode opaque buffer.
    std::uint32_t len;
    boost::asio::read(
        in,
        boost::asio::mutable_buffer(&len, sizeof(len)),
        boost::asio::transfer_all());
    boost::endian::big_to_native_inplace(len);

    std::vector<std::uint8_t> bytes;
    bytes.resize((len + 3u) / 4u * 4u); // Rounded up.
    boost::asio::read(
        in,
        boost::asio::mutable_buffer(bytes.data(), bytes.size()),
        boost::asio::transfer_all());
    bytes.resize(len);

    return std::make_unique<wal_record_write_many>(tx_id, std::move(offsets), std::move(bytes));
  }

  auto get_wal_entry() const noexcept -> wal_entry override { return wal_entry::write_many; }

  template<typename OffsetType>
  static void to_stream(boost::asio::dynamic_string_buffer<char, std::char_traits<char>, std::allocator<char>> out, tx_id_type tx_id, const std::vector<OffsetType>& offsets, const void* buf, std::size_t len) {
    wal_record::to_stream(out, wal_entry::write_many, tx_id);
    to_stream_internal_(out, offsets, buf, len);
  }

  private:
  template<typename OffsetType>
  static auto to_stream_internal_(boost::asio::dynamic_string_buffer<char, std::char_traits<char>, std::allocator<char>> out, const std::vector<OffsetType>& offsets, const void* buf, std::size_t len) -> void {
    std::uint32_t clen = offsets.size();
    boost::endian::native_to_big_inplace(clen);
    boost::asio::buffer_copy(
        dyn_grow(out, sizeof(clen)),
        boost::asio::const_buffer(&clen, sizeof(clen)));

    std::for_each(offsets.begin(), offsets.end(),
        [&out](std::uint64_t offset) {
          boost::endian::native_to_big_inplace(offset);
          boost::asio::buffer_copy(
              dyn_grow(out, sizeof(offset)),
              boost::asio::const_buffer(&offset, sizeof(offset)));
        });

    const std::array<char, 4> zpad{ '\0', '\0', '\0', '\0' };
    const auto zpad_len = (4u - (len % 4u)) % 4u;

    boost::asio::buffer_copy(
        dyn_grow(out, len + zpad_len),
        std::array<boost::asio::const_buffer, 2>{
          boost::asio::buffer(buf, len),
          boost::asio::buffer(zpad, zpad_len)
        });
  }

  auto do_write(boost::asio::dynamic_string_buffer<char, std::char_traits<char>, std::allocator<char>> out) const -> void override {
    to_stream_internal_(std::move(out), offsets, data.data(), data.size());
  }

  auto do_apply(wal_region& wal) const -> void override {
    for (const auto& offset : offsets)
      wal.repl_.write_at(offset, data.data(), data.size()).commit();
  }

  std::vector<std::uint64_t> offsets;
  std::vector<std::uint8_t> data;
};


wal_error::~wal_error() = default;

wal_bad_alloc::~wal_bad_alloc() = default;


wal_record::wal_record(tx_id_type tx_id)
: tx_id_(tx_id)
{
  if ((tx_id & tx_id_mask) != tx_id)
    throw std::logic_error("tx_id out of range (only 24 bit expected)");
}

wal_record::~wal_record() noexcept = default;

auto wal_record::read(buffered_read_stream_at<fd>& in) -> std::unique_ptr<wal_record> {
  std::uint32_t discriminant;
  boost::asio::read(
      in,
      boost::asio::mutable_buffer(&discriminant, sizeof(discriminant)),
      boost::asio::transfer_all());
  boost::endian::big_to_native_inplace(discriminant);

  std::unique_ptr<wal_record> result;
  const tx_id_type tx_id = discriminant >> 8;

  switch (discriminant & 0xffU) {
    case static_cast<std::uint8_t>(wal_entry::end):
      if (tx_id != 0u) throw wal_error("unrecognized WAL entry");
      result = std::make_unique<wal_record_end>();
      break;
    case static_cast<std::uint8_t>(wal_entry::commit):
      result = std::make_unique<wal_record_commit>(tx_id);
      break;
    case static_cast<std::uint8_t>(wal_entry::write):
      result = wal_record_write::from_stream(tx_id, in);
      break;
    case static_cast<std::uint8_t>(wal_entry::resize):
      result = wal_record_resize::from_stream(tx_id, in);
      break;
    case static_cast<std::uint8_t>(wal_entry::write_many):
      result = wal_record_write_many::from_stream(tx_id, in);
      break;
    default:
      throw wal_error("unrecognized WAL entry");
  }

  assert(result != nullptr
      && static_cast<std::uint32_t>(result->get_wal_entry()) == (discriminant & 0xffU));
  return result;
}

auto wal_record::write(boost::asio::dynamic_string_buffer<char, std::char_traits<char>, std::allocator<char>> out) const -> void {
  assert((tx_id() & tx_id_mask) == tx_id());
  to_stream(out, get_wal_entry(), tx_id());
  do_write(std::move(out));
}

auto wal_record::to_stream(boost::asio::dynamic_string_buffer<char, std::char_traits<char>, std::allocator<char>> out, wal_entry e, tx_id_type tx_id) -> void {
  std::uint32_t i = (static_cast<std::uint32_t>(e) | (tx_id << 8));
  boost::endian::native_to_big_inplace(i);
  boost::asio::buffer_copy(dyn_grow(out, sizeof(i)), boost::asio::const_buffer(&i, sizeof(i)));
}

void wal_record::apply(wal_region& wal) const {
  do_apply(wal);
}

auto wal_record::is_end() const noexcept -> bool {
  return get_wal_entry() == wal_entry::end;
}

auto wal_record::is_commit() const noexcept -> bool {
  return get_wal_entry() == wal_entry::commit;
}

auto wal_record::is_control_record() const noexcept -> bool {
  const auto e = get_wal_entry();
  return e == wal_entry::end;
}

auto wal_record::make_end() -> std::unique_ptr<wal_record> {
  return std::make_unique<wal_record_end>();
}

auto wal_record::make_commit(tx_id_type tx_id) -> std::unique_ptr<wal_record> {
  return std::make_unique<wal_record_commit>(tx_id);
}

auto wal_record::make_write(tx_id_type tx_id, std::uint64_t offset, std::vector<uint8_t>&& data) -> std::unique_ptr<wal_record> {
  return std::make_unique<wal_record_write>(tx_id, offset, std::move(data));
}

auto wal_record::make_write(tx_id_type tx_id, std::uint64_t offset, const std::vector<uint8_t>& data) -> std::unique_ptr<wal_record> {
  return std::make_unique<wal_record_write>(tx_id, offset, data);
}

auto wal_record::make_write_many(tx_id_type tx_id, const std::vector<std::uint64_t>& offsets, std::vector<uint8_t>&& data) -> std::unique_ptr<wal_record> {
  if (offsets.size() == 1) return make_write(tx_id, offsets[0], std::move(data));
  return std::make_unique<wal_record_write_many>(tx_id, offsets, std::move(data));
}

auto wal_record::make_write_many(tx_id_type tx_id, const std::vector<std::uint64_t>& offsets, const std::vector<uint8_t>& data) -> std::unique_ptr<wal_record> {
  if (offsets.size() == 1) return make_write(tx_id, offsets[0], data);
  return std::make_unique<wal_record_write_many>(tx_id, offsets, data);
}

auto wal_record::make_write_many(tx_id_type tx_id, std::vector<std::uint64_t>&& offsets, std::vector<uint8_t>&& data) -> std::unique_ptr<wal_record> {
  if (offsets.size() == 1) return make_write(tx_id, offsets[0], std::move(data));
  return std::make_unique<wal_record_write_many>(tx_id, std::move(offsets), std::move(data));
}

auto wal_record::make_write_many(tx_id_type tx_id, std::vector<std::uint64_t>&& offsets, const std::vector<uint8_t>& data) -> std::unique_ptr<wal_record> {
  if (offsets.size() == 1) return make_write(tx_id, offsets[0], data);
  return std::make_unique<wal_record_write_many>(tx_id, std::move(offsets), data);
}

auto wal_record::make_resize(tx_id_type tx_id, std::uint64_t new_size) -> std::unique_ptr<wal_record> {
  return std::make_unique<wal_record_resize>(tx_id, new_size);
}


wal_region::wal_region(std::string name, class fd&& file, fd::offset_type off, fd::size_type len, allocator_type alloc)
: off_(off),
  len_(len),
  tx_id_states_(alloc),
  tx_id_avail_(alloc),
  fd_(std::move(file)),
  repl_(alloc),
  commit_count_("monsoon.wal.commits", {{"name", name}}),
  write_ops_("monsoon.wal.writes", {{"name", name}}),
  compactions_("monsoon.wal.compactions", {{"name", name}}),
  file_flush_("monsoon.wal.file_flush", {{"name", name}})
{
  static_assert(num_segments_ == 2u, "Algorithm assumes two segments.");
  std::array<std::tuple<std::size_t, wal_header>, 2> segments{
    std::make_tuple(0u, read_segment_header_(0)),
    std::make_tuple(1u, read_segment_header_(1))
  };
  std::sort(
      segments.begin(),
      segments.end(),
      [](const auto& x, const auto& y) -> bool {
        // Use a sliding window for sequencing distance.
        // Note: y.seq - x.seq may wrap-around, the comparison is based on that.
        return std::get<wal_header>(y).seq - std::get<wal_header>(x).seq <= 0x7fffffffu;
      });

  current_slot_ = std::get<std::size_t>(segments[0]);
  const auto old_slot = 1u - current_slot_;
  const auto old_data = read_segment_(old_slot);
  fd_size_ = old_data.file_size;
  current_seq_ = old_data.seq + 1u;

  // In-memory application of the WAL log.
  // Note that if the WAL log is bad, this will throw.
  {
    std::unordered_map<wal_record::tx_id_type, std::vector<const wal_record*>> records_by_tx;
    objpipe::of(std::cref(old_data.data))
        .iterate()
        .filter(
            [](const auto& record_ptr) -> bool {
              return !record_ptr->is_control_record();
            })
        .transform(
            [&records_by_tx](const auto& ptr) {
              std::vector<const wal_record*> result;
              if (ptr->is_commit()) {
                result.swap(records_by_tx[ptr->tx_id()]);
              } else {
                records_by_tx[ptr->tx_id()].push_back(ptr.get());
              }
              return result;
            })
        .iterate()
        .deref()
        .for_each(
            [this](const wal_record& record) {
              record.apply(*this);
            });
  }

  // Recover the WAL log onto disk.
  {
    // Write all pending writes.
    for (const auto& w : repl_) {
      boost::asio::write_at(
          fd_,
          w.begin_offset() + wal_end_offset(),
          boost::asio::const_buffer(w.data(), w.size()),
          boost::asio::transfer_all());
    }
    repl_.clear();
    fd_.truncate(fd::size_type(wal_end_offset()) + fd_size_);
    fd_.flush(true);
    ++file_flush_;

    // Start a new segment.
    std::string xdr;
    wal_header(current_seq_, fd_size_).write(boost::asio::dynamic_buffer(xdr));
    slot_off_ = slot_begin_off(current_slot_) + xdr.size();
    wal_record_end().write(boost::asio::dynamic_buffer(xdr));
    assert(xdr.size() <= segment_len_());
    boost::asio::write_at(
        fd_,
        slot_begin_off(current_slot_),
        boost::asio::buffer(xdr),
        boost::asio::transfer_all());

    // Flush data onto disk.
    fd_.flush(true);
    ++file_flush_;
  }
}

wal_region::wal_region(std::string name, [[maybe_unused]] create c, class fd&& file, fd::offset_type off, fd::size_type len, allocator_type alloc)
: off_(off),
  len_(len),
  current_seq_(0),
  current_slot_(0),
  tx_id_states_(alloc),
  tx_id_avail_(alloc),
  fd_(std::move(file)),
  fd_size_(0),
  repl_(alloc),
  commit_count_("monsoon.wal.commits", {{"name", name}}),
  write_ops_("monsoon.wal.writes", {{"name", name}}),
  compactions_("monsoon.wal.compactions", {{"name", name}}),
  file_flush_("monsoon.wal.file_flush", {{"name", name}})
{
  if (fd_.size() < wal_end_offset()) fd_.truncate(wal_end_offset());

  const auto other_slot = 1u - current_slot_;

  {
    std::string xdr;
    wal_header(0, 0).write(boost::asio::dynamic_buffer(xdr));
    slot_off_ = slot_begin_off(current_slot_) + xdr.size();
    wal_record_end().write(boost::asio::dynamic_buffer(xdr));
    assert(xdr.size() <= segment_len_(len_));
    boost::asio::write_at(
        fd_,
        slot_begin_off(current_slot_),
        boost::asio::buffer(xdr),
        boost::asio::transfer_all());
  }

  {
    std::string xdr;
    wal_header(std::uint32_t(0) - 1u, 0).write(boost::asio::dynamic_buffer(xdr));
    wal_record_end().write(boost::asio::dynamic_buffer(xdr));
    assert(xdr.size() <= segment_len_(len_));
    boost::asio::write_at(
        fd_,
        slot_begin_off(other_slot),
        boost::asio::buffer(xdr),
        boost::asio::transfer_all());
  }

  // While we only require a dataflush, we do a full flush to get the
  // file metadata synced up. Because it seems like a nice thing to do.
  fd_.flush();
  ++file_flush_;
}

auto wal_region::allocate_tx_id() -> wal_record::tx_id_type {
  std::unique_lock<std::mutex> lck{ alloc_mtx_ };

  // First, ensure there is space to allocate a transaction ID.
  while (tx_id_avail_.empty() && tx_id_states_.size() > wal_record::tx_id_mask) [[unlikely]] {
    // Check if there is even room to be created by compacting.
    if (tx_id_completed_count_ == 0) [[unlikely]]
      throw wal_bad_alloc("Ran out of WAL transaction IDs.");

    // Compact the WAL log by replaying it.
    // We must release the lock temporarily, hence why this part is in a loop.
    lck.unlock();
    std::lock_guard<std::mutex> log_lck{ log_mtx_ };
    compact_();
    lck.lock();
  }

  // First recycle used IDs.
  if (!tx_id_avail_.empty()) {
    const auto tx_id = tx_id_avail_.top();
    assert(tx_id < tx_id_states_.size());
    assert((tx_id & wal_record::tx_id_mask) == tx_id);
    tx_id_states_[tx_id] = true;
    tx_id_avail_.pop();
    return tx_id;
  }

  // Only allocate a new ID if there are none for recycling.
  const wal_record::tx_id_type tx_id = tx_id_states_.size();
  assert((tx_id & wal_record::tx_id_mask) == tx_id);
  tx_id_states_.push_back(true);
  return tx_id;
}

auto wal_region::read_segment_header_(std::size_t idx) -> wal_header {
  assert(idx < num_segments_);

  auto xdr_stream =
      buffered_read_stream_at<class fd>(fd_, slot_begin_off(idx), segment_len_());
  return wal_header::read(xdr_stream);
}

auto wal_region::read_segment_(std::size_t idx) -> wal_vector {
  assert(idx < num_segments_);

  auto xdr_stream =
      buffered_read_stream_at<class fd>(fd_, slot_begin_off(idx), segment_len_());
  const auto header = wal_header::read(xdr_stream);

  wal_vector result;
  result.slot = idx;
  result.seq = header.seq;
  result.file_size = header.file_size;
  do {
    result.data.push_back(wal_record::read(xdr_stream));
  } while (!result.data.back()->is_end());

  return result;
}

auto wal_region::read_at(fd::offset_type off, void* buf, std::size_t len) -> std::size_t {
  std::shared_lock<std::shared_mutex> lck{ mtx_ };
  return read_at_(off, buf, len);
}

void wal_region::compact() {
  std::lock_guard<std::mutex> lck{ log_mtx_ };
  compact_();
}

auto wal_region::size() const noexcept -> fd::size_type {
  std::shared_lock<std::shared_mutex> lck{ mtx_ };
  return fd_size_;
}

auto wal_region::read_at_(fd::offset_type off, void* buf, std::size_t len) -> std::size_t {
  boost::system::error_code ec;
  std::size_t rlen = read_at_(off, buf, len, ec);
  if (ec) throw boost::system::system_error(ec, "read");
  return rlen;
}

auto wal_region::read_at_(fd::offset_type off, void* buf, std::size_t len, boost::system::error_code& ec) -> std::size_t {
  // Reads past the logic end of the file will fail.
  if (off >= fd_size_) {
    ec = boost::asio::stream_errc::eof;
    return 0;
  }
  // Clamp len, so we won't perform reads past-the-end.
  if (len > fd_size_ - off) len = fd_size_ - off;
  // Zero length reads are very easy.
  if (len == 0u) return 0u;

  // Try to read from the list of pending writes.
  const auto repl_rlen = repl_.read_at(off, buf, len); // May modify len.
  if (repl_rlen != 0u) return repl_rlen;
  assert(len != 0u);

  // We have to fall back to the file.
  const auto read_rlen = boost::asio::read_at(fd_, off + wal_end_offset(), boost::asio::mutable_buffer(buf, len));
  if (read_rlen != 0u) [[likely]] return read_rlen;
  assert(len != 0u);

  // If the file-read failed, it means the file is really smaller.
  // Pretend the file is zero-filled.
  assert(off + wal_end_offset() >= fd_.size());
  std::fill_n(reinterpret_cast<std::uint8_t*>(buf), len, std::uint8_t(0u));
  return len;
}

void wal_region::log_write_(const wal_record& r) {
  assert(!r.is_commit()); // Commit is special cased.

  std::string xdr;
  r.write(boost::asio::dynamic_buffer(xdr));
  assert(xdr.size() >= wal_record_end::XDR_SIZE);
  wal_record_end().write(boost::asio::dynamic_buffer(xdr));

  log_write_raw_(xdr);
}

void wal_region::log_write_raw_(std::string_view xdr) {
  std::lock_guard<std::mutex> lck{ log_mtx_ };

  assert(slot_begin_off(current_slot_) <= slot_off_ && slot_off_ < slot_end_off(current_slot_));

#ifndef NDEBUG // Assert that the slot offset contains a record-end-marker.
  {
    assert(slot_end_off(current_slot_) - slot_off_ >= wal_record_end::XDR_SIZE);

    auto xdr_read = buffered_read_stream_at<class fd>(fd_, slot_off_, wal_record_end::XDR_SIZE);
    const auto last_record = wal_record::read(xdr_read);
    assert(last_record->is_end());
  }
#endif

  // Run a compaction cycle if the log has insuficient space for the new data.
  if (slot_end_off(current_slot_) - slot_off_ < xdr.size()) {
    compact_();

    if (slot_end_off(current_slot_) - slot_off_ < xdr.size())
      throw wal_bad_alloc("no space in WAL");
  }

  // Perform write-ahead of the record.
  boost::asio::write_at(
      fd_,
      slot_off_ + wal_record_end::XDR_SIZE,
      boost::asio::buffer(xdr) + wal_record_end::XDR_SIZE,
      boost::asio::transfer_all());
  fd_.flush(true);
  ++file_flush_;

  // Write the marker of the record.
  boost::asio::write_at(
      fd_,
      slot_off_,
      boost::asio::buffer(xdr, wal_record_end::XDR_SIZE),
      boost::asio::transfer_all());

  // Advance slot offset.
  slot_off_ += xdr.size() - wal_record_end::XDR_SIZE;

  ++write_ops_;
}

void wal_region::compact_() {
  {
    // Don't run a compaction if we know we won't free up any information.
    std::lock_guard<std::mutex> alloc_lck{ alloc_mtx_ };
    if (tx_id_completed_count_ == 0u) return;
  }

  ++compactions_;
  std::string wal_segment_header;
  wal_header(current_seq_ + 1u, fd_size_).write(boost::asio::dynamic_buffer(wal_segment_header));

  const auto new_slot = 1u - current_slot_;
  buffered_write_stream_at<class fd> xdr(
      fd_,
      slot_begin_off(new_slot) + wal_segment_header.size(),
      slot_end_off(new_slot) - slot_begin_off(new_slot) - wal_segment_header.size());

  // Copy all information for in-progress transactions.
  objpipe::of(std::move(read_segment_(current_slot_).data))
      .iterate()
      .deref()
      .filter( // Skip control records.
          [](const auto& record) -> bool {
            return !record.is_control_record();
          })
      .filter( // Only valid transaction IDs.
          [this](const auto& record) -> bool {
            return tx_id_states_.size() > record.tx_id();
          })
      .filter( // Only copy in-progress transactions.
          [this](const auto& record) -> bool {
            return tx_id_states_[record.tx_id()];
          })
      .transform(
          [](const auto& record) -> std::string {
            std::string xdr;
            record.write(boost::asio::dynamic_buffer(xdr));
            return xdr;
          })
      .for_each(
          [&xdr](const auto& buffer) {
            boost::asio::write(xdr, boost::asio::buffer(buffer), boost::asio::transfer_all());
          });
  // Record the end-of-log offset.
  const auto new_slot_off = xdr.offset();
  // And write an end-of-log record.
  {
    std::string local_xdr;
    wal_record_end().write(boost::asio::dynamic_buffer(local_xdr));
    boost::asio::write(xdr, boost::asio::buffer(local_xdr), boost::asio::transfer_all());
  }
  xdr.complete();

  // Apply the replacement map.
  {
    std::lock_guard<std::shared_mutex> lck{ mtx_ };
    for (const auto& r : repl_) {
      boost::asio::write_at(
          fd_,
          r.begin_offset() + wal_end_offset(),
          boost::asio::buffer(r.data(), r.size()),
          boost::asio::transfer_all());
    }
    // Ensure all data is on disk, before activating the segment.
    fd_.flush(true);
    ++file_flush_;
    // Now that the replacement map is written out, we can clear it.
    repl_.clear();
  }

  // Now that all the writes from the old segment have been applied,
  // and all active transaction information has been copied,
  // we can finally activate this new segment.
  //
  // Note that we don't flush after this write.
  // We don't have to, as we know that all information in this log
  // is the same as the application of the previous log on top of
  // the (now updated) file contents.
  // Until a new commit happens, both logs are equivalent.
  boost::asio::write_at(fd_, slot_begin_off(new_slot), boost::asio::buffer(wal_segment_header), boost::asio::transfer_all());

  {
    std::lock_guard<std::mutex> alloc_lck{ alloc_mtx_ };
    // Update tx_id allocation state.
    while (!tx_id_states_.empty() && !tx_id_states_.back())
      tx_id_states_.pop_back();
    while (!tx_id_avail_.empty()) tx_id_avail_.pop(); // tx_id_avail_ lacks a clear() method.
    for (wal_record::tx_id_type tx_id = 0u; tx_id < tx_id_states_.size() && tx_id <= wal_record::tx_id_mask; ++tx_id) {
      if (!tx_id_states_[tx_id]) {
        try {
          tx_id_avail_.push(tx_id);
        } catch (const std::bad_alloc&) {
          // SKIP: ignore this exception
        }
      }
    }
  }

  // Update segment information.
  current_slot_ = new_slot;
  slot_off_ = new_slot_off;
  ++current_seq_;
  tx_id_completed_count_ = 0;
}

void wal_region::tx_write_(wal_record::tx_id_type tx_id, fd::offset_type off, const void* buf, std::size_t len) {
  std::string xdr;
  wal_record_write::to_stream(boost::asio::dynamic_buffer(xdr), tx_id, off, buf, len);
  assert(xdr.size() >= wal_record_end::XDR_SIZE);
  wal_record_end().write(boost::asio::dynamic_buffer(xdr));

  log_write_raw_(xdr);
}

void wal_region::tx_write_many_(wal_record::tx_id_type tx_id, const std::vector<fd::offset_type>& offs, const void* buf, std::size_t len) {
  if (offs.size() == 1) {
    tx_write_(tx_id, offs[0], buf, len);
  } else {
    std::string xdr;
    wal_record_write_many::to_stream(boost::asio::dynamic_buffer(xdr), tx_id, offs, buf, len);
    assert(xdr.size() >= wal_record_end::XDR_SIZE);
    wal_record_end().write(boost::asio::dynamic_buffer(xdr));

    log_write_raw_(xdr);
  }
}

void wal_region::tx_resize_(wal_record::tx_id_type tx_id, fd::size_type new_size) {
  if (new_size > std::numeric_limits<std::uint64_t>::max())
    throw std::overflow_error("wal_region::tx::resize");

  log_write_(wal_record_resize(tx_id, new_size));
}

void wal_region::tx_commit_(wal_record::tx_id_type tx_id, replacement_map&& writes, std::optional<fd::size_type> new_file_size, std::function<void(replacement_map)> undo_op_fn, tx_op_collection& ops) {
  // Create record of the commit.
  std::string xdr;
  wal_record_commit(tx_id).write(boost::asio::dynamic_buffer(xdr));
  assert(xdr.size() >= wal_record_end::XDR_SIZE);
  wal_record_end().write(boost::asio::dynamic_buffer(xdr));

  // Grab the WAL lock.
  std::lock_guard<std::mutex> log_lck{ log_mtx_ };
  assert(slot_begin_off(current_slot_) <= slot_off_ && slot_off_ < slot_end_off(current_slot_));

#ifndef NDEBUG // Assert that the slot offset contains a record-end-marker.
  {
    assert(slot_end_off(current_slot_) - slot_off_ >= wal_record_end::XDR_SIZE);

    auto xdr_read = buffered_read_stream_at(fd_, slot_off_, wal_record_end::XDR_SIZE);
    const auto last_record = wal_record::read(xdr_read);
    assert(last_record->is_end());
  }
#endif

  // Run a compaction cycle if the log has insuficient space for the new data.
  if (slot_end_off(current_slot_) - slot_off_ < xdr.size()) {
    compact_();

    if (slot_end_off(current_slot_) - slot_off_ < xdr.size())
      throw wal_bad_alloc("no space in WAL");
  }

  // Grab the lock that protects against non-wal changes.
  std::lock_guard<std::shared_mutex> lck{ mtx_ };

  // Prepare a merging of the transactions in repl.
  // We must prepare this after acquiring the mutex mtx_,
  // but before the WAL record is written.
  // We write into a copy of repl that we can swap later.
  replacement_map new_repl = repl_;
  for (const auto& w : writes)
    new_repl.write_at(w.begin_offset(), w.data(), w.size()).commit();

  // Prepare the undo map.
  // This map holds all data overwritten by this transaction.
  replacement_map undo;
  for (const auto& w : writes) {
    const std::unique_ptr<std::uint8_t[]> buf = std::make_unique<std::uint8_t[]>(w.size());

    auto off = w.begin_offset();
    while (off < w.end_offset()) {
      std::size_t len = w.end_offset() - off;
      assert(len <= w.size());

      const auto rlen = repl_.read_at(off, buf.get(), len);
      if (rlen != 0u) {
        undo.write_at(off, buf.get(), rlen).commit();
      } else if (off >= fd_size_) {
        std::fill_n(buf.get(), len, std::uint8_t(0));
        undo.write_at(off, buf.get(), len).commit();
      } else {
        if (len > fd_size_ - off) len = fd_size_ - off;
        undo.write_at_from_file(off, fd_, off + wal_end_offset(), len).commit();
      }
      off += len;
    }
  }

  // Write everything but the record header.
  // By not writing the record header, the transaction itself looks as if
  // the commit hasn't happened, because there's a wal_record_end message.
  boost::asio::write_at(
      fd_,
      slot_off_ + wal_record_end::XDR_SIZE,
      boost::asio::buffer(xdr) + wal_record_end::XDR_SIZE,
      boost::asio::transfer_all());
  fd_.flush(true);
  ++file_flush_;

  // Grab the allocation lock.
  std::lock_guard<std::mutex> alloc_lck{ alloc_mtx_ };
  assert(tx_id < tx_id_states_.size());
  assert(tx_id_states_[tx_id]);

  // Write the marker of the record.
  boost::asio::write_at(
      fd_,
      slot_off_,
      boost::asio::buffer(xdr, wal_record_end::XDR_SIZE),
      boost::asio::transfer_all());

  // If this flush fails, we can't recover.
  // The commit has been written in full and any attempt to undo it
  // would likely run into the same error as the flush operation.
  // So we'll log it and silently continue.
  //
  // While we only require a dataflush, we do a full flush to get the
  // file metadata synced up. Because it seems like a nice thing to do.
  try {
    fd_.flush();
    ++file_flush_;
  } catch (const std::exception& e) {
    std::cerr << "Warning: failed to flush WAL log: " << e.what() << std::endl;
  } catch (...) {
    std::cerr << "Warning: failed to flush WAL log." << std::endl;
  }

  // Now commit the change in repl_.
  swap(repl_, new_repl); // Never throws.
  // And update the tx_id_states_.
  tx_id_states_[tx_id] = false;
  ++tx_id_completed_count_;
  // Update the file size.
  if (new_file_size.has_value()) {
    fd_size_ = *new_file_size;
    repl_.truncate(fd_size_);
  }

  // Advance slot offset.
  slot_off_ += xdr.size() - wal_record_end::XDR_SIZE;

  undo_op_fn(std::move(undo));
  static_assert(noexcept(ops.commit()), "ops on-commit handlers may not throw");
  ops.commit();

  ++commit_count_;
}

void wal_region::tx_rollback_(wal_record::tx_id_type tx_id) noexcept {
  std::lock_guard<std::mutex> alloc_lck{ alloc_mtx_ };
  assert(tx_id < tx_id_states_.size());
  assert(tx_id_states_[tx_id]);

  tx_id_states_[tx_id] = false;
  ++tx_id_completed_count_;
}


wal_region::tx::tx(const std::shared_ptr<wal_region>& wal)
: tx(wal, (wal != nullptr ? allocator_type(wal->get_allocator()) : allocator_type()))
{}

wal_region::tx::tx(const std::shared_ptr<wal_region>& wal, allocator_type alloc)
: wal_(wal),
  writes_(alloc),
  ops_(alloc)
{
  if (wal != nullptr)
    tx_id_ = wal->allocate_tx_id();
}

wal_region::tx::~tx() noexcept {
  rollback();
}

wal_region::tx::operator bool() const noexcept {
  return wal_.lock() != nullptr;
}

void wal_region::tx::write_at(fd::offset_type off, const void* buf, std::size_t len) {
  const auto file_size = size();
  if (off > file_size || file_size - fd::size_type(off) < len)
    throw std::length_error("write past end of file (based on local transaction resize)");

  auto writes_tx = writes_.write_at(off, buf, len);
  std::shared_ptr<wal_region>(wal_)->tx_write_(tx_id_, off, buf, len);
  writes_tx.commit();
}

void wal_region::tx::write_at(std::vector<fd::offset_type> offs, const void* buf, std::size_t len) {
  if (offs.empty()) return;

  std::sort(offs.begin(), offs.end());
  for (auto off_iter = std::next(offs.begin()); off_iter != offs.end(); ++off_iter) {
    if (*std::prev(off_iter) > *off_iter - len)
      throw std::length_error("overlapped write");
  }

  const auto file_size = size();
  replacement_map tmp = writes_;

  for (const auto& off : offs) {
    if (off > file_size || file_size - fd::size_type(off) < len)
      throw std::length_error("write past end of file (based on local transaction resize)");

    tmp.write_at(off, buf, len).commit();
  }

  std::shared_ptr<wal_region>(wal_)->tx_write_many_(tx_id_, std::move(offs), buf, len);
  writes_ = std::move(tmp); // never throws
}

void wal_region::tx::write_at(std::initializer_list<fd::offset_type> offs, const void* buf, std::size_t len) {
  return write_at(std::vector<fd::offset_type>(offs), buf, len);
}

void wal_region::tx::resize(fd::size_type new_size) {
  std::shared_ptr<wal_region>(wal_)->tx_resize_(tx_id_, new_size);
  new_file_size_.emplace(new_size);
}

void wal_region::tx::commit(std::function<void(replacement_map)> undo_op_fn) {
  std::shared_ptr<wal_region>(wal_)->tx_commit_(tx_id_, std::move(writes_), new_file_size_, std::move(undo_op_fn), ops_);
  wal_.reset();
}

void wal_region::tx::commit() {
  commit([]([[maybe_unused]] replacement_map discard) {});
}

void wal_region::tx::rollback() noexcept {
  const auto wal = wal_.lock();
  if (wal != nullptr) wal->tx_rollback_(tx_id_);
  ops_.rollback(); // Never throws.
  wal_.reset();
}

auto wal_region::tx::read_at(fd::offset_type off, void* buf, std::size_t len) const -> std::size_t {
  boost::system::error_code ec;
  std::size_t rlen = read_at(off, buf, len, ec);
  if (ec) throw boost::system::system_error(ec);
  return rlen;
}

auto wal_region::tx::read_at(fd::offset_type off, void* buf, std::size_t len, boost::system::error_code& ec) const -> std::size_t {
  return read_at(
      off, buf, len,
      []([[maybe_unused]] const auto& off, [[maybe_unused]] const auto& buf, [[maybe_unused]] const auto& len) -> std::size_t {
        return 0u;
      },
      ec);
}

auto wal_region::tx::size() const -> fd::size_type {
  if (new_file_size_.has_value()) return *new_file_size_;
  return std::shared_ptr<wal_region>(wal_)->size();
}


} /* namespace earnest::detail */
