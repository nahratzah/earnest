#ifndef EARNEST_DETAIL_BUFFERED_WRITE_STREAM_AT_H
#define EARNEST_DETAIL_BUFFERED_WRITE_STREAM_AT_H

#include <cstddef>
#include <utility>
#include <memory>
#include <limits>
#include <boost/asio/buffer.hpp>
#include <boost/asio/write_at.hpp>
#include <boost/asio/error.hpp>
#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>

namespace earnest::detail {


template<typename Sink, typename Alloc = std::allocator<void>>
class buffered_write_stream_at {
  private:
  using buf_alloc = typename std::allocator_traits<Alloc>::template rebind_alloc<char>;

  public:
  buffered_write_stream_at() = default;

  buffered_write_stream_at(const buffered_write_stream_at&) = delete;
  buffered_write_stream_at& operator=(const buffered_write_stream_at&) = delete;

  buffered_write_stream_at(buffered_write_stream_at&& o) noexcept
  : snk_(std::exchange(o.snk_)),
    off_(o.off_),
    avl_(o.avl_),
    buf_(std::move(o.buf_)),
    cap_(o.cap_)
  {}

  buffered_write_stream_at& operator=(buffered_write_stream_at&& o) noexcept {
    using std::swap;

    swap(snk_, o.snk_);
    swap(off_, o.off_);
    swap(avl_, o.avl_);
    swap(buf_, o.buf_);
    swap(cap_, o.cap_);

    return *this;
  }

  buffered_write_stream_at(Sink& snk, std::uint64_t off, std::uint64_t len, std::size_t bufsiz = 64 * 1024)
  : snk_(&snk),
    off_(off),
    avl_(len),
    cap_(bufsiz)
  {}

  template<typename MB>
  auto write_some(MB&& mb) -> std::size_t {
    boost::system::error_code ec;
    std::size_t wlen = write_some(std::forward<MB>(mb), ec);
    if (ec) throw boost::system::system_error(ec, "write");
    return wlen;
  }

  template<typename MB>
  auto write_some(MB&& mb, boost::system::error_code& ec) -> std::size_t {
    using boost::asio::buffer_size;

    ec.clear();
    const auto buffer_size_mb = buffer_size(mb);
    if (buffer_size_mb == 0) return 0;

    // If the buffer is full, flush it out.
    if (buf_.size() >= cap_) {
      flush_buf_(ec);
      if (ec) return 0; // Report error.
    }

    // We must fill the buffer if:
    // - there is data in the buffer, or
    // - the write is less than buffer capacity, or
    // - the write is more than remaining space.
    assert(buf_.size() < cap_);
    if (buf_.size() != 0 || buffer_size_mb < cap_ || buffer_size_mb > avl_) {
      const auto old_bufsz = buf_.size();

      buf_.resize(cap_);
      std::size_t wlen;
      try {
        wlen = boost::asio::buffer_copy(
            boost::asio::buffer(buf_) + old_bufsz,
            mb,
            avl_ > std::numeric_limits<std::size_t>::max() ? std::numeric_limits<std::size_t>::max() : avl_);
        buf_.resize(old_bufsz + wlen);
        avl_ -= wlen;
      } catch (...) {
        buf_.resize(old_bufsz);
        throw;
      }

      // Flush out the buffer if it filled up.
      if (buf_.size() >= cap_) {
        flush_buf_(ec);
        if (ec) ec.clear(); // Something was written, so don't report the error.
      }

      return wlen;
    }

    // We have nothing in the buffer, and the write is larger than a buffer.
    // So we pass it straight through.
    assert(buf_.size() == 0);
    assert(buffer_size_mb >= cap_);
    assert(buffer_size_mb <= avl_);
    auto wlen = boost::asio::write_at(
        *snk_,
        off_,
        std::forward<MB>(mb),
        ec);
    off_ += wlen;
    avl_ -= wlen;
    return wlen;
  }

  void complete() {
    boost::system::error_code ec;
    complete(ec);
    if (ec) throw boost::system::system_error(ec, "write");
  }

  void complete(boost::system::error_code& ec) {
    if (buf_.size() != 0) flush_buf_(ec);
  }

  auto offset() const noexcept {
    return off_ + buf_.size();
  }

  private:
  void flush_buf_(boost::system::error_code& ec) {
    const auto wlen = boost::asio::write_at(
        *snk_,
        off_,
        boost::asio::buffer(buf_),
        ec);
    if (ec) return;

    buf_.erase(buf_.begin(), buf_.begin() + wlen);
    off_ += wlen;
  }

  Sink* snk_ = nullptr;
  std::uint64_t off_ = 0;
  std::uint64_t avl_ = 0;
  std::vector<char, buf_alloc> buf_;
  std::size_t cap_;
};


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_BUFFERED_WRITE_STREAM_AT_H */
