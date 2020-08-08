#ifndef EARNEST_DETAIL_BUFFERED_READ_STREAM_AT_H
#define EARNEST_DETAIL_BUFFERED_READ_STREAM_AT_H

#include <cstddef>
#include <utility>
#include <memory>
#include <vector>
#include <boost/asio/buffer.hpp>
#include <boost/asio/read_at.hpp>
#include <boost/asio/error.hpp>
#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>

namespace earnest::detail {


template<typename Source, typename Alloc = std::allocator<void>>
class buffered_read_stream_at {
  public:
  using allocator_type = typename std::allocator_traits<Alloc>::template rebind_alloc<char>;

  buffered_read_stream_at() = default;

  buffered_read_stream_at(const buffered_read_stream_at&) = delete;
  buffered_read_stream_at& operator=(const buffered_read_stream_at&) = delete;

  buffered_read_stream_at(buffered_read_stream_at&& o) noexcept
  : src_(std::exchange(o.src_, nullptr)),
    off_(o.off_),
    avl_(std::exchange(o.avl_, 0u)),
    buf_(std::move(o.buf_)),
    cap_(o.cap_),
    buf_off_(o.buf_off_)
  {}

  buffered_read_stream_at& operator=(buffered_read_stream_at&& o) noexcept {
    using std::swap;

    swap(src_, o.src_);
    swap(off_, o.off_);
    swap(avl_, o.avl_);
    swap(buf_, o.buf_);
    swap(cap_, o.cap_);
    swap(buf_off_, o.buf_off_);

    return *this;
  }

  buffered_read_stream_at(Source& src, std::uint64_t off, std::uint64_t len, std::size_t bufsiz, allocator_type alloc = allocator_type())
  : src_(&src),
    off_(off),
    avl_(len),
    buf_(alloc),
    cap_(bufsiz)
  {}

  buffered_read_stream_at(Source& src, std::uint64_t off, std::uint64_t len, allocator_type alloc = allocator_type())
  : buffered_read_stream_at(src, off, len, 64 * 1024, alloc)
  {}

  auto get_allocator() const -> allocator_type {
    return buf_.get_allocator();
  }

  template<typename MB>
  auto read_some(MB&& mb) -> std::size_t {
    boost::system::error_code ec;
    std::size_t rlen = read_some(std::forward<MB>(mb), ec);
    if (ec) throw boost::system::system_error(ec, "read");
    return rlen;
  }

  template<typename MB>
  auto read_some(MB mb, boost::system::error_code& ec) -> std::size_t {
    using boost::asio::buffer_size;

    ec.clear();
    if (buffer_size(mb) == 0) return 0;

    if (buf_off_ == buf_.size()) {
      if (avl_ == 0) {
        ec = boost::asio::stream_errc::eof;
        return 0;
      }

      buf_.resize(avl_ < cap_ ? avl_ : cap_);
      buf_off_ = buf_.size();
      const auto rlen = boost::asio::read_at(
          *src_,
          off_,
          boost::asio::buffer(buf_),
          ec);
      if (ec) return 0;

      buf_.resize(rlen);
      off_ += rlen;
      avl_ -= rlen;
      buf_off_ = 0;
    }

    std::size_t copy_len = boost::asio::buffer_copy(mb, boost::asio::buffer(buf_) + buf_off_);
    buf_off_ += copy_len;
    return copy_len;
  }

  auto offset() const noexcept -> std::uint64_t {
    return off_ - (buf_.size() - buf_off_);
  }

  private:
  Source* src_ = nullptr;
  std::uint64_t off_ = 0;
  std::uint64_t avl_ = 0;
  std::vector<char, allocator_type> buf_;
  std::size_t buf_off_ = 0;
  std::size_t cap_;
};


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_BUFFERED_READ_STREAM_AT_H */
