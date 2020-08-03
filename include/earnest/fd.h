#ifndef EARNEST_FD_H
#define EARNEST_FD_H

#include <earnest/detail/export_.h>

#ifdef WIN32
# include <boost/asio/windows/random_access_handle.hpp>
#else
# include <boost/asio/posix/stream_descriptor.hpp>
#endif

#include <boost/system/error_code.hpp>

#include <cstdint>
#include <string>
#include <utility>

#ifndef WIN32
# include <cerrno>
# include <unistd.h>
# include <sys/uio.h>
# include <boost/system/system_error.hpp>
# include <boost/asio/error.hpp>
#endif

namespace earnest {


class earnest_export_ fd {
  public:
  enum open_mode {
    READ_ONLY,
    WRITE_ONLY,
    READ_WRITE
  };

  using asio_type =
#ifdef WIN32
      boost::asio::windows::random_access_handle
#else
      boost::asio::posix::stream_descriptor
#endif
      ;
  using native_handle_type = asio_type::native_handle_type;
  using executor_type = asio_type::executor_type;
  using size_type = std::uint64_t;
  using offset_type = size_type;

  explicit fd(const executor_type& ex)
  : impl_(ex)
  {}

  fd(const executor_type& ex, const native_handle_type& handle)
  : impl_(ex, handle)
  {}

  fd(asio_type&& impl)
  : impl_(std::move(impl))
  {}

  fd(fd&& o) noexcept(std::is_nothrow_move_constructible_v<asio_type>)
  : impl_(std::move(o.impl_))
  {}

  ~fd() noexcept {
    if (is_open()) close();
  }

  native_handle_type native_handle() { return impl_.native_handle(); }
  executor_type get_executor() { return impl_.get_executor(); }
  asio_type& impl() { return impl_; }
  const asio_type& impl() const { return impl_; }

  bool is_open() const { return impl_.is_open(); }
  void open(const std::string& filename, open_mode mode);
  void open(const std::string& filename, open_mode mode, boost::system::error_code& ec);
  void create(const std::string& filename);
  void create(const std::string& filename, boost::system::error_code& ec);
  void tmpfile(const std::string& prefix);
  void tmpfile(const std::string& prefix, boost::system::error_code& ec);
  void close() { impl_.close(); }
  void close(boost::system::error_code& ec) { impl_.close(ec); }
  void flush(bool data_only = false);
  void flush(bool data_only, boost::system::error_code& ec);
  void flush(boost::system::error_code& ec) { flush(false, ec); }
  auto size() const -> size_type;
  auto size(boost::system::error_code& ec) const -> size_type;
  void truncate(size_type sz);
  void truncate(size_type sz, boost::system::error_code& ec);

  template<typename MB>
  auto read_some(MB&& mb) -> std::size_t {
    return impl_.read_some(std::forward<MB>(mb));
  }

  template<typename MB>
  auto read_some(MB&& mb, boost::system::error_code& ec) -> std::size_t {
    return impl_.read_some(std::forward<MB>(mb), ec);
  }

  template<typename MB>
  auto read_some_at(offset_type off, MB&& mb) -> std::size_t;
  template<typename MB>
  auto read_some_at(offset_type off, MB&& mb, boost::system::error_code& ec) -> std::size_t;

  template<typename MB>
  auto write_some_at(offset_type off, MB&& mb) -> std::size_t;
  template<typename MB>
  auto write_some_at(offset_type off, MB&& mb, boost::system::error_code& ec) -> std::size_t;

  private:
  asio_type impl_;
};

template<typename MB>
inline auto fd::read_some_at(offset_type off, MB&& mb) -> std::size_t {
#ifdef WIN32
  return impl_.read_some_at(off, std::forward<MB>(mb));
#else
  boost::system::error_code ec;
  std::size_t rlen = read_some_at(off, std::forward<MB>(mb), ec);
  if (ec) throw boost::system::system_error(ec, "read");
  return rlen;
#endif
}

template<typename MB>
inline auto fd::read_some_at(offset_type off, MB&& mb, boost::system::error_code& ec) -> std::size_t {
#ifdef WIN32
  return impl_.read_some_at(off, std::forward<MB>(mb), ec);
#else
  using boost::asio::buffer_size;

  ec.clear();
  if (buffer_size(mb) == 0) return 0;

  std::vector<struct ::iovec> iov;
  std::transform(
      boost::asio::buffer_sequence_begin(mb),
      boost::asio::buffer_sequence_end(mb),
      std::back_inserter(iov),
      [](const auto& buf) {
        struct ::iovec v;
        v.iov_base = buf.data();
        v.iov_len = buf.size();
        return v;
      });

  auto rlen = ::preadv(native_handle(), iov.data(), iov.size(), off);
  switch (rlen) {
    case 0:
      ec = boost::asio::stream_errc::eof;
      break;
    case -1:
      ec = boost::system::error_code(errno, boost::system::system_category());
      rlen = 0;
      break;
  }

  return rlen;
#endif
}

template<typename MB>
inline auto fd::write_some_at(offset_type off, MB&& mb) -> std::size_t {
#ifdef WIN32
  return impl_.write_some_at(off, std::forward<MB>(mb));
#else
  boost::system::error_code ec;
  std::size_t rlen = write_some_at(off, std::forward<MB>(mb), ec);
  if (ec) throw boost::system::system_error(ec, "read");
  return rlen;
#endif
}

template<typename MB>
inline auto fd::write_some_at(offset_type off, MB&& mb, boost::system::error_code& ec) -> std::size_t {
#ifdef WIN32
  return impl_.write_some_at(off, std::forward<MB>(mb), ec);
#else
  using boost::asio::buffer_size;

  ec.clear();
  if (buffer_size(mb) == 0) return 0;

  std::vector<struct ::iovec> iov;
  std::transform(
      boost::asio::buffer_sequence_begin(mb),
      boost::asio::buffer_sequence_end(mb),
      std::back_inserter(iov),
      [](const auto& buf) {
        struct ::iovec v;
        v.iov_base = const_cast<void*>(buf.data());
        v.iov_len = buf.size();
        return v;
      });

  auto wlen = ::pwritev(native_handle(), iov.data(), iov.size(), off);
  switch (wlen) {
    case 0:
      ec = boost::asio::stream_errc::eof;
      break;
    case -1:
      ec = boost::system::error_code(errno, boost::system::system_category());
      wlen = 0;
      break;
  }

  return wlen;
#endif
}


} /* namespace earnest */

#endif /* EARNEST_FD_H */
