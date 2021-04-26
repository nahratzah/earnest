#ifndef EARNEST_FD_H
#define EARNEST_FD_H

#include <earnest/detail/export_.h>

#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <iterator>
#include <mutex>
#include <system_error>
#include <tuple>
#include <utility>
#include <vector>

#ifndef WIN32
# include <cerrno>
# include <unistd.h>
# include <sys/uio.h>
# include <asio/error.hpp>

# if __has_include(<aio.h>)
#   include <aio.h>
# endif
#endif

#include <asio/async_result.hpp>
#include <asio/buffer.hpp>
#include <asio/execution_context.hpp>
#include <asio/executor.hpp>
#include <boost/intrusive/list.hpp>

#include <earnest/detail/aio/asio.h>

namespace earnest {


template<typename Executor = asio::executor>
class earnest_export_ fd;


namespace detail {


class earnest_export_ basic_fd {
  template<typename Executor> friend class ::earnest::fd;

  private:
  struct earnest_export_ sync_queue
  : public boost::intrusive::list_base_hook<>
  {
    explicit sync_queue(basic_fd& owner);
    sync_queue(const sync_queue&) = delete;
    ~sync_queue();

    auto cancel() -> std::error_code;

    basic_fd*const owner;
#ifdef WIN32
    OVERLAPPED overlapped;
#elif __has_include(<aio.h>)
    std::vector<struct ::aiocb> iocb;
#endif
  };

  public:
  enum open_mode {
    READ_ONLY,
    WRITE_ONLY,
    READ_WRITE
  };

  using native_handle_type =
#ifdef WIN32
      HANDLE
#else
      int
#endif
      ;

  static inline constexpr native_handle_type invalid_native_handle =
#ifdef WIN32
      INVALID_HANDLE_VALUE
#else
      -1
#endif
      ;

  using size_type = std::uint64_t;
  using offset_type = size_type;

  protected:
  basic_fd() noexcept
  : handle_(invalid_native_handle)
  {}

  /**
   * \brief Create a file descriptor, which uses the given descriptor.
   * \details
   * If \p handle is `INVALID_HANDLE_VALUE` (windows) or `-1` (posix),
   * fd shall refer to an unopened file.
   * Otherwise the \p handle must be a valid file descriptor.
   *
   * On windows, the \p handle must be opened for overlapped IO.
   */
  explicit basic_fd(const native_handle_type& handle) noexcept
  : handle_(handle)
  {}

  basic_fd(const basic_fd&) = delete;

  basic_fd(basic_fd&& other) noexcept
  : basic_fd()
  {
    // Lock ordering is irrelevant.
    std::unique_lock<std::mutex> other_lck = lock_and_drain_(), this_lck(mtx_);
    swap_(other, other_lck, this_lck);
  }

  ~basic_fd() {
    close_(lock_and_drain_());
  }

  auto operator=(const basic_fd&) -> basic_fd& = delete;

  auto operator=(basic_fd&& other) -> basic_fd& {
    basic_fd *x = this, *y = &other;
    if (x > y) std::swap(x, y);

    auto x_lck = x->lock_and_drain_();
    auto y_lck = y->lock_and_drain_();

    std::error_code ec = close_(x == this ? x_lck : y_lck);
    if (ec) throw std::system_error(ec, "close");

    swap_(other, x_lck, y_lck);
    return *this;
  }

  public:
  auto native_handle() const noexcept -> native_handle_type {
    std::lock_guard<std::mutex> lck(mtx_);
    return handle_;
  }

  auto is_open() const noexcept -> bool { return native_handle() != invalid_native_handle; }
  explicit operator bool() const noexcept { return is_open(); }
  auto operator!() const noexcept -> bool { return !is_open(); }

  void flush(bool data_only = false);
  void flush(bool data_only, std::error_code& ec);
  void flush(std::error_code& ec) { flush(false, ec); }

  auto size() const -> size_type;
  auto size(std::error_code& ec) const -> size_type;

  void truncate(size_type sz);
  void truncate(size_type sz, std::error_code& ec);

  template<typename Buffers>
  std::size_t read_some_at(offset_type offset, Buffers&& mb) {
    std::error_code ec;
    const std::size_t s = read_some_at(offset, std::forward<Buffers>(mb), ec);
    if (ec) throw std::system_error(ec);
    return s;
  }

  template<typename Buffers>
  std::size_t read_some_at(offset_type offset, Buffers&& mb, std::error_code& ec) {
    sync_queue sq_elem(*this);

#ifdef WIN32
    auto buf = std::find_if(
        asio::buffer_sequence_begin(mb), asio::buffer_sequence_end(mb),
        [](asio::mutable_buffer b) { return b.size() > 0; });
    if (buf == asio::buffer_sequence_end(mb)) [[unlikely]] {
      ec.clear();
      return 0;
    }

    return read_some_at_overlapped_(offset, buf, sq_elem.overlapped, ec);
#elif __has_include(<aio.h>)
    std::transform(
        asio::buffer_sequence_begin(mb), asio::buffer_sequence_end(mb),
        std::back_inserter(sq_elem.iocb),
        [this, offset](asio::mutable_buffer b) {
          struct ::aiocb v;
          std::memset(&v, 0, sizeof(v));
          v.aio_lio_opcode = LIO_READ;
          v.aio_fildes = handle_;
          v.aio_offset = offset;
          v.aio_buf = b.data();
          v.aio_nbytes = b.size();
          return v;
        });
    return some_aio_at_(sq_elem.iocb, ec);
#else
    ec.clear();

    std::vector<struct ::iovec> iov;
    std::transform(
        asio::buffer_sequence_begin(mb), asio::buffer_sequence_end(mb),
        std::back_inserter(iov),
        [](asio::mutable_buffer b) {
          struct ::iovec v;
          v.iov_base = b.data();
          v.iov_len = b.size();
          return v;
        });

    auto s = preadv(handle_, iov.data(), iov.size(), offset);
    switch (s) {
      default:
        return std::size_t(s);
      case -1:
        ec = std::system_error(errno, std::system_category());
        return 0;
      case 0:
        ec = asio::stream_errc::eof;
        return 0;
    }
#endif
  }

  template<typename Buffers>
  std::size_t write_some_at(offset_type offset, Buffers&& mb) {
    std::error_code ec;
    const std::size_t s = write_some_at(offset, std::forward<Buffers>(mb), ec);
    if (ec) throw std::system_error(ec);
    return s;
  }

  template<typename Buffers>
  std::size_t write_some_at(offset_type offset, Buffers&& mb, std::error_code& ec) {
    sync_queue sq_elem(*this);

#ifdef WIN32
    auto buf = std::find_if(
        asio::buffer_sequence_begin(mb), asio::buffer_sequence_end(mb),
        [](asio::const_buffer b) { return b.size() > 0; });
    if (buf == asio::buffer_sequence_end(mb)) [[unlikely]] {
      ec.clear();
      return 0;
    }

    return write_some_at_overlapped_(offset, buf, sq_elem.overlapped, ec);
#elif __has_include(<aio.h>)
    std::transform(
        asio::buffer_sequence_begin(mb), asio::buffer_sequence_end(mb),
        std::back_inserter(sq_elem.iocb),
        [this, offset](asio::const_buffer b) {
          struct ::aiocb v;
          std::memset(&v, 0, sizeof(v));
          v.aio_lio_opcode = LIO_WRITE;
          v.aio_fildes = handle_;
          v.aio_offset = offset;
          v.aio_buf = const_cast<void*>(b.data());
          v.aio_nbytes = b.size();
          return v;
        });
    return some_aio_at_(sq_elem.iocb, ec);
#else
    ec.clear();

    std::vector<struct ::iovec> iov;
    std::transform(
        asio::buffer_sequence_begin(mb), asio::buffer_sequence_end(mb),
        std::back_inserter(iov),
        [](asio::const_buffer b) {
          struct ::iovec v;
          v.iov_base = const_cast<void*>(b.data());
          v.iov_len = b.size();
          return v;
        });

    auto s = pwritev(handle_, iov.data(), iov.size(), offset);
    switch (s) {
      default:
        return std::size_t(s);
      case -1:
        ec = std::system_error(errno, std::system_category());
        return 0;
      case 0:
        ec = asio::stream_errc::eof;
        return 0;
    }
#endif
  }

  protected:
  static auto open_(const std::filesystem::path& filename, open_mode mode, std::error_code& ec) -> basic_fd;
  static auto create_(const std::filesystem::path& filename, std::error_code& ec) -> basic_fd;
  static auto tmpfile_(const std::filesystem::path& prefix, std::error_code& ec) -> basic_fd;
  auto close_(const std::unique_lock<std::mutex>& lck) -> std::error_code;
  auto cancel_(std::unique_lock<std::mutex>& lck) -> std::error_code; // Called without draining queues.

  void swap_(basic_fd& other, [[maybe_unused]] const std::unique_lock<std::mutex>& lck1, [[maybe_unused]] const std::unique_lock<std::mutex>& lck2) noexcept {
    using std::swap;

    assert(lck1.owns_lock() && lck2.owns_lock());
    assert(lck1.mutex() == &mtx_ || lck2.mutex() == &mtx_);
    assert(lck1.mutex() == &other.mtx_ || lck2.mutex() == &other.mtx_);
    assert(sync_queue_.empty() && other.sync_queue_.empty());

    swap(handle_, other.handle_);
    // Both sync_queue_ are empty, so we don't need to swap them.
    // Both mutexen are equivalent (and in known locked state) so we don't need to swap them either.
  }

  template<typename Pred>
  auto lock_and_drain_(Pred&& pred) const -> std::unique_lock<std::mutex> {
    std::unique_lock<std::mutex> lck(mtx_);
    notif_.wait(lck,
        [this, &pred]() -> bool {
          return sync_queue_.empty() && std::invoke(pred);
        });
    return lck;
  }

  auto lock_and_drain_() -> std::unique_lock<std::mutex> {
    return lock_and_drain_([]() -> bool { return true; });
  }

  private:
#ifdef WIN32
  auto read_some_at_overlapped_(offset_type offset, asio::mutable_buffer buf, OVERLAPPED& overlapped, std::error_code& ec) -> std::size_t;
  auto write_some_at_overlapped_(offset_type offset, asio::const_buffer buf, OVERLAPPED& overlapped, std::error_code& ec) -> std::size_t;
#elif __has_include(<aio.h>)
  auto some_aio_at_(std::vector<struct ::aiocb>& v, std::error_code& ec) -> std::size_t;
#endif

  protected:
  mutable std::mutex mtx_;

  private:
  native_handle_type handle_;
  boost::intrusive::list<sync_queue> sync_queue_;
  mutable std::condition_variable notif_;
};


} /* namespace earnest::detail */


template<typename Executor>
class earnest_export_ fd
: public detail::basic_fd
{
  public:
  using executor_type = Executor;

  template<typename OtherExecutor>
  struct rebind_executor {
    using other = fd<OtherExecutor>;
  };

  explicit fd(const executor_type& ex)
  : ex_(ex)
  {}

  /**
   * \brief Create a file descriptor, which uses the given descriptor.
   * \details
   * If \p handle is `INVALID_HANDLE_VALUE` (windows) or `-1` (posix),
   * fd shall refer to an unopened file.
   * Otherwise the \p handle must be a valid file descriptor.
   *
   * On windows, the \p handle must be opened for overlapped IO.
   */
  fd(const executor_type& ex, const native_handle_type& handle) noexcept(std::is_nothrow_copy_constructible_v<executor_type>)
  : basic_fd(),
    ex_(ex)
  {
    // Late copy, so we maintain strong exception guarantee if `ex` constructor throws.
    this->detail::basic_fd::operator=(detail::basic_fd(handle));
  }

  fd(const fd&) = delete;

  fd(fd&& o) noexcept(std::is_nothrow_move_constructible_v<executor_type>)
  : basic_fd(),
    ex_(std::move(o.ex_)),
    aio_(std::exchange(o.aio_, nullptr))
  {
    // Late copy, so we maintain strong exception guarantee if `ex` constructor throws.
    this->detail::basic_fd::operator=(std::move(o));
  }

  ~fd() noexcept {
    close();
  }

  void swap(fd& other) noexcept(std::is_nothrow_swappable_v<executor_type>) {
    using std::swap;

    basic_fd *x = this, *y = &other;
    if (x > y) std::swap(x, y);

    auto x_lck = x->lock_and_drain_();
    auto y_lck = y->lock_and_drain_();

    swap(ex_, other.ex_); // may throw
    this->detail::basic_fd::swap_(other, x_lck, y_lck); // never throws
    swap(aio_, other.aio_); // never throws
  }

  auto operator=(const fd&) -> fd& = delete;

  auto operator=(fd&& other) noexcept(std::is_nothrow_move_assignable_v<executor_type>) -> fd& {
    swap(other);
    other.close();
    return *this;
  }

  executor_type get_executor() const { return ex_; }

  void cancel() {
    std::error_code ec;
    cancel(ec);
    if (ec) throw std::system_error(ec, "earnest::fd::cancel");
  }

  void cancel(std::error_code& ec) {
    std::unique_lock<std::mutex> lck(mtx_);
    ec = cancel_(lck);
    if (aio_ != nullptr) aio_->close_fd(handle_);
  }

  void close() {
    std::error_code ec;
    close(ec);
    if (ec) throw std::system_error(ec, "earnest::fd::close");
  }

  void close(std::error_code& ec) {
    auto lck = lock_and_drain_();
    if (aio_ != nullptr) aio_->close_fd(handle_);
    close_(std::move(lck));
  }

  void open(const std::filesystem::path& filename, open_mode mode) {
    std::error_code ec;
    open(filename, mode, ec);
    if (ec) throw std::system_error(ec, "earnest::fd::open");
  }

  void open(const std::filesystem::path& filename, open_mode mode, std::error_code& ec) {
    auto new_fd = fd(ex_);
    new_fd.detail::basic_fd::operator=(detail::basic_fd::open_(filename, mode, ec));
    if (!ec) *this = std::move(new_fd);
  }

  void create(const std::filesystem::path& filename) {
    std::error_code ec;
    create(filename, ec);
    if (ec) throw std::system_error(ec, "earnest::fd::create");
  }

  void create(const std::filesystem::path& filename, std::error_code& ec) {
    auto new_fd = fd(ex_);
    new_fd.detail::basic_fd::operator=(detail::basic_fd::create_(filename, ec));
    if (!ec) *this = std::move(new_fd);
  }

  void tmpfile(const std::filesystem::path& prefix) {
    std::error_code ec;
    tmpfile(prefix, ec);
    if (ec) throw std::system_error(ec, "earnest::fd::tmpfile");
  }

  void tmpfile(const std::filesystem::path& prefix, std::error_code& ec) {
    auto new_fd = fd(ex_);
    new_fd.detail::basic_fd::operator=(detail::basic_fd::tmpfile_(prefix, ec));
    if (!ec) *this = std::move(new_fd);
  }

  template<typename Buffers, typename CompletionToken>
  auto async_read_some_at(offset_type offset, Buffers&& mb, CompletionToken&& token) -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code, std::size_t)>::return_type {
    detail::aio::asio_service& aio = get_service_();

    asio::async_completion<CompletionToken, void(std::error_code, std::size_t)> init(token);
    aio.read_some(native_handle(), offset, std::forward<Buffers>(mb), std::move(init.completion_handler), ex_);
    return init.result.get();
  }

  private:
  auto get_service_() -> detail::aio::asio_service& {
    std::lock_guard<std::mutex> lck(mtx_);
    if (aio_ == nullptr) [[unlikely]]
      aio_ = &asio::use_service<detail::aio::asio_service>(ex_.context());
    return *aio_;
  }

  executor_type ex_;
  detail::aio::asio_service* aio_ = nullptr;
};


template<typename Executor>
inline void swap(fd<Executor>& x, fd<Executor>& y) noexcept(std::is_nothrow_swappable_v<Executor>) {
  x.swap(y);
}


extern template class fd<>;


} /* namespace earnest */

#endif /* EARNEST_FD_H */
