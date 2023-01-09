#ifndef EARNEST_FD_H
#define EARNEST_FD_H

#ifndef WIN32
# include <cerrno>
# include <unistd.h>
#endif

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>

#include <asio/async_result.hpp>
#include <asio/buffer.hpp>
#include <asio/execution_context.hpp>
#include <asio/executor.hpp>
#include <asio/read_at.hpp>

#include <earnest/open_mode.h>
#include <earnest/dir.h>
#include <earnest/detail/aio/reactor.h>

namespace earnest {


class dir;


template<typename Executor = asio::executor, typename Reactor = detail::aio::reactor>
class fd {
  private:
  using reactor_type = Reactor;

  public:
  using executor_type = Executor;
  using lowest_layer_type = fd;
  using size_type = std::uint64_t;
  using offset_type = size_type;

  using open_mode = earnest::open_mode;
  static inline constexpr open_mode READ_ONLY = open_mode::READ_ONLY;
  static inline constexpr open_mode WRITE_ONLY = open_mode::WRITE_ONLY;
  static inline constexpr open_mode READ_WRITE = open_mode::READ_WRITE;

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

  template<typename OtherExecutor>
  struct rebind_executor {
    using other = fd<OtherExecutor, Reactor>;
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
  : ex_(ex),
    handle_(handle)
  {}

  fd(const fd&) = delete;

  fd(fd&& o) noexcept(std::is_nothrow_move_constructible_v<executor_type>)
  : ex_(std::move(o.ex_)),
    handle_(std::exchange(o.handle_, invalid_native_handle))
  {}

  ~fd() noexcept {
    close();
  }

  auto lowest_layer() -> lowest_layer_type& { return *this; }

  void swap(fd& other) noexcept(std::is_nothrow_swappable_v<executor_type>) {
    using std::swap;

    swap(ex_, other.ex_); // may throw
    swap(handle_, other.handle_); // never throws
  }

  auto operator=(const fd&) -> fd& = delete;

  auto operator=(fd&& other) noexcept(std::is_nothrow_move_assignable_v<executor_type>) -> fd& {
    swap(other);
    other.close();
    return *this;
  }

  auto get_executor() const -> executor_type { return ex_; }
  auto is_open() const noexcept -> bool { return handle_ != invalid_native_handle; }
  explicit operator bool() const noexcept { return is_open(); }
  auto operator!() const noexcept -> bool { return !is_open(); }

  void flush(bool data_only = false) {
    std::error_code ec;
    flush(data_only, ec);
    if (ec) throw std::system_error(ec, "earnest::fd::flush");
  }

  void flush(bool data_only, std::error_code& ec) {
    ec = aio_.flush(handle_, data_only);
  }

  void flush(std::error_code& ec) {
    flush(false, ec);
  }

  template<typename CompletionToken>
  auto async_flush(bool data_only, CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [this](auto completion_handler, bool data_only) {
          aio_.async_flush(handle_, data_only, std::move(completion_handler), ex_);
        },
        token, data_only);
  }

  auto size() const -> size_type {
    std::error_code ec;
    size_type sz = size(ec);
    if (ec) throw std::system_error(ec, "earnest::fd::size");
    return sz;
  }

  auto size(std::error_code& ec) const -> size_type {
    struct ::stat sb;
    if (::fstat(handle_, &sb)) ec.assign(errno, std::generic_category());
    return sb.st_size;
  }

  void truncate(size_type sz) {
    std::error_code ec;
    truncate(sz, ec);
    if (ec) throw std::system_error(ec, "earnest::fd::truncate");
  }

  void truncate(size_type sz, std::error_code& ec) {
    ec.clear();

    if (::ftruncate(handle_, sz)) ec.assign(errno, std::generic_category());
  }

  void cancel() {
    std::error_code ec;
    cancel(ec);
    if (ec) throw std::system_error(ec, "earnest::fd::cancel");
  }

  void cancel(std::error_code& ec) {
    if (handle_ != invalid_native_handle) {
      ec = aio_.cancel(handle_);
    } else {
      ec.clear();
    }
  }

  void close() {
    std::error_code ec;
    close(ec);
    if (ec) throw std::system_error(ec, "earnest::fd::close");
  }

  void close(std::error_code& ec) {
    if (handle_ == invalid_native_handle) {
      ec.clear();
    } else {
      cancel(ec);
      if (ec) return;
      if (::close(handle_))
        ec.assign(errno, std::generic_category());
      else
        handle_ = invalid_native_handle;
    }
  }

  void open(const std::filesystem::path& filename, open_mode mode) {
    std::error_code ec;
    open(filename, mode, ec);
    if (ec) throw std::system_error(ec, "earnest::fd::open");
  }

  void open(const std::filesystem::path& filename, open_mode mode, std::error_code& ec) {
    ec.clear();

    int fl = 0;
#ifdef O_CLOEXEC
    fl |= O_CLOEXEC;
#endif
    switch (mode) {
      case open_mode::READ_ONLY:
        fl |= O_RDONLY;
        break;
      case open_mode::WRITE_ONLY:
        fl |= O_WRONLY;
        break;
      case open_mode::READ_WRITE:
        fl |= O_RDWR;
        break;
    }

    auto new_fd = fd(get_executor());
    new_fd.handle_ = ::open(filename.c_str(), fl);
    if (new_fd.handle_ == invalid_native_handle) {
      ec.assign(errno, std::generic_category());
      return;
    }

    // Close current file.
    close(ec);
    if (ec) return;

    swap(new_fd);
  }

  void open(const dir& parent, const std::filesystem::path& filename, open_mode mode) {
    std::error_code ec;
    open(parent, filename, mode, ec);
    if (ec) throw std::system_error(ec, "earnest::fd::open");
  }

  void open(const dir& parent, const std::filesystem::path& filename, open_mode mode, std::error_code& ec) {
    if (!parent) {
      ec = std::make_error_code(std::errc::bad_file_descriptor);
      return;
    }

    ec.clear();

    int fl = 0;
#ifdef O_CLOEXEC
    fl |= O_CLOEXEC;
#endif
    switch (mode) {
      case open_mode::READ_ONLY:
        fl |= O_RDONLY;
        break;
      case open_mode::WRITE_ONLY:
        fl |= O_WRONLY;
        break;
      case open_mode::READ_WRITE:
        fl |= O_RDWR;
        break;
    }

    auto new_fd = fd(get_executor());
    new_fd.handle_ = ::openat(parent.native_handle(), filename.c_str(), fl);
    if (new_fd.handle_ == invalid_native_handle) {
      ec.assign(errno, std::generic_category());
      return;
    }

    // Close current file.
    close(ec);
    if (ec) return;

    swap(new_fd);
  }

  void create(const std::filesystem::path& filename) {
    std::error_code ec;
    create(filename, ec);
    if (ec) throw std::system_error(ec, "earnest::fd::create");
  }

  void create(const std::filesystem::path& filename, std::error_code& ec) {
    ec.clear();

    int fl = O_CREAT | O_EXCL | O_RDWR;
#ifdef O_CLOEXEC
    fl |= O_CLOEXEC;
#endif

    auto new_fd = fd(get_executor());
    new_fd.handle_ = ::open(filename.c_str(), fl, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
    if (new_fd.handle_ == invalid_native_handle) {
      ec.assign(errno, std::generic_category());
      return;
    }

    // Close current file.
    close(ec);
    if (ec) return;

    swap(new_fd);
  }

  void create(const dir& parent, const std::filesystem::path& filename) {
    std::error_code ec;
    create(parent, filename, ec);
    if (ec) throw std::system_error(ec, "earnest::fd::create");
  }

  void create(const dir& parent, const std::filesystem::path& filename, std::error_code& ec) {
    if (!parent) {
      ec = std::make_error_code(std::errc::bad_file_descriptor);
      return;
    }

    ec.clear();

    int fl = O_CREAT | O_EXCL | O_RDWR;
#ifdef O_CLOEXEC
    fl |= O_CLOEXEC;
#endif

    auto new_fd = fd(get_executor());
    new_fd.handle_ = ::openat(parent.native_handle(), filename.c_str(), fl, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
    if (new_fd.handle_ == invalid_native_handle) {
      ec.assign(errno, std::generic_category());
      return;
    }

    // Close current file.
    close(ec);
    if (ec) return;

    swap(new_fd);
  }

  void tmpfile(const std::filesystem::path& prefix) {
    std::error_code ec;
    tmpfile(prefix, ec);
    if (ec) throw std::system_error(ec, "earnest::fd::tmpfile");
  }

  void tmpfile(const std::filesystem::path& prefix, std::error_code& ec) {
    using namespace std::string_literals;

    ec.clear();

    std::filesystem::path prefix_path = prefix.native() + "XXXXXX"s;
    if (!prefix.has_parent_path() && prefix_path.is_relative())
      prefix_path = std::filesystem::temp_directory_path() / prefix_path;
    prefix_path = std::filesystem::absolute(prefix_path);

restart:
    std::string template_name = prefix_path.native();

    auto new_fd = fd(get_executor());
#ifdef HAS_MKSTEMP
    new_fd.handle_ = ::mkstemp(template_name.data());
    if (new_fd.handle_ == invalid_native_handle) {
      ec.assign(errno, std::generic_category());
      return;
    }
#else
    int fl = O_CREAT | O_EXCL | O_RDWR;
#ifdef O_CLOEXEC
    fl |= O_CLOEXEC;
#endif
    new_fd.handle_ = ::open(::mktemp(template_name.data()), fl, 0666);
    if (new_fd.handle_ == invalid_native_handle) {
      if (errno == EEXIST) goto restart;
      ec.assign(errno, std::generic_category());
      return;
    }
#endif

    // Close current file.
    close(ec);
    if (ec) return;

    swap(new_fd);
  }

  template<typename Buffers, typename CompletionToken>
  auto async_read_some_at(offset_type offset, Buffers&& mb, CompletionToken&& token) const {
    return asio::async_initiate<CompletionToken, void(std::error_code, std::size_t)>(
        [this](auto completion_handler, offset_type offset, Buffers&& mb) {
          aio_.async_read_some_at(handle_, offset, std::forward<Buffers>(mb), std::move(completion_handler), get_executor());
        },
        token, offset, std::forward<Buffers>(mb));
  }

  template<typename Buffers, typename CompletionToken>
  auto async_write_some_at(offset_type offset, Buffers&& mb, CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(std::error_code, std::size_t)>(
        [this](auto completion_handler, offset_type offset, Buffers&& mb) {
          aio_.async_write_some_at(handle_, offset, std::forward<Buffers>(mb), std::move(completion_handler), get_executor());
        },
        token, offset, std::forward<Buffers>(mb));
  }

  template<typename Buffers>
  auto read_some_at(offset_type offset, Buffers&& mb, std::error_code& ec) const -> std::size_t {
    return aio_.read_some_at(handle_, offset, std::forward<Buffers>(mb), ec);
  }

  template<typename Buffers>
  auto write_some_at(offset_type offset, Buffers&& mb, std::error_code& ec) {
    return aio_.write_some_at(handle_, offset, std::forward<Buffers>(mb), ec);
  }

  template<typename Collection>
#if __cpp_concepts >= 201907L
  requires (sizeof(typename Collection::value_type) == 1) && requires(Collection c, std::size_t size) {
    { c.resize(size) };
    { asio::buffer(c) } -> std::convertible_to<asio::mutable_buffer>;
  }
#endif
  auto contents(Collection c = Collection()) const -> Collection {
    c.resize(size());
    asio::read_at(*this, 0, asio::buffer(c));
    return c;
  }

  private:
  executor_type ex_;
  reactor_type& aio_ = asio::use_service<reactor_type>(ex_.context());
  native_handle_type handle_ = invalid_native_handle;
};


template<typename Executor, typename Reactor>
inline void swap(fd<Executor, Reactor>& x, fd<Executor, Reactor>& y) noexcept(std::is_nothrow_swappable_v<Executor>) {
  x.swap(y);
}


} /* namespace earnest */

#endif /* EARNEST_FD_H */
