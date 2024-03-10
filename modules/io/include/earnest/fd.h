#ifndef EARNEST_FD_H
#define EARNEST_FD_H

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>

#include <sys/file.h>
#include <unistd.h>

#include <earnest/dir.h>
#include <earnest/open_mode.h>
#include <earnest/execution_io.h>

namespace earnest {


class dir;


class fd {
  public:
  using offset_type = ::earnest::execution::io::offset_type;
  using size_type = offset_type;

  using open_mode = earnest::open_mode;
  static inline constexpr open_mode READ_ONLY = open_mode::READ_ONLY;
  static inline constexpr open_mode WRITE_ONLY = open_mode::WRITE_ONLY;
  static inline constexpr open_mode READ_WRITE = open_mode::READ_WRITE;

  using native_handle_type = int;
  static inline constexpr native_handle_type invalid_native_handle = -1;

  fd() noexcept = default;

  /**
   * \brief Create a file descriptor, which uses the given descriptor.
   * \details
   * If \p handle is `INVALID_HANDLE_VALUE` (windows) or `-1` (posix),
   * fd shall refer to an unopened file.
   * Otherwise the \p handle must be a valid file descriptor.
   *
   * On windows, the \p handle must be opened for overlapped IO.
   */
  explicit fd(const native_handle_type& handle) noexcept
  : handle_(handle)
  {}

  fd(const fd&) = delete;

  fd(fd&& o) noexcept
  : handle_(std::exchange(o.handle_, invalid_native_handle))
  {}

  ~fd() noexcept {
    close();
  }

  void swap(fd& other) noexcept {
    using std::swap;
    swap(handle_, other.handle_); // never throws
  }

  auto operator=(const fd&) -> fd& = delete;

  auto operator=(fd&& other) noexcept -> fd& {
    swap(other);
    other.close();
    return *this;
  }

  auto is_open() const noexcept -> bool { return handle_ != invalid_native_handle; }
  explicit operator bool() const noexcept { return is_open(); }
  auto operator!() const noexcept -> bool { return !is_open(); }

  void flock(std::error_code& ec) {
    ec.clear();
    if (::flock(handle_, LOCK_EX))
      ec.assign(errno, std::generic_category());
  }

  void flock() {
    std::error_code ec;
    flock(ec);
    if (ec) throw std::system_error(ec, "earnest::fd::flock");
  }

  void flock_shared(std::error_code& ec) {
    ec.clear();
    if (::flock(handle_, LOCK_SH))
      ec.assign(errno, std::generic_category());
  }

  void flock_shared() {
    std::error_code ec;
    flock_shared(ec);
    if (ec) throw std::system_error(ec, "earnest::fd::flock_shared");
  }

  void funlock(std::error_code& ec) {
    ec.clear();
    if (::flock(handle_, LOCK_UN))
      ec.assign(errno, std::generic_category());
  }

  void funlock() {
    std::error_code ec;
    funlock(ec);
    if (ec) throw std::system_error(ec, "earnest::fd::funlock");
  }

  void funlock_shared(std::error_code& ec) {
    ec.clear();
    if (::flock(handle_, LOCK_UN))
      ec.assign(errno, std::generic_category());
  }

  void funlock_shared() {
    std::error_code ec;
    funlock_shared(ec);
    if (ec) throw std::system_error(ec, "earnest::fd::funlock_shared");
  }

  bool ftrylock(std::error_code& ec) {
    ec.clear();
    if (::flock(handle_, LOCK_EX|LOCK_NB)) {
      if (errno != EWOULDBLOCK) ec.assign(errno, std::generic_category());
      return false;
    }
    return true;
  }

  bool ftrylock() {
    std::error_code ec;
    bool success = ftrylock(ec);
    if (ec) throw std::system_error(ec, "earnest::fd::ftrylock");
    return success;
  }

  bool ftrylock_shared(std::error_code& ec) {
    ec.clear();
    if (::flock(handle_, LOCK_SH|LOCK_NB)) {
      if (errno != EWOULDBLOCK) ec.assign(errno, std::generic_category());
      return false;
    }
    return true;
  }

  bool ftrylock_shared() {
    std::error_code ec;
    bool success = ftrylock_shared(ec);
    if (ec) throw std::system_error(ec, "earnest::fd::ftrylock_shared");
    return success;
  }

  friend auto tag_invoke(execution::io::lazy_datasync_ec_t tag, fd& self) {
    return tag(self.handle_);
  }

  friend auto tag_invoke(execution::io::lazy_sync_ec_t tag, fd& self) {
    return tag(self.handle_);
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

  friend auto tag_invoke(execution::io::lazy_truncate_ec_t tag, fd& self, offset_type len) {
    return tag(self.handle_, len);
  }

  void close() {
    std::error_code ec;
    close(ec);
    if (ec) throw std::system_error(ec, "earnest::fd::close");
  }

  void close(std::error_code& ec) {
    ec.clear();
    if (handle_ != invalid_native_handle) {
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

    fd new_fd = fd(::open(filename.c_str(), fl));
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
#ifdef O_RESOLVE_BENEATH
    fl |= O_RESOLVE_BENEATH;
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

    fd new_fd = fd(::openat(parent.native_handle(), filename.c_str(), fl));
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

    auto new_fd = fd(::open(filename.c_str(), fl, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH));
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
#ifdef O_RESOLVE_BENEATH
    fl |= O_RESOLVE_BENEATH;
#endif

    auto new_fd = fd(::openat(parent.native_handle(), filename.c_str(), fl, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH));
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

#ifdef HAS_MKSTEMP
    fd new_fd = fd(::mkstemp(template_name.data()));
    if (new_fd.handle_ == invalid_native_handle) {
      ec.assign(errno, std::generic_category());
      return;
    }
#else
    int fl = O_CREAT | O_EXCL | O_RDWR;
#ifdef O_CLOEXEC
    fl |= O_CLOEXEC;
#endif
    fd new_fd = fd(::open(::mktemp(template_name.data()), fl, 0666));
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

  template<execution::io::mutable_buffers Buffers>
  friend auto tag_invoke(execution::io::lazy_read_some_at_ec_t tag, const fd& self, offset_type offset, Buffers&& buffers) {
    return tag(self.handle_, offset, std::forward<Buffers>(buffers));
  }

  template<execution::io::const_buffers Buffers>
  friend auto tag_invoke(execution::io::lazy_write_some_at_ec_t tag, fd& self, offset_type offset, Buffers&& buffers) {
    return tag(self.handle_, offset, std::forward<Buffers>(buffers));
  }

  template<typename Collection>
  requires (sizeof(typename Collection::value_type) == 1) && requires(Collection c, std::size_t size) {
    { c.resize(size) };
  }
  auto contents(Collection c = Collection()) const -> Collection {
    using namespace execution;
    c.resize(size());
    sync_wait(io::read_at(*this, 0, std::as_writable_bytes(std::span<typename Collection::value_type>(c))));
    return c;
  }

  private:
  native_handle_type handle_ = invalid_native_handle;
};


inline void swap(fd& x, fd& y) noexcept {
  x.swap(y);
}


} /* namespace earnest */

#endif /* EARNEST_FD_H */
