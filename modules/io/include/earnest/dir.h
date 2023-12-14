#pragma once

#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <iterator>
#include <memory>
#include <system_error>
#include <utility>

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace earnest {


class dir {
  public:
  using native_handle_type = int;

  static inline constexpr native_handle_type invalid_native_handle = -1;

  class entry;
  class iterator;

  dir() noexcept
  : handle_(invalid_native_handle)
  {}

  explicit dir(native_handle_type handle) noexcept
  : handle_(handle)
  {}

  dir(const dir& other)
  : dir()
  {
    if (other.handle_ != invalid_native_handle) {
      handle_ = ::dup(other.handle_);
      if (handle_ == -1) [[unlikely]]
        throw std::system_error(std::error_code(errno, std::generic_category()));
    }
  }

  auto operator=(const dir& other) -> dir& {
    using std::swap;

    dir copy = other;
    swap(handle_, copy.handle_);
    return *this;
  }

  dir(dir&& other) noexcept
  : handle_(std::exchange(other.handle_, invalid_native_handle))
  {}

  dir(const std::filesystem::path& dirname)
  : dir()
  {
    open(dirname);
  }

  dir(const dir& parent, const std::filesystem::path& dirname)
  : dir()
  {
    open(parent, dirname);
  }

  auto operator=(dir&& other) noexcept -> dir& {
    close();
    handle_ = std::exchange(other.handle_, invalid_native_handle);
    return *this;
  }

  ~dir() noexcept {
    close();
  }

  auto native_handle() const noexcept -> native_handle_type {
    return handle_;
  }

  auto is_open() const noexcept -> bool { return native_handle() != invalid_native_handle; }
  explicit operator bool() const noexcept { return is_open(); }
  auto operator!() const noexcept -> bool { return !is_open(); }

  void open(const std::filesystem::path& dirname) {
    std::error_code ec;
    open(dirname, ec);
    if (ec) throw std::system_error(ec, "earnest::dir::open");
  }

  void open(const std::filesystem::path& dirname, std::error_code& ec) {
    dir newdir = open_(dirname, ec);
    if (!ec) *this = std::move(newdir);
  }

  void open(const dir& parent, const std::filesystem::path& dirname) {
    std::error_code ec;
    open(parent, dirname, ec);
    if (ec) throw std::system_error(ec, "earnest::dir::open");
  }

  void open(const dir& parent, const std::filesystem::path& dirname, std::error_code& ec) {
    dir newdir = open_(parent, dirname, ec);
    if (!ec) *this = std::move(newdir);
  }

  void create(const std::filesystem::path& dirname) {
    std::error_code ec;
    create(dirname, ec);
    if (ec) throw std::system_error(ec, "earnest::dir::create");
  }

  void create(const std::filesystem::path& dirname, std::error_code& ec) {
    dir newdir = create_(dirname, ec);
    if (!ec) *this = std::move(newdir);
  }

  void create(const dir& parent, const std::filesystem::path& dirname) {
    std::error_code ec;
    create(parent, dirname, ec);
    if (ec) throw std::system_error(ec, "earnest::dir::create");
  }

  void create(const dir& parent, const std::filesystem::path& dirname, std::error_code& ec) {
    dir newdir = create_(parent, dirname, ec);
    if (!ec) *this = std::move(newdir);
  }

  void close() {
    std::error_code ec;
    close(ec);
    if (ec) throw std::system_error(ec, "earnest::dir::close");
  }

  void close(std::error_code& ec) {
    ec = close_();
  }

  auto mkdir(const std::filesystem::path& dirname) const -> dir {
    dir d;
    d.create(*this, dirname);
    return d;
  }

  auto mkdir(const std::filesystem::path& dirname, std::error_code& ec) const -> dir {
    dir d;
    d.create(*this, dirname, ec);
    return d;
  }

  void erase(const std::filesystem::path& name) {
    std::error_code ec;
    erase(name, ec);
    if (ec) throw std::system_error(ec, "earnest::dir::erase");
  }

  void erase(const std::filesystem::path& name, std::error_code& ec) {
    ec = erase_(name);
  }

  auto begin() const -> iterator;
  auto end() const -> iterator;

  private:
  static auto open_(const std::filesystem::path& dirname, std::error_code& ec) -> dir {
    ec.clear();

    int fl = O_DIRECTORY | O_RDONLY;
#ifdef O_CLOEXEC
    fl |= O_CLOEXEC;
#endif

    dir d;
    d.handle_ = ::open(dirname.c_str(), fl);
    if (d.handle_ == -1) ec = std::error_code(errno, std::generic_category());
    return d;
  }

  static auto open_(const dir& parent, const std::filesystem::path& dirname, std::error_code& ec) -> dir {
    dir d;

    if (!parent) {
      ec = std::make_error_code(std::errc::bad_file_descriptor);
      return d;
    }

    ec.clear();

    int fl = O_DIRECTORY | O_RDONLY;
#ifdef O_CLOEXEC
    fl |= O_CLOEXEC;
#endif

    d.handle_ = ::openat(parent.handle_, dirname.c_str(), fl);
    if (d.handle_ == -1) ec = std::error_code(errno, std::generic_category());
    return d;
  }

  static auto create_(const std::filesystem::path& dirname, std::error_code& ec) -> dir {
    dir d;

    ec.clear();

    if (::mkdir(dirname.c_str(), S_IRWXU|S_IRWXG|S_IRWXO) != 0)
      ec = std::error_code(errno, std::generic_category());
    else
      d = open_(dirname, ec);
    return d;
  }

  static auto create_(const dir& parent, const std::filesystem::path& dirname, std::error_code& ec) -> dir {
    dir d;

    if (!parent) {
      ec = std::make_error_code(std::errc::bad_file_descriptor);
      return d;
    }

    ec.clear();

    if (::mkdirat(parent.handle_, dirname.c_str(), S_IRWXU|S_IRWXG|S_IRWXO) != 0)
      ec = std::error_code(errno, std::generic_category());
    else
      d = open_(parent, dirname, ec);
    return d;
  }

  auto close_() -> std::error_code {
    if (handle_ == invalid_native_handle) return {};

    if (::close(handle_) != 0) return std::error_code(errno, std::generic_category());
    handle_ = invalid_native_handle;
    return {};
  }

  auto erase_(const std::filesystem::path& name) -> std::error_code {
    if (handle_ == invalid_native_handle)
      return std::make_error_code(std::errc::bad_file_descriptor);

    struct stat sb;
    if (::fstatat(handle_, name.c_str(), &sb, AT_SYMLINK_NOFOLLOW) != 0)
      return std::error_code(errno, std::generic_category());
    if (::unlinkat(handle_, name.c_str(), S_ISDIR(sb.st_mode) ? AT_REMOVEDIR : 0) != 0)
      return std::error_code(errno, std::generic_category());
    return {};
  }

  native_handle_type handle_;
};


class dir::entry {
  friend dir::iterator;

  public:
  entry()
  : path_(),
    file_size_(0),
    hard_link_count_(0),
    exists_(false),
    is_block_file_(false),
    is_character_file_(false),
    is_directory_(false),
    is_fifo_(false),
    is_other_(false),
    is_regular_file_(false),
    is_socket_(false),
    is_symlink_(false)
  {}

  auto path() const noexcept -> const std::filesystem::path& { return path_; }
  operator const std::filesystem::path&() const noexcept { return path_; }
  auto exists() const noexcept -> bool { return exists_; }
  auto is_block_file() const noexcept -> bool { return is_block_file_; }
  auto is_character_file() const noexcept -> bool { return is_character_file_; }
  auto is_directory() const noexcept -> bool { return is_directory_; }
  auto is_fifo() const noexcept -> bool { return is_fifo_; }
  auto is_other() const noexcept -> bool { return is_other_; }
  auto is_regular_file() const noexcept -> bool { return is_regular_file_; }
  auto is_socket() const noexcept -> bool { return is_socket_; }
  auto is_symlink() const noexcept -> bool { return is_symlink_; }
  auto file_size() const noexcept -> std::uintmax_t { return file_size_; }
  auto hard_link_count() const noexcept -> std::uintmax_t { return hard_link_count_; }

  private:
  std::filesystem::path path_;
  std::uintmax_t file_size_, hard_link_count_;
  bool exists_ : 1;
  bool is_block_file_ : 1;
  bool is_character_file_ : 1;
  bool is_directory_ : 1;
  bool is_fifo_ : 1;
  bool is_other_ : 1;
  bool is_regular_file_ : 1;
  bool is_socket_ : 1;
  bool is_symlink_ : 1;
};


class dir::iterator {
  friend dir;

  private:
  struct up_free_ {
    void operator()(void* ptr) {
      std::free(ptr);
    }
  };

  public:
  using iterator_category = std::input_iterator_tag;
  using value_type = entry;
  using pointer = const entry*;
  using reference = const entry&;
  using difference_type = std::ptrdiff_t;

  constexpr iterator() noexcept = default;

  private:
  explicit iterator(const dir& d)
  : iterator()
  {
    // The `dup` system call will have the duplicated file descriptor share its
    // position with the original file descriptor. This means that moving this
    // iterator would move all iterators that currently exist.
    // We want iterators to be independent, so we require an independent copy
    // of the descriptor. We achieve that by opening the same directory.
    dir fd_copy;
    fd_copy.open(d, ".");

    // Now move the handle to the iterator.
    handle_ = ::fdopendir(fd_copy.handle_);
    if (handle_ == nullptr) throw std::system_error(std::error_code(errno, std::generic_category()), "fdopendir");
    fd_copy.handle_ = invalid_native_handle; // Claim for ourselves.

    // Load the `begin()` state.
    query_();
  }

  public:
  iterator(const iterator& other) = delete;

  iterator(iterator&& other) noexcept
  : handle_(std::exchange(other.handle_, nullptr)),
    dp_(std::exchange(other.dp_, nullptr))
  {}

  ~iterator() {
    close_();
  }

  auto operator=(const iterator& other) = delete;

  auto operator=(iterator&& other) noexcept -> iterator& {
    using std::swap;

    swap(handle_, other.handle_);
    swap(dp_, other.dp_);

    other.close_();
    return *this;
  }

  auto operator*() const -> entry {
    struct ::stat sb;
    if (::fstatat(dirfd(handle_), dp_->d_name, &sb, AT_SYMLINK_NOFOLLOW) != 0)
      throw std::system_error(std::error_code(errno, std::generic_category()), "fstatat");

    entry e;
    e.path_.assign(dp_->d_name);
    e.file_size_ = sb.st_size;
    e.hard_link_count_ = sb.st_nlink;
    e.exists_ = true;
    e.is_block_file_ = S_ISBLK(sb.st_mode);
    e.is_character_file_ = S_ISCHR(sb.st_mode);
    e.is_directory_ = S_ISDIR(sb.st_mode);
    e.is_fifo_ = S_ISFIFO(sb.st_mode);
    e.is_regular_file_ = S_ISREG(sb.st_mode);
    e.is_socket_ = S_ISSOCK(sb.st_mode);
    e.is_symlink_ = S_ISLNK(sb.st_mode);
    e.is_other_ = e.exists_ && !e.is_block_file_ && !e.is_character_file_ && !e.is_directory_ && !e.is_fifo_ && !e.is_regular_file_ && !e.is_socket_ && !e.is_symlink_;
    return e;
  }

  auto operator++() -> iterator& {
    query_();
    return *this;
  }

  auto operator==(const iterator& other) const noexcept -> bool {
    return handle_ == nullptr && other.handle_ == nullptr;
  }

  auto operator!=(const iterator& other) const noexcept -> bool {
    return !(*this == other);
  }

  private:
  void close_() noexcept {
    if (handle_ != nullptr) ::closedir(handle_);
    handle_ = nullptr;
  }

  void query_() {
    assert(handle_ != nullptr);

    dp_ = ::readdir(handle_);
    if (dp_ == nullptr) close_();
  }

  DIR* handle_ = nullptr;
  struct dirent* dp_ = nullptr;
};


inline auto dir::begin() const -> iterator {
  return iterator(*this);
}

inline auto dir::end() const -> iterator {
  return {};
}


} /* namespace earnest */
