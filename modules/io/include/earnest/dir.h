#ifndef EARNEST_DIR_H
#define EARNEST_DIR_H

#include <cstdlib>
#include <filesystem>
#include <iterator>
#include <memory>
#include <system_error>
#include <utility>

#ifdef _MSC_VER
# include <Ntifs.h>
#else
# include <dirent.h>
#endif

namespace earnest {


class dir {
  public:
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

  class entry;
  class iterator;

  dir() noexcept
  : handle_(invalid_native_handle)
  {}

  explicit dir(native_handle_type handle) noexcept
  : handle_(handle)
  {}

  dir(const dir&) = delete;
  dir& operator=(const dir&) = delete;

  dir(dir&& other) noexcept
  : handle_(std::exchange(other.handle_, invalid_native_handle))
  {}

  dir(const std::filesystem::path& dirname)
  : dir()
  {
    open(dirname);
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
    if (ec) throw std::system_error(ec, "earnest::dir::create");
  }

  void erase(const std::filesystem::path& name, std::error_code& ec) {
    ec = erase_(name);
  }

  auto begin() const -> iterator;
  auto end() const -> iterator;

  private:
  static auto open_(const std::filesystem::path& dirname, std::error_code& ec) -> dir;
  static auto open_(const dir& parent, const std::filesystem::path& dirname, std::error_code& ec) -> dir;
  static auto create_(const std::filesystem::path& dirname, std::error_code& ec) -> dir;
  static auto create_(const dir& parent, const std::filesystem::path& dirname, std::error_code& ec) -> dir;
  auto close_() -> std::error_code;
  auto erase_(const std::filesystem::path& name) -> std::error_code;

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
  explicit iterator(const dir& d);

  public:
  iterator(const iterator& other) = delete;

#ifdef _MSC_VER
  iterator(iterator&& other) noexcept
  : handle_(std::exchange(other.handle_, invalid_native_handle)),
    fdi_(std::move(other.fdi_)),
    fdi_extra_bytes_(std::exchange(other.fdi_extra_bytes_, 0))
  {}
#else
  iterator(iterator&& other) noexcept
  : handle_(std::exchange(other.handle_, nullptr)),
    dp_(std::exchange(other.dp_, nullptr))
  {}
#endif

  ~iterator() {
    close_();
  }

  auto operator=(const iterator& other) = delete;

  auto operator=(iterator&& other) noexcept -> iterator& {
    using std::swap;

#ifdef _MSC_VER
    swap(handle_, other.handle_);
    swap(fdi_, other.fdi_);
    swap(fdi_extra_bytes_, other.fdi_extra_bytes_);
#else
    swap(handle_, other.handle_);
    swap(dp_, other.dp_);
#endif

    other.close_();
    return *this;
  }

  auto operator*() const -> entry;

  auto operator++() -> iterator& {
    query_(false);
    return *this;
  }

  auto operator==(const iterator& other) const noexcept -> bool {
#ifdef _MSC_VER
    return handle_ == invalid_native_handle && other.handle_ == invalid_native_handle;
#else
    return handle_ == nullptr && other.handle_ == nullptr;
#endif
  }

  auto operator!=(const iterator& other) const noexcept -> bool {
    return !(*this == other);
  }

  private:
  void close_() noexcept;
  void query_(bool first);

#ifdef _MSC_VER
  auto new_io_status_block_(std::size_t extra_bytes = 8) -> std::unique_ptr<IO_STATUS_BLOCK, up_free_>;
#endif

#ifdef _MSC_VER
  native_handle_type handle_ = invalid_native_handle;
  std::unique_ptr<FILE_DIRECTORY_INFORMATION, up_free_> fdi_;
  std::size_t fdi_extra_bytes_ = 0;
#else
  DIR* handle_ = nullptr;
  struct dirent* dp_ = nullptr;
#endif
};


inline auto dir::begin() const -> iterator {
  return iterator(*this);
}

inline auto dir::end() const -> iterator {
  return {};
}


} /* namespace earnest */

#endif /* EARNEST_DIR_H */
