#include <earnest/fd.h>

#ifdef WIN32
# include <Windows.h>
#else
# include <cerrno>
# include <cstdlib>
# include <fcntl.h>
# include <sys/stat.h>
# include <unistd.h>
#endif

#include <cassert>
#include <cstring>
#include <system_error>
#include <vector>

#include <asio/error.hpp>

namespace earnest {
namespace detail {


void basic_fd::flush(bool data_only) {
  std::error_code ec;
  flush(data_only, ec);
  if (ec) throw std::system_error(ec, "file IO error");
}

auto basic_fd::size() const -> size_type {
  std::error_code ec;
  auto s = size(ec);
  if (ec) throw std::system_error(ec, "file IO error");
  return s;
}

void basic_fd::truncate(size_type sz) {
  std::error_code ec;
  truncate(sz, ec);
  if (ec) throw std::system_error(ec, "file IO error");
}


} /* namespace earnest::detail */


#ifdef WIN32

namespace {


static auto last_error_() -> std::error_code {
  return std::error_code(GetLastError(), std::system_category());
}

static auto tmpdir_(error_code& ec) -> std::filesystem::path {
  std::string dir;

#if __cplusplus >= 201703L // mutable std::string::data()
  dir.resize(MAX_PATH + 1);
  auto dirlen = GetTempPath(dir.size(), dir.data());
  if (dirlen > dir.size()) {
    dir.resize(dirlen + 1);
    dirlen = GetTempPath(dir.size(), dir.data());
  }
  if (dirlen == 0) {
    ec = last_error_();
    return dir;
  }

  assert(dirlen <= dir.size());
  dir.resize(dirlen);
#else
  std::vector<char> dirbuf;
  dirbuf.resize(MAX_PATH + 1);
  auto dirlen = GetTempPath(dirbuf.size(), dirbuf.data());
  if (dirlen > dirbuf.size()) {
    dirbuf.resize(dirlen + 1);
    dirlen = GetTempPath(dirbuf.size(), dirbuf.data());
  }
  if (dirlen == 0) {
    ec = last_error_();
    return dir;
  }

  assert(dirlen <= dirbuf.size());
  dir.assign(dirbuf.begin(), dirbuf.begin() + dirlen);
#endif

  return dir;
}


} /* namespace earnest::<unnamed> */

namespace detail {


auto basic_fd::open_(const std::filesystem::path& filename, open_mode mode, std::error_code& ec) -> basic_fd {
  ec.clear();

  DWORD dwDesiredAccess = 0;
  switch (mode) {
    case READ_ONLY:
      dwDesiredAccess |= GENERIC_READ;
      break;
    case WRITE_ONLY:
      dwDesiredAccess |= GENERIC_WRITE;
      break;
    case READ_WRITE:
      dwDesiredAccess |= GENERIC_READ | GENERIC_WRITE;
      break;
  }

  basic_fd f;
  f.handle_ = CreateFile(
      filename.c_str(),
      dwDesiredAccess,
      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
      nullptr,
      OPEN_EXISTING,
      FILE_FLAG_OVERLAPPED,
      nullptr);
  if (!f) {
    ec = last_error_();
    return f;
  }

  FILE_BASIC_INFO basic_info;
  GetFileInformationByHandle(f.native_handle(), FileBasicInfo, &basic_info, sizeof(basic_info));
  if (basic_info.FileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
    ec = std::make_error_code(std::errc::is_a_directory);
    f.close();
  }

  return f;
}

auto basic_fd::create_(const std::filesystem::path& filename, std::error_code& ec) -> basic_fd {
  ec.clear();

  native_handle_type handle = CreateFile(
      filename.c_str(),
      GENERIC_READ | GENERIC_WRITE,
      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
      nullptr,
      CREATE_NEW,
      FILE_FLAG_OVERLAPPED,
      nullptr);
  if (handle == INVALID_HANDLE_VALUE) {
    ec = last_error_();
    return {};
  }

  return basic_fd(handle);
}

auto basic_fd::tmpfile_(const std::filesystem::path& prefix_path, std::error_code& ec) -> basic_fd {
  ec.clear();

  // Figure out the directory to use.
  const std::filesystem::path dir = (prefix_path.has_parent_path() ? prefix_path.parent() : tmpdir_(ec));
  if (ec) return {};
  const std::string prefix_fname = prefix_path.filename();

  std::string name;
  for (;;) {
    // Figure out the temporary file name.
#if __cplusplus >= 201703L // mutable std::string::data()
    name.resize(MAX_PATH + 1);
    const UINT uRetVal = GetTempFileName(dir.c_str(), prefix_fname.c_str(), 0, name.data());
    if (uRetVal == 0) {
      ec = last_error_();
      return;
    }

    const auto name_end = name.find('\0');
    if (name_end != std::string::npos) name.resize(name_end);
#else
    std::vector<char> namebuf;
    namebuf.resize(MAX_PATH + 1);
    const UINT uRetVal = GetTempFileName(dir.c_str(), prefix_fname.c_str(), 0, namebuf.data());
    if (uRetVal == 0) {
      ec = last_error_();
      return;
    }

    name.assign(namebuf.data()); // zero-terminated string
#endif

    // Create the new file.
    native_handle_type handle = CreateFile(
        name.c_str(),
        GENERIC_READ | GENERIC_WRITE,
        0, // No sharing of tmp files.
        nullptr,
        CREATE_ALWAYS,
        FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE | FILE_FLAG_OVERLAPPED,
        nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
      ec = last_error_();
      DeleteFile(name.c_str()); // Clean up temporary file.
      return;
    }

    return basic_fd(handle);
  }
}

auto basic_fd::close_(const std::unique_lock<std::mutex>& lck) -> std::error_code {
  assert(lck.owns_lock() && lck.mutex() == &mtx_);
  assert(sync_queue_.empty());
  if (handle_ == invalid_native_handle) return {};

  if (!CloseHandle(handle_)) return last_error_();
  handle_ = invalid_native_handle;
  return {};
}

void basic_fd::flush([[maybe_unused]] bool data_only, std::error_code& ec) {
  std::lock_guard<std::mutex> lck(mtx_);
  ec.clear();

  if (!FlushFileBuffers(handle_)) ec = last_error_();
}

auto basic_fd::size(std::error_code& ec) const -> size_type {
  std::lock_guard<std::mutex> lck(mtx_);
  ec.clear();

  LARGE_INTEGER v;
  if (!GetFileSizeEx(handle_, &v)) ec = last_error_();
  return v;
}

void basic_fd::truncate(size_type sz, std::error_code& ec) {
  std::lock_guard<std::mutex> lck(mtx_);
  ec.clear();

  FILE_END_OF_FILE_INFO v;
  v.EndOfFile = sz;
  if (!SetFileInformationByHandle(handle_, FileEndOfFileInfo, &v, sizeof(v)))
    ec = last_error_();
}


} /* namespace earnest::detail */


#else

namespace {


static auto last_error_() -> std::error_code {
  return std::error_code(errno, std::system_category());
}


} /* namespace earnest::<unnamed> */

namespace detail {


auto basic_fd::open_(const std::filesystem::path& filename, open_mode mode, std::error_code& ec) -> basic_fd {
  ec.clear();

  int fl = 0;
#ifdef O_CLOEXEC
  fl |= O_CLOEXEC;
#endif
  switch (mode) {
    case READ_ONLY:
      fl |= O_RDONLY;
      break;
    case WRITE_ONLY:
      fl |= O_WRONLY;
      break;
    case READ_WRITE:
      fl |= O_RDWR;
      break;
  }

  basic_fd result_fd;
  result_fd.handle_ = ::open(filename.c_str(), fl);
  if (result_fd.handle_ == -1) {
    ec = last_error_();
    return {};
  }

  struct stat sb;
  auto fstat_rv = ::fstat(result_fd.handle_, &sb);
  if (fstat_rv != 0) {
    ec = last_error_();
    return {};
  }

  if (S_ISDIR(sb.st_mode)) {
    ec = std::make_error_code(std::errc::is_a_directory);
    return {};
  }
  if (!S_ISREG(sb.st_mode)) {
    ec = std::error_code(EFTYPE, std::system_category());
    return {};
  }

  return result_fd;
}

auto basic_fd::create_(const std::filesystem::path& filename, std::error_code& ec) -> basic_fd {
  ec.clear();

  int fl = O_CREAT | O_EXCL | O_RDWR;
#ifdef O_CLOEXEC
  fl |= O_CLOEXEC;
#endif

  basic_fd result_fd;
  result_fd.handle_ = ::open(filename.c_str(), fl, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
  if (result_fd.handle_ == -1) {
    ec = last_error_();
    return {};
  }

  return result_fd;
}

auto basic_fd::tmpfile_(const std::filesystem::path& prefix, std::error_code& ec) -> basic_fd {
  using namespace std::string_literals;
  static const std::string tmpl_replacement = "XXXXXX"s;

  ec.clear();

  std::filesystem::path prefix_path = prefix.native() + tmpl_replacement;
  if (!prefix_path.has_parent_path() && prefix_path.is_relative())
    prefix_path = std::filesystem::temp_directory_path() / prefix_path;
  prefix_path = std::filesystem::absolute(prefix_path);

restart:
# if __cplusplus >= 201703L // std::string::data() is modifiable
  std::string template_name = prefix_path.native();
# else
  std::vector<char> template_name;
  {
    const std::string ppstr = prefix_path.native();
    template_name.insert(template_name.end(), ppstr.begin(), ppstr.end());
  }
  template_name.push_back('\0');
# endif

#ifdef HAS_MKSTEMP
  native_handle_type handle = mkstemp(template_name.data());
  if (handle == -1) {
    ec = last_error_();
    return {};
  }
#else // HAS_MKSTEMP
  int fl = O_CREAT | O_EXCL | O_RDWR;
#ifdef O_CLOEXEC
  fl |= O_CLOEXEC;
#endif

  // This call mutates the data in 'template_name'.
  native_handle_type handle = ::open(::mktemp(template_name.data()), fl, 0666);
  if (handle == -1) {
    if (errno == EEXIST) goto restart;
    ec = last_error_();
    return {};
  }
#endif // HAS_MKSTEMP

  auto fd = basic_fd(handle);

  unlink(template_name.data());
  return fd;
}

auto basic_fd::close_(const std::unique_lock<std::mutex>& lck) -> std::error_code {
  assert(lck.owns_lock() && lck.mutex() == &mtx_);
  assert(sync_queue_.empty());
  if (handle_ == invalid_native_handle) return {};

  if (::close(handle_) != 0) return last_error_();
  handle_ = invalid_native_handle;
  return {};
}

void basic_fd::flush([[maybe_unused]] bool data_only, std::error_code& ec) {
  std::lock_guard<std::mutex> lck(mtx_);
  ec.clear();

#if _POSIX_SYNCHRONIZED_IO >= 200112L
  if (data_only) {
    if (fdatasync(native_handle())) ec = last_error_();
  } else {
    if (fsync(native_handle())) ec = last_error_();
  }
#else
  if (fsync(native_handle())) ec = last_error_();
#endif
}

auto basic_fd::size(std::error_code& ec) const -> size_type {
  std::lock_guard<std::mutex> lck(mtx_);
  ec.clear();

  struct stat sb;
  if (fstat(handle_, &sb)) ec = last_error_();
  return sb.st_size;
}

void basic_fd::truncate(size_type sz, std::error_code& ec) {
  std::lock_guard<std::mutex> lck(mtx_);
  ec.clear();

  if (::ftruncate(handle_, sz)) ec = last_error_();
}


} /* namespace earnest::detail */

#endif

namespace detail {


basic_fd::sync_queue::sync_queue(basic_fd& owner)
: owner(&owner)
{
  owner.sync_queue_.push_back(*this);

#ifdef WIN32
  std::memset(&overlapped, 0, sizeof(overlapped));
#elif __has_include(<aio.h>)
  std::memset(&iocb, 0, sizeof(iocb));
#endif
}

basic_fd::sync_queue::~sync_queue() {
  std::lock_guard<std::mutex> lck{ owner->mtx_ };
  owner->sync_queue_.erase(owner->sync_queue_.iterator_to(*this));
  owner->notif_.notify_all();
}

auto basic_fd::sync_queue::cancel() -> std::error_code {
#ifdef WIN32
  if (!CancelIoEx(owner->handle_, &overlapped)) {
    const auto err = GetLastError();
    if (err != ERROR_NOT_FOUND)
      return std::error_code(err, std::system_category());
  }
  return {};
#elif __has_include(<aio.h>)
  std::error_code ec;

  std::for_each(
      iocb.begin(), iocb.end(),
      [&ec](struct ::aiocb& iocb) {
        switch (::aio_cancel(iocb.aio_fildes, &iocb)) {
          case -1:
            if (!ec) ec = std::error_code(errno, std::system_category());
          case AIO_CANCELED: [[fallthrough]];
          case AIO_ALLDONE:
            break;
          case AIO_NOTCANCELED:
            switch (::aio_error(&iocb)) {
              case -1:
                if (errno == EINVAL) break; // Not a queued operation.
                if (!ec) ec = std::error_code(errno, std::system_category());
                break;
              case EINPROGRESS:
                if (!ec) ec = std::error_code(EINPROGRESS, std::system_category());
                break;
              default:
                break;
            }
            break;
        }
      });

  return ec;
#else
  return {};
#endif
}


auto basic_fd::cancel_(std::unique_lock<std::mutex>& lck) -> std::error_code {
  assert(lck.owns_lock() && lck.mutex() == &mtx_);

  std::error_code ec;
  std::for_each(
      sync_queue_.begin(), sync_queue_.end(),
      [&ec](sync_queue& e) {
        auto new_ec = e.cancel();
        if (new_ec && !ec) ec = std::move(new_ec);
      });
  if (ec) return ec;

  notif_.wait(lck, [this]() { return sync_queue_.empty(); });
  return {};
}

#if WIN32
auto basic_fd::read_some_at_overlapped_(offset_type offset, asio::mutable_buffer buf, OVERLAPPED& overlapped, std::error_code& ec) -> std::size_t {
  if (!ReadFile(handle_, buf.data(), buf.size(), nullptr, &overlapped)) {
    ec = last_error_();
    return 0;
  }

  DWORD bytes_transferred;
  if (!GetOverlappedResult(handle_, &overlapped, &bytes_transferred, TRUE)) {
    const auto e = GetLastError();
    switch (e) {
      default:
        ec = std::error_code(e, std::system_category());
        break;
      case ERROR_HANDLE_EOF:
        ec = asio::stream_errc::eof;
        break;
    }
    return 0;
  }

  ec.clear();
  return bytes_transferred;
}

auto basic_fd::write_some_at_overlapped_(offset_type offset, asio::const_buffer buf, OVERLAPPED& overlapped, std::error_code& ec) -> std::size_t {
  if (!WriteFile(handle_, buf.data(), buf.size(), nullptr, &overlapped)) {
    ec = last_error_();
    return 0;
  }

  DWORD bytes_transferred;
  if (!GetOverlappedResult(handle_, &overlapped, &bytes_transferred, TRUE)) {
    const auto e = GetLastError();
    switch (e) {
      default:
        ec = std::error_code(e, std::system_category());
        break;
      case ERROR_HANDLE_EOF:
        ec = asio::stream_errc::eof;
        break;
    }
    return 0;
  }

  ec.clear();
  return bytes_transferred;
}
#elif __has_include(<aio.h>)
auto basic_fd::some_aio_at_(std::vector<struct ::aiocb>& v, std::error_code& ec) -> std::size_t {
  ec.clear();

  std::vector<struct ::aiocb*> vptr(v.size());
  std::transform(
      v.begin(), v.end(), vptr.begin(),
      [](struct ::aiocb& v) {
        return (v.aio_nbytes == 0 ? nullptr : &v);
      });
  // If there is no work to be done, we must return immediately (and without error).
  if (std::all_of(
          vptr.begin(), vptr.end(),
          [](struct ::aiocb* ptr) {
            return ptr == nullptr;
          }))
    return 0;

  std::size_t bytes = 0;

restart:
  const int lio_return = lio_listio(LIO_WAIT, vptr.data(), vptr.size(), nullptr);
  const int e = (lio_return == 0 ? 0 : errno);

  if (lio_return == 0 || e == EINTR || e == EAGAIN || e == EIO) {
    bool all_done = (lio_return == 0);
    std::for_each(
        vptr.begin(), vptr.end(),
        [&bytes, &ec, &all_done](struct ::aiocb*& v) {
          if (v == nullptr) return;

          const int e = ::aio_error(v);
          switch (e) {
            case -1:
              throw std::system_error(errno, std::system_category(), "aio_error");
            case EINPROGRESS:
              throw std::system_error(EINPROGRESS, std::system_category(), "aio_error");
            case 0:
              break;
            case ECANCELED:
              {
                [[maybe_unused]] const auto rv = ::aio_return(v);
                assert(rv == -1);
              }
              ec = asio::error::operation_aborted;
              return;
            default:
              {
                [[maybe_unused]] const auto rv = ::aio_return(v);
                assert(rv == -1);
              }
              if (!ec) ec = std::error_code(e, std::system_category());
              return;
          }

          auto nbytes = ::aio_return(v);
          assert(nbytes >= 0);
          bytes += nbytes;
          v->aio_offset += nbytes;
          v->aio_buf = reinterpret_cast<void*>(reinterpret_cast<std::uintptr_t>(v->aio_buf) + nbytes);
          v->aio_nbytes -= nbytes;

          if (v->aio_nbytes == 0)
            v = nullptr;
          else if (nbytes != 0)
            all_done = false;
        });

    if (ec || all_done) {
      if (bytes == 0 && !ec) ec = asio::stream_errc::eof;
      return bytes;
    }
    goto restart;
  } else {
    ec = std::error_code(e, std::system_category());
    return 0;
  }
}
#endif


} /* namespace earnest::detail */


template class fd<>;


} /* namespace earnest */
