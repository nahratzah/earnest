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
#include <vector>
#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>
#include <boost/filesystem.hpp>

namespace earnest {


void fd::open(const std::string& filename, open_mode mode) {
  boost::system::error_code ec;
  open(filename, mode, ec);
  if (ec) throw boost::system::system_error(ec, "file IO error");
}

void fd::create(const std::string& filename) {
  boost::system::error_code ec;
  create(filename, ec);
  if (ec) throw boost::system::system_error(ec, "file IO error");
}

void fd::tmpfile(const std::string& prefix) {
  boost::system::error_code ec;
  tmpfile(prefix, ec);
  if (ec) throw boost::system::system_error(ec, "file IO error");
}

void fd::flush(bool data_only) {
  boost::system::error_code ec;
  flush(data_only, ec);
  if (ec) throw boost::system::system_error(ec, "file IO error");
}

auto fd::size() const -> size_type {
  boost::system::error_code ec;
  auto s = size(ec);
  if (ec) throw boost::system::system_error(ec, "file IO error");
  return s;
}

void fd::truncate(size_type sz) {
  boost::system::error_code ec;
  truncate(sz, ec);
  if (ec) throw boost::system::system_error(ec, "file IO error");
}


#ifdef WIN32

namespace {


static auto last_error_() -> boost::system::error_code {
  return boost::system::error_code(GetLastError(), boost::system::system_category());
}

static auto tmpdir_(error_code& ec) -> std::string {
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


void fd::open(const std::string& filename, open_mode mode, boost::system::error_code& ec) {
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

  native_handle_type handle = CreateFile(
      filename.c_str(),
      dwDesiredAccess,
      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
      nullptr,
      OPEN_EXISTING,
      FILE_ATTRIBUTE_NORMAL,
      nullptr);
  if (handle == INVALID_HANDLE_VALUE) {
    ec = last_error_();
    return;
  }

  impl_.assign(handle);
};

void fd::create(const std::string& filename, boost::system::error_code& ec) {
  native_handle_type handle = CreateFile(
      filename.c_str(),
      GENERIC_READ | GENERIC_WRITE,
      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
      nullptr,
      CREATE_NEW,
      FILE_ATTRIBUTE_NORMAL,
      nullptr);
  if (handle == INVALID_HANDLE_VALUE) {
    ec = last_error_();
    return;
  }

  impl_.assign(handle);
}

void fd::tmpfile(const std::string& prefix, boost::system::error_code& ec) {
  boost::filesystem::path prefix_path = prefix;

  // Figure out the directory to use.
  const std::string dir = (prefix_path.has_parent_path() ? prefix_path.parent() : tmpdir_(ec));
  if (!prefix.has_parent_path() && ec) return;
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
        FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE,
        nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
      ec = last_error_();
      DeleteFile(name.c_str()); // Clean up temporary file.
      return;
    }

    impl_.assign(handle);
    return;
  }
}

void fd::flush([[maybe_unused]] bool data_only, boost::system::error_code& ec) {
  if (!FlushFileBuffers(native_handle())) ec = last_error_();
}

auto fd::size(boost::system::error_code& ec) const -> size_type {
  LARGE_INTEGER v;

  if (!GetFileSizeEx(const_cast<fd&>(*this).native_handle(), &v)) ec = last_error_();
  return v;
}

void fd::truncate(size_type sz, boost::system::error_code& ec) {
  FILE_END_OF_FILE_INFO v;
  v.EndOfFile = sz;
  if (!SetFileInformationByHandle(handle_, FileEndOfFileInfo, &v, sizeof(v)))
    ec = last_error_();
}

#else

namespace {


static auto last_error_() -> boost::system::error_code {
  return boost::system::error_code(errno, boost::system::system_category());
}


} /* namespace earnest::<unnamed> */

void fd::open(const std::string& filename, open_mode mode, boost::system::error_code& ec) {
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

  native_handle_type handle = ::open(filename.c_str(), fl);
  if (handle == -1) {
    ec = last_error_();
    return;
  }

  struct stat sb;
  auto fstat_rv = ::fstat(handle, &sb);
  if (fstat_rv != 0) {
    ec = last_error_();
    ::close(handle);
    return;
  }

  if (!S_ISREG(sb.st_mode)) {
    ec = boost::system::error_code(EFTYPE, boost::system::system_category());
    ::close(handle);
    return;
  }

  impl_.assign(handle);
}

void fd::create(const std::string& filename, boost::system::error_code& ec) {
  int fl = O_CREAT | O_EXCL | O_RDWR;
#ifdef O_CLOEXEC
  fl |= O_CLOEXEC;
#endif

  native_handle_type handle = ::open(filename.c_str(), fl);
  if (handle == -1) {
    ec = last_error_();
    return;
  }

  impl_.assign(handle);
}

void fd::tmpfile(const std::string& prefix, boost::system::error_code& ec) {
  using namespace std::string_literals;
  static const std::string tmpl_replacement = "XXXXXX"s;

  boost::filesystem::path prefix_path = prefix + tmpl_replacement;
  if (!prefix_path.has_parent_path() && prefix_path.is_relative())
    prefix_path = boost::filesystem::temp_directory_path() / prefix_path;
  prefix_path = boost::filesystem::absolute(prefix_path);

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
    return;
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
    return;
  }
#endif // HAS_MKSTEMP

  unlink(template_name.data());
  impl_.assign(handle);
}

void fd::flush([[maybe_unused]] bool data_only, boost::system::error_code& ec) {
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

auto fd::size(boost::system::error_code& ec) const -> size_type {
  struct stat sb;

  if (fstat(const_cast<fd&>(*this).native_handle(), &sb)) ec = last_error_();
  return sb.st_size;
}

void fd::truncate(size_type sz, boost::system::error_code& ec) {
  if (::ftruncate(native_handle(), sz)) ec = last_error_();
}

#endif


} /* namespace earnest */
