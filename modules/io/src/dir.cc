#include <earnest/dir.h>

#ifdef WIN32
# include <Windows.h>
# include <NtDef.h>
# include <Wdm.h>
# include <string_view>
#else
# include <cassert>
# include <cerrno>
# include <fcntl.h>
# include <sys/stat.h>
# include <unistd.h>
#endif

#include <system_error>

namespace earnest {


#ifdef WIN32

namespace {


static auto last_error_() -> std::error_code {
  return std::error_code(::GetLastError(), std::system_category());
}

static auto ntdll_() -> HINSTANCE {
  static const HINSTANCE ntdll_handle = ::GetModuleHandleA(TEXT("NtDll.dll"));
  if (ntdll_handle == nullptr) throw std::system_error(last_error_());
  return ntdll_handle;
}

template<typename FunctionType>
static auto get_ntdll_proc_(const char* fn_name, bool allow_null = false) -> FunctionType {
  const FunctionType impl = static_cast<FunctionType>(::GetProcAddress(ntdll_(), fn_name));
  if (!allow_null && impl == nullptr) throw std::system_error(last_error_());
  return impl;
}

static auto rtl_nt_status_to_dos_error_(NTSTATUS status) -> ULONG {
  using RtlNtStatusToDosErrorFn = ULONG (*)(NTSTATUS Status);

  static const RtlNtStatusToDosErrorFn impl = get_ntdll_proc_<RtlNtStatusToDosErrorFn>("RtlNtStatusToDosError");
  return impl(status);
}

static auto rtl_nt_status_to_error_code_(NTSTATUS status) -> std::error_code {
  if (status == STATUS_SUCCESS) return {};
  return std::error_code(rtl_nt_status_to_dos_error_(status), std::system_category());
}

static auto nt_create_file_(
    PHANDLE            FileHandle,
    ACCESS_MASK        DesiredAccess,
    POBJECT_ATTRIBUTES ObjectAttributes,
    PIO_STATUS_BLOCK   IoStatusBlock,
    PLARGE_INTEGER     AllocationSize,
    ULONG              FileAttributes,
    ULONG              ShareAccess,
    ULONG              CreateDisposition,
    ULONG              CreateOptions,
    PVOID              EaBuffer,
    ULONG              EaLength)
-> NTSTATUS {
  using NtCreateFileFn = NTAPI NTSTATUS (*)(
    PHANDLE            FileHandle,
    ACCESS_MASK        DesiredAccess,
    POBJECT_ATTRIBUTES ObjectAttributes,
    PIO_STATUS_BLOCK   IoStatusBlock,
    PLARGE_INTEGER     AllocationSize,
    ULONG              FileAttributes,
    ULONG              ShareAccess,
    ULONG              CreateDisposition,
    ULONG              CreateOptions,
    PVOID              EaBuffer,
    ULONG              EaLength
  );

  static const NtCreateFileFn impl = get_ntdll_proc_<NtCreateFileFn>("NtCreateFile");
  return impl(FileHandle, DesiredAccess, ObjectAttributes, IoStatusBlock, AllocationSize, FileAttributes, ShareAccess, CreateDisposition, CreateOptions, EaBuffer, EaLength);
}

static auto nt_close_(
    HANDLE Handle)
-> NTSTATUS {
  using NtCloseFn = NTAPI NTSTATUS (*)(HANDLE Handle);

  static const NtCloseFn impl = get_ntdll_proc_<NtCloseFn>("NtClose");
  return impl(Handle);
}

static auto nt_set_information_file_(
    HANDLE                 FileHandle,
    PIO_STATUS_BLOCK       IoStatusBlock,
    PVOID                  FileInformation,
    ULONG                  Length,
    FILE_INFORMATION_CLASS FileInformationClass)
-> NTSTATUS {
  using NtSetInformationFileFn = NTAPI NTSTATUS (*)(
    HANDLE                 FileHandle,
    PIO_STATUS_BLOCK       IoStatusBlock,
    PVOID                  FileInformation,
    ULONG                  Length,
    FILE_INFORMATION_CLASS FileInformationClass
  );

  static const NtSetInformationFileFn impl = get_ntdll_proc_<NtSetInformationFileFn>("NtSetInformationFile");
  return impl(FileHandle, IoStatusBlock, FileInformation, Length, FileInformationClass);
}


} /* namespace earnest::<unnamed> */


auto dir::open_(const std::filesystem::path& dirname, std::error_code& ec) -> dir {
  ec.clear();

  DWORD dwDesiredAccess = GENERIC_READ | GENERIC_WRITE;
  dir d;
  d.handle_ = ::CreateFile(
      filename.c_str(),
      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
      nullptr,
      OPEN_EXISTING,
      FILE_FLAG_BACKUP_SEMANTICS,
      nullptr);
  if (!d) {
    ec = last_error_();
    return d;
  }

  FILE_BASIC_INFO basic_info;
  ::GetFileInformationByHandle(d.native_handle(), FileBasicInfo, &basic_info, sizeof(basic_info));
  if (!(basic_info.FileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
    ec = std::make_error_code(std::errc::not_a_directory);
    d.close();
  }

  return d;
}

auto dir::open_(const dir& parent, const std::filesystem::path& dirname, std::error_code& ec) -> dir {
  dir d;

  if (!parent) {
    ec = std::make_error_code(std::errc::bad_file_descriptor);
    return d;
  }

  // Build up the unicode string for the dirname.
  UNICODE_STRING dirname_ustring;
  dirname_ustring.Buffer = dirname.c_str();
  dirname_ustring.MaximumLength = dirname_ustring.Length = std::wstring_view(dirname_ustring.Buffer).length();
  // Set up create-file attributes.
  // We mark case-insensitive matching, to match the usual windows behaviour.
  OBJECT_ATTRIBUTES obj_attributes;
  InitializeObjectAttributes(&obj_attributes, &dirname_ustring, OBJ_CASE_INSENSITIVE, parent.handle_, nullptr);
  // Output parameter.
  // We don't actually care about its values.
  IO_STATUS_BLOCK io_status_block;

  ec = rtl_nt_status_to_error_code_(
      nt_create_file_(
          &d.handle_,
          FILE_READ_ATTRIBUTES | FILE_LIST_DIRECTORY | FILE_TRAVERSE,
          &obj_attributes,
          &io_status_block,
          /* AllocationSize */ 0,
          FILE_ATTRIBUTE_NORMAL,
          FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
          FILE_OPEN,
          FILE_DIRECTORY_FILE,
          /* EaBuffer */ nullptr,
          /* EaLength */ 0
          ));
  return d;
}

auto dir::create_(const std::filesystem::path& dirname, std::error_code& ec) -> dir {
  dir d;

  // Build up the unicode string for the dirname.
  UNICODE_STRING dirname_ustring;
  dirname_ustring.Buffer = dirname.c_str();
  dirname_ustring.MaximumLength = dirname_ustring.Length = std::wstring_view(dirname_ustring.Buffer).length();
  // Set up create-file attributes.
  // We mark case-insensitive matching, to match the usual windows behaviour.
  OBJECT_ATTRIBUTES obj_attributes;
  InitializeObjectAttributes(&obj_attributes, &dirname_ustring, OBJ_CASE_INSENSITIVE, nullptr, nullptr);
  // Output parameter.
  // We don't actually care about its values.
  IO_STATUS_BLOCK io_status_block;

  ec = rtl_nt_status_to_error_code_(
      nt_create_file_(
          &d.handle_,
          FILE_READ_ATTRIBUTES | FILE_LIST_DIRECTORY | FILE_TRAVERSE,
          &obj_attributes,
          &io_status_block,
          /* AllocationSize */ 0,
          FILE_ATTRIBUTE_NORMAL,
          FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
          FILE_CREATE,
          FILE_DIRECTORY_FILE,
          /* EaBuffer */ nullptr,
          /* EaLength */ 0
          ));
  return d;
}

auto dir::create_(const dir& parent, const std::filesystem::path& dirname, std::error_code& ec) -> dir {
  dir d;

  if (!parent) {
    ec = std::make_error_code(std::errc::bad_file_descriptor);
    return d;
  }

  // Build up the unicode string for the dirname.
  UNICODE_STRING dirname_ustring;
  dirname_ustring.Buffer = dirname.c_str();
  dirname_ustring.MaximumLength = dirname_ustring.Length = std::wstring_view(dirname_ustring.Buffer).length();
  // Set up create-file attributes.
  // We mark case-insensitive matching, to match the usual windows behaviour.
  OBJECT_ATTRIBUTES obj_attributes;
  InitializeObjectAttributes(&obj_attributes, &dirname_ustring, OBJ_CASE_INSENSITIVE, parent.handle_, nullptr);
  // Output parameter.
  // We don't actually care about its values.
  IO_STATUS_BLOCK io_status_block;

  ec = rtl_nt_status_to_error_code_(
      nt_create_file_(
          &d.handle_,
          FILE_READ_ATTRIBUTES | FILE_LIST_DIRECTORY | FILE_TRAVERSE,
          &obj_attributes,
          &io_status_block,
          /* AllocationSize */ 0,
          FILE_ATTRIBUTE_NORMAL,
          FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
          FILE_CREATE,
          FILE_DIRECTORY_FILE,
          /* EaBuffer */ nullptr,
          /* EaLength */ 0
          ));
  return d;
}

auto dir::close_() -> std::error_code {
  if (handle_ == invalid_native_handle) return {};

  std::error_code ec = rtl_nt_status_to_error_code_(nt_close_(handle_));
  if (!ec) handle_ = invalid_native_handle;
  return ec;
}

auto dir::erase_(const std::filesystem::path& name) -> std::error_code {
  dir tmp;

  // Build up the unicode string for the dirname.
  UNICODE_STRING name_ustring;
  name_ustring.Buffer = name.c_str();
  name_ustring.MaximumLength = name_ustring.Length = std::wstring_view(name_ustring.Buffer).length();
  // Set up create-file attributes.
  // We mark case-insensitive matching, to match the usual windows behaviour.
  OBJECT_ATTRIBUTES obj_attributes;
  InitializeObjectAttributes(&obj_attributes, &name_ustring, OBJ_CASE_INSENSITIVE, parent.handle_, nullptr);
  // Output parameter.
  // We don't actually care about its values.
  IO_STATUS_BLOCK io_status_block;

  ec = rtl_nt_status_to_error_code_(
      nt_create_file_(
          &tmp.handle_,
          FILE_READ_ATTRIBUTES | FILE_LIST_DIRECTORY | FILE_TRAVERSE,
          &obj_attributes,
          &io_status_block,
          /* AllocationSize */ 0,
          FILE_ATTRIBUTE_NORMAL,
          FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
          FILE_OPEN,
          FILE_DIRECTORY_FILE,
          /* EaBuffer */ nullptr,
          /* EaLength */ 0
          ));
  if (ec) return ec;

  FILE_DISPOSITION_INFORMATION fdi;
  fdi.DeleteFile = TRUE;
  ec = rtl_nt_status_to_error_code(
      nt_set_information_file_(
          tmp.handle_,
          &io_status_block,
          &fdi,
          sizeof(fdi),
          FileDispositionInformation
          ));
  if (ec) return ec;

  ec = tmp.close_();
  return ec;
}


dir::iterator::iterator(const dir& d)
: iterator()
{
  // If we clone the handle, it will share its iteration position with the
  // original file descriptor. This means that moving this
  // iterator would move all iterators that currently exist.
  // We want iterators to be independent, so we require an independent copy
  // of the descriptor. We achieve that by opening the same directory.
  dir fd_copy;
  fd_copy.open(d, ".", READ_ONLY);

  // Now move the handle to the iterator.
  std::swap(handle_, fd_copy.handle_);

  query_(true);
}

auto dir::iterator::operator*() const -> entry {
  entry e;
  e.path_.assign(&fdi_->FileName[0], fdi_->FileNameLength);
  e.file_size_ = fdi_->EndOfFile;
  e.hard_link_count_ = 1;
  e.exists_ = true;
  e.is_block_file_ = false;
  e.is_character_file_ = false;
  e.is_directory_ = ((fdi_->FileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0);
  e.is_fifo_ = false;
  e.is_other_ = false;
  e.is_regular_file_ = ((fdi_->FileAttributes & FILE_ATTRIBUTE_DIRECTORY) == 0);
  e.is_socket_ = false;
  e.is_symlink = false;
  return e;
}

void dir::iterator::close_() noexcept {
  if (handle_ != invalid_native_handle) nt_close_(handle_);
  handle_ = invalid_native_handle;
}

void dir::iterator::query_(bool first) {
  using NtQueryDirectoryFileExFn = NTAPI NTSTATUS (*)(
    HANDLE                 FileHandle,
    HANDLE                 Event,
    PIO_APC_ROUTINE        ApcRoutine,
    PVOID                  ApcContext,
    PIO_STATUS_BLOCK       IoStatusBlock,
    PVOID                  FileInformation,
    ULONG                  Length,
    FILE_INFORMATION_CLASS FileInformationClass,
    ULONG                  QueryFlags,
    PUNICODE_STRING        FileName
  );
  using NtQueryDirectoryFileFn = NTAPI NTSTATUS (*)(
    HANDLE                 FileHandle,
    HANDLE                 Event,
    PIO_APC_ROUTINE        ApcRoutine,
    PVOID                  ApcContext,
    PIO_STATUS_BLOCK       IoStatusBlock,
    PVOID                  FileInformation,
    ULONG                  Length,
    FILE_INFORMATION_CLASS FileInformationClass,
    BOOLEAN                ReturnSingleEntry,
    PUNICODE_STRING        FileName,
    BOOLEAN                RestartScan
  );

  static const NtQueryDirectoryFileExFn NtQueryDirectoryFileEx = get_ntdll_proc_<NtQueryDirectoryFileExFn>("NtQueryDirectoryFileEx", true);
  static const NtQueryDirectoryFileFn NtQueryDirectoryFile = get_ntdll_proc_<NtQueryDirectoryFileFn>("NtQueryDirectoryFile");

  ULONG queryFlags = SL_RETURN_SINGLE_ENTRY;
  if (first) {
    queryFlags |= SL_RESTART_SCAN;
    io_status_block_ = new_io_status_block_();
  }

restart:
  // Populate with initial entry.
  FILE_DIRECTORY_INFORMATION fdi;

  IO_STATUS_BLOCK io_status_block;
  NTSTATUS status;
  std::string_view function_name;
  if (NtQueryDirectoryFileEx != nullptr) {
    function_name = "NtQueryDirectoryFileEx";
    status = NtQueryDirectoryFileEx(
        handle_,
        /*Event*/ nullptr,
        /*ApcRoutine*/ nullptr,
        /*ApcContext*/ nullptr,
        &io_status_block,
        &fdi,
        sizeof(*fdi) + fdi_extra_bytes_,
        ::FileDirectoryInformation,
        queryFlags,
        /*FileName*/ nullptr);
  } else {
    function_name = "NtQueryDirectoryFile";
    status = NtQueryDirectoryFile(
        handle_,
        /*Event*/ nullptr,
        /*ApcRoutine*/ nullptr,
        /*ApcContext*/ nullptr,
        &io_status_block,
        &fdi,
        sizeof(*fdi) + fdi_extra_bytes_,
        ::FileDirectoryInformation,
        /*ReturnSingleEntry*/ true,
        /*FileName*/ nullptr,
        /*RestartScan*/ first);
  }

  switch (status) {
    default:
      throw std::system_error(rtl_nt_status_to_error_code_(status), function_name);
    case STATUS_SUCCESS:
      break;
    case STATUS_NO_MORE_FILES:
      close_();
      break;
    case STATUS_BUFFER_OVERFLOW:
      new_io_status_block_(fdi.Information);
      goto restart;
  }
}

void dir::iterator::new_io_status_block_(std::size_t extra_bytes) {
  if (extra_bytes > SIZE_MAX - sizeof(FILE_DIRECTORY_INFORMATION)) throw std::overflow_error("malloc(IO_STATUS_BLOCK): too many extra bytes");

  auto ptr = std::unique_ptr<FILE_DIRECTORY_INFORMATION, up_free_>(
      static_cast<PFILE_DIRECTORY_INFORMATION>(::malloc(sizeof(FILE_DIRECTORY_INFORMATION) + extra_bytes)));
  if (ptr == nullptr) throw std::bad_alloc();
  fdi_extra_bytes_ = extra_bytes;
  fdi_ = std::move(ptr);
}


#else


namespace {


static auto last_error_() -> std::error_code {
  return std::error_code(errno, std::generic_category());
}


} /* namespace earnest::<unnamed> */


dir::dir(const dir& other)
: dir()
{
  if (other.handle_ != invalid_native_handle) {
    handle_ = ::dup(other.handle_);
    if (handle_ == -1) [[unlikely]]
      throw std::system_error(last_error_());
  }
}

auto dir::operator=(const dir& other) -> dir& {
  using std::swap;

  dir copy = other;
  swap(handle_, copy.handle_);
  return *this;
}

auto dir::open_(const std::filesystem::path& dirname, std::error_code& ec) -> dir {
  ec.clear();

  int fl = O_DIRECTORY | O_RDONLY;
#ifdef O_CLOEXEC
  fl |= O_CLOEXEC;
#endif

  dir d;
  d.handle_ = ::open(dirname.c_str(), fl);
  if (d.handle_ == -1) ec = last_error_();
  return d;
}

auto dir::open_(const dir& parent, const std::filesystem::path& dirname, std::error_code& ec) -> dir {
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
  if (d.handle_ == -1) ec = last_error_();
  return d;
}

auto dir::create_(const std::filesystem::path& dirname, std::error_code& ec) -> dir {
  dir d;

  ec.clear();

  int fl = O_CREAT | O_EXCL | O_RDWR | O_DIRECTORY;
#ifdef O_CLOEXEC
  fl |= O_CLOEXEC;
#endif

  d.handle_ = ::open(dirname.c_str(), fl);
  if (d.handle_ == -1) ec = last_error_();
  return d;
}

auto dir::create_(const dir& parent, const std::filesystem::path& dirname, std::error_code& ec) -> dir {
  dir d;

  if (!parent) {
    ec = std::make_error_code(std::errc::bad_file_descriptor);
    return d;
  }

  ec.clear();

  int fl = O_CREAT | O_EXCL | O_RDWR | O_DIRECTORY;
#ifdef O_CLOEXEC
  fl |= O_CLOEXEC;
#endif

  d.handle_ = ::openat(parent.handle_, dirname.c_str(), fl);
  if (d.handle_ == -1) ec = last_error_();
  return d;
}

auto dir::close_() -> std::error_code {
  if (handle_ == invalid_native_handle) return {};

  if (::close(handle_) != 0) return last_error_();
  handle_ = invalid_native_handle;
  return {};
}

auto dir::erase_(const std::filesystem::path& name) -> std::error_code {
  if (handle_ == invalid_native_handle)
    return std::make_error_code(std::errc::bad_file_descriptor);

  struct stat sb;
  if (::fstatat(handle_, name.c_str(), &sb, AT_SYMLINK_NOFOLLOW) != 0)
    return last_error_();
  if (::unlinkat(handle_, name.c_str(), S_ISDIR(sb.st_mode) ? AT_REMOVEDIR : 0) != 0)
    return last_error_();
  return {};
}


dir::iterator::iterator(const dir& d)
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
  if (handle_ == nullptr) throw std::system_error(last_error_(), "fdopendir");
  fd_copy.handle_ = invalid_native_handle; // Claim for ourselves.

  // Load the `begin()` state.
  query_(true);
}

void dir::iterator::query_([[maybe_unused]] bool first) {
  assert(handle_ != nullptr); // undefined behaviour

  dp_ = ::readdir(handle_);
  if (dp_ == nullptr) close_();
}

void dir::iterator::close_() noexcept {
  if (handle_ != nullptr) ::closedir(handle_);
  handle_ = nullptr;
}

#endif


} /* namespace earnest */
