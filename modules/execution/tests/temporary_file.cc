#include "temporary_file.h"

#include <cstdlib>
#include <span>
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>

temporary_file::temporary_file()
: filename("/tmp/earnest_execution_test.XXXXXX")
{
  fd = ::mkstemp(filename.data());
}

temporary_file::~temporary_file() {
  if (fd != -1) ::unlink(filename.data());
}

auto temporary_file::get_contents() const -> std::string {
  struct ::stat sb;
  if (::fstat(fd, &sb) == -1)
    throw std::system_error(std::error_code(errno, std::generic_category()));

  std::string buffer_str;
  buffer_str.resize(sb.st_size);
  auto buffer = std::as_writable_bytes(std::span(buffer_str.data(), buffer_str.size()));
  if (::lseek(fd, 0, SEEK_SET) == -1)
    throw std::system_error(std::error_code(errno, std::generic_category()));

  while (!buffer.empty()) {
    const auto rlen = ::read(fd, buffer.data(), buffer.size());
    if (rlen == -1)
      throw std::system_error(std::error_code(errno, std::generic_category()));
    buffer = buffer.subspan(static_cast<std::size_t>(rlen));
  }

  return buffer_str;
}

auto temporary_file::set_contents(std::string_view new_contents) -> void {
  if (::ftruncate(fd, 0) == -1)
    throw std::system_error(std::error_code(errno, std::generic_category()));

  auto buffer = std::as_bytes(std::span(new_contents.data(), new_contents.size()));
  while (!buffer.empty()) {
    const auto wlen = ::write(fd, buffer.data(), buffer.size());
    if (wlen == -1)
      throw std::system_error(std::error_code(errno, std::generic_category()));
    buffer = buffer.subspan(static_cast<std::size_t>(wlen));
  }

  // set the file-offset to the start of the file
  if (::lseek(fd, 0, SEEK_SET) == -1)
    throw std::system_error(std::error_code(errno, std::generic_category()));
}
