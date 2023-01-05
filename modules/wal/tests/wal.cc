#include <earnest/detail/wal_file.h>

#include "UnitTest++/UnitTest++.h"

#include <iostream>
#include <system_error>
#include <string>
#include <string_view>
#include <sstream>
#include <iomanip>

#include <asio/io_context.hpp>

earnest::dir source_files, write_dir;

auto hex_string(std::string_view sv) -> std::string {
  using namespace std::literals;

  std::ostringstream out;
  out << std::setfill('0');
  bool first = true;
  for (char c : sv) {
    if (!std::exchange(first, false)) out << " "sv;
    if (c < 32 || c >= 127) {
      out << "0x"sv << std::setw(2) << std::hex << static_cast<unsigned int>(static_cast<unsigned char>(c));
    } else {
      out << " '"sv << std::string_view(&c, 1) << "'"sv;
    }
  }

  return out.str();
}

TEST(read_empty_wal_file_entry) {
  asio::io_context ioctx;
  auto f = earnest::detail::wal_file_entry<asio::io_context::executor_type>(ioctx.get_executor());
  f.async_open(source_files, "empty",
      [](std::error_code ec) {
        CHECK(ec);
      });
  ioctx.run();
}

TEST(read_wal_file_entry) {
  asio::io_context ioctx;
  auto f = earnest::detail::wal_file_entry<asio::io_context::executor_type>(ioctx.get_executor());
  f.async_open(source_files, "version_0",
      [](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(0, f.version);
  CHECK_EQUAL(17, f.sequence);
  CHECK_EQUAL(32, f.write_offset());
  CHECK_EQUAL(28, f.link_offset());
}

TEST(write_wal_file_entry) {
  using namespace std::string_literals;

  // We have to make sure the file doesn't exist, or the test will fail.
  write_dir.erase("wal_19");

  asio::io_context ioctx;
  auto f = earnest::detail::wal_file_entry<asio::io_context::executor_type>(ioctx.get_executor());
  f.async_create(write_dir, "wal_19", 19,
      [](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(f.file.is_open());
  CHECK_EQUAL(earnest::detail::wal_file_entry<asio::io_context::executor_type>::max_version, f.version);
  CHECK_EQUAL(19, f.sequence);
  CHECK_EQUAL(32, f.write_offset());
  CHECK_EQUAL(28, f.link_offset());

  CHECK_EQUAL(
      hex_string("\013\013earnest.wal\000\000\000\000\000\000\000\000\000\000\000\000\000\000\023\000\000\000\000"s),
      hex_string(f.file.contents<std::string>()));
}

int main(int argc, char** argv) {
  if (argc < 3) {
    std::cerr << "Usage:  " << (argc > 0 ? argv[0] : "wal_test") << " wal_source_dir writeable_dir\n"
        << "  wal_source_dir:  points at the directory containing test files\n"
        << "  writeable_dir:   points at a directory where we can write files\n";
    return 1;
  }
  try {
    source_files = earnest::dir(argv[1]);
    write_dir = earnest::dir(argv[2]);
  } catch (const std::exception& e) {
    std::cerr << "error opening dirs: " << e.what() << std::endl;
    return 1;
  }

  return UnitTest::RunAllTests();
}
