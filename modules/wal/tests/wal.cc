#include <earnest/detail/wal_file.h>

#include "UnitTest++/UnitTest++.h"

#include <iostream>
#include <system_error>

#include <asio/io_context.hpp>

earnest::dir source_files;

TEST(read_empty_wal_file_entry) {
  asio::io_context ioctx;
  auto f = earnest::detail::wal_file_entry<asio::io_context::executor_type>(ioctx.get_executor());
  f.async_open(source_files, "empty",
      [](std::error_code ec) {
        CHECK_EQUAL(make_error_code(std::errc::bad_file_descriptor), ec); // XXX
      });
  ioctx.run();
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "require directory argument pointing to the directory containing test files\n";
    return 1;
  }
  source_files = earnest::dir(argv[1], earnest::dir::READ_ONLY);

  return UnitTest::RunAllTests();
}
