#include <earnest/detail/wal_file.h>

#include "UnitTest++/UnitTest++.h"

#include <iostream>

#include <asio/io_context.hpp>

earnest::dir write_dir;

auto ensure_dir_exists_and_is_empty(std::filesystem::path name) -> earnest::dir {
  using namespace std::literals;

  std::cerr << "ensure_dir_exists_and_is_empty(" << name << ")\n";
  if (name.has_parent_path())
    throw std::runtime_error("directory '"s + name.string() + "' is not a relative name"s);

  bool exists = false;
  for (auto d : write_dir) {
    if (d.path() == name) exists = true;
  }

  earnest::dir new_dir;
  if (!exists)
    new_dir.create(write_dir, name);
  else
    new_dir.open(write_dir, name);

  for (auto file : new_dir) {
    if (file.path() == "." || file.path() == "..") continue;

    try {
      new_dir.erase(file);
    } catch (const std::exception& e) {
      std::cerr << "error cleaning dir'" << name << "', failed to erase '" << file.path() << "': " << e.what() << std::endl;
      throw;
    }
  }
  return new_dir;
}

TEST(create_wal) {
  const earnest::dir testdir = ensure_dir_exists_and_is_empty("create_wal");

  asio::io_context ioctx;
  auto w = earnest::detail::wal_file<asio::io_context::executor_type, std::allocator<std::byte>>(ioctx.get_executor(), std::allocator<std::byte>());

  bool handler_was_called = false;
  w.async_create(testdir,
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
        handler_was_called = true;
      });
  ioctx.run();

  CHECK(handler_was_called);
}

TEST(reread_wal) {
  const earnest::dir testdir = ensure_dir_exists_and_is_empty("reread_wal");

  { // Preparation stage.
    asio::io_context ioctx;
    auto w = earnest::detail::wal_file<asio::io_context::executor_type, std::allocator<std::byte>>(ioctx.get_executor(), std::allocator<std::byte>());
    w.async_create(testdir,
        [](std::error_code ec) {
          REQUIRE CHECK_EQUAL(std::error_code(), ec);
        });
    ioctx.run();
  }

  asio::io_context ioctx;
  auto w = earnest::detail::wal_file<asio::io_context::executor_type, std::allocator<std::byte>>(ioctx.get_executor(), std::allocator<std::byte>());
  w.async_open(testdir,
      [](std::error_code ec) {
        REQUIRE CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: " << (argc > 0 ? argv[0] : "wal_test") << " writeable_dir\n"
        <<  "writeable_dir: points at a directory where we can write files\n";
    return 1;
  }
  try {
    write_dir = earnest::dir(argv[1]);
  } catch (const std::exception& e) {
    std::cerr << "error opening dirs: " << e.what() << std::endl;
    return 1;
  }

  return UnitTest::RunAllTests();
}
