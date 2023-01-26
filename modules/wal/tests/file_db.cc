#include <earnest/file_db.h>

#include "UnitTest++/UnitTest++.h"

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

TEST(create_file_db) {
  const earnest::dir testdir = ensure_dir_exists_and_is_empty("create_file_db");

  asio::io_context ioctx;
  auto fdb = std::make_shared<earnest::file_db<asio::io_context::executor_type>>(ioctx.get_executor());

  /*
   * Test: create a new file-db.
   */
  bool handler_was_called = false;
  fdb->async_create(testdir,
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
        handler_was_called = true;
      });
  ioctx.run();

  CHECK(handler_was_called);
  CHECK(fdb->wal != nullptr);
  CHECK(fdb->namespaces.empty());

  CHECK_EQUAL(1u, fdb->files.size());
  CHECK(fdb->files.contains(earnest::file_id("", earnest::file_db<asio::io_context::executor_type>::namespaces_filename)));
}

TEST(open_file_db) {
  const earnest::dir testdir = ensure_dir_exists_and_is_empty("open_file_db");

  { // Preparation: create file-db.
    asio::io_context ioctx;
    auto fdb = std::make_shared<earnest::file_db<asio::io_context::executor_type>>(ioctx.get_executor());
    fdb->async_create(testdir,
        [](std::error_code ec) {
          REQUIRE CHECK_EQUAL(std::error_code(), ec);
        });
    ioctx.run();
  }

  /*
   * Test: open a file-db.
   */
  bool handler_was_called = false;
  asio::io_context ioctx;
  auto fdb = std::make_shared<earnest::file_db<asio::io_context::executor_type>>(ioctx.get_executor());
  fdb->async_open(testdir,
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
        handler_was_called = true;
      });
  ioctx.run();

  CHECK(handler_was_called);
  CHECK(fdb->wal != nullptr);
  CHECK(fdb->namespaces.empty());

  CHECK_EQUAL(1u, fdb->files.size());
  CHECK(fdb->files.contains(earnest::file_id("", earnest::file_db<asio::io_context::executor_type>::namespaces_filename)));
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
