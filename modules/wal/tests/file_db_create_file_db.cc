#include <earnest/file_db.h>

#include "file_db.h"
#include "UnitTest++/UnitTest++.h"

#include <asio/io_context.hpp>

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
