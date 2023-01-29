#include <earnest/file_db.h>

#include "file_db.h"
#include "UnitTest++/UnitTest++.h"

#include <asio/io_context.hpp>

TEST(tx) {
  const earnest::dir testdir = ensure_dir_exists_and_is_empty("tx");

  asio::io_context ioctx;
  auto fdb = std::make_shared<earnest::file_db<asio::io_context::executor_type>>(ioctx.get_executor());
  fdb->async_create(testdir,
      [](std::error_code ec) {
        REQUIRE CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();
  ioctx.restart();

  auto tx = fdb->tx_begin(earnest::isolation::repeatable_read);
  // XXX do something?
  ioctx.run();
}
