#include <earnest/file_db.h>

#include "file_db.h"
#include "UnitTest++/UnitTest++.h"

#include <asio/io_context.hpp>
#include <asio/use_future.hpp>

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
  std::vector<std::byte> contents;
  bool callback_called = false;
  tx.async_file_contents(
      earnest::file_id("", std::string(earnest::file_db<asio::io_context::executor_type>::namespaces_filename)),
      [&](std::error_code ec, std::vector<std::byte> bytes) {
        CHECK_EQUAL(std::error_code(), ec);
        contents = std::move(bytes);
        callback_called = true;
      });
  ioctx.run();

  CHECK(callback_called);
  CHECK(!contents.empty());
}
