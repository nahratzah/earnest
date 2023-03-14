#include <earnest/detail/wal_file.h>

#include "wal_file.h"
#include <UnitTest++/UnitTest++.h>

#include <iostream>
#include <algorithm>

#include <asio/io_context.hpp>

TEST(reread_wal) {
  const earnest::dir testdir = ensure_dir_exists_and_is_empty("reread_wal");

  { // Preparation stage.
    asio::io_context ioctx;
    auto w = std::make_shared<earnest::detail::wal_file<asio::io_context::executor_type, std::allocator<std::byte>>>(ioctx.get_executor(), std::allocator<std::byte>());
    w->async_create(testdir,
        [](std::error_code ec) {
          REQUIRE CHECK_EQUAL(std::error_code(), ec);
        });
    ioctx.run();
  }

  /*
   * Test: reopen a previously create wal.
   */
  asio::io_context ioctx;
  auto w = std::make_shared<earnest::detail::wal_file<asio::io_context::executor_type, std::allocator<std::byte>>>(ioctx.get_executor(), std::allocator<std::byte>());
  w->async_open(testdir,
      [](std::error_code ec) {
        REQUIRE CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  /*
   * Validation.
   */
  CHECK(w->entries.empty());
  CHECK_EQUAL(0u, w->active->file->sequence);
  CHECK_EQUAL("0000000000000000.wal", w->active->file->name);
  CHECK_EQUAL(::earnest::detail::wal_file_entry_state::ready, w->active->file->state());
}
