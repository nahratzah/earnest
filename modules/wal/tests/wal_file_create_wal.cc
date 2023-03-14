#include <earnest/detail/wal_file.h>

#include "wal_file.h"
#include <UnitTest++/UnitTest++.h>

#include <iostream>
#include <algorithm>

#include <asio/io_context.hpp>

TEST(create_wal) {
  const earnest::dir testdir = ensure_dir_exists_and_is_empty("create_wal");

  asio::io_context ioctx;
  auto w = std::make_shared<earnest::detail::wal_file<asio::io_context::executor_type, std::allocator<std::byte>>>(ioctx.get_executor(), std::allocator<std::byte>());

  /*
   * Test: create a new wal.
   */
  bool handler_was_called = false;
  w->async_create(testdir,
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
        handler_was_called = true;
      });
  ioctx.run();

  CHECK(handler_was_called);

  /*
   * Validation.
   */
  CHECK(w->entries.empty());
  CHECK_EQUAL(0u, w->active->file->sequence);
  CHECK_EQUAL("0000000000000000.wal", w->active->file->name);
  CHECK_EQUAL(::earnest::detail::wal_file_entry_state::ready, w->active->file->state());
}
