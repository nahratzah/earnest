#include <earnest/detail/wal_file.h>

#include "wal_file.h"
#include "UnitTest++/UnitTest++.h"

#include <iostream>
#include <algorithm>

#include <asio/io_context.hpp>

TEST(can_recover_from_interrupted_rollover) {
  using wal_file_t = earnest::detail::wal_file<asio::io_context::executor_type, std::allocator<std::byte>>;
  const earnest::dir testdir = ensure_dir_exists_and_is_empty("can_recover_from_interrupted_rollover");

  { // Preparation stage.
    using wal_file_entry_t = earnest::detail::wal_file_entry<asio::io_context::executor_type, std::allocator<std::byte>>;
    asio::io_context ioctx;
    auto f0 = std::make_shared<wal_file_entry_t>(ioctx.get_executor(), std::allocator<std::byte>());
    auto f1 = std::make_shared<wal_file_entry_t>(ioctx.get_executor(), std::allocator<std::byte>());
    f0->async_create(testdir, "0000000000000000.wal", 0,
        [f0](std::error_code ec, auto link_event) {
          REQUIRE CHECK_EQUAL(std::error_code(), ec);
          std::invoke(link_event, std::error_code());

          f0->async_append(std::initializer_list<wal_file_t::write_variant_type>{
                earnest::detail::wal_record_rollover_intent{ .filename="0000000000000001.wal" },
                earnest::detail::wal_record_noop{}
              },
              [](std::error_code ec) {
                REQUIRE CHECK_EQUAL(std::error_code(), ec);
              });
        });
    f1->async_create(testdir, "0000000000000001.wal", 1,
        [](std::error_code ec, auto link_event) {
          REQUIRE CHECK_EQUAL(std::error_code(), ec);
          std::invoke(link_event, std::error_code());
        });
    ioctx.run();
    f1->file.truncate(28);
  }

  /*
   * Test: recovering from a interrupted wal.
   */
  asio::io_context ioctx;
  auto w = std::make_shared<wal_file_t>(ioctx.get_executor(), std::allocator<std::byte>());
  w->async_open(testdir,
      [](std::error_code ec) {
        REQUIRE CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  /*
   * Validation.
   * The incomplete file (0000000000000001.wal) is removed during recovery.
   */
  CHECK_EQUAL("0000000000000000.wal", w->active->file->name);
  CHECK_EQUAL(::earnest::detail::wal_file_entry_state::ready, w->active->file->state());
  CHECK_EQUAL(0u, w->entries.size());

  auto wdir = w->get_dir();
  CHECK_EQUAL(0u, std::count_if(wdir.begin(), wdir.end(), [](const auto& entry) { return entry.path() == "0000000000000001.wal"; }));
}
