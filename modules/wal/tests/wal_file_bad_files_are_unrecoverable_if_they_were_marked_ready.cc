#include <earnest/detail/wal_file.h>

#include "wal_file.h"
#include "UnitTest++/UnitTest++.h"

#include <iostream>
#include <algorithm>

#include <asio/io_context.hpp>

TEST(bad_files_are_unrecoverable_if_they_were_marked_ready) {
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
                earnest::detail::wal_record_noop{},
                earnest::detail::wal_record_rollover_ready{ .filename="0000000000000001.wal" },
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
        // Validation
        REQUIRE CHECK_EQUAL(make_error_code(earnest::wal_errc::unrecoverable), ec);
      });
  ioctx.run();
}
