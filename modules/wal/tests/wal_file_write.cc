#include <earnest/detail/wal_file.h>

#include "wal_file.h"
#include "UnitTest++/UnitTest++.h"

#include <iostream>
#include <algorithm>

#include <asio/io_context.hpp>

TEST(write) {
  using wal_file_t = earnest::detail::wal_file<asio::io_context::executor_type, std::allocator<std::byte>>;
  using ::earnest::detail::wal_record_create_file;

  const earnest::dir testdir = ensure_dir_exists_and_is_empty("write");

  asio::io_context ioctx;
  auto w = std::make_shared<wal_file_t>(ioctx.get_executor(), std::allocator<std::byte>());
  w->async_create(testdir,
      [](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();
  ioctx.restart();

  /*
   * Test: write to wal.
   */
  bool append_callback_was_called = false;
  w->async_append(std::initializer_list<wal_file_t::write_variant_type>{
        wal_record_create_file{ .file{ "", "foo"} },
        wal_record_create_file{ .file{ "", "bar"} },
        wal_record_create_file{ .file{ "", "baz"} },
      },
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
        append_callback_was_called = true;
      });
  ioctx.run();
  ioctx.restart();

  /*
   * Validation.
   */
  CHECK(append_callback_was_called);
  CHECK(w->entries.empty());

  std::vector<wal_file_t::record_type> records;
  w->async_records(
      [&records](auto v) {
        records.push_back(std::move(v));
        return std::error_code();
      },
      [&records](std::error_code ec) {
        REQUIRE CHECK_EQUAL(std::error_code(), ec);

        auto expected = std::initializer_list<wal_file_t::record_type>{
          wal_record_create_file{ .file{ "", "foo"} },
          wal_record_create_file{ .file{ "", "bar"} },
          wal_record_create_file{ .file{ "", "baz"} },
        };
        CHECK(std::equal(
                expected.begin(), expected.end(),
                records.cbegin(), records.cend()));
      });
  ioctx.run();
}
