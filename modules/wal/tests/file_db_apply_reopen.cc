#include <earnest/file_db.h>

#include "file_db.h"
#include <UnitTest++/UnitTest++.h>

#include <algorithm>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include <asio/io_context.hpp>
#include <asio/read_at.hpp>
#include <asio/use_future.hpp>

TEST(apply_reopen) {
  const std::string text = "bla bla chocoladevla";
  const earnest::file_id testfile = earnest::file_id("", "testfile");

  earnest::dir testdir = ensure_dir_exists_and_is_empty("apply_reopen");
  asio::io_context ioctx;

  { // preparation
    auto fdb = std::make_shared<earnest::file_db<asio::io_context::executor_type>>(ioctx.get_executor());
    bool rollover_done = false;
    fdb->async_create(testdir,
        [&](std::error_code ec) {
          REQUIRE CHECK_EQUAL(std::error_code(), ec);

          fdb->wal->async_append(
              std::initializer_list<earnest::detail::wal_file<asio::io_context::executor_type>::write_variant_type>{
                earnest::detail::wal_record_create_file{
                  .file=testfile,
                },
                earnest::detail::wal_record_truncate_file{
                  .file=testfile,
                  .new_size=text.size(),
                },
                earnest::detail::wal_record_modify_file_write32{
                  .file=testfile,
                  .file_offset=0,
                  .data=str_to_byte_vector(text),
                },
              },
              [&](std::error_code ec) {
                REQUIRE CHECK_EQUAL(std::error_code(), ec);

                fdb->async_wal_rollover(
                    [&](std::error_code ec) {
                      REQUIRE CHECK_EQUAL(std::error_code(), ec);
                      rollover_done = true;
                    });
              });
        });
    ioctx.run();
    ioctx.restart();
    REQUIRE CHECK(rollover_done);

    // We remove the rolled-over wal file, so that the recover protocol won't recreate the file.
    // Since this test is to see that the file is readable.
    fdb.reset();
    testdir.erase("0000000000000000.wal");
  }

  /*
   * Check that we can still read the file after rollover.
   */
  auto fdb = std::make_shared<earnest::file_db<asio::io_context::executor_type>>(ioctx.get_executor());
  fdb->async_open(testdir,
      [](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();
  ioctx.restart();

  auto tx = fdb->tx_begin(earnest::isolation::repeatable_read, earnest::tx_mode::read_only);
  std::string contents;
  bool callback_called = false;
  tx.async_file_contents(
      testfile,
      [&](std::error_code ec, std::vector<std::byte> bytes) {
        CHECK_EQUAL(std::error_code(), ec);
        callback_called = true;

        std::transform(bytes.begin(), bytes.end(),
            std::back_inserter(contents),
            [](std::byte b) -> char {
              return static_cast<char>(static_cast<unsigned char>(b));
            });
      });
  ioctx.run();

  CHECK(callback_called);
  CHECK_EQUAL(text, contents);
}
