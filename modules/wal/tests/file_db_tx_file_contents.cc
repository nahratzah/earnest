#include <earnest/file_db.h>

#include "file_db.h"
#include "UnitTest++/UnitTest++.h"

#include <algorithm>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include <asio/io_context.hpp>
#include <asio/use_future.hpp>

class tx_file_contents {
  public:
  tx_file_contents();

  const std::string text = "bla bla chocoladevla";
  const earnest::file_id testfile = earnest::file_id("", "testfile");
  asio::io_context ioctx;
  std::shared_ptr<earnest::file_db<asio::io_context::executor_type>> fdb;
};

TEST_FIXTURE(tx_file_contents, read_contents) {
  auto tx = fdb->tx_begin(earnest::isolation::repeatable_read);
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

TEST_FIXTURE(tx_file_contents, read_fails_when_not_permitted) {
  auto tx = fdb->tx_begin(earnest::isolation::repeatable_read, earnest::tx_mode::write_only);
  bool callback_called = false;
  tx.async_file_contents(
      testfile,
      [&](std::error_code ec, [[maybe_unused]] std::vector<std::byte> bytes) {
        CHECK_EQUAL(make_error_code(earnest::file_db_errc::read_not_permitted), ec);
        callback_called = true;
      });
  ioctx.run();

  CHECK(callback_called);
}

inline auto str_to_byte_vector(std::string_view s) -> std::vector<std::byte> {
  std::vector<std::byte> v;
  std::transform(s.begin(), s.end(),
      std::back_inserter(v),
      [](char c) -> std::byte {
        return static_cast<std::byte>(static_cast<unsigned char>(c));
      });
  return v;
}

tx_file_contents::tx_file_contents() {
  const earnest::dir testdir = ensure_dir_exists_and_is_empty("tx_file_contents");
  fdb = std::make_shared<earnest::file_db<asio::io_context::executor_type>>(ioctx.get_executor());

  fdb->async_create(testdir,
      [this](std::error_code ec) {
        REQUIRE CHECK_EQUAL(std::error_code(), ec);

        this->fdb->wal->async_append(
            std::initializer_list<earnest::detail::wal_file<asio::io_context::executor_type>::write_variant_type>{
              earnest::detail::wal_record_create_file{
                .file=this->testfile,
              },
              earnest::detail::wal_record_truncate_file{
                .file=this->testfile,
                .new_size=this->text.size(),
              },
              earnest::detail::wal_record_modify_file_write32{
                .file=this->testfile,
                .file_offset=0,
                .data=str_to_byte_vector(this->text),
              },
            },
            [](std::error_code ec) {
              REQUIRE CHECK_EQUAL(std::error_code(), ec);
            });
      });
  ioctx.run();
  ioctx.restart();
}
