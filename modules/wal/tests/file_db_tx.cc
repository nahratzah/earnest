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

inline auto str_to_byte_vector(std::string_view s) -> std::vector<std::byte> {
  std::vector<std::byte> v;
  std::transform(s.begin(), s.end(),
      std::back_inserter(v),
      [](char c) -> std::byte {
        return static_cast<std::byte>(static_cast<unsigned char>(c));
      });
  return v;
}

TEST(tx) {
  const earnest::dir testdir = ensure_dir_exists_and_is_empty("tx");
  static const std::string text = "bla bla chocoladevla";

  asio::io_context ioctx;
  auto fdb = std::make_shared<earnest::file_db<asio::io_context::executor_type>>(ioctx.get_executor());
  fdb->async_create(testdir,
      [fdb](std::error_code ec) {
        REQUIRE CHECK_EQUAL(std::error_code(), ec);

        fdb->wal->async_append(
            std::initializer_list<earnest::detail::wal_file<asio::io_context::executor_type>::write_variant_type>{
              earnest::detail::wal_record_create_file{
                .file{ "", "testfile"}
              },
              earnest::detail::wal_record_truncate_file{
                .file{ "", "testfile"},
                .new_size=text.size(),
              },
              earnest::detail::wal_record_modify_file_write32{
                .file{ "", "testfile"},
                .file_offset=0,
                .data=str_to_byte_vector(text),
              },
            },
            [](std::error_code ec) {
              REQUIRE CHECK_EQUAL(std::error_code(), ec);
            });
      });
  ioctx.run();
  ioctx.restart();

  auto tx = fdb->tx_begin(earnest::isolation::repeatable_read);
  std::string contents;
  bool callback_called = false;
  tx.async_file_contents(
      earnest::file_id("", "testfile"),
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
