#include <earnest/file_db.h>

#include "file_db.h"
#include "UnitTest++/UnitTest++.h"

#include <algorithm>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include <asio/io_context.hpp>
#include <asio/read_at.hpp>
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

TEST_FIXTURE(tx_file_contents, cannot_read_contents_on_nonexistent_file) {
  auto tx = fdb->tx_begin(earnest::isolation::repeatable_read);
  bool callback_called = false;
  tx.async_file_contents(
      earnest::file_id("", "non-exsistant-file"),
      [&](std::error_code ec, [[maybe_unused]] std::vector<std::byte> bytes) {
        CHECK_EQUAL(make_error_code(earnest::file_db_errc::file_erased), ec);
        callback_called = true;
      });
  ioctx.run();

  CHECK(callback_called);
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

TEST_FIXTURE(tx_file_contents, can_read_using_file) {
  auto tx = fdb->tx_begin(earnest::isolation::repeatable_read, earnest::tx_mode::read_only);
  auto f = tx[testfile];

  bool callback_called = false;
  std::string contents;
  contents.resize(text.size());
  asio::async_read_at(f, 0, asio::buffer(contents),
      [&](std::error_code ec, std::size_t nbytes) {
        CHECK_EQUAL(std::error_code(), ec);
        CHECK_EQUAL(text.size(), nbytes);
        callback_called = true;
      });
  ioctx.run();

  CHECK(callback_called);
  CHECK_EQUAL(text, contents);
}

TEST_FIXTURE(tx_file_contents, modify_file) {
  using namespace std::literals;

  auto tx = fdb->tx_begin(earnest::isolation::repeatable_read);
  auto f = tx[testfile];

  bool write_callback_called = false, read_callback_called = false;
  std::string contents;
  asio::async_write_at(f, 4, asio::buffer("xxx"sv),
      [&](std::error_code ec, std::size_t nbytes) {
        CHECK_EQUAL(std::error_code(), ec);
        CHECK_EQUAL(3u, nbytes);
        write_callback_called = true;

        tx.async_file_contents(
            testfile,
            [&](std::error_code ec, std::vector<std::byte> bytes) {
              CHECK_EQUAL(std::error_code(), ec);
              read_callback_called = true;

              std::transform(bytes.begin(), bytes.end(),
                  std::back_inserter(contents),
                  [](std::byte b) -> char {
                    return static_cast<char>(static_cast<unsigned char>(b));
                  });
            });
      });
  ioctx.run();

  std::string expected = text;
  expected[4] = expected[5] = expected[6] = 'x';

  CHECK(write_callback_called);
  CHECK(read_callback_called);
  CHECK_EQUAL(expected, contents);
}

TEST_FIXTURE(tx_file_contents, cannot_modify_file_past_eof) {
  using namespace std::literals;

  auto tx = fdb->tx_begin(earnest::isolation::repeatable_read);
  auto f = tx[testfile];

  bool callback_called = false;
  std::string contents;
  asio::async_write_at(f, text.size() - 2u, asio::buffer("xxx"sv),
      [&](std::error_code ec, [[maybe_unused]] std::size_t nbytes) {
        CHECK_EQUAL(make_error_code(earnest::file_db_errc::write_past_eof), ec);
        callback_called = true;
      });
  ioctx.run();

  CHECK(callback_called);
}

TEST_FIXTURE(tx_file_contents, cannot_modify_nonexistent_file) {
  using namespace std::literals;

  auto tx = fdb->tx_begin(earnest::isolation::repeatable_read);
  auto f = tx[earnest::file_id("", "non_existent_file.txt")];

  bool write_callback_called = false;
  std::string contents;
  asio::async_write_at(f, 4, asio::buffer("xxx"sv),
      [&](std::error_code ec, [[maybe_unused]] std::size_t nbytes) {
        CHECK_EQUAL(make_error_code(earnest::file_db_errc::file_erased), ec);
        write_callback_called = true;
      });
  ioctx.run();

  CHECK(write_callback_called);
}

TEST_FIXTURE(tx_file_contents, modifications_are_local) {
  using namespace std::literals;

  auto tx1 = fdb->tx_begin(earnest::isolation::repeatable_read, earnest::tx_mode::write_only);
  auto tx2 = fdb->tx_begin(earnest::isolation::repeatable_read, earnest::tx_mode::read_only);
  auto f = tx1[testfile];
  asio::async_write_at(f, 4, asio::buffer("xxx"sv),
      [&](std::error_code ec, [[maybe_unused]] std::size_t nbytes) {
        REQUIRE CHECK_EQUAL(std::error_code(), ec);
        REQUIRE CHECK_EQUAL(3u, nbytes);
      });
  ioctx.run();
  ioctx.restart();

  std::string contents;
  tx2.async_file_contents(
      testfile,
      [&](std::error_code ec, std::vector<std::byte> bytes) {
        CHECK_EQUAL(std::error_code(), ec);

        std::transform(bytes.begin(), bytes.end(),
            std::back_inserter(contents),
            [](std::byte b) -> char {
              return static_cast<char>(static_cast<unsigned char>(b));
            });
      });
  ioctx.run();

  CHECK_EQUAL(text, contents);
}

TEST_FIXTURE(tx_file_contents, truncate_shorten) {
  auto tx = fdb->tx_begin(earnest::isolation::repeatable_read);
  bool callback_called = false;
  tx[testfile].async_truncate(7,
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
        callback_called = true;
      });
  ioctx.run();
  ioctx.restart();

  CHECK(callback_called);

  std::string contents;
  tx.async_file_contents(
      testfile,
      [&](std::error_code ec, std::vector<std::byte> bytes) {
        CHECK_EQUAL(std::error_code(), ec);

        std::transform(bytes.begin(), bytes.end(),
            std::back_inserter(contents),
            [](std::byte b) -> char {
              return static_cast<char>(static_cast<unsigned char>(b));
            });
      });
  ioctx.run();

  CHECK_EQUAL(text.substr(0, 7), contents);
}

TEST_FIXTURE(tx_file_contents, truncate_lengthen) {
  auto tx = fdb->tx_begin(earnest::isolation::repeatable_read);
  bool callback_called = false;
  auto f = tx[testfile];
  f.async_truncate(text.size() + 6u,
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
        callback_called = true;

        asio::async_write_at(
            f,
            text.size(),
            asio::buffer(std::string_view(" 1 2 3")),
            [](std::error_code ec, [[maybe_unused]] std::size_t nbytes) {
              CHECK_EQUAL(std::error_code(), ec);
            });
      });
  ioctx.run();
  ioctx.restart();

  CHECK(callback_called);

  std::string contents;
  tx.async_file_contents(
      testfile,
      [&](std::error_code ec, std::vector<std::byte> bytes) {
        CHECK_EQUAL(std::error_code(), ec);

        std::transform(bytes.begin(), bytes.end(),
            std::back_inserter(contents),
            [](std::byte b) -> char {
              return static_cast<char>(static_cast<unsigned char>(b));
            });
      });
  ioctx.run();

  CHECK_EQUAL(text + " 1 2 3", contents);
}

TEST_FIXTURE(tx_file_contents, cannot_truncate_non_existant_file) {
  auto tx = fdb->tx_begin(earnest::isolation::repeatable_read);
  bool callback_called = false;
  auto f = tx[earnest::file_id("", "does_not_exist.txt")];
  f.async_truncate(text.size() + 6u,
      [&](std::error_code ec) {
        CHECK_EQUAL(make_error_code(earnest::file_db_errc::file_erased), ec);
        callback_called = true;
      });
  ioctx.run();
  ioctx.restart();

  CHECK(callback_called);
}

TEST_FIXTURE(tx_file_contents, create_file) {
  auto tx = fdb->tx_begin(earnest::isolation::repeatable_read);
  bool callback_called = false;
  auto f = tx[earnest::file_id("", "does_not_exist.txt")];
  f.async_create(
      [&](std::error_code ec) {
        CHECK_EQUAL(make_error_code(earnest::file_db_errc::file_erased), ec);
        callback_called = true;
      });
  ioctx.run();
  ioctx.restart();

  CHECK(callback_called);
  tx.async_file_contents(
      earnest::file_id("", "does_not_exist.txt"),
      [&](std::error_code ec, std::vector<std::byte> bytes) {
        CHECK_EQUAL(std::error_code(), ec);
        CHECK(bytes.empty());
      });
  ioctx.run();
}

TEST_FIXTURE(tx_file_contents, cannot_create_existing_file) {
  auto tx = fdb->tx_begin(earnest::isolation::repeatable_read);
  bool callback_called = false;
  auto f = tx[testfile];
  f.async_create(
      [&](std::error_code ec) {
        CHECK_EQUAL(make_error_code(earnest::file_db_errc::file_exists), ec);
        callback_called = true;
      });
  ioctx.run();
  ioctx.restart();

  CHECK(callback_called);
}

TEST_FIXTURE(tx_file_contents, erase_file) {
  auto tx = fdb->tx_begin(earnest::isolation::repeatable_read);
  bool callback_called = false;
  auto f = tx[testfile];
  f.async_erase(
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
        callback_called = true;
      });
  ioctx.run();
  ioctx.restart();

  CHECK(callback_called);
  tx.async_file_contents(
      testfile,
      [](std::error_code ec, [[maybe_unused]] std::vector<std::byte> bytes) {
        CHECK_EQUAL(make_error_code(earnest::file_db_errc::file_erased), ec);
      });
  ioctx.run();
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
