#include <earnest/file_db.h>

#include "file_db.h"
#include "UnitTest++/UnitTest++.h"

#include <algorithm>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include <asio/io_context.hpp>
#include <asio/write_at.hpp>

class tx_commit {
  public:
  tx_commit();

  auto make_tx(earnest::tx_mode m = earnest::tx_mode::read_write) {
    return fdb->tx_begin(earnest::isolation::repeatable_read, m);
  }

  auto get_file_contents(earnest::file_id id) -> std::string {
    std::string contents;
    make_tx(earnest::tx_mode::read_only).async_file_contents(
        std::move(id),
        [&contents](std::error_code ec, std::vector<std::byte> bytes) {
          CHECK_EQUAL(std::error_code(), ec);
          std::transform(bytes.begin(), bytes.end(),
              std::back_inserter(contents),
              [](std::byte b) -> char {
                return static_cast<char>(static_cast<unsigned char>(b));
              });
        });
    ioctx.run();
    ioctx.restart();

    return contents;
  }

  const std::string text = "bla bla chocoladevla";
  const earnest::file_id testfile = earnest::file_id("", "testfile");
  const earnest::file_id new_file = earnest::file_id("", "new_file");
  asio::io_context ioctx;
  std::shared_ptr<earnest::file_db<asio::io_context::executor_type>> fdb;
};

TEST_FIXTURE(tx_commit, commit) {
  using namespace std::literals;

  auto tx = make_tx();
  auto f = tx[new_file];
  bool callback_called = false;
  f.async_create(asio::deferred)
  | asio::deferred(
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
        return f.async_truncate(4, asio::deferred);
      })
  | asio::deferred(
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
        return asio::async_write_at(f, 0, asio::buffer("abba"sv), asio::deferred);
      })
  | asio::deferred(
      [&](std::error_code ec, std::size_t nbytes) {
        CHECK_EQUAL(std::error_code(), ec);
        CHECK_EQUAL(4u, nbytes);
        return tx.async_commit(asio::deferred);
      })
  | [&](std::error_code ec) {
      CHECK_EQUAL(std::error_code(), ec);
      callback_called = true;
    };
  ioctx.run();
  ioctx.restart();

  CHECK(callback_called);
  CHECK_EQUAL("abba"s, get_file_contents(new_file));
}

tx_commit::tx_commit() {
  const earnest::dir testdir = ensure_dir_exists_and_is_empty("tx_commit");
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
