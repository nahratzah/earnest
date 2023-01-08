#include <earnest/detail/wal_file.h>

#include "UnitTest++/UnitTest++.h"

#include <iostream>
#include <system_error>
#include <string>
#include <string_view>
#include <sstream>
#include <iomanip>

#include <asio/io_context.hpp>

earnest::dir source_files, write_dir;

auto hex_string(std::string_view sv) -> std::string {
  using namespace std::literals;

  std::ostringstream out;
  out << std::setfill('0');
  bool first = true;
  for (char c : sv) {
    if (!std::exchange(first, false)) out << " "sv;
    if (c < 32 || c >= 127) {
      out << "0x"sv << std::setw(2) << std::hex << static_cast<unsigned int>(static_cast<unsigned char>(c));
    } else {
      out << " '"sv << std::string_view(&c, 1) << "'"sv;
    }
  }

  return std::move(out).str();
}

void ensure_file_is_gone(std::string filename) {
  std::error_code ec;
  write_dir.erase(filename, ec);
  REQUIRE CHECK(ec == std::error_code() || ec == make_error_code(std::errc::no_such_file_or_directory));
}

TEST(read_empty_wal_file_entry) {
  asio::io_context ioctx;
  auto f = earnest::detail::wal_file_entry<asio::io_context::executor_type, std::allocator<std::byte>>(ioctx.get_executor(), std::allocator<std::byte>());
  CHECK_EQUAL(earnest::detail::wal_file_entry_state::uninitialized, f.state());
  f.async_open(source_files, "empty",
      [&f](std::error_code ec, [[maybe_unused]] auto records) {
        CHECK(ec);
        CHECK_EQUAL(earnest::detail::wal_file_entry_state::uninitialized, f.state());
      });
  ioctx.run();

  CHECK_EQUAL(earnest::detail::wal_file_entry_state::uninitialized, f.state());
  CHECK(!f.file.is_open());
}

TEST(read_non_existant_wal_file_entry) {
  asio::io_context ioctx;
  auto f = earnest::detail::wal_file_entry<asio::io_context::executor_type, std::allocator<std::byte>>(ioctx.get_executor(), std::allocator<std::byte>());
  CHECK_EQUAL(earnest::detail::wal_file_entry_state::uninitialized, f.state());
  f.async_open(source_files, "non_existant_file",
      [&f](std::error_code ec, [[maybe_unused]] auto records) {
        CHECK(ec);
        CHECK_EQUAL(earnest::detail::wal_file_entry_state::uninitialized, f.state());
      });
  ioctx.run();

  CHECK_EQUAL(earnest::detail::wal_file_entry_state::uninitialized, f.state());
  CHECK(!f.file.is_open());
}

TEST(read_wal_file_entry) {
  asio::io_context ioctx;
  auto f = earnest::detail::wal_file_entry<asio::io_context::executor_type, std::allocator<std::byte>>(ioctx.get_executor(), std::allocator<std::byte>());
  CHECK_EQUAL(earnest::detail::wal_file_entry_state::uninitialized, f.state());
  f.async_open(source_files, "version_0",
      [&f](std::error_code ec, auto records) {
        CHECK_EQUAL(std::error_code(), ec);
        CHECK_EQUAL(earnest::detail::wal_file_entry_state::ready, f.state());

        CHECK_EQUAL(1u, records.size());
        if (!records.empty()) CHECK_EQUAL(0u, records.back().index());
      });
  ioctx.run();

  CHECK_EQUAL(0u, f.version);
  CHECK_EQUAL(17u, f.sequence);
  CHECK_EQUAL(32u, f.write_offset());
  CHECK_EQUAL(28u, f.link_offset());

  REQUIRE CHECK_EQUAL(earnest::detail::wal_file_entry_state::ready, f.state());

  ioctx.restart();
  f.async_records(
      [](std::error_code ec, auto records) {
        CHECK_EQUAL(std::error_code(), ec);
        CHECK_EQUAL(0u, records.size());
      });
  ioctx.run();
}

TEST(write_wal_file_entry) {
  using namespace std::string_literals;

  // We have to make sure the file doesn't exist, or the test will fail.
  ensure_file_is_gone("wal_19");

  asio::io_context ioctx;
  auto f = earnest::detail::wal_file_entry<asio::io_context::executor_type, std::allocator<std::byte>>(ioctx.get_executor(), std::allocator<std::byte>());
  CHECK_EQUAL(earnest::detail::wal_file_entry_state::uninitialized, f.state());
  f.async_create(write_dir, "wal_19", 19,
      [&f](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
        CHECK_EQUAL(earnest::detail::wal_file_entry_state::ready, f.state());
      });
  ioctx.run();

  CHECK(f.file.is_open());
  CHECK_EQUAL(
      (earnest::detail::wal_file_entry<asio::io_context::executor_type, std::allocator<std::byte>>::max_version),
      f.version);
  CHECK_EQUAL(19u, f.sequence);
  CHECK_EQUAL(32u, f.write_offset());
  CHECK_EQUAL(28u, f.link_offset());

  CHECK_EQUAL(
      hex_string("\013\013earnest.wal\000\000\000\000\000\000\000\000\000\000\000\000\000\000\023\000\000\000\000"s),
      hex_string(f.file.contents<std::string>()));

  REQUIRE CHECK_EQUAL(earnest::detail::wal_file_entry_state::ready, f.state());

  ioctx.restart();
  f.async_records(
      [](std::error_code ec, auto records) {
        CHECK_EQUAL(std::error_code(), ec);
        CHECK_EQUAL(0u, records.size());
      });
  ioctx.run();
}

TEST(append_wal_file_entry) {
  using namespace std::string_literals;
  using wal_file_entry_t = earnest::detail::wal_file_entry<asio::io_context::executor_type, std::allocator<std::byte>>;
  using ::earnest::detail::wal_record_noop;
  using ::earnest::detail::wal_record_skip32;

  // We have to make sure the file doesn't exist, or the test will fail.
  ensure_file_is_gone("append_log");

  asio::io_context ioctx;
  auto f = wal_file_entry_t(ioctx.get_executor(), std::allocator<std::byte>());
  f.async_create(write_dir, "append_log", 17,
      [](std::error_code ec) {
        REQUIRE CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();
  ioctx.restart();

  bool append_callback_was_called = false;
  f.async_append(std::initializer_list<wal_file_entry_t::write_variant_type>{
        wal_record_noop{}, wal_record_skip32{ .bytes = 8 }, wal_record_skip32{ .bytes = 0 }
      },
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
        append_callback_was_called = true;
      });
  ioctx.run();

  CHECK(append_callback_was_called);

  CHECK_EQUAL(
      hex_string("\013\013earnest.wal\000\000\000\000\000\000\000\000\000\000\000\000\000\000\021"s
          + "\000\000\000\001"s // wal_record_noop
          + "\000\000\000\002\000\000\000\010\000\000\000\000\000\000\000\000"s // wal_record_skip32(8)
          + "\000\000\000\002\000\000\000\000"s // wal_record_skip32(0)
          + "\000\000\000\000"s // sentinel (std::monostate)
          ),
      hex_string(f.file.contents<std::string>()));
}

int main(int argc, char** argv) {
  if (argc < 3) {
    std::cerr << "Usage:  " << (argc > 0 ? argv[0] : "wal_test") << " wal_source_dir writeable_dir\n"
        << "  wal_source_dir:  points at the directory containing test files\n"
        << "  writeable_dir:   points at a directory where we can write files\n";
    return 1;
  }
  try {
    source_files = earnest::dir(argv[1]);
    write_dir = earnest::dir(argv[2]);
  } catch (const std::exception& e) {
    std::cerr << "error opening dirs: " << e.what() << std::endl;
    return 1;
  }

  return UnitTest::RunAllTests();
}
