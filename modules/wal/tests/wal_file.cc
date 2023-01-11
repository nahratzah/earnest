#include <earnest/detail/wal_file.h>

#include "UnitTest++/UnitTest++.h"

#include <iostream>
#include <algorithm>

#include <asio/io_context.hpp>

earnest::dir write_dir;

auto ensure_dir_exists_and_is_empty(std::filesystem::path name) -> earnest::dir {
  using namespace std::literals;

  std::cerr << "ensure_dir_exists_and_is_empty(" << name << ")\n";
  if (name.has_parent_path())
    throw std::runtime_error("directory '"s + name.string() + "' is not a relative name"s);

  bool exists = false;
  for (auto d : write_dir) {
    if (d.path() == name) exists = true;
  }

  earnest::dir new_dir;
  if (!exists)
    new_dir.create(write_dir, name);
  else
    new_dir.open(write_dir, name);

  for (auto file : new_dir) {
    if (file.path() == "." || file.path() == "..") continue;

    try {
      new_dir.erase(file);
    } catch (const std::exception& e) {
      std::cerr << "error cleaning dir'" << name << "', failed to erase '" << file.path() << "': " << e.what() << std::endl;
      throw;
    }
  }
  return new_dir;
}

TEST(create_wal) {
  const earnest::dir testdir = ensure_dir_exists_and_is_empty("create_wal");

  asio::io_context ioctx;
  auto w = earnest::detail::wal_file<asio::io_context::executor_type, std::allocator<std::byte>>(ioctx.get_executor(), std::allocator<std::byte>());

  /*
   * Test: create a new wal.
   */
  bool handler_was_called = false;
  w.async_create(testdir,
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
        handler_was_called = true;
      });
  ioctx.run();

  CHECK(handler_was_called);

  /*
   * Validation.
   */
  REQUIRE CHECK_EQUAL(1u, w.entries.size());
  CHECK(std::prev(w.entries.end()) == w.active);

  const std::array<std::uint64_t, 1> expected_indices{ 0 };
  CHECK(std::equal(
          w.entries.begin(), w.entries.end(),
          expected_indices.begin(), expected_indices.end(),
          [](const auto& entry, std::uint64_t expected_index) -> bool {
            return entry.sequence == expected_index;
          }));

  const std::array<std::string_view, 1> expected_names{
    "0000000000000000.wal",
  };
  CHECK(std::equal(
          w.entries.begin(), w.entries.end(),
          expected_names.begin(), expected_names.end(),
          [](const auto& entry, std::string_view expected_name) -> bool {
            return entry.name == expected_name;
          }));

  const std::array<::earnest::detail::wal_file_entry_state, 1> expected_states{
    ::earnest::detail::wal_file_entry_state::ready,
  };
  CHECK(std::equal(
          w.entries.begin(), w.entries.end(),
          expected_states.begin(), expected_states.end(),
          [](const auto& entry, auto expected_state) -> bool {
            return entry.state() == expected_state;
          }));
}

TEST(reread_wal) {
  const earnest::dir testdir = ensure_dir_exists_and_is_empty("reread_wal");

  { // Preparation stage.
    asio::io_context ioctx;
    auto w = earnest::detail::wal_file<asio::io_context::executor_type, std::allocator<std::byte>>(ioctx.get_executor(), std::allocator<std::byte>());
    w.async_create(testdir,
        [](std::error_code ec) {
          REQUIRE CHECK_EQUAL(std::error_code(), ec);
        });
    ioctx.run();
  }

  /*
   * Test: reopen a previously create wal.
   */
  asio::io_context ioctx;
  auto w = earnest::detail::wal_file<asio::io_context::executor_type, std::allocator<std::byte>>(ioctx.get_executor(), std::allocator<std::byte>());
  w.async_open(testdir,
      [](std::error_code ec) {
        REQUIRE CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  /*
   * Validation.
   */
  REQUIRE CHECK_EQUAL(1u, w.entries.size());
  CHECK(std::prev(w.entries.end()) == w.active);

  const std::array<std::uint64_t, 1> expected_indices{ 0 };
  CHECK(std::equal(
          w.entries.begin(), w.entries.end(),
          expected_indices.begin(), expected_indices.end(),
          [](const auto& entry, std::uint64_t expected_index) -> bool {
            return entry.sequence == expected_index;
          }));

  const std::array<std::string_view, 1> expected_names{
    "0000000000000000.wal",
  };
  CHECK(std::equal(
          w.entries.begin(), w.entries.end(),
          expected_names.begin(), expected_names.end(),
          [](const auto& entry, std::string_view expected_name) -> bool {
            return entry.name == expected_name;
          }));

  const std::array<::earnest::detail::wal_file_entry_state, 1> expected_states{
    ::earnest::detail::wal_file_entry_state::ready,
  };
  CHECK(std::equal(
          w.entries.begin(), w.entries.end(),
          expected_states.begin(), expected_states.end(),
          [](const auto& entry, auto expected_state) -> bool {
            return entry.state() == expected_state;
          }));
}

TEST(recovery) {
  const earnest::dir testdir = ensure_dir_exists_and_is_empty("recovery");

  { // Preparation stage.
    asio::io_context ioctx;
    auto f0 = earnest::detail::wal_file_entry<asio::io_context::executor_type, std::allocator<std::byte>>(ioctx.get_executor(), std::allocator<std::byte>());
    auto f3 = earnest::detail::wal_file_entry<asio::io_context::executor_type, std::allocator<std::byte>>(ioctx.get_executor(), std::allocator<std::byte>());
    f0.async_create(testdir, "0.wal", 0,
        [](std::error_code ec, auto link_event) {
          REQUIRE CHECK_EQUAL(std::error_code(), ec);
          std::invoke(link_event, std::error_code());
        });
    f3.async_create(testdir, "3.wal", 3,
        [](std::error_code ec, auto link_event) {
          REQUIRE CHECK_EQUAL(std::error_code(), ec);
          std::invoke(link_event, std::error_code());
        });
    ioctx.run();
  }

  /*
   * Test: recovering from a interrupted wal.
   */
  asio::io_context ioctx;
  auto w = earnest::detail::wal_file<asio::io_context::executor_type, std::allocator<std::byte>>(ioctx.get_executor(), std::allocator<std::byte>());
  w.async_open(testdir,
      [](std::error_code ec) {
        REQUIRE CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  /*
   * Validation.
   */
  REQUIRE CHECK_EQUAL(5u, w.entries.size());
  CHECK(std::prev(w.entries.end()) == w.active);

  const std::array<std::uint64_t, 5> expected_indices{ 0, 1, 2, 3, 4 };
  CHECK(std::equal(
          w.entries.begin(), w.entries.end(),
          expected_indices.begin(), expected_indices.end(),
          [](const auto& entry, std::uint64_t expected_index) -> bool {
            return entry.sequence == expected_index;
          }));

  const std::array<std::string_view, 5> expected_names{
    "0.wal",
    "0000000000000001.wal",
    "0000000000000002.wal",
    "3.wal",
    "0000000000000004.wal",
  };
  CHECK(std::equal(
          w.entries.begin(), w.entries.end(),
          expected_names.begin(), expected_names.end(),
          [](const auto& entry, std::string_view expected_name) -> bool {
            return entry.name == expected_name;
          }));

  const std::array<::earnest::detail::wal_file_entry_state, 5> expected_states{
    ::earnest::detail::wal_file_entry_state::sealed,
    ::earnest::detail::wal_file_entry_state::sealed,
    ::earnest::detail::wal_file_entry_state::sealed,
    ::earnest::detail::wal_file_entry_state::sealed,
    ::earnest::detail::wal_file_entry_state::ready,
  };
  CHECK(std::equal(
          w.entries.begin(), w.entries.end(),
          expected_states.begin(), expected_states.end(),
          [](const auto& entry, auto expected_state) -> bool {
            return entry.state() == expected_state;
          }));
}

TEST(write) {
  using wal_file_t = earnest::detail::wal_file<asio::io_context::executor_type, std::allocator<std::byte>>;
  using ::earnest::detail::wal_record_noop;
  using ::earnest::detail::wal_record_skip32;

  const earnest::dir testdir = ensure_dir_exists_and_is_empty("write");

  asio::io_context ioctx;
  auto w = wal_file_t(ioctx.get_executor(), std::allocator<std::byte>());
  w.async_create(testdir,
      [](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();
  ioctx.restart();

  /*
   * Test: write to wal.
   */
  bool append_callback_was_called = false;
  w.async_append(std::initializer_list<wal_file_t::write_variant_type>{
        wal_record_noop{}, wal_record_skip32{ .bytes = 8 }, wal_record_skip32{ .bytes = 0 }
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
  REQUIRE CHECK_EQUAL(1u, w.entries.size());
  CHECK(std::prev(w.entries.end()) == w.active);

  w.entries.front().async_records(
      [](std::error_code ec, const auto& records) {
        REQUIRE CHECK_EQUAL(std::error_code(), ec);

        auto expected = std::initializer_list<wal_file_t::write_variant_type>{
          wal_record_noop{}, wal_record_skip32{ .bytes = 8 }, wal_record_skip32{ .bytes = 0 }
        };
        CHECK(std::equal(
                expected.begin(), expected.end(),
                records.begin(), records.end()));
      });
}

TEST(rollover) {
  using wal_file_t = earnest::detail::wal_file<asio::io_context::executor_type, std::allocator<std::byte>>;
  using ::earnest::detail::wal_record_noop;
  using ::earnest::detail::wal_record_skip32;
  using ::earnest::detail::wal_record_seal;

  const earnest::dir testdir = ensure_dir_exists_and_is_empty("rollover");

  asio::io_context ioctx;
  auto w = wal_file_t(ioctx.get_executor(), std::allocator<std::byte>());
  w.async_create(testdir,
      [](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();
  ioctx.restart();
  REQUIRE CHECK_EQUAL(1u, w.entries.size());
  REQUIRE CHECK_EQUAL(0u, w.active->sequence);

  /*
   * Test: rollover, while writing to wal.
   */
  bool rollover_callback_was_called = false;
  w.async_append(std::initializer_list<wal_file_t::write_variant_type>{
        wal_record_skip32{ .bytes = 4 }, wal_record_skip32{ .bytes = 8 }
      },
      [](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  w.async_rollover(
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
        rollover_callback_was_called = true;
      });
  w.async_append(std::initializer_list<wal_file_t::write_variant_type>{
        wal_record_skip32{ .bytes = 12 }, wal_record_skip32{ .bytes = 16 }
      },
      [](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();
  ioctx.restart();

  /*
   * Validation.
   */
  CHECK(rollover_callback_was_called);
  REQUIRE CHECK_EQUAL(2u, w.entries.size());
  CHECK(std::prev(w.entries.end()) == w.active);

  w.async_records(
      [](std::error_code ec, const auto& records) {
        REQUIRE CHECK_EQUAL(std::error_code(), ec);

        auto expected = std::initializer_list<wal_file_t::write_variant_type>{
          wal_record_seal{}, // There is a single seal in the list, because of the rollover.
          wal_record_skip32{ .bytes = 4 }, wal_record_skip32{ .bytes = 8 },
          wal_record_skip32{ .bytes = 12 }, wal_record_skip32{ .bytes = 16 },
        };
        CHECK(std::is_permutation(
                expected.begin(), expected.end(),
                records.begin(), records.end()));
      });
  ioctx.run();

  const std::array<std::uint64_t, 2> expected_indices{ 0, 1 };
  CHECK(std::equal(
          w.entries.begin(), w.entries.end(),
          expected_indices.begin(), expected_indices.end(),
          [](const auto& entry, std::uint64_t expected_index) -> bool {
            return entry.sequence == expected_index;
          }));

  const std::array<::earnest::detail::wal_file_entry_state, 2> expected_states{
    ::earnest::detail::wal_file_entry_state::sealed,
    ::earnest::detail::wal_file_entry_state::ready,
  };
  CHECK(std::equal(
          w.entries.begin(), w.entries.end(),
          expected_states.begin(), expected_states.end(),
          [](const auto& entry, auto expected_state) -> bool {
            return entry.state() == expected_state;
          }));
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: " << (argc > 0 ? argv[0] : "wal_test") << " writeable_dir\n"
        <<  "writeable_dir: points at a directory where we can write files\n";
    return 1;
  }
  try {
    write_dir = earnest::dir(argv[1]);
  } catch (const std::exception& e) {
    std::cerr << "error opening dirs: " << e.what() << std::endl;
    return 1;
  }

  return UnitTest::RunAllTests();
}
