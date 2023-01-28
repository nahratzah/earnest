#include <earnest/detail/wal_file.h>

#include "wal_file.h"
#include "UnitTest++/UnitTest++.h"

#include <iostream>
#include <algorithm>

#include <asio/io_context.hpp>

TEST(open_with_gaps) {
  const earnest::dir testdir = ensure_dir_exists_and_is_empty("open_with_gaps");

  { // Preparation stage:
    // 0.wal -- sealed
    // 1.wal -- absent
    // 2.wal -- absent
    // 3.wal -- sealed
    // 4.wal -- active
    asio::io_context ioctx;
    auto f0 = std::make_shared<earnest::detail::wal_file_entry<asio::io_context::executor_type, std::allocator<std::byte>>>(ioctx.get_executor(), std::allocator<std::byte>());
    auto f3 = std::make_shared<earnest::detail::wal_file_entry<asio::io_context::executor_type, std::allocator<std::byte>>>(ioctx.get_executor(), std::allocator<std::byte>());
    auto f4 = std::make_shared<earnest::detail::wal_file_entry<asio::io_context::executor_type, std::allocator<std::byte>>>(ioctx.get_executor(), std::allocator<std::byte>());
    f0->async_create(testdir, "0.wal", 0,
        [f0](std::error_code ec, auto link_event) {
          REQUIRE CHECK_EQUAL(std::error_code(), ec);
          std::invoke(link_event, std::error_code());

          f0->async_seal(
              [](std::error_code ec) {
                REQUIRE CHECK_EQUAL(std::error_code(), ec);
              });
        });
    f3->async_create(testdir, "3.wal", 3,
        [f3](std::error_code ec, auto link_event) {
          REQUIRE CHECK_EQUAL(std::error_code(), ec);
          std::invoke(link_event, std::error_code());

          f3->async_seal(
              [](std::error_code ec) {
                REQUIRE CHECK_EQUAL(std::error_code(), ec);
              });
        });
    f4->async_create(testdir, "4.wal", 4,
        [f3](std::error_code ec, auto link_event) {
          REQUIRE CHECK_EQUAL(std::error_code(), ec);
          std::invoke(link_event, std::error_code());
        });
    ioctx.run();
  }

  /*
   * Test: open a WAL where some entries have been archived.
   */
  asio::io_context ioctx;
  auto w = std::make_shared<earnest::detail::wal_file<asio::io_context::executor_type, std::allocator<std::byte>>>(ioctx.get_executor(), std::allocator<std::byte>());
  w->async_open(testdir,
      [](std::error_code ec) {
        REQUIRE CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  /*
   * Validation.
   */
  CHECK_EQUAL("4.wal", w->active->name);
  CHECK_EQUAL(::earnest::detail::wal_file_entry_state::ready, w->active->state());

  {
    // entries holds only 3.wal
    const std::array<std::uint64_t, 1> expected_indices{ 3 };
    CHECK(std::equal(
            w->entries.begin(), w->entries.end(),
            expected_indices.begin(), expected_indices.end(),
            [](const auto& entry, std::uint64_t expected_index) -> bool {
              return entry->sequence == expected_index;
            }));

    // entries holds only 3.wal
    const std::array<std::string_view, 1> expected_names{
      "3.wal",
    };
    CHECK(std::equal(
            w->entries.begin(), w->entries.end(),
            expected_names.begin(), expected_names.end(),
            [](const auto& entry, std::string_view expected_name) -> bool {
              return entry->name == expected_name;
            }));

    // entries holds only 3.wal
    const std::array<::earnest::detail::wal_file_entry_state, 1> expected_states{
      ::earnest::detail::wal_file_entry_state::sealed,
    };
    CHECK(std::equal(
            w->entries.begin(), w->entries.end(),
            expected_states.begin(), expected_states.end(),
            [](const auto& entry, auto expected_state) -> bool {
              return entry->state() == expected_state;
            }));
  }

  {
    // old holds only 0.wal
    const std::array<std::uint64_t, 1> expected_indices{ 0 };
    CHECK(std::equal(
            w->old.begin(), w->old.end(),
            expected_indices.begin(), expected_indices.end(),
            [](const auto& entry, std::uint64_t expected_index) -> bool {
              return entry->sequence == expected_index;
            }));

    // entries holds only 3.wal
    const std::array<std::string_view, 1> expected_names{
      "0.wal",
    };
    CHECK(std::equal(
            w->old.begin(), w->old.end(),
            expected_names.begin(), expected_names.end(),
            [](const auto& entry, std::string_view expected_name) -> bool {
              return entry->name == expected_name;
            }));

    // entries holds only 3.wal
    const std::array<::earnest::detail::wal_file_entry_state, 1> expected_states{
      ::earnest::detail::wal_file_entry_state::sealed,
    };
    CHECK(std::equal(
            w->old.begin(), w->old.end(),
            expected_states.begin(), expected_states.end(),
            [](const auto& entry, auto expected_state) -> bool {
              return entry->state() == expected_state;
            }));
  }
}
