#include <earnest/detail/wal_file.h>

#include "wal_file.h"
#include "UnitTest++/UnitTest++.h"

#include <iostream>
#include <algorithm>

#include <asio/io_context.hpp>

TEST(recovery) {
  const earnest::dir testdir = ensure_dir_exists_and_is_empty("recovery");

  { // Preparation stage.
    asio::io_context ioctx;
    auto f0 = std::make_shared<earnest::detail::wal_file_entry<asio::io_context::executor_type, std::allocator<std::byte>>>(ioctx.get_executor(), std::allocator<std::byte>());
    auto f3 = std::make_shared<earnest::detail::wal_file_entry<asio::io_context::executor_type, std::allocator<std::byte>>>(ioctx.get_executor(), std::allocator<std::byte>());
    f0->async_create(testdir, "0.wal", 0,
        [](std::error_code ec, auto link_event) {
          REQUIRE CHECK_EQUAL(std::error_code(), ec);
          std::invoke(link_event, std::error_code());
        });
    f3->async_create(testdir, "3.wal", 3,
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
  auto w = std::make_shared<earnest::detail::wal_file<asio::io_context::executor_type, std::allocator<std::byte>>>(ioctx.get_executor(), std::allocator<std::byte>());
  w->async_open(testdir,
      [](std::error_code ec) {
        REQUIRE CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  /*
   * Validation.
   */
  CHECK_EQUAL("0000000000000004.wal", w->active->name);
  CHECK_EQUAL(::earnest::detail::wal_file_entry_state::ready, w->active->state());
  REQUIRE CHECK_EQUAL(4u, w->entries.size());

  const std::array<std::uint64_t, 4> expected_indices{ 0, 1, 2, 3 };
  CHECK(std::equal(
          w->entries.begin(), w->entries.end(),
          expected_indices.begin(), expected_indices.end(),
          [](const auto& entry, std::uint64_t expected_index) -> bool {
            return entry->sequence == expected_index;
          }));

  const std::array<std::string_view, 4> expected_names{
    "0.wal",
    "0000000000000001.wal",
    "0000000000000002.wal",
    "3.wal",
  };
  CHECK(std::equal(
          w->entries.begin(), w->entries.end(),
          expected_names.begin(), expected_names.end(),
          [](const auto& entry, std::string_view expected_name) -> bool {
            return entry->name == expected_name;
          }));

  const std::array<::earnest::detail::wal_file_entry_state, 4> expected_states{
    ::earnest::detail::wal_file_entry_state::sealed,
    ::earnest::detail::wal_file_entry_state::sealed,
    ::earnest::detail::wal_file_entry_state::sealed,
    ::earnest::detail::wal_file_entry_state::sealed,
  };
  CHECK(std::equal(
          w->entries.begin(), w->entries.end(),
          expected_states.begin(), expected_states.end(),
          [](const auto& entry, auto expected_state) -> bool {
            return entry->state() == expected_state;
          }));
}
