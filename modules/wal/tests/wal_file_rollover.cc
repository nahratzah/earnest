#include <earnest/detail/wal_file.h>

#include "wal_file.h"
#include "UnitTest++/UnitTest++.h"

#include <iostream>
#include <algorithm>

#include <asio/io_context.hpp>

TEST(rollover) {
  using wal_file_t = earnest::detail::wal_file<asio::io_context::executor_type, std::allocator<std::byte>>;
  using ::earnest::detail::wal_record_noop;
  using ::earnest::detail::wal_record_skip32;
  using ::earnest::detail::wal_record_seal;
  using ::earnest::detail::wal_record_rollover_intent;
  using ::earnest::detail::wal_record_rollover_ready;

  const earnest::dir testdir = ensure_dir_exists_and_is_empty("rollover");

  asio::io_context ioctx;
  auto w = std::make_shared<wal_file_t>(ioctx.get_executor(), std::allocator<std::byte>());
  w->async_create(testdir,
      [](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();
  ioctx.restart();
  REQUIRE CHECK_EQUAL(0u, w->entries.size());
  REQUIRE CHECK_EQUAL(0u, w->active->sequence);

  /*
   * Test: rollover, while writing to wal.
   */
  bool rollover_callback_was_called = false;
  w->async_append(std::initializer_list<wal_file_t::write_variant_type>{
        wal_record_skip32{ .bytes = 4 }, wal_record_skip32{ .bytes = 8 }
      },
      [](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  w->async_rollover(
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
        rollover_callback_was_called = true;
      });
  w->async_append(std::initializer_list<wal_file_t::write_variant_type>{
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
  REQUIRE CHECK_EQUAL(1u, w->entries.size());
  CHECK_EQUAL(1u, w->active->sequence);
  CHECK_EQUAL(::earnest::detail::wal_file_entry_state::ready, w->active->state());

  std::vector<wal_file_t::variant_type> records;
  w->async_records(
      [&records](auto v) {
        records.push_back(v);
        return std::error_code();
      },
      [&records](std::error_code ec) {
        REQUIRE CHECK_EQUAL(std::error_code(), ec);

        auto expected = std::initializer_list<wal_file_t::variant_type>{
          wal_record_seal{}, // There is a single seal in the list, because of the rollover.
          wal_record_skip32{ .bytes = 4 }, wal_record_skip32{ .bytes = 8 },
          wal_record_skip32{ .bytes = 12 }, wal_record_skip32{ .bytes = 16 },
          // Records introduced by the rollover call.
          wal_record_rollover_intent{ .filename = "0000000000000001.wal" },
          wal_record_rollover_ready{ .filename = "0000000000000001.wal" },
        };
        CHECK(std::is_permutation(
                expected.begin(), expected.end(),
                records.cbegin(), records.cend()));
      });
  ioctx.run();

  const std::array<std::uint64_t, 1> expected_indices{ 0 };
  CHECK(std::equal(
          w->entries.begin(), w->entries.end(),
          expected_indices.begin(), expected_indices.end(),
          [](const auto& entry, std::uint64_t expected_index) -> bool {
            return entry->sequence == expected_index;
          }));

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
