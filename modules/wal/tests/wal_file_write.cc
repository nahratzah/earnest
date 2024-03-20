#include <earnest/detail/wal_file.h>

#include "wal_file.h"
#include <UnitTest++/UnitTest++.h>
#include <earnest/execution.h>

TEST(write) {
  using ::earnest::detail::wal_record_create_file;

  const earnest::dir testdir = ensure_dir_exists_and_is_empty("write");
  auto [w] = earnest::execution::sync_wait(earnest::detail::wal_file::create(testdir)).value();

  /*
   * Test: write to wal.
   */
  earnest::execution::sync_wait(
      w->append(
          earnest::detail::write_records_buffer(
              std::initializer_list<earnest::detail::wal_file::write_variant_type>{
                wal_record_create_file{ .file{ "", "foo"} },
                wal_record_create_file{ .file{ "", "bar"} },
                wal_record_create_file{ .file{ "", "baz"} },
              })));

  /*
   * Validation.
   */
  CHECK(w->entries_empty());
  CHECK(w->old_entries_empty());
  CHECK(w->very_old_entries_empty());

  auto expected = std::initializer_list<earnest::detail::wal_file::record_type>{
    wal_record_create_file{ .file{ "", "foo"} },
    wal_record_create_file{ .file{ "", "bar"} },
    wal_record_create_file{ .file{ "", "baz"} },
  };
  auto [records] = earnest::execution::sync_wait(w->records()).value();
  CHECK(std::equal(
          expected.begin(), expected.end(),
          records.cbegin(), records.cend()));
}
