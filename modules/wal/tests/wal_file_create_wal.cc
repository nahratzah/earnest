#include <earnest/detail/wal_file.h>

#include "wal_file.h"
#include <UnitTest++/UnitTest++.h>
#include <earnest/execution.h>

TEST(create_wal) {
  const earnest::dir testdir = ensure_dir_exists_and_is_empty("create_wal");

  /*
   * Test: create a new wal.
   */
  auto [w] = earnest::execution::sync_wait(earnest::detail::wal_file::create(testdir)).value();

  /*
   * Validation.
   */
  CHECK(w->entries_empty());
  CHECK(w->old_entries_empty());
  CHECK(w->very_old_entries_empty());
  CHECK_EQUAL(0u, w->active_sequence());
  CHECK_EQUAL("earnest-wal-0000000000000000.wal", w->active_filename());
  CHECK_EQUAL(::earnest::detail::wal_file_entry_state::ready, w->active_state());
}
