#include <earnest/strand.h>

#include <UnitTest++/UnitTest++.h>

#include "fake_scheduler.h"

using namespace earnest::execution;

TEST(strand) {
  bool scheduler_started = false;
  strand<> s;
  auto sch = s.scheduler(fake_scheduler<false>{&scheduler_started});

  auto [x] = sync_wait(
      schedule(sch)
      | then([&scheduler_started]() noexcept { CHECK(scheduler_started); return 1; })).value();
  CHECK_EQUAL(1, x);
}

TEST(strand_scheduler_equality) {
  strand<> s;
  CHECK(s.scheduler(fake_scheduler<false>{}) == s.scheduler(fake_scheduler<false>{}));
  CHECK(!(s.scheduler(fake_scheduler<false>{}) != s.scheduler(fake_scheduler<false>{})));
}
