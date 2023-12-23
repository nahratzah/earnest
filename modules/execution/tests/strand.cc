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

TEST(strand_running_in_this_thread) {
  strand<> s;
  CHECK(!s.running_in_this_thread());
  auto sch = s.scheduler(fake_scheduler<false>{});

  auto [x] = sync_wait(
      schedule(sch)
      | then([&s]() noexcept { CHECK(s.running_in_this_thread()); return 1; })).value();
  CHECK_EQUAL(1, x);
}

TEST(strand_scheduler_equality) {
  strand<> s;
  CHECK(s.scheduler(fake_scheduler<false>{}) == s.scheduler(fake_scheduler<false>{}));
  CHECK(!(s.scheduler(fake_scheduler<false>{}) != s.scheduler(fake_scheduler<false>{})));
}

TEST(strand_transfer) {
  bool scheduler_started = false;
  strand<> s;
  auto sch = s.scheduler(fake_scheduler<false>{&scheduler_started});

  auto [x] = sync_wait(
      just(15)
      | transfer(sch)
      | then(
          [&scheduler_started, &s](int x) noexcept {
            CHECK_EQUAL(15, x);
            CHECK(scheduler_started);
            CHECK(s.running_in_this_thread());
            return 1;
          })).value();
  CHECK_EQUAL(1, x);
}
