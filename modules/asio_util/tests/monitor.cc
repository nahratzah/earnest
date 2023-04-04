#include <earnest/detail/monitor.h>

#include <UnitTest++/UnitTest++.h>

#include <asio/post.hpp>
#include <asio/io_context.hpp>

struct fixture {
  using mon_type = earnest::detail::monitor<asio::io_context::executor_type>;

  fixture()
  : ioctx(),
    mon(ioctx.get_executor())
  {}

  auto enqueue_shared() -> void {
    mon.async_shared(
        []([[maybe_unused]] mon_type::shared_lock lock) {
          /* skip */
        });
  }

  auto enqueue_upgrade() -> void {
    mon.async_upgrade(
        []([[maybe_unused]] mon_type::upgrade_lock lock) {
          /* skip */
        });
  }

  auto enqueue_exclusive() -> void {
    mon.async_exclusive(
        []([[maybe_unused]] mon_type::exclusive_lock lock) {
          /* skip */
        });
  }

  asio::io_context ioctx;
  mon_type mon;
};

TEST_FIXTURE(fixture, uncontested_shared) {
  auto lock = mon.try_shared();

  CHECK(lock.has_value());
  CHECK(lock->is_locked());
  CHECK(lock->holds_monitor(mon));
}

TEST_FIXTURE(fixture, uncontested_upgrade) {
  auto lock = mon.try_upgrade();

  CHECK(lock.has_value());
  CHECK(lock->is_locked());
  CHECK(lock->holds_monitor(mon));
}

TEST_FIXTURE(fixture, uncontested_exclusive) {
  auto lock = mon.try_upgrade();

  CHECK(lock.has_value());
  CHECK(lock->is_locked());
  CHECK(lock->holds_monitor(mon));
}

TEST_FIXTURE(fixture, shared_and_shared) {
  auto contest = mon.try_shared();
  auto lock = mon.try_shared();

  CHECK(lock.has_value());
  CHECK(lock->is_locked());
  CHECK(lock->holds_monitor(mon));
}

TEST_FIXTURE(fixture, shared_and_upgrade) {
  auto contest = mon.try_shared();
  auto lock = mon.try_upgrade();

  CHECK(lock.has_value());
  CHECK(lock->is_locked());
  CHECK(lock->holds_monitor(mon));
}

TEST_FIXTURE(fixture, shared_and_exclusive) {
  auto contest = mon.try_shared();
  auto lock = mon.try_exclusive();

  CHECK(!lock.has_value());
}

TEST_FIXTURE(fixture, upgrade_and_shared) {
  auto contest = mon.try_upgrade();
  auto lock = mon.try_shared();

  CHECK(lock.has_value());
  CHECK(lock->is_locked());
  CHECK(lock->holds_monitor(mon));
}

TEST_FIXTURE(fixture, upgrade_and_upgrade) {
  auto contest = mon.try_upgrade();
  auto lock = mon.try_upgrade();

  CHECK(!lock.has_value());
}

TEST_FIXTURE(fixture, upgrade_and_exclusive) {
  auto contest = mon.try_upgrade();
  auto lock = mon.try_exclusive();

  CHECK(!lock.has_value());
}

TEST_FIXTURE(fixture, exclusive_and_shared) {
  auto contest = mon.try_exclusive();
  auto lock = mon.try_shared();

  CHECK(!lock.has_value());
}

TEST_FIXTURE(fixture, exclusive_and_upgrade) {
  auto contest = mon.try_exclusive();
  auto lock = mon.try_upgrade();

  CHECK(!lock.has_value());
}

TEST_FIXTURE(fixture, exclusive_and_exclusive) {
  auto contest = mon.try_exclusive();
  auto lock = mon.try_exclusive();

  CHECK(!lock.has_value());
}

TEST_FIXTURE(fixture, uncontested_dispatch_shared) {
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.dispatch_shared(
            [&](mon_type::shared_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(callback_called);
      });
  ioctx.run();

  CHECK(callback_called);
}

TEST_FIXTURE(fixture, uncontested_dispatch_upgrade) {
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.dispatch_upgrade(
            [&](mon_type::upgrade_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(callback_called);
      });
  ioctx.run();

  CHECK(callback_called);
}

TEST_FIXTURE(fixture, uncontested_dispatch_exclusive) {
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.dispatch_exclusive(
            [&](mon_type::exclusive_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(callback_called);
      });
  ioctx.run();

  CHECK(callback_called);
}

TEST_FIXTURE(fixture, uncontested_async_shared) {
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.async_shared(
            [&](mon_type::shared_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(!callback_called);
      });
  ioctx.run();

  CHECK(callback_called);
}

TEST_FIXTURE(fixture, uncontested_async_upgrade) {
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.async_upgrade(
            [&](mon_type::upgrade_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(!callback_called);
      });
  ioctx.run();

  CHECK(callback_called);
}

TEST_FIXTURE(fixture, uncontested_async_exclusive) {
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.async_exclusive(
            [&](mon_type::exclusive_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(!callback_called);
      });
  ioctx.run();

  CHECK(callback_called);
}

TEST_FIXTURE(fixture, shared_and_dispatch_shared) {
  auto contest = mon.try_shared();
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.dispatch_shared(
            [&](mon_type::shared_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(callback_called);
      });
  ioctx.poll();
  CHECK(callback_called);

  contest.reset();
  ioctx.poll();
  CHECK(callback_called);
}

TEST_FIXTURE(fixture, shared_and_async_shared) {
  auto contest = mon.try_shared();
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.async_shared(
            [&](mon_type::shared_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(!callback_called);
      });
  ioctx.poll();
  CHECK(callback_called);

  contest.reset();
  ioctx.poll();
  CHECK(callback_called);
}

TEST_FIXTURE(fixture, shared_and_dispatch_upgrade) {
  auto contest = mon.try_shared();
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.dispatch_upgrade(
            [&](mon_type::upgrade_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(callback_called);
      });
  ioctx.poll();
  CHECK(callback_called);

  contest.reset();
  ioctx.poll();
  CHECK(callback_called);
}

TEST_FIXTURE(fixture, shared_and_async_upgrade) {
  auto contest = mon.try_shared();
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.async_upgrade(
            [&](mon_type::upgrade_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(!callback_called);
      });
  ioctx.poll();
  CHECK(callback_called);

  contest.reset();
  ioctx.poll();
  CHECK(callback_called);
}

TEST_FIXTURE(fixture, shared_and_dispatch_exclusive) {
  auto contest = mon.try_shared();
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.dispatch_exclusive(
            [&](mon_type::exclusive_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(!callback_called);
      });
  ioctx.poll();
  CHECK(!callback_called);

  contest.reset();
  ioctx.poll();
  CHECK(callback_called);
}

TEST_FIXTURE(fixture, shared_and_async_exclusive) {
  auto contest = mon.try_shared();
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.async_exclusive(
            [&](mon_type::exclusive_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(!callback_called);
      });
  ioctx.poll();
  CHECK(!callback_called);

  contest.reset();
  ioctx.poll();
  CHECK(callback_called);
}

TEST_FIXTURE(fixture, upgrade_and_dispatch_shared) {
  auto contest = mon.try_upgrade();
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.dispatch_shared(
            [&](mon_type::shared_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(callback_called);
      });
  ioctx.poll();
  CHECK(callback_called);

  contest.reset();
  ioctx.poll();
  CHECK(callback_called);
}

TEST_FIXTURE(fixture, upgrade_and_async_shared) {
  auto contest = mon.try_upgrade();
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.async_shared(
            [&](mon_type::shared_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(!callback_called);
      });
  ioctx.poll();
  CHECK(callback_called);

  contest.reset();
  ioctx.poll();
  CHECK(callback_called);
}

TEST_FIXTURE(fixture, upgrade_and_dispatch_upgrade) {
  auto contest = mon.try_upgrade();
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.dispatch_upgrade(
            [&](mon_type::upgrade_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(!callback_called);
      });
  ioctx.poll();
  CHECK(!callback_called);

  contest.reset();
  ioctx.poll();
  CHECK(callback_called);
}

TEST_FIXTURE(fixture, upgrade_and_async_upgrade) {
  auto contest = mon.try_upgrade();
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.async_upgrade(
            [&](mon_type::upgrade_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(!callback_called);
      });
  ioctx.poll();
  CHECK(!callback_called);

  contest.reset();
  ioctx.poll();
  CHECK(callback_called);
}

TEST_FIXTURE(fixture, upgrade_and_dispatch_exclusive) {
  auto contest = mon.try_upgrade();
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.dispatch_exclusive(
            [&](mon_type::exclusive_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(!callback_called);
      });
  ioctx.poll();
  CHECK(!callback_called);

  contest.reset();
  ioctx.poll();
  CHECK(callback_called);
}

TEST_FIXTURE(fixture, upgrade_and_async_exclusive) {
  auto contest = mon.try_upgrade();
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.async_exclusive(
            [&](mon_type::exclusive_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(!callback_called);
      });
  ioctx.poll();
  CHECK(!callback_called);

  contest.reset();
  ioctx.poll();
  CHECK(callback_called);
}

TEST_FIXTURE(fixture, exclusive_and_dispatch_shared) {
  auto contest = mon.try_exclusive();
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.dispatch_shared(
            [&](mon_type::shared_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(!callback_called);
      });
  ioctx.poll();
  CHECK(!callback_called);

  contest.reset();
  ioctx.poll();
  CHECK(callback_called);
}

TEST_FIXTURE(fixture, exclusive_and_async_shared) {
  auto contest = mon.try_exclusive();
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.async_shared(
            [&](mon_type::shared_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(!callback_called);
      });
  ioctx.poll();
  CHECK(!callback_called);

  contest.reset();
  ioctx.poll();
  CHECK(callback_called);
}

TEST_FIXTURE(fixture, exclusive_and_dispatch_upgrade) {
  auto contest = mon.try_exclusive();
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.dispatch_upgrade(
            [&](mon_type::upgrade_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(!callback_called);
      });
  ioctx.poll();
  CHECK(!callback_called);

  contest.reset();
  ioctx.poll();
  CHECK(callback_called);
}

TEST_FIXTURE(fixture, exclusive_and_async_upgrade) {
  auto contest = mon.try_exclusive();
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.async_upgrade(
            [&](mon_type::upgrade_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(!callback_called);
      });
  ioctx.poll();
  CHECK(!callback_called);

  contest.reset();
  ioctx.poll();
  CHECK(callback_called);
}

TEST_FIXTURE(fixture, exclusive_and_dispatch_exclusive) {
  auto contest = mon.try_exclusive();
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.dispatch_exclusive(
            [&](mon_type::exclusive_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(!callback_called);
      });
  ioctx.poll();
  CHECK(!callback_called);

  contest.reset();
  ioctx.poll();
  CHECK(callback_called);
}

TEST_FIXTURE(fixture, exclusive_and_async_exclusive) {
  auto contest = mon.try_exclusive();
  bool callback_called = false;
  asio::post(
      ioctx.get_executor(),
      [&]() {
        this->mon.async_exclusive(
            [&](mon_type::exclusive_lock lock) {
              callback_called = true;
              CHECK(lock.is_locked());
            });
        CHECK(!callback_called);
      });
  ioctx.poll();
  CHECK(!callback_called);

  contest.reset();
  ioctx.poll();
  CHECK(callback_called);
}

TEST_FIXTURE(fixture, uncontested_upgrade_to_try_exclusive) {
  auto uplock = mon.try_upgrade();
  auto exlock = uplock->try_exclusive();

  CHECK(uplock->is_locked());
  CHECK(exlock.has_value());
  CHECK(exlock->is_locked());
}

TEST_FIXTURE(fixture, uncontested_move_upgrade_to_try_exclusive) {
  auto uplock = mon.try_upgrade();
  auto exlock = std::move(*uplock).try_exclusive();

  CHECK(!uplock->is_locked());
  CHECK(exlock.has_value());
  CHECK(exlock->is_locked());
}

TEST_FIXTURE(fixture, shared_and_upgrade_to_try_exclusive) {
  auto contest = mon.try_shared();
  auto uplock = mon.try_upgrade();
  auto exlock = uplock->try_exclusive();

  CHECK(uplock->is_locked());
  CHECK(!exlock.has_value());
}

TEST_FIXTURE(fixture, shared_and_move_upgrade_to_try_exclusive) {
  auto contest = mon.try_shared();
  auto uplock = mon.try_upgrade();
  auto exlock = std::move(*uplock).try_exclusive();

  CHECK(uplock->is_locked());
  CHECK(!exlock.has_value());
}

TEST_FIXTURE(fixture, upgrade_and_upgrade_to_try_exclusive) {
  auto uplock = mon.try_upgrade();
  auto uplock_2 = uplock;
  auto exlock = uplock->try_exclusive();

  CHECK(uplock->is_locked());
  CHECK(exlock.has_value());
}

TEST_FIXTURE(fixture, upgrade_and_move_upgrade_to_try_exclusive) {
  auto uplock = mon.try_upgrade();
  auto uplock_2 = uplock;
  auto exlock = std::move(*uplock).try_exclusive();

  CHECK(!uplock->is_locked());
  CHECK(exlock.has_value());
}

TEST_FIXTURE(fixture, exclusive_and_upgrade_to_try_exclusive) {
  auto uplock = mon.try_upgrade();
  auto exlock_2 = uplock->try_exclusive();
  auto exlock = uplock->try_exclusive();

  CHECK(uplock->is_locked());
  CHECK(exlock.has_value());
}

TEST_FIXTURE(fixture, exclusive_and_move_upgrade_to_try_exclusive) {
  auto uplock = mon.try_upgrade();
  auto exlock_2 = uplock->try_exclusive();
  auto exlock = std::move(*uplock).try_exclusive();

  CHECK(!uplock->is_locked());
  CHECK(exlock.has_value());
}

TEST_FIXTURE(fixture, uncontested_exclusive_to_upgrade) {
  auto exlock = mon.try_exclusive();
  auto uplock = exlock->as_upgrade_lock();

  CHECK(exlock->is_locked());
  CHECK(uplock.is_locked());
}

TEST_FIXTURE(fixture, uncontested_move_exclusive_to_upgrade) {
  auto exlock = mon.try_exclusive();
  auto uplock = std::move(*exlock).as_upgrade_lock();

  CHECK(!exlock->is_locked());
  CHECK(uplock.is_locked());
}

TEST_FIXTURE(fixture, upgrade_while_upgrade_queued) {
  auto exlock = mon.try_exclusive();
  auto uplock = exlock->as_upgrade_lock();
  enqueue_upgrade();
  exlock.reset();

  exlock = uplock.try_exclusive();

  CHECK(uplock.is_locked());
  CHECK(exlock.has_value());
  CHECK(exlock->is_locked());
}

TEST_FIXTURE(fixture, upgrade_while_exclusive_queued) {
  auto exlock = mon.try_exclusive();
  auto uplock = exlock->as_upgrade_lock();
  enqueue_upgrade();
  exlock.reset();

  exlock = uplock.try_exclusive();

  CHECK(uplock.is_locked());
  CHECK(exlock.has_value());
  CHECK(exlock->is_locked());
}

TEST_FIXTURE(fixture, upgrade_while_upgrade_held) {
  auto exlock = mon.try_exclusive();
  auto uplock = exlock->as_upgrade_lock();
  auto extra = uplock;
  enqueue_upgrade();
  exlock.reset();

  exlock = uplock.try_exclusive();

  CHECK(uplock.is_locked());
  CHECK(exlock.has_value());
  CHECK(exlock->is_locked());
}

TEST_FIXTURE(fixture, upgrade_while_exclusive_held) {
  auto exlock = mon.try_exclusive();
  auto uplock = exlock->as_upgrade_lock();
  auto extra = *exlock;
  enqueue_upgrade();
  exlock.reset();

  exlock = uplock.try_exclusive();

  CHECK(uplock.is_locked());
  CHECK(exlock.has_value());
  CHECK(exlock->is_locked());
}

int main() {
  return UnitTest::RunAllTests();
}
