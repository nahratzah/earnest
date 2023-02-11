#include <earnest/db_cache.h>
#include "UnitTest++/UnitTest++.h"

#include <asio/io_context.hpp>
#include <asio/deferred.hpp>

class db_cache_fixture {
  public:
  using db_type = earnest::db_cache<asio::io_context::executor_type>;
  template<typename T> using key_type = db_type::key_type<T>;

  db_cache_fixture();

  private:
  class cache_wrapper
  : private cycle_ptr::cycle_base
  {
    public:
    cache_wrapper(db_type::executor_type ex, db_type::allocator_type alloc)
    : cache_(*this, ex, alloc)
    {}

    cache_wrapper(db_type::executor_type ex)
    : cache_(*this, ex)
    {}

    auto get() -> db_type& { return cache_; }
    auto get() const -> const db_type& { return cache_; }

    private:
    db_type cache_;
  };

  public:
  asio::io_context ioctx;

  private:
  const cycle_ptr::cycle_gptr<cache_wrapper> cache_impl;

  public:
  db_type*const cache;
};

class element
: public earnest::db_cache_value
{
  public:
  explicit element(int i) noexcept
  : i(i)
  {}

  int i;
};

TEST_FIXTURE(db_cache_fixture, get) {
  bool callback_called = false;
  cache->async_get(
      key_type<element>{
        .file{"", "testfile"},
        .offset=0,
      },
      [&callback_called](std::error_code ec, cycle_ptr::cycle_gptr<element> ptr) {
        callback_called = true;
        CHECK_EQUAL(std::error_code(), ec);
        CHECK(ptr != nullptr);
        if (ptr != nullptr) CHECK_EQUAL(4, ptr->i);
      },
      [](int i) {
        return asio::deferred.values(std::error_code(), cycle_ptr::make_cycle<element>(i));
      },
      4);
  ioctx.run();
  ioctx.restart();

  CHECK(callback_called);
}

TEST_FIXTURE(db_cache_fixture, get_error) {
  bool callback_called = false;
  cache->async_get(
      key_type<element>{
        .file{"", "testfile"},
        .offset=0,
      },
      [&callback_called](std::error_code ec, cycle_ptr::cycle_gptr<element> ptr) {
        callback_called = true;
        CHECK_EQUAL(make_error_code(std::errc::bad_address), ec);
        CHECK(ptr == nullptr);
      },
      []([[maybe_unused]] int i) {
        return asio::deferred.values(make_error_code(std::errc::bad_address), nullptr);
      },
      4);
  ioctx.run();
  ioctx.restart();

  CHECK(callback_called);
}

int main() {
  return UnitTest::RunAllTests();
}

db_cache_fixture::db_cache_fixture()
: cache_impl(cycle_ptr::make_cycle<cache_wrapper>(ioctx.get_executor())),
  cache(&this->cache_impl->get())
{
  ioctx.run();
  ioctx.restart();
}
