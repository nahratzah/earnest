#include <earnest/db_cache.h>

#include <UnitTest++/UnitTest++.h>

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
    cache_wrapper(db_type::executor_type ex, std::allocator<std::byte> alloc)
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

TEST_FIXTURE(db_cache_fixture, on_load_called_on_get) {
  class element_with_onload : public earnest::db_cache_value {
    public:
    element_with_onload(int* on_load_counter)
    : on_load_counter(on_load_counter)
    {
      REQUIRE CHECK(on_load_counter != nullptr);
    }

    private:
    auto on_load() noexcept -> void override {
      ++*on_load_counter;
    }

    int* on_load_counter = nullptr;
  };

  cycle_ptr::cycle_gptr<element_with_onload> p0;
  int on_load_called = 0;

  // First load. Should call the onload callback.
  cache->async_get(
      key_type<element_with_onload>{
        .file{"", "testfile"},
        .offset=0,
      },
      [&p0](std::error_code ec, cycle_ptr::cycle_gptr<element_with_onload> ptr) {
        CHECK_EQUAL(std::error_code(), ec);
        CHECK(ptr != nullptr);
        p0 = ptr;
      },
      [](int* on_load_called) {
        return asio::deferred.values(std::error_code(), cycle_ptr::make_cycle<element_with_onload>(on_load_called));
      },
      &on_load_called);
  ioctx.run();
  ioctx.restart();
  CHECK_EQUAL(1, on_load_called); // Called at construction.

  // Second load. Since the pointer is still valid (saved in `p0` to make sure), this should not cause an onload callback.
  cache->async_get(
      key_type<element_with_onload>{
        .file{"", "testfile"},
        .offset=0,
      },
      [&p0](std::error_code ec, cycle_ptr::cycle_gptr<element_with_onload> ptr) {
        CHECK_EQUAL(std::error_code(), ec);
        CHECK(ptr != nullptr);
        CHECK_EQUAL(p0, ptr);
      },
      [](int* on_load_called) {
        return asio::deferred.values(std::error_code(), cycle_ptr::make_cycle<element_with_onload>(on_load_called));
      },
      &on_load_called);
  ioctx.run();
  ioctx.restart();
  CHECK_EQUAL(1, on_load_called); // Second load doesn't call the on-load callback.
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
