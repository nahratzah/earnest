#include <earnest/execution.h>

#include <UnitTest++/UnitTest++.h>
#include <tuple>
#include <type_traits>
#include <variant>
#include <string>

using namespace earnest::execution;


// Confirm sender concept works correctly.
// It's based on the sender_traits.
struct not_a_sender {};
static_assert(!sender<not_a_sender>);

struct my_untyped_sender : public sender_base {};
static_assert(sender<my_untyped_sender>);

struct my_typed_sender {
  template<template<typename...> class Tuple, template<typename...> class Variant>
  using value_types = Variant<Tuple<>>;

  template<template<typename...> class Variant>
  using error_types = Variant<>;

  static constexpr bool sends_done = false;
};
static_assert(sender<my_typed_sender>);


template<bool is_noexcept>
struct fake_scheduler {
  template<typename Receiver>
  struct op_state {
    friend auto tag_invoke([[maybe_unused]] start_t, op_state& self) noexcept -> void {
      if (self.sch.start_mark != nullptr) *self.sch.start_mark = true;

      try {
        set_value(std::move(self.rcv));
      } catch (...) {
        set_error(std::move(self.rcv), std::current_exception());
      }
    }

    fake_scheduler sch;
    Receiver rcv;
  };

  struct sender_impl {
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<>;

    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr>;

    static constexpr bool sends_done = false;

    template<typename Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& receiver) noexcept(is_noexcept) -> op_state<std::remove_cvref_t<Receiver>> {
      return {std::move(self.sch), std::forward<Receiver>(receiver)};
    }

    friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_value_t>, const sender_impl& self) noexcept -> fake_scheduler {
      return self.sch;
    }
    friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_error_t>, const sender_impl& self) noexcept -> fake_scheduler {
      return self.sch;
    }
    friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_done_t>, const sender_impl& self) noexcept -> fake_scheduler {
      return self.sch;
    }

    fake_scheduler sch;
  };

  friend auto tag_invoke([[maybe_unused]] schedule_t, fake_scheduler&& self) noexcept(is_noexcept) -> sender_impl {
    return {self};
  }

  friend auto tag_invoke([[maybe_unused]] schedule_t, fake_scheduler& self) noexcept(is_noexcept) -> sender_impl {
    return {self};
  }

  constexpr auto operator==([[maybe_unused]] const fake_scheduler&) const noexcept { return true; }
  constexpr auto operator!=([[maybe_unused]] const fake_scheduler&) const noexcept { return false; }

  // If this pointer is non-null, will set it to `true` once the scheduler is started.
  bool* start_mark = nullptr;
};

TEST(just) {
  // Confirm type-defs match.
  using just_traits = sender_traits<decltype(just(1, 2, 3))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<int, int, int>>,
      just_traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr>,
      just_traits::error_types<std::variant>>);
  static_assert(!just_traits::sends_done);

  // Confirm outcome matches.
  auto [x, y, z] = sync_wait(just(1, 2, 3)).value();
  CHECK_EQUAL(1, x);
  CHECK_EQUAL(2, y);
  CHECK_EQUAL(3, z);
}

TEST(start_detached) {
  bool executed = false;
  start_detached(
      just()
      | lazy_then(
          [&executed]() {
            CHECK(!executed);
            executed = true;
          }));
  CHECK(executed);
}

TEST(then) {
  // Confirm type-defs match.
  using just_traits = sender_traits<decltype(just())>;
  using then_traits = sender_traits<decltype(just() | then([]() { return 1; }))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<int>>,
      then_traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      just_traits::error_types<std::variant>,
      then_traits::error_types<std::variant>>);
  static_assert(then_traits::sends_done == just_traits::sends_done);

  auto [x] = sync_wait(
      just(1)
      | then([](int x) noexcept { return x + 1; })).value();
  CHECK_EQUAL(2, x);

  auto [y] = sync_wait(
      just(1, 2, 3)
      | then([](int x, int y, int z) noexcept { return x + y + z; })).value();
  CHECK_EQUAL(6, y);

  auto [z] = sync_wait(
      just(4)
      | then([](int x) { return x + 1; })).value();
  CHECK_EQUAL(5, z);

  // Check that a function returning void works correctly.
  auto void_function = sync_wait(
      just(4)
      | then([](int x) { return; })).value();
  CHECK(std::tuple<>() == void_function);
}

TEST(lazy_then) {
  // Confirm type-defs match.
  using just_traits = sender_traits<decltype(just())>;
  using lazy_then_traits = sender_traits<decltype(just() | lazy_then([]() { return 1; }))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<int>>,
      lazy_then_traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      just_traits::error_types<std::variant>,
      lazy_then_traits::error_types<std::variant>>);
  static_assert(lazy_then_traits::sends_done == just_traits::sends_done);

  auto [x] = sync_wait(
      just(3)
      | lazy_then([](int x) noexcept { return x + 1; })).value();
  CHECK_EQUAL(4, x);

  auto [y] = sync_wait(
      just(1, 2, 3)
      | lazy_then([](int x, int y, int z) noexcept { return x + y + z; })).value();
  CHECK_EQUAL(6, y);
}

TEST(lazy_upon_error) {
  // Confirm type-defs match.
  using lazy_upon_error_traits = sender_traits<decltype(just(1) | lazy_upon_error([](std::exception_ptr x) { return std::string(); }))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<int>, std::tuple<std::string>>,
      lazy_upon_error_traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr>,
      lazy_upon_error_traits::error_types<std::variant>>);
  static_assert(!lazy_upon_error_traits::sends_done);

  class test_exception {};

  // Test that the exception is received.
  auto [x] = sync_wait(
      just(1)
      | then([](int x) -> int { throw test_exception{}; })
      | lazy_upon_error(
          [](std::exception_ptr ex) {
            CHECK(ex != nullptr);
            try {
              std::rethrow_exception(ex);
            } catch (const test_exception&) {
              // Nothing to do here, this is the result we want.
            }
            return 2;
          })).value();
  CHECK_EQUAL(2, x);

  // If there is no exception, upon_error will not invoke the function.
  auto [y] = sync_wait(
      just(1)
      | lazy_upon_error(
          [](std::exception_ptr ex) {
            CHECK(ex != nullptr);
            try {
              std::rethrow_exception(ex);
            } catch (const test_exception&) {
              // Nothing to do here, this is the result we want.
            }
            return 2;
          })).value();
  CHECK_EQUAL(1, y);

  // Upon-error will not handle exceptions that are later down the chain.
  // If there is no exception, upon_error will not invoke the function.
  auto [z] = sync_wait(
      just(1)
      | lazy_upon_error(
          [](std::exception_ptr ex) {
            CHECK(false);
            return 2;
          })
      | then([](int x) -> int { throw test_exception{}; })
      | lazy_upon_error(
          [](std::exception_ptr ex) {
            CHECK(ex != nullptr);
            try {
              std::rethrow_exception(ex);
            } catch (const test_exception&) {
              // Nothing to do here, this is the result we want.
            }
            return 3;
          })).value();
  CHECK_EQUAL(3, z);
}

TEST(upon_error) {
  auto then_fn = []() noexcept(false) { return 1; };
  auto upon_error_fn = [](std::exception_ptr ex) { return; };
  // upon_error forwards to lazy_upon_error, so the two invocations should be the same.
  //
  // This test declares then_fn and upon_error_fn, so that they'll have the same types in both chains.
  // It also uses lazy_then, to assert that the chain won't eagerly resolve the upon_error.
  bool same_types =
      std::is_same_v<
          decltype(just() | lazy_then(then_fn) | lazy_upon_error(upon_error_fn)),
          decltype(just() | lazy_then(then_fn) | upon_error(upon_error_fn))>;
  CHECK(same_types);
}

// XXX create test for lazy_upon_done.

TEST(upon_done) {
  auto then_fn = []() noexcept(false) { return 1; };
  auto upon_error_fn = []() { return; };
  // upon_done forwards to lazy_upon_done, so the two invocations should be the same.
  //
  // This test declares then_fn and upon_error_fn, so that they'll have the same types in both chains.
  // It also uses lazy_then, to assert that the chain won't eagerly resolve the upon_error.
  bool same_types =
      std::is_same_v<
          decltype(just() | lazy_then(then_fn) | lazy_upon_done(upon_error_fn)),
          decltype(just() | lazy_then(then_fn) | upon_done(upon_error_fn))>;
  CHECK(same_types);
}

TEST(into_variant) {
  // Confirm type-defs match.
  using lazy_upon_error_traits = sender_traits<decltype(just(1, 2) | into_variant())>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::variant<std::tuple<int, int>>>>,
      lazy_upon_error_traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr>,
      lazy_upon_error_traits::error_types<std::variant>>);
  static_assert(!lazy_upon_error_traits::sends_done);

  const auto expected = std::variant<std::tuple<int, int>>(std::make_tuple(3, 4));
  sync_wait(
      just(3, 4)
      | into_variant()
      | then(
          [expected](std::variant<std::tuple<int, int>> x) {
            CHECK(expected == x);
          }));
}

TEST(transfer) {
  bool scheduler_started = false;
  auto [x] = sync_wait(
      just(1)
      | lazy_then([&scheduler_started](int x) { CHECK(!scheduler_started); return x; })
      | transfer(fake_scheduler<false>{&scheduler_started})
      | then([&scheduler_started](int x) noexcept { CHECK(scheduler_started); return x + 1; })).value();
  CHECK_EQUAL(2, x);

  scheduler_started = false;
  auto [y] = sync_wait(
      just(2)
      | lazy_then([&scheduler_started](int x) { CHECK(!scheduler_started); return x; })
      | transfer(fake_scheduler<true>{&scheduler_started})
      | then([&scheduler_started](int x) noexcept { CHECK(scheduler_started); return x + 2; })).value();
  CHECK_EQUAL(4, y);
}

TEST(lazy_let_value) {
  // Confirm type-defs match.
  using let_value_traits = sender_traits<decltype(just() | lazy_let_value([]() { return just(1); }))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<int>>,
      let_value_traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr>,
      let_value_traits::error_types<std::variant>>);
  static_assert(let_value_traits::sends_done == false);

  auto [x] = sync_wait(
      just(17)
      | lazy_let_value(
          [](int i) {
            CHECK_EQUAL(17, i);
            return just(19);
          })
      ).value();
  CHECK_EQUAL(19, x);
}

TEST(adapters_are_chainable) {
  // We want to verify that an adapter-chain can be late-bound.
  // In order to allow check this, we create an adapter chain with
  // all adapters.
  //
  // The first and last adapter are not so much part of the test,
  // but to ensure that even the first element in the chain
  // gets added to an adapter, and the last adapter can be passed
  // to an adapter.
  auto adapter_chain =
      then([]() { return 1; }) // to make sure the next adapter will have to deal with |
      | then([](int i) { return i + 1; }) // returns 2
      | then([](int i) -> int {
            CHECK(i == 2);
            throw std::exception(); // test-exception will be handled in the next step
          })
      | upon_error([](std::exception_ptr ex) { return 4; }) // returns 4
      | transfer(fake_scheduler<true>{}) // still returning 4
      | let_value(
          [](int i) {
            return just(i + 1); // returns 5
          })
      | then([](int i) { return i; }); // to make sure the previous adapter will have to deal with |

  auto [x] = sync_wait(
      just()
      | std::move(adapter_chain)).value();
  CHECK_EQUAL(5, x);
}

TEST(adapter_chains_are_chainable) {
  auto adapter_chain_1 =
      then([](std::string s) { return s + "a"; })
      | then([](std::string s) { return s + "b"; });
  auto adapter_chain_2 =
      then([](std::string s) { return s + "c"; })
      | then([](std::string s) { return s + "d"; });
  auto adapter_chain = std::move(adapter_chain_1) | std::move(adapter_chain_2); // <-- what we're testing.

  auto [x] = sync_wait(
      just(std::string())
      | std::move(adapter_chain)).value();
  CHECK_EQUAL(std::string("abcd"), x);
}

int main() {
  return UnitTest::RunAllTests();
}
