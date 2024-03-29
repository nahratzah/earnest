#include <earnest/execution_util.h>

#include <UnitTest++/UnitTest++.h>

#include <optional>
#include <string>
#include <vector>

#include "fake_scheduler.h"
#include "senders.h"

using namespace earnest::execution;


TEST(repeat_with_scheduler) {
  std::vector<std::string> expected{ "0", "1", "2", "3" };

  // Test with a scheduler present.
  auto [result_1] = sync_wait(
      just(std::vector<std::string>(4))
      | repeat(
          [](std::size_t idx, std::vector<std::string>& arr) {
            CHECK(idx <= arr.size());

            auto invocation = [&]() {
              return std::make_optional(
                  just()
                  | then(
                      [idx, &arr]() {
                        REQUIRE CHECK(idx >= 0 && idx < arr.size());
                        CHECK_EQUAL(std::string{}, arr[idx]); // Must be the first time.
                        arr[idx] = std::to_string(idx);
                      }));
            };
            using result_type = decltype(invocation());

            if (idx < arr.size()) {
              return invocation();
            } else {
              return result_type(std::nullopt);
            }
          })
      | pretend_scheduler(fake_scheduler<false>{})).value();

  CHECK_EQUAL(expected.size(), result_1.size());
  CHECK_ARRAY_EQUAL(expected.data(), result_1.data(), std::min(expected.size(), result_1.size()));

  // Test without a scheduler.
  auto [result_2] = sync_wait(
      just(std::vector<std::string>(4))
      | repeat(
          [](std::size_t idx, std::vector<std::string>& arr) {
            CHECK(idx <= arr.size());

            auto invocation = [&]() {
              return std::make_optional(
                  just()
                  | then(
                      [idx, &arr]() {
                        REQUIRE CHECK(idx >= 0 && idx < arr.size());
                        CHECK_EQUAL(std::string{}, arr[idx]); // Must be the first time.
                        arr[idx] = std::to_string(idx);
                      }));
            };
            using result_type = decltype(invocation());

            if (idx < arr.size()) {
              return invocation();
            } else {
              return result_type(std::nullopt);
            }
          })
      | pretend_no_scheduler()).value();

  CHECK_EQUAL(expected.size(), result_2.size());
  CHECK_ARRAY_EQUAL(expected.data(), result_2.data(), std::min(expected.size(), result_2.size()));
}

TEST(explode_tuple) {
  auto fn = []() {
    return std::make_tuple(std::string("abc"), std::string("def"));
  };

  using explode_tuple_traits = sender_traits<decltype(just() | then(fn) | explode_tuple())>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::string, std::string>>,
      explode_tuple_traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr>,
      explode_tuple_traits::error_types<std::variant>>);
  static_assert(explode_tuple_traits::sends_done == false);

  auto [x, y] = sync_wait(just() | then(fn) | explode_tuple()).value();
  CHECK_EQUAL(std::string("abc"), x);
  CHECK_EQUAL(std::string("def"), y);
}

TEST(explode_variant) {
  auto fn = []() {
    return std::variant<std::string, int>(std::string("abc"));
  };

  using explode_variant_traits = sender_traits<decltype(just() | then(fn) | explode_variant())>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::string>, std::tuple<int>>,
      explode_variant_traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr>,
      explode_variant_traits::error_types<std::variant>>);
  static_assert(explode_variant_traits::sends_done == false);

  auto [x] = sync_wait(just() | then(fn) | explode_variant() | into_variant()).value();
  REQUIRE CHECK(std::holds_alternative<std::tuple<std::string>>(x));
  CHECK_EQUAL(
      std::string("abc"),
      std::get<0>(std::get<std::tuple<std::string>>(x)));
}

TEST(let_variant) {
  auto fn = []() {
    auto string_sender = just(std::string("bla bla chocoladevla"));
    auto int_sender = just(int(0));
    return std::variant<decltype(string_sender), decltype(int_sender)>(std::move(string_sender));
  };

  using let_variant_traits = sender_traits<decltype(just() | let_variant(fn))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::string>, std::tuple<int>>,
      let_variant_traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr>,
      let_variant_traits::error_types<std::variant>>);
  static_assert(let_variant_traits::sends_done == false);

  auto [x] = sync_wait(just() | let_variant(fn) | into_variant()).value();
  REQUIRE CHECK(std::holds_alternative<std::tuple<std::string>>(x));
  CHECK_EQUAL(
      std::string("bla bla chocoladevla"),
      std::get<0>(std::get<std::tuple<std::string>>(x)));
}

TEST(type_erased_sender) {
  using traits = sender_traits<decltype(type_erased_sender(just(2, 3, 4)))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<int, int, int>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr>,
      traits::error_types<std::variant>>);
  static_assert(traits::sends_done == false);

  auto [x, y, z] = sync_wait(type_erased_sender(just(2, 3, 4))).value();
  CHECK_EQUAL(2, x);
  CHECK_EQUAL(3, y);
  CHECK_EQUAL(4, z);
}

TEST(validation) {
  class test_error {};
  auto vfn = [](int i) -> std::optional<test_error> {
    if (i == 4)
      return test_error{};
    else
      return std::nullopt;
  };

  using traits = sender_traits<decltype(just(4) | validation(vfn))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<int>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<test_error, std::exception_ptr>,
      traits::error_types<std::variant>>);
  static_assert(traits::sends_done == false);

  auto [x] = sync_wait(just(0) | validation(vfn)).value(); // no error
  CHECK_EQUAL(0, x);

  // Input 4, and if there is a test_error, transform it into 5.
  auto [y] = sync_wait(
      just(4)
      | validation(vfn)
      | lazy_upon_error(
          [](auto err) {
            CHECK((std::is_same_v<test_error, decltype(err)>));
            return 5;
          })).value();
  CHECK_EQUAL(5, y); // Confirms that the error was received.
}
