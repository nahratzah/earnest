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
