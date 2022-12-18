#include <earnest/detail/constexpr_sum.h>

#include <type_traits>

#include "UnitTest++/UnitTest++.h"

using earnest::detail::constexpr_sum;

TEST(zero) {
  constexpr auto x = constexpr_sum();

  static_assert(x == 0);
  static_assert(std::is_unsigned_v<decltype(x)>);
}

TEST(negative_value) {
  constexpr auto x = constexpr_sum(-1, -2, -3);

  static_assert(x == -6);
  static_assert(std::is_signed_v<decltype(x)>);
}

TEST(signed_value) {
  constexpr auto x = constexpr_sum(1, 2, 3);

  static_assert(x == 6);
  static_assert(std::is_signed_v<decltype(x)>);
}

TEST(unsigned_value) {
  constexpr auto x = constexpr_sum(1u, 2u, 3u);

  static_assert(x == 6u);
  static_assert(std::is_unsigned_v<decltype(x)>);
}

TEST(mixed_signs) {
  constexpr auto x = constexpr_sum(1, 2u);

  static_assert(x == 3);
  static_assert(std::is_unsigned_v<decltype(x)>);
}

int main() {
  return UnitTest::RunAllTests();
}
