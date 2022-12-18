#include <earnest/detail/array_cat.h>

#include "UnitTest++/UnitTest++.h"

using earnest::detail::array_cat;

TEST(cat) {
  const auto expected = std::array<int, 3>{ 1, 2, 3 };
  auto result = array_cat<int>(std::array<int, 1>{ 1 }, std::array<int, 2>{ 2, 3 });

  CHECK(expected == result);
}

int main() {
  return UnitTest::RunAllTests();
}
