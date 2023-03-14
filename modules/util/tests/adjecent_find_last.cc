#include <earnest/detail/adjecent_find_last.h>

#include <UnitTest++/UnitTest++.h>

#include <array>
#include <vector>

using earnest::detail::adjecent_find_last;

TEST(can_find_adjecent) {
  std::array<int, 4> arr{ 0, 1, 1, 2 };

  CHECK(adjecent_find_last(arr.begin(), arr.end()) == arr.begin() + 1);
}

TEST(finds_the_last_adjecent_element) {
  std::vector<int> arr{ 0, 1, 1, 2, 2, 3, 4, 5, 5, 5, 5, 6, 7, 7, 8, 9 };

  CHECK(adjecent_find_last(arr.begin(), arr.end()) == arr.begin() + 12);
}

TEST(can_find_the_first_element) {
  std::vector<int> arr{ 0, 0, 1, 2, 3, 4, 5, 6, 7 };

  CHECK(adjecent_find_last(arr.begin(), arr.end()) == arr.begin());
}

TEST(can_find_adjecent_element_even_if_it_is_the_last_element) {
  std::vector<int> arr{ 0, 1, 2, 3, 4, 4 };

  CHECK(adjecent_find_last(arr.begin(), arr.end()) == arr.begin() + 4);
}

TEST(when_there_is_no_adjecent_elements_returns_last) {
  std::vector<int> arr{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

  CHECK(adjecent_find_last(arr.begin(), arr.end()) == arr.end());
}

int main() {
  return UnitTest::RunAllTests();
}
