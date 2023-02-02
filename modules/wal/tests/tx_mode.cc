#include <earnest/tx_mode.h>

#include "UnitTest++/UnitTest++.h"

namespace earnest {

TEST(read_permitted) {
  CHECK(!read_permitted(tx_mode::decline));
  CHECK(read_permitted(tx_mode::read_only));
  CHECK(!read_permitted(tx_mode::write_only));
  CHECK(read_permitted(tx_mode::read_write));
}

TEST(write_permitted) {
  CHECK(!write_permitted(tx_mode::decline));
  CHECK(!write_permitted(tx_mode::read_only));
  CHECK(write_permitted(tx_mode::write_only));
  CHECK(write_permitted(tx_mode::read_write));
}

TEST(tx_mode_invert) {
  CHECK_EQUAL(tx_mode::read_write, ~tx_mode::decline);
  CHECK_EQUAL(tx_mode::write_only, ~tx_mode::read_only);
  CHECK_EQUAL(tx_mode::read_only, ~tx_mode::write_only);
  CHECK_EQUAL(tx_mode::decline, ~tx_mode::read_write);
}

TEST(tx_mode_and) {
  CHECK_EQUAL(tx_mode::decline,    tx_mode::decline    & tx_mode::decline);
  CHECK_EQUAL(tx_mode::decline,    tx_mode::decline    & tx_mode::read_only);
  CHECK_EQUAL(tx_mode::decline,    tx_mode::decline    & tx_mode::write_only);
  CHECK_EQUAL(tx_mode::decline,    tx_mode::decline    & tx_mode::read_write);

  CHECK_EQUAL(tx_mode::decline,    tx_mode::read_only  & tx_mode::decline);
  CHECK_EQUAL(tx_mode::read_only,  tx_mode::read_only  & tx_mode::read_only);
  CHECK_EQUAL(tx_mode::decline,    tx_mode::read_only  & tx_mode::write_only);
  CHECK_EQUAL(tx_mode::read_only,  tx_mode::read_only  & tx_mode::read_write);

  CHECK_EQUAL(tx_mode::decline,    tx_mode::write_only & tx_mode::decline);
  CHECK_EQUAL(tx_mode::decline,    tx_mode::write_only & tx_mode::read_only);
  CHECK_EQUAL(tx_mode::write_only, tx_mode::write_only & tx_mode::write_only);
  CHECK_EQUAL(tx_mode::write_only, tx_mode::write_only & tx_mode::read_write);

  CHECK_EQUAL(tx_mode::decline,    tx_mode::read_write & tx_mode::decline);
  CHECK_EQUAL(tx_mode::read_only,  tx_mode::read_write & tx_mode::read_only);
  CHECK_EQUAL(tx_mode::write_only, tx_mode::read_write & tx_mode::write_only);
  CHECK_EQUAL(tx_mode::read_write, tx_mode::read_write & tx_mode::read_write);
}

TEST(tx_mode_or) {
  CHECK_EQUAL(tx_mode::decline,    tx_mode::decline    | tx_mode::decline);
  CHECK_EQUAL(tx_mode::read_only,  tx_mode::decline    | tx_mode::read_only);
  CHECK_EQUAL(tx_mode::write_only, tx_mode::decline    | tx_mode::write_only);
  CHECK_EQUAL(tx_mode::read_write, tx_mode::decline    | tx_mode::read_write);

  CHECK_EQUAL(tx_mode::read_only,  tx_mode::read_only  | tx_mode::decline);
  CHECK_EQUAL(tx_mode::read_only,  tx_mode::read_only  | tx_mode::read_only);
  CHECK_EQUAL(tx_mode::read_write, tx_mode::read_only  | tx_mode::write_only);
  CHECK_EQUAL(tx_mode::read_write, tx_mode::read_only  | tx_mode::read_write);

  CHECK_EQUAL(tx_mode::write_only, tx_mode::write_only | tx_mode::decline);
  CHECK_EQUAL(tx_mode::read_write, tx_mode::write_only | tx_mode::read_only);
  CHECK_EQUAL(tx_mode::write_only, tx_mode::write_only | tx_mode::write_only);
  CHECK_EQUAL(tx_mode::read_write, tx_mode::write_only | tx_mode::read_write);

  CHECK_EQUAL(tx_mode::read_write, tx_mode::read_write | tx_mode::decline);
  CHECK_EQUAL(tx_mode::read_write, tx_mode::read_write | tx_mode::read_only);
  CHECK_EQUAL(tx_mode::read_write, tx_mode::read_write | tx_mode::write_only);
  CHECK_EQUAL(tx_mode::read_write, tx_mode::read_write | tx_mode::read_write);
}

} /* namespace earnest */

int main() {
  return UnitTest::RunAllTests();
}
