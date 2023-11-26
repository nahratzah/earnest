#include <earnest/xdr.h>

#include <UnitTest++/UnitTest++.h>

#include "buffer.h"
#include <string>
#include <string_view>
#include <vector>
#include <array>

using earnest::execution::just;
using earnest::execution::sync_wait;
using namespace earnest::xdr;

SUITE(collection) {

TEST(read_vector_empty_collection) {
  std::vector<std::string> c;
  static_assert(decltype(collection.read(c, ascii_string))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x00, 0x00, 'x', 'x', 'x', 'x' }))
      | collection.read(c, ascii_string).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result);
  CHECK_EQUAL(0, c.size());
}

TEST(read_vector) {
  std::vector<std::string> c;
  static_assert(decltype(collection.read(c, ascii_string))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 'e', 0x00, 0x00, 0x00, 'x', 'x', 'x', 'x' }))
      | collection.read(c, ascii_string).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result);
  REQUIRE CHECK_EQUAL(1, c.size());
  CHECK_EQUAL(std::string_view("abcde"), c[0]);
}

TEST(read_array) {
  std::array<std::string, 1> c;
  static_assert(decltype(fixed_collection.read(c, ascii_string))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 'e', 0x00, 0x00, 0x00, 'x', 'x', 'x', 'x' }))
      | fixed_collection.read(c, ascii_string).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result);
  REQUIRE CHECK_EQUAL(1, c.size());
  CHECK_EQUAL(std::string_view("abcde"), c[0]);
}

}
