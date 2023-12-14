#include <earnest/xdr_v2.h>

#include <UnitTest++/UnitTest++.h>

#include "buffer.h"
#include <array>
#include <set>
#include <string>
#include <string_view>
#include <vector>

using earnest::execution::just;
using earnest::execution::sync_wait;
using namespace earnest::xdr_v2;

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

TEST(write_vector_empty_collection) {
  std::vector<std::string> c;
  static_assert(decltype(collection.write(c, ascii_string))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer())
      | collection.write(c, ascii_string).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x00 }),
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

TEST(write_vector) {
  std::vector<std::string> c = { std::string("abcde") };
  static_assert(decltype(collection.write(c, ascii_string))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer())
      | collection.write(c, ascii_string).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 'e', 0x00, 0x00, 0x00 }),
      result);
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

TEST(write_array) {
  std::array<std::string, 1> c = { std::string("abcde") };
  static_assert(decltype(fixed_collection.write(c, ascii_string))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer())
      | fixed_collection.write(c, ascii_string).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 'e', 0x00, 0x00, 0x00 }),
      result);
}

TEST(read_array_with_bound_operation) {
  std::array<std::string, 1> c;
  static_assert(decltype(fixed_collection(ascii_string).read(c))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 'e', 0x00, 0x00, 0x00, 'x', 'x', 'x', 'x' }))
      | fixed_collection(ascii_string).read(c).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result);
  REQUIRE CHECK_EQUAL(1, c.size());
  CHECK_EQUAL(std::string_view("abcde"), c[0]);
}

TEST(write_array_with_bound_operation) {
  std::array<std::string, 1> c = { std::string("abcde") };
  static_assert(decltype(fixed_collection(ascii_string).write(c))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer())
      | fixed_collection(ascii_string).write(c).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 'e', 0x00, 0x00, 0x00 }),
      result);
}

TEST(read_set) {
  std::set<std::string> c;
  static_assert(decltype(collection.read(c, ascii_string))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 'e', 0x00, 0x00, 0x00, 'x', 'x', 'x', 'x' }))
      | collection.read(c, ascii_string).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result);
  REQUIRE CHECK_EQUAL(1, c.size());
  CHECK_EQUAL(std::string_view("abcde"), *c.begin());
}

TEST(write_set) {
  std::set<std::string> c = { std::string("abcde") };
  static_assert(decltype(collection.write(c, ascii_string))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer())
      | collection.write(c, ascii_string).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 'e', 0x00, 0x00, 0x00 }),
      result);
}

TEST(read_set_with_duplicates) {
  std::set<std::string> c;
  CHECK_THROW(
      sync_wait(
          just(buffer({ 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 'e', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 'e', 0x00, 0x00, 0x00 }))
          | collection.read(c, ascii_string).sender_chain()),
      xdr_error);
}

TEST(read_multiset_with_duplicates) {
  std::multiset<std::string> c;
  auto [result] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 'e', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 'e', 0x00, 0x00, 0x00 }))
      | collection.read(c, ascii_string).sender_chain()).value();
  CHECK_EQUAL(
      buffer(),
      result);
  REQUIRE CHECK_EQUAL(2, c.size());
  for (const auto& elem : c) CHECK_EQUAL(std::string_view("abcde"), elem);
}

TEST(write_multiset_with_duplicates) {
  std::multiset<std::string> c = { std::string("abcde"), std::string("abcde") };
  auto [result] = sync_wait(
      just(buffer())
      | collection.write(c, ascii_string).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 'e', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 'e', 0x00, 0x00, 0x00 }),
      result);
}

}
