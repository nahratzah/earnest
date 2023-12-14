#include <earnest/xdr_v2.h>

#include <UnitTest++/UnitTest++.h>

#include "buffer.h"

using earnest::execution::just;
using earnest::execution::sync_wait;
using namespace earnest::xdr_v2;

SUITE(variant) {

TEST(write_variant_value_0) {
  std::variant<int, std::string> v = int(7);
  static_assert(decltype(variant.write(v, uint32, ascii_string))::extent == std::dynamic_extent);
  static_assert(decltype(variant.write(std::declval<const std::variant<int, int>&>(), uint32, uint16))::extent == 8);
  auto [result] = sync_wait(
      just(buffer())
      | variant.write(v, uint32, ascii_string).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07 }),
      result);
};

TEST(write_variant_value_1) {
  std::variant<int, std::string> v = std::string("abcd");
  static_assert(decltype(variant.write(v, uint32, ascii_string))::extent == std::dynamic_extent);
  static_assert(decltype(variant.write(std::declval<const std::variant<int, int>&>(), uint32, uint16))::extent == 8);
  auto [result] = sync_wait(
      just(buffer())
      | variant.write(v, uint32, ascii_string).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 'a', 'b', 'c', 'd' }),
      result);
};

TEST(read_variant_value_0) {
  std::variant<int, std::string> v;
  static_assert(decltype(variant.read(v, uint32, ascii_string))::extent == std::dynamic_extent);
  static_assert(decltype(variant.read(std::declval<std::variant<int, int>&>(), uint32, uint16))::extent == 8);
  auto [result] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 'x', 'x', 'x', 'x' }))
      | variant.read(v, uint32, ascii_string).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result);
  REQUIRE CHECK(std::holds_alternative<int>(v));
  CHECK_EQUAL(7, std::get<int>(v));
};

TEST(read_variant_value_1) {
  std::variant<int, std::string> v;
  static_assert(decltype(variant.read(v, uint32, ascii_string))::extent == std::dynamic_extent);
  static_assert(decltype(variant.read(std::declval<std::variant<int, int>&>(), uint32, uint16))::extent == 8);
  auto [result] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 'a', 'b', 'c', 'd', 'x', 'x', 'x', 'x' }))
      | variant.read(v, uint32, ascii_string).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result);
  REQUIRE CHECK(std::holds_alternative<std::string>(v));
  CHECK_EQUAL(std::string_view("abcd"), std::get<std::string>(v));
};

TEST(write_variant_value_0_with_discriminant) {
  std::variant<int, std::string> v = int(7);
  static_assert(decltype(variant.write(v, uint32, variant.discriminant<0x13>(ascii_string)))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer())
      | variant.write(v, uint32, variant.discriminant<0x13>(ascii_string)).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07 }),
      result);
};

TEST(write_variant_value_1_with_discriminant) {
  std::variant<int, std::string> v = std::string("abcd");
  static_assert(decltype(variant.write(v, uint32, variant.discriminant<0x13>(ascii_string)))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer())
      | variant.write(v, uint32, variant.discriminant<0x13>(ascii_string)).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x13, 0x00, 0x00, 0x00, 0x04, 'a', 'b', 'c', 'd' }),
      result);
};

TEST(read_variant_value_0_with_discriminant) {
  std::variant<int, std::string> v;
  static_assert(decltype(variant.read(v, uint32, variant.discriminant<0x13>(ascii_string)))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 'x', 'x', 'x', 'x' }))
      | variant.read(v, uint32, variant.discriminant<0x13>(ascii_string)).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result);
  REQUIRE CHECK(std::holds_alternative<int>(v));
  CHECK_EQUAL(7, std::get<int>(v));
};

TEST(read_variant_value_1_with_discriminant) {
  std::variant<int, std::string> v;
  static_assert(decltype(variant.read(v, uint32, variant.discriminant<0x13>(ascii_string)))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x00, 0x13, 0x00, 0x00, 0x00, 0x04, 'a', 'b', 'c', 'd', 'x', 'x', 'x', 'x' }))
      | variant.read(v, uint32, variant.discriminant<0x13>(ascii_string)).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result);
  REQUIRE CHECK(std::holds_alternative<std::string>(v));
  CHECK_EQUAL(std::string_view("abcd"), std::get<std::string>(v));
};

}
