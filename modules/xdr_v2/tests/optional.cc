#include <earnest/xdr.h>

#include <UnitTest++/UnitTest++.h>

#include "buffer.h"

using earnest::execution::just;
using earnest::execution::sync_wait;
using namespace earnest::xdr;

SUITE(optional) {

TEST(write_optional_with_value) {
  std::optional<int> o = 4;
  static_assert(decltype(optional.write(o, uint32()))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer())
      | optional.write(o, uint32()).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04 }),
      result);
}

TEST(write_optional_without_value) {
  std::optional<int> o = std::nullopt;
  static_assert(decltype(optional.write(o, uint32()))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer())
      | optional.write(o, uint32()).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x00 }),
      result);
}

TEST(read_optional_with_value) {
  std::optional<int> o = std::nullopt; // We want to confirm the optional gets initialized.
  static_assert(decltype(optional.read(o, uint32()))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 'x', 'x', 'x', 'x' }))
      | optional.read(o, uint32()).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result);
  REQUIRE CHECK(o.has_value());
  CHECK_EQUAL(4, o.value());
}

TEST(read_optional_without_value) {
  std::optional<int> o = 4; // We want to confirm the optional gets cleared.
  static_assert(decltype(optional.read(o, uint32()))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x00, 0x00, 'x', 'x', 'x', 'x' }))
      | optional.read(o, uint32()).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result);
  CHECK(!o.has_value());
}

TEST(write_optional_of_skipped_data) {
  std::optional<std::monostate> o = std::monostate{};
  static_assert(decltype(optional.write(o, skip()))::extent == 4);
  auto [result] = sync_wait(
      just(buffer())
      | optional.write(o, skip()).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x01 }),
      result);
}

TEST(read_optional_of_skipped_data) {
  std::optional<std::monostate> o = std::nullopt;
  static_assert(decltype(optional.read(o, skip()))::extent == 4);
  auto [result] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x00, 0x01 }))
      | optional.read(o, skip()).sender_chain()).value();
  CHECK_EQUAL(
      buffer(),
      result);
  CHECK(o.has_value());
}

}
