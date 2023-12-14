#include <earnest/xdr_v2.h>

#include <UnitTest++/UnitTest++.h>

#include "buffer.h"

using earnest::execution::just;
using earnest::execution::sync_wait;
using namespace earnest::xdr_v2;

SUITE(uint32) {

TEST(write_uint64) {
  std::uint64_t value_64 = 0x01020304u;
  static_assert(decltype(uint32.write(value_64))::extent == 4);
  auto [result_64] = sync_wait(
      just(buffer())
      | uint32.write(value_64).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x01, 0x02, 0x03, 0x04 }),
      result_64);
}

TEST(write_uint64_using_buffer_ref) {
  std::uint64_t value_64 = 0x01020304u;
  buffer buf;
  static_assert(decltype(uint32.write(value_64))::extent == 4);
  auto [result_64] = sync_wait(
      just(std::ref(buf))
      | uint32.write(value_64).sender_chain()).value();
  CHECK_EQUAL(&buf, &result_64.get());
  CHECK_EQUAL(
      buffer({ 0x01, 0x02, 0x03, 0x04 }),
      buf);
}

TEST(write_uint32) {
  std::uint32_t value_32 = 0x01020304u;
  static_assert(decltype(uint32.write(value_32))::extent == 4);
  auto [result_32] = sync_wait(
      just(buffer())
      | uint32.write(value_32).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x01, 0x02, 0x03, 0x04 }),
      result_32);
}

TEST(write_uint16) {
  std::uint16_t value_16 = 0x0102u;
  static_assert(decltype(uint32.write(value_16))::extent == 4);
  auto [result_16] = sync_wait(
      just(buffer())
      | uint32.write(value_16).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x01, 0x02 }),
      result_16);
}

TEST(write_uint8) {
  std::uint8_t value_8 = 0x01u;
  static_assert(decltype(uint32.write(value_8))::extent == 4);
  auto [result_8] = sync_wait(
      just(buffer())
      | uint32.write(value_8).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x01 }),
      result_8);
}

TEST(write_uint64_throws_when_too_large) {
  std::uint64_t too_big = 0x0102030405060708ull;
  CHECK_THROW(sync_wait(just(buffer()) | uint32.write(too_big).sender_chain()), xdr_error);
}

TEST(write_int64) {
  std::int64_t value_64 = 0x01020304;
  static_assert(decltype(uint32.write(value_64))::extent == 4);
  auto [result_64] = sync_wait(
      just(buffer())
      | uint32.write(value_64).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x01, 0x02, 0x03, 0x04 }),
      result_64);
}

TEST(write_int32) {
  std::int32_t value_32 = 0x01020304;
  static_assert(decltype(uint32.write(value_32))::extent == 4);
  auto [result_32] = sync_wait(
      just(buffer())
      | uint32.write(value_32).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x01, 0x02, 0x03, 0x04 }),
      result_32);
}

TEST(write_int16) {
  std::int16_t value_16 = 0x0102;
  static_assert(decltype(uint32.write(value_16))::extent == 4);
  auto [result_16] = sync_wait(
      just(buffer())
      | uint32.write(value_16).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x01, 0x02 }),
      result_16);
}

TEST(write_int8) {
  std::int8_t value_8 = 0x01;
  static_assert(decltype(uint32.write(value_8))::extent == 4);
  auto [result_8] = sync_wait(
      just(buffer())
      | uint32.write(value_8).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x01 }),
      result_8);
}

TEST(write_int64_throws_when_too_large) {
  std::int64_t too_big = 0x0102030405060708ull;
  CHECK_THROW(sync_wait(just(buffer()) | uint32.write(too_big).sender_chain()), xdr_error);
}

TEST(write_int64_throws_when_negative) {
  std::int64_t value_64 = -0x0102030405060708ll;
  CHECK_THROW(sync_wait(just(buffer()) | uint32.write(value_64).sender_chain()), xdr_error);
}

TEST(write_int32_throws_when_negative) {
  std::int32_t value_32 = -0x01020304ll;
  CHECK_THROW(sync_wait(just(buffer()) | uint32.write(value_32).sender_chain()), xdr_error);
}

TEST(write_int16_throws_when_negative) {
  std::int16_t value_16 = -0x0102ll;
  CHECK_THROW(sync_wait(just(buffer()) | uint32.write(value_16).sender_chain()), xdr_error);
}

TEST(write_int8_throws_when_negative) {
  std::int8_t value_8 = -0x01ll;
  CHECK_THROW(sync_wait(just(buffer()) | uint32.write(value_8).sender_chain()), xdr_error);
}

TEST(read_uint64) {
  std::uint64_t value_64 = 0;
  static_assert(decltype(uint32.read(value_64))::extent == 4);
  auto [result_64] = sync_wait(
      just(buffer({ 0x01, 0x02, 0x03, 0x04, 'x', 'x', 'x', 'x' }))
      | uint32.read(value_64).sender_chain()).value();
  CHECK_EQUAL(0x01020304u, value_64);
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result_64);
}

TEST(read_uint32) {
  std::uint32_t value_32 = 0;
  static_assert(decltype(uint32.read(value_32))::extent == 4);
  auto [result_32] = sync_wait(
      just(buffer({ 0x01, 0x02, 0x03, 0x04, 'x', 'x', 'x', 'x' }))
      | uint32.read(value_32).sender_chain()).value();
  CHECK_EQUAL(0x01020304u, value_32);
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result_32);
}

TEST(read_uint16) {
  std::uint16_t value_16 = 0;
  static_assert(decltype(uint32.read(value_16))::extent == 4);
  auto [result_16] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x03, 0x04, 'x', 'x', 'x', 'x' }))
      | uint32.read(value_16).sender_chain()).value();
  CHECK_EQUAL(0x0304u, value_16);
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result_16);
}

TEST(read_uint8) {
  std::uint8_t value_8 = 0;
  static_assert(decltype(uint32.read(value_8))::extent == 4);
  auto [result_8] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x00, 0x04, 'x', 'x', 'x', 'x' }))
      | uint32.read(value_8).sender_chain()).value();
  CHECK_EQUAL(0x04u, value_8 + 0u);
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result_8);
}

TEST(read_int64) {
  std::int64_t value_64 = 0;
  static_assert(decltype(uint32.read(value_64))::extent == 4);
  auto [result_64] = sync_wait(
      just(buffer({ 0x01, 0x02, 0x03, 0x04, 'x', 'x', 'x', 'x' }))
      | uint32.read(value_64).sender_chain()).value();
  CHECK_EQUAL(0x01020304u, value_64);
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result_64);
}

TEST(read_int32) {
  std::int32_t value_32 = 0;
  static_assert(decltype(uint32.read(value_32))::extent == 4);
  auto [result_32] = sync_wait(
      just(buffer({ 0x01, 0x02, 0x03, 0x04, 'x', 'x', 'x', 'x' }))
      | uint32.read(value_32).sender_chain()).value();
  CHECK_EQUAL(0x01020304u, value_32);
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result_32);
}

TEST(read_int16) {
  std::int16_t value_16 = 0;
  static_assert(decltype(uint32.read(value_16))::extent == 4);
  auto [result_16] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x03, 0x04, 'x', 'x', 'x', 'x' }))
      | uint32.read(value_16).sender_chain()).value();
  CHECK_EQUAL(0x0304u, value_16);
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result_16);
}

TEST(read_int8) {
  std::int8_t value_8 = 0;
  static_assert(decltype(uint32.read(value_8))::extent == 4);
  auto [result_8] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x00, 0x04, 'x', 'x', 'x', 'x' }))
      | uint32.read(value_8).sender_chain()).value();
  CHECK_EQUAL(0x04u, value_8 + 0u);
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result_8);
}

TEST(read_uint16_throws_if_too_large) {
  std::uint16_t too_big_16;
  CHECK_THROW(sync_wait(just(buffer({ 0x01, 0x00, 0x00, 0x00 })) | uint32.read(too_big_16).sender_chain()), xdr_error);
}

TEST(read_uint8_throws_if_too_large) {
  std::uint8_t too_big_8;
  CHECK_THROW(sync_wait(just(buffer({ 0x01, 0x00, 0x00, 0x00 })) | uint32.read(too_big_8).sender_chain()), xdr_error);
}

TEST(read_int32_throws_if_too_large) {
  std::int32_t too_big_32;
  CHECK_THROW(sync_wait(just(buffer({ 0x81, 0x00, 0x00, 0x00 })) | uint32.read(too_big_32).sender_chain()), xdr_error);
}

TEST(read_int16_throws_if_too_large) {
  std::int16_t too_big_16;
  CHECK_THROW(sync_wait(just(buffer({ 0x00, 0x00, 0x81, 0x00 })) | uint32.read(too_big_16).sender_chain()), xdr_error);
}

TEST(read_int8_throws_if_too_large) {
  std::int8_t too_big_8;
  CHECK_THROW(sync_wait(just(buffer({ 0x00, 0x00, 0x00, 0x81 })) | uint32.read(too_big_8).sender_chain()), xdr_error);
}

}
