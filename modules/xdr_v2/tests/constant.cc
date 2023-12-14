#include <earnest/xdr_v2.h>

#include <UnitTest++/UnitTest++.h>

#include "buffer.h"

using earnest::execution::just;
using earnest::execution::sync_wait;
using namespace earnest::xdr_v2;

SUITE(constant) {

TEST(write_uint8) {
  static_assert(decltype(constant.write(0x04u, uint8))::extent == 4);
  auto [result_8] = sync_wait(
      just(buffer())
      | constant.write(0x04u, uint8).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x04 }),
      result_8);
}

TEST(write_uint16) {
  static_assert(decltype(constant.write(0x0304u, uint16))::extent == 4);
  auto [result_16] = sync_wait(
      just(buffer())
      | constant.write(0x0304u, uint16).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x03, 0x04 }),
      result_16);
}

TEST(write_uint32) {
  static_assert(decltype(constant.write(0x01020304u, uint32))::extent == 4);
  auto [result_32] = sync_wait(
      just(buffer())
      | constant.write(0x01020304u, uint32).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x01, 0x02, 0x03, 0x04 }),
      result_32);
}

TEST(write_uint64) {
  static_assert(decltype(constant.write(0x0102030405060708ull, uint64))::extent == 8);
  auto [result_64] = sync_wait(
      just(buffer())
      | constant.write(0x0102030405060708ull, uint64).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 }),
      result_64);
}

TEST(read_uint8) {
  static_assert(decltype(constant.read(0x04u, uint8))::extent == 4);
  auto [result_8] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x00, 0x04 }))
      | constant.read(0x04u, uint8).sender_chain()).value();
  CHECK_EQUAL(buffer(), result_8);
}

TEST(read_uint16) {
  static_assert(decltype(constant.read(0x0304u, uint16))::extent == 4);
  auto [result_16] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x03, 0x04 }))
      | constant.read(0x0304u, uint16).sender_chain()).value();
  CHECK_EQUAL(buffer(), result_16);
}

TEST(read_uint32) {
  static_assert(decltype(constant.read(0x01020304u, uint32))::extent == 4);
  auto [result_32] = sync_wait(
      just(buffer({ 0x01, 0x02, 0x03, 0x04 }))
      | constant.read(0x01020304u, uint32).sender_chain()).value();
  CHECK_EQUAL(buffer(), result_32);
}

TEST(read_uint64) {
  static_assert(decltype(constant.read(0x0102030405060708ull, uint64))::extent == 8);
  auto [result_64] = sync_wait(
      just(buffer({ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 }))
      | constant.read(0x0102030405060708ull, uint64).sender_chain()).value();
  CHECK_EQUAL(buffer(), result_64);
}

TEST(read_uint8_mismatch) {
  static_assert(decltype(constant.read(0x04u, uint8))::extent == 4);
  CHECK_THROW(
      sync_wait(
          just(buffer({ 0x00, 0x00, 0x00, 0x0d }))
          | constant.read(0x04u, uint8).sender_chain()),
      xdr_error);
}

TEST(read_uint16_mismatch) {
  static_assert(decltype(constant.read(0x0304u, uint16))::extent == 4);
  CHECK_THROW(
      sync_wait(
          just(buffer({ 0x00, 0x00, 0x0c, 0x0d }))
          | constant.read(0x0304u, uint16).sender_chain()),
      xdr_error);
}

TEST(read_uint32_mismatch) {
  static_assert(decltype(constant.read(0x01020304u, uint32))::extent == 4);
  CHECK_THROW(
      sync_wait(
          just(buffer({ 0x0a, 0x0b, 0x0c, 0x0d }))
          | constant.read(0x01020304u, uint32).sender_chain()),
      xdr_error);
}

TEST(read_uint64_mismatch) {
  static_assert(decltype(constant.read(0x0102030405060708ull, uint64))::extent == 8);
  CHECK_THROW(
      sync_wait(
          just(buffer({ 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11 }))
          | constant.read(0x0102030405060708ull, uint64).sender_chain()),
      xdr_error);
}

}
