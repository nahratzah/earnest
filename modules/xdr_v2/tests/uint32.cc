#include <earnest/xdr.h>

#include <UnitTest++/UnitTest++.h>

#include "buffer.h"

using earnest::execution::just;
using earnest::execution::sync_wait;
using namespace earnest::xdr;

TEST(write_uint32) {
  std::uint64_t value_64 = 0x01020304u;
  auto [result_64] = sync_wait(
      just(buffer())
      | uint32.write(value_64).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x01, 0x02, 0x03, 0x04 }),
      result_64);

  std::uint32_t value_32 = 0x01020304u;
  auto [result_32] = sync_wait(
      just(buffer())
      | uint32.write(value_32).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x01, 0x02, 0x03, 0x04 }),
      result_32);

  std::uint16_t value_16 = 0x0102u;
  auto [result_16] = sync_wait(
      just(buffer())
      | uint32.write(value_16).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x01, 0x02 }),
      result_16);

  std::uint8_t value_8 = 0x01u;
  auto [result_8] = sync_wait(
      just(buffer())
      | uint32.write(value_8).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x01 }),
      result_8);

  std::uint64_t too_big = 0x0102030405060708ull;
  CHECK_THROW(sync_wait(just(buffer()) | uint32.write(too_big).sender_chain()), xdr_error);
}

TEST(read_uint32) {
  std::uint64_t value_64 = 0;
  auto [result_64] = sync_wait(
      just(buffer({ 0x01, 0x02, 0x03, 0x04, 'x', 'x', 'x', 'x' }))
      | uint32.read(value_64).sender_chain()).value();
  CHECK_EQUAL(0x01020304u, value_64);
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result_64);

  std::uint32_t value_32 = 0;
  auto [result_32] = sync_wait(
      just(buffer({ 0x01, 0x02, 0x03, 0x04, 'x', 'x', 'x', 'x' }))
      | uint32.read(value_32).sender_chain()).value();
  CHECK_EQUAL(0x01020304u, value_32);
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result_32);

  std::uint16_t value_16 = 0;
  auto [result_16] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x03, 0x04, 'x', 'x', 'x', 'x' }))
      | uint32.read(value_16).sender_chain()).value();
  CHECK_EQUAL(0x0304u, value_16);
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result_16);

  std::uint8_t value_8 = 0;
  auto [result_8] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x00, 0x04, 'x', 'x', 'x', 'x' }))
      | uint32.read(value_8).sender_chain()).value();
  CHECK_EQUAL(0x04u, value_8 + 0u);
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result_8);

  std::uint16_t too_big_16;
  CHECK_THROW(sync_wait(just(buffer({ 0x01, 0x00, 0x00, 0x00 })) | uint32.read(too_big_16).sender_chain()), xdr_error);

  std::uint8_t too_big_8;
  CHECK_THROW(sync_wait(just(buffer({ 0x01, 0x00, 0x00, 0x00 })) | uint32.read(too_big_8).sender_chain()), xdr_error);
}
