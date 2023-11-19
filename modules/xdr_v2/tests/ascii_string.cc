#include <earnest/xdr.h>

#include <UnitTest++/UnitTest++.h>

#include "buffer.h"
#include <string>
#include <string_view>

using earnest::execution::just;
using earnest::execution::sync_wait;
using namespace earnest::xdr;

SUITE(ascii_string) {

TEST(read_with_padding) {
  std::string s;
  static_assert(decltype(ascii_string.read(s))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 'e', 0x00, 0x00, 0x00, 'x', 'x', 'x', 'x' }))
      | ascii_string.read(s).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result);
  CHECK_EQUAL(std::string_view("abcde"), s);
}

TEST(read_without_padding) {
  std::string s;
  static_assert(decltype(ascii_string.read(s))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer({ 0x00, 0x00, 0x00, 0x08, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'x', 'x', 'x', 'x' }))
      | ascii_string.read(s).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 'x', 'x', 'x', 'x' }),
      result);
  CHECK_EQUAL(std::string_view("abcdefgh"), s);
}

TEST(read_rejects_bad_padding) {
  std::string s;
  static_assert(decltype(ascii_string.read(s))::extent == std::dynamic_extent);
  CHECK_THROW(
      sync_wait(
          just(buffer({ 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'x', 'x', 'x', 'x' }))
          | ascii_string.read(s).sender_chain()),
      xdr_error);
}

TEST(read_rejects_non_ascii) {
  std::string s;
  static_assert(decltype(ascii_string.read(s))::extent == std::dynamic_extent);
  CHECK_THROW(
      sync_wait(
          just(buffer({ 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 0xfe, 0x00, 0x00, 0x00, 'x', 'x', 'x', 'x' }))
          | ascii_string.read(s).sender_chain()),
      xdr_error);
}

TEST(read_rejects_too_large) {
  std::string s;
  static_assert(decltype(ascii_string.read(s))::extent == std::dynamic_extent);
  CHECK_THROW(
      sync_wait(
          just(buffer({ 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 0xfe, 0x00, 0x00, 0x00, 'x', 'x', 'x', 'x' }))
          | ascii_string.read(s, 4).sender_chain()),
      xdr_error);
}

TEST(write_string_with_padding) {
  std::string s = "abcde";
  static_assert(decltype(ascii_string.write(s))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer())
      | ascii_string.write(s).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 'e', 0x00, 0x00, 0x00 }),
      result);
}

TEST(write_string_view) {
  std::string_view s = "abcde";
  static_assert(decltype(ascii_string.write(s))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer())
      | ascii_string.write(s).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x05, 'a', 'b', 'c', 'd', 'e', 0x00, 0x00, 0x00 }),
      result);
}

TEST(write_string_without_padding) {
  std::string s = "abcdefgh";
  static_assert(decltype(ascii_string.write(s))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer())
      | ascii_string.write(s).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x08, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h' }),
      result);
}

TEST(write_string_view_without_padding) {
  std::string_view s = "abcdefgh";
  static_assert(decltype(ascii_string.write(s))::extent == std::dynamic_extent);
  auto [result] = sync_wait(
      just(buffer())
      | ascii_string.write(s).sender_chain()).value();
  CHECK_EQUAL(
      buffer({ 0x00, 0x00, 0x00, 0x08, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h' }),
      result);
}

TEST(write_string_rejects_non_ascii) {
  std::string s = "abcd\xfe";
  static_assert(decltype(ascii_string.write(s))::extent == std::dynamic_extent);
  CHECK_THROW(
      sync_wait(
          just(buffer())
          | ascii_string.write(s).sender_chain()),
      xdr_error);
}

TEST(write_string_view_rejects_non_ascii) {
  std::string_view s = "abcd\xfe";
  static_assert(decltype(ascii_string.write(s))::extent == std::dynamic_extent);
  CHECK_THROW(
      sync_wait(
          just(buffer())
          | ascii_string.write(s).sender_chain()),
      xdr_error);
}

}
