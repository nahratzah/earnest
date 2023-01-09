#include <earnest/xdr.h>

#include "UnitTest++/UnitTest++.h"

#include <asio/io_context.hpp>
#include <earnest/detail/byte_stream.h>


#define PADDING_TESTS(NUM_BYTES, GOOD_BYTES, BAD_BYTES)                                            \
TEST(read_fixed_padding_##NUM_BYTES) {                                                             \
  asio::io_context ioctx;                                                                          \
  byte_stream<asio::io_context::executor_type> stream(                                             \
      GOOD_BYTES,                                                                                  \
      ioctx.get_executor());                                                                       \
                                                                                                   \
  async_read(                                                                                      \
      stream,                                                                                      \
      earnest::xdr_reader<>() & earnest::xdr_padding<NUM_BYTES>,                                   \
      [&](std::error_code ec) {                                                                    \
        CHECK_EQUAL(std::error_code(), ec);                                                        \
      });                                                                                          \
  ioctx.run();                                                                                     \
                                                                                                   \
  CHECK(stream.empty());                                                                           \
}                                                                                                  \
                                                                                                   \
TEST(read_fixed_padding_does_validation_##NUM_BYTES) {                                             \
  asio::io_context ioctx;                                                                          \
  byte_stream<asio::io_context::executor_type> stream(                                             \
      BAD_BYTES,                                                                                   \
      ioctx.get_executor());                                                                       \
                                                                                                   \
  async_read(                                                                                      \
      stream,                                                                                      \
      earnest::xdr_reader<>() & earnest::xdr_padding<NUM_BYTES>,                                   \
      [&](std::error_code ec) {                                                                    \
        const std::error_code expected = (NUM_BYTES == 0                                           \
                                         ? std::error_code()                                       \
                                         : make_error_code(earnest::xdr_errc::decoding_error));    \
        CHECK_EQUAL(expected, ec);                                                                 \
      });                                                                                          \
  ioctx.run();                                                                                     \
                                                                                                   \
  CHECK(stream.empty());                                                                           \
}                                                                                                  \
                                                                                                   \
TEST(write_fixed_padding_##NUM_BYTES) {                                                            \
  asio::io_context ioctx;                                                                          \
  byte_stream<asio::io_context::executor_type> stream(                                             \
      ioctx.get_executor());                                                                       \
                                                                                                   \
  async_write(                                                                                     \
      stream,                                                                                      \
      earnest::xdr_writer<>() & earnest::xdr_padding<NUM_BYTES>,                                   \
      [&](std::error_code ec) {                                                                    \
        CHECK_EQUAL(std::error_code(), ec);                                                        \
      });                                                                                          \
  ioctx.run();                                                                                     \
                                                                                                   \
  CHECK_EQUAL(                                                                                     \
      byte_stream<asio::io_context::executor_type>(GOOD_BYTES, ioctx.get_executor()),              \
      stream);                                                                                     \
}                                                                                                  \
                                                                                                   \
TEST(read_dynamic_padding_##NUM_BYTES) {                                                           \
  asio::io_context ioctx;                                                                          \
  byte_stream<asio::io_context::executor_type> stream(                                             \
      GOOD_BYTES,                                                                                  \
      ioctx.get_executor());                                                                       \
                                                                                                   \
  async_read(                                                                                      \
      stream,                                                                                      \
      earnest::xdr_reader<>() & earnest::xdr_dynamic_padding(NUM_BYTES),                           \
      [&](std::error_code ec) {                                                                    \
        CHECK_EQUAL(std::error_code(), ec);                                                        \
      });                                                                                          \
  ioctx.run();                                                                                     \
                                                                                                   \
  CHECK(stream.empty());                                                                           \
}                                                                                                  \
                                                                                                   \
TEST(read_dynamic_padding_does_validation_##NUM_BYTES) {                                           \
  asio::io_context ioctx;                                                                          \
  byte_stream<asio::io_context::executor_type> stream(                                             \
      BAD_BYTES,                                                                                   \
      ioctx.get_executor());                                                                       \
                                                                                                   \
  async_read(                                                                                      \
      stream,                                                                                      \
      earnest::xdr_reader<>() & earnest::xdr_dynamic_padding(NUM_BYTES),                           \
      [&](std::error_code ec) {                                                                    \
        const std::error_code expected = (NUM_BYTES == 0                                           \
                                         ? std::error_code()                                       \
                                         : make_error_code(earnest::xdr_errc::decoding_error));    \
        CHECK_EQUAL(expected, ec);                                                                 \
      });                                                                                          \
  ioctx.run();                                                                                     \
                                                                                                   \
  CHECK(stream.empty());                                                                           \
}                                                                                                  \
                                                                                                   \
TEST(write_dynamic_padding_##NUM_BYTES) {                                                          \
  asio::io_context ioctx;                                                                          \
  byte_stream<asio::io_context::executor_type> stream(                                             \
      ioctx.get_executor());                                                                       \
                                                                                                   \
  async_write(                                                                                     \
      stream,                                                                                      \
      earnest::xdr_writer<>() & earnest::xdr_dynamic_padding(NUM_BYTES),                           \
      [&](std::error_code ec) {                                                                    \
        CHECK_EQUAL(std::error_code(), ec);                                                        \
      });                                                                                          \
  ioctx.run();                                                                                     \
                                                                                                   \
  CHECK_EQUAL(                                                                                     \
      byte_stream<asio::io_context::executor_type>(GOOD_BYTES, ioctx.get_executor()),              \
      stream);                                                                                     \
}

PADDING_TESTS(0, (std::initializer_list<std::uint8_t>{}),          (std::initializer_list<std::uint8_t>{}))
PADDING_TESTS(1, (std::initializer_list<std::uint8_t>{ 0 }),       (std::initializer_list<std::uint8_t>{ 1 }))
PADDING_TESTS(2, (std::initializer_list<std::uint8_t>{ 0, 0 }),    (std::initializer_list<std::uint8_t>{ 1, 1 }))
PADDING_TESTS(3, (std::initializer_list<std::uint8_t>{ 0, 0, 0 }), (std::initializer_list<std::uint8_t>{ 1, 1, 1 }))
