#include <earnest/xdr.h>

#include "UnitTest++/UnitTest++.h"

#include <list>
#include <map>
#include <optional>
#include <ostream>
#include <set>
#include <tuple>
#include <utility>
#include <vector>
#include <asio/io_context.hpp>
#include <earnest/detail/byte_stream.h>


TEST(read_processor) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{},
      ioctx.get_executor());

  bool was_called = false;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_processor(
          [&was_called]() {
            CHECK(!was_called);
            was_called = true;
            return std::error_code();
          }),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(was_called);
  CHECK(stream.empty());
}

TEST(write_processor) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  bool was_called = false;
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_processor(
          [&was_called]() {
            CHECK(!was_called);
            was_called = true;
            return std::error_code();
          }),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(was_called);
  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{},
          ioctx.get_executor()),
      stream);
}

TEST(read_processor_with_error) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{},
      ioctx.get_executor());

  bool was_called = false;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_processor(
          [&was_called]() {
            CHECK(!was_called);
            was_called = true;
            return std::make_error_code(std::errc::invalid_argument);
          }),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::make_error_code(std::errc::invalid_argument), ec);
      });
  ioctx.run();

  CHECK(was_called);
}

TEST(write_processor_with_error) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  bool was_called = false;
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_processor(
          [&was_called]() {
            CHECK(!was_called);
            was_called = true;
            return std::make_error_code(std::errc::invalid_argument);
          }),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::make_error_code(std::errc::invalid_argument), ec);
      });
  ioctx.run();

  CHECK(was_called);
}

int main() {
  return UnitTest::RunAllTests();
}
