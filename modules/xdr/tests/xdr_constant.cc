#include <earnest/xdr.h>

#include <UnitTest++/UnitTest++.h>

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


TEST(read_constant) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x01, 0x02, 0x03, 0x04,
      },
      ioctx.get_executor());

  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_constant(0x01020304).as(earnest::xdr_uint32),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();
}

TEST(read_bad_constant) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x01, 0x02, 0x03,
      },
      ioctx.get_executor());

  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_constant(0x01020304).as(earnest::xdr_uint32),
      [&](std::error_code ec) {
        CHECK_EQUAL(make_error_code(earnest::xdr_errc::decoding_error), ec);
      });
  ioctx.run();
}

TEST(write_constant) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_constant(0x01020304).as(earnest::xdr_uint32),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x01, 0x02, 0x03, 0x04,
          },
          ioctx.get_executor()),
      stream);
}
