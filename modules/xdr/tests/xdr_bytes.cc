#include <earnest/xdr.h>

#include "UnitTest++/UnitTest++.h"

#include <string>
#include <asio/io_context.hpp>
#include <earnest/detail/byte_stream.h>


TEST(read_dynamic_raw_bytes_aligned) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{ '1', '2', '3', '4', '5', '6', '7', '8' },
      ioctx.get_executor());

  std::string buf(8, '-');
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_dynamic_raw_bytes(asio::buffer(buf)),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK_EQUAL(std::string("12345678"), buf);
}

TEST(read_dynamic_raw_bytes_unaligned) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{ '1', '2', '3', '4', '5', '6', '7', 0x00 },
      ioctx.get_executor());

  std::string buf(7, '-');
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_dynamic_raw_bytes(asio::buffer(buf)),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK_EQUAL(std::string("1234567"), buf);
}

TEST(read_raw_bytes_aligned) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{ '1', '2', '3', '4', '5', '6', '7', '8' },
      ioctx.get_executor());

  std::string buf(8, '-');
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_raw_bytes<8>(asio::buffer(buf)),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK_EQUAL(std::string("12345678"), buf);
}

TEST(read_raw_bytes_unaligned) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{ '1', '2', '3', '4', '5', '6', '7', 0x00 },
      ioctx.get_executor());

  std::string buf(7, '-');
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_raw_bytes<7>(asio::buffer(buf)),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK_EQUAL(std::string("1234567"), buf);
}

TEST(write_dynamic_raw_bytes_aligned) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  const std::string buf = "12345678";
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_dynamic_raw_bytes(asio::buffer(buf)),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{ '1', '2', '3', '4', '5', '6', '7', '8' },
          ioctx.get_executor()),
      stream);
}

TEST(write_dynamic_raw_bytes_unaligned) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  const std::string buf = "1234567";
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_dynamic_raw_bytes(asio::buffer(buf)),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{ '1', '2', '3', '4', '5', '6', '7', 0x00 },
          ioctx.get_executor()),
      stream);
}

TEST(write_raw_bytes_aligned) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  const std::string buf = "12345678";
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_raw_bytes<8>(asio::buffer(buf)),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{ '1', '2', '3', '4', '5', '6', '7', '8' },
          ioctx.get_executor()),
      stream);
}

TEST(write_raw_bytes_unaligned) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  const std::string buf = "1234567";
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_raw_bytes<7>(asio::buffer(buf)),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{ '1', '2', '3', '4', '5', '6', '7', 0x00 },
          ioctx.get_executor()),
      stream);
}

TEST(read_bytes_aligned) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x08, // number of bytes
          '1', '2', '3', '4', '5', '6', '7', '8' // text (no padding)
      },
      ioctx.get_executor());

  std::vector<char> buf;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_bytes(buf),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK_EQUAL(std::string("12345678"), std::string(buf.begin(), buf.end()));
}

TEST(read_bytes_unaligned) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x07, // number of bytes
          '1', '2', '3', '4', '5', '6', '7', 0x00 // text + padding
      },
      ioctx.get_executor());

  std::vector<char> buf;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_bytes(buf),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK_EQUAL(std::string("1234567"), std::string(buf.begin(), buf.end()));
}

TEST(write_bytes_aligned) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::vector<char> buf = { '1', '2', '3', '4', '5', '6', '7', '8' };
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_bytes(buf),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x08, // number of bytes
              '1', '2', '3', '4', '5', '6', '7', '8' // text (no padding)
          },
          ioctx.get_executor()),
      stream);
}

TEST(write_bytes_unaligned) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  const std::vector<char> buf = { '1', '2', '3', '4', '5', '6', '7' };
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_bytes(buf),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x07, // number of bytes
              '1', '2', '3', '4', '5', '6', '7', 0x00 // text + padding
          },
          ioctx.get_executor()),
      stream);
}

TEST(read_large_bytes_aligned) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x03, // number of bytes
          0, '1', 0, '2', 0, '3', 0, 0 // text (no padding)
      },
      ioctx.get_executor());

  std::vector<std::uint16_t> buf;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_bytes(buf),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  // We have to convert the read bytes, which are in big-endian.
  // Otherwise the string will be a sequence of '\0'.
  std::string buf_as_str;
  std::transform(buf.begin(), buf.end(), std::back_inserter(buf_as_str),
      [](std::uint16_t x) { return boost::endian::big_to_native(x); });

  CHECK(stream.empty());
  CHECK_EQUAL(std::string("123"), buf_as_str);
}

TEST(write_large_bytes_aligned) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::vector<std::uint16_t> buf = {
    boost::endian::native_to_big(std::uint16_t('1')),
    boost::endian::native_to_big(std::uint16_t('2')),
    boost::endian::native_to_big(std::uint16_t('3'))
  };
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_bytes(buf),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x03, // number of bytes
              0x00, '1', 0x00, '2', 0x00, '3', 0x00, 0x00 // text (no padding)
          },
          ioctx.get_executor()),
      stream);
}

TEST(read_string_bytes) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x08, // number of bytes
          '1', '2', '3', '4', '5', '6', '7', '8' // text (no padding)
      },
      ioctx.get_executor());

  std::string buf;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_bytes(buf),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK_EQUAL(std::string("12345678"), buf);
}

TEST(write_string_bytes) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::string buf = "12345678";
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_bytes(buf),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x08, // number of bytes
              '1', '2', '3', '4', '5', '6', '7', '8' // text (no padding)
          },
          ioctx.get_executor()),
      stream);
}

int main() {
  return UnitTest::RunAllTests();
}
