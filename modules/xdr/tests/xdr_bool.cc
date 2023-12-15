#include <earnest/xdr.h>

#include <UnitTest++/UnitTest++.h>

#include <asio/io_context.hpp>
#include <earnest/detail/byte_stream.h>

template<typename Executor, typename Allocator = std::allocator<std::byte>>
using byte_stream = earnest::detail::byte_stream<Executor, Allocator>;

TEST(read_bool) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x00, // false
          0x00, 0x00, 0x00, 0x01, // true
      },
      ioctx.get_executor());

  bool false_bool, true_bool;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_bool(false_bool) & earnest::xdr_bool(true_bool),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK_EQUAL(false, false_bool);
  CHECK_EQUAL(true, true_bool);
}

TEST(read_bad_bool) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{ 0xff, 0xff, 0xff, 0xff },
      ioctx.get_executor());

  bool b;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_bool(b),
      [&](std::error_code ec) {
        CHECK_EQUAL(make_error_code(earnest::xdr_errc::decoding_error), ec);
      });
  ioctx.run();
}

TEST(write_bool) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  bool false_bool = false, true_bool = true;
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_bool(false_bool) & earnest::xdr_bool(true_bool),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x00, // false
              0x00, 0x00, 0x00, 0x01, // true
          },
          ioctx.get_executor()),
      stream);
}
