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

template<typename Executor, typename Allocator = std::allocator<std::byte>>
using byte_stream = earnest::detail::byte_stream<Executor, Allocator>;

TEST(read_variant) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
        0x00, 0x00, 0x00, 0x00,                          // discriminant for v1: 0
        0x00, 0x00, 0x00, 0x07,                          // value for v1
        0x00, 0x00, 0x00, 0x01,                          // discriminant for v2: 1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,  // value for v2
      },
      ioctx.get_executor());

  auto v1 = std::variant<int, int>(std::in_place_index<1>, 77);
  auto v2 = std::variant<int, int>(std::in_place_index<0>, 99);
  async_read(
      stream,
      earnest::xdr_reader<>()
      & earnest::xdr_variant(v1).of(earnest::xdr_int32, earnest::xdr_int64)
      & earnest::xdr_variant(v2).of(earnest::xdr_int32, earnest::xdr_int64),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK_EQUAL(0u, v1.index());
  if (0 == v1.index()) CHECK_EQUAL(7, std::get<0>(v1));
  CHECK_EQUAL(1u, v2.index());
  if (1 == v2.index()) CHECK_EQUAL(9, std::get<1>(v2));
}

TEST(write_variant) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  auto v1 = std::variant<int, int>(std::in_place_index<0>, 7);
  auto v2 = std::variant<int, int>(std::in_place_index<1>, 9);

  async_write(
      stream,
      earnest::xdr_writer<>()
      & earnest::xdr_variant(v1).of(earnest::xdr_int32, earnest::xdr_int64)
      & earnest::xdr_variant(v2).of(earnest::xdr_int32, earnest::xdr_int64),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
            0x00, 0x00, 0x00, 0x00,                          // discriminant for v1: 0
            0x00, 0x00, 0x00, 0x07,                          // value for v1
            0x00, 0x00, 0x00, 0x01,                          // discriminant for v2: 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,  // value for v2
          },
          ioctx.get_executor()),
      stream);
}
