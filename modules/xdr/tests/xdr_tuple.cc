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

struct test_value {
  int value;
};

template<typename... X>
inline auto operator&(earnest::xdr_reader<X...>&& x, test_value& y) {
  return std::move(x) & earnest::xdr_int32(y.value);
}

template<typename... X>
inline auto operator&(earnest::xdr_writer<X...>&& x, const test_value& y) {
  return std::move(x) & earnest::xdr_int32(y.value);
}

template<typename Char, typename Traits>
inline auto operator<<(std::basic_ostream<Char, Traits>& out, const test_value& v) -> std::basic_ostream<Char, Traits>& {
  return out << v.value;
}

inline auto operator==(const test_value& x, const test_value& y) noexcept -> bool {
  return x.value == y.value;
}

inline auto operator!=(const test_value& x, const test_value& y) noexcept -> bool {
  return x.value != y.value;
}

inline auto operator<(const test_value& x, const test_value& y) noexcept -> bool {
  return x.value < y.value;
}


TEST(read_pair) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x01,
          0x00, 0x00, 0x00, 0x02,
      },
      ioctx.get_executor());

  std::pair<test_value, test_value> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & v,
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK_EQUAL(1, v.first.value);
  CHECK_EQUAL(2, v.second.value);
}

TEST(write_pair) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::pair<test_value, test_value> v{ test_value{ 1 }, test_value{ 2 } };
  async_write(
      stream,
      earnest::xdr_writer<>() & v,
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x01,
              0x00, 0x00, 0x00, 0x02,
          },
          ioctx.get_executor()),
      stream);
}

TEST(read_tuple) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x01,
          0x00, 0x00, 0x00, 0x02,
          0x00, 0x00, 0x00, 0x03,
          0x00, 0x00, 0x00, 0x04,
      },
      ioctx.get_executor());

  std::tuple<test_value, test_value, test_value, test_value> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & v,
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK_EQUAL(1, std::get<0>(v).value);
  CHECK_EQUAL(2, std::get<1>(v).value);
  CHECK_EQUAL(3, std::get<2>(v).value);
  CHECK_EQUAL(4, std::get<3>(v).value);
}

TEST(write_tuple) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::tuple<test_value, test_value, test_value, test_value> v = std::make_tuple(test_value{ 1 }, test_value{ 2 }, test_value{ 3 }, test_value{ 4 });
  async_write(
      stream,
      earnest::xdr_writer<>() & v,
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x01,
              0x00, 0x00, 0x00, 0x02,
              0x00, 0x00, 0x00, 0x03,
              0x00, 0x00, 0x00, 0x04,
          },
          ioctx.get_executor()),
      stream);
}
