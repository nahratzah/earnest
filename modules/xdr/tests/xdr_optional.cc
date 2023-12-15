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


#define OPT_TEST(TYPE, CXX_TYPE, VALUE, BYTES)                                                     \
TEST(read_nullopt_##TYPE) {                                                                        \
  asio::io_context ioctx;                                                                          \
  byte_stream<asio::io_context::executor_type> stream(                                             \
      std::initializer_list<std::uint8_t>{                                                         \
          0x00, 0x00, 0x00, 0x00, /* false */                                                      \
      },                                                                                           \
      ioctx.get_executor());                                                                       \
                                                                                                   \
  std::optional<CXX_TYPE> v;                                                                       \
  async_read(                                                                                      \
      stream,                                                                                      \
      earnest::xdr_reader<>() & earnest::xdr_opt(v).as(earnest::xdr_##TYPE),                       \
      [&](std::error_code ec) {                                                                    \
        CHECK_EQUAL(std::error_code(), ec);                                                        \
      });                                                                                          \
  ioctx.run();                                                                                     \
                                                                                                   \
  CHECK(stream.empty());                                                                           \
  CHECK(!v.has_value());                                                                           \
}                                                                                                  \
                                                                                                   \
TEST(write_nullopt_##TYPE) {                                                                       \
  asio::io_context ioctx;                                                                          \
  byte_stream<asio::io_context::executor_type> stream(                                             \
      ioctx.get_executor());                                                                       \
                                                                                                   \
  std::optional<CXX_TYPE> v = std::nullopt;                                                        \
  async_write(                                                                                     \
      stream,                                                                                      \
      earnest::xdr_writer<>() & earnest::xdr_opt(v).as(earnest::xdr_##TYPE),                       \
      [&](std::error_code ec) {                                                                    \
        CHECK_EQUAL(std::error_code(), ec);                                                        \
      });                                                                                          \
  ioctx.run();                                                                                     \
                                                                                                   \
  CHECK_EQUAL(                                                                                     \
      byte_stream<asio::io_context::executor_type>(                                                \
          std::initializer_list<std::uint8_t>{                                                     \
              0x00, 0x00, 0x00, 0x00, /* false */                                                  \
          },                                                                                       \
          ioctx.get_executor()),                                                                   \
      stream);                                                                                     \
}                                                                                                  \
                                                                                                   \
TEST(read_opt_##TYPE) {                                                                            \
  asio::io_context ioctx;                                                                          \
  byte_stream<asio::io_context::executor_type> stream(                                             \
      BYTES,                                                                                       \
      ioctx.get_executor());                                                                       \
                                                                                                   \
  std::optional<CXX_TYPE> v;                                                                       \
  async_read(                                                                                      \
      stream,                                                                                      \
      earnest::xdr_reader<>() & earnest::xdr_opt(v).as(earnest::xdr_##TYPE),                       \
      [&](std::error_code ec) {                                                                    \
        CHECK_EQUAL(std::error_code(), ec);                                                        \
      });                                                                                          \
  ioctx.run();                                                                                     \
                                                                                                   \
  CHECK(stream.empty());                                                                           \
  CHECK(v.has_value());                                                                            \
  CHECK_EQUAL(VALUE, v.value());                                                                   \
}                                                                                                  \
                                                                                                   \
TEST(write_opt_##TYPE) {                                                                           \
  asio::io_context ioctx;                                                                          \
  byte_stream<asio::io_context::executor_type> stream(                                             \
      ioctx.get_executor());                                                                       \
                                                                                                   \
  std::optional<CXX_TYPE> v = VALUE;                                                               \
  async_write(                                                                                     \
      stream,                                                                                      \
      earnest::xdr_writer<>() & earnest::xdr_opt(v).as(earnest::xdr_##TYPE),                       \
      [&](std::error_code ec) {                                                                    \
        CHECK_EQUAL(std::error_code(), ec);                                                        \
      });                                                                                          \
  ioctx.run();                                                                                     \
                                                                                                   \
  CHECK_EQUAL(                                                                                     \
      byte_stream<asio::io_context::executor_type>(                                                \
          BYTES,                                                                                   \
          ioctx.get_executor()),                                                                   \
      stream);                                                                                     \
}

OPT_TEST(uint8,   std::uint8_t,    23u, (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x17                         }))
OPT_TEST(uint16,  std::uint16_t,   23u, (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x17                         }))
OPT_TEST(uint32,  std::uint32_t,   23u, (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x17                         }))
OPT_TEST(uint64,  std::uint64_t,   23u, (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17 }))
OPT_TEST(int8,    std::int8_t,    -17,  (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0xff, 0xff, 0xff, 0xef                         }))
OPT_TEST(int16,   std::int16_t,   -17,  (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0xff, 0xff, 0xff, 0xef                         }))
OPT_TEST(int32,   std::int32_t,   -17,  (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0xff, 0xff, 0xff, 0xef                         }))
OPT_TEST(int64,   std::int64_t,   -17,  (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xef }))
OPT_TEST(float32, float,          6.0,  (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0x40, 0xc0, 0x00, 0x00                         }))
OPT_TEST(float64, double,        -2.0,  (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }))
OPT_TEST(bool,    bool,          true,  (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01                         }))

TEST(read_nullopt_testval) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x00, /* false */
      },
      ioctx.get_executor());

  std::optional<test_value> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & v,
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK(!v.has_value());
}

TEST(write_nullopt_testval) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  const std::optional<test_value> v = std::nullopt;
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
              0x00, 0x00, 0x00, 0x00, /* false */
          },
          ioctx.get_executor()),
      stream);
}

TEST(read_opt_testval) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x01, /* true */
          0x00, 0x00, 0x02, 0x00, /* 512 */
      },
      ioctx.get_executor());

  std::optional<test_value> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & v,
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK(v.has_value());
  CHECK_EQUAL(test_value{ 512 }, v.value());
}

TEST(write_opt_testval) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::optional<test_value> v = test_value{ 512 };
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
              0x00, 0x00, 0x00, 0x01, /* true */
              0x00, 0x00, 0x02, 0x00, /* 512 */
          },
          ioctx.get_executor()),
      stream);
}
