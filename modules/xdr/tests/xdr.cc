#include <earnest/xdr.h>
#include "byte_stream.h"

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


#define READ_TEST_INT(TYPE, BITS)                                                                  \
TEST(read_##TYPE##BITS) {                                                                          \
  asio::io_context ioctx;                                                                          \
  byte_stream<asio::io_context::executor_type> stream(                                             \
      BITS <= 32                                                                                   \
      ? std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01 }                              \
      : std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 },     \
      ioctx.get_executor());                                                                       \
                                                                                                   \
  std::TYPE##BITS##_t v;                                                                           \
  async_read(                                                                                      \
      stream,                                                                                      \
      earnest::xdr_reader<>() & earnest::xdr_##TYPE##BITS(v),                                      \
      [&](std::error_code ec) {                                                                    \
        CHECK_EQUAL(std::error_code(), ec);                                                        \
      });                                                                                          \
  ioctx.run();                                                                                     \
                                                                                                   \
  CHECK_EQUAL(1u, v);                                                                              \
  CHECK(stream.empty());                                                                           \
}                                                                                                  \
                                                                                                   \
TEST(read_##TYPE##BITS##_using_setter) {                                                           \
  asio::io_context ioctx;                                                                          \
  byte_stream<asio::io_context::executor_type> stream(                                             \
      BITS <= 32                                                                                   \
      ? std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x17 }                              \
      : std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17 },     \
      ioctx.get_executor());                                                                       \
                                                                                                   \
  int v = 0;                                                                                       \
  async_read(                                                                                      \
      stream,                                                                                      \
      earnest::xdr_reader<>() &                                                                    \
          earnest::xdr_getset<int>([&v](auto x) { v = x; }).as(earnest::xdr_##TYPE##BITS),         \
      [&](std::error_code ec) {                                                                    \
        CHECK_EQUAL(std::error_code(), ec);                                                        \
      });                                                                                          \
  ioctx.run();                                                                                     \
                                                                                                   \
  CHECK_EQUAL(0x17, v);                                                                            \
  CHECK(stream.empty());                                                                           \
}

#define WRITE_TEST_INT(TYPE, BITS)                                                                 \
TEST(write_##TYPE##BITS) {                                                                         \
  asio::io_context ioctx;                                                                          \
  byte_stream<asio::io_context::executor_type> stream(ioctx.get_executor());                       \
                                                                                                   \
  const std::TYPE##BITS##_t v = 1;                                                                 \
  async_write(                                                                                     \
      stream,                                                                                      \
      earnest::xdr_writer<>() & earnest::xdr_##TYPE##BITS(v),                                      \
      [&](std::error_code ec) {                                                                    \
        CHECK_EQUAL(std::error_code(), ec);                                                        \
      });                                                                                          \
  ioctx.run();                                                                                     \
                                                                                                   \
  CHECK_EQUAL(                                                                                     \
      byte_stream<asio::io_context::executor_type>(                                                \
          BITS <= 32                                                                               \
          ? std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01 }                          \
          : std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 }, \
          ioctx.get_executor()),                                                                   \
      stream);                                                                                     \
}                                                                                                  \
                                                                                                   \
TEST(write_##TYPE##BITS##_using_getter) {                                                          \
  asio::io_context ioctx;                                                                          \
  byte_stream<asio::io_context::executor_type> stream(ioctx.get_executor());                       \
                                                                                                   \
  const int v = 0x17;                                                                              \
  async_write(                                                                                     \
      stream,                                                                                      \
      earnest::xdr_writer<>() &                                                                    \
          earnest::xdr_getset<int>([&v]() { return v; }).as(earnest::xdr_##TYPE##BITS),            \
      [&](std::error_code ec) {                                                                    \
        CHECK_EQUAL(std::error_code(), ec);                                                        \
      });                                                                                          \
  ioctx.run();                                                                                     \
                                                                                                   \
  CHECK_EQUAL(                                                                                     \
      byte_stream<asio::io_context::executor_type>(                                                \
          BITS <= 32                                                                               \
          ? std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x17 }                          \
          : std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17 }, \
          ioctx.get_executor()),                                                                   \
      stream);                                                                                     \
}

READ_TEST_INT(uint, 8)
READ_TEST_INT(uint, 16)
READ_TEST_INT(uint, 32)
READ_TEST_INT(uint, 64)

READ_TEST_INT(int, 8)
READ_TEST_INT(int, 16)
READ_TEST_INT(int, 32)
READ_TEST_INT(int, 64)

TEST(read_float32) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x00, //  0.0
          0x80, 0x00, 0x00, 0x00, // -0.0
          0x40, 0xc0, 0x00, 0x00, //  6.0
          0xC0, 0x00, 0x00, 0x00, // -2.0
      },
      ioctx.get_executor());
  float zero, negative_zero, six, negative_two;

  async_read(
      stream,
      earnest::xdr_reader<>()
          & earnest::xdr_float32(zero)
          & earnest::xdr_float32(negative_zero)
          & earnest::xdr_float32(six)
          & earnest::xdr_float32(negative_two),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL( float(0.0), zero);
  CHECK_EQUAL(-float(0.0), negative_zero);
  CHECK_EQUAL( float(6.0), six);
  CHECK_EQUAL(-float(2.0), negative_two);
  CHECK(stream.empty());
}

TEST(read_float32_using_setter) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x00, //  0.0
          0x80, 0x00, 0x00, 0x00, // -0.0
          0x40, 0xc0, 0x00, 0x00, //  6.0
          0xC0, 0x00, 0x00, 0x00, // -2.0
      },
      ioctx.get_executor());
  float zero, negative_zero, six, negative_two;

  async_read(
      stream,
      earnest::xdr_reader<>()
          & earnest::xdr_getset<float>([&zero](auto x) { zero = x; }).as(earnest::xdr_float32)
          & earnest::xdr_getset<float>([&negative_zero](auto x) { negative_zero = x; }).as(earnest::xdr_float32)
          & earnest::xdr_getset<float>([&six](auto x) { six = x; }).as(earnest::xdr_float32)
          & earnest::xdr_getset<float>([&negative_two](auto x) { negative_two = x; }).as(earnest::xdr_float32),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL( float(0.0), zero);
  CHECK_EQUAL(-float(0.0), negative_zero);
  CHECK_EQUAL( float(6.0), six);
  CHECK_EQUAL(-float(2.0), negative_two);
  CHECK(stream.empty());
}

TEST(read_float64) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  0.0
          0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // -0.0
          0x40, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  6.0
          0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // -2.0
      },
      ioctx.get_executor());
  double zero, negative_zero, six, negative_two;

  async_read(
      stream,
      earnest::xdr_reader<>()
          & earnest::xdr_float64(zero)
          & earnest::xdr_float64(negative_zero)
          & earnest::xdr_float64(six)
          & earnest::xdr_float64(negative_two),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL( double(0.0), zero);
  CHECK_EQUAL(-double(0.0), negative_zero);
  CHECK_EQUAL( double(6.0), six);
  CHECK_EQUAL(-double(2.0), negative_two);
  CHECK(stream.empty());
}

TEST(read_float64_using_setter) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  0.0
          0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // -0.0
          0x40, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  6.0
          0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // -2.0
      },
      ioctx.get_executor());
  double zero, negative_zero, six, negative_two;

  async_read(
      stream,
      earnest::xdr_reader<>()
          & earnest::xdr_getset<double>([&zero](auto x) { zero = x; }).as(earnest::xdr_float64)
          & earnest::xdr_getset<double>([&negative_zero](auto x) { negative_zero = x; }).as(earnest::xdr_float64)
          & earnest::xdr_getset<double>([&six](auto x) { six = x; }).as(earnest::xdr_float64)
          & earnest::xdr_getset<double>([&negative_two](auto x) { negative_two = x; }).as(earnest::xdr_float64),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL( double(0.0), zero);
  CHECK_EQUAL(-double(0.0), negative_zero);
  CHECK_EQUAL( double(6.0), six);
  CHECK_EQUAL(-double(2.0), negative_two);
  CHECK(stream.empty());
}

WRITE_TEST_INT(uint, 8)
WRITE_TEST_INT(uint, 16)
WRITE_TEST_INT(uint, 32)
WRITE_TEST_INT(uint, 64)

WRITE_TEST_INT(int, 8)
WRITE_TEST_INT(int, 16)
WRITE_TEST_INT(int, 32)
WRITE_TEST_INT(int, 64)

TEST(write_float32) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());
  const float zero = 0.0, negative_zero = -0.0, six = 6, negative_two = -2.0;

  async_write(
      stream,
      earnest::xdr_writer<>()
          & earnest::xdr_float32(zero)
          & earnest::xdr_float32(negative_zero)
          & earnest::xdr_float32(six)
          & earnest::xdr_float32(negative_two),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x00, //  0.0
              0x80, 0x00, 0x00, 0x00, // -0.0
              0x40, 0xc0, 0x00, 0x00, //  6.0
              0xC0, 0x00, 0x00, 0x00, // -2.0
          },
          ioctx.get_executor()),
      stream);
}

TEST(write_float32_using_getter) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());
  const float zero = 0.0, negative_zero = -0.0, six = 6, negative_two = -2.0;

  async_write(
      stream,
      earnest::xdr_writer<>()
          & earnest::xdr_getset<float>([zero]() { return zero; }).as(earnest::xdr_float32)
          & earnest::xdr_getset<float>([negative_zero]() { return negative_zero; }).as(earnest::xdr_float32)
          & earnest::xdr_getset<float>([six]() { return six; }).as(earnest::xdr_float32)
          & earnest::xdr_getset<float>([negative_two]() { return negative_two; }).as(earnest::xdr_float32),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x00, //  0.0
              0x80, 0x00, 0x00, 0x00, // -0.0
              0x40, 0xc0, 0x00, 0x00, //  6.0
              0xC0, 0x00, 0x00, 0x00, // -2.0
          },
          ioctx.get_executor()),
      stream);
}

TEST(write_float64) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());
  const double zero = 0.0, negative_zero = -0.0, six = 6, negative_two = -2.0;

  async_write(
      stream,
      earnest::xdr_writer<>()
          & earnest::xdr_float64(zero)
          & earnest::xdr_float64(negative_zero)
          & earnest::xdr_float64(six)
          & earnest::xdr_float64(negative_two),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  0.0
              0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // -0.0
              0x40, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  6.0
              0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // -2.0
          },
          ioctx.get_executor()),
      stream);
}

TEST(write_float64_using_getter) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());
  const double zero = 0.0, negative_zero = -0.0, six = 6, negative_two = -2.0;

  async_write(
      stream,
      earnest::xdr_writer<>()
          & earnest::xdr_getset<double>([zero]() { return zero; }).as(earnest::xdr_float64)
          & earnest::xdr_getset<double>([negative_zero]() { return negative_zero; }).as(earnest::xdr_float64)
          & earnest::xdr_getset<double>([six]() { return six; }).as(earnest::xdr_float64)
          & earnest::xdr_getset<double>([negative_two]() { return negative_two; }).as(earnest::xdr_float64),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  0.0
              0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // -0.0
              0x40, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  6.0
              0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // -2.0
          },
          ioctx.get_executor()),
      stream);
}

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

OPT_TEST(uint8,   std::uint8_t,    23, (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x17                         }))
OPT_TEST(uint16,  std::uint16_t,   23, (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x17                         }))
OPT_TEST(uint32,  std::uint32_t,   23, (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x17                         }))
OPT_TEST(uint64,  std::uint64_t,   23, (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17 }))
OPT_TEST(int8,    std::int8_t,    -17, (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0xff, 0xff, 0xff, 0xef                         }))
OPT_TEST(int16,   std::int16_t,   -17, (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0xff, 0xff, 0xff, 0xef                         }))
OPT_TEST(int32,   std::int32_t,   -17, (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0xff, 0xff, 0xff, 0xef                         }))
OPT_TEST(int64,   std::int64_t,   -17, (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xef }))
OPT_TEST(float32, float,          6.0, (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0x40, 0xc0, 0x00, 0x00                         }))
OPT_TEST(float64, double,        -2.0, (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }))
OPT_TEST(bool,    bool,          true, (std::initializer_list<std::uint8_t>{ 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01                         }))

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

TEST(read_vector) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x04, // 4 elements
          0x00, 0x00, 0x00, 0x01, // first element
          0x00, 0x00, 0x00, 0x02,
          0x00, 0x00, 0x00, 0x03,
          0x00, 0x00, 0x00, 0x04, // last element
      },
      ioctx.get_executor());

  std::vector<test_value> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_sequence(v),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK_EQUAL(4u, v.size());
  CHECK_EQUAL(1, v.at(0).value);
  CHECK_EQUAL(2, v.at(1).value);
  CHECK_EQUAL(3, v.at(2).value);
  CHECK_EQUAL(4, v.at(3).value);
}

TEST(read_vector_of_ints) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x04, // 4 elements
          0x00, 0x00, 0x00, 0x01, // first element
          0x00, 0x00, 0x00, 0x02,
          0x00, 0x00, 0x00, 0x03,
          0x00, 0x00, 0x00, 0x04, // last element
      },
      ioctx.get_executor());

  std::vector<int> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_sequence(v).of(earnest::xdr_int32),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK_EQUAL(4u, v.size());
  CHECK_EQUAL(1, v.at(0));
  CHECK_EQUAL(2, v.at(1));
  CHECK_EQUAL(3, v.at(2));
  CHECK_EQUAL(4, v.at(3));
}

TEST(read_vector_reject_if_too_large) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x04, // 4 elements
          0x00, 0x00, 0x00, 0x01, // first element
          0x00, 0x00, 0x00, 0x02,
          0x00, 0x00, 0x00, 0x03,
          0x00, 0x00, 0x00, 0x04, // last element
      },
      ioctx.get_executor());

  std::vector<test_value> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_sequence(v, 2),
      [&](std::error_code ec) {
        CHECK_EQUAL(make_error_code(earnest::xdr_errc::decoding_error), ec);
      });
  ioctx.run();
}

TEST(write_vector) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::vector<test_value> v = { test_value{ 1 }, test_value{ 2 }, test_value{ 3 }, test_value{ 4 } };
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_sequence(v),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x04, // 4 elements
              0x00, 0x00, 0x00, 0x01, // first element
              0x00, 0x00, 0x00, 0x02,
              0x00, 0x00, 0x00, 0x03,
              0x00, 0x00, 0x00, 0x04, // last element
          },
          ioctx.get_executor()),
      stream);
}

TEST(write_vector_of_ints) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::vector<int> v = { 1, 2, 3, 4 };
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_sequence(v).of(earnest::xdr_int32),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x04, // 4 elements
              0x00, 0x00, 0x00, 0x01, // first element
              0x00, 0x00, 0x00, 0x02,
              0x00, 0x00, 0x00, 0x03,
              0x00, 0x00, 0x00, 0x04, // last element
          },
          ioctx.get_executor()),
      stream);
}

TEST(write_vector_reject_if_too_large) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::vector<test_value> v = { test_value{ 1 }, test_value{ 2 }, test_value{ 3 }, test_value{ 4 } };
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_sequence(v, 2),
      [&](std::error_code ec) {
        CHECK_EQUAL(make_error_code(earnest::xdr_errc::encoding_error), ec);
      });
  ioctx.run();
}

TEST(read_list) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x04, // 4 elements
          0x00, 0x00, 0x00, 0x01, // first element
          0x00, 0x00, 0x00, 0x02,
          0x00, 0x00, 0x00, 0x03,
          0x00, 0x00, 0x00, 0x04, // last element
      },
      ioctx.get_executor());

  std::list<test_value> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_sequence(v),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK_EQUAL(4u, v.size());
  CHECK_EQUAL(1, std::vector<test_value>(v.cbegin(), v.cend()).at(0).value);
  CHECK_EQUAL(2, std::vector<test_value>(v.cbegin(), v.cend()).at(1).value);
  CHECK_EQUAL(3, std::vector<test_value>(v.cbegin(), v.cend()).at(2).value);
  CHECK_EQUAL(4, std::vector<test_value>(v.cbegin(), v.cend()).at(3).value);
}

TEST(read_list_of_ints) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x04, // 4 elements
          0x00, 0x00, 0x00, 0x01, // first element
          0x00, 0x00, 0x00, 0x02,
          0x00, 0x00, 0x00, 0x03,
          0x00, 0x00, 0x00, 0x04, // last element
      },
      ioctx.get_executor());

  std::list<int> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_sequence(v).of(earnest::xdr_int32),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK_EQUAL(4u, v.size());
  CHECK_EQUAL(1, std::vector<int>(v.cbegin(), v.cend()).at(0));
  CHECK_EQUAL(2, std::vector<int>(v.cbegin(), v.cend()).at(1));
  CHECK_EQUAL(3, std::vector<int>(v.cbegin(), v.cend()).at(2));
  CHECK_EQUAL(4, std::vector<int>(v.cbegin(), v.cend()).at(3));
}

TEST(read_list_reject_if_too_large) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x04, // 4 elements
          0x00, 0x00, 0x00, 0x01, // first element
          0x00, 0x00, 0x00, 0x02,
          0x00, 0x00, 0x00, 0x03,
          0x00, 0x00, 0x00, 0x04, // last element
      },
      ioctx.get_executor());

  std::list<test_value> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_sequence(v, 2),
      [&](std::error_code ec) {
        CHECK_EQUAL(make_error_code(earnest::xdr_errc::decoding_error), ec);
      });
  ioctx.run();
}

TEST(write_list) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::list<test_value> v = { test_value{ 1 }, test_value{ 2 }, test_value{ 3 }, test_value{ 4 } };
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_sequence(v),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x04, // 4 elements
              0x00, 0x00, 0x00, 0x01, // first element
              0x00, 0x00, 0x00, 0x02,
              0x00, 0x00, 0x00, 0x03,
              0x00, 0x00, 0x00, 0x04, // last element
          },
          ioctx.get_executor()),
      stream);
}

TEST(write_list_of_ints) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::list<int> v = { 1, 2, 3, 4 };
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_sequence(v).of(earnest::xdr_int32),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x04, // 4 elements
              0x00, 0x00, 0x00, 0x01, // first element
              0x00, 0x00, 0x00, 0x02,
              0x00, 0x00, 0x00, 0x03,
              0x00, 0x00, 0x00, 0x04, // last element
          },
          ioctx.get_executor()),
      stream);
}

TEST(write_list_reject_if_too_large) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::list<test_value> v = { test_value{ 1 }, test_value{ 2 }, test_value{ 3 }, test_value{ 4 } };
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_sequence(v, 2),
      [&](std::error_code ec) {
        CHECK_EQUAL(make_error_code(earnest::xdr_errc::encoding_error), ec);
      });
  ioctx.run();
}

TEST(read_set) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x04, // 4 elements
          0x00, 0x00, 0x00, 0x04, // first element
          0x00, 0x00, 0x00, 0x01,
          0x00, 0x00, 0x00, 0x03,
          0x00, 0x00, 0x00, 0x02, // last element
      },
      ioctx.get_executor());

  std::set<test_value> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_set(v),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK_EQUAL(4u, v.size());
  CHECK_EQUAL(1, std::vector<test_value>(v.cbegin(), v.cend()).at(0).value);
  CHECK_EQUAL(2, std::vector<test_value>(v.cbegin(), v.cend()).at(1).value);
  CHECK_EQUAL(3, std::vector<test_value>(v.cbegin(), v.cend()).at(2).value);
  CHECK_EQUAL(4, std::vector<test_value>(v.cbegin(), v.cend()).at(3).value);
}

TEST(read_set_of_ints) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x04, // 4 elements
          0x00, 0x00, 0x00, 0x04, // first element
          0x00, 0x00, 0x00, 0x01,
          0x00, 0x00, 0x00, 0x03,
          0x00, 0x00, 0x00, 0x02, // last element
      },
      ioctx.get_executor());

  std::set<int> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_set(v).of(earnest::xdr_int32),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK_EQUAL(4u, v.size());
  CHECK_EQUAL(1, std::vector<int>(v.cbegin(), v.cend()).at(0));
  CHECK_EQUAL(2, std::vector<int>(v.cbegin(), v.cend()).at(1));
  CHECK_EQUAL(3, std::vector<int>(v.cbegin(), v.cend()).at(2));
  CHECK_EQUAL(4, std::vector<int>(v.cbegin(), v.cend()).at(3));
}

TEST(read_set_reject_if_duplicate) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x04, // 4 elements
          0x00, 0x00, 0x00, 0x01, // first element
          0x00, 0x00, 0x00, 0x02,
          0x00, 0x00, 0x00, 0x01, // duplicate of first element
          0x00, 0x00, 0x00, 0x04, // last element
      },
      ioctx.get_executor());

  std::set<test_value> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_set(v),
      [&](std::error_code ec) {
        CHECK_EQUAL(make_error_code(earnest::xdr_errc::decoding_error), ec);
      });
  ioctx.run();
}

TEST(read_set_reject_if_too_large) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x04, // 4 elements
          0x00, 0x00, 0x00, 0x01, // first element
          0x00, 0x00, 0x00, 0x02,
          0x00, 0x00, 0x00, 0x03,
          0x00, 0x00, 0x00, 0x04, // last element
      },
      ioctx.get_executor());

  std::set<test_value> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_set(v, 2),
      [&](std::error_code ec) {
        CHECK_EQUAL(make_error_code(earnest::xdr_errc::decoding_error), ec);
      });
  ioctx.run();
}

TEST(write_set) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::set<test_value> v = { test_value{ 1 }, test_value{ 2 }, test_value{ 3 }, test_value{ 4 } };
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_set(v),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x04, // 4 elements
              0x00, 0x00, 0x00, 0x01, // first element
              0x00, 0x00, 0x00, 0x02,
              0x00, 0x00, 0x00, 0x03,
              0x00, 0x00, 0x00, 0x04, // last element
          },
          ioctx.get_executor()),
      stream);
}

TEST(write_set_of_ints) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::set<int> v = { 1, 2, 3, 4 };
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_set(v).of(earnest::xdr_int32),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x04, // 4 elements
              0x00, 0x00, 0x00, 0x01, // first element
              0x00, 0x00, 0x00, 0x02,
              0x00, 0x00, 0x00, 0x03,
              0x00, 0x00, 0x00, 0x04, // last element
          },
          ioctx.get_executor()),
      stream);
}

TEST(write_set_reject_if_too_large) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::set<test_value> v = { test_value{ 1 }, test_value{ 2 }, test_value{ 3 }, test_value{ 4 } };
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_set(v, 2),
      [&](std::error_code ec) {
        CHECK_EQUAL(make_error_code(earnest::xdr_errc::encoding_error), ec);
      });
  ioctx.run();
}

TEST(read_multiset) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x04, // 4 elements
          0x00, 0x00, 0x00, 0x04, // first element
          0x00, 0x00, 0x00, 0x01,
          0x00, 0x00, 0x00, 0x03,
          0x00, 0x00, 0x00, 0x01, // last element
      },
      ioctx.get_executor());

  std::multiset<test_value> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_set(v),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK_EQUAL(4u, v.size());
  CHECK_EQUAL(1, std::vector<test_value>(v.cbegin(), v.cend()).at(0).value);
  CHECK_EQUAL(1, std::vector<test_value>(v.cbegin(), v.cend()).at(1).value);
  CHECK_EQUAL(3, std::vector<test_value>(v.cbegin(), v.cend()).at(2).value);
  CHECK_EQUAL(4, std::vector<test_value>(v.cbegin(), v.cend()).at(3).value);
}

TEST(read_multiset_reject_if_too_large) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x04, // 4 elements
          0x00, 0x00, 0x00, 0x01, // first element
          0x00, 0x00, 0x00, 0x02,
          0x00, 0x00, 0x00, 0x03,
          0x00, 0x00, 0x00, 0x04, // last element
      },
      ioctx.get_executor());

  std::multiset<test_value> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_set(v, 2),
      [&](std::error_code ec) {
        CHECK_EQUAL(make_error_code(earnest::xdr_errc::decoding_error), ec);
      });
  ioctx.run();
}

TEST(write_multiset) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::multiset<test_value> v = { test_value{ 1 }, test_value{ 1 }, test_value{ 3 }, test_value{ 4 } };
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_set(v),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x04, // 4 elements
              0x00, 0x00, 0x00, 0x01, // first element
              0x00, 0x00, 0x00, 0x01,
              0x00, 0x00, 0x00, 0x03,
              0x00, 0x00, 0x00, 0x04, // last element
          },
          ioctx.get_executor()),
      stream);
}

TEST(write_multiset_reject_if_too_large) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::multiset<test_value> v = { test_value{ 1 }, test_value{ 1 }, test_value{ 3 }, test_value{ 4 } };
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_set(v, 2),
      [&](std::error_code ec) {
        CHECK_EQUAL(make_error_code(earnest::xdr_errc::encoding_error), ec);
      });
  ioctx.run();
}

TEST(read_map) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x02, // 2 elements
          0x00, 0x00, 0x00, 0x01,
          0x00, 0x00, 0x00, 0x03,
          0x00, 0x00, 0x00, 0x02,
          0x00, 0x00, 0x00, 0x04,
      },
      ioctx.get_executor());

  std::map<test_value, test_value> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_map(v),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK((std::map<test_value, test_value>{ { test_value{1}, test_value{3} }, { test_value{2}, test_value{4} }, }) == v);
}

TEST(read_map_of_ints) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x02, // 2 elements
          0x00, 0x00, 0x00, 0x01,
          0x00, 0x00, 0x00, 0x03,
          0x00, 0x00, 0x00, 0x02,
          0x00, 0x00, 0x00, 0x04,
      },
      ioctx.get_executor());

  std::map<int, int> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_map(v).of(earnest::xdr_int32, earnest::xdr_int32),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK((std::map<int, int>{ { 1, 3 }, { 2, 4 }, }) == v);
}

TEST(read_map_reject_if_duplicate) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x02, // 2 elements
          0x00, 0x00, 0x00, 0x01,
          0x00, 0x00, 0x00, 0x02,
          0x00, 0x00, 0x00, 0x01, // duplicate of first element
          0x00, 0x00, 0x00, 0x04,
      },
      ioctx.get_executor());

  std::map<test_value, test_value> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_map(v),
      [&](std::error_code ec) {
        CHECK_EQUAL(make_error_code(earnest::xdr_errc::decoding_error), ec);
      });
  ioctx.run();
}

TEST(read_map_reject_if_too_large) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x02, // 2 elements
          0x00, 0x00, 0x00, 0x01, // first element
          0x00, 0x00, 0x00, 0x02,
          0x00, 0x00, 0x00, 0x03,
          0x00, 0x00, 0x00, 0x04, // last element
      },
      ioctx.get_executor());

  std::map<test_value, test_value> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_map(v, 1),
      [&](std::error_code ec) {
        CHECK_EQUAL(make_error_code(earnest::xdr_errc::decoding_error), ec);
      });
  ioctx.run();
}

TEST(write_map) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::map<test_value, test_value> v = { { test_value{ 1 }, test_value{ 2 } }, { test_value{ 3 }, test_value{ 4 } } };
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_map(v),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x02, // 2 elements
              0x00, 0x00, 0x00, 0x01, // first element
              0x00, 0x00, 0x00, 0x02,
              0x00, 0x00, 0x00, 0x03,
              0x00, 0x00, 0x00, 0x04, // last element
          },
          ioctx.get_executor()),
      stream);
}

TEST(write_map_of_ints) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::map<int, int> v = { { 1, 2 }, { 3, 4 } };
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_map(v).of(earnest::xdr_int32, earnest::xdr_int32),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x02, // 2 elements
              0x00, 0x00, 0x00, 0x01, // first element
              0x00, 0x00, 0x00, 0x02,
              0x00, 0x00, 0x00, 0x03,
              0x00, 0x00, 0x00, 0x04, // last element
          },
          ioctx.get_executor()),
      stream);
}

TEST(write_map_reject_if_too_large) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::map<test_value, test_value> v = { { test_value{ 1 }, test_value{ 2 } }, { test_value{ 3 }, test_value{ 4 } } };
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_map(v, 1),
      [&](std::error_code ec) {
        CHECK_EQUAL(make_error_code(earnest::xdr_errc::encoding_error), ec);
      });
  ioctx.run();
}

TEST(read_multimap) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x02, // 2 elements
          0x00, 0x00, 0x00, 0x01,
          0x00, 0x00, 0x00, 0x03,
          0x00, 0x00, 0x00, 0x01,
          0x00, 0x00, 0x00, 0x04,
      },
      ioctx.get_executor());

  std::multimap<test_value, test_value> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_map(v),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK(stream.empty());
  CHECK((std::multimap<test_value, test_value>{ { test_value{1}, test_value{3} }, { test_value{1}, test_value{4} }, }) == v);
}

TEST(read_multimap_reject_if_too_large) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      std::initializer_list<std::uint8_t>{
          0x00, 0x00, 0x00, 0x02, // 2 elements
          0x00, 0x00, 0x00, 0x01,
          0x00, 0x00, 0x00, 0x02,
          0x00, 0x00, 0x00, 0x03,
          0x00, 0x00, 0x00, 0x04,
      },
      ioctx.get_executor());

  std::multimap<test_value, test_value> v;
  async_read(
      stream,
      earnest::xdr_reader<>() & earnest::xdr_map(v, 1),
      [&](std::error_code ec) {
        CHECK_EQUAL(make_error_code(earnest::xdr_errc::decoding_error), ec);
      });
  ioctx.run();
}

TEST(write_multimap) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::multimap<test_value, test_value> v = { { test_value{ 1 }, test_value{ 2 } }, { test_value{ 1 }, test_value{ 4 } } };
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_map(v),
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
      });
  ioctx.run();

  CHECK_EQUAL(
      byte_stream<asio::io_context::executor_type>(
          std::initializer_list<std::uint8_t>{
              0x00, 0x00, 0x00, 0x02, // 2 elements
              0x00, 0x00, 0x00, 0x01,
              0x00, 0x00, 0x00, 0x02,
              0x00, 0x00, 0x00, 0x01,
              0x00, 0x00, 0x00, 0x04,
          },
          ioctx.get_executor()),
      stream);
}

TEST(write_multimap_reject_if_too_large) {
  asio::io_context ioctx;
  byte_stream<asio::io_context::executor_type> stream(
      ioctx.get_executor());

  std::multimap<test_value, test_value> v = { { test_value{ 1 }, test_value{ 2 } }, { test_value{ 3 }, test_value{ 4 } } };
  async_write(
      stream,
      earnest::xdr_writer<>() & earnest::xdr_map(v, 1),
      [&](std::error_code ec) {
        CHECK_EQUAL(make_error_code(earnest::xdr_errc::encoding_error), ec);
      });
  ioctx.run();
}

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
