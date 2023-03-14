#include <earnest/xdr.h>

#include <UnitTest++/UnitTest++.h>

#include <asio/io_context.hpp>
#include <earnest/detail/byte_stream.h>


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
  CHECK_EQUAL(std::TYPE##BITS##_t(1), v);                                                          \
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
  async_write(                                                                                     \
      stream,                                                                                      \
      earnest::xdr_writer<>() &                                                                    \
          earnest::xdr_getset<int>([]() { return 0x17; }).as(earnest::xdr_##TYPE##BITS),           \
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
