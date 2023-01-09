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

int main() {
  return UnitTest::RunAllTests();
}
