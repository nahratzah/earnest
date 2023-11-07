#include <earnest/execution_io.h>

#include <UnitTest++/UnitTest++.h>
#include <tuple>
#include <variant>

#include "temporary_file.h"

using namespace earnest::execution;


TEST(lazy_write_ec) {
  using traits = sender_traits<decltype(io::lazy_write_ec(std::declval<int>(), std::declval<std::span<const std::byte>>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::size_t>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr, std::error_code>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  temporary_file tmp;

  auto [wlen] = sync_wait(
      io::lazy_write_ec(tmp.fd, std::as_bytes(std::span("abcd", 4)))).value();
  CHECK_EQUAL(4, wlen);
  CHECK_EQUAL(std::string("abcd"), tmp.get_contents());
}

TEST(lazy_write) {
  using traits = sender_traits<decltype(io::lazy_write(std::declval<int>(), std::declval<std::span<const std::byte>>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::size_t>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  temporary_file tmp;

  auto [wlen] = sync_wait(
      io::lazy_write(tmp.fd, std::as_bytes(std::span("abcd", 4)))).value();
  CHECK_EQUAL(4, wlen);
  CHECK_EQUAL(std::string("abcd"), tmp.get_contents());
}

TEST(lazy_read_ec) {
  using traits = sender_traits<decltype(io::lazy_read_ec(std::declval<int>(), std::declval<std::span<std::byte>>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::size_t, bool>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr, std::error_code>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  { // partial read of the file
    temporary_file tmp;
    tmp.set_contents("abcd");

    std::string s;
    s.resize(3, 'x');
    auto [rlen, eof] = sync_wait(
        io::lazy_read_ec(tmp.fd, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
    CHECK(!eof);
    CHECK_EQUAL(3, rlen);
    CHECK_EQUAL(std::string("abc"), s.substr(0, rlen));
  }

  { // over-sized read of the file
    temporary_file tmp;
    tmp.set_contents("abcd");

    std::string s;
    s.resize(10, 'x');
    auto [rlen, eof] = sync_wait(
        io::lazy_read_ec(tmp.fd, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
    CHECK(eof);
    CHECK_EQUAL(4, rlen);
    CHECK_EQUAL(std::string("abcd"), s.substr(0, rlen));
    CHECK_EQUAL(std::string("abcdxxxxxx"), s); // rest of the string is untouched
  }
}

TEST(lazy_read) {
  using traits = sender_traits<decltype(io::lazy_read(std::declval<int>(), std::declval<std::span<std::byte>>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::size_t, bool>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  { // partial read of the file
    temporary_file tmp;
    tmp.set_contents("abcd");

    std::string s;
    s.resize(3, 'x');
    auto [rlen, eof] = sync_wait(
        io::lazy_read(tmp.fd, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
    CHECK(!eof);
    CHECK_EQUAL(3, rlen);
    CHECK_EQUAL(std::string("abc"), s.substr(0, rlen));
  }

  { // over-sized read of the file
    temporary_file tmp;
    tmp.set_contents("abcd");

    std::string s;
    s.resize(10, 'x');
    auto [rlen, eof] = sync_wait(
        io::lazy_read(tmp.fd, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
    CHECK(eof);
    CHECK_EQUAL(4, rlen);
    CHECK_EQUAL(std::string("abcd"), s.substr(0, rlen));
    CHECK_EQUAL(std::string("abcdxxxxxx"), s); // rest of the string is untouched
  }
}


TEST(lazy_write_at_ec) {
  using traits = sender_traits<decltype(io::lazy_write_at_ec(std::declval<int>(), std::declval<io::offset_type>(), std::declval<std::span<const std::byte>>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::size_t>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr, std::error_code>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  temporary_file tmp;
  tmp.set_contents("xxxx");

  auto [wlen] = sync_wait(
      io::lazy_write_at_ec(tmp.fd, 2, std::as_bytes(std::span("abcd", 4)))).value();
  CHECK_EQUAL(4, wlen);
  CHECK_EQUAL(std::string("xxabcd"), tmp.get_contents());
}

TEST(lazy_write_at) {
  using traits = sender_traits<decltype(io::lazy_write_at(std::declval<int>(), std::declval<io::offset_type>(), std::declval<std::span<const std::byte>>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::size_t>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  temporary_file tmp;
  tmp.set_contents("xxxx");

  auto [wlen] = sync_wait(
      io::lazy_write_at(tmp.fd, 2, std::as_bytes(std::span("abcd", 4)))).value();
  CHECK_EQUAL(4, wlen);
  CHECK_EQUAL(std::string("xxabcd"), tmp.get_contents());
}

TEST(lazy_read_at_ec) {
  using traits = sender_traits<decltype(io::lazy_read_at_ec(std::declval<int>(), std::declval<io::offset_type>(), std::declval<std::span<std::byte>>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::size_t, bool>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr, std::error_code>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  { // partial read of the file
    temporary_file tmp;
    tmp.set_contents("xxabcd");

    std::string s;
    s.resize(3, 'x');
    auto [rlen, eof] = sync_wait(
        io::lazy_read_at_ec(tmp.fd, 2, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
    CHECK(!eof);
    CHECK_EQUAL(3, rlen);
    CHECK_EQUAL(std::string("abc"), s.substr(0, rlen));
  }

  { // over-sized read of the file
    temporary_file tmp;
    tmp.set_contents("xxabcd");

    std::string s;
    s.resize(10, 'x');
    auto [rlen, eof] = sync_wait(
        io::lazy_read_at_ec(tmp.fd, 2, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
    CHECK(eof);
    CHECK_EQUAL(4, rlen);
    CHECK_EQUAL(std::string("abcd"), s.substr(0, rlen));
    CHECK_EQUAL(std::string("abcdxxxxxx"), s); // rest of the string is untouched
  }
}

TEST(lazy_read_at) {
  using traits = sender_traits<decltype(io::lazy_read_at(std::declval<int>(), std::declval<io::offset_type>(), std::declval<std::span<std::byte>>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::size_t, bool>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  { // partial read of the file
    temporary_file tmp;
    tmp.set_contents("xxabcd");

    std::string s;
    s.resize(3, 'x');
    auto [rlen, eof] = sync_wait(
        io::lazy_read_at(tmp.fd, 2, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
    CHECK(!eof);
    CHECK_EQUAL(3, rlen);
    CHECK_EQUAL(std::string("abc"), s.substr(0, rlen));
  }

  { // over-sized read of the file
    temporary_file tmp;
    tmp.set_contents("xxabcd");

    std::string s;
    s.resize(10, 'x');
    auto [rlen, eof] = sync_wait(
        io::lazy_read_at(tmp.fd, 2, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
    CHECK(eof);
    CHECK_EQUAL(4, rlen);
    CHECK_EQUAL(std::string("abcd"), s.substr(0, rlen));
    CHECK_EQUAL(std::string("abcdxxxxxx"), s); // rest of the string is untouched
  }
}


TEST(write_ec) {
  using traits = sender_traits<decltype(io::write_ec(std::declval<int>(), std::declval<std::span<const std::byte>>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::size_t>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr, std::error_code>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  temporary_file tmp;

  auto [wlen] = sync_wait(
      io::write_ec(tmp.fd, std::as_bytes(std::span("abcd", 4)))).value();
  CHECK_EQUAL(4, wlen);
  CHECK_EQUAL(std::string("abcd"), tmp.get_contents());
}

TEST(write) {
  using traits = sender_traits<decltype(io::write(std::declval<int>(), std::declval<std::span<const std::byte>>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::size_t>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  temporary_file tmp;

  auto [wlen] = sync_wait(
      io::write(tmp.fd, std::as_bytes(std::span("abcd", 4)))).value();
  CHECK_EQUAL(4, wlen);
  CHECK_EQUAL(std::string("abcd"), tmp.get_contents());
}

TEST(read_ec) {
  using traits = sender_traits<decltype(io::read_ec(std::declval<int>(), std::declval<std::span<std::byte>>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::size_t, bool>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr, std::error_code>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  { // partial read of the file
    temporary_file tmp;
    tmp.set_contents("abcd");

    std::string s;
    s.resize(3, 'x');
    auto [rlen, eof] = sync_wait(
        io::read_ec(tmp.fd, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
    CHECK(!eof);
    CHECK_EQUAL(3, rlen);
    CHECK_EQUAL(std::string("abc"), s.substr(0, rlen));
  }

  { // over-sized read of the file
    temporary_file tmp;
    tmp.set_contents("abcd");

    std::string s;
    s.resize(10, 'x');
    auto [rlen, eof] = sync_wait(
        io::read_ec(tmp.fd, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
    CHECK(eof);
    CHECK_EQUAL(4, rlen);
    CHECK_EQUAL(std::string("abcd"), s.substr(0, rlen));
    CHECK_EQUAL(std::string("abcdxxxxxx"), s); // rest of the string is untouched
  }
}

TEST(read) {
  using traits = sender_traits<decltype(io::read(std::declval<int>(), std::declval<std::span<std::byte>>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::size_t, bool>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  { // partial read of the file
    temporary_file tmp;
    tmp.set_contents("abcd");

    std::string s;
    s.resize(3, 'x');
    auto [rlen, eof] = sync_wait(
        io::read(tmp.fd, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
    CHECK(!eof);
    CHECK_EQUAL(3, rlen);
    CHECK_EQUAL(std::string("abc"), s.substr(0, rlen));
  }

  { // over-sized read of the file
    temporary_file tmp;
    tmp.set_contents("abcd");

    std::string s;
    s.resize(10, 'x');
    auto [rlen, eof] = sync_wait(
        io::read(tmp.fd, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
    CHECK(eof);
    CHECK_EQUAL(4, rlen);
    CHECK_EQUAL(std::string("abcd"), s.substr(0, rlen));
    CHECK_EQUAL(std::string("abcdxxxxxx"), s); // rest of the string is untouched
  }
}


TEST(write_at_ec) {
  using traits = sender_traits<decltype(io::write_at_ec(std::declval<int>(), std::declval<io::offset_type>(), std::declval<std::span<const std::byte>>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::size_t>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr, std::error_code>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  temporary_file tmp;
  tmp.set_contents("xxxx");

  auto [wlen] = sync_wait(
      io::write_at_ec(tmp.fd, 2, std::as_bytes(std::span("abcd", 4)))).value();
  CHECK_EQUAL(4, wlen);
  CHECK_EQUAL(std::string("xxabcd"), tmp.get_contents());
}

TEST(write_at) {
  using traits = sender_traits<decltype(io::write_at(std::declval<int>(), std::declval<io::offset_type>(), std::declval<std::span<const std::byte>>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::size_t>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  temporary_file tmp;
  tmp.set_contents("xxxx");

  auto [wlen] = sync_wait(
      io::write_at(tmp.fd, 2, std::as_bytes(std::span("abcd", 4)))).value();
  CHECK_EQUAL(4, wlen);
  CHECK_EQUAL(std::string("xxabcd"), tmp.get_contents());
}

TEST(read_at_ec) {
  using traits = sender_traits<decltype(io::read_at_ec(std::declval<int>(), std::declval<io::offset_type>(), std::declval<std::span<std::byte>>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::size_t, bool>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr, std::error_code>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  { // partial read of the file
    temporary_file tmp;
    tmp.set_contents("xxabcd");

    std::string s;
    s.resize(3, 'x');
    auto [rlen, eof] = sync_wait(
        io::read_at_ec(tmp.fd, 2, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
    CHECK(!eof);
    CHECK_EQUAL(3, rlen);
    CHECK_EQUAL(std::string("abc"), s.substr(0, rlen));
  }

  { // over-sized read of the file
    temporary_file tmp;
    tmp.set_contents("xxabcd");

    std::string s;
    s.resize(10, 'x');
    auto [rlen, eof] = sync_wait(
        io::read_at_ec(tmp.fd, 2, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
    CHECK(eof);
    CHECK_EQUAL(4, rlen);
    CHECK_EQUAL(std::string("abcd"), s.substr(0, rlen));
    CHECK_EQUAL(std::string("abcdxxxxxx"), s); // rest of the string is untouched
  }
}

TEST(read_at) {
  using traits = sender_traits<decltype(io::read_at(std::declval<int>(), std::declval<io::offset_type>(), std::declval<std::span<std::byte>>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::size_t, bool>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  { // partial read of the file
    temporary_file tmp;
    tmp.set_contents("xxabcd");

    std::string s;
    s.resize(3, 'x');
    auto [rlen, eof] = sync_wait(
        io::read_at(tmp.fd, 2, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
    CHECK(!eof);
    CHECK_EQUAL(3, rlen);
    CHECK_EQUAL(std::string("abc"), s.substr(0, rlen));
  }

  { // over-sized read of the file
    temporary_file tmp;
    tmp.set_contents("xxabcd");

    std::string s;
    s.resize(10, 'x');
    auto [rlen, eof] = sync_wait(
        io::read_at(tmp.fd, 2, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
    CHECK(eof);
    CHECK_EQUAL(4, rlen);
    CHECK_EQUAL(std::string("abcd"), s.substr(0, rlen));
    CHECK_EQUAL(std::string("abcdxxxxxx"), s); // rest of the string is untouched
  }
}


TEST(lazy_truncate_ec) {
  using traits = sender_traits<decltype(io::lazy_truncate_ec(std::declval<int>(), std::declval<io::offset_type>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr, std::error_code>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  temporary_file tmp;
  tmp.set_contents("abcdefg");

  auto x = sync_wait(io::lazy_truncate_ec(tmp.fd, 4)).value();
  CHECK(x == std::tuple<>{});
  CHECK_EQUAL("abcd", tmp.get_contents());
}

TEST(lazy_truncate) {
  using traits = sender_traits<decltype(io::lazy_truncate(std::declval<int>(), std::declval<io::offset_type>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  temporary_file tmp;
  tmp.set_contents("abcdefg");

  auto x = sync_wait(io::lazy_truncate(tmp.fd, 4)).value();
  CHECK(x == std::tuple<>{});
  CHECK_EQUAL("abcd", tmp.get_contents());
}

TEST(truncate_ec) {
  using traits = sender_traits<decltype(io::truncate_ec(std::declval<int>(), std::declval<io::offset_type>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr, std::error_code>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  temporary_file tmp;
  tmp.set_contents("abcdefg");

  auto x = sync_wait(io::truncate_ec(tmp.fd, 4)).value();
  CHECK(x == std::tuple<>{});
  CHECK_EQUAL("abcd", tmp.get_contents());
}

TEST(truncate) {
  using traits = sender_traits<decltype(io::truncate(std::declval<int>(), std::declval<io::offset_type>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  temporary_file tmp;
  tmp.set_contents("abcdefg");

  auto x = sync_wait(io::truncate(tmp.fd, 4)).value();
  CHECK(x == std::tuple<>{});
  CHECK_EQUAL("abcd", tmp.get_contents());
}