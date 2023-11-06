#include <earnest/execution_io.h>

#include <UnitTest++/UnitTest++.h>
#include <tuple>
#include <variant>

#include "temporary_file.h"

using namespace earnest::execution;


TEST(lazy_write_ec) {
  using traits = sender_traits<decltype(lazy_write_ec(std::declval<int>(), std::declval<std::span<const std::byte>>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::size_t>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr, std::error_code>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  temporary_file tmp;

  auto [wlen] = sync_wait(
      lazy_write_ec(tmp.fd, std::as_bytes(std::span("abcd", 4)))).value();
  CHECK_EQUAL(4, wlen);
  CHECK_EQUAL(std::string("abcd"), tmp.get_contents());
}

TEST(lazy_write) {
  using traits = sender_traits<decltype(lazy_write(std::declval<int>(), std::declval<std::span<const std::byte>>()))>;
  static_assert(std::is_same_v<
      std::variant<std::tuple<std::size_t>>,
      traits::value_types<std::tuple, std::variant>>);
  static_assert(std::is_same_v<
      std::variant<std::exception_ptr>,
      traits::error_types<std::variant>>);
  static_assert(!traits::sends_done);

  temporary_file tmp;

  auto [wlen] = sync_wait(
      lazy_write(tmp.fd, std::as_bytes(std::span("abcd", 4)))).value();
  CHECK_EQUAL(4, wlen);
  CHECK_EQUAL(std::string("abcd"), tmp.get_contents());
}

TEST(lazy_read_ec) {
  using traits = sender_traits<decltype(lazy_read_ec(std::declval<int>(), std::declval<std::span<std::byte>>()))>;
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
        lazy_read_ec(tmp.fd, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
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
        lazy_read_ec(tmp.fd, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
    CHECK(eof);
    CHECK_EQUAL(4, rlen);
    CHECK_EQUAL(std::string("abcd"), s.substr(0, rlen));
    CHECK_EQUAL(std::string("abcdxxxxxx"), s); // rest of the string is untouched
  }
}

TEST(lazy_read) {
  using traits = sender_traits<decltype(lazy_read_ec(std::declval<int>(), std::declval<std::span<std::byte>>()))>;
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
        lazy_read(tmp.fd, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
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
        lazy_read(tmp.fd, std::as_writable_bytes(std::span(s.data(), s.size())))).value();
    CHECK(eof);
    CHECK_EQUAL(4, rlen);
    CHECK_EQUAL(std::string("abcd"), s.substr(0, rlen));
    CHECK_EQUAL(std::string("abcdxxxxxx"), s); // rest of the string is untouched
  }
}
