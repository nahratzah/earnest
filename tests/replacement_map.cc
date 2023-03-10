#include "UnitTest++/UnitTest++.h"
#include <earnest/detail/replacement_map.h>
#include <earnest/fd.h>
#include <vector>
#include <cstring>

using earnest::detail::replacement_map;
using earnest::fd;

template<typename T>
auto read_all_at(const replacement_map& map, fd::offset_type off, std::size_t len) -> std::vector<T> {
  static_assert(sizeof(T) == 1);

  auto buf = std::vector<T>(len, T());
  for (std::size_t total_rlen = 0; total_rlen < len; ) {
    std::size_t nbytes = len - total_rlen;
    const auto rlen = map.read_at(off + total_rlen, buf.data() + total_rlen, nbytes);
    if (rlen == 0u) {
      buf.resize(total_rlen);
      break;
    }

    CHECK(rlen > 0); // some data was read.
    CHECK_EQUAL(len - total_rlen, nbytes); // nbytes is not changed.

    total_rlen += rlen;
  }

  return buf;
}

TEST(empty_read) {
  replacement_map map;

  std::size_t nbytes = 17;
  auto buf = std::vector<std::uint8_t>(nbytes, std::uint8_t());
  auto rlen = map.read_at(5, buf.data(), nbytes);

  CHECK_EQUAL(17u, nbytes);
  CHECK_EQUAL(0u, rlen);
}

TEST(write_and_commit) {
  replacement_map map;
  const auto bytes = u8"foobar";

  map.write_at(17, &bytes[0], 6).commit();
  auto buf = read_all_at<char>(map, 17, 6);

  CHECK_EQUAL(std::string_view(bytes, 6), std::string_view(buf.data(), buf.size()));
}

TEST(write_uncommitted) {
  replacement_map map;
  const auto bytes = u8"foobar";

  map.write_at(17, &bytes[0], 6);
  auto buf = read_all_at<char>(map, 17, 6);

  CHECK_EQUAL(0u, buf.size());
}

TEST(write_replace) {
  replacement_map map;

  map.write_at(17, u8"foobar", 6).commit();
  map.write_at(18, u8"ffrr", 4).commit();
  auto buf = read_all_at<char>(map, 17, 6);

  CHECK_EQUAL(u8"fffrrr", std::string_view(buf.data(), buf.size()));
}

TEST(write_non_replace) {
  replacement_map map;

  map.write_at(17, u8"foo", 3).commit();
  map.write_at(19, u8"rbar", 4, false).commit();
  auto buf = read_all_at<char>(map, 17, 6);

  CHECK_EQUAL(u8"foobar", std::string_view(buf.data(), buf.size()));
}

TEST(write_replace_head) {
  replacement_map map;

  map.write_at(20, u8"xxxx", 4).commit();
  map.write_at(18, u8"yyyy", 4).commit();
  auto buf = read_all_at<char>(map, 18, 6);

  CHECK_EQUAL(u8"yyyyxx", std::string_view(buf.data(), buf.size()));
}

TEST(write_replace_tail) {
  replacement_map map;

  map.write_at(18, u8"xxxx", 4).commit();
  map.write_at(20, u8"yyyy", 4).commit();
  auto buf = read_all_at<char>(map, 18, 6);

  CHECK_EQUAL(u8"xxyyyy", std::string_view(buf.data(), buf.size()));
}

TEST(write_replace_many) {
  replacement_map map;

  map.write_at( 6, u8"x", 1).commit();
  map.write_at( 8, u8"x", 1).commit();
  map.write_at(10, u8"x", 1).commit();
  map.write_at( 6, u8"67890", 5).commit();
  auto buf = read_all_at<char>(map, 6, 5);

  CHECK_EQUAL(u8"67890", std::string_view(buf.data(), buf.size()));
}

TEST(write_replace_many_with_overlapping_head_and_tail) {
  replacement_map map;

  map.write_at( 5, u8"xx", 2).commit();
  map.write_at( 8, u8"y",  1).commit();
  map.write_at(10, u8"zz", 2).commit();
  map.write_at( 6, u8"67890", 5).commit();
  auto buf = read_all_at<char>(map, 5, 7);

  CHECK_EQUAL(u8"x67890z", std::string_view(buf.data(), buf.size()));
}

TEST(iterate) {
  replacement_map map;
  map.write_at( 0, u8"xx", 2).commit();
  map.write_at( 4, u8"y",  1).commit();
  map.write_at(10, u8"zz", 2).commit();

  replacement_map::const_iterator iter = map.cbegin();
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(0u, iter->begin_offset());
  CHECK_EQUAL(2u, iter->end_offset());
  CHECK_EQUAL(2u, iter->size());
  if (iter->size() >= 2u) CHECK(std::memcmp(u8"xx", iter->data(), 2u) == 0);

  ++iter;
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(4u, iter->begin_offset());
  CHECK_EQUAL(5u, iter->end_offset());
  CHECK_EQUAL(1u, iter->size());
  if (iter->size() >= 1u) CHECK(std::memcmp(u8"y", iter->data(), 1u) == 0);

  ++iter;
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(10u, iter->begin_offset());
  CHECK_EQUAL(12u, iter->end_offset());
  CHECK_EQUAL(2u, iter->size());
  if (iter->size() >= 2u) CHECK(std::memcmp(u8"zz", iter->data(), 2u) == 0);

  ++iter;
  CHECK(iter == map.end());
}

TEST(clear) {
  replacement_map map;
  map.write_at( 0, u8"xx", 2).commit();
  map.write_at( 4, u8"y",  1).commit();
  map.write_at(10, u8"zz", 2).commit();
  map.clear();

  CHECK(map.empty());
  CHECK(map.begin() == map.end());
}

TEST(copy_constructor) {
  replacement_map orig_map;
  orig_map.write_at( 0, u8"xx", 2).commit();
  orig_map.write_at( 4, u8"y",  1).commit();
  orig_map.write_at(10, u8"zz", 2).commit();
  replacement_map map = orig_map;
  orig_map.clear();

  replacement_map::const_iterator iter = map.cbegin();
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(0u, iter->begin_offset());
  CHECK_EQUAL(2u, iter->end_offset());
  CHECK_EQUAL(2u, iter->size());
  if (iter->size() >= 2u) CHECK(std::memcmp(u8"xx", iter->data(), 2u) == 0);

  ++iter;
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(4u, iter->begin_offset());
  CHECK_EQUAL(5u, iter->end_offset());
  CHECK_EQUAL(1u, iter->size());
  if (iter->size() >= 1u) CHECK(std::memcmp(u8"y", iter->data(), 1u) == 0);

  ++iter;
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(10u, iter->begin_offset());
  CHECK_EQUAL(12u, iter->end_offset());
  CHECK_EQUAL(2u, iter->size());
  if (iter->size() >= 2u) CHECK(std::memcmp(u8"zz", iter->data(), 2u) == 0);

  ++iter;
  CHECK(iter == map.end());
}

TEST(move_constructor) {
  replacement_map orig_map;
  orig_map.write_at( 0, u8"xx", 2).commit();
  orig_map.write_at( 4, u8"y",  1).commit();
  orig_map.write_at(10, u8"zz", 2).commit();
  replacement_map map = std::move(orig_map);

  replacement_map::const_iterator iter = map.cbegin();
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(0u, iter->begin_offset());
  CHECK_EQUAL(2u, iter->end_offset());
  CHECK_EQUAL(2u, iter->size());
  if (iter->size() >= 2u) CHECK(std::memcmp(u8"xx", iter->data(), 2u) == 0);

  ++iter;
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(4u, iter->begin_offset());
  CHECK_EQUAL(5u, iter->end_offset());
  CHECK_EQUAL(1u, iter->size());
  if (iter->size() >= 1u) CHECK(std::memcmp(u8"y", iter->data(), 1u) == 0);

  ++iter;
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(10u, iter->begin_offset());
  CHECK_EQUAL(12u, iter->end_offset());
  CHECK_EQUAL(2u, iter->size());
  if (iter->size() >= 2u) CHECK(std::memcmp(u8"zz", iter->data(), 2u) == 0);

  ++iter;
  CHECK(iter == map.end());

  CHECK(orig_map.empty());
  CHECK(orig_map.begin() == orig_map.end());
}

TEST(copy_assignment) {
  replacement_map map;
  map.write_at( 0, u8"aa", 2).commit();
  map.write_at( 3, u8"b", 1).commit();

  replacement_map orig_map;
  orig_map.write_at( 0, u8"xx", 2).commit();
  orig_map.write_at( 4, u8"y",  1).commit();
  orig_map.write_at(10, u8"zz", 2).commit();
  map = orig_map;
  orig_map.clear();

  replacement_map::const_iterator iter = map.cbegin();
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(0u, iter->begin_offset());
  CHECK_EQUAL(2u, iter->end_offset());
  CHECK_EQUAL(2u, iter->size());
  if (iter->size() >= 2u) CHECK(std::memcmp(u8"xx", iter->data(), 2u) == 0);

  ++iter;
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(4u, iter->begin_offset());
  CHECK_EQUAL(5u, iter->end_offset());
  CHECK_EQUAL(1u, iter->size());
  if (iter->size() >= 1u) CHECK(std::memcmp(u8"y", iter->data(), 1u) == 0);

  ++iter;
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(10u, iter->begin_offset());
  CHECK_EQUAL(12u, iter->end_offset());
  CHECK_EQUAL(2u, iter->size());
  if (iter->size() >= 2u) CHECK(std::memcmp(u8"zz", iter->data(), 2u) == 0);

  ++iter;
  CHECK(iter == map.end());
}

TEST(move_assignment) {
  replacement_map map;
  map.write_at( 0, u8"aa", 2).commit();
  map.write_at( 3, u8"b", 1).commit();

  replacement_map orig_map;
  orig_map.write_at( 0, u8"xx", 2).commit();
  orig_map.write_at( 4, u8"y",  1).commit();
  orig_map.write_at(10, u8"zz", 2).commit();
  map = std::move(orig_map);

  replacement_map::const_iterator iter = map.cbegin();
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(0u, iter->begin_offset());
  CHECK_EQUAL(2u, iter->end_offset());
  CHECK_EQUAL(2u, iter->size());
  if (iter->size() >= 2u) CHECK(std::memcmp(u8"xx", iter->data(), 2u) == 0);

  ++iter;
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(4u, iter->begin_offset());
  CHECK_EQUAL(5u, iter->end_offset());
  CHECK_EQUAL(1u, iter->size());
  if (iter->size() >= 1u) CHECK(std::memcmp(u8"y", iter->data(), 1u) == 0);

  ++iter;
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(10u, iter->begin_offset());
  CHECK_EQUAL(12u, iter->end_offset());
  CHECK_EQUAL(2u, iter->size());
  if (iter->size() >= 2u) CHECK(std::memcmp(u8"zz", iter->data(), 2u) == 0);

  ++iter;
  CHECK(iter == map.end());

  CHECK(orig_map.empty());
  CHECK(orig_map.begin() == orig_map.end());
}

TEST(truncate) {
  replacement_map map;
  map.write_at( 0, u8"aa", 2).commit();
  map.write_at( 2, u8"b", 1).commit();

  // Truncation past-the-end does nothing.
  {
    map.truncate(9000);
    auto buf = read_all_at<char>(map, 0, 9000);
    CHECK_EQUAL(u8"aab", std::string_view(buf.data(), buf.size()));
  }

  // Truncation at-the-end does nothing.
  {
    map.truncate(3);
    auto buf = read_all_at<char>(map, 0, 9000);
    CHECK_EQUAL(u8"aab", std::string_view(buf.data(), buf.size()));
  }

  // Truncation drops 1 segment in its entirety, and 1 partially.
  {
    map.truncate(1);
    auto buf = read_all_at<char>(map, 0, 9000);
    CHECK_EQUAL(u8"a", std::string_view(buf.data(), buf.size()));
  }

  // Truncation drops 1 segment in its entirety.
  {
    map.truncate(0);
    auto buf = read_all_at<char>(map, 0, 9000);
    CHECK_EQUAL(u8"", std::string_view(buf.data(), buf.size()));
  }
}

int main(int argc, char** argv) {
  return UnitTest::RunAllTests();
}
