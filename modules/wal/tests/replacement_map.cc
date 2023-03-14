#include <earnest/detail/replacement_map.h>

#include <UnitTest++/UnitTest++.h>

#include <earnest/detail/byte_positional_stream.h>

#include <asio/io_context.hpp>

TEST(replacement_map_create) {
  using replacement_map = earnest::detail::replacement_map<earnest::detail::byte_positional_stream<asio::io_context::executor_type>>;

  replacement_map map;

  CHECK(map.empty());
  CHECK(map.begin() == map.end());
}

TEST(replacement_map_insert) {
  using wal_file = earnest::detail::byte_positional_stream<asio::io_context::executor_type>;
  using replacement_map = earnest::detail::replacement_map<earnest::detail::byte_positional_stream<asio::io_context::executor_type>>;

  asio::io_context ioctx;
  const auto wal = std::make_shared<const wal_file>(ioctx.get_executor());
  replacement_map map;
  map.insert(15, 16, 7, wal);

  CHECK(!map.empty());
  REQUIRE CHECK(std::next(map.begin()) == map.end());
  CHECK_EQUAL(15u, map.begin()->offset());
  CHECK_EQUAL(16u, map.begin()->wal_offset());
  CHECK_EQUAL(7u, map.begin()->size());
  CHECK_EQUAL(15u+7u, map.begin()->end_offset());
  CHECK_EQUAL(wal.get(), &map.begin()->wal_file());
}

TEST(replacement_map_insert_maintains_order) {
  using wal_file = earnest::detail::byte_positional_stream<asio::io_context::executor_type>;
  using replacement_map = earnest::detail::replacement_map<earnest::detail::byte_positional_stream<asio::io_context::executor_type>>;

  asio::io_context ioctx;
  const auto wal1 = std::make_shared<const wal_file>(ioctx.get_executor());
  const auto wal2 = std::make_shared<const wal_file>(ioctx.get_executor());
  replacement_map map;
  map.insert(115, 0, 4, wal2); // element 1
  map.insert(15, 16, 7, wal1); // element 0

  CHECK(!map.empty());

  // element 0
  auto iter = map.begin();
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(15u, iter->offset());
  CHECK_EQUAL(16u, iter->wal_offset());
  CHECK_EQUAL(7u, iter->size());
  CHECK_EQUAL(15u+7u, iter->end_offset());
  CHECK_EQUAL(wal1.get(), &iter->wal_file());

  // element 1
  ++iter;
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(115u, iter->offset());
  CHECK_EQUAL(0u, iter->wal_offset());
  CHECK_EQUAL(4u, iter->size());
  CHECK_EQUAL(115u+4u, iter->end_offset());
  CHECK_EQUAL(wal2.get(), &iter->wal_file());

  // no more elements
  ++iter;
  CHECK(iter == map.end());
}

TEST(replacement_map_insert_replacement) {
  using wal_file = earnest::detail::byte_positional_stream<asio::io_context::executor_type>;
  using replacement_map = earnest::detail::replacement_map<earnest::detail::byte_positional_stream<asio::io_context::executor_type>>;

  asio::io_context ioctx;
  const auto wal1 = std::make_shared<const wal_file>(ioctx.get_executor());
  const auto wal2 = std::make_shared<const wal_file>(ioctx.get_executor());
  replacement_map map;
  map.insert(15, 16, 7, wal1); // element 0
  map.insert(15, 0, 7, wal2); // replaces first element

  CHECK(!map.empty());

  // only contains replacement
  auto iter = map.begin();
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(15u, iter->offset());
  CHECK_EQUAL(0u, iter->wal_offset());
  CHECK_EQUAL(7u, iter->size());
  CHECK_EQUAL(15u+7u, iter->end_offset());
  CHECK_EQUAL(wal2.get(), &iter->wal_file());

  // no more elements
  ++iter;
  CHECK(iter == map.end());
}

TEST(replacement_map_insert_big_replacement) {
  using wal_file = earnest::detail::byte_positional_stream<asio::io_context::executor_type>;
  using replacement_map = earnest::detail::replacement_map<earnest::detail::byte_positional_stream<asio::io_context::executor_type>>;

  asio::io_context ioctx;
  const auto wal1 = std::make_shared<const wal_file>(ioctx.get_executor());
  const auto wal2 = std::make_shared<const wal_file>(ioctx.get_executor());
  replacement_map map;
  map.insert(15, 16, 7, wal1); // element 0
  map.insert(10, 0, 17, wal2); // replaces first element

  CHECK(!map.empty());

  // only contains replacement
  auto iter = map.begin();
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(10u, iter->offset());
  CHECK_EQUAL(0u, iter->wal_offset());
  CHECK_EQUAL(17u, iter->size());
  CHECK_EQUAL(10u+17u, iter->end_offset());
  CHECK_EQUAL(wal2.get(), &iter->wal_file());

  // no more elements
  ++iter;
  CHECK(iter == map.end());
}

TEST(replacement_map_insert_small_replacement) {
  using wal_file = earnest::detail::byte_positional_stream<asio::io_context::executor_type>;
  using replacement_map = earnest::detail::replacement_map<earnest::detail::byte_positional_stream<asio::io_context::executor_type>>;

  asio::io_context ioctx;
  const auto wal1 = std::make_shared<const wal_file>(ioctx.get_executor());
  const auto wal2 = std::make_shared<const wal_file>(ioctx.get_executor());
  replacement_map map;
  map.insert(10, 0, 17, wal1); // replaces first element
  map.insert(15, 16, 7, wal2); // overwrites the middle of the first element

  CHECK(!map.empty());

  // element 1, containing the begin of the first element
  auto iter = map.begin();
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(10u, iter->offset());
  CHECK_EQUAL(0u, iter->wal_offset());
  CHECK_EQUAL(5u, iter->size());
  CHECK_EQUAL(10u+5u, iter->end_offset());
  CHECK_EQUAL(wal1.get(), &iter->wal_file());

  // element 2, containing the overwriting element
  ++iter;
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(15u, iter->offset());
  CHECK_EQUAL(16u, iter->wal_offset());
  CHECK_EQUAL(7u, iter->size());
  CHECK_EQUAL(15u+7u, iter->end_offset());
  CHECK_EQUAL(wal2.get(), &iter->wal_file());

  // element 3, containing the end of the first element
  ++iter;
  REQUIRE CHECK(iter != map.end());
  CHECK_EQUAL(22u, iter->offset());
  CHECK_EQUAL(12u, iter->wal_offset());
  CHECK_EQUAL(5u, iter->size());
  CHECK_EQUAL(22u+5u, iter->end_offset());
  CHECK_EQUAL(wal1.get(), &iter->wal_file());

  // no more elements
  ++iter;
  CHECK(iter == map.end());
}

int main() {
  return UnitTest::RunAllTests();
}
