#include "print.h"
#include "UnitTest++/UnitTest++.h"
#include <earnest/detail/wal.h>
#include <earnest/fd.h>
#include <stdexcept>
#include <string_view>
#include <boost/asio/io_context.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/completion_condition.hpp>
#include <boost/asio/write_at.hpp>
#include <boost/asio/read_at.hpp>

using earnest::detail::wal_region;

namespace {

template<typename FD>
auto set_file_contents(FD&& fd, std::vector<std::uint8_t> v) -> FD&& {
  boost::asio::write_at(fd, 0, boost::asio::buffer(v), boost::asio::transfer_all());
  return std::forward<FD>(fd);
}

auto file_contents(earnest::fd& file) -> std::vector<std::uint8_t> {
  std::vector<std::uint8_t> v;
  if (file.size() > v.max_size()) throw std::length_error("raw file too large for vector");
  v.resize(file.size());

  boost::asio::read_at(file, 0, boost::asio::buffer(v), boost::asio::transfer_all());
  return v;
}

auto file_contents(wal_region& wal) -> std::vector<std::uint8_t> {
  std::vector<std::uint8_t> v;
  if (wal.size() > v.max_size()) throw std::length_error("WAL file too large for vector");
  v.resize(wal.size());

  boost::asio::read_at(wal, 0, boost::asio::buffer(v), boost::asio::transfer_all());
  return v;
}

auto file_contents(wal_region::tx& tx) -> std::vector<std::uint8_t> {
  std::vector<std::uint8_t> v;
  if (tx.size() > v.max_size()) throw std::length_error("WAL file too large for vector");
  v.resize(tx.size());

  boost::asio::read_at(tx, 0, boost::asio::buffer(v), boost::asio::transfer_all());
  return v;
}

template<typename FD>
void check_file_equals(std::vector<std::uint8_t> expect, FD&& fd, std::size_t off = 0) {
  CHECK_EQUAL(expect.size() + off, fd.size());
  const auto fd_v = file_contents(fd);
  if (expect.size() + off == fd_v.size())
    CHECK_ARRAY_EQUAL(expect.data() + off, fd_v.data() + off, expect.size());
}

template<typename FD>
void check_file_equals(std::string_view expect, FD&& fd, std::size_t off = 0) {
  CHECK_EQUAL(expect.size() + off, fd.size());
  const auto fd_v = file_contents(fd);
  const auto fd_s = std::string_view(reinterpret_cast<const char*>(fd_v.data()), fd_v.size()).substr(off);
  CHECK_EQUAL(expect, fd_s);
}

template<typename FD>
void check_file_empty(FD&& fd) {
  CHECK_EQUAL(0u, fd.size());
  const auto fd_v = file_contents(fd);
  std::uint8_t expect[1];
  CHECK_ARRAY_EQUAL(expect, fd_v.data(), 0);
}

auto tmpfile_(earnest::fd::executor_type x, std::string file) {
  earnest::fd f(x);
  f.tmpfile(file);
  return f;
}

} /* namespace <unnamed> */

#define TMPFILE(x) tmpfile_((x), __FILE__)
#define TMPFILE_WITH_DATA(x, data) set_file_contents(TMPFILE(x), data)

TEST(new_file) {
  boost::asio::io_context io_context;

  auto wal = wal_region("waltest", wal_region::create(), TMPFILE(io_context.get_executor()), 0, 64);

  check_file_equals(
      {
        0u, 0u, 0u, 0u, // seqence number
        0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, // file size
        0u, 0u, 0u, 0u, // end of WAL
        // 16 bytes so far
        // zero padding rest of the segment (16 bytes)
        0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u,
        0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u,
        // Next segment
        0xffu, 0xffu, 0xffu, 0xffu, // seqence number
        0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, // file size
        0u, 0u, 0u, 0u, // end of WAL
        // zero padding rest of the segment (16 bytes)
        0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u,
        0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u
      },
      wal.fd());
  check_file_empty(wal);
}

TEST(existing_file_read_write) {
  boost::asio::io_context io_context;

  auto wal = wal_region(
      "waltest",
      TMPFILE_WITH_DATA(
          io_context.get_executor(),
          std::vector<std::uint8_t>({
              0u, 0u, 0u, 0u, // seqence number
              0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, // file size
              0u, 0u, 0u, 0u, // end of WAL
              // 16 bytes so far
              // padding rest of the segment (16 bytes)
              // we use non-zero padding, to check it does not get rewritten
              17u, 19u, 23u, 29u, 31u, 37u, 41u, 43u,
              17u, 19u, 23u, 29u, 31u, 37u, 41u, 43u,
              // Next segment
              0xffu, 0xffu, 0xffu, 0xffu, // seqence number
              0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, // file size
              0u, 0u, 0u, 0u, // end of WAL
              // zero padding rest of the segment (16 bytes)
              0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u,
              0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u
          })),
      0, 64);

  check_file_equals(
      {
        0u, 0u, 0u, 0u, // seqence number
        0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, // file size
        0u, 0u, 0u, 0u, // end of WAL
        // 16 bytes so far
        // padding rest of the segment (16 bytes)
        17u, 19u, 23u, 29u, 31u, 37u, 41u, 43u,
        17u, 19u, 23u, 29u, 31u, 37u, 41u, 43u,
        // Next segment
        0u, 0u, 0u, 1u, // seqence number
        0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u, // file size
        0u, 0u, 0u, 0u, // end of WAL
        // zero padding rest of the segment (16 bytes)
        0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u,
        0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u
      },
      wal.fd());
  check_file_empty(wal);
}

TEST(resize_and_commit) {
  boost::asio::io_context io_context;

  auto wal = std::make_shared<wal_region>("waltest", wal_region::create(), TMPFILE(io_context.get_executor()), 0, 256);
  auto tx = wal_region::tx(wal);

  check_file_empty(tx);
  tx.resize(3);
  check_file_equals({ 0u, 0u, 0u }, tx);
  tx.commit();

  check_file_equals({ 0u, 0u, 0u }, *wal);
}

TEST(write_and_commit) {
  boost::asio::io_context io_context;

  auto wal = std::make_shared<wal_region>("waltest", wal_region::create(), TMPFILE(io_context.get_executor()), 0, 256);
  auto tx = wal_region::tx(wal);

  tx.resize(8);
  tx.write_at(0, u8"01234567", 8);
  check_file_equals(u8"01234567", tx);
  tx.commit();

  check_file_equals(u8"01234567", *wal);
}

TEST(write_many_and_commit) {
  boost::asio::io_context io_context;

  auto wal = std::make_shared<wal_region>("waltest", wal_region::create(), TMPFILE(io_context.get_executor()), 0, 256);
  auto tx = wal_region::tx(wal);

  tx.resize(32);
  tx.write_at({ 0, 8, 16, 24 }, u8"01234567", 8);
  check_file_equals(u8"01234567012345670123456701234567", tx);
  tx.commit();

  check_file_equals(u8"01234567012345670123456701234567", *wal);
}

TEST(write_but_dont_commit) {
  boost::asio::io_context io_context;

  auto wal = std::make_shared<wal_region>("waltest", wal_region::create(), TMPFILE(io_context.get_executor()), 0, 256);
  auto tx = wal_region::tx(wal);

  tx.resize(8);
  tx.write_at(0, u8"01234567", 8);
  check_file_equals(u8"01234567", tx);

  check_file_equals(u8"", *wal);
}

TEST(write_and_commit_and_compact) {
  boost::asio::io_context io_context;

  auto wal = std::make_shared<wal_region>("waltest", wal_region::create(), TMPFILE(io_context.get_executor()), 0, 256);
  auto tx = wal_region::tx(wal);

  tx.resize(8);
  tx.write_at(0, u8"01234567", 8);
  tx.commit();
  wal->compact();

  check_file_equals(u8"01234567", *wal);
  check_file_equals(u8"01234567", wal->fd(), 256);
}

TEST(write_many_and_commit_and_compact) {
  boost::asio::io_context io_context;

  auto wal = std::make_shared<wal_region>("waltest", wal_region::create(), TMPFILE(io_context.get_executor()), 0, 256);
  auto tx = wal_region::tx(wal);

  tx.resize(32);
  tx.write_at({ 0, 8, 16, 24 }, u8"01234567", 8);
  tx.commit();
  wal->compact();

  check_file_equals(u8"01234567012345670123456701234567", *wal);
  check_file_equals(u8"01234567012345670123456701234567", wal->fd(), 256);
}

TEST(write_and_commit_and_reopen) {
  boost::asio::io_context io_context;

  auto wal = std::make_shared<wal_region>("waltest", wal_region::create(), TMPFILE(io_context.get_executor()), 0, 256);
  auto tx = wal_region::tx(wal);

  tx.resize(8);
  tx.write_at(0, u8"01234567", 8);
  tx.commit();

  wal = std::make_shared<wal_region>("waltest", std::move(*wal).fd(), 0, 256);

  check_file_equals(u8"01234567", *wal);
  check_file_equals(u8"01234567", wal->fd(), 256);
}

TEST(commit_order_matters) {
  boost::asio::io_context io_context;

  auto wal = std::make_shared<wal_region>("waltest", wal_region::create(), TMPFILE(io_context.get_executor()), 0, 256);
  {
    // We need three bytes of space.
    auto r = wal_region::tx(wal);
    r.resize(3);
    r.commit();
  }

  auto tx1 = wal_region::tx(wal);
  tx1.write_at(0, u8"one", 3);

  auto tx2 = wal_region::tx(wal);
  tx2.write_at(0, u8"two", 3);

  tx2.commit(); // writes "two" at the position
  tx1.commit(); // writes "one" at the position

  check_file_equals(u8"one", *wal);
}

TEST(commit_order_matters_on_reopen) {
  boost::asio::io_context io_context;

  auto wal = std::make_shared<wal_region>("waltest", wal_region::create(), TMPFILE(io_context.get_executor()), 0, 256);
  {
    // We need three bytes of space.
    auto r = wal_region::tx(wal);
    r.resize(3);
    r.commit();
  }

  auto tx1 = wal_region::tx(wal);
  tx1.write_at(0, u8"one", 3);

  auto tx2 = wal_region::tx(wal);
  tx2.write_at(0, u8"two", 3);

  tx2.commit(); // writes "two" at the position
  tx1.commit(); // writes "one" at the position

  // Reopen.
  wal = std::make_shared<wal_region>("waltest", std::move(*wal).fd(), 0, 256);
  check_file_equals(u8"one", *wal);
}

int main() {
  return UnitTest::RunAllTests();
}
