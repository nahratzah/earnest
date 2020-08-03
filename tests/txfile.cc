#include "print.h"
#include "UnitTest++/UnitTest++.h"
#include <earnest/txfile.h>
#include <earnest/fd.h>
#include <string>
#include <string_view>
#include <boost/asio/buffer.hpp>
#include <boost/asio/io_context.hpp>

using earnest::txfile;
using earnest::fd;
constexpr std::size_t WAL_SIZE = 4u << 20;

namespace {

auto read(const txfile::transaction& tx) -> std::string {
  constexpr std::size_t GROWTH = 8192;

  std::string s;
  auto buf = boost::asio::dynamic_buffer(s);

  boost::system::error_code ec;
  while (!ec) {
    const auto old_size = buf.size();
    buf.grow(GROWTH);

    const auto rlen = boost::asio::read_at(tx, old_size, buf.data(old_size, GROWTH), ec);
    buf.shrink(GROWTH - rlen);
  }

  if (ec == boost::asio::stream_errc::eof) return s;

  throw boost::system::system_error(ec, "tx read");
}

auto read(const txfile& f) -> std::string {
  return read(f.begin());
}

void write_all_at(txfile::transaction& tx, fd::offset_type off, std::string_view s) {
  auto buf = s.data();
  auto len = s.size();
  while (len > 0) {
    const auto wlen = tx.write_at(off, buf, len);
    buf += wlen;
    len -= wlen;
    off += wlen;
  }
}

auto fd_tmpfile_(fd::executor_type x, const std::string& prefix) -> fd {
  fd f(x);
  f.tmpfile(prefix);
  return f;
}

} /* namespace <unnamed> */

#define TMPFILE(x) fd_tmpfile_(x, __FILE__)

TEST(new_file) {
  boost::asio::io_context io_context;

  auto f = txfile::create(__func__, TMPFILE(io_context.get_executor()), 0, WAL_SIZE);

  CHECK_EQUAL(u8"", read(f));
}

TEST(write_no_commit) {
  boost::asio::io_context io_context;

  auto f = txfile::create(__func__, TMPFILE(io_context.get_executor()), 0, WAL_SIZE);

  auto tx = f.begin(false);
  tx.resize(6);
  write_all_at(tx, 0, u8"foobar");

  CHECK_EQUAL(u8"", read(f));
}

TEST(write_commit) {
  boost::asio::io_context io_context;

  auto f = txfile::create(__func__, TMPFILE(io_context.get_executor()), 0, WAL_SIZE);

  auto tx = f.begin(false);
  tx.resize(6);
  write_all_at(tx, 0, u8"foobar");
  tx.commit();

  CHECK_EQUAL(u8"foobar", read(f));
}

TEST(write_many_commit) {
  boost::asio::io_context io_context;

  auto f = txfile::create(__func__, TMPFILE(io_context.get_executor()), 0, WAL_SIZE);

  auto tx = f.begin(false);
  tx.resize(32);
  tx.write_at_many({ 0, 8, 16, 24 }, u8"_foobar_", 8);
  tx.commit();

  CHECK_EQUAL(u8"_foobar__foobar__foobar__foobar_", read(f));
}

TEST(multi_transaction) {
  boost::asio::io_context io_context;

  auto f = txfile::create(__func__, TMPFILE(io_context.get_executor()), 0, WAL_SIZE);
  {
    auto tx = f.begin(false);
    tx.resize(1);
    write_all_at(tx, 0, u8"X");
    tx.commit();
  }

  auto tx1 = f.begin(false);
  auto tx2 = f.begin(false);
  auto tx3 = f.begin(false);
  auto ro = f.begin();

  write_all_at(tx1, 0, u8"1");
  write_all_at(tx2, 0, u8"2");
  write_all_at(tx3, 0, u8"3");

  CHECK_EQUAL(u8"1", read(tx1));
  CHECK_EQUAL(u8"2", read(tx2));
  CHECK_EQUAL(u8"3", read(tx3));
  CHECK_EQUAL(u8"X", read(ro));

  tx1.commit();
  tx2.commit();
  tx3.commit();
  ro.rollback();

  CHECK_EQUAL(u8"3", read(f));
}

TEST(cant_see_started_before_commited_after_data) {
  boost::asio::io_context io_context;

  auto f = txfile::create(__func__, TMPFILE(io_context.get_executor()), 0, WAL_SIZE);
  {
    auto tx = f.begin(false);
    tx.resize(6);
    write_all_at(tx, 0, u8"XXXXXX");
    tx.commit();
  }

  auto tx1 = f.begin(false);
  write_all_at(tx1, 0, u8"foobar");
  CHECK_EQUAL(u8"foobar", read(tx1));

  auto tx2 = f.begin();
  CHECK_EQUAL(u8"XXXXXX", read(tx2));

  tx1.commit();

  // tx2 doesn't see the commit of tx1.
  CHECK_EQUAL(u8"XXXXXX", read(tx2));
}

int main() {
  return UnitTest::RunAllTests();
}
