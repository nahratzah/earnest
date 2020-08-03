#include "print.h"
#include "UnitTest++/UnitTest++.h"
#include <earnest/detail/commit_manager.h>
#include <earnest/detail/commit_manager_impl.h>
#include <earnest/txfile.h>
#include <cstddef>
#include <system_error>
#include <vector>
#include <boost/endian/conversion.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/completion_condition.hpp>
#include <boost/asio/read_at.hpp>
#include <boost/asio/write_at.hpp>
#include <boost/asio/io_context.hpp>


using earnest::detail::commit_manager;
using earnest::detail::commit_manager_impl;
using earnest::fd;

namespace {

auto read(const earnest::txfile::transaction& tx) -> std::vector<std::uint8_t> {
  constexpr std::size_t GROWTH = 8192;

  std::vector<std::uint8_t> v;
  auto buf = boost::asio::dynamic_buffer(v);

  boost::system::error_code ec;
  while (!ec) {
    const auto old_size = buf.size();
    buf.grow(GROWTH);

    const auto rlen = boost::asio::read_at(tx, old_size, buf.data(old_size, GROWTH), ec);
    buf.shrink(GROWTH - rlen);
  }

  if (ec == boost::asio::stream_errc::eof) return v;

  throw boost::system::system_error(ec, "tx read");
}

auto read(const earnest::txfile& f) -> std::vector<std::uint8_t> {
  return read(f.begin());
}

void check_file_equals(const std::vector<std::uint8_t>& expect, const earnest::txfile& fd) {
  const std::vector<std::uint8_t> fd_v = read(fd);
  CHECK_EQUAL(expect.size(), fd_v.size());
  if (expect.size() == fd_v.size())
    CHECK_ARRAY_EQUAL(expect.data(), fd_v.data(), expect.size());
}

auto tmp_txfile(fd::executor_type x, std::string name) -> earnest::txfile {
  fd f(x);
  f.tmpfile(__FILE__);
  return earnest::txfile::create(name, std::move(f), 0, 4u << 20);
}
#define tmp_txfile(x) tmp_txfile((x), __func__)

auto file_with_inits(
    earnest::txfile&& f,
    commit_manager::type tx_start,
    commit_manager::type last_write,
    commit_manager::type completed_commit) -> earnest::txfile&& {
  std::uint32_t magic = commit_manager_impl::magic;

  boost::endian::native_to_big_inplace(magic);
  boost::endian::native_to_big_inplace(tx_start);
  boost::endian::native_to_big_inplace(last_write);
  boost::endian::native_to_big_inplace(completed_commit);

  auto t = f.begin(false);
  t.resize(16);
  boost::asio::write_at(t,  0, boost::asio::buffer(&magic, sizeof(magic)), boost::asio::transfer_all());
  boost::asio::write_at(t,  4, boost::asio::buffer(&tx_start, sizeof(tx_start)), boost::asio::transfer_all());
  boost::asio::write_at(t,  8, boost::asio::buffer(&last_write, sizeof(last_write)), boost::asio::transfer_all());
  boost::asio::write_at(t, 12, boost::asio::buffer(&completed_commit, sizeof(completed_commit)), boost::asio::transfer_all());
  t.commit();

  return std::move(f);
}

} /* namespace <unnamed> */


TEST(new_file) {
  boost::asio::io_context io_context;

  auto f = tmp_txfile(io_context.get_executor());
  {
    auto t = f.begin(false);
    t.resize(commit_manager_impl::SIZE);
    commit_manager_impl::init(t, 0);
    t.commit();
  }

  check_file_equals(
      {
        0x69, 0x7f, 0x64, 0x31,
        0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
      },
      f);
}

TEST(commit_manager_knows_of_commit_manager_impl) {
  boost::asio::io_context io_context;

  auto f = file_with_inits(tmp_txfile(io_context.get_executor()), 1, 17, 15);
  auto cm = commit_manager::allocate(f, 0);

  REQUIRE CHECK(cm != nullptr);
}

TEST(get_tx_commit_id) {
  boost::asio::io_context io_context;

  auto f = file_with_inits(tmp_txfile(io_context.get_executor()), 1, 17, 15);
  auto cm = commit_manager_impl::allocate(f, 0);
  REQUIRE CHECK(cm != nullptr);
  auto ci = cm->get_tx_commit_id();

  CHECK_EQUAL( 1, ci.tx_start());
  CHECK_EQUAL(15, ci.val());
}

TEST(get_tx_commit_id_repeats) {
  boost::asio::io_context io_context;

  auto f = file_with_inits(tmp_txfile(io_context.get_executor()), 1, 17, 15);
  auto cm = commit_manager_impl::allocate(f, 0);
  REQUIRE CHECK(cm != nullptr);

  auto ci1 = cm->get_tx_commit_id();
  auto ci2 = cm->get_tx_commit_id();

  CHECK_EQUAL(ci1, ci2);
}

TEST(prepare_commit) {
  boost::asio::io_context io_context;

  auto f = file_with_inits(tmp_txfile(io_context.get_executor()), 1, 17, 15);
  auto cm = commit_manager_impl::allocate(f, 0);
  REQUIRE CHECK(cm != nullptr);
  const auto ci_before = cm->get_tx_commit_id();

  auto wi = cm->prepare_commit(f);
  CHECK_EQUAL( 1, wi.seq().tx_start());
  CHECK_EQUAL(18, wi.seq().val());

  CHECK_EQUAL(ci_before, cm->get_tx_commit_id()); // Doesn't change read ID.

  CHECK(wi.seq() != cm->prepare_commit(f).seq()); // Doesn't hand out the same ID twice.
}

TEST(commit) {
  boost::asio::io_context io_context;

  auto f = file_with_inits(tmp_txfile(io_context.get_executor()), 1, 17, 15);
  auto cm = commit_manager_impl::allocate(f, 0);
  REQUIRE CHECK(cm != nullptr);

  auto wi = cm->prepare_commit(f);
  REQUIRE CHECK_EQUAL( 1, wi.seq().tx_start());
  REQUIRE CHECK_EQUAL(18, wi.seq().val());

  int validation_called = 0;
  int phase2_called = 0;
  auto ec = wi.apply(
      [&validation_called, &phase2_called]() -> std::error_code {
        CHECK_EQUAL(0, validation_called);
        CHECK_EQUAL(0, phase2_called);
        ++validation_called;
        return {};
      },
      [&validation_called, &phase2_called]() noexcept {
        CHECK_EQUAL(1, validation_called);
        CHECK_EQUAL(0, phase2_called);
        ++phase2_called;
      });
  // Apply function returned the error code.
  CHECK_EQUAL(std::error_code(), ec);
  CHECK_EQUAL(1, validation_called);
  CHECK_EQUAL(1, phase2_called);

  // New read transactions now use the commited transaction.
  auto cid_after_commit = cm->get_tx_commit_id();
  CHECK_EQUAL( 1, wi.seq().tx_start());
  CHECK_EQUAL(18, wi.seq().val());
}

TEST(failed_commit) {
  boost::asio::io_context io_context;

  auto f = file_with_inits(tmp_txfile(io_context.get_executor()), 1, 17, 15);
  auto cm = commit_manager_impl::allocate(f, 0);
  REQUIRE CHECK(cm != nullptr);
  const auto ci_before = cm->get_tx_commit_id();

  auto wi = cm->prepare_commit(f);
  REQUIRE CHECK_EQUAL( 1, wi.seq().tx_start());
  REQUIRE CHECK_EQUAL(18, wi.seq().val());

  int validation_called = 0;
  int phase2_called = 0;
  auto ec = wi.apply(
      [&validation_called, &phase2_called]() -> std::error_code {
        CHECK_EQUAL(0, validation_called);
        CHECK_EQUAL(0, phase2_called);
        ++validation_called;
        return std::make_error_code(std::errc::no_link);
      },
      [&validation_called, &phase2_called]() noexcept {
        CHECK_EQUAL(1, validation_called);
        CHECK_EQUAL(0, phase2_called);
        ++phase2_called;
      });
  // Apply function returned the error code.
  CHECK_EQUAL(std::make_error_code(std::errc::no_link), ec);
  CHECK_EQUAL(1, validation_called);
  CHECK_EQUAL(0, phase2_called);

  // Canceled transaction doesn't affect read transactions.
  CHECK_EQUAL(ci_before, cm->get_tx_commit_id());
}

int main() {
  return UnitTest::RunAllTests();
}
