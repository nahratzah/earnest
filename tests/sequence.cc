#include "UnitTest++/UnitTest++.h"
#include <cstddef>
#include <iostream>
#include <earnest/sequence.h>
#include <earnest/sequence.h>
#include <earnest/fd.h>
#include <boost/asio/io_context.hpp>

using earnest::txfile;
using earnest::sequence;

constexpr std::size_t WAL_SIZE = 4u << 20;

auto tmpfile_(earnest::fd::executor_type x, std::string file) {
  earnest::fd f(x);
  f.tmpfile(file);
  return f;
}

#define TMPFILE(x) tmpfile_((x), __FILE__)

TEST(sequence) {
  boost::asio::io_context io_context;

  auto f = txfile::create(__func__, TMPFILE(io_context.get_executor()), 0, WAL_SIZE);
  {
    auto tx = f.begin(false);
    tx.resize(sequence::SIZE);
    sequence::init(tx, 0, 17);
    tx.commit();
  }

  {
    auto s = sequence(f, 0);
    CHECK_EQUAL(17u, s()); // Sequence is initialized to 17.
    CHECK_EQUAL(18u, s()); // Sequence increases.
    CHECK_EQUAL(19u, s()); // Sequence increases.
  }
  {
    auto s = sequence(f, 0);
    CHECK_EQUAL(20u, s()); // Sequence can be re-opened.
    CHECK_EQUAL(21u, s()); // Sequence still increases.
  }
}

TEST(sequence_cache) {
  boost::asio::io_context io_context;

  auto f = txfile::create(__func__, TMPFILE(io_context.get_executor()), 0, WAL_SIZE);
  {
    auto tx = f.begin(false);
    tx.resize(sequence::SIZE);
    sequence::init(tx, 0);
    tx.commit();
  }

  {
    auto s = sequence(f, 0, 1000);
    CHECK_EQUAL(0u, s()); // Sequence is initialized to 17.
    CHECK_EQUAL(1u, s()); // Sequence increases.
    CHECK_EQUAL(2u, s()); // Sequence increases.
  }
  {
    auto s = sequence(f, 0);
    CHECK_EQUAL(1000u, s()); // Sequence can be re-opened.
    CHECK_EQUAL(1001u, s()); // Sequence still increases.
  }
}

TEST(reject_bad_checksum) {
  boost::asio::io_context io_context;

  auto f = txfile::create(__func__, TMPFILE(io_context.get_executor()), 0, WAL_SIZE);
  {
    auto tx = f.begin(false);
    tx.resize(sequence::SIZE);
    for (unsigned int i = 0; i < 8u; ++i)
      tx.write_at(i, "a", 1);
    tx.commit();
  }

  bool did_throw = false;
  try {
    auto s = sequence(f, 0);
  } catch (const std::exception& e) {
    std::cerr << "exception " << e.what() << std::endl;
    did_throw = true;
  }
  CHECK(did_throw);
}

int main() {
  return UnitTest::RunAllTests();
}
