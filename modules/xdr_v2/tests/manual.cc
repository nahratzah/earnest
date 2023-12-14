#include <earnest/xdr_v2.h>

#include <UnitTest++/UnitTest++.h>

#include <span>
#include <earnest/execution_io.h>
#include "buffer.h"

using earnest::execution::just;
using earnest::execution::then;
using earnest::execution::sync_wait;
namespace io = earnest::execution::io;
using namespace earnest::xdr_v2;

SUITE(manual) {

TEST(read) {
  std::string tmp(4, 'x');
  auto [result] = sync_wait(
      just(buffer({'a', 'b', 'c', 'd', 'x'}))
      | manual.read(
          [&tmp](auto& fd) {
            CHECK_EQUAL(buffer({ 'a', 'b', 'c', 'd', 'x' }), fd);
            return io::read(fd, std::as_writable_bytes(std::span<char, 4>(tmp.data(), tmp.size())))
            | then(
                [&fd]([[maybe_unused]] std::size_t rlen) {
                  return std::move(fd);
                });
          }).sender_chain()).value();
  CHECK_EQUAL(buffer({'x'}), result);
  CHECK_EQUAL(std::string("abcd"), tmp);
}

TEST(write) {
  auto [result] = sync_wait(
      just(buffer())
      | manual.write(
          [](auto& fd) {
            return io::write(fd, std::as_bytes(std::span<const char, 4>("abcd", 4)))
            | then(
                [&fd]([[maybe_unused]] std::size_t wlen) {
                  return std::move(fd);
                });
          }).sender_chain()).value();
  CHECK_EQUAL(buffer({ 'a', 'b', 'c', 'd' }), result);
}

}
