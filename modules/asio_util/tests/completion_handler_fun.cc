#include <earnest/detail/completion_handler_fun.h>

#include "UnitTest++/UnitTest++.h"

#include <asio/io_context.hpp>
#include <asio/strand.hpp>
#include <asio/associated_executor.hpp>
#include <asio/associated_allocator.hpp>

using earnest::detail::completion_handler_fun;

template<typename T>
class mock_allocator
: private std::allocator<T>
{
  public:
  explicit mock_allocator(int i) : i(i) {}

  using std::allocator<T>::allocate;
  using std::allocator<T>::deallocate;

  auto operator==(const mock_allocator& y) const noexcept -> bool {
    return i == y.i;
  }

  auto operator!=(const mock_allocator& y) const noexcept -> bool {
    return i != y.i;
  }

  private:
  int i;
};

TEST(has_correct_bindings) {
  asio::io_context ioctx;

  auto executing_strand = asio::make_strand(ioctx);
  auto bound_strand = asio::make_strand(ioctx);

  auto chf = completion_handler_fun(
      []() {}, // noop
      executing_strand,
      bound_strand,
      mock_allocator<std::byte>(17));

  CHECK(asio::get_associated_executor(chf, ioctx.get_executor()) == bound_strand);
  CHECK(asio::get_associated_allocator(chf) == mock_allocator<std::byte>(17));
}

int main() {
  return UnitTest::RunAllTests();
}
