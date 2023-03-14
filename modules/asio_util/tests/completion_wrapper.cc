#include <earnest/detail/completion_wrapper.h>

#include <UnitTest++/UnitTest++.h>

#include <asio/io_context.hpp>
#include <asio/strand.hpp>
#include <asio/associated_executor.hpp>
#include <asio/associated_allocator.hpp>

using earnest::detail::completion_wrapper;

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

TEST(propagate_correct_bindings) {
  asio::io_context ioctx;

  auto executing_strand = asio::make_strand(ioctx);

  struct functor {
    using allocator_type = mock_allocator<std::byte>;
    using executor_type = decltype(executing_strand);

    auto get_allocator() const -> allocator_type { return alloc_; }
    auto get_executor() const -> executor_type { return ex_; }
    auto operator()() const {} // noop functor

    allocator_type alloc_;
    executor_type ex_;
  };

  auto cw = completion_wrapper<void()>(
      functor{
        .alloc_ = mock_allocator<std::byte>(17),
        .ex_ = executing_strand
      },
      [](auto handler) {
        handler();
      });

  CHECK(asio::get_associated_executor(cw, ioctx.get_executor()) == executing_strand);
  CHECK(asio::get_associated_allocator(cw) == mock_allocator<std::byte>(17));
}

int main() {
  return UnitTest::RunAllTests();
}
