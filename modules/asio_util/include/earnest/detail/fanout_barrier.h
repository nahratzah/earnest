#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <utility>

#include <asio/associated_allocator.hpp>
#include <asio/associated_executor.hpp>
#include <asio/async_result.hpp>
#include <asio/executor_work_guard.hpp>

#include <earnest/detail/completion_handler_fun.h>
#include <earnest/detail/move_only_function.h>

namespace earnest::detail {


template<typename Executor, typename Allocator>
class fanout_barrier {
  public:
  using executor_type = Executor;
  using allocator_type = Allocator;

  private:
  class impl
  : public std::enable_shared_from_this<impl>
  {
    public:
    using function_type = move_only_function<void(std::error_code)>;

    impl(executor_type ex, allocator_type alloc)
    : ex_(std::move(ex)),
      queued_(std::move(alloc))
    {}

    auto get_executor() const -> executor_type { return ex_; }
    auto get_allocator() const -> allocator_type { return queued_.get_allocator(); }

    auto ready() const noexcept -> bool {
      std::scoped_lock lck{mtx_};
      return ready_;
    }

    auto update(std::error_code ec) -> void {
      std::unique_lock lck{mtx_};
      if (await_ == 0) [[unlikely]] throw std::logic_error("fanout_barrier: too many invocations");

      // Memorize the first failure (and discard further failures).
      if (!ec_) ec_ = ec;
      --await_;

      // When all completions have arrived, invoke all handlers.
      if (await_ == 0) {
        ec = ec_;
        lck.unlock();

        std::for_each(queued_.begin(), queued_.end(),
            [ec](function_type& fn) {
              std::invoke(fn, ec);
            });
        queued_.clear();
      }
    }

    template<typename CompletionToken>
    auto async_on_ready(CompletionToken&& token) {
      return asio::async_initiate<CompletionToken, void(std::error_code)>(
          [](auto completion_handler, std::shared_ptr<impl> self) {
            std::unique_lock lck{self->mtx_};
            if (self->await_ == 0) { // Already completed, so we'll just post the callback immediately.
              lck.unlock();
              auto ex = asio::get_associated_executor(completion_handler, self->get_executor());
              auto alloc = asio::get_associated_allocator(completion_handler);
              ex.post(
                  [completion_handler=std::move(completion_handler), ec=self->ec_]() mutable {
                    std::invoke(completion_handler, ec);
                  },
                  std::move(alloc));
              return;
            }

            self->queued_.emplace_back(completion_handler_fun(std::move(completion_handler), self->get_executor()));
          },
          token, this->shared_from_this());
    }

    auto inc(std::size_t n = 1) -> void {
      std::scoped_lock lck{mtx_};
      if (await_ == 0) [[unlikely]]
        throw std::logic_error("cannot raise barrier when it has reached level 0");
      if (n > std::numeric_limits<std::size_t>::max() - await_) [[unlikely]]
        throw std::overflow_error("too many barriers");
      await_ += n;
    }

    private:
    mutable std::mutex mtx_;
    executor_type ex_;
    std::vector<function_type, typename std::allocator_traits<allocator_type>::template rebind_alloc<function_type>> queued_;
    std::error_code ec_;
    std::size_t await_ = 1;
    bool ready_ = false;
  };

  public:
  explicit fanout_barrier(executor_type ex, allocator_type alloc = allocator_type())
  : impl_(std::allocate_shared<impl>(alloc, std::move(ex), alloc))
  {}

  auto get_executor() const -> executor_type { return impl_->get_executor(); }
  auto get_allocator() const -> allocator_type { return impl_->get_allocator(); }
  auto ready() const noexcept -> bool { return impl_->ready(); }

  auto operator++() -> fanout_barrier& {
    impl_->inc();
    return *this;
  }

  auto operator+=(std::size_t n) -> fanout_barrier& {
    impl_->inc(n);
    return *this;
  }

  auto operator()(std::error_code ec) {
    impl_->update(ec);
  }

  template<typename CompletionToken>
  auto async_on_ready(CompletionToken&& token) {
    return impl_->async_on_ready(std::forward<CompletionToken>(token));
  }

  private:
  std::shared_ptr<impl> impl_;
};


} /* namespace earnest::detail */
