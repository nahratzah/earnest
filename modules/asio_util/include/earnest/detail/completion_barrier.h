#ifndef EARNEST_DETAIL_COMPLETION_BARRIER_H
#define EARNEST_DETAIL_COMPLETION_BARRIER_H

#include <cassert>
#include <cstddef>
#include <limits>
#include <memory>
#include <mutex>
#include <system_error>

#include <asio/associated_allocator.hpp>
#include <asio/associated_executor.hpp>
#include <asio/executor_work_guard.hpp>

#include <earnest/detail/completion_handler_fun.h>
#include <earnest/detail/move_only_function.h>

namespace earnest::detail {


class completion_handler_state_ {
  public:
  completion_handler_state_() = delete;
  completion_handler_state_(const completion_handler_state_&) = delete;
  completion_handler_state_(completion_handler_state_&&) = delete;
  completion_handler_state_& operator=(const completion_handler_state_&) = delete;
  completion_handler_state_& operator=(completion_handler_state_&&) = delete;

  template<typename Handler>
  explicit completion_handler_state_(Handler&& handler)
  : handler_(std::move(handler))
  {}

  void update(std::error_code ec) {
    std::unique_lock<std::mutex> lck(mtx_);
    if (await_ == 0) [[unlikely]] throw std::logic_error("fanout_barrier: too many invocations");

    // Memorize the first failure (and discard further failures).
    if (!ec_) ec_ = ec;
    --await_;

    // When all completions have arrived, invoke the handler.
    if (await_ == 0) {
      ec = ec_;
      lck.unlock();

      std::invoke(handler_, ec);
      handler_ = nullptr;
    }
  }

  void inc(std::size_t n = 1) {
    std::lock_guard<std::mutex> lck(mtx_);
    if (await_ == 0) [[unlikely]]
      throw std::logic_error("cannot raise barrier when it has reached level 0");
    if (n > std::numeric_limits<std::size_t>::max() - await_) [[unlikely]]
      throw std::overflow_error("too many barriers");
    await_ += n;
  }

  private:
  mutable std::mutex mtx_;
  std::error_code ec_;
  std::size_t await_ = 1;
  move_only_function<void(std::error_code)> handler_;
};

template<typename Executor>
class completion_barrier {
  public:
  using executor_type = Executor;

  private:
  using state = completion_handler_state_;

  public:
  template<typename Handler>
  completion_barrier(Handler handler, const executor_type& ex)
  : ex_(ex),
    state_(allocate_state_(std::move(handler), ex_))
  {}

  void operator()(std::error_code ec) {
    assert(state_ != nullptr);
    state_->update(ec);
  }

  auto get_executor() const -> executor_type {
    return ex_;
  }

  ///\brief Increment barrier.
  auto operator++() -> completion_barrier& {
    state_->inc();
    return *this;
  }

  ///\brief Increment barrier.
  auto operator+=(std::size_t n) -> completion_barrier& {
    state_->inc(n);
    return *this;
  }

  private:
  template<typename Handler>
  static auto allocate_state_(Handler&& handler, executor_type& strand) -> std::shared_ptr<state> {
    auto alloc = asio::associated_allocator<Handler>::get(handler);
    return std::allocate_shared<state>(alloc, completion_handler_fun(std::move(handler), strand));
  }

  executor_type ex_;
  std::shared_ptr<state> state_;
};


template<typename Handler, typename Executor>
auto make_completion_barrier(Handler&& handler, Executor&& executor)
-> completion_barrier<std::decay_t<Executor>> {
  return completion_barrier<std::decay_t<Executor>>(std::forward<Handler>(handler), std::forward<Executor>(executor));
}


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_COMPLETION_BARRIER_H */
