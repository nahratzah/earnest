#pragma once

#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <utility>
#include <variant>
#include <vector>

#include <earnest/detail/completion_handler_fun.h>
#include <earnest/detail/move_only_function.h>

#include <asio/async_result.hpp>

namespace earnest::detail {


template<typename Executor, typename Allocator = std::allocator<std::byte>>
class monitor {
  public:
  using executor_type = Executor;
  using allocator_type = Allocator;

  private:
  class state;

  public:
  class exclusive_lock {
    friend class monitor::state;

    private:
    explicit exclusive_lock(std::shared_ptr<state> state) noexcept
    : state_(state)
    {}

    public:
    exclusive_lock() noexcept = default;

    exclusive_lock(const exclusive_lock& y) noexcept
    : state_(y.state_)
    {
      if (state_) state_->inc_lock();
    }

    exclusive_lock(exclusive_lock&& y) noexcept
    : state_(std::move(y.state_))
    {}

    auto operator=(const exclusive_lock& y) noexcept -> exclusive_lock& {
      using std::swap;
      auto copy = y;
      swap(state_, copy.state_);
      return *this;
    }

    auto operator=(exclusive_lock&& y) noexcept -> exclusive_lock& {
      using std::swap;
      auto copy = std::move(y);
      swap(state_, copy.state_);
      return *this;
    }

    ~exclusive_lock() {
      if (state_) state_->unlock();
    }

    auto reset() {
      if (state_) {
        state_->unlock();
        state_.reset();
      }
    }

    auto is_locked() const noexcept -> bool {
      return state_ != nullptr;
    }

    explicit operator bool() const noexcept {
      return is_locked();
    }

    auto operator!() const noexcept -> bool {
      return !is_locked();
    }

    private:
    std::shared_ptr<state> state_;
  };

  class shared_lock {
    friend class monitor::state;

    private:
    explicit shared_lock(std::shared_ptr<state> state) noexcept
    : state_(state)
    {}

    public:
    shared_lock() noexcept = default;

    shared_lock(const shared_lock& y) noexcept
    : state_(y.state_)
    {
      if (state_) state_->inc_lock();
    }

    shared_lock(shared_lock&& y) noexcept
    : state_(std::move(y.state_))
    {}

    auto operator=(const shared_lock& y) noexcept -> shared_lock& {
      using std::swap;
      auto copy = y;
      swap(state_, copy.state_);
      return *this;
    }

    auto operator=(shared_lock&& y) noexcept -> shared_lock& {
      using std::swap;
      auto copy = std::move(y);
      swap(state_, copy.state_);
      return *this;
    }

    ~shared_lock() {
      if (state_) state_->unlock();
    }

    auto reset() {
      if (state_) {
        state_->unlock();
        state_.reset();
      }
    }

    auto is_locked() const noexcept -> bool {
      return state_ != nullptr;
    }

    explicit operator bool() const noexcept {
      return is_locked();
    }

    auto operator!() const noexcept -> bool {
      return !is_locked();
    }

    private:
    std::shared_ptr<state> state_;
  };

  private:
  using exclusive_function = move_only_function<void(exclusive_lock, bool)>;
  using shared_function = move_only_function<void(shared_lock, bool)>;

  public:
  explicit monitor(executor_type ex, allocator_type alloc = allocator_type())
  : state_(std::allocate_shared<state>(alloc, std::move(ex), alloc))
  {}

  template<typename CompletionToken>
  auto async_shared(CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(shared_lock)>(
        [](auto handler, std::shared_ptr<state> state_) {
          state_->add(
              shared_function(
                  completion_wrapper<void(shared_lock, bool)>(
                      completion_handler_fun(std::move(handler), state_->get_executor()),
                      [](auto handler, shared_lock lock, [[maybe_unused]] bool immediate) {
                        std::invoke(handler, std::move(lock));
                      })));
        },
        token, state_);
  }

  template<typename CompletionToken>
  auto async_exclusive(CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(exclusive_lock)>(
        [](auto handler, std::shared_ptr<state> state_) {
          state_->add(
              exclusive_function(
                  completion_wrapper<void(exclusive_lock, bool)>(
                      completion_handler_fun(std::move(handler), state_->get_executor()),
                      [](auto handler, exclusive_lock lock, [[maybe_unused]] bool immediate) {
                        std::invoke(handler, std::move(lock));
                      })));
        },
        token, state_);
  }

  template<typename CompletionToken>
  auto dispatch_shared(CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(shared_lock)>(
        [](auto handler, std::shared_ptr<state> state_) {
          state_->add(
              shared_function(
                  completion_wrapper<void(shared_lock, bool)>(
                      completion_handler_fun(std::move(handler), state_->get_executor()),
                      [](auto handler, shared_lock lock, bool immediate) {
                        if (immediate)
                          std::invoke(handler.inner_handler(), std::move(lock));
                        else
                          std::invoke(handler, std::move(lock));
                      })));
        },
        token, state_);
  }

  template<typename CompletionToken>
  auto dispatch_exclusive(CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(exclusive_lock)>(
        [](auto handler, std::shared_ptr<state> state_) {
          state_->add(
              exclusive_function(
                  completion_wrapper<void(exclusive_lock, bool)>(
                      completion_handler_fun(std::move(handler), state_->get_executor()),
                      [](auto handler, exclusive_lock lock, bool immediate) {
                        if (immediate)
                          std::invoke(handler.inner_handler(), std::move(lock));
                        else
                          std::invoke(handler, std::move(lock));
                      })));
        },
        token, state_);
  }

  auto get_executor() const -> executor_type {
    return state_->get_executor();
  }

  auto get_allocator() const -> allocator_type {
    return state_->get_allocator();
  }

  private:
  std::shared_ptr<state> state_;
};


template<typename Executor, typename Allocator>
class monitor<Executor, Allocator>::state
: public std::enable_shared_from_this<monitor<Executor, Allocator>::state>
{
  private:
  enum class state_t {
    idle,
    shared,
    waiting,
  };

  using shared_fn_list = std::vector<shared_function, typename std::allocator_traits<allocator_type>::template rebind_alloc<shared_function>>;
  using queue_element = std::variant<exclusive_function, shared_fn_list*>;
  using queue_type = std::queue<
      queue_element,
      std::deque<queue_element, typename std::allocator_traits<allocator_type>::template rebind_alloc<queue_element>>>;

  public:
  state(executor_type ex, allocator_type alloc)
  : ex_(std::move(ex)),
    alloc_(alloc),
    sh_fn_(alloc),
    q_(alloc)
  {}

  auto get_executor() const -> executor_type {
    return ex_;
  }

  auto get_allocator() const -> allocator_type {
    return alloc_;
  }

  auto add(shared_function&& fn) -> void {
    std::unique_lock lck{mtx_};
    if (s_ == state_t::idle) {
      assert(lock_count_ == 0);
      auto mon_lck = shared_lock(this->shared_from_this());
      ++lock_count_;
      s_ = state_t::shared;
      lck.unlock();
      std::invoke(fn, std::move(mon_lck), true);
    } else if (s_ == state_t::shared) {
      assert(lock_count_ > 0);
      assert(q_.empty());
      auto mon_lck = shared_lock(this->shared_from_this());
      ++lock_count_;
      lck.unlock();
      std::invoke(fn, std::move(mon_lck), true);
    } else {
      assert(lock_count_ > 0);
      sh_fn_.push_back(std::move(fn));
      if (!shared_is_enqueued_) {
        try {
          q_.push(&sh_fn_);
        } catch (...) {
          sh_fn_.pop_back(); // undo, so we maintain strong exception guarantee
          throw;
        }
        shared_is_enqueued_ = true;
      }
    }
  }

  auto add(exclusive_function&& fn) -> void {
    std::unique_lock lck{mtx_};
    if (s_ == state_t::idle) {
      assert(lock_count_ == 0);
      auto mon_lck = exclusive_lock(this->shared_from_this());
      ++lock_count_;
      s_ = state_t::waiting;
      lck.unlock();
      std::invoke(fn, std::move(mon_lck), true);
    } else {
      assert(lock_count_ > 0);
      q_.push(std::move(fn));
      s_ = state_t::waiting;
    }
  }

  auto unlock() {
    std::unique_lock lck{mtx_};
    assert(lock_count_ > 0);
    if (--lock_count_ != 0) return;

    assert(lock_count_ == 0);
    if (q_.empty()) {
      s_ = state_t::idle;
    } else if (std::holds_alternative<exclusive_function>(q_.front())) {
      auto mon_lck = exclusive_lock(this->shared_from_this());
      ++lock_count_;
      s_ = state_t::waiting;
      auto fn = std::get<exclusive_function>(std::move(q_.front()));
      q_.pop();
      lck.unlock();
      std::invoke(fn, std::move(mon_lck), false);
    } else {
      auto mon_lck = shared_lock(this->shared_from_this());
      ++lock_count_;
      s_ = state_t::waiting;
      auto functions = std::move(sh_fn_);
      q_.pop();
      shared_is_enqueued_ = false;
      if (q_.empty()) s_ = state_t::shared;
      lck.unlock();
      for (auto& fn : functions) std::invoke(fn, std::move(mon_lck), false);
    }
  }

  auto inc_lock() {
    std::lock_guard lck{mtx_};
    assert(lock_count_ > 0);
    ++lock_count_;
  }

  private:
  executor_type ex_;
  allocator_type alloc_;
  std::mutex mtx_;
  state_t s_ = state_t::idle;
  bool shared_is_enqueued_ = false;
  shared_fn_list sh_fn_;
  queue_type q_;
  std::size_t lock_count_ = 0;
};


} /* namespace earnest::detail */
