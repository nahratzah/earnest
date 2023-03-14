#pragma once

#include <algorithm>
#include <cstddef>
#include <deque>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <queue>
#include <utility>
#include <variant>
#include <vector>

#include <earnest/detail/completion_handler_fun.h>
#include <earnest/detail/move_only_function.h>
#include <earnest/detail/overload.h>

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
  class exclusive_lock;
  class upgrade_lock;
  class shared_lock;

  private:
  using exclusive_function = move_only_function<void(exclusive_lock, bool)>;
  using upgrade_function = move_only_function<void(upgrade_lock, bool)>;
  using shared_function = move_only_function<void(shared_lock, bool)>;

  public:
  explicit monitor(executor_type ex, allocator_type alloc = allocator_type())
  : state_(std::allocate_shared<state>(alloc, std::move(ex), alloc))
  {}

  template<typename CompletionToken> auto async_shared(CompletionToken&& token);
  template<typename CompletionToken> auto async_upgrade(CompletionToken&& token);
  template<typename CompletionToken> auto async_exclusive(CompletionToken&& token);
  template<typename CompletionToken> auto dispatch_shared(CompletionToken&& token);
  template<typename CompletionToken> auto dispatch_upgrade(CompletionToken&& token);
  template<typename CompletionToken> auto dispatch_exclusive(CompletionToken&& token);

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
  using queue_element = std::variant<exclusive_function, upgrade_function>;
  using queue_type = std::queue<
      queue_element,
      std::deque<queue_element, typename std::allocator_traits<allocator_type>::template rebind_alloc<queue_element>>>;
  using pending_upgrades_list = std::vector<exclusive_function, typename std::allocator_traits<allocator_type>::template rebind_alloc<exclusive_function>>;

  public:
  state(executor_type ex, allocator_type alloc)
  : ex_(std::move(ex)),
    alloc_(alloc),
    shq_(alloc),
    wq_(alloc),
    pq_(alloc)
  {}

  state() = delete;
  state(const state&) = delete;
  state(state&&) = delete;
  state& operator=(const state&) = delete;
  state& operator=(state&&) = delete;

  auto get_executor() const -> executor_type { return ex_; }
  auto get_allocator() const -> allocator_type { return alloc_; }

  auto add(shared_function&& fn) -> void {
    std::unique_lock lck{mtx_};
    if (exlocks == 0 && !want_exclusive() && pq_.empty()) {
      assert(shlocks < std::numeric_limits<std::remove_cvref_t<decltype(shlocks)>>::max());
      ++shlocks;
      lck.unlock();
      std::invoke(fn, shared_lock(this->shared_from_this()), true);
      return;
    }

    shq_.emplace_back(std::move(fn));
  }

  auto add(upgrade_function&& fn) -> void {
    std::unique_lock lck{mtx_};
    if (exlocks == 0 && uplocks == 0 && !want_exclusive()) {
      assert(uplocks < std::numeric_limits<std::remove_cvref_t<decltype(shlocks)>>::max());
      ++uplocks;
      lck.unlock();
      std::invoke(fn, upgrade_lock(this->shared_from_this()), true);
      return;
    }

    wq_.emplace(std::move(fn));
  }

  auto add(exclusive_function&& fn) -> void {
    std::unique_lock lck{mtx_};
    if (exlocks == 0 && uplocks == 0 && shlocks == 0) {
      assert(wq_.empty());
      assert(exlocks < std::numeric_limits<std::remove_cvref_t<decltype(shlocks)>>::max());
      ++exlocks;

      lck.unlock();
      std::invoke(fn, exclusive_lock(this->shared_from_this()), true);
      return;
    }

    wq_.emplace(std::move(fn));
  }

  auto unlock_shared() -> void {
    std::unique_lock lck{mtx_};
    assert(shlocks > 0);
    --shlocks;
    if (shlocks > 0) return;

    if (!pq_.empty()) {
      assert(uplocks >= pq_.size());
      assert(exlocks < std::numeric_limits<std::remove_cvref_t<decltype(shlocks)>>::max() - pq_.size());
      uplocks -= pq_.size();
      exlocks += pq_.size();
      auto pending = std::move(pq_);
      lck.unlock();

      std::for_each(
          std::make_move_iterator(pending.begin()), std::make_move_iterator(pending.end()),
          [this](exclusive_function&& invocation) {
            std::invoke(invocation, exclusive_lock(this->shared_from_this()), false);
          });
    } else if (!wq_.empty()) {
      auto wq_front = std::move(wq_.front());
      wq_.pop();
      std::visit(
          overload(
              [this, &lck](exclusive_function&& fn) {
                assert(this->exlocks == 0 && this->uplocks == 0 && this->shlocks == 0);
                assert(this->exlocks < std::numeric_limits<std::remove_cvref_t<decltype(shlocks)>>::max());
                ++this->exlocks;
                lck.unlock();

                std::invoke(fn, exclusive_lock(this->shared_from_this()), false);
              },
              [this, &lck](upgrade_function&& fn) {
                assert(exlocks == 0);
                assert(uplocks < std::numeric_limits<std::remove_cvref_t<decltype(shlocks)>>::max());
                ++uplocks;
                lck.unlock();

                std::invoke(fn, upgrade_lock(this->shared_from_this()), false);
              }),
          std::move(wq_front));
    }
  }

  auto unlock_upgrade() -> void {
    std::unique_lock lck{mtx_};
    assert(uplocks > 0);
    --uplocks;
    if (uplocks > 0 || exlocks > 0 || wq_.empty()) return;

    if (std::holds_alternative<upgrade_function>(wq_.front())) {
      assert(exlocks == 0);
      assert(uplocks < std::numeric_limits<std::remove_cvref_t<decltype(shlocks)>>::max());
      ++uplocks;

      upgrade_function wq_front = std::get<upgrade_function>(std::move(wq_.front()));
      wq_.pop();
      lck.unlock();

      std::invoke(wq_front, upgrade_lock(this->shared_from_this()), false);
    } else if (shlocks == 0) {
      assert(exlocks == 0 && uplocks == 0 && shlocks == 0);
      assert(exlocks < std::numeric_limits<std::remove_cvref_t<decltype(shlocks)>>::max());
      ++exlocks;

      exclusive_function wq_front = std::get<exclusive_function>(std::move(wq_.front()));
      wq_.pop();
      lck.unlock();

      std::invoke(wq_front, exclusive_lock(this->shared_from_this()), false);
    }
  }

  auto unlock_exclusive() -> void {
    std::unique_lock lck{mtx_};
    assert(exlocks > 0);
    --exlocks;
    if (exlocks > 0) return;

    if (!wq_.empty()) {
      auto wq_front = std::move(wq_.front());
      wq_.pop();
      std::visit(
          overload(
              [this, &lck](exclusive_function&& fn) {
                assert(this->exlocks == 0 && this->uplocks == 0 && this->shlocks == 0);
                assert(this->exlocks < std::numeric_limits<std::remove_cvref_t<decltype(shlocks)>>::max());
                ++this->exlocks;
                lck.unlock();

                std::invoke(fn, exclusive_lock(this->shared_from_this()), false);
              },
              [this, &lck](upgrade_function&& fn) {
                assert(this->exlocks == 0);
                assert(this->uplocks < std::numeric_limits<std::remove_cvref_t<decltype(shlocks)>>::max());
                ++this->uplocks;

                assert(this->exlocks == 0 && this->pq_.empty());
                assert(this->shlocks < std::numeric_limits<std::remove_cvref_t<decltype(shlocks)>>::max() - this->shq_.size());
                this->shlocks += this->shq_.size();
                auto shared = std::move(this->shq_);
                lck.unlock();

                std::invoke(fn, upgrade_lock(this->shared_from_this()), false);
                std::for_each(
                    std::make_move_iterator(shared.begin()), std::make_move_iterator(shared.end()),
                    [this](shared_function&& invocation) {
                      std::invoke(invocation, shared_lock(this->shared_from_this()), false);
                    });
              }),
          std::move(wq_front));
    } else {
      assert(exlocks == 0 && pq_.empty());
      assert(shlocks < std::numeric_limits<std::remove_cvref_t<decltype(shlocks)>>::max() - shq_.size());
      shlocks += shq_.size();
      auto shared = std::move(shq_);
      lck.unlock();

      std::for_each(
          std::make_move_iterator(shared.begin()), std::make_move_iterator(shared.end()),
          [this](shared_function&& invocation) {
            std::invoke(invocation, shared_lock(this->shared_from_this()), false);
          });
    }
  }

  auto shared_inc() noexcept -> void {
    std::lock_guard lck{mtx_};
    assert(shlocks > 0);
    assert(shlocks < std::numeric_limits<std::remove_cvref_t<decltype(shlocks)>>::max());
    ++shlocks;
  }

  auto upgrade_inc() noexcept -> void {
    std::lock_guard lck{mtx_};
    assert(uplocks > 0);
    assert(uplocks < std::numeric_limits<std::remove_cvref_t<decltype(shlocks)>>::max());
    ++uplocks;
  }

  auto exclusive_inc() noexcept -> void {
    std::lock_guard lck{mtx_};
    assert(exlocks > 0);
    assert(exlocks < std::numeric_limits<std::remove_cvref_t<decltype(shlocks)>>::max());
    ++exlocks;
  }

  auto make_upgrade_lock_from_lock() noexcept -> upgrade_lock {
    std::lock_guard lck{mtx_};
    assert(exlocks > 0);
    assert(uplocks < std::numeric_limits<std::remove_cvref_t<decltype(shlocks)>>::max());
    ++uplocks;
    return upgrade_lock(this->shared_from_this());
  }

  // Note: must grant the upgrade lock.
  auto add_upgrade(exclusive_function&& fn) -> void {
    std::unique_lock lck{mtx_};
    assert(uplocks > 0);
    if (shlocks == 0) {
      assert(uplocks > 0);
      assert(exlocks < std::numeric_limits<std::remove_cvref_t<decltype(shlocks)>>::max());
      --uplocks;
      ++exlocks;
      lck.unlock();

      std::invoke(fn, exclusive_lock(this->shared_from_this()), true);
      return;
    }

    pq_.emplace_back(std::move(fn));
  }

  private:
  auto want_exclusive() const -> bool {
    return exlocks == 0 && uplocks == 0 && !wq_.empty() && std::holds_alternative<exclusive_function>(wq_.front());
  }

  executor_type ex_;
  allocator_type alloc_;
  mutable std::mutex mtx_;
  shared_fn_list shq_;
  queue_type wq_;
  pending_upgrades_list pq_;
  std::uintptr_t shlocks = 0, exlocks = 0, uplocks = 0;
};


template<typename Executor, typename Allocator>
class monitor<Executor, Allocator>::shared_lock {
  friend class monitor::state;
  friend class monitor::upgrade_lock;
  friend class monitor::exclusive_lock;

  private:
  explicit shared_lock(std::shared_ptr<state> s) noexcept
  : state_(std::move(s))
  {}

  public:
  shared_lock() noexcept = default;

  shared_lock(const shared_lock& y) noexcept
  : state_(y.state_)
  {
    if (state_ != nullptr) state_->shared_inc();
  }

  shared_lock(shared_lock&& y) noexcept
  : state_(std::move(y.state_))
  {}

  auto operator=(const shared_lock& y) noexcept -> shared_lock& {
    using std::swap;
    shared_lock copy = y;
    swap(state_, copy.state_);
    return *this;
  }

  auto operator=(shared_lock&& y) noexcept -> shared_lock& {
    using std::swap;
    shared_lock copy = std::move(y);
    swap(state_, copy.state_);
    return *this;
  }

  ~shared_lock() {
    if (state_ != nullptr) state_->unlock_shared();
  }

  auto reset() -> void {
    if (state_ != nullptr) {
      state_->unlock_shared();
      state_.reset();
    }
  }

  auto is_locked() const noexcept -> bool { return state_ != nullptr; }
  explicit operator bool() const noexcept { return is_locked(); }
  auto operator!() const noexcept -> bool { return !is_locked(); }

  private:
  std::shared_ptr<state> state_;
};


template<typename Executor, typename Allocator>
class monitor<Executor, Allocator>::upgrade_lock {
  friend class monitor::state;

  private:
  explicit upgrade_lock(std::shared_ptr<state> state) noexcept
  : state_(std::move(state))
  {}

  public:
  upgrade_lock() noexcept = default;

  upgrade_lock(const upgrade_lock& y) noexcept
  : state_(y.state_)
  {
    if (state_ != nullptr) state_->upgrade_inc();
  }

  upgrade_lock(upgrade_lock&& y) noexcept
  : state_(std::move(y.state_))
  {}

  auto operator=(const upgrade_lock& y) noexcept -> upgrade_lock& {
    using std::swap;
    upgrade_lock copy = y;
    swap(state_, copy.state_);
    return *this;
  }

  auto operator=(upgrade_lock&& y) noexcept -> upgrade_lock& {
    using std::swap;
    upgrade_lock copy = std::move(y);
    swap(state_, copy.state_);
    return *this;
  }

  ~upgrade_lock() {
    if (state_ != nullptr) state_->unlock_upgrade();
  }

  auto reset() -> void {
    if (state_ != nullptr) {
      state_->unlock_upgrade();
      state_.reset();
    }
  }

  auto is_locked() const noexcept -> bool { return state_ != nullptr; }
  explicit operator bool() const noexcept { return is_locked(); }
  auto operator!() const noexcept -> bool { return !is_locked(); }

  template<typename CompletionToken> auto async_exclusive(CompletionToken&& token) const &;
  template<typename CompletionToken> auto async_exclusive(CompletionToken&& token) &&;
  template<typename CompletionToken> auto dispatch_exclusive(CompletionToken&& token) const &;
  template<typename CompletionToken> auto dispatch_exclusive(CompletionToken&& token) &&;

  private:
  std::shared_ptr<state> state_;
};


template<typename Executor, typename Allocator>
class monitor<Executor, Allocator>::exclusive_lock {
  friend class monitor::state;

  private:
  explicit exclusive_lock(std::shared_ptr<state> state) noexcept
  : state_(std::move(state))
  {}

  public:
  exclusive_lock() noexcept = default;

  exclusive_lock(const exclusive_lock& y) noexcept
  : state_(y.state_)
  {
    if (state_ != nullptr) state_->exclusive_inc();
  }

  exclusive_lock(exclusive_lock&& y) noexcept
  : state_(std::move(y.state_))
  {}

  auto operator=(const exclusive_lock& y) noexcept -> exclusive_lock& {
    using std::swap;
    exclusive_lock copy = y;
    swap(state_, copy.state_);
    return *this;
  }

  auto operator=(exclusive_lock&& y) noexcept -> exclusive_lock& {
    using std::swap;
    exclusive_lock copy = std::move(y);
    swap(state_, copy.state_);
    return *this;
  }

  ~exclusive_lock() {
    if (state_ != nullptr) state_->unlock_exclusive();
  }

  auto reset() -> void {
    if (state_ != nullptr) {
      state_->unlock_exclusive();
      state_.reset();
    }
  }

  auto is_locked() const noexcept -> bool { return state_ != nullptr; }
  explicit operator bool() const noexcept { return is_locked(); }
  auto operator!() const noexcept -> bool { return !is_locked(); }

  auto as_upgrade_lock() const -> upgrade_lock {
    if (state_ == nullptr) throw std::logic_error("lock not held");
    return state_->make_upgrade_lock_from_lock();
  }

  private:
  std::shared_ptr<state> state_;
};


template<typename Executor, typename Allocator>
template<typename CompletionToken>
inline auto monitor<Executor, Allocator>::async_shared(CompletionToken&& token) {
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

template<typename Executor, typename Allocator>
template<typename CompletionToken>
inline auto monitor<Executor, Allocator>::async_upgrade(CompletionToken&& token) {
  return asio::async_initiate<CompletionToken, void(upgrade_lock)>(
      [](auto handler, std::shared_ptr<state> state_) {
        state_->add(
            shared_function(
                completion_wrapper<void(upgrade_lock, bool)>(
                    completion_handler_fun(std::move(handler), state_->get_executor()),
                    [](auto handler, upgrade_lock lock, [[maybe_unused]] bool immediate) {
                      std::invoke(handler, std::move(lock));
                    })));
      },
      token, state_);
}

template<typename Executor, typename Allocator>
template<typename CompletionToken>
inline auto monitor<Executor, Allocator>::async_exclusive(CompletionToken&& token) {
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

template<typename Executor, typename Allocator>
template<typename CompletionToken>
inline auto monitor<Executor, Allocator>::dispatch_shared(CompletionToken&& token) {
  return asio::async_initiate<CompletionToken, void(shared_lock)>(
      [](auto handler, std::shared_ptr<state> state_) {
        state_->add(
            shared_function(
                completion_wrapper<void(shared_lock, bool)>(
                    completion_handler_fun(std::move(handler), state_->get_executor()),
                    [](auto handler, shared_lock lock, bool immediate) {
                      if (immediate)
                        handler.dispatch(std::move(lock));
                      else
                        handler.post(std::move(lock));
                    })));
      },
      token, state_);
}

template<typename Executor, typename Allocator>
template<typename CompletionToken>
inline auto monitor<Executor, Allocator>::dispatch_upgrade(CompletionToken&& token) {
  return asio::async_initiate<CompletionToken, void(upgrade_lock)>(
      [](auto handler, std::shared_ptr<state> state_) {
        state_->add(
            shared_function(
                completion_wrapper<void(upgrade_lock, bool)>(
                    completion_handler_fun(std::move(handler), state_->get_executor()),
                    [](auto handler, upgrade_lock lock, bool immediate) {
                      if (immediate)
                        handler.dispatch(std::move(lock));
                      else
                        handler.post(std::move(lock));
                    })));
      },
      token, state_);
}

template<typename Executor, typename Allocator>
template<typename CompletionToken>
inline auto monitor<Executor, Allocator>::dispatch_exclusive(CompletionToken&& token) {
  return asio::async_initiate<CompletionToken, void(exclusive_lock)>(
      [](auto handler, std::shared_ptr<state> state_) {
        state_->add(
            exclusive_function(
                completion_wrapper<void(exclusive_lock, bool)>(
                    completion_handler_fun(std::move(handler), state_->get_executor()),
                    [](auto handler, exclusive_lock lock, bool immediate) {
                      if (immediate)
                        handler.dispatch(std::move(lock));
                      else
                        handler.post(std::move(lock));
                    })));
      },
      token, state_);
}


template<typename Executor, typename Allocator>
template<typename CompletionToken>
inline auto monitor<Executor, Allocator>::upgrade_lock::async_exclusive(CompletionToken&& token) const & {
  if (state_ == nullptr) throw std::logic_error("lock not held");

  return asio::async_initiate<CompletionToken, void(exclusive_lock)>(
      [](auto handler, upgrade_lock self) {
        self.state_->add_upgrade(
            exclusive_function(
                completion_wrapper<void(exclusive_lock, bool)>(
                    completion_handler_fun(std::move(handler), self.state_->get_executor()),
                    [](auto handler, exclusive_lock lock, [[maybe_unused]] bool immediate) {
                      std::invoke(handler, std::move(lock));
                    })));
        self.state_.reset(); // Grant uplock to the 'add_upgrade' function.
      },
      token, *this);
}

template<typename Executor, typename Allocator>
template<typename CompletionToken>
inline auto monitor<Executor, Allocator>::upgrade_lock::async_exclusive(CompletionToken&& token) && {
  if (state_ == nullptr) throw std::logic_error("lock not held");

  return asio::async_initiate<CompletionToken, void(exclusive_lock)>(
      [](auto handler, upgrade_lock self) {
        self.state_->add_upgrade(
            exclusive_function(
                completion_wrapper<void(exclusive_lock, bool)>(
                    completion_handler_fun(std::move(handler), self.state_->get_executor()),
                    [](auto handler, exclusive_lock lock, [[maybe_unused]] bool immediate) {
                      std::invoke(handler, std::move(lock));
                    })));
        self.state_.reset(); // Grant uplock to the 'add_upgrade' function.
      },
      token, std::move(*this));
}

template<typename Executor, typename Allocator>
template<typename CompletionToken>
inline auto monitor<Executor, Allocator>::upgrade_lock::dispatch_exclusive(CompletionToken&& token) const & {
  if (state_ == nullptr) throw std::logic_error("lock not held");

  return asio::async_initiate<CompletionToken, void(exclusive_lock)>(
      [](auto handler, upgrade_lock self) {
        self.state_->add_upgrade(
            exclusive_function(
                completion_wrapper<void(exclusive_lock, bool)>(
                    completion_handler_fun(std::move(handler), self.state_->get_executor()),
                    [](auto handler, exclusive_lock lock, bool immediate) {
                      if (immediate)
                        handler.dispatch(std::move(lock));
                      else
                        handler.post(std::move(lock));
                    })));
        self.state_.reset(); // Grant uplock to the 'add_upgrade' function.
      },
      token, *this);
}

template<typename Executor, typename Allocator>
template<typename CompletionToken>
inline auto monitor<Executor, Allocator>::upgrade_lock::dispatch_exclusive(CompletionToken&& token) && {
  if (state_ == nullptr) throw std::logic_error("lock not held");

  return asio::async_initiate<CompletionToken, void(exclusive_lock)>(
      [](auto handler, upgrade_lock self) {
        self.state_->add_upgrade(
            exclusive_function(
                completion_wrapper<void(exclusive_lock, bool)>(
                    completion_handler_fun(std::move(handler), self.state_->get_executor()),
                    [](auto handler, exclusive_lock lock, bool immediate) {
                      if (immediate)
                        handler.dispatch(std::move(lock));
                      else
                        handler.post(std::move(lock));
                    })));
        self.state_.reset(); // Grant uplock to the 'add_upgrade' function.
      },
      token, std::move(*this));
}


} /* namespace earnest::detail */
