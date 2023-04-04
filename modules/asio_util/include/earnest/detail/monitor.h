#pragma once

#include <algorithm>
#include <cstddef>
#include <deque>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <utility>
#include <variant>
#include <vector>

#include <earnest/detail/completion_handler_fun.h>
#include <earnest/detail/completion_wrapper.h>
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

  [[nodiscard]] auto try_shared() noexcept -> std::optional<shared_lock>;
  [[nodiscard]] auto try_upgrade() noexcept -> std::optional<upgrade_lock>;
  [[nodiscard]] auto try_exclusive() noexcept -> std::optional<exclusive_lock>;
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

  [[nodiscard]] auto try_shared() noexcept -> std::optional<shared_lock> {
    std::lock_guard lck{mtx_};
    return maybe_lock_shared();
  }

  auto add(shared_function&& fn) -> void {
    std::unique_lock lck{mtx_};
    auto shlock = maybe_lock_shared();
    if (shlock.has_value()) {
      lck.unlock();
      std::invoke(fn, std::move(shlock).value(), true);
      return;
    }

    shq_.emplace_back(std::move(fn));
  }

  [[nodiscard]] auto try_upgrade() noexcept -> std::optional<upgrade_lock> {
    std::lock_guard lck{mtx_};
    return maybe_lock_upgrade();
  }

  auto add(upgrade_function&& fn) -> void {
    std::unique_lock lck{mtx_};
    auto uplock = maybe_lock_upgrade();
    if (uplock.has_value()) {
      lck.unlock();
      std::invoke(fn, std::move(uplock).value(), true);
      return;
    }

    wq_.emplace(std::move(fn));
  }

  [[nodiscard]] auto try_exclusive() noexcept -> std::optional<exclusive_lock> {
    std::lock_guard lck{mtx_};
    return maybe_lock_exclusive();
  }

  auto add(exclusive_function&& fn) -> void {
    std::unique_lock lck{mtx_};
    auto exlock = maybe_lock_exclusive();
    if (exlock.has_value()) {
      lck.unlock();
      std::invoke(fn, std::move(exlock).value(), true);
      return;
    }

    wq_.emplace(std::move(fn));
  }

  auto unlock_shared() -> void {
    std::lock_guard lck{mtx_};
    assert(shlocks > 0);
    --shlocks;
    if (shlocks > 0) return;

    if (!pq_.empty()) {
      assert(uplocks >= pq_.size());
      if (exlocks == 0) {
        std::for_each(
            pq_.begin(), pq_.end(),
            [this](exclusive_function& f) {
              assert(exlocks < std::numeric_limits<std::remove_cvref_t<decltype(exlocks)>>::max());
              ++exlocks;
              --uplocks;
              std::invoke(f, exclusive_lock(this->shared_from_this()), false);
            });
        pq_.clear();
      }
      return;
    }

    if (!wq_.empty() && std::holds_alternative<exclusive_function>(wq_.front())) {
      auto exlock = maybe_lock_exclusive();
      if (exlock.has_value()) {
        exclusive_function f = std::get<exclusive_function>(std::move(wq_.front()));
        wq_.pop();
        std::invoke(f, std::move(exlock).value(), false);
      }
      return;
    }

    assert(shq_.empty());
  }

  auto unlock_upgrade() -> void {
    std::lock_guard lck{mtx_};
    assert(uplocks > 0);
    --uplocks;

    if (exlocks != 0) return;
    if (!pq_.empty()) return;

    if (!wq_.empty()) {
      if (std::holds_alternative<exclusive_function>(wq_.front())) {
        auto exlock = maybe_lock_exclusive();
        if (exlock.has_value()) {
          exclusive_function f = std::get<exclusive_function>(std::move(wq_.front()));
          wq_.pop();
          std::invoke(f, std::move(exlock).value(), false);
        }
        return;
      } else {
        auto uplock = maybe_lock_upgrade();
        if (uplock.has_value()) {
          upgrade_function f = std::get<upgrade_function>(std::move(wq_.front()));
          wq_.pop();
          std::invoke(f, std::move(uplock).value(), false);
        }
      }
    }

    assert(shq_.empty() || !maybe_lock_shared().has_value());
  }

  auto unlock_exclusive() -> void {
    std::lock_guard lck{mtx_};
    assert(exlocks > 0);
    --exlocks;

    // While an exclusive lock is held, all upgrades succeed.
    assert(pq_.empty());

    // If there are still exclusive locks held, nothing can be locked.
    if (exlocks != 0) return;

    if (!wq_.empty()) {
      if (std::holds_alternative<exclusive_function>(wq_.front())) {
        auto exlock = maybe_lock_exclusive();
        if (exlock.has_value()) {
          exclusive_function f = std::get<exclusive_function>(std::move(wq_.front()));
          wq_.pop();
          std::invoke(f, std::move(exlock).value(), false);
        }
        return;
      } else {
        auto uplock = maybe_lock_upgrade();
        if (uplock.has_value()) {
          upgrade_function f = std::get<upgrade_function>(std::move(wq_.front()));
          wq_.pop();
          std::invoke(f, std::move(uplock).value(), false);
        }
      }
    }

    std::for_each(
        shq_.begin(), shq_.end(),
        [this](shared_function& f) {
          std::invoke(f, this->maybe_lock_shared().value(), false);
        });
    shq_.clear();
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
    assert(uplocks < std::numeric_limits<std::remove_cvref_t<decltype(uplocks)>>::max());
    ++uplocks;
  }

  auto exclusive_inc() noexcept -> void {
    std::lock_guard lck{mtx_};
    assert(exlocks > 0);
    assert(exlocks < std::numeric_limits<std::remove_cvref_t<decltype(exlocks)>>::max());
    ++exlocks;
  }

  auto make_upgrade_lock_from_lock() noexcept -> upgrade_lock {
    std::lock_guard lck{mtx_};
    assert(exlocks > 0);
    assert(uplocks < std::numeric_limits<std::remove_cvref_t<decltype(uplocks)>>::max());
    ++uplocks;
    return upgrade_lock(this->shared_from_this());
  }

  // Note: must grant the upgrade lock.
  auto add_upgrade(exclusive_function&& fn) -> void {
    std::unique_lock lck{mtx_};
    assert(uplocks > 0);
    if (shlocks == 0) {
      assert(exlocks < std::numeric_limits<std::remove_cvref_t<decltype(exlocks)>>::max());
      --uplocks;
      ++exlocks;
      lck.unlock();

      std::invoke(fn, exclusive_lock(this->shared_from_this()), true);
      return;
    }

    pq_.emplace_back(std::move(fn));
  }

  // Note: must grant the upgrade lock, if the attempt is successful.
  auto add_upgrade_try() noexcept -> std::optional<exclusive_lock> {
    std::unique_lock lck{mtx_};
    assert(uplocks > 0);
    if (shlocks == 0) {
      assert(exlocks < std::numeric_limits<std::remove_cvref_t<decltype(exlocks)>>::max());
      --uplocks;
      ++exlocks;
      lck.unlock();

      return exclusive_lock(this->shared_from_this());
    }

    return std::nullopt;
  }

  private:
  auto maybe_lock_exclusive() noexcept -> std::optional<exclusive_lock> {
    if (exlocks != 0) return std::nullopt;
    if (uplocks != 0) return std::nullopt;
    if (shlocks != 0) return std::nullopt;

    ++exlocks;
    return exclusive_lock(this->shared_from_this());
  }

  auto maybe_lock_upgrade() noexcept -> std::optional<upgrade_lock> {
    if (exlocks != 0) return std::nullopt;
    if (uplocks != 0) return std::nullopt;

    ++uplocks;
    return upgrade_lock(this->shared_from_this());
  }

  auto maybe_lock_shared() noexcept -> std::optional<shared_lock> {
    if (exlocks != 0) return std::nullopt;
    if (!pq_.empty()) return std::nullopt;
    if (uplocks == 0 && !wq_.empty() && std::holds_alternative<exclusive_function>(wq_.front())) return std::nullopt;

    ++shlocks;
    return shared_lock(this->shared_from_this());
  }

  [[deprecated]]
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
  auto holds_monitor(const monitor& m) const noexcept -> bool { return m.state_ == state_; }

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
  auto holds_monitor(const monitor& m) const noexcept -> bool { return m.state_ == state_; }

  [[nodiscard]] auto try_exclusive() const & noexcept -> std::optional<exclusive_lock>;
  [[nodiscard]] auto try_exclusive() && noexcept -> std::optional<exclusive_lock>;
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
  auto holds_monitor(const monitor& m) const noexcept -> bool { return m.state_ == state_; }

  auto as_upgrade_lock() const & -> upgrade_lock {
    if (state_ == nullptr) throw std::logic_error("lock not held");
    return state_->make_upgrade_lock_from_lock();
  }

  auto as_upgrade_lock() && -> upgrade_lock {
    if (state_ == nullptr) throw std::logic_error("lock not held");
    upgrade_lock uplock = state_->make_upgrade_lock_from_lock();
    reset();
    return uplock;
  }

  private:
  std::shared_ptr<state> state_;
};


template<typename Executor, typename Allocator>
inline auto monitor<Executor, Allocator>::try_shared() noexcept -> std::optional<shared_lock> {
  assert(state_ != nullptr);
  return state_->try_shared();
}

template<typename Executor, typename Allocator>
inline auto monitor<Executor, Allocator>::try_upgrade() noexcept -> std::optional<upgrade_lock> {
  assert(state_ != nullptr);
  return state_->try_upgrade();
}

template<typename Executor, typename Allocator>
inline auto monitor<Executor, Allocator>::try_exclusive() noexcept -> std::optional<exclusive_lock> {
  assert(state_ != nullptr);
  return state_->try_exclusive();
}

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
            upgrade_function(
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
            upgrade_function(
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
[[nodiscard]] inline auto monitor<Executor, Allocator>::upgrade_lock::try_exclusive() const & noexcept -> std::optional<exclusive_lock> {
  return upgrade_lock(*this).try_exclusive(); // Invoke the move-operation.
}

template<typename Executor, typename Allocator>
[[nodiscard]] inline auto monitor<Executor, Allocator>::upgrade_lock::try_exclusive() && noexcept -> std::optional<exclusive_lock> {
  std::optional<exclusive_lock> lck = state_->add_upgrade_try();
  if (lck.has_value()) state_.reset(); // Grant uplock to the 'add_upgrade' function.
  return lck;
}

template<typename Executor, typename Allocator>
template<typename CompletionToken>
inline auto monitor<Executor, Allocator>::upgrade_lock::async_exclusive(CompletionToken&& token) const & {
  return asio::async_initiate<CompletionToken, void(exclusive_lock)>(
      [](auto handler, upgrade_lock self) {
        if (self.state_ == nullptr) throw std::logic_error("lock not held");

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
  return asio::async_initiate<CompletionToken, void(exclusive_lock)>(
      [](auto handler, upgrade_lock self) {
        if (self.state_ == nullptr) throw std::logic_error("lock not held");

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
  return asio::async_initiate<CompletionToken, void(exclusive_lock)>(
      [](auto handler, upgrade_lock self) {
        if (self.state_ == nullptr) throw std::logic_error("lock not held");

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
  return asio::async_initiate<CompletionToken, void(exclusive_lock)>(
      [](auto handler, upgrade_lock self) {
        if (self.state_ == nullptr) throw std::logic_error("lock not held");

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
