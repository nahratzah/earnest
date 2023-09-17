#pragma once

#include <algorithm>
#include <cstddef>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <utility>

#include <earnest/detail/completion_handler_fun.h>
#include <earnest/detail/completion_wrapper.h>
#include <earnest/detail/move_only_function.h>

namespace earnest::detail {


template<typename Executor, typename Alloc> class semaphore_state_;
template<typename Executor, typename Alloc> class semaphore;
class semaphore_lock;


class semaphore_lock {
  template<typename Executor, typename Alloc> friend class semaphore;
  template<typename Executor, typename Alloc> friend class semaphore_state_;

  private:
  class lock_intf {
    public:
    virtual ~lock_intf() noexcept = default;

    private:
    virtual auto get_state_voidptr() const noexcept -> void* = 0;

    public:
    template<typename Executor, typename Alloc>
    auto holds_semaphore(const semaphore<Executor, Alloc>& sem) const noexcept -> bool {
      return sem.state_ != nullptr && sem.state_.get() == get_state_voidptr();
    }
  };

  public:
  constexpr semaphore_lock() noexcept
  : lockptr()
  {}

  constexpr semaphore_lock([[maybe_unused]] std::nullptr_t np) noexcept
  : lockptr()
  {}

  private:
  explicit semaphore_lock(std::shared_ptr<lock_intf> lockptr) noexcept
  : lockptr(lockptr)
  {}

  public:
  explicit operator bool() const noexcept {
    return lockptr != nullptr;
  }

  void reset() {
    lockptr.reset();
  }

  template<typename Executor, typename Allocator>
  auto holds_semaphore(const semaphore<Executor, Allocator>& sem) const noexcept -> bool {
    if (lockptr == nullptr) return false;
    return lockptr->holds_semaphore(sem);
  }

  private:
  std::shared_ptr<lock_intf> lockptr;
};


template<typename Executor, typename Alloc>
class semaphore_state_
: public std::enable_shared_from_this<semaphore_state_<Executor, Alloc>>
{
  public:
  using executor_type = Executor;
  using allocator_type = Alloc;

  private:
  template<typename T> using rebind_alloc = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;

  using pending_queue_type = std::queue<
      move_only_function<void(semaphore_lock)>,
      std::deque<
          move_only_function<void(semaphore_lock)>,
          rebind_alloc<move_only_function<void(semaphore_lock)>>>>;

  class lock_impl
  : public semaphore_lock::lock_intf
  {
    public:
    constexpr lock_impl() noexcept
    : state()
    {}

    explicit lock_impl(semaphore_state_& s) noexcept
    : lock_impl()
    {
      assert(s.avail_ > 0);
      state = s.shared_from_this();
      --s.avail_;
      ++s.active_;
    }

    lock_impl(const lock_impl&) = delete;
    lock_impl(lock_impl&&) = delete;

    ~lock_impl() noexcept override {
      if (state != nullptr) state->release_();
    }

    auto get_state_voidptr() const noexcept -> void* override {
      return state.get();
    }

    private:
    std::shared_ptr<semaphore_state_> state;
  };

  public:
  semaphore_state_(executor_type ex, std::size_t semcount, allocator_type alloc)
  : ex_(ex),
    alloc_(alloc),
    avail_(semcount),
    pending_(alloc)
  {}

  semaphore_state_(const semaphore_state_&) = delete;
  semaphore_state_(semaphore_state_&&) = delete;

  auto get_executor() const -> executor_type {
    return ex_;
  }

  auto get_allocator() const -> allocator_type {
    return alloc_;
  }

  auto get_semcount() const noexcept -> std::size_t {
    std::lock_guard lck{mtx_};
    return get_semcount_();
  }

  auto set_semcount(std::size_t new_semcount) -> void {
    std::lock_guard lck{mtx_};
    const std::size_t old_semcount = get_semcount_();
    if (new_semcount < old_semcount)
      reduce_(old_semcount - new_semcount);
    else if (new_semcount > old_semcount)
      increase_(new_semcount - old_semcount);
  }

  // Acquire the semaphore.
  template<typename CompletionToken>
  auto async_wait(CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(semaphore_lock)>(
        [](auto handler, const std::shared_ptr<semaphore_state_>& self) {
          self->async_acquire_(std::move(handler));
        },
        token,
        this->shared_from_this());
  }

  [[nodiscard]]
  auto try_acquire() noexcept -> std::optional<semaphore_lock> {
    std::lock_guard lck{mtx_};
    if (avail_ > 0)
      return create_lock_();
    else
      return std::nullopt;
  }

  private:
  template<typename Fn>
  auto async_acquire_(Fn&& fn) -> void {
    auto handler = completion_handler_fun(std::move(fn), get_executor());

    std::lock_guard lck{mtx_};
    if (avail_ > 0)
      std::invoke(handler, create_lock_());
    else
      pending_.push(std::move(handler));
  }

  auto release_() noexcept -> void {
    std::lock_guard lck{mtx_};
    --active_;

    if (deficient_ > 0) [[unlikely]] {
      --deficient_;
    } else {
      ++avail_;
      if (!pending_.empty()) {
        std::invoke(pending_.front(), create_lock_());
        pending_.pop();
      }
    }
  }

  auto create_lock_() -> semaphore_lock {
    return semaphore_lock(std::allocate_shared<lock_impl>(get_allocator(), *this));
  }

  auto get_semcount_() const noexcept -> std::size_t {
    return avail_ + active_ - deficient_;
  }

  auto reduce_(std::size_t delta) noexcept -> void {
    assert(get_semcount_() >= delta);

    const std::size_t availdelta = std::min(avail_, delta);
    avail_ -= availdelta;
    delta -= availdelta;

    deficient_ += delta;
  }

  auto increase_(std::size_t delta) noexcept -> void {
    assert(delta < std::numeric_limits<std::size_t>::max() - get_semcount_());

    const auto deficient_count = std::min(deficient_, delta);
    deficient_ -= deficient_count;
    delta -= deficient_count;

    avail_ += delta;
    while (!pending_.empty() && avail_ > 0) {
      std::invoke(std::move(pending_.front()), create_lock_());
      pending_.pop();
    }
  }

  std::mutex mtx_;
  executor_type ex_;
  allocator_type alloc_;
  std::size_t avail_, active_ = 0, deficient_ = 0;
  pending_queue_type pending_;
};


/* Semaphore.
 *
 * A semaphore helps you run at most N parallel executions.
 *
 * This semaphore grants copyable shared locks.
 * A single acquisition corresponds to a single lock.
 * Copied locks share the underlying acquisition.
 */
template<typename Executor, typename Allocator>
class semaphore {
  friend class semaphore_lock::lock_intf;

  public:
  using executor_type = Executor;
  using allocator_type = Allocator;
  using lock_type = semaphore_lock;

  private:
  using state_t = semaphore_state_<executor_type, allocator_type>;

  public:
  constexpr semaphore() noexcept
  : state_()
  {}

  explicit semaphore(executor_type ex, std::size_t semcount, allocator_type alloc = allocator_type())
  : state_(std::allocate_shared<state_t>(alloc, std::move(ex), semcount, alloc))
  {}

  auto get_executor() const -> executor_type {
    return state_->ex;
  }

  template<typename CompletionToken>
  auto async_wait(CompletionToken&& token) {
    return state_->async_wait(std::forward<CompletionToken>(token));
  }

  auto try_acquire() -> std::optional<lock_type> {
    return state_->try_acquire();
  }

  auto get_semcount() const noexcept -> std::size_t {
    return state_->get_semcount();
  }

  private:
  std::shared_ptr<state_t> state_;
};


} /* namespace earnest::detail */
