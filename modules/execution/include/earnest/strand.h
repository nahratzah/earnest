#pragma once

#include <deque>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>

#include <earnest/execution.h>

namespace earnest::execution {
inline namespace extensions {


// Common-base for strands.
//
// Provides the `running_in_this_thread' function.
//
// Because strands can be nested, we store the thread-ID of the active usage in the strand.
// (This also makes the implementation somewhat robust against coroutines. Note that the
// `running_in_this_thread' method won't work properly if you use co-routines that are thread-hopping.)
class strand_base_ {
  protected:
  strand_base_() = default;
  strand_base_(const strand_base_&) = delete;
  strand_base_(strand_base_&&) = delete;
  ~strand_base_() = default;

  public:
  auto running_in_this_thread() const noexcept -> bool {
    return active_thread_ == std::this_thread::get_id();
  }

  protected:
  class active_thread_updater {
    public:
    explicit active_thread_updater(strand_base_& self)
    : self(self)
    {
      self.active_thread_ = std::this_thread::get_id();
    }

    ~active_thread_updater() {
      self.active_thread_ = std::thread::id{};
    }

    private:
    strand_base_& self;
  };

  private:
  std::thread::id active_thread_{};
};


// A strand ensures no two functions can be running at the same time.
// (It sorta pretends to be a single thread. Or like a critical-section.)
//
// Sender/receiver strands are adapters on a scheduler.
// When the strand's scheduler is started:
// - it'll wait (non-blocking) until the strand becomes available
// - then it'll acquire the strand (ensuring it's no longer available to other users)
// - then start the nested scheduler
// Inside this nested scheduler:
// - it'll start the actual sender chain
// - and then after the start method returns, it'll release the strand (allowing another user to do the whole thing).
//
// To acquire a strand's scheduler, you have to use the strand.scheduler() method,
// and pass it an existing scheduler.
// The resulting scheduler will wrap this scheduler.
//
// Because strand-schedulers are scheduler-wrappers, they can wrap other strand-schedulers.
// If you do this, be very careful you don't get deadlocked. A useful approach is to consider
// strands as-if they are mutexes, and to maintain lock-leveling.
template<typename Alloc = std::allocator<std::byte>>
class strand
: private strand_base_
{
  public:
  // Strands use an allocator.
  using allocator_type = Alloc;

  private:
  template<typename T>
  using rebind_alloc = typename std::allocator_traits<Alloc>::template rebind_alloc<T>;

  using callback_type = void (*)(void*) noexcept;
  struct entry {
    callback_type cb;
    void* ptr;

    auto operator()() const noexcept -> void {
      std::invoke(cb, ptr);
    }
  };
  using queue_type = std::queue<entry, std::deque<entry, rebind_alloc<entry>>>;

  template<typename NestedScheduler, typename Receiver>
  class opstate
  : private operation_state_base_
  {
    private:
    // Receiver that we attach to the nested scheduler.
    // This receiver:
    // - forwards the set-value/set-error/set-done signals.
    // - and after each of those, releases the strand (permitting the next thread to start).
    class scheduler_receiver {
      public:
      explicit scheduler_receiver(opstate& state) noexcept
      : state(state)
      {}

      template<typename... Args>
      friend auto tag_invoke([[maybe_unused]] set_value_t tag, scheduler_receiver&& self, Args&&... args) noexcept -> void {
        { // Limit scope of the active-thread-updater.
          strand_base_::active_thread_updater atu{self.state.s};
          try {
            execution::set_value(std::move(self.state.r), std::forward<Args>(args)...);
          } catch (...) {
            execution::set_error(std::move(self.state.r), std::current_exception());
          }
        }
        self.state.release(); // Release. Since this starts a new sender, the active-thread-updater _must not_ be running.
      }

      template<typename Error>
      friend auto tag_invoke([[maybe_unused]] set_error_t tag, scheduler_receiver&& self, Error&& error) noexcept -> void {
        { // Limit scope of the active-thread-updater.
          strand_base_::active_thread_updater atu(self.state.s);
          execution::set_error(std::move(self.state.r), std::forward<Error>(error));
        }
        self.state.release(); // Release. Since this starts a new sender, the active-thread-updater _must not_ be running.
      }

      friend auto tag_invoke([[maybe_unused]] set_done_t tag, scheduler_receiver&& self) noexcept -> void {
        { // Limit scope of the active-thread-updater.
          strand_base_::active_thread_updater atu(self.state.s);
          execution::set_done(std::move(self.state.r));
        }
        self.state.release(); // Release. Since this starts a new sender, the active-thread-updater _must not_ be running.
      }

      private:
      opstate& state;
    };

    static auto make_scheduler_opstate(opstate& self, NestedScheduler&& nested_scheduler) {
      return execution::connect(execution::schedule(nested_scheduler), scheduler_receiver(self));
    }
    using scheduler_opstate_t = decltype(make_scheduler_opstate(std::declval<opstate&>(), std::declval<NestedScheduler>()));

    public:
    explicit opstate(strand& s, NestedScheduler&& scheduler, Receiver&& r)
    : s(s),
      r(std::move(r)),
      scheduler_opstate(make_scheduler_opstate(*this, std::move(scheduler)))
    {}

    friend auto tag_invoke([[maybe_unused]] start_t tag, opstate& self) noexcept -> void {
      try {
        self.s.run_or_enqueue(
            entry{
              .cb=[](void* self_vptr) noexcept -> void {
                const auto self_ptr = static_cast<opstate*>(self_vptr);
                execution::start(self_ptr->scheduler_opstate);
              },
              .ptr=&self,
            });
      } catch (...) {
        execution::set_error(std::move(self.r), std::current_exception());
      }
    }

    private:
    auto release() noexcept -> void {
      s.release();
    }

    strand& s;
    Receiver r;
    scheduler_opstate_t scheduler_opstate;
  };

  // Forward decaration, so that the bound-scheduler can refer to the sender-impl.
  template<execution::scheduler UnderlyingScheduler>
  class sender_impl;

  template<execution::scheduler UnderlyingScheduler>
  class bound_scheduler_t {
    public:
    explicit bound_scheduler_t(strand& s, const UnderlyingScheduler& underlying_sched)
    noexcept(std::is_nothrow_copy_constructible_v<UnderlyingScheduler>)
    : s(s),
      underlying_sched(underlying_sched)
    {}

    explicit bound_scheduler_t(strand& s, UnderlyingScheduler&& underlying_sched)
    noexcept(std::is_nothrow_move_constructible_v<UnderlyingScheduler>)
    : s(s),
      underlying_sched(std::move(underlying_sched))
    {}

    auto operator==(const bound_scheduler_t& y) const noexcept -> bool {
      return &s == &y.s && underlying_sched == y.underlying_sched;
    }

    friend auto tag_invoke([[maybe_unused]] execution::schedule_t tag, bound_scheduler_t& self) -> sender_impl<UnderlyingScheduler> {
      return sender_impl<UnderlyingScheduler>(self.s, self.underlying_sched);
    }

    private:
    strand& s;
    UnderlyingScheduler underlying_sched;
  };

  template<execution::scheduler UnderlyingScheduler>
  class sender_impl
  : public sender_traits<decltype(execution::schedule(std::declval<UnderlyingScheduler>()))>
  {
    public:
    explicit sender_impl(strand& s, const UnderlyingScheduler& underlying_scheduler)
    : s(s),
      underlying_scheduler(underlying_scheduler)
    {}

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t tag, sender_impl&& self, Receiver&& r)
    -> opstate<UnderlyingScheduler, std::remove_cvref_t<Receiver>> {
      return opstate<UnderlyingScheduler, std::remove_cvref_t<Receiver>>(self.s, std::move(self.underlying_scheduler), std::forward<Receiver>(r));
    }

    friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_value_t> tag, const sender_impl& self)
    -> bound_scheduler_t<UnderlyingScheduler> {
      return bound_scheduler_t<UnderlyingScheduler>(self.s, self.underlying_scheduler);
    }

    friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_error_t> tag, const sender_impl& self)
    -> bound_scheduler_t<UnderlyingScheduler> {
      return bound_scheduler_t<UnderlyingScheduler>(self.s, self.underlying_scheduler);
    }

    friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_done_t> tag, const sender_impl& self)
    -> bound_scheduler_t<UnderlyingScheduler> {
      return bound_scheduler_t<UnderlyingScheduler>(self.s, self.underlying_scheduler);
    }

    private:
    strand& s;
    UnderlyingScheduler underlying_scheduler;
  };

  public:
  explicit strand(allocator_type alloc = allocator_type())
  noexcept(std::is_nothrow_default_constructible_v<std::mutex> && std::is_nothrow_constructible_v<queue_type, allocator_type>)
  : q(std::move(alloc))
  {}

  strand(const strand&) = delete;
  strand(strand&&) = delete;

#ifndef NDEBUG // If NDEBUG, we rely on the default destructor.
  ~strand() {
    // Destroying the strand, while it is in-progress, or has enqueued tasks,
    // will result in undefined behaviour.
    std::lock_guard lck{mtx};
    assert(!engaged);
    assert(q.empty());
  }
#endif

  using strand_base_::running_in_this_thread;

  // Adapt an existing scheduler, so that its scheduled tasks run on this strand.
  template<typename UnderlyingSched>
  auto scheduler(UnderlyingSched&& underlying_sched)
  noexcept(std::is_nothrow_constructible_v<std::remove_cvref_t<UnderlyingSched>, UnderlyingSched>)
  -> bound_scheduler_t<std::remove_cvref_t<UnderlyingSched>> {
    return bound_scheduler_t<std::remove_cvref_t<UnderlyingSched>>(*this, std::forward<UnderlyingSched>(underlying_sched));
  }

  private:
  auto run_or_enqueue(entry&& e) {
    std::unique_lock lck{mtx};
    if (engaged) {
      q.push(std::move(e));
    } else {
      engaged = true;

      // Run the item, without holding the lock.
      lck.unlock();
      std::invoke(e);
    }
  }

  auto release() noexcept -> void {
    std::unique_lock lck{mtx};
    assert(engaged);
    if (q.empty()) {
      engaged = false;
    } else {
      auto todo = std::move(q.front());
      q.pop();

      // Run the item, without holding the lock.
      lck.unlock();
      std::invoke(todo); // never throws
    }
  }

  std::mutex mtx;
  bool engaged = false;
  queue_type q;
};


} /* inline namespace extensions */
} /* namespace earnest::execution */
