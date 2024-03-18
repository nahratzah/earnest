#pragma once

#include <deque>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>

#include <gsl/gsl>

#include <earnest/execution.h>
#include <earnest/blocking_scheduler.h>

namespace earnest::execution {
inline namespace extensions {


template<typename Alloc, execution::scheduler UnderlyingScheduler>
class strand_bound_scheduler_;


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
//
// Because strands can be nested, we store the thread-ID of the active usage in the strand.
// (This also makes the implementation somewhat robust against coroutines. Note that the
// `running_in_this_thread' method won't work properly if you use co-routines that are thread-hopping.)
template<typename Alloc = std::allocator<std::byte>>
class strand {
  friend struct not_on_strand_t;
  template<typename, execution::scheduler> friend class strand_bound_scheduler_;

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

  // We use a state pointer, so that the strand can be destroyed while it's running,
  // without creating all kinds of pointer problems.
  class state {
    private:
    using queue_type = std::queue<entry, std::deque<entry, rebind_alloc<entry>>>;

    public:
    explicit state(allocator_type alloc)
    : q(alloc),
      alloc(alloc)
    {}

    state(const state&) = delete;
    state(state&&) = delete;

#ifndef NDEBUG
    ~state() {
      std::lock_guard lck{mtx};
      assert(!engaged);
      assert(q.empty());
    }
#endif

    auto get_allocator() const -> allocator_type {
      return alloc;
    }

    auto try_lock() -> bool {
      std::unique_lock lck{mtx, std::try_to_lock};
      if (lck.owns_lock() && !engaged) {
        engaged = true;
        active_thread_ = std::this_thread::get_id();
        return true;
      } else {
        return false;
      }
    }

    private:
    auto block_(std::unique_lock<std::mutex>& lck) -> void {
      struct lock_state_t {
        std::mutex& mtx;
        std::condition_variable cond_var;
        bool active = false;

        auto wait(std::unique_lock<std::mutex>& lck) noexcept -> void {
          // wait_until may throw (which ought to be impossible under normal circumstances).
          // If it does, we cannot recover, so we mark this function as noexcept.
          cond_var.wait(
              lck,
              [this]() -> bool {
                return this->active;
              });
        }
      };

      if (!engaged) {
        engaged = true;
        active_thread_ = std::this_thread::get_id();
        return;
      }

      lock_state_t lock_state{ .mtx=this->mtx };
      q.push(
          entry{
            .cb=[](void* lock_state_vptr) noexcept -> void {
              gsl::not_null<lock_state_t*> lock_state = static_cast<lock_state_t*>(lock_state_vptr);
              std::lock_guard lck{lock_state->mtx};
              lock_state->active = true;
              lock_state->cond_var.notify_one();
            },
            .ptr=&lock_state,
          });

      lock_state.wait(lck);
      assert(engaged);
    }

    public:
    auto block() -> void {
      std::unique_lock lck{mtx};
      block_(lck);
    }

    auto lock() -> void {
      std::unique_lock lck{mtx};
      block_(lck);
      active_thread_ = std::this_thread::get_id();
    }

    auto unlock() -> void {
      std::unique_lock lck{mtx};
      if (!engaged)
        throw std::logic_error("strand: unlock called when not locked");
      if (active_thread_ != std::this_thread::get_id())
        throw std::logic_error("strand: unlock called from different thread");
      release_(std::move(lck));
    }

    auto release() noexcept -> void {
      release_(std::unique_lock{mtx});
    }

    auto running_in_this_thread() const noexcept -> bool {
      std::lock_guard lck{mtx};
      return active_thread_ == std::this_thread::get_id();
    }

    auto run_or_enqueue(entry&& e) -> void {
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

    auto mark_running() noexcept -> void {
      std::lock_guard lck{mtx};
      active_thread_ = std::this_thread::get_id();
    }

    private:
    auto release_(std::unique_lock<std::mutex> lck) noexcept -> void {
      assert(lck.owns_lock() && lck.mutex() == &mtx);
      assert(engaged);
      active_thread_ = std::thread::id{}; // Clear the active-thread-ID
      if (q.empty()) {
        engaged = false;
      } else {
        auto todo = std::move(q.front());
        q.pop();

        // Run the item, without holding the lock.
        lck.unlock();
        std::invoke(todo); // Never throws.
      }
    }

    mutable std::mutex mtx;
    mutable std::condition_variable cond_var;
    bool engaged = false;
    queue_type q;
    [[no_unique_address]] allocator_type alloc;
    std::thread::id active_thread_{};
  };

  using state_ptr = gsl::not_null<std::shared_ptr<state>>;

  static auto new_state(allocator_type alloc) -> state_ptr {
    return std::allocate_shared<state>(alloc, alloc);
  }

  template<typename NestedScheduler, typename Receiver>
  class opstate
  : private operation_state_base_
  {
    private:
    // Receiver that we attach to the nested scheduler.
    // This receiver:
    // - sets the active-thread (so that `running_in_this_thread()' works as intended)
    // - forwards the set-value/set-error/set-done signals.
    // - and after each of those, releases the strand (permitting the next thread to start).
    class scheduler_receiver {
      public:
      explicit scheduler_receiver(opstate& state) noexcept
      : state(state)
      {}

      template<typename... Args>
      friend auto tag_invoke([[maybe_unused]] set_value_t tag, scheduler_receiver&& self, Args&&... args) noexcept -> void {
        self.do_(
            []<typename LReceiver, typename... LArgs>(LReceiver&& r, LArgs&&... args) noexcept {
              try {
                execution::set_value(std::forward<LReceiver>(r), std::forward<LArgs>(args)...);
              } catch (...) {
                execution::set_error(std::forward<LReceiver>(r), std::current_exception());
              }
            });
      }

      template<typename Error>
      friend auto tag_invoke(set_error_t tag, scheduler_receiver&& self, Error&& error) noexcept -> void {
        self.do_(tag, std::forward<Error>(error));
      }

      friend auto tag_invoke(set_done_t tag, scheduler_receiver&& self) noexcept -> void {
        self.do_(tag);
      }

      template<typename Tag, typename... Args>
      requires (!std::same_as<set_value_t, Tag> && !std::same_as<set_error_t, Tag> && !std::same_as<set_done_t, Tag>)
      friend auto tag_invoke(Tag tag, const scheduler_receiver& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, const Receiver&, Args...>)
      -> tag_invoke_result_t<Tag, const Receiver&, Args...> {
        return execution::tag_invoke(tag, std::as_const(self.state.r), std::forward<Args>(args)...);
      }

      template<typename Tag, typename... Args>
      requires (!std::same_as<set_value_t, Tag> && !std::same_as<set_error_t, Tag> && !std::same_as<set_done_t, Tag>)
      friend auto tag_invoke(Tag tag, scheduler_receiver& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, Receiver&, Args...>)
      -> tag_invoke_result_t<Tag, Receiver&, Args...> {
        return execution::tag_invoke(tag, self.state.r, std::forward<Args>(args)...);
      }

      template<typename Tag, typename... Args>
      requires (!std::same_as<set_value_t, Tag> && !std::same_as<set_error_t, Tag> && !std::same_as<set_done_t, Tag>)
      friend auto tag_invoke(Tag tag, scheduler_receiver&& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, Receiver&&, Args...>)
      -> tag_invoke_result_t<Tag, Receiver&&, Args...> {
        return execution::tag_invoke(tag, std::move(self.state.r), std::forward<Args>(args)...);
      }

      private:
      template<typename Fn, typename... Args>
      auto do_(Fn&& fn, Args&&... args) noexcept -> void {
        const state_ptr s = state.s; // Make a local pointer, because set_done may destroy our operation-state.
        s->mark_running();
        std::invoke(std::forward<Fn>(fn), std::move(state.r), std::forward<Args>(args)...);
        s->release();
      }

      opstate& state;
    };

    static auto make_scheduler_opstate(opstate& self, NestedScheduler&& nested_scheduler) {
      return execution::connect(execution::schedule(nested_scheduler), scheduler_receiver(self));
    }
    using scheduler_opstate_t = decltype(make_scheduler_opstate(std::declval<opstate&>(), std::declval<NestedScheduler>()));

    public:
    explicit opstate(state_ptr s, NestedScheduler&& scheduler, Receiver&& r)
    : s(s),
      r(std::move(r)),
      scheduler_opstate(make_scheduler_opstate(*this, std::move(scheduler)))
    {}

    friend auto tag_invoke([[maybe_unused]] start_t tag, opstate& self) noexcept -> void {
      try {
        self.s->run_or_enqueue(
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
      s->release();
    }

    state_ptr s;
    Receiver r;
    scheduler_opstate_t scheduler_opstate;
  };

  // Forward decaration, so that the bound-scheduler can refer to the sender-impl.
  template<execution::scheduler UnderlyingScheduler>
  class sender_impl;

  template<execution::scheduler UnderlyingScheduler>
  using bound_scheduler_t = strand_bound_scheduler_<allocator_type, UnderlyingScheduler>;

  template<execution::scheduler UnderlyingScheduler>
  class sender_impl
  : public sender_traits<decltype(execution::schedule(std::declval<UnderlyingScheduler>()))>
  {
    public:
    explicit sender_impl(state_ptr s, const UnderlyingScheduler& underlying_scheduler)
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
    state_ptr s;
    UnderlyingScheduler underlying_scheduler;
  };

  // Sender used in a `schedule(strand)` call.
  // When using the strand in this way, it's unbound, and will select a nested scheduler at connect-time.
  struct schedule_sender_ {
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<Tuple<>>;

    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr>;

    static constexpr bool sends_done = false;

    explicit schedule_sender_(strand s) noexcept
    : s(s)
    {}

    schedule_sender_(const schedule_sender_&) = delete;
    schedule_sender_(schedule_sender_&&) = default;

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t connect, schedule_sender_&& self, Receiver&& r) {
      auto get_scheduler = [&self, &r]() -> execution::scheduler auto {
        if constexpr(std::invocable<execution::tag_t<execution::get_scheduler>, std::remove_cvref_t<Receiver>&>) {
          return self.s.scheduler(execution::get_scheduler(r));
        } else {
          return self.s.scheduler(blocking_scheduler<allocator_type>(self.s.get_allocator()));
        }
      };

      return connect(execution::schedule(get_scheduler()), std::forward<Receiver>(r));
    }

    friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_value_t> tag, const schedule_sender_& self) -> strand {
      return self.s;
    }

    friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_error_t> tag, const schedule_sender_& self) -> strand {
      return self.s;
    }

    friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_done_t> tag, const schedule_sender_& self) -> strand {
      return self.s;
    }

    private:
    [[no_unique_address]] strand s;
  };

  // Sender used in a `on(strand)` call.
  // When using the strand in this way, it's unbound, and will select a nested scheduler at connect-time.
  template<typed_sender Sender>
  struct on_sender_
  : _generic_sender_wrapper<on_sender_<Sender>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>
  {
    template<typename, sender, typename...> friend class ::earnest::execution::_generic_sender_wrapper;

    public:
    on_sender_(strand self, Sender&& s)
    : _generic_sender_wrapper<on_sender_<Sender>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>(std::move(s)),
      self(self)
    {}

    template<receiver Receiver>
    friend auto tag_invoke(connect_t connect, on_sender_&& self, Receiver&& r)
    -> operation_state auto {
      auto get_scheduler = [&self, &r]() -> execution::scheduler auto {
        if constexpr(std::invocable<execution::tag_t<execution::get_scheduler>, std::remove_cvref_t<Receiver>&>) {
          return self.self.scheduler(execution::get_scheduler(r));
        } else {
          return self.self.scheduler(blocking_scheduler<allocator_type>(self.self.get_allocator()));
        }
      };

      return connect(execution::on(get_scheduler(), std::move(self.s)), std::forward<Receiver>(r));
    }

    private:
    // Cannot do `some_sender | *this`.
    template<sender OtherSender>
    auto rebind(OtherSender&&) && = delete;

    [[no_unique_address]] strand self;
  };

  explicit strand(state_ptr s) noexcept
  : s(s)
  {}

  public:
  explicit strand(allocator_type alloc = allocator_type())
  : strand(new_state(alloc))
  {}

  auto operator==(const strand<allocator_type>& y) const noexcept -> bool {
    return s == y.s;
  }

  auto operator!=(const strand<allocator_type>& y) const noexcept -> bool {
    return !(*this == y);
  }

  auto get_allocator() const -> allocator_type {
    return s->get_allocator();
  }

  auto running_in_this_thread() const noexcept -> bool {
    return s->running_in_this_thread();
  }

  auto try_lock() -> bool {
    return s->try_lock();
  }

  auto lock() -> void {
    s->lock();
  }

  auto unlock() -> void {
    s->unlock();
  }

  private:
  template<typed_sender Sender, receiver Receiver>
  class wrap_opstate
  : operation_state_base_
  {
    private:
    // This receiver is responsible for releasing the strand.
    class release_receiver
    : public _generic_receiver_wrapper<_generic_rawptr_receiver<Receiver>, set_value_t, set_error_t, set_done_t>
    {
      public:
      release_receiver(_generic_rawptr_receiver<Receiver>&& r, state_ptr sptr)
      : _generic_receiver_wrapper<_generic_rawptr_receiver<Receiver>, set_value_t, set_error_t, set_done_t>(std::move(r)),
        sptr(sptr)
      {}

      template<typename... Args>
      friend auto tag_invoke(set_value_t set_value, release_receiver&& self, Args&&... args) noexcept -> void {
        self.sptr->release();
        try {
          set_value(std::move(self.r), std::forward<Args>(args)...);
        } catch (...) { // We must handle the exception here, otherwise we would double-release the strand.
          execution::set_error(std::move(self.r), std::current_exception());
        }
      }

      template<typename Error>
      friend auto tag_invoke(set_error_t set_error, release_receiver&& self, Error&& error) noexcept -> void {
        self.sptr->release();
        set_error(std::move(self.r), std::forward<Error>(error));
      }

      friend auto tag_invoke(set_done_t set_done, release_receiver&& self) noexcept -> void {
        self.sptr->release();
        set_done(std::move(self.r));
      }

      private:
      state_ptr sptr;
    };

    static auto create_nested_opstate(Sender&& s, Receiver& r, state_ptr sptr) {
      return execution::connect(
          // We need to call mark_running, before letting sender `s` take over.
          just(sptr, std::move(s))
          | lazy_let_value(
              [](const state_ptr& sptr, Sender& s) -> Sender {
                sptr->mark_running(); // Never throws.
                return std::move(s);
              }),
          // And once sender `s` completes, we need to release the strand, prior to handing control to the real receiver.
          release_receiver(_generic_rawptr_receiver(&r), sptr));
    }
    using nested_opstate_type = decltype(create_nested_opstate(std::declval<Sender>(), std::declval<Receiver&>(), std::declval<state_ptr>()));

    public:
    wrap_opstate(Sender&& s, Receiver&& r, state_ptr sptr)
    : r(std::move(r)),
      sptr(sptr),
      nested_opstate(create_nested_opstate(std::move(s), this->r, sptr))
    {}

    friend auto tag_invoke([[maybe_unused]] start_t start, wrap_opstate& self) noexcept -> void {
      try {
        if (self.scheduler_may_block()) {
          self.sptr->block();
          start(self.nested_opstate); // Never throws.
        } else {
          self.sptr->run_or_enqueue(
              entry{
                .cb=[](void* self_vptr) noexcept -> void {
                  const auto self_ptr = static_cast<wrap_opstate*>(self_vptr);
                  execution::start(self_ptr->nested_opstate);
                },
                .ptr=&self,
              });
        }
      } catch (...) { // If we fail to start the nested receiver, we'll propagate the exception.
        execution::set_error(std::move(self.r), std::current_exception()); // Never throws.
      }
    }

    private:
    auto scheduler_may_block() const -> bool {
      if constexpr(requires { execution::get_scheduler(r); })
        return execution::execute_may_block_caller(execution::get_scheduler(r));
      else // If there's no scheduler, start(nested_opstate) will always block.
        return true;
    }

    Receiver r;
    state_ptr sptr;
    nested_opstate_type nested_opstate;
  };

  template<typed_sender Sender>
  class wrap_sender_impl
  : public _generic_sender_wrapper<wrap_sender_impl<Sender>, Sender, connect_t>
  {
    public:
    wrap_sender_impl(Sender&& s, state_ptr sptr)
    : _generic_sender_wrapper<wrap_sender_impl<Sender>, Sender, connect_t>(std::move(s)),
      sptr(sptr)
    {}

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t connect, wrap_sender_impl&& self, Receiver&& r) -> operation_state auto {
      return wrap_opstate<Sender, std::remove_cvref_t<Receiver>>(std::move(self.s), std::forward<Receiver>(r), self.sptr);
    }

    private:
    state_ptr sptr;
  };

  public:
  template<typed_sender Sender>
  auto wrap(Sender&& s) const -> typed_sender auto {
    return wrap_sender_impl<std::remove_cvref_t<Sender>>(std::forward<Sender>(s), this->s);
  }

  // Adapt an existing scheduler, so that its scheduled tasks run on this strand.
  template<execution::scheduler UnderlyingSched>
  auto scheduler(UnderlyingSched&& underlying_sched) const
  noexcept(std::is_nothrow_constructible_v<std::remove_cvref_t<UnderlyingSched>, UnderlyingSched>)
  -> bound_scheduler_t<std::remove_cvref_t<UnderlyingSched>> {
    return bound_scheduler_t<std::remove_cvref_t<UnderlyingSched>>(*this, std::forward<UnderlyingSched>(underlying_sched));
  }

  friend auto tag_invoke(schedule_t tag, strand self) -> sender_of<> auto {
    return schedule_sender_(self);
  }

  template<typed_sender Sender>
  friend auto tag_invoke(lazy_on_t tag, strand self, Sender s) -> typed_sender auto {
    return on_sender_<std::remove_cvref_t<Sender>>(self, std::move(s));
  }

  private:
  state_ptr s;
};

static_assert(scheduler<strand<>>, "strand should be a scheduler");
// Confirm we did the specialization for schedule_t correct.
static_assert(tag_invocable<schedule_t, strand<>>);
static_assert(tag_invocable<schedule_t, const strand<>&>);
static_assert(tag_invocable<schedule_t, strand<>&&>);
static_assert(tag_invocable<schedule_t, strand<>&>);
// Confirm we did the override for lazy_on_t correct.
static_assert(tag_invocable<lazy_on_t, strand<>, decltype(just())>);
static_assert(tag_invocable<lazy_on_t, const strand<>&, decltype(just())>);
static_assert(tag_invocable<lazy_on_t, strand<>&&, decltype(just())>);
static_assert(tag_invocable<lazy_on_t, strand<>&, decltype(just())>);


template<typename>
struct is_strand_bound_scheduler_ : std::false_type {};
template<typename Alloc, typename UnderlyingScheduler>
struct is_strand_bound_scheduler_<strand_bound_scheduler_<Alloc, UnderlyingScheduler>> : std::true_type {};

template<typename>
struct is_strand_ : std::false_type {};
template<typename Alloc>
struct is_strand_<strand<Alloc>> : std::true_type {};

template<typename Alloc, execution::scheduler UnderlyingScheduler>
class strand_bound_scheduler_ {
  friend struct not_on_strand_t;

  static_assert(!is_strand_bound_scheduler_<UnderlyingScheduler>::value);
  static_assert(!is_strand_<UnderlyingScheduler>::value);

  public:
  explicit strand_bound_scheduler_(strand<Alloc> s, const UnderlyingScheduler& underlying_sched)
  noexcept(std::is_nothrow_copy_constructible_v<UnderlyingScheduler>)
  : s(s),
    underlying_sched(underlying_sched)
  {}

  explicit strand_bound_scheduler_(typename strand<Alloc>::state_ptr s, UnderlyingScheduler&& underlying_sched)
  noexcept(std::is_nothrow_move_constructible_v<UnderlyingScheduler>)
  : s(s),
    underlying_sched(std::move(underlying_sched))
  {}

  auto operator==(const strand_bound_scheduler_& y) const noexcept -> bool {
    return s == y.s && underlying_sched == y.underlying_sched;
  }

  auto operator!=(const strand_bound_scheduler_& y) const noexcept -> bool {
    return !(*this == y);
  }

  friend auto tag_invoke([[maybe_unused]] execution::schedule_t tag, strand_bound_scheduler_&& self)
  -> typename strand<Alloc>::template sender_impl<UnderlyingScheduler> {
    return typename strand<Alloc>::template sender_impl<UnderlyingScheduler>(self.s.s, self.underlying_sched);
  }

  friend auto tag_invoke(get_idle_scheduler_t get_idle_scheduler, const strand_bound_scheduler_& self) {
    return self.s.scheduler(get_idle_scheduler(self.underlying_sched));
  }

  private:
  strand<Alloc> s;
  UnderlyingScheduler underlying_sched;
};


// Run a task, but ensure it won't be running on a strand.
//
// Takes the receiver scheduler (which must be a strand), and extracts the underlying scheduler.
// The task is then run on the unwrapped scheduler.
//
// Afterwards, the task is then transferred to the strand scheduler that was on the receiver.
struct not_on_strand_t {
  private:
  template<receiver Receiver>
  class receiver_without_scheduler
  : public _generic_receiver_wrapper<Receiver, get_scheduler_t>
  {
    public:
    explicit receiver_without_scheduler(Receiver&& r)
    : _generic_receiver_wrapper<Receiver, get_scheduler_t>(std::move(r))
    {}
  };

  template<typed_sender Sender, typename Alloc>
  class sender_impl
  : public _generic_sender_wrapper<sender_impl<Sender, Alloc>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>
  {
    public:
    explicit sender_impl(Sender s, strand<Alloc> expected_strand)
    : _generic_sender_wrapper<sender_impl<Sender, Alloc>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>(std::move(s)),
      expected_strand(expected_strand)
    {}

    template<receiver Receiver>
    requires (
        is_strand_<tag_invoke_result_t<get_scheduler_t, const std::remove_cvref_t<Receiver>&>>::value ||
        is_strand_bound_scheduler_<tag_invoke_result_t<get_scheduler_t, const std::remove_cvref_t<Receiver>&>>::value)
    friend auto tag_invoke(connect_t connect, sender_impl&& self, Receiver&& r) {
      static_assert(std::invocable<get_scheduler_t, std::remove_reference_t<Receiver>&>,
          "receiver must have a scheduler");
      auto sch = get_scheduler(r);
      using scheduler_type = decltype(sch);
      static_assert(is_strand_<scheduler_type>::value || is_strand_bound_scheduler_<scheduler_type>::value,
          "receiver scheduler must be a strand-based scheduler");

      if constexpr(is_strand_<scheduler_type>::value) {
        assert(sch == self.expected_strand);
        if (sch != self.expected_strand) throw std::logic_error("strand expectation not met");
        return connect(
            std::move(self.s) | execution::transfer(sch),
            receiver_without_scheduler<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r)));
      } else {
        assert(sch.s == self.expected_strand);
        if (sch.s != self.expected_strand) throw std::logic_error("strand expectation not met");
        return connect(
            execution::on(sch.underlying_sched, std::move(self.s) | execution::transfer(sch)),
            std::forward<Receiver>(r));
      }
    }

    private:
    strand<Alloc> expected_strand;
  };

  public:
  template<typename Alloc, typed_sender Sender>
  auto operator()(strand<Alloc> expected_strand, Sender&& s) const -> typed_sender auto {
    return sender_impl<std::remove_cvref_t<Sender>, Alloc>(std::forward<Sender>(s), std::move(expected_strand));
  }
};
inline constexpr not_on_strand_t not_on_strand;


} /* inline namespace extensions */
} /* namespace earnest::execution */
