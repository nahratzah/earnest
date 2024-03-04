#pragma once

#include <cstddef>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <utility>

#include <gsl/gsl>

#include <earnest/execution.h>

namespace earnest::execution {
inline namespace extensions {


template<typename Alloc = std::allocator<std::byte>>
class blocking_scheduler {
  public:
  using allocator_type = Alloc;

  private:
  template<typename T>
  using rebind_alloc = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;

  using callback_type = void (*)(void*) noexcept;
  struct entry {
    callback_type cb;
    void* ptr;

    auto operator()() const noexcept -> void {
      std::invoke(cb, ptr);
    }
  };

  class state {
    private:
    using queue_type = std::queue<entry, std::deque<entry, rebind_alloc<entry>>>;

    public:
    explicit state(allocator_type alloc)
    : q(alloc)
    {}

    state(const state&) = delete;
    state(state&&) = delete;

#ifndef NDEBUG
    ~state() {
      std::lock_guard lck{mtx};
      assert(q.empty());
    }
#endif

    auto get_allocator() const -> allocator_type {
      return q.get_allocator();
    }

    template<operation_state OpState>
    auto push(OpState& opstate) -> void {
      using opstate_type = std::remove_cvref_t<OpState>;

      std::lock_guard lck{mtx};
      // Error if we won't run the enqueued tasks.
      if (no_more_enqueues) throw std::logic_error("blocking_scheduler: attempt to schedule task on scheduler that'll not be run");

      q.push(
          entry{
            .cb=[](void* opstate_vptr) {
              gsl::not_null<opstate_type*> opstate = static_cast<opstate_type*>(opstate_vptr);
              execution::start(*opstate);
            },
            .ptr=&opstate,
          });
    }

    // Run all enqueued tasks.
    // - block_further_enqueues_when_empty:
    //   If set (default) the queue will be marked as "won't run again".
    //   This will cause further attempts to enqueue tasks to result in an exception.
    //
    //   Set this to `false`, if you intend to run the queue again at a later moment.
    auto drain(bool block_further_enqueues_when_empty = true) noexcept {
      // Drain the queue of tasks.
      // Note that we run each task outside the mutex,
      // so that it may call push, without deadlocking.
      while (auto opt_entry = pop_entry(block_further_enqueues_when_empty))
        std::invoke(*opt_entry);
    }

    private:
    auto pop_entry(bool block_further_enqueues_when_empty) noexcept -> std::optional<entry> {
      std::lock_guard lck{mtx};
      if (q.empty()) {
        if (block_further_enqueues_when_empty) no_more_enqueues = true;
        return std::nullopt;
      }
      entry e = std::move(q.front());
      q.pop();
      return e;
    }

    std::mutex mtx;
    queue_type q;
    bool no_more_enqueues = false;
  };
  using state_ptr = gsl::not_null<std::shared_ptr<state>>;

  static auto new_state(allocator_type alloc) -> state_ptr {
    return std::allocate_shared<state>(alloc, alloc);
  }

  template<receiver Receiver>
  class opstate_impl
  : private operation_state_base_
  {
    public:
    explicit opstate_impl(blocking_scheduler s, Receiver&& r)
    : s(s),
      r(std::move(r))
    {}

    friend auto tag_invoke(start_t start, opstate_impl& self) noexcept -> void {
      state_ptr s = self.s.s; // Must have a copy, because once nested_opstate completes, *this may be invalidated.
                              // And that would mean we would segfault during drain, due to the dangling *this pointer.

      // Complete the receiver.
      try {
        execution::set_value(std::move(self.r));
      } catch (...) {
        execution::set_error(std::move(self.r), std::current_exception());
      }

      s->drain(); // *this may be a dangling pointer here, so we run on a stack-local copy of the scheduler instead.
    }

    private:
    blocking_scheduler s;
    Receiver r;
  };

  class sender_impl {
    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<Tuple<>>;

    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr>;

    static constexpr bool sends_done = false;

    explicit sender_impl(blocking_scheduler s)
    : s(s)
    {}

    template<receiver Receiver>
    friend auto tag_invoke(connect_t connect, sender_impl&& self, Receiver&& r) {
      return opstate_impl<std::remove_cvref_t<Receiver>>(self.s, std::forward<Receiver>(r));
    }

    friend auto tag_invoke(get_completion_scheduler_t<set_value_t> tag, const sender_impl& self) -> blocking_scheduler {
      return self.s;
    }

    friend auto tag_invoke(get_completion_scheduler_t<set_error_t> tag, const sender_impl& self) -> blocking_scheduler {
      return self.s;
    }

    friend auto tag_invoke(get_completion_scheduler_t<set_done_t> tag, const sender_impl& self) -> blocking_scheduler {
      return self.s;
    }

    private:
    blocking_scheduler s;
  };

  public:
  explicit blocking_scheduler(allocator_type alloc = allocator_type())
  : s(new_state(alloc))
  {}

  auto operator==(const blocking_scheduler& y) const noexcept -> bool {
    return s == y.s;
  }

  auto operator!=(const blocking_scheduler& y) const noexcept -> bool {
    return s == y.s;
  }

  friend auto tag_invoke(schedule_t schedule, blocking_scheduler self) {
    return sender_impl(self);
  }

  friend constexpr auto tag_invoke([[maybe_unused]] execute_may_block_caller_t, [[maybe_unused]] const blocking_scheduler& self) noexcept -> bool {
    return false;
  }

  private:
  state_ptr s;
};

static_assert(scheduler<blocking_scheduler<>>, "blocking_scheduler should be a scheduler");


} /* inline namespace extensions */
} /* namespace earnest::execution */
