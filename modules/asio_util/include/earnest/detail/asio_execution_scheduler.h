#pragma once

#include <concepts>
#include <exception>
#include <utility>
#include <type_traits>

#include <earnest/execution.h>

namespace earnest::detail {


// Adapt an asio-executor into a senders/receivers scheduler.
//
// In order to be compatible with both plain-asio, and boost-asio,
// this code uses ADL-lookup to locate the `post' operation.
template<typename Executor>
requires
    std::copy_constructible<Executor> &&
    std::equality_comparable<Executor> &&
    requires (const Executor ex, void (*fn)()) {
      { post(ex, fn) };
    }
class asio_execution_scheduler {
  public:
  using executor_type = Executor;

  private:
  template<execution::receiver Receiver>
  class opstate
  : public execution::operation_state_base_
  {
    public:
    explicit opstate(executor_type&& ex, Receiver&& r)
    : ex(std::move(ex)),
      r(std::move(r))
    {}

    friend auto tag_invoke([[maybe_unused]] execution::start_t start, opstate& self) noexcept -> void {
      try {
        post( // ADL
            self.ex,
            [&self]() {
              try {
                execution::set_value(std::move(self.r));
              } catch (...) {
                execution::set_error(std::move(self.r), std::current_exception());
              }
            });
      } catch (...) {
        execution::set_error(std::move(self.r), std::current_exception());
      }
    }

    private:
    executor_type ex;
    Receiver r;
  };

  class sender_impl {
    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<Tuple<>>;

    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr>;

    static inline constexpr bool sends_done = false;

    explicit sender_impl(asio_execution_scheduler&& sch)
    : sch(std::move(sch))
    {}

    template<execution::receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] execution::connect_t connect, sender_impl&& self, Receiver&& r) -> opstate<std::remove_cvref_t<Receiver>> {
      return opstate<std::remove_cvref_t<Receiver>>(std::move(self), std::forward<Receiver>(r));
    }

    friend auto tag_invoke([[maybe_unused]] execution::get_completion_scheduler_t<execution::set_value_t> tag, const sender_impl& self) noexcept -> const asio_execution_scheduler& {
      return self.sch;
    }

    friend auto tag_invoke([[maybe_unused]] execution::get_completion_scheduler_t<execution::set_error_t> tag, const sender_impl& self) noexcept -> const asio_execution_scheduler& {
      return self.sch;
    }

    friend auto tag_invoke([[maybe_unused]] execution::get_completion_scheduler_t<execution::set_done_t> tag, const sender_impl& self) noexcept -> const asio_execution_scheduler& {
      return self.sch;
    }

    private:
    asio_execution_scheduler sch;
  };

  public:
  explicit asio_execution_scheduler(const executor_type& ex) noexcept(std::is_nothrow_copy_constructible_v<executor_type>)
  : ex(ex)
  {}

  explicit asio_execution_scheduler(executor_type&& ex) noexcept(std::is_nothrow_move_constructible_v<executor_type>)
  : ex(std::move(ex))
  {}

  auto get_executor() const noexcept -> const executor_type& {
    return ex;
  }

  auto operator==(const asio_execution_scheduler& y) const noexcept -> bool {
    return ex == y.ex;
  }

  friend auto tag_invoke([[maybe_unused]] execution::schedule_t tag, asio_execution_scheduler self) -> sender_impl {
    return sender_impl(std::move(self));
  }

  friend auto tag_invoke([[maybe_unused]] execution::get_forward_progress_guarantee_t tag, const asio_execution_scheduler& self) noexcept -> execution::forward_progress_guarantee {
    return execution::forward_progress_guarantee::parallel;
  }

  friend auto tag_invoke([[maybe_unused]] execution::execute_may_block_caller_t tag, [[maybe_unused]] const asio_execution_scheduler& self) noexcept -> bool {
    return false;
  }

  template<std::invocable<> Fn>
  friend auto tag_invoke([[maybe_unused]] execution::execute_t tag, asio_execution_scheduler self, Fn&& fn) -> void {
    // ADL
    post(self.ex, std::forward<Fn>(fn));
  }

  private:
  executor_type ex;
};


} /* namespace earnest::detail */
