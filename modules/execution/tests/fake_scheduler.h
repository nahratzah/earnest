#pragma once

#include <earnest/execution.h>

using namespace earnest::execution;

template<bool is_noexcept>
struct fake_scheduler {
  template<typename Receiver>
  struct op_state {
    friend auto tag_invoke([[maybe_unused]] start_t, op_state& self) noexcept -> void {
      if (self.sch.start_mark != nullptr) *self.sch.start_mark = true;

      try {
        set_value(std::move(self.rcv));
      } catch (...) {
        set_error(std::move(self.rcv), std::current_exception());
      }
    }

    fake_scheduler sch;
    Receiver rcv;
  };

  struct sender_impl {
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<Tuple<>>;

    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr>;

    static constexpr bool sends_done = false;

    template<typename Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& receiver) noexcept(is_noexcept) -> op_state<std::remove_cvref_t<Receiver>> {
      return {std::move(self.sch), std::forward<Receiver>(receiver)};
    }

    friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_value_t>, const sender_impl& self) noexcept -> fake_scheduler {
      return self.sch;
    }
    friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_error_t>, const sender_impl& self) noexcept -> fake_scheduler {
      return self.sch;
    }
    friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_done_t>, const sender_impl& self) noexcept -> fake_scheduler {
      return self.sch;
    }

    fake_scheduler sch;
  };

  friend auto tag_invoke([[maybe_unused]] schedule_t, fake_scheduler&& self) noexcept(is_noexcept) -> sender_impl {
    return {self};
  }

  friend auto tag_invoke([[maybe_unused]] schedule_t, fake_scheduler& self) noexcept(is_noexcept) -> sender_impl {
    return {self};
  }

  constexpr auto operator==([[maybe_unused]] const fake_scheduler&) const noexcept { return true; }
  constexpr auto operator!=([[maybe_unused]] const fake_scheduler&) const noexcept { return false; }

  // If this pointer is non-null, will set it to `true` once the scheduler is started.
  bool* start_mark = nullptr;
};
