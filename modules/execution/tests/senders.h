#pragma once

#include <earnest/execution.h>

#include <type_traits>
#include <utility>

using namespace earnest::execution;


// Adapter.
// Pretends that the receiver has a specific scheduler.
struct pretend_scheduler_t {
  template<sender Sender, scheduler Scheduler>
  auto operator()(Sender&& s, Scheduler&& sch) const {
    return sender_impl<std::remove_cvref_t<Sender>, std::remove_cvref_t<Scheduler>>(std::forward<Sender>(s), std::forward<Scheduler>(sch));
  }

  template<scheduler Scheduler>
  auto operator()(Scheduler&& sch) const {
    return _generic_adapter(*this, std::forward<Scheduler>(sch));
  }

  private:
  template<receiver Receiver, scheduler Scheduler>
  class receiver_impl
  : public _generic_receiver_wrapper<Receiver, get_scheduler_t>
  {
    public:
    template<typename Receiver_, typename Scheduler_>
    explicit receiver_impl(Receiver_&& r, Scheduler_&& sch)
    : _generic_receiver_wrapper<Receiver, get_scheduler_t>(std::forward<Receiver_>(r)),
      sch(std::forward<Scheduler_>(sch))
    {}

    friend auto tag_invoke([[maybe_unused]] get_scheduler_t, const receiver_impl& self) noexcept {
      return self.sch;
    }

    private:
    Scheduler sch;
  };

  template<sender Sender, scheduler Scheduler>
  class sender_impl
  : public _generic_sender_wrapper<sender_impl<Sender, Scheduler>, Sender, connect_t>
  {
    public:
    template<typename Sender_, typename Scheduler_>
    explicit sender_impl(Sender_&& s, Scheduler_&& sch)
    : _generic_sender_wrapper<sender_impl<Sender, Scheduler>, Sender, connect_t>(std::forward<Sender_>(s)),
      sch(std::forward<Scheduler_>(sch))
    {}

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r) {
      return connect(
          std::move(self.s),
          receiver_impl<std::remove_cvref_t<Receiver>, Scheduler>(std::forward<Receiver>(r), std::move(self.sch)));
    }

    private:
    Scheduler sch;
  };
};
inline constexpr pretend_scheduler_t pretend_scheduler{};


// Adapter.
// Pretends that the receiver has no scheduler.
struct pretend_no_scheduler_t {
  template<sender Sender>
  auto operator()(Sender&& s) const {
    return sender_impl<std::remove_cvref_t<Sender>>(std::forward<Sender>(s));
  }

  auto operator()() const {
    return _generic_adapter(*this);
  }

  private:
  template<receiver Receiver>
  class receiver_impl
  : public _generic_receiver_wrapper<Receiver, get_scheduler_t>
  {
    public:
    template<typename Receiver_>
    explicit receiver_impl(Receiver_&& r)
    : _generic_receiver_wrapper<Receiver, get_scheduler_t>(std::forward<Receiver_>(r))
    {}
  };

  template<sender Sender>
  class sender_impl
  : public _generic_sender_wrapper<sender_impl<Sender>, Sender, connect_t>
  {
    public:
    template<typename Sender_>
    explicit sender_impl(Sender_&& s)
    : _generic_sender_wrapper<sender_impl<Sender>, Sender, connect_t>(std::forward<Sender_>(s))
    {}

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r) {
      return connect(
          std::move(self.s),
          receiver_impl<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r)));
    }
  };
};
inline constexpr pretend_no_scheduler_t pretend_no_scheduler{};
