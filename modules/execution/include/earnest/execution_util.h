/* Additional utilities on top of execution.h
 */
#pragma once

#include "execution.h"

#include <cstddef>
#include <exception>
#include <functional>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

namespace earnest::execution {
inline namespace extensions {


// Repeat a certain set of operations.
//
// Usage:
// `just(myArgs...)
//  | repeat([](std::size_t idx, myArgs&...) {
//      if (should_not_stop) {
//        return std::make_optional(
//            just()
//            | ...);
//      } else {
//        return std::nullopt;
//      }
//    })
//  | then([](myArgs&&...) {
//      ...;
//    })
// `
//
// The `repeat' adapter accepts zero or more arguments.
// It holds on to those arguments.
// It will then repeatedly invoke the function-argument,
// stopping only if the function return value indicates
// it should stop.
//
// When the repeat operation stops successfully, forwards
// the original arguments onward.
struct lazy_repeat_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender S, typename Fn>
  constexpr auto operator()(S&& s, Fn&& fn) const
  noexcept(noexcept(_generic_operand_base<set_value_t>(std::declval<const lazy_repeat_t&>(), std::declval<S>(), std::declval<Fn>())))
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<S>(s), std::forward<Fn>(fn));
  }

  template<typename Fn>
  constexpr auto operator()(Fn&& fn) const
  noexcept(noexcept(_generic_adapter(std::declval<lazy_repeat_t>(), std::declval<Fn>())))
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<typename... T>
  requires (!std::is_reference_v<T> &&...) && (!std::is_const_v<T> &&...)
  class values_container {
    public:
    template<typename... T_>
    explicit values_container(T_&&... v)
    noexcept(std::is_nothrow_constructible_v<std::tuple<T...>, T_...>)
    : values(std::forward<T_>(v)...)
    {}

    template<std::invocable<T&...> Fn>
    auto apply(Fn&& fn) & -> decltype(auto) {
      return std::apply(std::forward<Fn>(fn), values);
    }

    template<std::invocable<T&&...> Fn>
    auto apply(Fn&& fn) && -> decltype(auto) {
      return std::apply(std::forward<Fn>(fn), std::move(values));
    }

    private:
    std::tuple<T...> values;
  };

  template<typename TypeAppender>
  using type_appender_to_values_container =
      typename TypeAppender::
      template transform<std::remove_cvref_t>::
      template type<values_container>;

  // Determine if a receiver has an associated scheduler.
  template<receiver Receiver, typename = void>
  static inline constexpr bool has_scheduler = false;
  // Specialization of has_scheduler, for receivers which have a scheduler.
  template<receiver Receiver>
  static inline constexpr bool has_scheduler<Receiver, std::void_t<decltype(::earnest::execution::get_scheduler(std::declval<Receiver>()))>> = true;

  // Operation-state, that's used for if we have a scheduler.
  //
  // The operation-state grabs hold of the arguments, and then repeatedly enqueues
  // the operation returned by the function. (One at a time.)
  template<sender Sender, receiver Receiver, typename Fn>
  class opstate {
    private:
    // Compute all the container types.
    // `_type_appender<values_container<...>, values_container<...>, ...>'
    using all_container_types = typename sender_traits<Sender>::template value_types<_type_appender, _type_appender>::
        template transform<type_appender_to_values_container>;

    // Compute the variant type.
    //
    // The computed variant has a `std::monostate' as the first type, so that it'll initialize to that.
    // The monostate indicates that no values have been assigned.
    using variant_type =
        typename _type_appender<>::merge<                                                                        // The union of
            _type_appender<std::monostate>,                                                                      // monostate (indicating there is
                                                                                                                 // as yet no values present)
            all_container_types                                                                                  // each of the possible value-types
                                                                                                                 // `_type_appender<
                                                                                                                 //     values_container<...>,
                                                                                                                 //     values_container<...>,
                                                                                                                 //     ...>'
        >::                                                                                                      //
        template type<std::variant>;                                                                             // All placed into a std::variant.

    // Local-receiver accepts the result of the fn-sender.
    // It wraps a receiver (the one we get from `start_detached')
    // and holds a reference to both the state and the container-values.
    //
    // It then:
    // - invokes the state.apply function, if the value-signal was received.
    // - otherwise passes the signal down the operation-chain.
    // In all cases, it'll complete the wrapped-receiver (using the done-signal).
    template<receiver NestedReceiver, typename ValuesContainer>
    class local_receiver
    : public _generic_receiver_wrapper<NestedReceiver, set_value_t, set_error_t, set_done_t>
    {
      public:
      explicit local_receiver(opstate& state, ValuesContainer& container, NestedReceiver&& r)
      noexcept(std::is_nothrow_move_constructible_v<NestedReceiver>)
      : _generic_receiver_wrapper<NestedReceiver, set_value_t, set_error_t, set_done_t>(std::move(r)),
        state(state),
        container(container)
      {}

      explicit local_receiver(opstate& state, const NestedReceiver& r)
      noexcept(std::is_nothrow_copy_constructible_v<NestedReceiver>)
      : _generic_receiver_wrapper<NestedReceiver, set_value_t, set_error_t, set_done_t>(r),
        state(state)
      {}

      template<typename... Args>
      friend auto tag_invoke([[maybe_unused]] set_value_t, local_receiver&& self, [[maybe_unused]] Args&&...) noexcept -> void {
        try {
          self.state.apply(self.container);
        } catch (...) {
          ::earnest::execution::set_error(std::move(self.state.r), std::current_exception());
        }

        self.complete();
      }

      template<typename Error>
      friend auto tag_invoke([[maybe_unused]] set_error_t, local_receiver&& self, Error&& error) noexcept -> void {
        ::earnest::execution::set_error(std::move(self.state.r), std::forward<Error>(error));
        self.complete();
      }

      friend auto tag_invoke([[maybe_unused]] set_done_t, local_receiver&& self) noexcept -> void {
        ::earnest::execution::set_done(std::move(self.state.r));
        self.complete();
      }

      private:
      auto complete() noexcept -> void {
        execution::set_done(std::move(this->r));
      }

      opstate& state;
      ValuesContainer& container;
    };

    // Local-sender wraps the sender from the function.
    //
    // When connected, it'll wrap the receiver in local-receiver.
    // That receiver will handle propagation into opstate.
    template<typed_sender FnSender, typename ValuesContainer>
    class local_sender
    : public _generic_sender_wrapper<local_sender<FnSender, ValuesContainer>, FnSender, connect_t>
    {
      public:
      template<template<typename...> class Tuple, template<typename...> class Variant>
      using value_types = Variant<Tuple<>>;

      static inline constexpr bool sends_done = true;

      explicit local_sender(opstate& state, ValuesContainer& container, FnSender&& fn_sender)
      noexcept(std::is_nothrow_move_constructible_v<FnSender>)
      : _generic_sender_wrapper<local_sender<FnSender, ValuesContainer>, FnSender, connect_t>(std::move(fn_sender)),
        state(state),
        container(container)
      {}

      explicit local_sender(opstate& state, ValuesContainer& container, const FnSender& fn_sender)
      noexcept(std::is_nothrow_copy_constructible_v<FnSender>)
      : _generic_sender_wrapper<local_sender<FnSender, ValuesContainer>, FnSender, connect_t>(fn_sender),
        state(state),
        container(container)
      {}

      template<receiver NestedReceiver>
      friend auto tag_invoke([[maybe_unused]] connect_t, local_sender&& self, NestedReceiver&& r)
      -> decltype(auto) {
        return execution::connect(
            std::move(self.s),
            local_receiver<std::remove_cvref_t<NestedReceiver>, ValuesContainer>(self.state, self.container, std::forward<NestedReceiver>(r)));
      }

      private:
      opstate& state;
      ValuesContainer& container;
    };

    // Receiver that delegates to this opstate.
    class accepting_receiver {
      public:
      explicit accepting_receiver(opstate& state) noexcept
      : state(state)
      {}

      // When the set-value signal is receiver, pass control to the opstate.
      template<typename... Args>
      friend auto tag_invoke([[maybe_unused]] set_value_t, accepting_receiver&& self, Args&&... args) -> void {
        self.state.start(std::forward<Args>(args)...);
      }

      // Forward all other tags to the wrapped receiver.
      // This version accepts const-reference accepting_receiver.
      template<typename Tag, typename... Args,
          typename = std::enable_if_t<_is_forwardable_receiver_tag<Tag>>,
          typename = std::enable_if_t<std::negation_v<std::is_same<set_value_t, Tag>>>>
      friend auto tag_invoke(Tag tag, accepting_receiver& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, const Receiver&, Args...>)
      -> tag_invoke_result_t<Tag, const Receiver&, Args...> {
        execution::tag_invoke(std::move(tag), self.state.r, std::forward<Args>(args)...);
      }

      // Forward all other tags to the wrapped receiver.
      // This version accepts non-const lvalue-reference accepting_receiver.
      template<typename Tag, typename... Args,
          typename = std::enable_if_t<_is_forwardable_receiver_tag<Tag>>,
          typename = std::enable_if_t<std::negation_v<std::is_same<set_value_t, Tag>>>>
      friend auto tag_invoke(Tag tag, accepting_receiver& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, Receiver&, Args...>)
      -> tag_invoke_result_t<Tag, Receiver&, Args...> {
        execution::tag_invoke(std::move(tag), self.state.r, std::forward<Args>(args)...);
      }

      // Forward all other tags to the wrapped receiver.
      // This version accepts rvalue-reference accepting_receiver.
      template<typename Tag, typename... Args,
          typename = std::enable_if_t<_is_forwardable_receiver_tag<Tag>>,
          typename = std::enable_if_t<std::negation_v<std::is_same<set_value_t, Tag>>>>
      friend auto tag_invoke(Tag tag, accepting_receiver&& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, Receiver&&, Args...>)
      -> tag_invoke_result_t<Tag, Receiver&&, Args...> {
        execution::tag_invoke(std::move(tag), std::move(self.state.r), std::forward<Args>(args)...);
      }

      private:
      opstate& state;
    };

    using nested_opstate_type = std::remove_cvref_t<decltype(execution::connect(std::declval<Sender>(), std::declval<accepting_receiver>()))>;

    public:
    explicit opstate(Sender&& s, Receiver&& r, Fn&& fn)
    : r(std::move(r)),
      fn(std::move(fn)),
      nested_opstate(execution::connect(std::move(s), accepting_receiver(*this)))
    {}

    // block copy/move operations, so we are pointer-safe.
    opstate(opstate&&) = delete;
    opstate(const opstate&) = delete;

    friend auto tag_invoke([[maybe_unused]] start_t, opstate& self) noexcept -> decltype(auto) {
      execution::start(self.nested_opstate);
    }

    private:
    template<typename... Args>
    auto start(Args&&... args) -> void {
      using container_type = values_container<std::remove_cvref_t<Args>...>;
      container_type& container = values.template emplace<container_type>(std::forward<Args>(args)...);
      apply(container);
    }

    template<typename ValuesContainer>
    auto apply(ValuesContainer& container) -> void {
      apply(
          container,
          container.apply(
              [this](auto&... args) {
                return std::invoke(this->fn, this->idx++, args...);
              }));
    }

    template<typename ValuesContainer, sender FnSender>
    auto apply(ValuesContainer& container, std::optional<FnSender> opt_fn_sender) -> void {
      if (opt_fn_sender.has_value()) {
        // We start the sub-task in detached mode.
        // The local-sender will create a local-receiver which will intercept all the signals.
        // We use `lazy_on(get_scheduler(r))' so that we won't recurse on the stack.
        execution::start_detached(
            execution::lazy_on(
                execution::get_scheduler(r),
                local_sender<FnSender, ValuesContainer>(*this, container, *std::move(opt_fn_sender))));
      } else {
        std::move(container).apply(
            [this]<typename... Args>(Args&&... args) {
              ::earnest::execution::set_value(std::move(this->r), std::forward<Args>(args)...);
            });
      }
    }

    [[no_unique_address]] Receiver r;
    [[no_unique_address]] Fn fn;
    std::size_t idx = 0;
    variant_type values;
    nested_opstate_type nested_opstate;
  };

  // The loop-receiver is used, if there is no scheduler.
  //
  // In this case, we run in the foreground (using `sync_wait').
  template<receiver Receiver, typename Fn>
  class loop_receiver
  : public _generic_receiver_wrapper<Receiver, set_value_t>
  {
    private:
    template<typename... Args>
    using fn_sender_type = std::remove_cvref_t<std::invoke_result_t<Fn, std::size_t, std::remove_cvref_t<Args>&...>>;

    // Local-receiver is a receiver, that takes the output-signal of the function-returned-sender.
    //
    // If the signal is the value-signal, does nothing, and signals false (indicating we're not done yet).
    // Otherwise, propagates the signal into the loop-receiver, and signals true (indicating we're done).
    template<typename NestedReceiver>
    class local_receiver
    : public _generic_receiver_wrapper<NestedReceiver, set_value_t, set_error_t, set_done_t>
    {
      public:
      explicit local_receiver(NestedReceiver&& r, loop_receiver& lr) noexcept
      : _generic_receiver_wrapper<NestedReceiver, set_value_t, set_error_t, set_done_t>(std::move(r)),
        lr(lr)
      {}

      template<typename... Args>
      friend auto tag_invoke([[maybe_unused]] set_value_t, local_receiver&& self, [[maybe_unused]] Args&&...) noexcept -> void {
        self.complete(false);
      }

      template<typename Error>
      friend auto tag_invoke([[maybe_unused]] set_error_t, local_receiver&& self, Error&& error) noexcept -> void {
        execution::set_error(std::move(self.lr.r), std::forward<Error>(error));
        self.complete(true);
      }

      friend auto tag_invoke([[maybe_unused]] set_done_t, local_receiver&& self) noexcept -> void {
        execution::set_done(std::move(self.lr.r));
        self.complete(true);
      }

      private:
      auto complete(bool completed) noexcept -> void {
        try {
          execution::set_value(std::move(this->r), completed);
        } catch (...) {
          execution::set_error(std::move(this->r), std::current_exception());
        }
      }

      loop_receiver& lr;
    };

    // The local-sender wraps the function-sender.
    // It produces a `true' if the sender caused us to complete the repeat-operation.
    // Otherwise false.
    //
    // The sender causes us to complete the repeat-operation, if it emits an error-signal
    // or a done-signal. In this case the value-signal `true' is propagated.
    //
    // The value-signal means that the operation competed successfully, and that
    // more work is to be done. In this case, the value-signal `false' is propagated.
    template<sender Sender>
    class local_sender
    : public _generic_sender_wrapper<local_sender<Sender>, Sender, connect_t>
    {
      public:
      template<template<typename...> class Tuple, template<typename...> class Variant>
      using value_types = Variant<Tuple<bool>>;

      template<template<typename...> class Variant>
      using error_types = Variant<std::exception_ptr>;

      static inline constexpr bool sends_done = true;

      explicit local_sender(Sender&& s, loop_receiver& lr)
      noexcept(std::is_nothrow_move_constructible_v<Sender>)
      : _generic_sender_wrapper<local_sender<Sender>, Sender, connect_t>(std::move(s)),
        lr(lr)
      {}

      template<receiver NestedReceiver>
      friend auto tag_invoke([[maybe_unused]] connect_t, local_sender&& self, NestedReceiver&& r)
      -> decltype(auto) {
        return execution::connect(std::move(self.s), local_receiver<std::remove_cvref_t<NestedReceiver>>(std::forward<NestedReceiver>(r), self.lr));
      }

      private:
      loop_receiver& lr;
    };

    public:
    explicit loop_receiver(Receiver&& r, Fn&& fn)
    noexcept(std::is_nothrow_move_constructible_v<Receiver> && std::is_nothrow_move_constructible_v<Fn>)
    : _generic_receiver_wrapper<Receiver, set_value_t>(std::move(r)),
      fn(std::move(fn))
    {}

    template<typename... Args>
    friend auto tag_invoke([[maybe_unused]] set_value_t, loop_receiver&& self, Args&&... args)
    -> void {
      using opt_sender_type = fn_sender_type<Args...>;
      using sender_type = typename opt_sender_type::value_type;
      static_assert(sender<sender_type>);

      for (std::size_t idx = 0; /*forever*/; ++idx) {
        opt_sender_type opt_fn_sender =
            std::invoke(self.fn, std::as_const(idx), args...); // passing args by lvalue-reference

        if (opt_fn_sender.has_value()) {
          auto [completed] = execution::sync_wait(
              local_sender<sender_type>(*std::move(opt_fn_sender), self)).value();
          if (completed) [[unlikely]] return;
        } else {
          break;
        }
      }

      execution::set_value(std::move(self.r), std::forward<Args>(args)...);
    }

    private:
    Fn fn;
  };

  // Helper type, that figure out sender type of the function, for given arguments.
  template<typename Fn>
  struct sender_type_from_fn {
    template<typename... Args>
    using optional_type = std::remove_cvref_t<std::invoke_result_t<Fn&, std::size_t, std::remove_cvref_t<Args>&...>>;

    template<typename... Args>
    using type = typename optional_type<Args...>::value_type;

    template<typename... Args>
    using traits = sender_traits<type<Args...>>;
  };
  // Translate the `sends_done' constant into std::true_type or std::false_type.
  template<typename Traits>
  using sends_done_as_integral_constant = std::integral_constant<bool, Traits::sends_done>;
  // Translate the sender-traits into a collection of errors.
  template<typename Traits>
  using all_error_types_for_sender = typename Traits::template error_types<_type_appender>;

  // Forward-declaration of sender_impl, because we need to know this for `sender_types_for_impl'.
  template<sender Sender, typename Fn> class sender_impl;

  // Wrapper around the generic sender wrapper, which overrides sender_traits.
  // The default implementation overrides nothing, since the underlying sender is incomplete.
  template<sender Sender, typename Fn>
  struct sender_types_for_impl
  : public _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>
  {
    // Inherit all constructors.
    using _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>::_generic_sender_wrapper;
  };
  // Specialization for sender_types_for_impl.
  // This one has a typed_sender, and thus can compute updated traits.
  // It overrides the error_types (to be the union of all the errors)
  // and the sends_done value.
  template<typed_sender Sender, typename Fn>
  struct sender_types_for_impl<Sender, Fn>
  : public _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>
  {
    private:
    // Figure out all the sender types.
    // `_type_appender<sender_traits<Sender1>, sender_traits<Sender2>, ...>'
    using _all_sender_types_from_fn_ = typename sender_traits<Sender>::template value_types<sender_type_from_fn<Fn>::template traits, _type_appender>;

    public:
    // Figure out the error-types.
    template<template<typename...> class Variant>
    using error_types =
        typename _type_appender<>::merge<                                          // The union of
            typename sender_traits<Sender>::template error_types<_type_appender>,  // the error-types of our sender,
            typename _all_sender_types_from_fn_::                                  // from any of the sender from the function
                template transform<all_error_types_for_sender>::                   // take all the errors
                                                                                   // (`_type_appender<_type_appender<E1, E2, ...>, ...>')
                template type<_type_appender<>::template merge>                    // and flatten the collection
        >::                                                                        //
        template type<Variant>;                                                    // And pass them all to the variant.

    // Figure out if we send the done-signal.
    static inline constexpr bool sends_done =
        _type_appender<>::merge<                                                              // Collect the union of
            _type_appender<sender_traits<Sender>>,                                            // sender_traits for our sender,
            _all_sender_types_from_fn_                                                        // sender_traits for any of the senders from
                                                                                              // the function
        >::                                                                                   //
        template transform<sends_done_as_integral_constant>::                                 // Transform them all into an integral-constant
                                                                                              // based on the sends_done value.
        template type<std::disjunction>::value;                                               // And then we take the logic-OR of this.

    // Inherit all constructors.
    using _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>::_generic_sender_wrapper;
  };

  // We use a single sender.
  //
  // When the sender is connected to a receiver,
  // it'll create the loop_receiver if there's not get_receiver(receiver) function.
  // It'll create the opstate if there is a get_receiver(receiver) function.
  template<sender Sender, typename Fn>
  class sender_impl
  : public sender_types_for_impl<Sender, Fn>
  {
    template<typename, sender, typename...> friend class ::earnest::execution::_generic_sender_wrapper;

    public:
    explicit sender_impl(Sender&& s, Fn&& fn)
    noexcept(std::is_nothrow_move_constructible_v<Sender> && std::is_nothrow_move_constructible_v<Fn>)
    : sender_types_for_impl<Sender, Fn>(std::move(s)),
      fn(std::move(fn))
    {}

    explicit sender_impl(Sender&& s, const Fn& fn)
    noexcept(std::is_nothrow_move_constructible_v<Sender> && std::is_nothrow_copy_constructible_v<Fn>)
    : sender_types_for_impl<Sender, Fn>(std::move(s)),
      fn(fn)
    {}

    explicit sender_impl(const Sender& s, Fn&& fn)
    noexcept(std::is_nothrow_copy_constructible_v<Sender> && std::is_nothrow_move_constructible_v<Fn>)
    : sender_types_for_impl<Sender, Fn>(s),
      fn(std::move(fn))
    {}

    explicit sender_impl(const Sender& s, const Fn& fn)
    noexcept(std::is_nothrow_copy_constructible_v<Sender> && std::is_nothrow_copy_constructible_v<Fn>)
    : sender_types_for_impl<Sender, Fn>(s),
      fn(fn)
    {}

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> decltype(auto) {
      if constexpr(has_scheduler<std::remove_cvref_t<Receiver>>)
        return opstate<Sender, std::remove_cvref_t<Receiver>, Fn>(std::move(self.s), std::forward<Receiver>(r), std::move(self.fn));
      else
        return execution::connect(std::move(self.s), loop_receiver<std::remove_cvref_t<Receiver>, Fn>(std::forward<Receiver>(r), std::move(self.fn)));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    noexcept(std::is_nothrow_constructible_v<sender_impl<std::remove_cvref_t<OtherSender>, Fn>, OtherSender, Fn>)
    -> sender_impl<std::remove_cvref_t<OtherSender>, Fn> {
      return sender_impl<std::remove_cvref_t<OtherSender>, Fn>(std::forward<OtherSender>(other_sender), std::move(fn));
    }

    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) const &
    noexcept(std::is_nothrow_constructible_v<sender_impl<std::remove_cvref_t<OtherSender>, Fn>, OtherSender, const Fn&>)
    -> sender_impl<std::remove_cvref_t<OtherSender>, Fn> {
      return sender_impl<std::remove_cvref_t<OtherSender>, Fn>(std::forward<OtherSender>(other_sender), fn);
    }

    [[no_unique_address]] Fn fn;
  };

  template<sender S, typename Fn>
  constexpr auto default_impl(S&& s, Fn&& fn) const
  noexcept(std::is_nothrow_constructible_v<sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Fn>>, S, Fn>)
  -> sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Fn>> {
    return sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Fn>>(std::forward<S>(s), std::forward<Fn>(fn));
  }
};
inline constexpr lazy_repeat_t lazy_repeat{};


// Repeat a certain set of operations.
//
// This operation forwards to lazy_repeat.
struct repeat_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender S, typename Fn>
  constexpr auto operator()(S&& s, Fn&& fn) const
  noexcept(noexcept(_generic_operand_base<set_value_t>(std::declval<const repeat_t&>(), std::declval<S>(), std::declval<Fn>())))
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<S>(s), std::forward<Fn>(fn));
  }

  template<typename Fn>
  constexpr auto operator()(Fn&& fn) const
  noexcept(noexcept(_generic_adapter(std::declval<repeat_t>(), std::declval<Fn>())))
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<sender S, typename Fn>
  constexpr auto default_impl(S&& s, Fn&& fn) const
  noexcept(noexcept(lazy_repeat(std::declval<S>(), std::declval<Fn>())))
  -> sender decltype(auto) {
    return lazy_repeat(std::forward<S>(s), std::forward<Fn>(fn));
  }
};
inline constexpr repeat_t repeat{};


// Given a tuple-return, convert it to a multi-argument return.
struct lazy_explode_tuple_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender S>
  constexpr auto operator()(S&& s) const
  noexcept(noexcept(_generic_operand_base<set_value_t>(std::declval<const lazy_explode_tuple_t&>(), std::declval<S>())))
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<S>(s));
  }

  constexpr auto operator()() const
  noexcept(noexcept(_generic_adapter(std::declval<lazy_explode_tuple_t>())))
  -> decltype(auto) {
    return _generic_adapter(*this);
  }

  private:
  // Forward-declaration.
  template<sender Sender> class sender_impl;

  template<sender Sender>
  struct sender_types_for_impl
  : _generic_sender_wrapper<sender_impl<Sender>, Sender, connect_t>
  {
    // Inherit constructors.
    using _generic_sender_wrapper<sender_impl<Sender>, Sender, connect_t>::_generic_sender_wrapper;
  };

  // Unpack a type-appender containing a tuple, into a type-appender containing all the values in that tuple.
  template<typename TypeAppenderOfTuple> struct unpack_tuple_;
  template<typename... T>
  struct unpack_tuple_<_type_appender<std::tuple<T...>>> {
    using type = _type_appender<T...>;
  };
  template<typename TypeAppenderOfTuple>
  using unpack_tuple = typename unpack_tuple_<TypeAppenderOfTuple>::type;

  // Convert _type_appender<T...> into Tuple<T...>.
  template<template<typename...> class Tuple>
  struct apply_tuple {
    template<typename TypeAppender>
    using type = typename TypeAppender::template type<Tuple>;
  };

  template<typed_sender Sender>
  struct sender_types_for_impl<Sender>
  : _generic_sender_wrapper<sender_impl<Sender>, Sender, connect_t>
  {
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types =
        typename sender_traits<Sender>::                        // From the sender
        template value_types<_type_appender, _type_appender>::  // take all the value-types `_type_appender<_type_appender<std::tuple<T...>>, ...>'
        template transform<unpack_tuple>::                      // and extract the types from that tuple: `_type_appender<_type_appender<T...>, ...>'
        template transform<apply_tuple<Tuple>::template type>:: // then transform the inner type-appender to the requested Tuple type
                                                                // `_type_appender<Tuple<T...>, ...>'
        template type<Variant>;                                 // and finally wrap the whole thing in the Variant.

    // Inherit constructors.
    using _generic_sender_wrapper<sender_impl<Sender>, Sender, connect_t>::_generic_sender_wrapper;
  };

  template<receiver Receiver>
  class receiver_impl
  : public _generic_receiver_wrapper<Receiver, set_value_t>
  {
    public:
    explicit receiver_impl(Receiver&& r)
    noexcept(std::is_nothrow_move_constructible_v<Receiver>)
    : _generic_receiver_wrapper<Receiver, set_value_t>(std::move(r))
    {}

    template<typename Tpl>
    friend auto tag_invoke([[maybe_unused]] set_value_t, receiver_impl&& self, Tpl&& tpl)
    -> void {
      std::apply(
          [&]<typename... Args>(Args&&... args) {
            execution::set_value(std::move(self.r), std::forward<Args>(args)...);
          },
          std::forward<Tpl>(tpl));
    }
  };

  template<sender Sender>
  class sender_impl
  : public sender_types_for_impl<Sender>
  {
    template<typename, sender, typename...> friend class ::earnest::execution::_generic_sender_wrapper;

    public:
    explicit sender_impl(Sender&& s)
    noexcept(std::is_nothrow_move_constructible_v<Sender>)
    : sender_types_for_impl<Sender>(std::move(s))
    {}

    explicit sender_impl(const Sender& s)
    noexcept(std::is_nothrow_copy_constructible_v<Sender>)
    : sender_types_for_impl<Sender>(s)
    {}

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    noexcept(noexcept(execution::connect(std::declval<Sender>(), receiver_impl<std::remove_cvref_t<Receiver>>(std::declval<Receiver>()))))
    -> decltype(auto) {
      return execution::connect(std::move(self.s), receiver_impl<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r)));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    noexcept(std::is_nothrow_constructible_v<sender_impl<std::remove_cvref_t<OtherSender>>, OtherSender>)
    -> sender_impl<std::remove_cvref_t<OtherSender>> {
      return sender_impl<std::remove_cvref_t<OtherSender>>(std::forward<OtherSender>(other_sender));
    }

    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) const &
    noexcept(std::is_nothrow_constructible_v<sender_impl<std::remove_cvref_t<OtherSender>>, OtherSender>)
    -> sender_impl<std::remove_cvref_t<OtherSender>> {
      return sender_impl<std::remove_cvref_t<OtherSender>>(std::forward<OtherSender>(other_sender));
    }
  };

  template<sender S>
  auto default_impl(S&& s) const
  noexcept(std::is_nothrow_constructible_v<sender_impl<std::remove_cvref_t<S>>, S>)
  -> sender_impl<std::remove_cvref_t<S>> {
    return sender_impl<std::remove_cvref_t<S>>(std::forward<S>(s));
  }
};
inline constexpr lazy_explode_tuple_t lazy_explode_tuple{};


// Given a tuple-return, convert it to a multi-argument return.
struct explode_tuple_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender S>
  constexpr auto operator()(S&& s) const
  noexcept(noexcept(_generic_operand_base<set_value_t>(std::declval<const explode_tuple_t&>(), std::declval<S>())))
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<S>(s));
  }

  constexpr auto operator()() const
  noexcept(noexcept(_generic_adapter(std::declval<explode_tuple_t>())))
  -> decltype(auto) {
    return _generic_adapter(*this);
  }

  private:
  template<sender S>
  auto default_impl(S&& s) const
  noexcept(noexcept(lazy_explode_tuple(std::forward<S>(s))))
  -> sender decltype(auto) {
    return lazy_explode_tuple(std::forward<S>(s));
  }
};
inline constexpr explode_tuple_t explode_tuple{};


} /* inline namespace extensions */
} /* namespace earnest::execution */
