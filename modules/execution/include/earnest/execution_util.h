/* Additional utilities on top of execution.h
 */
#pragma once

#include "execution.h"
#include "blocking_scheduler.h"
#include "move_only_function.h"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <numeric>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

#include <gsl/pointers>

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
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<S>(s), std::forward<Fn>(fn));
  }

  template<typename Fn>
  constexpr auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<typename Fn, template<typename> class Receiver, typename... T>
  requires (!std::is_reference_v<T> &&...) && (!std::is_const_v<T> &&...)
  class values_container {
    private:
    using scheduler_type = decltype(execution::get_scheduler(std::declval<const Receiver<values_container>&>()));
    using fn_sender_optional = std::invoke_result_t<Fn&, std::size_t, T&...>;
    using fn_sender = typename fn_sender_optional::value_type;
    using opstate_type = std::remove_cvref_t<std::invoke_result_t<connect_t, fn_sender, Receiver<values_container>>>;

    public:
    template<typename... T_>
    explicit values_container(T_&&... v)
    : values(std::forward<T_>(v)...)
    {}

    template<typename ParentState>
    auto build_opstate(Fn& fn, std::size_t idx, ParentState& parent_state) -> opstate_type* {
      state.reset();
      auto opt_fn_sender = std::apply(
          [&](auto&... args) -> decltype(auto) {
            return std::invoke(fn, idx, args...);
          },
          values);
      if (!opt_fn_sender.has_value()) return nullptr;

      return &state.emplace(*std::move(opt_fn_sender), Receiver<values_container>(parent_state, *this));
    }

    template<std::invocable<T&&...> Fn_>
    auto apply(Fn_&& fn) && -> decltype(auto) {
      return std::apply(std::forward<Fn_>(fn), std::move(values));
    }

    private:
    std::tuple<T...> values;
    optional_operation_state_<opstate_type> state;
  };

  template<typename Fn, template<typename> class Receiver>
  struct type_appender_to_values_container {
    private:
    template<typename... T>
    using type_ = values_container<Fn, Receiver, T...>;

    public:
    template<typename TypeAppender>
    using type = typename TypeAppender::template transform<std::remove_cvref_t>::template type<type_>;
  };

  // Determine if a receiver has an associated scheduler.
  template<receiver Receiver, typename = void>
  struct has_scheduler_
  : std::false_type
  {};
  // Specialization of has_scheduler, for receivers which have a scheduler.
  template<receiver Receiver>
  struct has_scheduler_<Receiver, std::void_t<decltype(::earnest::execution::get_scheduler(std::declval<Receiver>()))>>
  : std::true_type
  {};

  // Determine if a receiver has an associated scheduler.
  template<receiver Receiver>
  static inline constexpr bool has_scheduler = has_scheduler_<Receiver>::value;

  // Operation-state, that's used for if we have a scheduler.
  //
  // The operation-state grabs hold of the arguments, and then repeatedly enqueues
  // the operation returned by the function. (One at a time.)
  template<sender Sender, receiver Receiver, typename Fn>
  class opstate
  : public operation_state_base_
  {
    private:
    // Local-receiver accepts the result of the fn-sender.
    // It wraps a receiver (the one we get from `start_detached')
    // and holds a reference to both the state and the container-values.
    //
    // It then:
    // - invokes the state.apply function, if the value-signal was received.
    // - otherwise passes the signal down the operation-chain.
    // In all cases, it'll complete the wrapped-receiver (using the done-signal).
    template<typename ValuesContainer>
    class local_receiver {
      public:
      explicit local_receiver(opstate& state, ValuesContainer& container) noexcept
      : state(state),
        container(container)
      {}

      template<typename... Args>
      friend auto tag_invoke([[maybe_unused]] set_value_t, local_receiver&& self, [[maybe_unused]] Args&&...) noexcept -> void {
        try {
          self.state.apply(self.container);
        } catch (...) {
          ::earnest::execution::set_error(std::move(self.state.r), std::current_exception());
        }
      }

      template<typename Tag, typename... Args>
      requires (_is_forwardable_receiver_tag<Tag> && !std::same_as<Tag, set_value_t>)
      friend auto tag_invoke([[maybe_unused]] Tag tag, const local_receiver& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, const Receiver&, Args...>)
      -> tag_invoke_result_t<Tag, const Receiver&, Args...> {
        return execution::tag_invoke(std::move(tag), std::as_const(self.state.r), std::forward<Args>(args)...);
      }

      template<typename Tag, typename... Args>
      requires (_is_forwardable_receiver_tag<Tag> && !std::same_as<Tag, set_value_t>)
      friend auto tag_invoke([[maybe_unused]] Tag tag, local_receiver& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, Receiver&, Args...>)
      -> tag_invoke_result_t<Tag, Receiver&, Args...> {
        return execution::tag_invoke(std::move(tag), self.state.r, std::forward<Args>(args)...);
      }

      template<typename Tag, typename... Args>
      requires (_is_forwardable_receiver_tag<Tag> && !std::same_as<Tag, set_value_t>)
      friend auto tag_invoke([[maybe_unused]] Tag tag, local_receiver&& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, Receiver, Args...>)
      -> tag_invoke_result_t<Tag, Receiver, Args...> {
        return execution::tag_invoke(std::move(tag), std::move(self.state.r), std::forward<Args>(args)...);
      }

      private:
      opstate& state;
      ValuesContainer& container;
    };

    // Compute all the container types.
    // `_type_appender<values_container<...>, values_container<...>, ...>'
    using all_container_types = typename sender_traits<Sender>::template value_types<_type_appender, _type_appender>::
        template transform<type_appender_to_values_container<Fn, local_receiver>::template type>;

    // Compute the variant type.
    //
    // The computed variant has a `std::monostate' as the first type, so that it'll initialize to that.
    // The monostate indicates that no values have been assigned.
    using variant_type =
        typename _type_appender<>::merge<   // The union of
            _type_appender<std::monostate>, // monostate (indicating there is
                                            // as yet no values present)
            all_container_types             // each of the possible value-types
                                            // `_type_appender<
                                            //     values_container<...>,
                                            //     values_container<...>,
                                            //     ...>'
        >::                                 //
        template type<std::variant>;        // All placed into a std::variant.

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
      template<typename Tag, typename... Args>
      requires (_is_forwardable_receiver_tag<Tag> && !std::same_as<Tag, set_value_t>)
      friend auto tag_invoke(Tag tag, accepting_receiver& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, const Receiver&, Args...>)
      -> tag_invoke_result_t<Tag, const Receiver&, Args...> {
        return execution::tag_invoke(std::move(tag), self.state.r, std::forward<Args>(args)...);
      }

      // Forward all other tags to the wrapped receiver.
      // This version accepts non-const lvalue-reference accepting_receiver.
      template<typename Tag, typename... Args>
      requires (_is_forwardable_receiver_tag<Tag> && !std::same_as<Tag, set_value_t>)
      friend auto tag_invoke(Tag tag, accepting_receiver& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, Receiver&, Args...>)
      -> tag_invoke_result_t<Tag, Receiver&, Args...> {
        return execution::tag_invoke(std::move(tag), self.state.r, std::forward<Args>(args)...);
      }

      // Forward all other tags to the wrapped receiver.
      // This version accepts rvalue-reference accepting_receiver.
      template<typename Tag, typename... Args>
      requires (_is_forwardable_receiver_tag<Tag> && !std::same_as<Tag, set_value_t>)
      friend auto tag_invoke(Tag tag, accepting_receiver&& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, Receiver&&, Args...>)
      -> tag_invoke_result_t<Tag, Receiver&&, Args...> {
        return execution::tag_invoke(std::move(tag), std::move(self.state.r), std::forward<Args>(args)...);
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

    opstate(opstate&&) = default;
    opstate(const opstate&) = delete;

    friend auto tag_invoke([[maybe_unused]] start_t, opstate& self) noexcept -> decltype(auto) {
      execution::start(self.nested_opstate);
    }

    private:
    template<typename... Args>
    auto start(Args&&... args) -> void {
      using container_type = values_container<Fn, local_receiver, std::remove_cvref_t<Args>...>;
      assert(std::holds_alternative<std::monostate>(values));
      container_type& container = values.template emplace<container_type>(std::forward<Args>(args)...);
      apply(container);
    }

    template<typename ValuesContainer>
    auto apply(ValuesContainer& container) -> void {
      assert(std::holds_alternative<ValuesContainer>(values));
      auto repeated_opstate_ptr = container.build_opstate(fn, idx++, *this);
      if (repeated_opstate_ptr == nullptr) {
        std::move(container).apply(
            [this]<typename... Args>(Args&&... args) {
              ::earnest::execution::set_value(std::move(this->r), std::forward<Args>(args)...);
            });
      } else {
        execution::execute(
            execution::get_scheduler(std::as_const(r)),
            [repeated_opstate_ptr]() noexcept { execution::start(*repeated_opstate_ptr); });
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
    : sender_types_for_impl<Sender, Fn>(std::move(s)),
      fn(std::move(fn))
    {}

    explicit sender_impl(Sender&& s, const Fn& fn)
    : sender_types_for_impl<Sender, Fn>(std::move(s)),
      fn(fn)
    {}

    explicit sender_impl(const Sender& s, Fn&& fn)
    : sender_types_for_impl<Sender, Fn>(s),
      fn(std::move(fn))
    {}

    explicit sender_impl(const Sender& s, const Fn& fn)
    : sender_types_for_impl<Sender, Fn>(s),
      fn(fn)
    {}

    template<receiver Receiver>
    class variant_opstate
    : operation_state_base_
    {
      private:
      using scheduler_type = std::invoke_result_t<get_scheduler_t, const Receiver&>;

      static auto make_scheduler_based_opstate(sender_impl&& self, Receiver&& r) {
        return opstate<Sender, std::remove_cvref_t<Receiver>, Fn>(std::move(self.s), std::move(r), std::move(self.fn));
      }

      template<typename T>
      static auto as_void_ptr(T* ptr) -> void* {
        return ptr;
      }

      struct loop_type
      : operation_state_base_
      {
        using type = decltype(execution::connect(std::declval<Sender>(), std::declval<loop_receiver<std::remove_cvref_t<Receiver>, Fn>>()));

        loop_type(sender_impl&& self, Receiver&& r)
        : impl(execution::connect(std::move(self.s), loop_receiver<std::remove_cvref_t<Receiver>, Fn>(std::move(r), std::move(self.fn))))
        {}

        friend auto tag_invoke(start_t start, loop_type& self) noexcept -> void {
          start(self.impl);
        }

        private:
        type impl;
      };

      struct scheduler_based_type
      : operation_state_base_
      {
        using type = opstate<Sender, std::remove_cvref_t<Receiver>, Fn>;

        scheduler_based_type(sender_impl&& self, Receiver&& r)
        : impl(std::move(self.s), std::move(r), std::move(self.fn))
        {}

        friend auto tag_invoke(start_t start, scheduler_based_type& self) noexcept -> void {
          start(self.impl);
        }

        private:
        type impl;
      };

      // We can't use std::variant, because std::variant refuses types that aren't copy-constructible.
      // So we'll have to use uninitialized storage.
      // Sadly, the types for this are declared deprecated in C++23, so we sadly have to roll our own.
      struct storage_type {
        static inline constexpr std::size_t len = std::max(sizeof(loop_type), sizeof(scheduler_based_type));
        static inline constexpr std::size_t align = std::lcm(alignof(loop_type), alignof(scheduler_based_type));

        alignas(align) std::byte data[len];
      };

      public:
      variant_opstate(sender_impl&& self, Receiver&& r)
      : loop_based(execution::execute_may_block_caller(execution::get_scheduler(r)))
      {
        if (loop_based)
          std::construct_at(&get_loop_state(), std::move(self), std::move(r));
        else
          std::construct_at(&get_scheduler_based_state(), std::move(self), std::move(r));
      }

      variant_opstate(const variant_opstate&) = delete;
      variant_opstate(variant_opstate&&) = delete;
      variant_opstate& operator=(const variant_opstate&) = delete;
      variant_opstate& operator=(variant_opstate&&) = delete;

      ~variant_opstate() {
        if (loop_based)
          std::destroy_at(&get_loop_state());
        else
          std::destroy_at(&get_scheduler_based_state());
      }

      friend auto tag_invoke([[maybe_unused]] start_t start, variant_opstate& self) noexcept -> void {
        if (self.loop_based)
          start(self.get_loop_state());
        else
          start(self.get_scheduler_based_state());
      }

      private:
      auto get_loop_state() -> loop_type& {
        void* vptr = &storage.data[0];
        return *static_cast<loop_type*>(vptr);
      }

      auto get_scheduler_based_state() -> scheduler_based_type& {
        void* vptr = &storage.data[0];
        return *static_cast<scheduler_based_type*>(vptr);
      }

      storage_type storage;
      const bool loop_based;
    };

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> decltype(auto) {
      if constexpr(std::invocable<get_scheduler_t, std::remove_reference_t<Receiver>&>)
        return variant_opstate<std::remove_cvref_t<Receiver>>(std::move(self), std::forward<Receiver>(r));
      else
        return execution::connect(std::move(self.s), loop_receiver<std::remove_cvref_t<Receiver>, Fn>(std::forward<Receiver>(r), std::move(self.fn)));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    -> sender_impl<std::remove_cvref_t<OtherSender>, Fn> {
      return sender_impl<std::remove_cvref_t<OtherSender>, Fn>(std::forward<OtherSender>(other_sender), std::move(fn));
    }

    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) const &
    -> sender_impl<std::remove_cvref_t<OtherSender>, Fn> {
      return sender_impl<std::remove_cvref_t<OtherSender>, Fn>(std::forward<OtherSender>(other_sender), fn);
    }

    [[no_unique_address]] Fn fn;
  };

  template<sender S, typename Fn>
  constexpr auto default_impl(S&& s, Fn&& fn) const
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
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<S>(s), std::forward<Fn>(fn));
  }

  template<typename Fn>
  constexpr auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<sender S, typename Fn>
  constexpr auto default_impl(S&& s, Fn&& fn) const
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
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<S>(s));
  }

  constexpr auto operator()() const
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
    : sender_types_for_impl<Sender>(std::move(s))
    {}

    explicit sender_impl(const Sender& s)
    : sender_types_for_impl<Sender>(s)
    {}

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> decltype(auto) {
      return execution::connect(std::move(self.s), receiver_impl<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r)));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    -> sender_impl<std::remove_cvref_t<OtherSender>> {
      return sender_impl<std::remove_cvref_t<OtherSender>>(std::forward<OtherSender>(other_sender));
    }
  };

  template<sender S>
  auto default_impl(S&& s) const
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
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<S>(s));
  }

  constexpr auto operator()() const
  -> decltype(auto) {
    return _generic_adapter(*this);
  }

  private:
  template<sender S>
  auto default_impl(S&& s) const
  -> sender decltype(auto) {
    return lazy_explode_tuple(std::forward<S>(s));
  }
};
inline constexpr explode_tuple_t explode_tuple{};


// Given a tuple-return, convert it to a multi-argument return.
struct lazy_explode_variant_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender S>
  constexpr auto operator()(S&& s) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<S>(s));
  }

  constexpr auto operator()() const
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

  // Unpack a type-appender containing a variant, into a type-appender containing all the values in that variant.
  template<typename TypeAppenderOfTuple> struct unpack_variant_;
  template<typename... T>
  struct unpack_variant_<_type_appender<std::variant<T...>>> {
    using type = _type_appender<T...>;
  };
  template<typename TypeAppenderOfVariant>
  using unpack_variant = typename unpack_variant_<TypeAppenderOfVariant>::type;

  template<typed_sender Sender>
  struct sender_types_for_impl<Sender>
  : _generic_sender_wrapper<sender_impl<Sender>, Sender, connect_t>
  {
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types =
        typename sender_traits<Sender>::                          // From the sender
        template value_types<_type_appender, _type_appender>::    // take all the value-types `_type_appender<_type_appender<std::variant<T...>>, ...>'
        template transform<unpack_variant>::                      // and extract the types from that variant: `_type_appender<_type_appender<T...>, ...>'
        template type<_type_appender<>::merge>::                  // squash all the types into a single collection: `_type_appender<T1, T2, ...>'
        template transform<Tuple>::                               // wrap each value in its own Tuple: `_type_appender<Tuple<T1>, Tuple<T2>, ...>'
        template type<Variant>;                                   // and finally wrap the whole thing in the Variant.

    // Inherit constructors.
    using _generic_sender_wrapper<sender_impl<Sender>, Sender, connect_t>::_generic_sender_wrapper;
  };

  template<receiver Receiver>
  class receiver_impl
  : public _generic_receiver_wrapper<Receiver, set_value_t>
  {
    public:
    explicit receiver_impl(Receiver&& r)
    : _generic_receiver_wrapper<Receiver, set_value_t>(std::move(r))
    {}

    template<typename Variant>
    friend auto tag_invoke([[maybe_unused]] set_value_t, receiver_impl&& self, Variant&& var)
    -> void {
      std::visit(
          [&]<typename Args>(Args&& args) {
            execution::set_value(std::move(self.r), std::forward<Args>(args));
          },
          std::forward<Variant>(var));
    }
  };

  template<sender Sender>
  class sender_impl
  : public sender_types_for_impl<Sender>
  {
    template<typename, sender, typename...> friend class ::earnest::execution::_generic_sender_wrapper;

    public:
    explicit sender_impl(Sender&& s)
    : sender_types_for_impl<Sender>(std::move(s))
    {}

    explicit sender_impl(const Sender& s)
    : sender_types_for_impl<Sender>(s)
    {}

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> decltype(auto) {
      return execution::connect(std::move(self.s), receiver_impl<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r)));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    -> sender_impl<std::remove_cvref_t<OtherSender>> {
      return sender_impl<std::remove_cvref_t<OtherSender>>(std::forward<OtherSender>(other_sender));
    }
  };

  template<sender S>
  auto default_impl(S&& s) const
  -> sender_impl<std::remove_cvref_t<S>> {
    return sender_impl<std::remove_cvref_t<S>>(std::forward<S>(s));
  }
};
inline constexpr lazy_explode_variant_t lazy_explode_variant{};


// Given a variant-return, convert it to a multi-argument return.
struct explode_variant_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender S>
  constexpr auto operator()(S&& s) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<S>(s));
  }

  constexpr auto operator()() const
  -> decltype(auto) {
    return _generic_adapter(*this);
  }

  private:
  template<sender S>
  auto default_impl(S&& s) const
  -> sender decltype(auto) {
    return lazy_explode_variant(std::forward<S>(s));
  }
};
inline constexpr explode_variant_t explode_variant{};


// An adapter that does nothing.
struct noop_t {
  template<sender S>
  constexpr auto operator()(S&& s) const noexcept -> S&& {
    return std::forward<S>(s);
  }

  constexpr auto operator()() const
  -> decltype(auto) {
    return _generic_adapter(*this);
  }
};
inline constexpr noop_t noop{};


// An adapter that turns an exception-pointer holding a std::system_error,
// into its corresponding std::error_code.
struct system_error_to_error_code_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender S>
  constexpr auto operator()(S&& s) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<S>(s));
  }

  constexpr auto operator()() const
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

  template<typed_sender Sender>
  struct sender_types_for_impl<Sender>
  : _generic_sender_wrapper<sender_impl<Sender>, Sender, connect_t>
  {
    // Publish the same error types as our parent, but add std::error_code to the set.
    template<template<typename...> class Variant>
    using error_types =
        typename _type_appender<std::error_code>::merge<
            typename sender_traits<Sender>::template error_types<_type_appender>
        >::template type<Variant>;

    // Inherit constructors.
    using _generic_sender_wrapper<sender_impl<Sender>, Sender, connect_t>::_generic_sender_wrapper;
  };

  template<receiver Receiver>
  class receiver_impl
  : public _generic_receiver_wrapper<Receiver, set_error_t>
  {
    public:
    explicit receiver_impl(Receiver&& r)
    : _generic_receiver_wrapper<Receiver, set_error_t>(std::move(r))
    {}

    template<typename Error>
    friend auto tag_invoke([[maybe_unused]] set_error_t, receiver_impl&& self, Error&& err)
    noexcept
    -> void {
      if constexpr(std::is_same_v<std::remove_cvref_t<Error>, std::exception_ptr>) {
        // We forward exception-pointers by unpacking them (aka rethrowing)
        // and the trying to catch a system-error.
        try {
          std::rethrow_exception(std::forward<Error>(err));
        } catch (const std::system_error& ex) {
          // Grab the error code from the exception.
          execution::set_error(std::move(self.r), ex.code());
        } catch (...) {
          // Any exception that isn't std::system_error, is passed through as-is.
          execution::set_error(std::move(self.r), std::current_exception());
        }
      } else {
        // Anything that isn't an exception pointer is passed through as-is.
        execution::set_error(std::move(self.r), std::forward<Error>(err));
      }
    }
  };

  template<sender Sender>
  class sender_impl
  : public sender_types_for_impl<Sender>
  {
    template<typename, sender, typename...> friend class ::earnest::execution::_generic_sender_wrapper;

    public:
    explicit sender_impl(Sender&& s)
    : sender_types_for_impl<Sender>(std::move(s))
    {}

    explicit sender_impl(const Sender& s)
    : sender_types_for_impl<Sender>(s)
    {}

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> decltype(auto) {
      return execution::connect(std::move(self.s), receiver_impl<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r)));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    -> sender_impl<std::remove_cvref_t<OtherSender>> {
      return sender_impl<std::remove_cvref_t<OtherSender>>(std::forward<OtherSender>(other_sender));
    }
  };

  template<sender S>
  auto default_impl(S&& s) const
  -> sender_impl<std::remove_cvref_t<S>> {
    return sender_impl<std::remove_cvref_t<S>>(std::forward<S>(s));
  }
};
inline constexpr system_error_to_error_code_t system_error_to_error_code{};


// Adapter that takes a function, and invokes the variant-of-senders returned by that function.
struct let_variant_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender S, typename Fn>
  constexpr auto operator()(S&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<S>(s), std::forward<Fn>(fn));
  }

  template<typename Fn>
  constexpr auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  // We need to translate `std::variant<T1, T2, ...>' to `_type_appender<T1, T2, ...>'.
  template<typename T> struct replace_variant_with_type_appender_;
  template<typename... T>
  struct replace_variant_with_type_appender_<std::variant<T...>>
  {
    using type = _type_appender<T...>;
  };
  template<typename T>
  using replace_variant_with_type_appender = typename replace_variant_with_type_appender_<T>::type;

  template<typename Fn>
  struct return_types_for_fn {
    private:
    // Figure out what the function returns.
    // Something like `std::variant<sender_1, sender_2, ...>'.
    template<typename... Args>
    using raw_return_type = std::invoke_result_t<Fn, Args...>;

    // Figure out what senders the function returns.
    template<typename... Args>
    using return_type = replace_variant_with_type_appender<std::remove_cvref_t<raw_return_type<std::add_lvalue_reference_t<Args>...>>>;

    public:
    // Given a type-appender of arguments, figures out what the different senders will be.
    // `_type_appender<Sender1, Sender2, Sender3, ...>'
    template<typename TypeAppender>
    using type = typename TypeAppender::template type<return_type>;
  };

  // Helper, translates sender_traits to std::bool_constant for sends_done.
  template<typename Traits>
  using sends_done_for_traits = std::bool_constant<Traits::sends_done>;

  // Helper, translates sender_traits to list of error-types: `_type_appender<E1, E2, ...>'.
  template<typename Traits>
  using error_types_for_traits = typename Traits::template error_types<_type_appender>;

  // Helper, translates sender_traits to list of value-types: `_type_appender<Tuple<...>, Tuple<...>, ...>'.
  template<template<typename...> class Tuple>
  struct value_types_for_traits {
    template<typename Traits>
    using type = typename Traits::template value_types<Tuple, _type_appender>;
  };

  // Here we do the error-types, value-types, and sends-done computations.
  template<typed_sender Sender, typename Fn>
  struct sender_types_helper {
    using traits = sender_traits<Sender>;

    // Capture existing error types.
    // _type_appender<ErrorType1, ErrorType2, ...>
    using existing_error_types = typename traits::template error_types<_type_appender>;
    // Capture input value types.
    // _type_appender<_type_appender<T...>, _type_appender<T...>, ...>
    using input_value_types = typename traits::template value_types<_type_appender, _type_appender>;

    // Figure out all the variant-types that can be returned.
    using all_sender_types = typename input_value_types::
        template transform<return_types_for_fn<Fn>::template type>:: // Translate each argument-set to a sender-set.
                                                                     // `_type_appender<
                                                                     //   _type_appender<Sender1a, Sender1b, ...>
                                                                     //   _type_appender<Sender2a, Sender2b, ...>
                                                                     // >'
        template type<_type_appender<>::merge>;                      // Merge them all together:
                                                                     // `_type_appender<Sender1a, Sender1b, ..., Sender2a, Sender2b, ...>'

    // Each of the sender-traits, from the senders returned by `fn'.
    using all_sender_trait_types = typename all_sender_types::template transform<sender_traits>;

    // Error types are the union of error types in each of these.
    // And also std::exception_ptr, if an exception occurs.
    using error_types = _type_appender<>::merge<
        _type_appender<std::exception_ptr>,                     // We may introduce an exception.
        typename traits::template error_types<_type_appender>,  // Retain error-types from input-sender.
        typename all_sender_trait_types::                       // All the sender-types from `fn': `_type_appender<Traits1, Traits2, ...>'
            template transform<error_types_for_traits>::        // Take the error types: `_type_appender<
                                                                //   _type_appender<E1, E2, ...>, _type_appender<E3, ...>, ...
                                                                // >'
            template type<typename _type_appender<>::merge>     // And squash them all together: `_type_appender<E1, E2, ..., E3, ...>'
        >;

    template<template<typename...> class Tuple>
    using value_types =
        typename all_sender_trait_types::
            template transform<value_types_for_traits<Tuple>::template type>::
            template type<typename _type_appender<>::merge>;

    // We will send-done, if our input sends-done, or any of the senders from `fn' sends-done.
    static inline constexpr bool sends_done =
        _type_appender<std::bool_constant<traits::sends_done>>::template merge<        // Take the sends-done from our input-sender.
            typename all_sender_trait_types::template transform<sends_done_for_traits> // And all of the sends-done from senders returned by `fn'
        >::template type<std::disjunction>::value;                                     // And take the logic-or of those.
  };

  template<typed_sender Sender, receiver Receiver>
  class nested_opstate {
    private:
    using impl_type = std::remove_cvref_t<decltype(execution::connect(std::declval<Sender>(), std::declval<Receiver>()))>;

    public:
    nested_opstate(Sender&& sender, Receiver&& receiver)
    : impl(execution::connect(std::move(sender), std::move(receiver)))
    {}

    auto start() noexcept -> void {
      execution::start(impl);
    }

    private:
    impl_type impl;
  };

  template<typed_sender Sender, typename Fn, receiver Receiver>
  class opstate
  : public operation_state_base_
  {
    private:
    using types_helper = sender_types_helper<Sender, Fn>;

    // A variant of all the possible values tuples.
    // The first type of the variant is std::monostate, so it can be default-initialized.
    using values_variant = _type_appender<std::monostate>::
        template merge<typename types_helper::traits::template value_types<std::tuple, _type_appender>>::
        template type<_deduplicate<std::variant>::template type>;

    using local_receiver = _generic_rawptr_receiver<Receiver>;

    template<typed_sender NestedSender>
    using nested_opstate_for_sender = nested_opstate<NestedSender, local_receiver>;

    using opstate_variant = typename _type_appender<std::monostate>::
        template merge<typename types_helper::all_sender_types::template transform<nested_opstate_for_sender>>::
        template type<_deduplicate<std::variant>::template type>;

    class accepting_receiver {
      public:
      explicit accepting_receiver(opstate& state) noexcept
      : state(state)
      {}

      template<typename... Args>
      friend auto tag_invoke([[maybe_unused]] set_value_t, accepting_receiver&& self, Args&&... args) noexcept -> void {
        try {
          self.state.apply(std::forward<Args>(args)...);
        } catch (...) {
          execution::set_error(std::move(self.state.r), std::current_exception());
        }
      }

      template<typename Tag, typename... Args,
          typename = std::enable_if_t<_is_forwardable_receiver_tag<Tag>>,
          typename = std::enable_if_t<!std::is_same_v<set_value_t, Tag>>>
      friend constexpr auto tag_invoke(Tag tag, const accepting_receiver& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, const Receiver&, Args...>)
      -> tag_invoke_result_t<Tag, const Receiver&, Args...> {
        return tag_invoke(std::move(tag), self.state.r, std::forward<Args>(args)...);
      }

      template<typename Tag, typename... Args,
          typename = std::enable_if_t<_is_forwardable_receiver_tag<Tag>>,
          typename = std::enable_if_t<!std::is_same_v<set_value_t, Tag>>>
      friend constexpr auto tag_invoke(Tag tag, accepting_receiver& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, Receiver&, Args...>)
      -> tag_invoke_result_t<Tag, Receiver&, Args...> {
        return tag_invoke(std::move(tag), self.state.r, std::forward<Args>(args)...);
      }

      template<typename Tag, typename... Args,
          typename = std::enable_if_t<_is_forwardable_receiver_tag<Tag>>,
          typename = std::enable_if_t<!std::is_same_v<set_value_t, Tag>>>
      friend constexpr auto tag_invoke(Tag tag, accepting_receiver&& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, Receiver, Args...>)
      -> tag_invoke_result_t<Tag, Receiver&&, Args...> {
        return tag_invoke(std::move(tag), std::move(self.state.r), std::forward<Args>(args)...);
      }

      private:
      opstate& state;
    };

    using parent_opstate = std::remove_cvref_t<decltype(execution::connect(std::declval<Sender>(), std::declval<accepting_receiver>()))>;

    public:
    opstate(Sender&& sender, Fn&& fn, Receiver&& r)
    : fn(fn),
      r(std::move(r)),
      parent(execution::connect(std::move(sender), accepting_receiver(*this)))
    {}

    friend auto tag_invoke([[maybe_unused]] start_t, opstate& self) noexcept -> void {
      execution::start(self.parent);
    }

    private:
    template<typename... Args>
    auto apply(Args&&... args) {
      assert(std::holds_alternative<std::monostate>(values));
      auto& tuple = values.template emplace<std::tuple<std::remove_cvref_t<Args>...>>(std::forward<Args>(args)...);

      auto sender_variant = std::apply(std::move(fn), tuple);
      std::visit(
          [this]<typed_sender NestedSender>(NestedSender&& nested_sender) {
            using opstate_type = nested_opstate_for_sender<std::remove_cvref_t<NestedSender>>;
            assert(std::holds_alternative<std::monostate>(opstates));
            auto& opstate_impl = opstates.template emplace<opstate_type>(std::forward<NestedSender>(nested_sender), local_receiver(&this->r));
            opstate_impl.start();
          },
          std::move(sender_variant));
    }

    values_variant values;
    opstate_variant opstates;
    Fn fn;
    Receiver r;
    parent_opstate parent;
  };

  // Forward-declaration.
  template<sender Sender, typename Fn> class sender_impl;

  // Figure out value-types, error-types, and sends-done.
  // For untyped senders, we won't know anything.
  template<sender Sender, typename Fn>
  struct sender_types_for_impl
  : _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>
  {
    // Inherit constructors.
    using _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>::_generic_sender_wrapper;
  };

  // Figure out value-types, error-types, and sends-done.
  // For typed senders, we can figure this out.
  template<typed_sender Sender, typename Fn>
  struct sender_types_for_impl<Sender, Fn>
  : _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>
  {
    private:
    using types_helper = sender_types_helper<Sender, Fn>;

    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = typename types_helper::template value_types<Tuple>::template type<Variant>;

    template<template<typename...> class Variant>
    using error_types = typename types_helper::error_types::template type<Variant>;

    static inline constexpr bool sends_done = types_helper::sends_done;

    // Inherit constructors.
    using _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>::_generic_sender_wrapper;
  };

  template<sender Sender, typename Fn>
  class sender_impl
  : public sender_types_for_impl<Sender, Fn>
  {
    public:
    sender_impl(Sender&& s, Fn&& fn)
    : sender_types_for_impl<Sender, Fn>(std::move(s)),
      fn(std::move(fn))
    {}

    sender_impl(const Sender& s, Fn&& fn)
    : sender_types_for_impl<Sender, Fn>(s),
      fn(std::move(fn))
    {}

    sender_impl(Sender&& s, const Fn& fn)
    : sender_types_for_impl<Sender, Fn>(std::move(s)),
      fn(fn)
    {}

    sender_impl(const Sender& s, const Fn& fn)
    : sender_types_for_impl<Sender, Fn>(s),
      fn(fn)
    {}

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r) -> opstate<Sender, Fn, std::remove_cvref_t<Receiver>> {
      return opstate<Sender, Fn, std::remove_cvref_t<Receiver>>(std::move(self.s), std::move(self.fn), std::forward<Receiver>(r));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    -> sender_impl<std::remove_cvref_t<OtherSender>, Fn> {
      return sender_impl<std::remove_cvref_t<OtherSender>, Fn>(std::forward<OtherSender>(other_sender), std::move(fn));
    }

    Fn fn;
  };

  template<sender S, typename Fn>
  auto default_impl(S&& s, Fn&& fn) const
  -> sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Fn>> {
    return sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Fn>>(std::forward<S>(s), std::forward<Fn>(fn));
  }
};
inline constexpr let_variant_t let_variant{};


class type_erased_scheduler {
  private:
  class schedule_receiver_intf {
    protected:
    ~schedule_receiver_intf() = default;

    public:
    virtual auto set_value() noexcept -> void = 0;
    virtual auto set_error(std::exception_ptr ex) noexcept -> void = 0;
  };

  class schedule_receiver {
    public:
    explicit schedule_receiver(gsl::not_null<schedule_receiver_intf*> ptr)
    : ptr(ptr)
    {}

    friend auto tag_invoke([[maybe_unused]] set_value_t tag, schedule_receiver&& r) noexcept -> void {
      assert(r.ptr != nullptr);
      r.ptr->set_value();
    }

    friend auto tag_invoke([[maybe_unused]] set_error_t tag, schedule_receiver&& r, std::exception_ptr ex) noexcept -> void {
      assert(r.ptr != nullptr);
      r.ptr->set_error(ex);
    }

    private:
    gsl::not_null<schedule_receiver_intf*> ptr;
  };

  template<receiver_of<> Receiver>
  class schedule_receiver_impl
  : public schedule_receiver_intf
  {
    public:
    explicit schedule_receiver_impl(Receiver r)
    : r(std::move(r))
    {}

    auto set_value() noexcept -> void override {
      try {
        execution::set_value(std::move(r));
      } catch (...) {
        execution::set_error(std::move(r), std::current_exception());
      }
    }

    auto set_error(std::exception_ptr ex) noexcept -> void override {
      execution::set_error(std::move(r), std::move(ex));
    }

    private:
    Receiver r;
  };

  class intf {
    protected:
    intf(execution::forward_progress_guarantee forward_progress_guarantee, bool execute_may_block_caller)
    : forward_progress_guarantee(forward_progress_guarantee),
      execute_may_block_caller(execute_may_block_caller)
    {}

    ~intf() = default;

    public:
    virtual auto schedule(schedule_receiver r) -> move_only_function<void() noexcept> = 0;
    virtual auto equals(const intf& other) const noexcept -> bool = 0;
    // XXX stop_token

    const execution::forward_progress_guarantee forward_progress_guarantee;
    const bool execute_may_block_caller;
  };

  class schedule_sender;

  template<scheduler Scheduler>
  class impl
  : public intf
  {
    private:
    class opstate_type {
      private:
      class impl_type
      : operation_state_base_
      {
        private:
        using actual_impl_type = decltype(execution::connect(execution::schedule(std::declval<Scheduler&>()), std::declval<schedule_receiver>()));

        public:
        impl_type(Scheduler sch, schedule_receiver&& r)
        : actual_impl(execution::connect(execution::schedule(sch), std::move(r)))
        {}

        friend auto tag_invoke(start_t start, impl_type& self) noexcept -> void {
          return start(self.actual_impl);
        }

        private:
        actual_impl_type actual_impl;
      };

      public:
      opstate_type(Scheduler sch, schedule_receiver&& r)
      : impl(std::make_unique<impl_type>(sch, std::move(r)))
      {}

      auto operator()() noexcept -> void {
        execution::start(*impl);
      }

      private:
      std::unique_ptr<impl_type> impl; // We use a pointer, so that our opstate_type is moveable (since it needs to be in a move-only-function).
    };

    public:
    explicit impl(Scheduler sch)
    : intf(execution::get_forward_progress_guarantee(sch), execution::execute_may_block_caller(sch)),
      sch(sch)
    {}

    auto schedule(schedule_receiver r) -> move_only_function<void() noexcept> override {
      return opstate_type(sch, std::move(r));
    }

    auto equals(const intf& other) const noexcept -> bool override {
      const impl*const other_impl_ptr = dynamic_cast<const impl*>(&other);
      return (other_impl_ptr != nullptr) && (sch == other_impl_ptr->sch);
    }

    private:
    [[no_unique_address]] Scheduler sch;
  };

  class schedule_sender {
    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<Tuple<>>;
    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr>;
    static inline constexpr bool sends_done = false;

    template<receiver_of<> Receiver>
    class opstate
    : operation_state_base_
    {
      public:
      explicit opstate(intf& sch, Receiver&& r)
      : r(std::move(r)),
        impl(sch.schedule(schedule_receiver(&this->r)))
      {}

      friend auto tag_invoke([[maybe_unused]] start_t tag, opstate& self) noexcept -> void {
        std::invoke(self.impl);
      }

      private:
      [[no_unique_address]] schedule_receiver_impl<Receiver> r;
      [[no_unique_address]] move_only_function<void() noexcept> impl;
    };

    explicit schedule_sender(gsl::not_null<std::shared_ptr<intf>> ptr)
    : ptr(ptr)
    {}

    template<receiver_of<> Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, schedule_sender&& self, Receiver&& r) -> opstate<std::remove_cvref_t<Receiver>> {
      return opstate<std::remove_cvref_t<Receiver>>(*self.ptr, std::forward<Receiver>(r));
    }

    friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_value_t> tag, const schedule_sender& self) {
      return type_erased_scheduler(self.ptr);
    }

    friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_error_t> tag, const schedule_sender& self) {
      return type_erased_scheduler(self.ptr);
    }

    friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_done_t> tag, const schedule_sender& self) {
      return type_erased_scheduler(self.ptr);
    }

    private:
    gsl::not_null<std::shared_ptr<intf>> ptr;
  };

  explicit type_erased_scheduler(gsl::not_null<std::shared_ptr<intf>> ptr) noexcept
  : ptr(ptr)
  {}

  public:
  template<typename Scheduler>
  requires (!std::same_as<type_erased_scheduler, std::remove_cvref_t<Scheduler>>) &&
      requires(Scheduler& sch) { // Annoyingly, if we check against the scheduler concept,
                                 // the compiler will say scheduler-concept depends on itself here.
                                 // So we have to go manual.
        { ::earnest::execution::schedule(sch) };
      }
  explicit type_erased_scheduler(Scheduler&& sch)
  : ptr(std::make_shared<impl<std::remove_cvref_t<Scheduler>>>(std::forward<Scheduler>(sch)))
  {}

  auto operator==(const type_erased_scheduler& y) const noexcept -> bool {
    return ptr->equals(*y.ptr);
  }

  auto operator!=(const type_erased_scheduler& y) const noexcept -> bool {
    return !(*this == y);
  }

  friend auto tag_invoke([[maybe_unused]] schedule_t, const type_erased_scheduler& self) -> schedule_sender {
    return schedule_sender(self.ptr);
  }

  friend auto tag_invoke([[maybe_unused]] get_forward_progress_guarantee_t, const type_erased_scheduler& self) noexcept {
    return self.ptr->forward_progress_guarantee;
  }

  friend auto tag_invoke([[maybe_unused]] execute_may_block_caller_t, const type_erased_scheduler& self) noexcept {
    return self.ptr->execute_may_block_caller;
  }

  friend auto swap(type_erased_scheduler& x, type_erased_scheduler& y) noexcept -> void {
    using std::swap;
    swap(x.ptr, y.ptr);
  }

  private:
  gsl::not_null<std::shared_ptr<intf>> ptr;
};


template<typename ValueTypes, typename ErrorTypes = std::variant<std::exception_ptr>, bool SendsDone = true>
struct type_erased_sender;

// We declare as many types as we can outside the type_erased_sender_t,
// so we can share types across implementations.
struct type_erased_sender_base_ {
  template<typename... T>
  class receiver_intf_for_values {
    static_assert((std::same_as<T, std::remove_cvref_t<T>> &&...)); // We want regular types, not references, nor const/volatile types.
    template<typename ReceiverIntf> friend class receiver_wrapper_t;

    public:
    template<typename Derived, typename CombinedInterfaceType> class impl;

    virtual ~receiver_intf_for_values() = default;

    friend auto tag_invoke([[maybe_unused]] set_value_t tag, receiver_intf_for_values&& self, T... v) noexcept -> void {
      std::move(self).set_value(std::move(v)...);
    }

    private:
    virtual void set_value(T... v) && noexcept = 0;
  };

  template<typename Tuple>
  struct receiver_intf_for_tuple_;

  template<typename... T>
  struct receiver_intf_for_tuple_<std::tuple<T...>> {
    using type = receiver_intf_for_values<T...>;
  };

  template<typename Tuple>
  using receiver_intf_for_tuple = typename receiver_intf_for_tuple_<Tuple>::type;

  template<typename Error>
  class receiver_intf_for_error {
    template<typename ReceiverIntf> friend class receiver_wrapper_t;

    public:
    template<typename Derived, typename CombinedInterfaceType> class impl;

    virtual ~receiver_intf_for_error() = default;

    friend auto tag_invoke([[maybe_unused]] set_error_t tag, receiver_intf_for_error&& self, Error error) noexcept -> void {
      std::move(self).set_error(std::move(error));
    }

    private:
    virtual void set_error(Error error) && noexcept = 0;
  };

  template<bool SendsDone>
  class receiver_intf_for_done {
    template<typename ReceiverIntf> friend class receiver_wrapper_t;

    public:
    template<typename Derived, typename CombinedInterfaceType> class impl;

    virtual ~receiver_intf_for_done() = default;

    friend auto tag_invoke([[maybe_unused]] set_done_t tag, receiver_intf_for_done&& self) noexcept -> void {
      std::move(self).set_done();
    }

    protected:
    virtual void set_done() && noexcept = 0;
  };

  template<typename ReceiverIntf>
  class receiver_wrapper_t {
    public:
    explicit receiver_wrapper_t(gsl::not_null<ReceiverIntf*> r) noexcept
    : r(r.get())
    {}

    receiver_wrapper_t(const receiver_wrapper_t&) = delete;

    receiver_wrapper_t(receiver_wrapper_t&& other) noexcept
    : r(std::exchange(other.r, nullptr))
    {}

    template<typename... T>
    friend auto tag_invoke(set_value_t tag, receiver_wrapper_t&& self, T&&... v) {
      tag(std::move(*self.r), std::forward<T>(v)...);
    }

    template<typename Error>
    friend auto tag_invoke(set_error_t tag, receiver_wrapper_t&& self, Error&& error) noexcept {
      tag(std::move(*self.r), std::forward<Error>(error));
    }

    friend auto tag_invoke(set_done_t tag, receiver_wrapper_t&& self) noexcept {
      tag(std::move(*self.r));
    }

    friend auto tag_invoke([[maybe_unused]] get_scheduler_t tag, const receiver_wrapper_t& self) noexcept {
      return self.r->get_scheduler();
    }

    private:
    ReceiverIntf *r;
  };

  class operation_state_intf
  {
    public:
    template<typed_sender Sender, receiver Receiver> class impl;

    virtual ~operation_state_intf() = default;

    virtual void start() noexcept = 0;
  };

  template<typename ReceiverIntf>
  class sender_intf {
    public:
    template<typed_sender SenderImpl> class impl;

    virtual ~sender_intf() = default;

    virtual auto connect(gsl::not_null<ReceiverIntf*> r) && -> std::unique_ptr<operation_state_intf> = 0;
  };

  template<typename ValueTypes, typename ErrorTypes, bool SendsDone>
  class receiver_intf;
  template<typename... ValueTypes, typename... ErrorTypes, bool SendsDone>
  class receiver_intf<std::variant<ValueTypes...>, std::variant<ErrorTypes...>, SendsDone>;

  template<receiver Receiver>
  class operation_state
  : public operation_state_base_
  {
    public:
    template<typename ReceiverIntf>
    explicit operation_state(sender_intf<ReceiverIntf>&& s, Receiver&& r) noexcept
    : r(std::move(r)),
      impl(std::move(s).connect(&this->r))
    {}

    friend auto tag_invoke([[maybe_unused]] start_t, operation_state& self) noexcept -> void {
      assert(self.impl != nullptr);
      self.impl->start();
    }

    private:
    Receiver r;
    std::unique_ptr<operation_state_intf> impl;
  };

  template<typename ValuesTuple> struct rebind_values_tuple;

  template<typename... T>
  struct rebind_values_tuple<std::tuple<T...>> {
    template<template<typename...> class Tuple>
    using type = Tuple<T...>;
  };
};

template<typename... T>
template<typename Derived, typename CombinedInterfaceType>
class type_erased_sender_base_::receiver_intf_for_values<T...>::impl
: public virtual CombinedInterfaceType
{
  public:
  ~impl() override = default;

  private:
  void set_value(T... v) && noexcept override final {
    assert(dynamic_cast<Derived*>(this) != nullptr);
    try {
      std::move(static_cast<Derived&>(*this)).set_value_impl(std::move(v)...);
    } catch (...) {
      std::move(static_cast<Derived&>(*this)).set_error_impl(std::current_exception());
    }
  }
};

template<typename Error>
template<typename Derived, typename CombinedInterfaceType>
class type_erased_sender_base_::receiver_intf_for_error<Error>::impl
: public virtual CombinedInterfaceType
{
  public:
  ~impl() override = default;

  private:
  void set_error(Error error) && noexcept override final {
    assert(dynamic_cast<Derived*>(this) != nullptr);
    std::move(static_cast<Derived&>(*this)).set_error_impl(std::move(error));
  }
};

template<bool SendsDone>
template<typename Derived, typename CombinedInterfaceType>
class type_erased_sender_base_::receiver_intf_for_done<SendsDone>::impl
: public virtual CombinedInterfaceType
{
  public:
  ~impl() override = default;

  private:
  void set_done() && noexcept override final {
    assert(dynamic_cast<Derived*>(this) != nullptr);
    if constexpr(SendsDone) {
      std::move(static_cast<Derived&>(*this)).set_done_impl();
    } else {
      // We must implement the sends-done interface.
      // But if the interface declares no done-signal to be sent,
      // we'll trip undefined behaviour.
#if __cpp_lib_unreachable >= 202202L
      std::unreachable();
#else
      std::abort();
#endif
    }
  }
};

template<typename ReceiverIntf>
template<typed_sender SenderImpl>
class type_erased_sender_base_::sender_intf<ReceiverIntf>::impl
: public sender_intf
{
  public:
  explicit impl(SenderImpl&& sender_impl)
  : sender_impl(std::move(sender_impl))
  {}

  template<bool IsCopyConstructible = std::is_copy_constructible_v<SenderImpl>, typename = std::enable_if_t<IsCopyConstructible>>
  explicit impl(const SenderImpl& sender_impl)
  : sender_impl(sender_impl)
  {}

  auto connect(gsl::not_null<ReceiverIntf*> r) && -> std::unique_ptr<operation_state_intf> override {
    return std::make_unique<operation_state_intf::impl<SenderImpl, receiver_wrapper_t<ReceiverIntf>>>(
        std::move(sender_impl),
        receiver_wrapper_t<ReceiverIntf>(r));
  }

  private:
  SenderImpl sender_impl;
};

template<typed_sender Sender, receiver Receiver>
class type_erased_sender_base_::operation_state_intf::impl
: public operation_state_intf
{
  private:
  using opstate_type = std::remove_cvref_t<decltype(execution::connect(std::declval<Sender>(), std::declval<Receiver>()))>;

  public:
  explicit impl(Sender&& s, Receiver&& r)
  : opstate(execution::connect(std::move(s), std::move(r)))
  {}

  ~impl() override = default;

  void start() noexcept override {
    execution::start(opstate);
  }

  private:
  opstate_type opstate;
};

template<typename... ValueTypes, typename... ErrorTypes, bool SendsDone>
class type_erased_sender_base_::receiver_intf<std::variant<ValueTypes...>, std::variant<ErrorTypes...>, SendsDone>
: public type_erased_sender_base_::receiver_intf_for_tuple<ValueTypes>...,
  public type_erased_sender_base_::receiver_intf_for_error<ErrorTypes>...,
  public type_erased_sender_base_::receiver_intf_for_done<SendsDone>
{
  public:
  virtual ~receiver_intf() = default;
  virtual auto get_scheduler() const -> type_erased_scheduler = 0;

  template<receiver Receiver>
  class impl
  : public virtual receiver_intf,
    public type_erased_sender_base_::receiver_intf_for_tuple<ValueTypes>::template impl<impl<Receiver>, receiver_intf>...,
    public type_erased_sender_base_::receiver_intf_for_error<ErrorTypes>::template impl<impl<Receiver>, receiver_intf>...,
    public type_erased_sender_base_::receiver_intf_for_done<SendsDone>::template impl<impl<Receiver>, receiver_intf>
  {
    public:
    explicit impl(Receiver&& r)
    : r(std::move(r))
    {}

    impl(const impl&) = delete;
    impl(impl&&) = default;

    ~impl() override = default;

    template<typename... T>
    auto set_value_impl(T&&... v) && {
      execution::set_value(std::move(r), std::forward<T>(v)...);
    }

    template<typename E>
    auto set_error_impl(E&& e) && noexcept {
      execution::set_error(std::move(r), std::forward<E>(e));
    }

    auto set_done_impl() && noexcept {
      execution::set_done(std::move(r));
    }

    auto get_scheduler() const -> type_erased_scheduler override {
      if constexpr(std::invocable<execution::get_scheduler_t, const Receiver&>)
        return type_erased_scheduler(execution::get_scheduler(r));
      else
        return type_erased_scheduler(blocking_scheduler<>());
    }

    private:
    Receiver r;
  };
};

template<typename... ValueTypes, typename... ErrorTypes, bool SendsDone>
class type_erased_sender<std::variant<ValueTypes...>, std::variant<ErrorTypes...>, SendsDone> {
  public:
  template<template<typename...> class Tuple, template<typename...> class Variant>
  using value_types = Variant<typename type_erased_sender_base_::rebind_values_tuple<ValueTypes>::template type<Tuple>...>;

  template<template<typename...> class Variant>
  using error_types = Variant<std::exception_ptr, ErrorTypes...>;

  static inline constexpr bool sends_done = SendsDone;

  private:
  using receiver_intf = type_erased_sender_base_::receiver_intf<std::variant<ValueTypes...>, std::variant<ErrorTypes...>, SendsDone>;

  using sender_intf = type_erased_sender_base_::sender_intf<receiver_intf>;

  template<typename Receiver>
  using operation_state = type_erased_sender_base_::operation_state<typename receiver_intf::template impl<Receiver>>;

  public:
  template<typename Impl> // Impl should be a typed_sender, but if I enforce it, the compiler says I'm recursing the move-constructible concept.
                          // I don't understand why it says that.
  type_erased_sender(Impl&& impl)
  : impl(std::make_unique<typename sender_intf::template impl<std::remove_cvref_t<Impl>>>(std::forward<Impl>(impl)))
  {}

  type_erased_sender(const type_erased_sender&) = delete;
  type_erased_sender(type_erased_sender&&) noexcept = default;

  template<receiver Receiver>
  friend auto tag_invoke([[maybe_unused]] connect_t tag, type_erased_sender&& self, Receiver&& r) -> operation_state<std::remove_cvref_t<Receiver>> {
    auto receiver_impl = typename receiver_intf::template impl<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r));
    return operation_state<std::remove_cvref_t<Receiver>>(std::move(*self.impl), std::move(receiver_impl));
  }

  private:
  std::unique_ptr<sender_intf> impl;
};

template<typed_sender Sender>
type_erased_sender(Sender&&) -> type_erased_sender<
    typename sender_traits<std::remove_cvref_t<Sender>>::template value_types<std::tuple, std::variant>,
    typename sender_traits<std::remove_cvref_t<Sender>>::template error_types<std::variant>,
    sender_traits<std::remove_cvref_t<Sender>>::sends_done>;


// Validation sender.
//
// The `Pred' must be an invocable. It'll be invoked with the sender-args.
// Upon invocation, it must return an `std::optional<ErrorType>'.
// If the optional holds a value, that value will be propagated as the error-outcome.
// Otherwise, the original arguments will be passed on.
//
// The arguments are passed in by const-reference.
struct lazy_validation_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender S, typename Pred>
  constexpr auto operator()(S&& s, Pred&& pred) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<S>(s), std::forward<Pred>(pred));
  }

  template<typename Pred>
  constexpr auto operator()(Pred&& pred) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Pred>(pred));
  }

  private:
  // Helper, that'll assert if something is an optional.
  template<typename>
  struct is_an_optional_ : std::false_type {};
  template<typename T>
  struct is_an_optional_<std::optional<T>> : std::true_type {};

  template<typename T>
  static inline constexpr bool is_an_optional = is_an_optional_<T>::value;

  // The receiver used by the validation operation.
  //
  // It basically executes `Pred(sender-values...)', and if the outcome is an empty optional,
  // it'll invoke the next receiver with the same sender-values.
  // Otherwise, it'll discard the values and send the optional's value as an error instead.
  template<receiver Receiver, typename Pred>
  class wrapped_receiver
  : public _generic_receiver_wrapper<Receiver, set_value_t>
  {
    public:
    wrapped_receiver(Receiver&& r, Pred&& pred)
    : _generic_receiver_wrapper<Receiver, set_value_t>(std::move(r)),
      pred(std::move(pred))
    {}

    template<typename... Args>
    friend auto tag_invoke([[maybe_unused]] set_value_t, wrapped_receiver&& self, Args&&... args) {
      auto opt_error = std::invoke(std::move(self.pred), std::as_const(args)...);
      static_assert(is_an_optional<decltype(opt_error)>, "predicate must return an std::optional<error-type>");

      if constexpr(is_an_optional<decltype(opt_error)>) { // If you don't provide an optional, the following code might produce errors.
                                                          // Those errors would be noisy.
                                                          // So we'll disable this code, in the hopes the above static_assert will stand out.
        if (!opt_error.has_value())
          ::earnest::execution::set_value(std::move(self.r), std::forward<Args>(args)...);
        else
          ::earnest::execution::set_error(std::move(self.r), *std::move(opt_error));
      }
    }

    private:
    [[no_unique_address]] Pred pred;
  };

  // Forward-declare the sender-impl, so we can refer to it in sender-types.
  template<sender Sender, typename Pred> class sender_impl;

  // Sender-types, for untyped senders.
  template<sender Sender, typename Pred>
  struct sender_types_for_impl
  : _generic_sender_wrapper<sender_impl<Sender, Pred>, Sender, connect_t, get_completion_scheduler_t<set_error_t>>
  {
    // Inherit all constructors.
    using _generic_sender_wrapper<sender_impl<Sender, Pred>, Sender, connect_t, get_completion_scheduler_t<set_error_t>>::_generic_sender_wrapper;
  };

  // Helper type that figures out the type of an error, when the predicate is invoked with arguments.
  template<typename Pred>
  struct figure_out_error_types {
    template<typename... Args>
    struct type_ {
      using result_type = std::invoke_result_t<Pred&&, const Args&...>;
      static_assert(is_an_optional<result_type>);

      using type = typename result_type::value_type;
    };

    template<typename... Args>
    using type = typename type_<Args...>::type;
  };

  // Sender-types for typed senders.
  // We inherit all the existing types, and merely add an error type.
  template<typed_sender Sender, typename Pred>
  struct sender_types_for_impl<Sender, Pred>
  : _generic_sender_wrapper<sender_impl<Sender, Pred>, Sender, connect_t, get_completion_scheduler_t<set_error_t>>
  {
    private:
    // Figure out newly-introduced error types.
    // `_type_appender<E1, E2, E3, ...>'
    using new_error_types = sender_traits<Sender>::template value_types<
        figure_out_error_types<Pred>::template type,
        _type_appender>;

    public:
    // Inherit all constructors.
    using _generic_sender_wrapper<sender_impl<Sender, Pred>, Sender, connect_t, get_completion_scheduler_t<set_error_t>>::_generic_sender_wrapper;

    // Error-types is the union of existing error types,
    // and all the new error types.
    template<template<typename...> class Variant>
    using error_types = typename _type_appender<>::merge<
            new_error_types,                                                     // The introduced error type:
                                                                                 // `_type_appender<dereferenced-validation-return-type>',
            typename sender_traits<Sender>::template error_types<_type_appender> // and all existing errors: `_type_appender<E1, E2, ...>'
        >::                                                                      // all merged together:
                                                                                 // `_type_appender<factory-return-type, E1, E2, ...>'
        template type<Variant>;                                                  // And then given to the variant:
                                                                                 // `Variant<factory-return-type, E1, E2, ...>'
  };

  // Implementation of the validation sender.
  //
  // This has an associated sender, but no final receiver yet.
  template<sender Sender, typename Pred>
  class sender_impl
  : public sender_types_for_impl<Sender, Pred>
  {
    template<typename, sender, typename...> friend class ::earnest::execution::_generic_sender_wrapper;

    public:
    template<typename Sender_, typename Pred_>
    requires std::constructible_from<Sender, Sender_> && std::constructible_from<Pred, Pred_>
    constexpr sender_impl(Sender_&& s, Pred_&& pred)
    : sender_types_for_impl<Sender, Pred>(std::forward<Sender_>(s)),
      pred(std::forward<Pred_>(pred))
    {}

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> operation_state decltype(auto) {
      return ::earnest::execution::connect(
          std::move(self.s),
          wrapped_receiver<std::remove_cvref_t<Receiver>, Pred>(std::forward<Receiver>(r), std::move(self.pred)));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    -> sender_impl<std::remove_cvref_t<OtherSender>, Pred> {
      return sender_impl<std::remove_cvref_t<OtherSender>, Pred>(std::forward<OtherSender>(other_sender), std::move(pred));
    }

    [[no_unique_address]] Pred pred;
  };

  template<sender S, typename Pred>
  constexpr auto default_impl(S&& s, Pred&& pred) const
  -> sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Pred>> {
    return sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Pred>>(std::forward<S>(s), std::forward<Pred>(pred));
  }
};
inline constexpr lazy_validation_t lazy_validation{};


// Validation sender.
//
// The `Pred' must be an invocable. It'll be invoked with the sender-args.
// Upon invocation, it must return an `std::optional<ErrorType>'.
// If the optional holds a value, that value will be propagated as the error-outcome.
// Otherwise, the original arguments will be passed on.
//
// The arguments are passed in by const-reference.
struct validation_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender S, typename Pred>
  constexpr auto operator()(S&& s, Pred&& pred) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<S>(s), std::forward<Pred>(pred));
  }

  template<typename Pred>
  constexpr auto operator()(Pred&& pred) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Pred>(pred));
  }

  private:
  template<sender S, typename Pred>
  constexpr auto default_impl(S&& s, Pred&& pred) const
  -> sender decltype(auto) {
    return execution::lazy_validation(std::forward<S>(s), std::forward<Pred>(pred));
  }
};
inline constexpr validation_t validation{};


// Observe the value channel of a sender chain.
struct lazy_observe_value_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender S, typename Fn>
  constexpr auto operator()(S&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<S>(s), std::forward<Fn>(fn));
  }

  template<typename Fn>
  constexpr auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<receiver Receiver, typename Fn>
  class receiver_impl
  : public _generic_receiver_wrapper<Receiver, set_value_t>
  {
    public:
    explicit receiver_impl(Receiver&& r, Fn&& fn)
    : _generic_receiver_wrapper<Receiver, set_value_t>(std::move(r)),
      fn(std::move(fn))
    {}

    template<typename... Args>
    friend auto tag_invoke(set_value_t set_value, receiver_impl&& self, Args&&... args) -> void {
      std::invoke(std::move(self.fn), std::as_const(args)...);
      set_value(std::move(self.r), std::forward<Args>(args)...);
    }

    private:
    Fn fn;
  };

  template<sender Sender, typename Fn>
  class sender_impl
  : public _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t>
  {
    template<typename, sender, typename...> friend class ::earnest::execution::_generic_sender_wrapper;

    public:
    explicit sender_impl(Sender&& s, Fn&& fn)
    : _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t>(std::forward<Sender>(s)),
      fn(std::move(fn))
    {}

    explicit sender_impl(Sender&& s, const Fn& fn)
    : _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t>(std::forward<Sender>(s)),
      fn(fn)
    {}

    template<receiver Receiver>
    friend auto tag_invoke(connect_t connect, sender_impl&& self, Receiver&& r) {
      return connect(
          std::move(self.s),
          receiver_impl<std::remove_cvref_t<Receiver>, Fn>(std::forward<Receiver>(r), std::move(self.fn)));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    -> sender_impl<std::remove_cvref_t<OtherSender>, Fn> {
      return sender_impl<std::remove_cvref_t<OtherSender>, Fn>(std::forward<OtherSender>(other_sender), std::move(fn));
    }

    Fn fn;
  };

  template<sender S, typename Fn>
  constexpr auto default_impl(S&& s, Fn&& fn) const
  -> sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Fn>> {
    return sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Fn>>(std::forward<S>(s), std::forward<Fn>(fn));
  }
};
inline constexpr lazy_observe_value_t lazy_observe_value{};


// Observe the value channel of a sender chain.
struct observe_value_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender S, typename Fn>
  constexpr auto operator()(S&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<S>(s), std::forward<Fn>(fn));
  }

  template<typename Fn>
  constexpr auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<sender S, typename Fn>
  constexpr auto default_impl(S&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return execution::lazy_observe_value(std::forward<S>(s), std::forward<Fn>(fn));
  }
};
inline constexpr observe_value_t observe_value{};


// Observe the error channel of a sender chain.
// Note: if the observer-function throws an exception, it'll be forwarded as the error.
struct lazy_observe_error_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender S, typename Fn>
  constexpr auto operator()(S&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_error_t>(*this, std::forward<S>(s), std::forward<Fn>(fn));
  }

  template<typename Fn>
  constexpr auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<receiver Receiver, typename Fn>
  class receiver_impl
  : public _generic_receiver_wrapper<Receiver, set_error_t>
  {
    public:
    explicit receiver_impl(Receiver&& r, Fn&& fn)
    : _generic_receiver_wrapper<Receiver, set_error_t>(std::move(r)),
      fn(std::move(fn))
    {}

    template<typename Error>
    friend auto tag_invoke(set_error_t set_error, receiver_impl&& self, Error&& error) noexcept -> void {
      try {
        std::invoke(std::move(self.fn), std::as_const(error));
      } catch (...) {
        set_error(std::move(self.r), std::current_exception());
        return;
      }
      set_error(std::move(self.r), std::forward<Error>(error));
    }

    private:
    Fn fn;
  };

  template<sender Sender, typename Fn>
  class sender_impl
  : public _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t>
  {
    template<typename, sender, typename...> friend class ::earnest::execution::_generic_sender_wrapper;

    public:
    explicit sender_impl(Sender&& s, Fn&& fn)
    : _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t>(std::forward<Sender>(s)),
      fn(std::move(fn))
    {}

    explicit sender_impl(Sender&& s, const Fn& fn)
    : _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t>(std::forward<Sender>(s)),
      fn(fn)
    {}

    template<receiver Receiver>
    friend auto tag_invoke(connect_t connect, sender_impl&& self, Receiver&& r) {
      return connect(
          std::move(self.s),
          receiver_impl<std::remove_cvref_t<Receiver>, Fn>(std::forward<Receiver>(r), std::move(self.fn)));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    -> sender_impl<std::remove_cvref_t<OtherSender>, Fn> {
      return sender_impl<std::remove_cvref_t<OtherSender>, Fn>(std::forward<OtherSender>(other_sender), std::move(fn));
    }

    Fn fn;
  };

  template<sender S, typename Fn>
  constexpr auto default_impl(S&& s, Fn&& fn) const
  -> sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Fn>> {
    return sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Fn>>(std::forward<S>(s), std::forward<Fn>(fn));
  }
};
inline constexpr lazy_observe_error_t lazy_observe_error{};


// Observe the error channel of a sender chain.
// Note: if the observer-function throws an exception, it'll be forwarded as the error.
struct observe_error_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender S, typename Fn>
  constexpr auto operator()(S&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_error_t>(*this, std::forward<S>(s), std::forward<Fn>(fn));
  }

  template<typename Fn>
  constexpr auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<sender S, typename Fn>
  constexpr auto default_impl(S&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return execution::lazy_observe_error(std::forward<S>(s), std::forward<Fn>(fn));
  }
};
inline constexpr observe_error_t observe_error{};


// Observe the done channel of a sender chain.
// Note: if the observer-function throws an exception, it'll be forwarded as the error.
struct lazy_observe_done_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender S, std::invocable Fn>
  constexpr auto operator()(S&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_done_t>(*this, std::forward<S>(s), std::forward<Fn>(fn));
  }

  template<std::invocable Fn>
  constexpr auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<receiver Receiver, std::invocable Fn>
  class receiver_impl
  : public _generic_receiver_wrapper<Receiver, set_done_t>
  {
    public:
    explicit receiver_impl(Receiver&& r, Fn&& fn)
    : _generic_receiver_wrapper<Receiver, set_done_t>(std::move(r)),
      fn(std::move(fn))
    {}

    friend auto tag_invoke(set_done_t set_done, receiver_impl&& self) noexcept -> void {
      try {
        std::invoke(std::move(self.fn));
      } catch (...) {
        set_error(std::move(self.r), std::current_exception());
        return;
      }
      set_done(std::move(self.r));
    }

    private:
    Fn fn;
  };

  template<sender Sender, std::invocable Fn>
  class sender_impl
  : public _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t>
  {
    template<typename, sender, typename...> friend class ::earnest::execution::_generic_sender_wrapper;

    public:
    explicit sender_impl(Sender&& s, Fn&& fn)
    : _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t>(std::forward<Sender>(s)),
      fn(std::move(fn))
    {}

    explicit sender_impl(Sender&& s, const Fn& fn)
    : _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t>(std::forward<Sender>(s)),
      fn(fn)
    {}

    template<receiver Receiver>
    friend auto tag_invoke(connect_t connect, sender_impl&& self, Receiver&& r) {
      return connect(
          std::move(self.s),
          receiver_impl<std::remove_cvref_t<Receiver>, Fn>(std::forward<Receiver>(r), std::move(self.fn)));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    -> sender_impl<std::remove_cvref_t<OtherSender>, Fn> {
      return sender_impl<std::remove_cvref_t<OtherSender>, Fn>(std::forward<OtherSender>(other_sender), std::move(fn));
    }

    Fn fn;
  };

  template<sender S, std::invocable Fn>
  constexpr auto default_impl(S&& s, Fn&& fn) const
  -> sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Fn>> {
    return sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Fn>>(std::forward<S>(s), std::forward<Fn>(fn));
  }
};
inline constexpr lazy_observe_done_t lazy_observe_done{};


// Observe the done channel of a sender chain.
// Note: if the observer-function throws an exception, it'll be forwarded as the error.
struct observe_done_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender S, std::invocable Fn>
  constexpr auto operator()(S&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_done_t>(*this, std::forward<S>(s), std::forward<Fn>(fn));
  }

  template<std::invocable Fn>
  constexpr auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<sender S, std::invocable Fn>
  constexpr auto default_impl(S&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return execution::lazy_observe_done(std::forward<S>(s), std::forward<Fn>(fn));
  }
};
inline constexpr observe_done_t observe_done{};


// Upon completion, drop all senders prior to this point.
//
// Normally, a sender/receiver chain hangs on to all components in the chain,
// until the entire chain completes. (There are exceptions, for example `repeat'
// drops any of the inner loops.)
// This can cause problems, if your sender/receiver chain is repeatedly extended,
// because the chain would grow and grow and grow each time it's extended,
// taking up more and more memory, until the system succombs to memory-starvation.
//
// This sender accepts the signals by value, then releases the preceding
// operation-state, and only after that, sends the values onwards.
//
// Note: the nested operation state is not a pointer, so it still takes up space
// after being dropped. All we do is run its destructor.
struct chain_breaker_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender S>
  constexpr auto operator()(S&& s) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<S>(s));
  }

  constexpr auto operator()() const
  -> sender decltype(auto) {
    return _generic_adapter(*this);
  }

  private:
  template<typed_sender Sender, receiver Receiver>
  class opstate
  : public operation_state_base_
  {
    private:
    // The local receiver is passed to the nested sender.
    // It forward completion to the opstate.handle_ method.
    class local_receiver {
      public:
      explicit local_receiver(gsl::not_null<opstate*> state) noexcept
      : state(state.get())
      {}

      local_receiver(local_receiver&& other) noexcept
      : state(std::exchange(other.state, nullptr))
      {}

      template<typename... Args>
      friend auto tag_invoke(set_value_t tag, local_receiver&& self, Args&&... args) noexcept -> void {
        self.state->handle_(tag, std::forward<Args>(args)...);
      }

      template<typename Error>
      friend auto tag_invoke(set_error_t tag, local_receiver&& self, Error&& error) noexcept -> void {
        self.state->handle_(tag, std::forward<Error>(error));
      }

      friend auto tag_invoke(set_done_t tag, local_receiver&& self) noexcept -> void {
        self.state->handle_(tag);
      }

      template<typename Tag, typename... Args>
      requires (_is_forwardable_receiver_tag<Tag> &&
          !std::same_as<Tag, set_value_t> &&
          !std::same_as<Tag, set_error_t> &&
          !std::same_as<Tag, set_done_t>)
      friend auto tag_invoke(Tag tag, const local_receiver& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, const Receiver&, Args...>)
      -> tag_invoke_result_t<Tag, const Receiver&, Args...> {
        return tag(std::as_const(self.state->r), std::forward<Args>(args)...);
      }

      template<typename Tag, typename... Args>
      requires (_is_forwardable_receiver_tag<Tag> &&
          !std::same_as<Tag, set_value_t> &&
          !std::same_as<Tag, set_error_t> &&
          !std::same_as<Tag, set_done_t>)
      friend auto tag_invoke(Tag tag, local_receiver& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, Receiver&, Args...>)
      -> tag_invoke_result_t<Tag, Receiver&, Args...> {
        return tag(self.state->r, std::forward<Args>(args)...);
      }

      template<typename Tag, typename... Args>
      requires (_is_forwardable_receiver_tag<Tag> &&
          !std::same_as<Tag, set_value_t> &&
          !std::same_as<Tag, set_error_t> &&
          !std::same_as<Tag, set_done_t>)
      friend auto tag_invoke(Tag tag, local_receiver&& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, Receiver&&, Args...>)
      -> tag_invoke_result_t<Tag, Receiver&&, Args...> {
        return tag(std::move(self.state->r), std::forward<Args>(args)...);
      }

      private:
      opstate* state = nullptr;
    };

    using nested_opstate_t = std::invoke_result_t<connect_t, Sender, local_receiver>;

    public:
    explicit opstate(Sender&& s, Receiver&& r)
    : r(std::move(r)),
      nested_opstate(std::move(s), local_receiver(this))
    {}

    friend auto tag_invoke(start_t start, opstate& self) noexcept -> void {
      start(*self.nested_opstate);
    }

    private:
    template<typename Tag, typename... Args>
    requires
        std::same_as<Tag, set_value_t> ||
        std::same_as<Tag, set_error_t> ||
        std::same_as<Tag, set_done_t>
    auto handle_(Tag tag, Args... args) noexcept -> void {
      // No, we do not want rvalue-references (`Args&&... args') here:
      // the nested operation state may be required for references to be valid,
      // and we're about to delete those, which would create dangling references.
      // So we accept them by value (making the compiler copy/move construct them),
      // thus ensuring their validity is independent of the nested operation-state.
      nested_opstate.reset();

      try {
        tag(std::move(r), std::move(args)...);
      } catch (...) {
        // set-value is allowed to fail. We mustn't propagate the exception upwards,
        // because we already deleted the nested opstate. So we must handle it locally.
        //
        // Note that this error-handling is optimized away for non-throwing tag invocations
        // (i.e. set_error, set_done, and some cases of set_value).
        set_error(std::move(r), std::current_exception());
      }
    }

    Receiver r;
    optional_operation_state_<nested_opstate_t> nested_opstate;
  };

  template<sender S>
  class sender_impl
  : public _generic_sender_wrapper<sender_impl<S>, S, connect_t>
  {
    template<typename, sender, typename...> friend class ::earnest::execution::_generic_sender_wrapper;

    public:
    explicit sender_impl(S&& s)
    : _generic_sender_wrapper<sender_impl<S>, S, connect_t>(std::move(s))
    {}

    template<receiver Receiver>
    friend auto tag_invoke(connect_t connect, sender_impl&& self, Receiver&& r) -> opstate<S, std::remove_cvref_t<Receiver>> {
      return opstate<S, std::remove_cvref_t<Receiver>>(std::move(self.s), std::forward<Receiver>(r));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    -> sender_impl<std::remove_cvref_t<OtherSender>> {
      return sender_impl<std::remove_cvref_t<OtherSender>>(std::forward<OtherSender>(other_sender));
    }
  };

  template<sender S>
  constexpr auto default_impl(S&& s) const
  -> sender_impl<std::remove_cvref_t<S>> {
    return sender_impl<std::remove_cvref_t<S>>(std::forward<S>(s));
  }
};
inline constexpr chain_breaker_t chain_breaker;


// Observe the value channel of a sender chain.
//
// The functor is invoked as `void(const Tag&, const Args&...)'.
template<typename... Tags>
requires ((std::same_as<Tags, set_value_t> || std::same_as<Tags, set_error_t> || std::same_as<Tags, set_done_t>) &&...)
struct lazy_observe_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender S, typename Fn>
  constexpr auto operator()(S&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<S>(s), std::forward<Fn>(fn));
  }

  template<typename Fn>
  constexpr auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<receiver Receiver, typename Fn>
  class receiver_impl
  : public _generic_receiver_wrapper<Receiver, Tags...>
  {
    public:
    explicit receiver_impl(Receiver&& r, Fn&& fn)
    : _generic_receiver_wrapper<Receiver, set_value_t>(std::move(r)),
      fn(std::move(fn))
    {}

    template<typename Tag, typename... Args>
    requires (std::same_as<Tag, Tags> ||...)
    friend auto tag_invoke(Tag tag, receiver_impl&& self, Args&&... args) -> void {
      std::invoke(std::move(self.fn), std::as_const(tag), std::as_const(args)...);
      tag(std::move(self.r), std::forward<Args>(args)...);
    }

    private:
    Fn fn;
  };

  template<sender Sender, typename Fn>
  class sender_impl
  : public _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t>
  {
    template<typename, sender, typename...> friend class ::earnest::execution::_generic_sender_wrapper;

    public:
    explicit sender_impl(Sender&& s, Fn&& fn)
    : _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t>(std::forward<Sender>(s)),
      fn(std::move(fn))
    {}

    explicit sender_impl(Sender&& s, const Fn& fn)
    : _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t>(std::forward<Sender>(s)),
      fn(fn)
    {}

    template<receiver Receiver>
    friend auto tag_invoke(connect_t connect, sender_impl&& self, Receiver&& r) {
      return connect(
          std::move(self.s),
          receiver_impl<std::remove_cvref_t<Receiver>, Fn>(std::forward<Receiver>(r), std::move(self.fn)));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    -> sender_impl<std::remove_cvref_t<OtherSender>, Fn> {
      return sender_impl<std::remove_cvref_t<OtherSender>, Fn>(std::forward<OtherSender>(other_sender), std::move(fn));
    }

    Fn fn;
  };

  template<sender S, typename Fn>
  constexpr auto default_impl(S&& s, Fn&& fn) const
  -> sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Fn>> {
    return sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Fn>>(std::forward<S>(s), std::forward<Fn>(fn));
  }
};
template<typename... Tags>
requires ((std::same_as<Tags, set_value_t> || std::same_as<Tags, set_error_t> || std::same_as<Tags, set_done_t>) &&...)
inline constexpr lazy_observe_t<Tags...> lazy_observe{};


// Observe the value channel of a sender chain.
//
// The functor is invoked as `void(const Tag&, const Args&...)'.
template<typename... Tags>
requires ((std::same_as<Tags, set_value_t> || std::same_as<Tags, set_error_t> || std::same_as<Tags, set_done_t>) &&...)
struct observe_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender S, typename Fn>
  constexpr auto operator()(S&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<S>(s), std::forward<Fn>(fn));
  }

  template<typename Fn>
  constexpr auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<sender S, typename Fn>
  constexpr auto default_impl(S&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return execution::lazy_observe<Tags...>(std::forward<S>(s), std::forward<Fn>(fn));
  }
};
template<typename... Tags>
requires ((std::same_as<Tags, set_value_t> || std::same_as<Tags, set_error_t> || std::same_as<Tags, set_done_t>) &&...)
inline constexpr observe_t<Tags...> observe{};


// Late-binding a sender.
template<typename Values, typename Errors = std::variant<std::error_code>>
struct late_binding_t;

template<typename... ValueTypes, typename... ErrorTypes>
struct late_binding_t<std::variant<ValueTypes...>, std::variant<ErrorTypes...>> {
  private:
  using receiver_intf = type_erased_sender_base_::receiver_intf<std::variant<ValueTypes...>, std::variant<ErrorTypes...>, true>;
  using sender_intf = type_erased_sender_base_::sender_intf<receiver_intf>;
  using operation_state_intf = type_erased_sender_base_::operation_state_intf;

  struct canceled {
    bool done = false;
  };

  struct direct_values_t {
    explicit direct_values_t(std::variant<ValueTypes...>&& values)
    : values(std::move(values))
    {}

    explicit direct_values_t(const std::variant<ValueTypes...>& values)
    : values(values)
    {}

    std::variant<ValueTypes...> values;
    bool done = false;
  };

  // The shared state between the acceptor and the sender.
  //
  // The acceptor may call either:
  // - attach_snd: attach a sender
  // - acceptor_detach: detach the acceptor without attaching a sender
  // The acceptor must call exactly one of these.
  //
  // The sender may call:
  // - attach_rcv: attach a receiver (caller must maintain the receiver)
  // - start: requeest the whole operation to be started (requires that attach_rcv has been called first)
  //
  // Only once:
  // - acceptor has called attach_snd or acceptor_detach
  // - and sender has called start
  // will the shared-state be started.
  class shared_state {
    public:
    shared_state() = default;

    ~shared_state() {
      // We must have started the operation, if it was requested.
      // So we assert on this prior to destruction.
      assert(!start_requested_ || is_started());
    }

    private:
    // Check if the acceptor has installed a sender.
    // (Caller must lock.)
    auto is_bound() const noexcept -> bool {
      return !std::holds_alternative<std::monostate>(late_binding);
    }

    // Check if the state has been started.
    // (Caller must lock.)
    auto is_started() const noexcept -> bool {
      if (std::holds_alternative<gsl::not_null<std::unique_ptr<operation_state_intf>>>(late_binding)) return true;
      if (std::holds_alternative<canceled>(late_binding)) return std::get<canceled>(late_binding).done;
      if (std::holds_alternative<direct_values_t>(late_binding)) return std::get<direct_values_t>(late_binding).done;
      return false;
    }

    public:
    auto start() noexcept -> void {
      std::unique_lock lck{mtx};
      assert(!start_requested_);
      assert(!is_started());
      assert(rcv != nullptr); // You cannot start an operation-state without first attaching a receiver.
      start_requested_ = true;
      maybe_actually_start(std::move(lck));
    }

    // Acceptor is detached without installing a sender.
    auto acceptor_detach() noexcept -> void {
      // If the shared-state was supposed to start, but never actually started, we'll propagate a cancelation signal.
      // (Because once a start is requested, we must produce exactly one signal, and we obviously can't propagate
      // the values signal.)
      // XXX An alternative is to propagate an exception (and maybe that's the correct answer, I don't know).
      std::unique_lock lck{mtx};
      assert(!is_bound());
      late_binding.template emplace<canceled>();
      maybe_actually_start(std::move(lck));
    }

    auto attach_rcv(gsl::not_null<receiver_intf*> rcv) noexcept -> void {
      assert(rcv != nullptr);
      std::lock_guard lck{mtx};
      assert(this->rcv == nullptr);
      assert(!start_requested_); // Attaching the receiver is needed to create the operation-state,
                                 // so a start cannot have been requested.
      assert(!is_started());
      this->rcv = std::move(rcv);
    }

    auto attach_snd(std::unique_ptr<sender_intf> snd) noexcept -> void {
      std::unique_lock lck{mtx};
      assert(!is_bound());
      late_binding.template emplace<gsl::not_null<std::unique_ptr<sender_intf>>>(std::move(snd));
      maybe_actually_start(std::move(lck));
    }

    auto attach_error(gsl::not_null<std::exception_ptr> ex) noexcept -> void {
      std::unique_lock lck{mtx};
      assert(!is_bound());
      late_binding.template emplace<gsl::not_null<std::exception_ptr>>(std::move(ex));
      maybe_actually_start(std::move(lck));
    }

    template<typename Tpl>
    auto attach_values(Tpl&& values_tuple) -> void {
      std::unique_lock lck{mtx};
      assert(!is_bound());
      late_binding.template emplace<direct_values_t>(std::forward<Tpl>(values_tuple));
      maybe_actually_start(std::move(lck));
    }

    private:
    // This should be part of the STL. But it's not.
    template<typename... T>
    struct overload_t
    : T...
    {
      explicit overload_t(T... t) : T(std::move(t))... {}
      using T::operator()...;
    };
    template<typename... T>
    static auto overload(T&&... t) -> overload_t<std::remove_cvref_t<T>...> {
      return overload_t<std::remove_cvref_t<T>...>(std::forward<T>(t)...);
    }

    auto maybe_actually_start(std::unique_lock<std::mutex> lck) noexcept -> void {
      assert(lck.owns_lock());
      if (!start_requested_) return; // No desire to start yet.
      if (!is_bound()) return; // No sender installed.
      if (rcv == nullptr) return; // No receiver installed.

      std::visit(
          overload(
              []([[maybe_unused]] const std::monostate&) noexcept -> void {
                return; // Nothing yet: we're still waiting for sender to be installed.
              },
              [this, &lck](canceled& c) noexcept -> void {
                assert(!c.done);
                c.done = true;
                lck.unlock();
                execution::set_done(std::move(*this->rcv)); // If no sender is installed, we assume it's due to being canceled.
                                                            // XXX should we error instead?
              },
              []([[maybe_unused]] const gsl::not_null<std::unique_ptr<operation_state_intf>>&) noexcept -> void {
                assert(false); // This state would mean the already-started operation is started twice.
#if __cpp_lib_unreachable >= 202202L
                std::unreachable();
#endif
              },
              [this, &lck](gsl::not_null<std::unique_ptr<sender_intf>>& s) noexcept -> void {
                using state_ptr_t = gsl::not_null<std::unique_ptr<operation_state_intf>>;

                operation_state_intf* state_ptr = nullptr;
                try {
                  state_ptr = this->late_binding.template emplace<state_ptr_t>(std::move(*s).connect(this->rcv)).get().get();
                } catch (...) {
                  this->late_binding.template emplace<canceled>(canceled{true}); // Mark the state as having been started.
                                                                                 // Otherwise our destructor will be angry.
                  lck.unlock();
                  execution::set_error(std::move(*this->rcv), std::current_exception());
                  return;
                }

                lck.unlock();
                assert(state_ptr != nullptr);
                state_ptr->start(); // Never throws.
                                    // Note that we must run this outside the lock,
                                    // because it could complete immediately,
                                    // at which point it might cause this shared_state to be destroyed.
              },
              [this, &lck](gsl::not_null<std::exception_ptr> ex) noexcept -> void {
                late_binding.template emplace<canceled>(canceled{true});
                lck.unlock();
                execution::set_error(std::move(*this->rcv), ex.get());
              },
              [this, &lck](direct_values_t& direct_values) noexcept -> void {
                assert(!direct_values.done);
                direct_values.done = true;
                lck.unlock();
                try {
                  std::visit(
                      [&](auto& tpl) -> void {
                        std::apply(
                            [&](auto&... v) -> void {
                              execution::set_value(std::move(*this->rcv), std::move(v)...);
                            },
                            tpl);
                      },
                      direct_values.values);
                } catch (...) {
                  execution::set_error(std::move(*this->rcv), std::current_exception());
                }
              }),
          late_binding);

      // At this point, the operation-state will have been started.
      // Note that, because the receiver can complete immediately,
      // and the receiver may clean up its operation-state,
      // and that operation-state might have held the last-remaining reference to this shared_state,
      // we can no longer rely on this operation-state being valid.
      //
      // This means that `this' is now a dangling pointer.
    }

    std::mutex mtx;
    std::variant<
        std::monostate,
        gsl::not_null<std::unique_ptr<sender_intf>>,
        gsl::not_null<std::unique_ptr<operation_state_intf>>,
        canceled,
        gsl::not_null<std::exception_ptr>,
        direct_values_t> late_binding;
    receiver_intf* rcv;
    bool start_requested_ = false;
  };

  template<receiver Receiver>
  class opstate
  : public operation_state_base_
  {
    public:
    explicit opstate(gsl::not_null<std::shared_ptr<shared_state>> state, Receiver&& rcv)
    : state(state),
      rcv(std::move(rcv))
    {
      state->attach_rcv(&this->rcv);
    }

    friend auto tag_invoke([[maybe_unused]] start_t start, opstate& self) noexcept -> void {
      self.state->start();
    }

    private:
    gsl::not_null<std::shared_ptr<shared_state>> state;
    receiver_intf::template impl<Receiver> rcv;
  };

  public:
  class acceptor {
    template<typename, typename> friend struct late_binding_t;

    public:
    acceptor() = default;
    acceptor(const acceptor&) = delete;
    acceptor(acceptor&&) noexcept = default;
    acceptor& operator=(const acceptor&) = delete;
    acceptor& operator=(acceptor&&) noexcept = default;

    ~acceptor() {
      // We must inform the state if we don't fulfill the promise of attaching.
      if (state != nullptr) state->acceptor_detach();
    }

    private:
    explicit acceptor(gsl::not_null<std::shared_ptr<shared_state>> state)
    : state(state)
    {}

    public:
    explicit operator bool() const noexcept {
      return state != nullptr;
    }

    auto operator!() const noexcept -> bool {
      return state == nullptr;
    }

    // Assign a sender chain.
    //
    // Strong exception guarantee.
    template<typed_sender Sender>
    auto assign(Sender&& sender) && -> void {
      if (state == nullptr) throw std::logic_error("invalid acceptor during assigning a sender");

      using impl_type = typename sender_intf::template impl<std::remove_cvref_t<Sender>>;
      state->attach_snd(std::make_unique<impl_type>(std::forward<Sender>(sender)));
      state.reset(); // Otherwise our destructor will cause a cancelation to be propagated.
    }

    // Assign an error.
    //
    // Strong exception guarantee.
    auto assign_error(gsl::not_null<std::exception_ptr> ex) && -> void {
      if (state == nullptr) throw std::logic_error("invalid acceptor during assigning a sender");
      state->attach_error(std::move(ex));
      state.reset();
    }

    // Assign specific values.
    //
    // Only throws if the acceptor is invalid, or any of the constructors for the values throws.
    template<typename... T>
    requires std::constructible_from<std::variant<ValueTypes...>, T...>
    auto assign_values(T&&... v) {
      if (state == nullptr) throw std::logic_error("invalid acceptor during assigning a sender");
      state->attach_values(std::variant<ValueTypes...>(std::forward<T>(v)...));
      state.reset();
    }

    private:
    std::shared_ptr<shared_state> state;
  };

  // The sender.
  //
  // Implements a typed-sender that may send a `done' signal.
  class sender {
    template<typename, typename> friend struct late_binding_t;

    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<typename type_erased_sender_base_::rebind_values_tuple<ValueTypes>::template type<Tuple>...>;

    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr, ErrorTypes...>;

    static inline constexpr bool sends_done = true;

    sender() = default;
    sender(const sender&) = delete;
    sender(sender&&) = default;
    sender& operator=(const sender&) = delete;
    sender& operator=(sender&&) = default;

    private:
    explicit sender(gsl::not_null<std::shared_ptr<shared_state>> state)
    : state(state)
    {}

    public:
    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t connect, sender&& self, Receiver&& rcv) -> opstate<std::remove_cvref_t<Receiver>> {
      return opstate<std::remove_cvref_t<Receiver>>(self.state, std::forward<Receiver>(rcv));
    }

    private:
    std::shared_ptr<shared_state> state;
  };

  auto operator()() const -> std::tuple<acceptor, sender> {
    auto state = std::make_shared<shared_state>();
    return std::make_tuple(acceptor(state), sender(state));
  }
};
template<typename Values, typename Errors = std::variant<std::exception_ptr>>
inline constexpr late_binding_t<Values, Errors> late_binding{};


template<typename... Tags>
requires ((std::same_as<Tags, set_value_t> || std::same_as<Tags, set_error_t> || std::same_as<Tags, set_done_t>) &&...)
struct lazy_let_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  // When sender produces the set-value signal, invoke fn.
  // Fn will return a sender, and we'll run that next.
  template<sender Sender, typename Fn>
  auto operator()(Sender&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<>(*this, std::forward<Sender>(s), std::forward<Fn>(fn));
  }

  template<typename Fn>
  auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<sender Sender, typename Fn>
  auto default_impl(Sender&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _let_adapter_common_t::impl<Tags...>(std::forward<Sender>(s), std::forward<Fn>(fn));
  }
};
template<typename... Tags>
requires ((std::same_as<Tags, set_value_t> || std::same_as<Tags, set_error_t> || std::same_as<Tags, set_done_t>) &&...)
inline constexpr lazy_let_t<Tags...> lazy_let{};


template<typename... Tags>
requires ((std::same_as<Tags, set_value_t> || std::same_as<Tags, set_error_t> || std::same_as<Tags, set_done_t>) &&...)
struct let_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  // When sender produces the set-value signal, invoke fn.
  // Fn will return a sender, and we'll run that next.
  template<sender Sender, typename Fn>
  auto operator()(Sender&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<>(*this, std::forward<Sender>(s), std::forward<Fn>(fn));
  }

  template<typename Fn>
  auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<sender Sender, typename Fn>
  auto default_impl(Sender&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return lazy_let<Tags...>(std::forward<Sender>(s), std::forward<Fn>(fn));
  }
};
template<typename... Tags>
requires ((std::same_as<Tags, set_value_t> || std::same_as<Tags, set_error_t> || std::same_as<Tags, set_done_t>) &&...)
inline constexpr let_t<Tags...> let{};


struct handle_error_code_t {
  template<typename> friend struct execution::_generic_operand_base_t;

  template<sender Sender>
  auto operator()(Sender&& s) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<Sender>(s));
  }

  auto operator()() const
  -> decltype(auto) {
    return _generic_adapter(*this);
  }

  private:
  template<receiver Receiver>
  struct wrapped_receiver
  : public _generic_receiver_wrapper<Receiver, set_value_t>
  {
    explicit wrapped_receiver(Receiver&& r)
    : _generic_receiver_wrapper<Receiver, set_value_t>(std::forward<Receiver>(r))
    {}

    template<typename... Args>
    friend auto tag_invoke([[maybe_unused]] set_value_t, wrapped_receiver&& self, std::error_code ec, Args&&... args) -> void {
      if (ec)
        execution::set_error(std::move(self.r), ec);
      else
        execution::set_value(std::move(self.r), std::forward<Args>(args)...);
    }
  };

  template<template<typename...> class Tuple>
  struct without_error_code_ {
    template<typename...> struct type_;

    template<typename... T>
    struct type_<std::error_code, T...> : _type_appender<T...> {};

    template<typename... T>
    using type = type_<T...>::template type<Tuple>;
  };

  template<sender Sender> class sender_impl;

  template<sender Sender>
  struct sender_types_for_impl
  : public _generic_sender_wrapper<sender_impl<Sender>, Sender, connect_t, get_completion_scheduler_t<set_error_t>>
  {
    using _generic_sender_wrapper<sender_impl<Sender>, Sender, connect_t, get_completion_scheduler_t<set_error_t>>::_generic_sender_wrapper;
  };

  template<typed_sender Sender>
  struct sender_types_for_impl<Sender>
  : public _generic_sender_wrapper<sender_impl<Sender>, Sender, connect_t, get_completion_scheduler_t<set_error_t>>
  {
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = typename sender_traits<Sender>::template value_types<without_error_code_<Tuple>::template type, Variant>;

    template<template<typename...> class Variant>
    using error_types = typename sender_traits<Sender>::template error_types<_type_appender>
        ::template append<std::error_code>
        ::template type<Variant>;

    using _generic_sender_wrapper<sender_impl<Sender>, Sender, connect_t, get_completion_scheduler_t<set_error_t>>::_generic_sender_wrapper;
  };

  template<sender Sender>
  class sender_impl
  : public sender_types_for_impl<Sender>
  {
    template<typename, sender, typename...> friend class ::earnest::execution::_generic_sender_wrapper;

    public:
    explicit sender_impl(Sender&& s)
    : sender_types_for_impl<Sender>(std::forward<Sender>(s))
    {}

    template<receiver Receiver>
    friend auto tag_invoke(connect_t connect, sender_impl&& self, Receiver&& r)
    -> decltype(auto) {
      return connect(std::move(self.s), wrapped_receiver<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r)));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    -> sender_impl<std::remove_cvref_t<OtherSender>> {
      return sender_impl<std::remove_cvref_t<OtherSender>>(std::forward<OtherSender>(other_sender));
    }
  };

  template<sender Sender>
  auto default_impl(Sender&& s) const
  -> sender auto {
    return sender_impl<std::remove_cvref_t<Sender>>(std::forward<Sender>(s));
  }
};
inline constexpr handle_error_code_t handle_error_code;


} /* inline namespace extensions */
} /* namespace earnest::execution */
