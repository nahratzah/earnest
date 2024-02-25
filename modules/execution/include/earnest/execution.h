/*
 * This is an implementation of p2300r1.
 * https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2021/p2300r1.html
 *
 * You probably want to read that, to get the what/how/why of this file. <3
 *
 * Notes:
 * - sender_traits: we always deduplicate the types presented to the Variant.
 *   The document doesn't mention if this should be done, nor if it should be prohibited.
 *   I think it's an oversight: deduplication is pretty cheap, and will greatly simplify implementing type-dependent receivers/adapters.
 * - into_variant: the spec is unclear if this should be eager or lazy. I chose eager.
 * - sync_wait: the spec requires this to exist in std::this_thread.
 *   Because this isn't an STL implementation, we didn't do that, and instead leave it here.
 * - sync_wait: the spec is unclear on how it thinks the associated scheduler should operate.
 *   I chose to have it have a job-submission-queue, and a loop executing jobs.
 *   The reason was that if I ran jobs in-line, I fear recursive-locking could cause hard-to-debug deadlocks.
 * - get_completion_scheduler: the spec says to only select tag-invoke specializations that are noexcept.
 *   I think this is wrong, because it would mean a forgotten noexcept would SFINAE skip the scheduler-specific optimizations.
 *   I think a static-assert should be used instead, if the noexcept property is considered important.
 * - get_scheduler: the spec says to only select tag-invoke specializations that are noexcept.
 *   I chose to instead use a static-assert.
 * - get_allocator: the spec says to only select tag-invoke specializations that are noexcept.
 *   I chose to instead use a static-assert.
 * - get_stop_token: the spec says to only select tag-invoke specializations that are noexcept.
 *   I chose to instead use a static-assert.
 * - lazy_upon_error: the spec says that, when receiving the set_value signal, to invoke set_value on the wrapped receiver.
 *   However, this means that if the wrapped receiver generates an error, it would escape the handler.
 *   In the case of `just() | upon_error(first{}) | then([]() { throw ...; } | upon_error(second{})`
 *   this would mean the `first`-functor would handle the exception thrown at the then-step.
 *   The correct behaviour should be that it invokes set_error, upon spotting an error in the set_value signal.
 * - lazy_upon_done: the spec says that, when receiving the set_value signal, to invoke set_value on the wrapped receiver.
 *   However, this means that if the wrapped receiver generates an error, it would escape the handler.
 *   In the case of `just() | upon_done(first{}) | then([]() { throw ...; } | upon_done(second{})`
 *   this would mean the `first`-functor would handle the exception thrown at the then-step.
 *   The correct behaviour should be that it invokes set_error, upon spotting an error in the set_value signal.
 * - lazy_upon_done: the spec doesn't explicitly say that the sends_done flag should be set to false.
 *   But I think it should be, because any done-signals are intercepted.
 * - upon_done/upon_error/let_value/lazy_let_value/let_error/lazy_let_error/let_done/lazy_let_done:
 *   These operations mix two sources of signals:
 *   - the original predecessor operation's signal,
 *   - a signal created from applying a function, causing a signal change.
 *   This means that there is no way these functions can be implemented conforming to the standard.
 *   For example, take `Predecessor | LetDone', where:
 *   - `LetDone = let_done([]() { return ""; })'
 *   - `get_completion_scheduler<set_value_t>(Predecessor) = ValueSched'
 *   - `get_completion_scheduler<set_done_t>(Predecessor) = DoneSched'
 *   If Predecessor in the chain issues a set_value signal, our `LetDone' adapter
 *   should advertise `get_completion_scheduler<set_value_t>(LetDone) = ValueSched'.
 *   But if Predecessor were to issue the set_done signal, our `LetDone` adapter
 *   should advertise `get_completion_scheduler<set_value_t>(LetDone) = DoneSched'.
 * - connect: Whenever a sender and receiver are connected, an operation state must be created.
 *   Thus, the sender must always be a complete sender.
 *   We've decided to use typed_sender instead of sender, for the sender-constraint on connect.
 * - lazy_bulk: the spec says that, if any of the lazy-bulk operations throws an exception, we are to catch-and-forward it.
 *   We've chosen not to do this, because all implementations of start in the spec are required to catch-and-forward
 *   exception whenever they occur. So we can rely on the exception handler of the start-invocation instead.
 *
 *   Unless both schedulers are the same, this is a contradiction. Even if they have the same type,
 *   they may still be different schedulers. So the only way to deal with that, is to not advertise
 *   the `get_completion_scheduler's that would thus contradict.
 * - ensure_started: the spec doesn't require us to hang on to the execution-state of the preceding operation-chain.
 *   So I opted to throw it away at earliest opportunity.
 *
 * 9.6.1 sender traits
 *
 * Item (5) talks about the error_types, stating that if an error type is emitted that's not in the error_types,
 * that the program shall be ill-formed. However the document itself shows the `just' sender do exactly that:
 * call set_error (with std::exception_ptr) while advertising an empty error_types.
 * I'm pretty sure that's unintentional. But it does leave one wondering what the correct implementation is.
 * I've decided to add std::exception_ptr to the error-types of just.
 *
 * Item (6) says that if a sender with `sends_done = true` sends a done-signal, the program is ill-formed.
 * I'm pretty sure that `sends_done = false` was meant here.
 *
 * IMPLEMENTATION
 *
 * *** Receivers are stored within operation_state ***
 *
 * We expect that any operation_state, will contain within it the receiver that was part of the attach operation.
 * Therefore when a nested operation-state is created, storing it inside the receiver will give it the same lifetime
 * as the operation-state from which it was created.
 *
 * Ex: lazy_let_value create a receiver MainReceiver which accepts values.
 * During operation-state MainState, it'll receive some values.
 * When it receives those values, it'll create a new operation-state NestedState
 * using its function and its original receiver.
 * NestedState is stored within MainReceiver.
 *
 * Because MainReceiver is stored within MainContext, it has
 * the same lifetime as MainContext.
 * Because NestedState is stored within MainReceiver, it has
 * the same lifetime as MainReceiver.
 * Therefore, NestedState has the same lifetime as MainContext.
 *
 *
 * *** Generic operand base ***
 *
 * The _generic_operand_base is an invocable, which handles the rules for how each adapter
 * should resolve.
 *
 * Each adapter must call `tag_invoke(tag, scheduler, sender, ...)' if it exists.
 * Otherwise, it must call `tag_invoke(tag, sender, ...)' if it exists.
 * Otherwise, it must perform a default operaton, if it has one.
 *
 * We capture this in the _generic_operand_base, so we don't have to keep writing
 * the same code each time. As noted above, we thing requiring selection on a tag_invoke
 * being noexcept, is wrong, and will lead to errors. So we don't care and instead
 * propagate the noexcept state.
 *
 * Adapters that require the whole operation to be noexcept, should error if this
 * requirement isn't met: `static_assert(noexcept(_generic_operand_base ...))'.
 *
 * XXX We should do the same to confirm a tag_invoke returns a sender/receiver/scheduler/etc
 * as needed.
 *
 *
 * *** Generic adapter ***
 *
 * When a tag is invoked without the sender parameter, a late-binding operation is created.
 * For example, `adapter = then([](int i) { return i + 1; })' creates a late-binding operation.
 * When later the expression `MySender | adapter' is called, it'll be bound.
 *
 * Since this pattern happens all the time, and always resolves into
 * `tag(MySender, adapter)', I'm using a `_generic_adapter_t' to capture all of this.
 *
 *
 * *** Generic receiver wrapper ***
 *
 * We often need to wrap a receiver inside another receiver. The spec requires
 * we forward receiver-queries to the wrapped receiver.
 *
 * Because forwarding requires a lot of the same code, we use `_generic_receiver_wrapper'
 * as a base class. It'll forward all receiver operations, except those marked as specialized.
 * Specialized tags are supposed to be implemented by the derived type.
 *
 *
 * *** Generic sender wrapper ***
 *
 * We often need to wrap a sender inside another sender. The spec requires
 * we forward sender-queries to the wrapped sender.
 *
 * Because it's a lot of the same code, we use `_generic_sender_wrapper'
 * as a base class. It'll forward all sender operations, except those marked as specialized.
 * Specialized tags are supposed to be implemented by the derived type.
 *
 * The `_generic_receiver_wrapper' needs to know its derived type, so it can use
 * the curiously-recurring-template trick to invoke `rebind' on the derived type.
 * The `rebind' method must create a new instance of itself, but with a replacement sender.
 * This method ensures we implement sender-chain handling.
 *
 *
 * *** Pipe operator ***
 *
 * We need to support any sender-chains using the pipe operator.
 *
 * We implement this by creating:
 * `decltype(auto) operator| (sender auto x, Adapter)' for each `Adapter'.
 *
 * Because all our adapters derive from _generic_adapter_t, and all
 * senders derive from _generic_sender_wrapper,
 * those two types declare the `operator|'.
 *
 * Note that for these pipes, the second type is required for resolving,
 * while the first type is unspecialized.
 *
 * The document didn't specify if I should make those `operator|' friend operations
 * or not. I decided to place them in the ::earnest::execution namespace.
 * (Although friend-declaration might be easier for users, as it'll result in less noise
 * when a `|' cannot be resolved. So maybe I'm chosing wrong.)
 *
 *
 * *** Pointer receiver wrapper ***
 *
 * Often, a receiver isn't immediately connected to another sender, but this is delayed.
 * For example, the `lazy_let_value'-adapter, when bound to a receiver, doesn't
 * connect immediately. Instead, it's only connected once the `lazy_let_value'-adapter
 * is given a value-signal.
 *
 * This connecting may fail, in which case we need to invoke the set_error-signal on
 * that receiver. Should we use normal move-construction, then there's a possibility
 * the move-construction is destructive, even if an exception is thrown.
 * In that scenario, we would no longer be able to invoke the receiver with the error.
 *
 * So instead, we use `_generic_rawptr_receiver', which wraps a receiver using a pointer.
 * If during connecting a failure happens, the original receiver will be unchanged,
 * and it's safe to call set_error on it.
 *
 * As an added bonus, the `_generic_rawptr_receiver' means we don't copy/move a
 * (potentially unwieldy) completion chain across during late-connecting.
 *
 *
 * *** Late connects ***
 *
 * When a connect happens as result of starting an operation-state, we refer to this
 * as a late connect. Whenever a late-connect happens, the operation-state that results
 * must be stored inside the parent operation-state.
 *
 * There are a couple of ways to do this:
 * - pointer to type-erased operation-state
 * - std::variant to different operation-types
 *
 * I've chosen to use the latter: a variant.
 * `std::variant<std::monostate, OperationState1, OptiorationState2, ... OperationStateN>'
 *
 * Initially, the variant is set to the monostate alternative.
 * In this case, it's copy-able/move-able.
 *
 * Upon a connect, the variant will be made to hold the new operation state.
 * At this point, the variant will no longer be moveable.
 * This is enforced using `assert(false);'.
 *
 * The only overhead from this is:
 * - memory: 1 index for the variant.
 * - complexity/code: only the destructor needs a visitor-pattern.
 * So it should be pretty efficient.
 *
 *
 * *** Noexcept ***
 *
 * This code tries to be very dilligent with regards to noexcept,
 * propagating the noexcept-property whenever possible.
 *
 * This is not great for readability.
 * It also introduces bugs, for example if decltype(auto) is used with a noexcept specifier,
 * in which case the return-type will be computed after the noexcept statement has been evaluated,
 * meaning SFINAE doesn't apply. This is super-annoying.
 *
 * Would be nice if we have `noexcept(auto)'. But alas.
 */

#pragma once

#include <cassert>
#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <execution>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

#include "stop_token.h"
#include "move_only_function.h"

namespace earnest::execution {

// tag_invoke needs to be set up in such a way that it performs ADL.
// We place an implementation in a sub-namespace, that handles the ADL part.
//
// The one that'll end up in namespace earnest::execution, we'll define even deeper in this namespace,
// so it won't be visible to the overload resolver.
//
// It's also important to have it in a different namespace, so we won't get symbol clashes.
// (Compiler gets pretty angry when a type is both a variable and a function.)
namespace _earnest_execution_support_ {

void tag_invoke();

struct do_tag_invoke {
  template<typename Tag, typename... Args>
  constexpr auto operator()(Tag tag, Args&&... args) const
  noexcept(noexcept(tag_invoke(std::declval<Tag>(), std::declval<Args>()...)))
  -> decltype(tag_invoke(std::declval<Tag>(), std::declval<Args>()...)) {
    return tag_invoke(std::move(tag), std::forward<Args>(args)...);
  }
};

namespace impl {
inline constexpr do_tag_invoke tag_invoke{};
} /* namespace impl */
} /* namespace _earnest_execution_support_ */


// Make it so that ::earnest::execution::tag_invoke will always do ADL-based invocation of tag_invoke.
inline namespace impl {
using namespace _earnest_execution_support_::impl;
} /* inline namespace impl */


// Helper type for type-deduplication.
//
// This type is used internally by the `_deduplicate` template.
//
// To use:
// - create an empty state: `using state = _deduplicator_state_<>`
// - repeatedly add types: `using new_state = typename state::template add_type<NewType>`
//   the new type is only added, if it wasn't yet present in the state.
// - apply to your std::variant etc: `using result = typename new_state::template type<std::variant>`
template<typename... T>
struct _deduplicator_state_ {
  // Apply the types to a template.
  template<template<typename...> class Variant>
  using type = Variant<T...>;

  // Add type U, unless there is already a type U present.
  template<typename U>
  using add_type =
      std::conditional_t<
        std::disjunction_v<std::is_same<T, U>...>,
        _deduplicator_state_,
        _deduplicator_state_<T..., U>
      >;
};

// Deduplicator.
//
// Takes a sequence of types, and one by one adds them to the state.
// State is expected to be `_deduplicator_state_` (or something that responds similar).
//
// To use:
// - create your deduplicator with an empty initial state, and all the the types:
//   `using dd = _deduplicator_<_deduplicator_state_<>, Ts...>`
// - apply to your std::variant etc: `using result = typename dd::template type<std::variant>`
template<typename State, typename... Ts>
struct _deduplicate_;

// Specialization of deduplicator.
// Adds 1 type to the state, then recurses.
template<typename State, typename T0, typename... Ts>
struct _deduplicate_<State, T0, Ts...>
: _deduplicate_<typename State::template add_type<T0>, Ts...>
{};

// Specialization of deduplicator.
// Ends the recursion when there are no more types left to add.
template<typename State>
struct _deduplicate_<State> : State {};

// Deduplicator, with early binding variant-template, and late-binding types.
//
// Use if `using result = std::variant<T...>` would lead to duplicate types:
// `using result = _deduplicate<std::variant>::type<T...>`.
//
// This can be used in the sender_traits::value_types and sender_traits::error_types,
// to remove duplicate types. (For example, the `lazy_next_t` operation uses this.)
template<template<typename...> class Variant>
struct _deduplicate {
  template<typename... Ts>
  using type = typename _deduplicate_<_deduplicator_state_<>, Ts...>::template type<Variant>;
};


// Forward declaration of _type_appender.
template<typename...> struct _type_appender;

template<typename, typename...> struct _merge;
template<typename... InitialTypes>
struct _merge<_type_appender<InitialTypes...>> {
  using type = _type_appender<InitialTypes...>;
};
template<typename... InitialTypes, typename... ExtraTypes, typename... TypeAppenders>
struct _merge<_type_appender<InitialTypes...>, _type_appender<ExtraTypes...>, TypeAppenders...>
: _merge<_type_appender<InitialTypes..., ExtraTypes...>, TypeAppenders...>
{};
template<typename... TypeAppenders>
using _empty_merge = _merge<_type_appender<>, TypeAppenders...>;

// This type gathers types.
// Use the nested `append`-type to add more types.
// Finally, use the `type`-type to resolve the type_appender into the variant.
template<typename... InitialTypes>
struct _type_appender {
  template<typename...> friend struct _type_appender;

  // Apply the type-appender to a template.
  template<template<typename...> class Variant>
  using type = Variant<InitialTypes...>;

  // Append types to this appender.
  template<typename... ExtraTypes>
  using append = _type_appender<InitialTypes..., ExtraTypes...>;

  // Merge multiple type-appenders into this type-appender.
  template<typename... TypeAppenders>
  using merge = typename _merge<_type_appender, TypeAppenders...>::type;

  // Apply a type-transformation on all types in this type-appender.
  template<template<typename...> class Transformation>
  using transform = _type_appender<Transformation<InitialTypes>...>;

  private:
  // Transformation for filtering.
  // Turns any element `T' into either `_type_appender<T>' or `_type_appender'.
  template<template<typename> class Predicate>
  struct retain_transformation_ {
    template<typename T>
    using type = std::conditional_t<Predicate<T>::value, _type_appender<T>, _type_appender<>>;
  };

  public:
  // Filter elements, only retaining elements for which the predicate is true.
  //
  // Works by converting the types into zero-element or single-element collections
  // (based on the Predicate) and then squashing that all into a single collection.
  template<template<typename> class Predicate>
  using retain =
      typename transform<retain_transformation_<Predicate>::template type>:: // Turn this into a collection of zero-element or one-element _type_appenders
                                                                             // `_type_appender<_type_appender<>, _type_appender<T>, ...>'
      template type<_empty_merge>::type;                                     // and then merge them all together
};


template<auto& Tag>
using tag_t = std::decay_t<decltype(Tag)>;

template<typename Tag, typename... Args>
concept tag_invocable = std::invocable<decltype(tag_invoke), Tag, Args...>;

template<typename Tag, typename... Args>
concept nothrow_tag_invocable =
    tag_invocable<Tag, Args...> &&
    std::is_nothrow_invocable_v<decltype(tag_invoke), Tag, Args...>;

template<typename Tag, typename... Args>
using tag_invoke_result = std::invoke_result<decltype(tag_invoke), Tag, Args...>;

template<typename Tag, typename... Args>
using tag_invoke_result_t = std::invoke_result_t<decltype(tag_invoke), Tag, Args...>;


// Helper, that removes cvref from all types, before wrapping it in a tuple.
template<template<typename...> class Tuple>
struct remove_cvref_tuple_ {
  template<typename... T>
  using type = Tuple<std::remove_cvref_t<T>...>;
};


// This class tags any senders, that don't specify value_types/error_types/sends_done.
class sender_base {
  protected:
  // Mark all constructors and destructors as protected,
  // to prevent slicing issues.
  constexpr sender_base() noexcept = default;
  constexpr sender_base(const sender_base&) noexcept = default;
  constexpr sender_base(sender_base&&) noexcept = default;
  constexpr sender_base& operator=(const sender_base&) noexcept = default;
  constexpr sender_base& operator=(sender_base&&) noexcept = default;
  ~sender_base() noexcept = default;
};

// Figure out if a type has the value_types, error_types, and sends_done members.
template<typename S>
concept _has_sender_traits_members_ = requires {
  typename S::template value_types<std::tuple, std::variant>;
  typename S::template error_types<std::variant>;
  { std::bool_constant<S::sends_done>{} };
};

// Implementation of sender traits.
//
// The default is to not have an implementation.
// So that if you use it, the compiler will complain if you try to use it.
//
// We use a hidden type, so that we can specialize. (That's what all those voids are for.)
template<typename S, typename = void>
struct _sender_traits_ {
  _sender_traits_() = delete;
  _sender_traits_(const _sender_traits_&) = delete;
  _sender_traits_(_sender_traits_&&) = delete;
  _sender_traits_& operator=(const _sender_traits_&) = delete;
  _sender_traits_& operator=(_sender_traits_&&) = delete;

  using __unspecialized = void; // exposition only
};

// We let sender-traits derive from _sender_traits_.
// This is because we have 3 different cases,
// and we also are required to allow specializations of this class.
template<typename S>
struct sender_traits
: public _sender_traits_<S>
{};

// For senders derived from sender_base,
// we specialize an empty sender_traits.
template<typename S>
struct _sender_traits_<S,
    std::enable_if_t<std::is_base_of_v<sender_base, S> && !_has_sender_traits_members_<S>>>
{
  protected:
  _sender_traits_() = default;
  _sender_traits_(const _sender_traits_&) = default;
  _sender_traits_(_sender_traits_&&) = default;
  _sender_traits_& operator=(const _sender_traits_&) = default;
  _sender_traits_& operator=(_sender_traits_&&) = default;
  ~_sender_traits_() = default;
};

// For senders which declare `value_types', `error_types', and `sends_done':
// specialize a sender_traits which forwards these types/values.
template<typename S>
struct _sender_traits_<S,
    std::enable_if_t<_has_sender_traits_members_<S>>>
{
  protected:
  _sender_traits_() = default;
  _sender_traits_(const _sender_traits_&) = default;
  _sender_traits_(_sender_traits_&&) = default;
  _sender_traits_& operator=(const _sender_traits_&) = default;
  _sender_traits_& operator=(_sender_traits_&&) = default;
  ~_sender_traits_() = default;

  public:
  // Define the value_types.
  // We use the ones exposed on the sender.
  //
  // Note: we deduplicate the types that'll be presented to the Variant.
  template<template<typename...> class Tuple, template<typename...> class Variant>
  using value_types = typename S::template value_types<remove_cvref_tuple_<Tuple>::template type, _deduplicate<Variant>::template type>;

  // Define the error_types.
  // We use the ones exposed on the sender.
  //
  // Note: we deduplicate the types that'll be presented to the Variant.
  template<template<typename...> class Variant>
  using error_types = typename S::template error_types<remove_cvref_tuple_<_deduplicate<Variant>::template type>::template type>;

  // Declare if this sender can send a done-operation.
  // We expose the value on the sender.
  static inline constexpr bool sends_done = S::sends_done;
};


struct operation_state_base_ {
  protected:
  operation_state_base_() = default;
  operation_state_base_(const operation_state_base_&) = delete;
  operation_state_base_& operator=(const operation_state_base_&) = delete;
  operation_state_base_& operator=(operation_state_base_&&) = delete;
  ~operation_state_base_() = default;

#if __cpp_guaranteed_copy_elision >= 201606L && \
    !(!defined(__clang__) && defined(__GNUC__)) // GCC requires a move-constructor. Don't know why.
  operation_state_base_(operation_state_base_&&) noexcept = delete;
#else
  // We only allow copy-elision.
  // But in order to permit copy-elision, we must pretend to have a move-constructor.
  // But we don't actually want to allow move-construction, so we ensure moves will abort.
  operation_state_base_(operation_state_base_&&) noexcept {
    std::abort();
  }
#endif
};


// Start an operation_state.
// An operation_state is an object that describes how to run an operation.
//
// Executes `tag_invoke(start_t{}, o)`
struct start_t {
  template<typename O>
  auto operator()(O&& o) const
  noexcept
  -> tag_invoke_result_t<start_t, O> {
    static_assert(nothrow_tag_invocable<start_t, O>, "start should be a no-except invocation");
    static_assert(std::is_base_of_v<operation_state_base_, std::remove_cvref_t<O>>); // XXX remove later
    return tag_invoke(*this, std::forward<O>(o));
  }
};
inline constexpr start_t start{};


// Create a sender that will run on a specific execution-context.
//
// Executes `tag_invoke(schedule_t{}, sch)`
struct schedule_t {
  template<typename Scheduler>
  auto operator()(Scheduler&& sch) const
  -> tag_invoke_result_t<schedule_t, Scheduler> {
    return tag_invoke(*this, std::forward<Scheduler>(sch));
  }

  // XXX extension
  template<typename Scheduler>
  requires (!tag_invocable<schedule_t, const Scheduler&>)
  auto operator()(const Scheduler& sch) const
  -> tag_invoke_result_t<schedule_t, std::remove_cvref_t<Scheduler>> {
    return (*this)(std::remove_cvref_t<Scheduler>(sch));
  }
};
inline constexpr schedule_t schedule{};


// Forward-progress-guarantees offered by schedulers.
// See also https://en.cppreference.com/w/cpp/language/memory_model#Progress_guarantee
enum class forward_progress_guarantee {
  concurrent,
  parallel,
  weakly_parallel
};


// Assign a value to the receiver.
//
// This function may fail, in which case you must either call the `set_error` or the `set_done` operations.
struct set_value_t {
  template<typename R, typename... Args>
  auto operator()(R&& r, Args&&... args) const -> void {
    tag_invoke(*this, std::forward<R>(r), std::forward<Args>(args)...);
  }
};
inline constexpr set_value_t set_value{};


// Assign an error to the receiver.
struct set_error_t {
  template<typename R, typename E>
  auto operator()(R&& r, E&& e) const noexcept -> void {
    static_assert(nothrow_tag_invocable<set_error_t, R, E>, "execution::set_error should be noexcept");
    tag_invoke(*this, std::forward<R>(r), std::forward<E>(e));
  }
};
inline constexpr set_error_t set_error{};


// Assign the done-state to the receiver.
//
// There are no arguments. `set_done` is used to signal that the work is no longer needed, and was canceled.
struct set_done_t {
  template<typename R>
  auto operator()(R&& r) const noexcept -> void {
    static_assert(nothrow_tag_invocable<set_done_t, R>, "execution::set_error should be noexcept");
    tag_invoke(*this, std::forward<R>(r));
  }
};
inline constexpr set_done_t set_done{};


// Constraints on a receiver.
template<typename T, typename E = std::exception_ptr>
concept receiver =
    std::move_constructible<std::remove_cvref_t<T>> &&
    std::constructible_from<std::remove_cvref_t<T>, T> &&
    requires(std::remove_cvref_t<T>&& t, E&& e) {
      { execution::set_done(std::move(t)) } noexcept;
      { execution::set_error(std::move(t), std::forward<E>(e)) } noexcept;
    };
// Constraints on a typed-receiver.
template<typename T, typename... A>
concept receiver_of =
    receiver<T> &&
    requires(std::remove_cvref_t<T>&& t, A&&... a) {
      { execution::set_value(std::move(t), std::forward<A>(a)...) };
    };
// A sender must have a sender_traits.
template<typename T>
concept sender =
    std::move_constructible<std::remove_cvref_t<T>> &&
    // A sender must have a sender_traits.
    !requires {
      typename sender_traits<std::remove_cvref_t<T>>::__unspecialized; // exposition only
    };
// A typed-sender, is a sender, for which the sender_traits are all populated.
template<typename T>
concept typed_sender = sender<T> && _has_sender_traits_members_<std::remove_cvref_t<T>>;

// An operation-state is something that can be started.
template<typename O>
concept operation_state =
    std::destructible<O> &&
    std::is_object_v<O> &&
    requires (O& o) {
      { ::earnest::execution::start(o) } noexcept;
    };

// The design talks about execution-contexts.
// An execution-context is a thing that'll run tasks.
// But execution-contexts cannot always be expressed in code.
//
// So the `scheduler` exists, as a thing that can take your task
// and have it be executed by the execution-context.
template<typename Scheduler>
concept scheduler =
    std::copy_constructible<std::remove_cvref_t<Scheduler>> &&
    std::equality_comparable<std::remove_cvref_t<Scheduler>> &&
    requires (Scheduler&& sch) {
      { ::earnest::execution::schedule((Scheduler&&)(sch)) };
    };


// Query the forward-progress-guarantee of a scheduler.
struct get_forward_progress_guarantee_t {
  template<scheduler Scheduler>
  auto operator()(const Scheduler& sch) const
  noexcept
  -> forward_progress_guarantee {
    if constexpr(tag_invocable<get_forward_progress_guarantee_t, const Scheduler&>) {
      static_assert(nothrow_tag_invocable<get_forward_progress_guarantee_t, const Scheduler&>,
          "get_forward_progress_guarantee must be a noexcept function");
      return tag_invoke(*this, sch);
    } else {
      return forward_progress_guarantee::weakly_parallel;
    }
  }
};
inline constexpr get_forward_progress_guarantee_t get_forward_progress_guarantee{};


// Check if a scheduler execute-operation may block the caller.
//
// Unless a specialization was implemented, this will return true.
struct execute_may_block_caller_t {
  template<scheduler Scheduler>
  constexpr auto operator()(const Scheduler& sch) noexcept -> bool {
    if constexpr(tag_invocable<execute_may_block_caller_t, const Scheduler&>) {
      static_assert(std::same_as<bool, tag_invoke_result_t<execute_may_block_caller_t, const Scheduler&>>,
          "execute_may_block_caller must return a bool");
      static_assert(nothrow_tag_invocable<execute_may_block_caller_t, const Scheduler&>,
          "execute_may_block_caller must be a noexcept function");
      return tag_invoke(*this, sch);
    } else {
      return true;
    }
  }
};
inline constexpr execute_may_block_caller_t execute_may_block_caller{};


// Return the completion scheduler that a sender will use for a given signal.
// Signal must be one of set_value_t, set_error_t, or set_done_t.
//
// Senders don't have to answer.
// But if they do, it's a guarantee that they'll invoke the corresponding signal,
// on the scheduler they advertised.
template<typename Signal>
struct get_completion_scheduler_t {
  template<sender S>
  constexpr auto operator()(const S& s) const
  noexcept
  -> tag_invoke_result_t<get_completion_scheduler_t, const S&> {
    static_assert(scheduler<tag_invoke_result_t<get_completion_scheduler_t, const S&>>,
        "get_completion_scheduler must return a scheduler");
    static_assert(nothrow_tag_invocable<get_completion_scheduler_t, const S&>,
        "get_completion_scheduler must be a noexcept function");
    return tag_invoke(*this, s);
  }
};
template<typename Signal>
inline constexpr get_completion_scheduler_t<Signal> get_completion_scheduler{};


// Return the scheduler for a given receiver.
struct get_scheduler_t {
  template<receiver R>
  constexpr auto operator()(const R& r) const
  noexcept
  -> tag_invoke_result_t<get_scheduler_t, const R&> {
    static_assert(scheduler<tag_invoke_result_t<get_scheduler_t, const R&>>,
        "get_scheduler must return a scheduler");
    static_assert(nothrow_tag_invocable<get_scheduler_t, const R&>,
        "get_scheduler must be a noexcept function");
    return tag_invoke(*this, r);
  }
};
inline constexpr get_scheduler_t get_scheduler{};


// Return the associated allocator for a receiver.
struct get_allocator_t {
  template<receiver R>
  constexpr auto operator()(const R& r) const
  noexcept
  -> tag_invoke_result_t<get_allocator_t, const R&> {
    // XXX I can't find an allocator concept. I guess it doesn't exist.
    static_assert(nothrow_tag_invocable<get_allocator_t, const R&>,
        "get_allocator must be a noexcept function");
    return tag_invoke(*this, r);
  }
};
inline constexpr get_allocator_t get_allocator{};


// All over the document, there are tag-invoke invocations that must the same pattern:
// - tag_invoke(tag, get_completion_scheduler<Signal>(s), s, args...)  <-- if implemented
// - tag_invoke(tag, s, args...)  <-- if implemented, and if the above isn't implemented
// - some default implementation  <-- if neither of the others are implemented
//
// Because that is a lot of repetition, we use a bit of code to do it for us.
//
// `_generic_operand_base_t<Signal>` is a callable, which will do these steps for us.
// If none of the tag_invoke calls work, it'll invoke a default implementation (if one exists).
//
// The default implementation is always:
// `tag.default_impl(s, args...)`
// If there is not default-implementation, it'll make sure the compiler complains.
template<typename Signal>
struct _generic_operand_base_t {
  private:
  // Confirm if a tag_invoke exists for a given implementation.
  //
  // Usage: `invocable_with_specific_scheduler<void(tag, sender, args...), scheduler>`
  // Will derive from std::true_type if the invocation exists, or std::false_type if not.
  // Will also declare an `noexcept_` contexpr, indicating if this is a no-except invocation.
  template<typename, typename, typename = void>
  struct invocable_with_specific_scheduler_ : std::false_type {
    static constexpr bool noexcept_ = true;
  };
  // Specialization of `invocable_with_specific_scheduler`
  // for those cases where the tag-invoke exists.
  template<typename Tag, typename S, typename... Args, typename CompletionScheduler>
  struct invocable_with_specific_scheduler_<
      void(Tag, S, Args...), CompletionScheduler,
      std::void_t<decltype(tag_invoke(std::declval<const Tag&>(), std::declval<CompletionScheduler>(), std::declval<S>(), std::declval<Args>()...))>>
  : std::negation<std::is_void<Signal>> {
    static constexpr bool noexcept_ =
        std::is_void_v<Signal> ||
        (noexcept(get_completion_scheduler<Signal>(std::declval<S>())) &&
         noexcept(tag_invoke(std::declval<const Tag&>(), std::declval<CompletionScheduler>(), std::declval<S>(), std::declval<Args>()...)));
  };

  // Confirm if a tag_invoke exists for a given implementation.
  //
  // Usage: `invocable_with_scheduler<void(tag, sender, args...)>`
  // This is implemented in terms of `invocable_with_specific_scheduler`.
  // The reason for having two steps, is that if `get_completion_scheduler<Status>` isn't implemented,
  // we would get a compilation failure if we did it all in one step.
  template<typename, typename = void>
  struct invocable_with_scheduler_ : std::false_type {
    static constexpr bool noexcept_ = true;
  };
  // Specialization for `invocable_with_scheduler`
  // for those cases where `get_completion_scheduler<Status>(s)` exists.
  //
  // Delegates to `invocable_with_specific_scheduler`, which checks the actual invocation.
  template<typename Tag, typename S, typename... Args>
  struct invocable_with_scheduler_<
      void(Tag, S, Args...),
      std::void_t<decltype(get_completion_scheduler<Signal>(std::declval<S>()))>>
  : invocable_with_specific_scheduler_<void(Tag, S, Args...), decltype(get_completion_scheduler<Signal>(std::declval<S>()))>
  {};

  // Confirm if a tag_invoke (without scheduler) exists for a given implementation.
  //
  // Usage: `invocable_without_scheduler<void(tag, sender, args...)>`
  template<typename, typename = void> struct invocable_without_scheduler_ : std::false_type {
    static constexpr bool noexcept_ = true;
  };
  // Specialization of `invocable_without_scheduler`
  // for those cases where `tag_invoke(tag, sender, args...)` exists.
  template<typename Tag, typename S, typename... Args>
  struct invocable_without_scheduler_<void(Tag, S, Args...), std::void_t<decltype(tag_invoke(std::declval<const Tag&>(), std::declval<S>(), std::declval<Args>()...))>>
  : std::true_type {
    static constexpr bool noexcept_ = noexcept(tag_invoke(std::declval<const Tag&>(), std::declval<S>(), std::declval<Args>()...));
  };

  // Invoke the default implementation.
  //
  // We use a function on the `_generic_operand_base_t`, so that tags can make their `default_impl` private,
  // and use a friend-declaration to allow `_generic_operand_base_t` to invoke it.
  template<typename Tag, typename... Args>
  static constexpr auto do_default_impl(const Tag& tag, Args&&... args)
  noexcept(noexcept(std::declval<const Tag&>().default_impl(std::declval<Args>()...)))
  -> decltype(std::declval<const Tag&>().default_impl(std::declval<Args>()...)) {
    return tag.default_impl(std::forward<Args>(args)...);
  }

  // Confirm if a default implementation exists.
  template<typename, typename = void> struct has_default_impl_ : std::false_type {
    static constexpr bool noexcept_ = true;
  };
  // Specialization of `has_default_impl_`, for those cases where a default implementation exists.
  template<typename Tag, typename... Args>
  struct has_default_impl_<void(Tag, Args...), std::void_t<decltype(do_default_impl(std::declval<Tag>(), std::declval<Args>()...))>>
  : std::true_type {
    static constexpr bool noexcept_ = noexcept(do_default_impl(std::declval<Tag>(), std::declval<Args>()...));
  };

  // This is always true, and provides a no-except value.
  // Used so that the no-except clause won't error out if there are no invocable constraints met.
  struct not_invocable_
  : std::true_type
  {
    static constexpr bool noexcept_ = true;
  };

  public:
  // Implementation of `_generic_operand_base_t`.
  //
  // It ensures that one of the implementations exists.
  // If none exist, the function will be hidden due to SFINAE.
  template<typename Tag, sender S, typename... Args>
  requires(
      invocable_with_scheduler_<void(Tag, S, Args...)>::value ||
      invocable_without_scheduler_<void(Tag, S, Args...)>::value ||
      has_default_impl_<void(Tag, S, Args...)>::value)
  constexpr auto operator()(const Tag& t, S&& s, Args&&... args) const
  noexcept( // Each of the checks has a `noexcept_` constant, that says if the invocation is noexcept.
            // We use disjunction to select the first of the checks that passes, and use its `noexcept_`.
      std::disjunction<
          invocable_with_scheduler_<void(Tag, S, Args...)>,
          invocable_without_scheduler_<void(Tag, S, Args...)>,
          has_default_impl_<void(Tag, S, Args...)>,
          not_invocable_
      >::noexcept_)
  -> decltype(auto) {
    // I wanted to write `else { static_assert(false) }`,
    // but the compiler evaluates that static_assert even if the branch isn't compiled.
    // So instead we use a static_assert to very much ensure that you must hit one of the branches.
    static_assert(
        invocable_with_scheduler_<void(Tag, S, Args...)>::value ||
        invocable_without_scheduler_<void(Tag, S, Args...)>::value ||
        has_default_impl_<void(Tag, S, Args...)>::value);

    if constexpr(invocable_with_scheduler_<void(Tag, S, Args...)>::value) {
      return tag_invoke(t, get_completion_scheduler<Signal>(s), std::forward<S>(s), std::forward<Args>(args)...);
    } else if constexpr(invocable_without_scheduler_<void(Tag, S, Args...)>::value) {
      return tag_invoke(t, std::forward<S>(s), std::forward<Args>(args)...);
    } else if constexpr(has_default_impl_<void(Tag, S, Args...)>::value) {
      return do_default_impl(t, std::forward<S>(s), std::forward<Args>(args)...);
    }
  }
};
template<typename Signal = void>
inline constexpr _generic_operand_base_t<Signal> _generic_operand_base{};


// An adapter.
// It can be attached to a sender, creating a new sender.
// It can be attached to an adapter, creating a new adapter.
//
// The generic adapter simply tracks the tag and arguments.
// Invocations:
// - `my_adapter(some_sender)`
// - `some_sender | my_adapter`
// will both result in a call:
// `tag(some_sender, args...)`.
//
// Recommend you use `_generic_adapter` function (below) to create instances of this adapter.
template<typename Tag, typename... Args>
class _generic_adapter_t
: public sender_base // All adapters are both receivers and senders, but with unknown sender_traits.
{
  public:
  template<typename... Args_>
  explicit constexpr _generic_adapter_t(const Tag& tag, Args_&&... args)
  : tag(tag),
    args(std::forward<Args_>(args)...)
  {}

  private:
  template<sender Sender, std::size_t... Idx>
  requires std::invocable<const Tag&, Sender, std::add_lvalue_reference_t<Args>...>
  constexpr auto invoke(Sender&& s, [[maybe_unused]] std::index_sequence<Idx...>) &
  -> std::invoke_result_t<const Tag&, Sender, std::add_lvalue_reference_t<Args>...> {
    return std::invoke(std::as_const(tag), std::forward<Sender>(s), std::get<Idx>(args)...);
  }

  template<sender Sender, std::size_t... Idx>
  requires std::invocable<const Tag&, Sender, Args...>
  constexpr auto invoke(Sender&& s, [[maybe_unused]] std::index_sequence<Idx...>) &&
  -> std::invoke_result_t<const Tag&, Sender, Args...> {
    return std::invoke(std::as_const(tag), std::forward<Sender>(s), std::get<Idx>(std::move(args))...);
  }

  public:
  template<sender Sender>
  constexpr auto operator()(Sender&& s) &
  -> decltype(std::declval<_generic_adapter_t&>().invoke(std::declval<Sender>(), std::index_sequence_for<Args...>())) {
    return invoke(std::forward<Sender>(s), std::index_sequence_for<Args...>());
  }

  template<sender Sender>
  constexpr auto operator()(Sender&& s) &&
  -> decltype(std::declval<_generic_adapter_t>().invoke(std::declval<Sender>(), std::index_sequence_for<Args...>())) {
    return std::move(*this).invoke(std::forward<Sender>(s), std::index_sequence_for<Args...>());
  }

  private:
  [[no_unique_address]] Tag tag;
  [[no_unique_address]] std::tuple<Args...> args;
};

// Pipe operation on an adapter: `... | _const generic_adapter_t&`
template<sender Sender, typename Tag, typename... Args>
constexpr auto operator|(Sender&& s, const _generic_adapter_t<Tag, Args...>& adapter)
-> decltype(auto) {
  return adapter(std::forward<Sender>(s));
}

// Pipe operation on an adapter: `... | _generic_adapter_t&`
template<sender Sender, typename Tag, typename... Args>
constexpr auto operator|(Sender&& s, _generic_adapter_t<Tag, Args...>& adapter)
-> decltype(auto) {
  return adapter(std::forward<Sender>(s));
}

// Pipe operation on an adapter: `... | _generic_adapter_t&&`
template<sender Sender, typename Tag, typename... Args>
constexpr auto operator|(Sender&& s, _generic_adapter_t<Tag, Args...>&& adapter)
-> decltype(std::declval<_generic_adapter_t<Tag, Args...>>()(std::declval<Sender>())) {
  return std::move(adapter)(std::forward<Sender>(s));
}

// Create a _generic_adapter_t.
template<typename Tag, typename... Args>
constexpr auto _generic_adapter(const Tag& tag, Args&&... args)
-> _generic_adapter_t<Tag, std::remove_cvref_t<Args>...> {
  return _generic_adapter_t<Tag, std::remove_cvref_t<Args>...>(tag, std::forward<Args>(args)...);
}


// Get stop token returns a stop-token for a receiver.
struct get_stop_token_t {
  template<receiver R>
  constexpr auto operator()(const R& r) const noexcept -> decltype(auto) {
    if constexpr(!tag_invocable<get_stop_token_t, const R&>) {
      return never_stop_token{};
    } else {
      static_assert(stoppable_token<decltype(_generic_operand_base<>(*this, r))>,
          "get_stop_token must return a stoppable token");
      static_assert(noexcept(_generic_operand_base<>(*this, r)),
          "get_stop_token must be a noexcept function");
      return _generic_operand_base<>(*this, r);
    }
  }
};
inline constexpr get_stop_token_t get_stop_token{};


// Connect a sender and a receiver.
//
// Returns an operation-state, which can be started to execute the whole chain of operations.
struct connect_t {
  private:
  template<typename... Error>
  struct accepts_errors {
    template<receiver R>
    static inline constexpr bool valid = (receiver<R, Error> &&...);
  };

  template<typename... ArgTuples>
  struct accepts_values;
  template<typename... Args, typename... ArgTuples>
  struct accepts_values<std::tuple<Args...>, ArgTuples...> {
    template<receiver R>
    static inline constexpr bool valid = receiver_of<R, Args...> && accepts_values<ArgTuples...>::template valid<R>;
  };

  public:
  template<typed_sender S, receiver R>
  requires (
      sender_traits<std::remove_cvref_t<S>>::template error_types<accepts_errors>::template valid<R> &&
      sender_traits<std::remove_cvref_t<S>>::template value_types<std::tuple, accepts_values>::template valid<R>
  )
  constexpr auto operator()(S&& s, R&& r) const
  -> operation_state decltype(auto) {
    return _generic_operand_base<>(*this, std::forward<S>(s), std::forward<R>(r));
  }
};
inline constexpr connect_t connect{};

template<>
struct connect_t::accepts_values<> {
  template<receiver R>
  static inline constexpr bool valid = true;
};


// We block the move-operator for operation-state, relying on only copy-elision.
//
// We cannot use std::optional, because it requires a move-constructor for its emplace operation to work.
// So instead, we'll use a dedicated optional.
template<operation_state State>
class optional_operation_state_ {
  static_assert(!std::is_reference_v<State>);
  static_assert(!std::is_const_v<State>);
  static_assert(sizeof(State) != 0, "operation-state must be a complete type");

  public:
  using value_type = State;

  optional_operation_state_() = default;

  optional_operation_state_([[maybe_unused]] std::nullopt_t) noexcept
  {}

  template<typed_sender Sender, receiver Receiver>
  explicit optional_operation_state_(Sender&& sender, Receiver&& receiver)
  : optional_operation_state_()
  {
    emplace(std::forward<Sender>(sender), std::forward<Receiver>(receiver));
  }

  ~optional_operation_state_() {
    reset();
  }

  // No copy/move operations: operation-state is not allowed to be moved (nor copied).
  optional_operation_state_(const optional_operation_state_&) = delete;
  optional_operation_state_(optional_operation_state_&&) = delete;
  optional_operation_state_& operator=(const optional_operation_state_&) = delete;
  optional_operation_state_& operator=(optional_operation_state_&&) = delete;

  auto has_value() const noexcept -> bool {
    return engaged_;
  }

  explicit operator bool() const noexcept {
    return engaged_;
  }

  auto value() & -> value_type& {
    if (!has_value()) throw std::bad_optional_access();
    return *reinterpret_cast<value_type*>(&state_);
  }

  auto value() && -> value_type&& {
    if (!has_value()) throw std::bad_optional_access();
    return std::move(*reinterpret_cast<value_type*>(&state_));
  }

  auto value() const & -> const value_type& {
    if (!has_value()) throw std::bad_optional_access();
    return *reinterpret_cast<const value_type*>(&state_);
  }

  auto operator*() & -> value_type& {
    assert(has_value());
    return *reinterpret_cast<value_type*>(&state_);
  }

  auto operator*() && -> value_type&& {
    assert(has_value());
    return std::move(*reinterpret_cast<value_type*>(&state_));
  }

  auto operator*() const & -> const value_type& {
    assert(has_value());
    return *reinterpret_cast<const value_type*>(&state_);
  }

  auto operator->() -> value_type* {
    assert(has_value());
    return reinterpret_cast<value_type*>(&state_);
  }

  auto operator->() const -> const value_type* {
    assert(has_value());
    return reinterpret_cast<const value_type*>(&state_);
  }

  auto reset() noexcept -> void {
    if (engaged_) {
      reinterpret_cast<value_type*>(&state_)->~value_type();
      engaged_ = false;
    }
  }

  template<typed_sender Sender, receiver Receiver>
  auto emplace(Sender&& sender, Receiver&& receiver) -> value_type& {
    static_assert(sizeof(state_) >= sizeof(value_type));
    assert(!engaged_);

    ::new(static_cast<void*>(reinterpret_cast<value_type*>(&state_))) value_type(
        execution::connect(std::forward<Sender>(sender), std::forward<Receiver>(receiver)));
    engaged_ = true;
    return *reinterpret_cast<value_type*>(&state_);
  }

  private:
  std::aligned_storage_t<sizeof(value_type), alignof(value_type)> state_;
  bool engaged_ = false;
};


// A sender-to checks if a sender (S) can be attached to a specific receiver (R).
template<typename S, typename R>
concept sender_to =
    sender<S> &&
    receiver<R> &&
    requires(S&& s, R&& r) {
      execution::connect(std::move(s), std::move(r));
    };

// Check if a sender produces the given value-type.
template<typename S, typename... T>
concept sender_of =
    typed_sender<S> &&
    std::same_as<
        std::tuple<T...>,
        typename sender_traits<S>::template value_types<std::tuple, std::type_identity_t>>;


// Check if something is a forwardable receiver operation.
template<typename Tag>
constexpr bool _is_forwardable_receiver_tag =
    std::disjunction_v<
        std::is_same<Tag, set_value_t>,
        std::is_same<Tag, set_error_t>,
        std::is_same<Tag, set_done_t>,
        std::is_same<Tag, get_scheduler_t>,
        std::is_same<Tag, get_allocator_t>,
        std::is_same<Tag, get_stop_token_t>>;

// Check if something is a forwardable sender operation.
template<typename Tag>
constexpr bool _is_forwardable_sender_tag =
    std::disjunction_v<
        std::is_same<Tag, connect_t>, // XXX maybe remove this, because this is never forwardable
        std::is_same<Tag, get_completion_scheduler_t<set_value_t>>,
        std::is_same<Tag, get_completion_scheduler_t<set_error_t>>,
        std::is_same<Tag, get_completion_scheduler_t<set_done_t>>>;


// Receiver wrapper.
//
// We use these as the base class for receivers.
// It implements forwarding for all tag-invoke operations, except for the SpecializedOperations.
//
// Intended to be used as a base class:
// ```
// class my_special_receiver
// : public _generic_receiver_wrapper<SomeOtherReceiver, set_value_t>
// {
//   ...
// };
// ```
// This would create a `my_special_receiver`, for which all operations are forwarded to SomeOtherReceiver.
// Except the `set_value_t` operation, which you would implement in `my_special_receiver`.
//
// The wrapper receiver is exposed as a protected-member-variable `r`.
template<receiver Receiver, typename... SpecializedOperations>
class _generic_receiver_wrapper {
  protected:
  // XXX delete this: receivers are always move-constructed, never copied
  constexpr _generic_receiver_wrapper(const Receiver& r)
  : r(r)
  {}

  constexpr _generic_receiver_wrapper(Receiver&& r)
  : r(std::move(r))
  {}

  _generic_receiver_wrapper(const _generic_receiver_wrapper&) = default;
  _generic_receiver_wrapper(_generic_receiver_wrapper&&) = default;
  _generic_receiver_wrapper& operator=(const _generic_receiver_wrapper&) = default;
  _generic_receiver_wrapper& operator=(_generic_receiver_wrapper&&) = default;

  ~_generic_receiver_wrapper() = default;

  public:
  // Forward all non-specialized operations to receiver.
  // This function specializes for `const _generic_receiver_wrapper&'.
  template<typename Tag, typename... Args,
      typename = std::enable_if_t<_is_forwardable_receiver_tag<Tag>>,
      typename = std::enable_if_t<std::conjunction_v<std::negation<std::is_same<SpecializedOperations, Tag>>...>>>
  friend constexpr auto tag_invoke(Tag tag, const _generic_receiver_wrapper& self, Args&&... args)
  noexcept(nothrow_tag_invocable<Tag, const Receiver&, Args...>)
  -> tag_invoke_result_t<Tag, const Receiver&, Args...> {
    return tag_invoke(std::move(tag), self.r, std::forward<Args>(args)...);
  }

  // Forward all non-specialized operations to receiver.
  // This function specializes for `_generic_receiver_wrapper&'.
  template<typename Tag, typename... Args,
      typename = std::enable_if_t<_is_forwardable_receiver_tag<Tag>>,
      typename = std::enable_if_t<std::conjunction_v<std::negation<std::is_same<SpecializedOperations, Tag>>...>>>
  friend constexpr auto tag_invoke(Tag tag, _generic_receiver_wrapper& self, Args&&... args)
  noexcept(nothrow_tag_invocable<Tag, Receiver&, Args...>)
  -> tag_invoke_result_t<Tag, Receiver&, Args...> {
    return tag_invoke(std::move(tag), self.r, std::forward<Args>(args)...);
  }

  // Forward all non-specialized operations to receiver.
  // This function specializes for `_generic_receiver_wrapper&&'.
  template<typename Tag, typename... Args,
      typename = std::enable_if_t<_is_forwardable_receiver_tag<Tag>>,
      typename = std::enable_if_t<std::conjunction_v<std::negation<std::is_same<SpecializedOperations, Tag>>...>>>
  friend constexpr auto tag_invoke(Tag tag, _generic_receiver_wrapper&& self, Args&&... args)
  noexcept(nothrow_tag_invocable<Tag, Receiver&&, Args...>)
  -> tag_invoke_result_t<Tag, Receiver&&, Args...> {
    return tag_invoke(std::move(tag), std::move(self.r), std::forward<Args>(args)...);
  }

  protected:
  [[no_unique_address]] Receiver r;
};


// Wrap a receiver via a pointer.
//
// This `_generic_rawptr_receiver' does not own the wrapped receiver.
// It is the responsibility of the calling code to ensure the lifetime
// of the wrapped receiver is sufficient.
//
// This class is noexcept copyable and moveable.
template<receiver Receiver>
class _generic_rawptr_receiver {
  public:
  // Create a raw-pointer receiver wrapper.
  // All tag-invoke calls forward to the wrapped receiver.
  explicit _generic_rawptr_receiver(Receiver* r_ptr) noexcept
  : r_ptr(r_ptr)
  {}

  // Forward all tag-invoke's to the wrapped receiver.
  // This implementation is for const-reference.
  template<typename Tag, typename... Args>
  friend auto tag_invoke([[maybe_unused]] Tag tag, const _generic_rawptr_receiver& self, Args&&... args)
  noexcept(noexcept(tag_invoke(std::declval<Tag>(), std::declval<const Receiver&>(), std::declval<Args>()...)))
  -> decltype(tag_invoke(std::declval<Tag>(), std::declval<const Receiver&>(), std::declval<Args>()...)) {
    return tag_invoke(std::move(tag), std::as_const(*self.r_ptr), std::forward<Args>(args)...);
  }

  // Forward all tag-invoke's to the wrapped receiver.
  // This implementation is for non-const-reference.
  template<typename Tag, typename... Args>
  friend auto tag_invoke([[maybe_unused]] Tag tag, _generic_rawptr_receiver& self, Args&&... args)
  noexcept(noexcept(tag_invoke(std::declval<Tag>(), std::declval<Receiver&>(), std::declval<Args>()...)))
  -> decltype(tag_invoke(std::declval<Tag>(), std::declval<Receiver&>(), std::declval<Args>()...)) {
    return tag_invoke(std::move(tag), *self.r_ptr, std::forward<Args>(args)...);
  }

  // Forward all tag-invoke's to the wrapped receiver.
  // This implementation is for rvalue-reference.
  template<typename Tag, typename... Args>
  friend auto tag_invoke([[maybe_unused]] Tag tag, _generic_rawptr_receiver&& self, Args&&... args)
  noexcept(noexcept(tag_invoke(std::declval<Tag>(), std::declval<Receiver&&>(), std::declval<Args>()...)))
  -> decltype(tag_invoke(std::declval<Tag>(), std::declval<Receiver&&>(), std::declval<Args>()...)) {
    return tag_invoke(std::move(tag), std::move(*self.r_ptr), std::forward<Args>(args)...);
  }

  private:
  // The actual receiver we're wrapping.
  // We have no ownership, so we use a raw pointer.
  Receiver* r_ptr = nullptr;
};


// A generic sender wrapper.
// Similar to `_generic_receiver_wrapper`, this wraps a sender.
// It'll forward all tag-invokes to the wrapped sender, except for the operations in SpecializedOperations.
//
// Intended to be used as a base class:
// ```
// class my_special_sender
// : public _generic_receiver_wrapper<SomeOtherSender, get_completion_scheduler>
// {
//   ...
// };
// ```
// This would create a `my_special_sender`, for which all operations are forwarded to SomeOtherSender.
// Except the `get_completion_scheduler` operation, which you would implement in `my_special_sender`.
//
// The `value_types`, `error_types`, and `sends_done` attributes from the wrapped-sender will also be exposed.
// If those need a specialization, define them in your derived class (which'll hide the definition in the parent class).
//
// The wrapper sender is exposed as a protected-member-variable `s`.
template<typename Derived, sender Sender, typename... SpecializedOperations>
class _generic_sender_wrapper
: public sender_traits<Sender>, // Publish value_types, error_types, and sends_done. Overriding type is supposed to specialize the ones that need changing.
  public sender_base // Mark this as a sender, if the sender-traits don't have the required types/constants published.
{
  protected:
  constexpr _generic_sender_wrapper(const Sender& s)
  noexcept(std::is_nothrow_copy_constructible_v<Sender>)
  : s(s)
  {}

  constexpr _generic_sender_wrapper(Sender&& s)
  noexcept(std::is_nothrow_move_constructible_v<Sender>)
  : s(std::move(s))
  {}

  _generic_sender_wrapper(const _generic_sender_wrapper&) = default;
  _generic_sender_wrapper(_generic_sender_wrapper&&) = default;
  _generic_sender_wrapper& operator=(const _generic_sender_wrapper&) = default;
  _generic_sender_wrapper& operator=(_generic_sender_wrapper&&) = default;

  ~_generic_sender_wrapper() = default;

  public:
  // Forward all non-specialized operations to sender.
  // This function specializes for `const _generic_sender_wrapper&'.
  template<typename Tag, typename... Args,
      typename = std::enable_if_t<_is_forwardable_sender_tag<Tag>>,
      typename = std::enable_if_t<std::conjunction_v<std::negation<std::is_same<SpecializedOperations, Tag>>...>>>
  friend constexpr auto tag_invoke(Tag tag, const _generic_sender_wrapper& self, Args&&... args)
  noexcept(nothrow_tag_invocable<Tag, const Sender&, Args...>)
  -> tag_invoke_result_t<Tag, const Sender&, Args...> {
    return tag_invoke(std::move(tag), self.s, std::forward<Args>(args)...);
  }

  // Forward all non-specialized operations to sender.
  // This function specializes for `_generic_sender_wrapper&'.
  template<typename Tag, typename... Args,
      typename = std::enable_if_t<_is_forwardable_sender_tag<Tag>>,
      typename = std::enable_if_t<std::conjunction_v<std::negation<std::is_same<SpecializedOperations, Tag>>...>>>
  friend constexpr auto tag_invoke(Tag tag, _generic_sender_wrapper& self, Args&&... args)
  noexcept(nothrow_tag_invocable<Tag, Sender&, Args...>)
  -> tag_invoke_result_t<Tag, Sender&, Args...> {
    return tag_invoke(std::move(tag), self.s, std::forward<Args>(args)...);
  }

  // Forward all non-specialized operations to sender.
  // This function specializes for `_generic_sender_wrapper&&'.
  template<typename Tag, typename... Args,
      typename = std::enable_if_t<_is_forwardable_sender_tag<Tag>>,
      typename = std::enable_if_t<std::conjunction_v<std::negation<std::is_same<SpecializedOperations, Tag>>...>>>
  friend constexpr auto tag_invoke(Tag tag, _generic_sender_wrapper&& self, Args&&... args)
  noexcept(nothrow_tag_invocable<Tag, Sender&&, Args...>)
  -> tag_invoke_result_t<Tag, Sender&&, Args...> {
    return tag_invoke(std::move(tag), std::move(self.s), std::forward<Args>(args)...);
  }

  // Pipe-completion implemenation.
  // This version is run by moving the adapter-chain.
  // It only works if the wrapped sender can appear on the right-hand-side of a pipe.
  //
  // This is implemented as operator(), because the operator| must always invoke
  // the functor-call operator.
  //
  // It relies on the derived type having implemented a `rebind' function,
  // which takes a new sender, and associates itself with that.
  template<sender PipeSource,
      typename = std::enable_if_t<std::is_invocable_v<Sender, PipeSource>>>
  auto operator()(PipeSource&& pipe_source) &&
  -> decltype(auto) {
    return std::move(static_cast<Derived&>(*this)).rebind(
        std::invoke(std::move(this->s), std::forward<PipeSource>(pipe_source)));
  }

  protected:
  [[no_unique_address]] Sender s;
};

// Allow a generic-sender to bind to a sender,
// if the generic-sender holds an adapter-chain.
template<sender PipeSource, typename Derived, sender Sender, typename... SpecializedOperations>
auto operator|(PipeSource&& pipe_source, _generic_sender_wrapper<Derived, Sender, SpecializedOperations...>&& wrapper)
-> decltype(auto) {
  return std::invoke(std::move(wrapper), std::forward<PipeSource>(pipe_source));
}

// Allow a generic-sender to bind to a sender,
// if the generic-sender holds an adapter-chain.
template<sender PipeSource, typename Derived, sender Sender, typename... SpecializedOperations>
auto operator|(PipeSource&& pipe_source, const _generic_sender_wrapper<Derived, Sender, SpecializedOperations...>& wrapper)
-> decltype(auto) {
  return std::invoke(std::move(wrapper), std::forward<PipeSource>(pipe_source));
}


// Create a then-continuation.
//
// A then-continuation takes the value-signal, and transforms it using a function.
// The transformed value is sent further along, to the next receiver.
//
// Lazy operations will never run before `::earnest::execution::start` has been called.
struct lazy_then_t {
  template<typename> friend struct _generic_operand_base_t;

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
  // The receiver used by the then-continuation.
  //
  // It forwards everything to the wrapped receiver, except for the `set_value_t` signal.
  // When `set_value` is called, it'll take the values, transform them using the function,
  // and then forward those values to the wrapped receiver.
  template<typename Fn, receiver Receiver>
  struct wrapped_receiver
  : public _generic_receiver_wrapper<Receiver, set_value_t>
  {
    // XXX remove all but one of those
    wrapped_receiver(const Fn& fn, const Receiver& r)
    : _generic_receiver_wrapper<Receiver, set_value_t>(r),
      fn(fn)
    {}

    wrapped_receiver(Fn&& fn, const Receiver& r)
    : _generic_receiver_wrapper<Receiver, set_value_t>(r),
      fn(std::move(fn))
    {}

    wrapped_receiver(const Fn& fn, Receiver&& r)
    : _generic_receiver_wrapper<Receiver, set_value_t>(std::move(r)),
      fn(fn)
    {}

    wrapped_receiver(Fn&& fn, Receiver&& r)
    : _generic_receiver_wrapper<Receiver, set_value_t>(std::move(r)),
      fn(std::move(fn))
    {}

    template<typename... Args>
    requires std::invocable<Fn, Args...>
    friend auto tag_invoke([[maybe_unused]] set_value_t, wrapped_receiver&& self, Args&&... args)
    -> decltype(auto) {
      if constexpr(std::is_void_v<std::invoke_result_t<Fn, Args...>>) {
        std::invoke(std::move(self.fn), std::forward<Args>(args)...);
        return ::earnest::execution::set_value(std::move(self.r));
      } else {
        return ::earnest::execution::set_value(std::move(self.r), std::invoke(std::move(self.fn), std::forward<Args>(args)...));
      }
    }

    private:
    [[no_unique_address]] Fn fn;
  };

  // Helper type: given a tuple-template, and a function return type, construct Tuple<FunctionReturnType>.
  // However, if the function returns void, then create Tuple<>.
  template<template<typename...> class Tuple, typename FnResultType>
  struct mk_tuple {
    using type = Tuple<FnResultType>;
  };

  template<template<typename...> class Tuple>
  struct mk_tuple<Tuple, void> {
    using type = Tuple<>;
  };

  template<typename Fn, template<typename...> class Tuple>
  struct fn_result_type_as_tuple {
    template<typename... Args>
    using type = typename mk_tuple<Tuple, decltype(std::invoke(std::declval<Fn>(), std::declval<Args>()...))>::type;
  };

  // Forward-declare impl, because we need it inside `sender_types_for_impl'.
  template<sender Sender, typename Fn> class sender_impl;

  // We need to update the sender-types.
  // But we can only do that, if the parent sender has sender types.
  //
  // Sadly, we can't selectively enable/disable having a local type-def.
  // So we have to wrap our base class as needed.
  template<sender Sender, typename Fn>
  struct sender_types_for_impl
  : _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t>
  {
    // Inherit all constructors.
    using _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t>::_generic_sender_wrapper;
  };

  // This specialization only applies for typed_senders.
  // Since we know the type, we can compute the transformed type.
  template<typed_sender Sender, typename Fn>
  struct sender_types_for_impl<Sender, Fn>
  : _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t>
  {
    // Define the value_types for this adapter.
    // They're computed from the parent sender.
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = typename sender_traits<Sender>::template value_types<fn_result_type_as_tuple<Fn, Tuple>::template type, Variant>;

    // Inherit all constructors.
    using _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t>::_generic_sender_wrapper;
  };

  // Implementation of a then-continuation.
  //
  // This has an associated sender, but has no final receiver yet.
  // Once a receiver is connected, it'll call connect.
  template<sender Sender, typename Fn>
  class sender_impl
  : public sender_types_for_impl<Sender, Fn>
  {
    template<typename, sender, typename...> friend class ::earnest::execution::_generic_sender_wrapper;

    public:
    template<typename Sender_, typename Fn_>
    requires std::constructible_from<Sender, Sender_> && std::constructible_from<Fn, Fn_>
    constexpr sender_impl(Sender_&& sender, Fn_&& fn)
    noexcept(std::is_nothrow_constructible_v<Fn, Fn_> && std::is_nothrow_constructible_v<Sender, Sender_>)
    : sender_types_for_impl<Sender, Fn>(std::forward<Sender_>(sender)),
      fn(std::forward<Fn_>(fn))
    {}

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> operation_state decltype(auto) {
      return ::earnest::execution::connect(
          std::move(self.s),
          wrapped_receiver<Fn, std::remove_cvref_t<Receiver>>(std::move(self.fn), std::forward<Receiver>(r)));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    -> sender_impl<std::remove_cvref_t<OtherSender>, Fn> {
      return sender_impl<std::remove_cvref_t<OtherSender>, Fn>(std::forward<OtherSender>(other_sender), std::move(fn));
    }

    [[no_unique_address]] Fn fn;
  };

  template<sender S, typename Fn>
  auto default_impl(S&& s, Fn&& fn) const
  -> sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Fn>> {
    return sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Fn>>(std::forward<S>(s), std::forward<Fn>(fn));
  }
};
inline constexpr lazy_then_t lazy_then{};


// Like lazy_then, but it's allowed to be eagerly evaluated.
//
// Unless specialized, this simply forwards to lazy_then.
struct then_t {
  template<typename> friend struct _generic_operand_base_t;

  template<sender S, typename Fn>
  constexpr auto operator()(S&& s, Fn&& fn) const
  -> sender decltype(auto)
  {
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
    return ::earnest::execution::lazy_then(std::forward<S>(s), std::forward<Fn>(fn));
  }
};
inline constexpr then_t then{};


// Create an upon-error-continuation.
//
// An upon-error-continuation takes the error-signal, and transforms it using a function.
// The transformed value is sent further along, to the next receiver.
// (Basically, this would allow you to implement error-recovery.)
//
// Lazy operations will never run before `::earnest::execution::start` has been called.
struct lazy_upon_error_t {
  template<typename> friend struct _generic_operand_base_t;

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
  // The receiver used by the then-continuation.
  //
  // It forwards everything to the wrapped receiver, except for the `set_value_t` signal.
  // When `set_value` is called, it'll take the values, transform them using the function,
  // and then forward those values to the wrapped receiver.
  template<typename Fn, receiver Receiver>
  struct wrapped_receiver
  : public _generic_receiver_wrapper<Receiver, set_value_t, set_error_t>
  {
    // XXX remove all but one of these constructors
    wrapped_receiver(const Fn& fn, const Receiver& r)
    : _generic_receiver_wrapper<Receiver, set_value_t, set_error_t>(r),
      fn(fn)
    {}

    wrapped_receiver(Fn&& fn, const Receiver& r)
    : _generic_receiver_wrapper<Receiver, set_value_t, set_error_t>(r),
      fn(std::move(fn))
    {}

    wrapped_receiver(const Fn& fn, Receiver&& r)
    : _generic_receiver_wrapper<Receiver, set_value_t, set_error_t>(std::move(r)),
      fn(fn)
    {}

    wrapped_receiver(Fn&& fn, Receiver&& r)
    : _generic_receiver_wrapper<Receiver, set_value_t, set_error_t>(std::move(r)),
      fn(std::move(fn))
    {}

    // Values are passed on.
    // We just want to ensure no exception escapes, otherwise that would be handled by our handler,
    // instead of a handler down the chain.
    template<typename... Args>
    friend auto tag_invoke([[maybe_unused]] set_value_t, wrapped_receiver&& self, Args&&... args) noexcept -> void {
      try {
        set_value(std::move(self.r), std::forward<Args>(args)...);
      } catch (...) {
        set_error(std::move(self.r), std::current_exception());
      }
    }

    // Errors are handled by the function, and its result is passed on as the value signal.
    template<typename Error>
    friend auto tag_invoke([[maybe_unused]] set_error_t, wrapped_receiver&& self, Error&& err) noexcept -> void {
      // If set_value or the function can throw, we must handle the exception:
      // because set_error must be a noexcept invocation.
      try {
        self.invoke_error(std::forward<Error>(err));
      } catch (...) {
        ::earnest::execution::set_error(std::move(self.r), std::current_exception());
      }
    }

    private:
    template<typename Error>
    auto invoke_error(Error&& err) -> void {
      if constexpr(std::is_void_v<std::invoke_result_t<Fn, Error>>) {
        std::invoke(std::move(fn), std::forward<Error>(err));
        ::earnest::execution::set_value(std::move(this->r));
      } else {
        ::earnest::execution::set_value(std::move(this->r), std::invoke(std::move(fn), std::forward<Error>(err)));
      }
    }

    [[no_unique_address]] Fn fn;
  };

  // Helper type: given a tuple-template, and a function return type, construct Tuple<FunctionReturnType>.
  // However, if the function returns void, then create Tuple<>.
  template<template<typename...> class Tuple, typename FnResultType>
  struct mk_tuple {
    using type = Tuple<FnResultType>;
  };

  template<template<typename...> class Tuple>
  struct mk_tuple<Tuple, void> {
    using type = Tuple<>;
  };

  template<typename Fn, template<typename...> class Tuple>
  struct fn_result_type_as_tuple {
    template<typename... Args>
    using type = typename mk_tuple<Tuple, decltype(std::invoke(std::declval<Fn>(), std::declval<Args>()...))>::type;
  };

  // Given a function, a tuple, adds error-types to the type-appender, by applying the function to them.
  template<typename Fn, template<typename...> class Tuple, typename TypeAppender>
  struct append_fn_result_types {
    // Create a new type-appender with the result of the function invocation.
    template<typename... ErrorType>
    using apply = typename TypeAppender::template append<typename fn_result_type_as_tuple<Fn, Tuple>::template type<ErrorType>...>;
  };

  // Forward-declare impl, because we need it inside `sender_types_for_impl'.
  template<sender Sender, typename Fn> class sender_impl;

  // We need to update the sender-types.
  // But we can only do that, if the parent sender has sender types.
  //
  // Sadly, we can't selectively enable/disable having a local type-def.
  // So we have to wrap our base class as needed.
  //
  // Since the wrapped sender could have different completion-schedulers for set_value and set_error,
  // we cannot advertise a completion-scheduler for the set_value signal.
  // We keep advertising the get_error completion-scheduler, since we made an exception for mismatch
  // during exception_ptr.
  template<sender Sender, typename Fn>
  struct sender_types_for_impl
  : _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t, get_completion_scheduler_t<set_value_t>>
  {
    // Inherit all constructors.
    using _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t, get_completion_scheduler_t<set_value_t>>::_generic_sender_wrapper;
  };

  // This specialization only applies for typed_senders.
  // Since we know the type, we can compute the transformed type.
  template<typed_sender Sender, typename Fn>
  struct sender_types_for_impl<Sender, Fn>
  : _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t, get_completion_scheduler_t<set_value_t>>
  {
    private:
    // Transform the existing value types from Sender, into something where we can add the result-types of our function to.
    // The resulting type of this declaration is a _type_appender, prefilled with the existing value_types of the Sender.
    template<template<typename...> class Tuple>
    using value_types_appender = typename sender_traits<Sender>::template value_types<Tuple, _type_appender>;

    public:
    // Define the value_types for this adapter.
    // They're computed from the parent sender's error_types, in combination with the existing value-types.
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = typename sender_traits<Sender>::template error_types<
          append_fn_result_types<
            Fn,
            Tuple,
            value_types_appender<Tuple> // Contains the value_types of the wrapped sender.
          >::template apply // This template will add the result-types of the error-function.
        >::template type<Variant>;
    // Since we transform errors, there are no more errors.
    // Except of course if there's an exception thrown from Fn.
    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr>;

    // Inherit all constructors.
    using _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t, get_completion_scheduler_t<set_value_t>>::_generic_sender_wrapper;
  };

  // Implementation of a then-continuation.
  //
  // This has an associated sender, but has no final receiver yet.
  // Once a receiver is connected, it'll call connect.
  template<sender Sender, typename Fn>
  class sender_impl
  : public sender_types_for_impl<Sender, Fn>
  {
    template<typename, sender, typename...> friend class ::earnest::execution::_generic_sender_wrapper;

    public:
    template<typename Sender_, typename Fn_>
    requires std::constructible_from<Sender, Sender_> && std::constructible_from<Fn, Fn_>
    constexpr sender_impl(Sender_&& sender, Fn_&& fn)
    : sender_types_for_impl<Sender, Fn>(std::move(sender)),
      fn(std::move(fn))
    {}

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> operation_state decltype(auto) {
      return ::earnest::execution::connect(
          std::move(self.s),
          wrapped_receiver<Fn, std::remove_cvref_t<Receiver>>(std::move(self.fn), std::forward<Receiver>(r)));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    -> sender_impl<std::remove_cvref_t<OtherSender>, Fn> {
      return sender_impl<std::remove_cvref_t<OtherSender>, Fn>(std::forward<OtherSender>(other_sender), std::move(fn));
    }

    [[no_unique_address]] Fn fn;
  };

  template<sender S, typename Fn>
  auto default_impl(S&& s, Fn&& fn) const
  -> sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Fn>> {
    return sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Fn>>(std::forward<S>(s), std::forward<Fn>(fn));
  }
};
inline constexpr lazy_upon_error_t lazy_upon_error{};


// Like lazy_upon_error, but it's allowed to be eagerly evaluated.
//
// Unless specialized, this simply forwards to lazy_upon_error.
struct upon_error_t {
  template<typename> friend struct _generic_operand_base_t;

  template<sender S, typename Fn>
  constexpr auto operator()(S&& s, Fn&& fn) const
  -> sender decltype(auto)
  {
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
    return ::earnest::execution::lazy_upon_error(std::forward<S>(s), std::forward<Fn>(fn));
  }
};
inline constexpr upon_error_t upon_error{};


// Create an upon-done-continuation.
//
// An upon-done-continuation takes the done-signal, and invokes a function.
// The result of that function is sent further along, to the next receiver.
// (Basically, this would allow you to implement cancelation-recovery.)
//
// Lazy operations will never run before `::earnest::execution::start` has been called.
struct lazy_upon_done_t {
  template<typename> friend struct _generic_operand_base_t;

  template<sender S, std::invocable<> Fn>
  constexpr auto operator()(S&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_done_t>(*this, std::forward<S>(s), std::forward<Fn>(fn));
  }

  template<std::invocable<> Fn>
  constexpr auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  // The receiver used by the then-continuation.
  //
  // It forwards everything to the wrapped receiver, except for the `set_value_t` signal.
  // When `set_value` is called, it'll take the values, transform them using the function,
  // and then forward those values to the wrapped receiver.
  template<typename Fn, receiver Receiver>
  struct wrapped_receiver
  : public _generic_receiver_wrapper<Receiver, set_value_t, set_done_t>
  {
    // XXX remove all but one of the constructors
    wrapped_receiver(const Fn& fn, const Receiver& r)
    : _generic_receiver_wrapper<Receiver, set_value_t, set_done_t>(r),
      fn(fn)
    {}

    wrapped_receiver(Fn&& fn, const Receiver& r)
    : _generic_receiver_wrapper<Receiver, set_value_t, set_done_t>(r),
      fn(std::move(fn))
    {}

    wrapped_receiver(const Fn& fn, Receiver&& r)
    : _generic_receiver_wrapper<Receiver, set_value_t, set_done_t>(std::move(r)),
      fn(fn)
    {}

    wrapped_receiver(Fn&& fn, Receiver&& r)
    : _generic_receiver_wrapper<Receiver, set_value_t, set_done_t>(std::move(r)),
      fn(std::move(fn))
    {}

    // Values are passed on.
    // We just want to ensure no exception escapes, otherwise that would be handled by our handler,
    // instead of a handler down the chain.
    template<typename... Args>
    friend auto tag_invoke([[maybe_unused]] set_value_t, wrapped_receiver&& self, Args&&... args) noexcept -> void {
      try {
        set_value(std::move(self.r), std::forward<Args>(args)...);
      } catch (...) {
        set_error(std::move(self.r), std::current_exception());
      }
    }

    // Errors are handled by the function, and its result is passed on as the value signal.
    friend auto tag_invoke([[maybe_unused]] set_done_t, wrapped_receiver&& self) noexcept -> void {
      // If set_value or the function can throw, we must handle the exception:
      // because set_done must be a noexcept invocation.
      try {
        self.invoke_done();
      } catch (...) {
        ::earnest::execution::set_error(std::move(self.r), std::current_exception());
      }
    }

    private:
    template<typename Error>
    auto invoke_done(Error&& err) -> void {
      if constexpr(std::is_void_v<std::invoke_result_t<Fn, Error>>) {
        std::invoke(std::move(fn));
        ::earnest::execution::set_value(std::move(this->r));
      } else {
        ::earnest::execution::set_value(std::move(this->r), std::invoke(std::move(fn)));
      }
    }

    [[no_unique_address]] Fn fn;
  };

  // Helper type: given a tuple-template, and a function return type, construct Tuple<FunctionReturnType>.
  // However, if the function returns void, then create Tuple<>.
  template<template<typename...> class Tuple, typename FnResultType>
  struct mk_tuple {
    using type = Tuple<FnResultType>;
  };

  template<template<typename...> class Tuple>
  struct mk_tuple<Tuple, void> {
    using type = Tuple<>;
  };

  template<typename Fn, template<typename...> class Tuple>
  using fn_result_type_as_tuple = typename mk_tuple<Tuple, decltype(std::invoke(std::declval<Fn>()))>::type;

  // Forward-declare impl, because we need it inside `sender_types_for_impl'.
  template<sender Sender, typename Fn> class sender_impl;

  // We need to update the sender-types.
  // But we can only do that, if the parent sender has sender types.
  //
  // Sadly, we can't selectively enable/disable having a local type-def.
  // So we have to wrap our base class as needed.
  //
  // Since the `set_done'-invocation runs on the done-scheduler of the nested sender,
  // while the `set_value'-invocation runs on the value-scheduler of the nested sender,
  // we cannot advertise a value-scheduler.
  // Also, if the set_value on the receiver fails, it'll invoke set_error, on potentially
  // the wrong completion-scheduler. But way at top we said we make an exclusion for exception-ptr,
  // so that's fine and we keep forwarding it.
  //
  // While we don't have ambiguity on the completion-scheduler for the set_done-signal,
  // since we don't emit dones, we might as well swallow up the
  // `get_completion_scheduler<set_done_t>'.
  template<sender Sender, typename Fn>
  struct sender_types_for_impl
  : _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_done_t>>
  {
    // Inherit all constructors.
    using _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_done_t>>::_generic_sender_wrapper;
  };

  // This specialization only applies for typed_senders.
  // Since we know the type, we can compute the transformed type.
  template<typed_sender Sender, typename Fn>
  struct sender_types_for_impl<Sender, Fn>
  : _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_done_t>>
  {
    private:
    // Transform the existing value types from Sender, into something where we can add the result-types of our function to.
    // The resulting type of this declaration is a _type_appender, prefilled with the existing value_types of the Sender.
    template<template<typename...> class Tuple>
    using value_types_appender = typename sender_traits<Sender>::template value_types<Tuple, _type_appender>;

    public:
    // Define the value_types for this adapter.
    // They're computed from the parent sender's error_types, in combination with the existing value-types.
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = typename
        value_types_appender<Tuple>:: // Original types, plus ...
        template append<fn_result_type_as_tuple<Fn, Tuple>>:: // Append result-type of the function (as a Tuple).
        template type<Variant>; // Resolve the Variant.

    static inline constexpr bool sends_done = false; // We handle the done signal.

    // Inherit all constructors.
    using _generic_sender_wrapper<sender_impl<Sender, Fn>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_done_t>>::_generic_sender_wrapper;
  };

  // Implementation of a then-continuation.
  //
  // This has an associated sender, but has no final receiver yet.
  // Once a receiver is connected, it'll call connect.
  template<sender Sender, typename Fn>
  class sender_impl
  : public sender_types_for_impl<Sender, Fn>
  {
    template<typename, sender, typename...> friend class ::earnest::execution::_generic_sender_wrapper;

    public:
    template<typename Sender_, typename Fn_>
    requires std::constructible_from<Sender, Sender_> && std::constructible_from<Fn, Fn_>
    constexpr sender_impl(Sender_&& sender, Fn_&& fn)
    : sender_types_for_impl<Sender, Fn>(std::forward<Sender_>(sender)),
      fn(std::forward<Fn_>(fn))
    {}

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> operation_state decltype(auto) {
      return ::earnest::execution::connect(
          std::move(self.s),
          wrapped_receiver<Fn, std::remove_cvref_t<Receiver>>(std::move(self.fn), std::forward<Receiver>(r)));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    -> sender_impl<std::remove_cvref_t<OtherSender>, Fn> {
      return sender_impl<std::remove_cvref_t<OtherSender>, Fn>(std::forward<OtherSender>(other_sender), std::move(fn));
    }

    [[no_unique_address]] Fn fn;
  };

  template<sender S, typename Fn>
  auto default_impl(S&& s, Fn&& fn) const
  -> sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Fn>> {
    return sender_impl<std::remove_cvref_t<S>, std::remove_cvref_t<Fn>>(std::forward<S>(s), std::forward<Fn>(fn));
  }
};
inline constexpr lazy_upon_done_t lazy_upon_done{};


// Like lazy_upon_done, but it's allowed to be eagerly evaluated.
//
// Unless specialized, this simply forwards to lazy_upon_done.
struct upon_done_t {
  template<typename> friend struct _generic_operand_base_t;

  template<sender S, std::invocable<> Fn>
  constexpr auto operator()(S&& s, Fn&& fn) const
  -> sender decltype(auto)
  {
    return _generic_operand_base<set_done_t>(*this, std::forward<S>(s), std::forward<Fn>(fn));
  }

  template<std::invocable<> Fn>
  constexpr auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<sender S, typename Fn>
  constexpr auto default_impl(S&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return ::earnest::execution::lazy_upon_done(std::forward<S>(s), std::forward<Fn>(fn));
  }
};
inline constexpr upon_done_t upon_done{};


// The sender for the `just` function.
//
// This sender holds on to zero or more values.
// And when started, passes those values on the chain.
//
// As an optional case, we allow `then` to be computed immediately,
// if the call would be noexcept.
template<typename... T>
class _just_sender {
  private:
  // The operation state for this sender.
  //
  // Does the actual work of passing values to a receiver.
  template<typename Receiver>
  struct operation_state
  : public operation_state_base_
  {
    public:
    operation_state(const std::tuple<T...>& values, Receiver&& r)
    : values(values),
      r(std::move(r))
    {}

    operation_state(std::tuple<T...>&& values, Receiver&& r)
    : values(std::move(values)),
      r(std::move(r))
    {}

    friend auto tag_invoke([[maybe_unused]] execution::start_t, operation_state& self) noexcept -> void {
      self.invoke(std::index_sequence_for<T...>());
    }

    private:
    template<std::size_t... Idx>
    auto invoke([[maybe_unused]] std::index_sequence<Idx...>) noexcept -> void {
      try {
        ::earnest::execution::set_value(std::move(r), std::get<Idx>(std::move(values))...);
      } catch (...) {
        ::earnest::execution::set_error(std::move(r), std::current_exception());
      }
    }

    [[no_unique_address]] std::tuple<T...> values;
    [[no_unique_address]] Receiver r;
  };

  public:
  template<template<typename...> class Tuple, template<typename...> class Variant>
  using value_types = Variant<Tuple<T...>>;

  template<template<typename...> class Variant>
  using error_types = Variant<std::exception_ptr>;

  static inline constexpr bool sends_done = false;

  template<typename... T_>
  explicit constexpr _just_sender(T_&&... v)
  : values(std::forward<T_>(v)...)
  {}

  template<receiver Receiver>
  friend constexpr auto tag_invoke([[maybe_unused]] connect_t, _just_sender&& self, Receiver&& r)
  -> operation_state<std::remove_cvref_t<Receiver>> {
    return operation_state<std::remove_cvref_t<Receiver>>(std::move(self.values), std::forward<Receiver>(r));
  }

  // Optimization: immediately compute eager then(), if the function is noexcept.
  template<std::invocable<T...> Fn>
  requires
      std::is_nothrow_invocable_v<Fn, T...> &&
      ( std::is_void_v<std::invoke_result_t<Fn, T...>> ||
        std::is_nothrow_constructible_v<
            std::remove_cvref_t<std::invoke_result_t<Fn, T...>>,
            std::invoke_result_t<Fn, T...>>)
  friend constexpr auto tag_invoke([[maybe_unused]] then_t, _just_sender&& self, Fn&& fn) noexcept {
    using new_type = std::remove_cvref_t<std::invoke_result_t<Fn, T...>>;

    if constexpr(std::is_void_v<new_type>) {
      std::apply(std::forward<Fn>(fn), std::move(self.values));
      return _just_sender<>();
    } else {
      return _just_sender<new_type>(std::apply(std::forward<Fn>(fn), std::move(self.values)));
    }
  }

  private:
  [[no_unique_address]] std::tuple<T...> values;
};

// Create a sender that sends the given values.
template<typename... T>
constexpr auto just(T&&... v)
-> _just_sender<std::remove_cvref_t<T>...> {
  return _just_sender<std::remove_cvref_t<T>...>(std::forward<T>(v)...);
}


// Collect multiple distinct value-types into a variant-of-tuple.
//
// This changes a sender with many different types, into a sender of 1 type (albeit a variant type).
struct into_variant_t {
  template<typename> friend struct _generic_operand_base_t;

  template<typed_sender Sender>
  auto operator()(Sender&& s) const
  -> sender decltype(auto) {
    return _generic_operand_base<>(*this, std::forward<Sender>(s));
  }

  auto operator()() const
  -> decltype(auto) {
    return _generic_adapter(*this);
  }

  private:
  template<typed_sender Sender>
  struct transformer {
    using result_type = typename sender_traits<Sender>::template value_types<std::tuple, std::variant>;

    template<typename... Args>
    requires std::constructible_from<result_type, std::tuple<std::remove_cvref_t<Args>...>>
    constexpr auto operator()(Args&&... args)
    -> result_type {
      return result_type(std::make_tuple(std::forward<Args>(args)...));
    }
  };

  // Implement in terms of lazy-then.
  template<typed_sender Sender>
  auto default_impl(Sender&& s) const
  -> decltype(lazy_then(std::forward<Sender>(s), transformer<Sender>{})) {
    return then(std::forward<Sender>(s), transformer<Sender>{});
  }
};
inline constexpr into_variant_t into_variant{};


// Start an operation.
//
// If the operation produces values, it'll discard those.
// If the operation produces an error, it'll call std::terminate.
struct start_detached_t {
  template<typename> friend struct _generic_operand_base_t;

  template<sender Sender>
  auto operator()(Sender&& s) const
  -> void {
    _generic_operand_base<set_value_t>(*this, std::forward<Sender>(s));
  }

  private:
  struct receiver {
    explicit receiver(std::shared_ptr<void> state) noexcept
    : state(std::move(state))
    {}

    template<typename... Args>
    friend auto tag_invoke([[maybe_unused]] set_value_t, [[maybe_unused]] receiver&& self, [[maybe_unused]] Args&&...) noexcept -> void {
      self.state.reset();
    }

    template<typename Ex>
    [[noreturn]]
    friend auto tag_invoke([[maybe_unused]] set_error_t, [[maybe_unused]] receiver&& self, [[maybe_unused]] Ex&& ex) noexcept -> void {
      std::terminate();
    }

    friend auto tag_invoke([[maybe_unused]] set_done_t, [[maybe_unused]] receiver&& self) noexcept -> void {
      self.state.reset();
    }

    private:
    std::shared_ptr<void> state;
  };

  template<sender Sender>
  auto default_impl(Sender&& s) const
  -> void {
    using op_state_type = std::remove_cvref_t<decltype(::earnest::execution::connect(std::declval<Sender>(), std::declval<receiver>()))>;

    const auto state_ptr = std::make_shared<optional_operation_state_<op_state_type>>(std::nullopt);
    state_ptr->emplace(std::forward<Sender>(s), receiver(state_ptr));
    ::earnest::execution::start(**state_ptr);
  }
};
inline constexpr start_detached_t start_detached;


// Dispatch a function on a specific scheduler.
//
// The function will be run in the execution-context of the scheduler.
struct execute_t {
  template<typename> friend struct _generic_operand_base_t;

  template<scheduler Scheduler, std::invocable<> Fn>
  auto operator()(Scheduler&& sch, Fn&& fn)
  -> void {
    _generic_operand_base<>(*this, sch, std::forward<Fn>(fn));
  }

  private:
  template<scheduler Scheduler, std::invocable<> Fn>
  auto default_impl(Scheduler&& sch, Fn&& fn)
  -> void {
    ::earnest::execution::start_detached(::earnest::execution::then(::earnest::execution::schedule(std::forward<Scheduler>(sch)), std::forward<Fn>(fn)));
  }
};
inline constexpr execute_t execute{};


// Execute a sender on the current thread.
//
// Execution of this thread will block, until the operation-chain resolves.
struct sync_wait_t {
  template<typename> friend struct _generic_operand_base_t;

  template<typed_sender Sender>
  requires (std::variant_size_v<typename sender_traits<std::remove_cvref_t<Sender>>::template value_types<std::tuple, std::variant>> == 1)
  constexpr auto operator()(Sender&& s) const
  -> decltype(_generic_operand_base<set_value_t>(std::declval<const sync_wait_t&>(), std::declval<Sender>())) {
    return _generic_operand_base<set_value_t>(*this, std::forward<Sender>(s));
  }

  private:
  // Execution context.
  //
  // This will exist until a result is supplied.
  // If a task is placed in this execution-context, it'll be run by the blocked thread.
  struct execution_context {
    public:
    // Operation state holds on to a nested operation state.
    // Its start() operation will execute it in the execution-context.
    template<operation_state OpState>
    class operation_state
    : public operation_state_base_
    {
      public:
      template<sender Sender, receiver Receiver>
      explicit operation_state(execution_context& ex_ctx, Sender&& s, Receiver&& r)
      : ex_ctx(ex_ctx),
        op_state(execution::connect(std::forward<Sender>(s), std::forward<Receiver>(r)))
      {}

      friend auto tag_invoke([[maybe_unused]] start_t, operation_state& o) noexcept -> void {
        // XXX this can actually throw.
        o.ex_ctx.push(
            [&o]() noexcept {
              start(o.op_state);
            });
      }

      private:
      execution_context& ex_ctx;
      OpState op_state;
    };

    // Create an operation state, wrapping the given operation state.
    // The returned operation state will execute on the execution-context.
    template<sender Sender, receiver Receiver>
    static auto make_operation_state(execution_context& ex_ctx, Sender&& s, Receiver&& r)
    -> operation_state<std::remove_cvref_t<decltype(execution::connect(std::declval<Sender>(), std::declval<Receiver>()))>> {
      return operation_state<std::remove_cvref_t<decltype(execution::connect(std::declval<Sender>(), std::declval<Receiver>()))>>(ex_ctx, std::forward<Sender>(s), std::forward<Receiver>(r));
    }

    // A sender for this execution context.
    class sender {
      public:
      template<template<typename...> class Tuple, template<typename...> class Variant>
      using value_types = Variant<Tuple<>>;

      template<template<typename...> class Variant>
      using error_types = Variant<std::exception_ptr>;

      static inline constexpr bool sends_done = false;

      template<receiver Receiver>
      friend auto tag_invoke([[maybe_unused]] connect_t, const sender& self, Receiver&& r)
      -> auto {
        return make_operation_state(self.ex_ctx, just(), std::forward<Receiver>(r));
      }

      template<receiver Receiver>
      friend auto tag_invoke([[maybe_unused]] connect_t, sender&& self, Receiver&& r)
      -> auto {
        return make_operation_state(self.ex_ctx, just(), std::forward<Receiver>(r));
      }

      friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_value_t>, const sender& self) noexcept -> auto {
        return self.ex_ctx.as_scheduler();
      }

      friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_error_t>, const sender& self) noexcept -> auto {
        return self.ex_ctx.as_scheduler();
      }

      friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_done_t>, const sender& self) noexcept -> auto {
        return self.ex_ctx.as_scheduler();
      }

      sender(execution_context& ex_ctx) noexcept
      : ex_ctx(ex_ctx)
      {}

      private:
      execution_context& ex_ctx;
    };

    // Scheduler for this execution-context.
    //
    // It handles the task of enqueueing tasks on the job-queue of the execution-context.
    class scheduler {
      public:
      template<std::invocable<> Fn>
      friend auto tag_invoke([[maybe_unused]] execute_t tag, scheduler&& self, Fn&& fn) -> void {
        self.ctx->push(std::forward<Fn>());
      }

      template<std::invocable<> Fn>
      friend auto tag_invoke([[maybe_unused]] execute_t, scheduler& self, Fn&& fn) -> void {
        self.ctx->push(std::forward<Fn>());
      }

      friend auto tag_invoke([[maybe_unused]] schedule_t, const scheduler& self) noexcept -> sender {
        return sender(*self.ctx);
      }

      friend constexpr auto tag_invoke([[maybe_unused]] const get_forward_progress_guarantee_t&, [[maybe_unused]] const scheduler&) noexcept -> forward_progress_guarantee {
        return forward_progress_guarantee::weakly_parallel;
      }

      explicit constexpr scheduler(execution_context* ctx) noexcept
      : ctx(ctx)
      {}

      constexpr auto operator==(const scheduler& other) const noexcept -> bool {
        return ctx == other.ctx;
      }

      constexpr auto operator!=(const scheduler& other) const noexcept -> bool {
        return !(*this == other);
      }

      private:
      execution_context* ctx;
    };

    execution_context() = default;

    // We don't allow copying/moving of the execution-context.
    execution_context(const execution_context&) = delete;
    execution_context(execution_context&&) = delete;
    execution_context& operator=(const execution_context&) = delete;
    execution_context& operator=(execution_context&&) = delete;

    ~execution_context() {
      assert(tasks.empty());
    }

    auto as_scheduler() noexcept -> scheduler {
      return scheduler(this);
    }

    // Notification wakes up the thread executing this execution-context.
    //
    // Call when new tasks are enqueued, or when the predicate may have changed.
    auto notify() noexcept -> void {
      cnd.notify_one();
    }

    // Block and run tasks.
    // Until the predicate becomes true.
    template<std::predicate<> Predicate>
    auto drain_until(Predicate&& pred) noexcept -> void {
      std::unique_lock lck{mtx};
      while (!std::invoke(pred)) {
        execute_(lck);
        cnd.wait(lck,
            [this, &pred]() {
              return !this->tasks.empty() || std::invoke(pred);
            });
      }
    }

    private:
    template<std::invocable<> Fn>
    auto push(Fn&& fn) -> void {
      std::lock_guard lck{mtx};
      tasks.emplace(std::forward<Fn>(fn)); // May throw.
      cnd.notify_one();
    }

    template<typename Task>
    auto execute(Task&& task) -> void {
      std::lock_guard lck{mtx};
      tasks.emplace(std::forward<Task>(task));
      cnd.notify_one();
    }

    private:
    // Execute tasks until the queue is empty.
    auto execute_(std::unique_lock<std::mutex>& lck) noexcept -> void {
      assert(lck.owns_lock() && lck.mutex() == &mtx);

      while (!tasks.empty()) {
        auto next_task = std::move(tasks.front());
        tasks.pop();
        lck.unlock();
        std::invoke(std::move(next_task));
        next_task = nullptr; // Run function destructor outside the mutex.
        lck.lock();
      }
    }

    std::mutex mtx;
    std::condition_variable cnd;
    std::queue<move_only_function<void() && noexcept>> tasks;
  };

  // Receiver for `sync_wait`.
  //
  // Can return the scheduler.
  // And accepts the outcome of the operation-chain.
  template<typename Tpl>
  struct receiver {
    public:
    // We share a variant with the function that started all this.
    // It has four types:
    // - monostate, indicating that there is no value ready.
    // - Tpl, tuple of values that the `sync_wait` is to return; we'll write the values from the value-channel into here.
    // - exception_ptr, which holds an exception; we'll write error-channels into here.
    // - monostate, indicating that the done-channel was executed.
    //
    // There is a fifth state: `valueless_by_exception()`, which can happen if the Tpl-construction failed.
    // This state is treated the same as monostate.
    using variant_type = std::variant<std::monostate, Tpl, std::exception_ptr, std::monostate>;

    // Create the receiver.
    //
    // It holds on to a reference of the execution-context, and a reference to the output variant.
    receiver(execution_context& ex_ctx, variant_type& var) noexcept
    : ex_ctx(ex_ctx),
      var(var)
    {}

    // Request the scheduler.
    friend auto tag_invoke([[maybe_unused]] const get_scheduler_t&, const receiver& r) noexcept -> decltype(auto) {
      return r.ex_ctx.as_scheduler();
    }

    // Set value.
    //
    // We use emplace, to be as-late-binding as possible.
    template<typename... Args>
    friend auto tag_invoke([[maybe_unused]] set_value_t, receiver&& r, Args&&... args)
    -> std::enable_if_t<std::is_constructible_v<Tpl, Args...>> {
      assert(r.var.index() == 0 || r.var.valueless_by_exception()); // Undefined behaviour if there was already an assignment.
                                                                    // Note: valueless-by-exception is probably not true here,
                                                                    // because for that to happen, this function would have to be called before.
                                                                    // And (I think) double-invocation is undefined behaviour.
      r.var.template emplace<1>(std::forward<Args>(args)...); // May throw.
      r.ex_ctx.notify();
    }

    // Set error.
    //
    // Assigns the error the variant `var`.
    //
    // There are two branches in this function:
    // - if an `exception_ptr` is passed in, we set the variant immediately.
    // - if something else is passed in, we throw it, immediately capture it in an exception_ptr, and then set the variant.
    template<typename Ex>
    friend auto tag_invoke([[maybe_unused]] set_error_t, receiver&& r, Ex&& ex) noexcept -> void {
      if constexpr(std::is_same_v<std::exception_ptr, std::remove_cvref_t<Ex>>) {
        assert(r.var.index() == 0 || r.var.valueless_by_exception()); // Undefined behaviour if there was already an assignment.
        r.var.template emplace<2>(std::forward<Ex>(ex)); // Never throws.
        r.ex_ctx.notify();
      } else {
        set_error(std::move(r), std::make_exception_ptr(std::forward<Ex>(ex))); // Invokes this function, but the other branch. Never throws.
      }
    }

    // Set done.
    //
    // Assigns the 3rd position to the variant `var`.
    friend auto tag_invoke([[maybe_unused]] set_done_t, receiver&& r) noexcept -> void {
      assert(r.var.index() == 0 || r.var.valueless_by_exception()); // Undefined behaviour if there was already an assignment.
      r.var.template emplace<3>();
      r.ex_ctx.notify();
    }

    private:
    execution_context& ex_ctx;
    variant_type& var;
  };

  public:
  // Expose the type of the scheduler.
  //
  // This is not part of the standard, but it's pretty useful when going to write IO handlers.
  // If you do a read or write, within this context, there's no need to spin up the asynchronous machinery,
  // and instead we could do a read or write in the foreground. Should aid in debugging.
  //
  // Note that if there is a specialization for this operation, it may not be run with this scheduler.
  using scheduler = execution_context::scheduler;

  private:
  // We must ensure we remove any references.
  template<typename... T>
  using tuple_type = std::tuple<std::remove_cvref_t<T>...>;
  // We only want a single tuple result. If there are multiple results, we bail.
  template<typename T>
  using no_variant_type = T;

  template<typed_sender Sender>
  requires (std::variant_size_v<typename sender_traits<std::remove_cvref_t<Sender>>::template value_types<std::tuple, std::variant>> == 1)
  auto default_impl(Sender&& s) const
  -> std::optional<typename sender_traits<std::remove_cvref_t<Sender>>::template value_types<tuple_type, no_variant_type>> {
    using receiver_type = receiver<typename sender_traits<std::remove_cvref_t<Sender>>::template value_types<tuple_type, no_variant_type>>;

    execution_context ex_ctx; // Execution context, used for any tasks that use the scheduler.
                              // XXX should we allocate this on the heap?
                              // Placing it on the stack would cause stack-overwrites if there was a bug leaving a dangling executor.
                              // On the other hand, C++ habitually move heap-structures to the stack if it can prove lifetime.
    typename receiver_type::variant_type result; // Result variant. 1 = return value, 2 = return throwable error.
    auto op_state = ::earnest::execution::connect(std::forward<Sender>(s), receiver_type(ex_ctx, result));

    // Start executing this.
    // First we have to start the operation.
    // It's possible the operation will (at some point) schedule things on the scheduler. So when start()
    // completes, we must drain the scheduler.
    ::earnest::execution::start(op_state);
    ex_ctx.drain_until([&result]() { return result.index() != 0 && !result.valueless_by_exception(); });

    switch (result.index()) { // Can't use std::visit: consider if the value-type was std::exception_ptr.
      default:
        // Default case is unreachable:
        // case 0:                      ex_ctx will block in this case.
        // case valueless_by_exception: ex_ctx will block in this case.
#if __cpp_lib_unreachable >= 202202L
        std::unreachable();
#else
        for (;;); // Just so the compiler stops complaining.
#endif
        break;
      case 1: // Completed with set_value.
        return std::get<1>(std::move(result));
      case 2: // Completed with set_error.
        std::rethrow_exception(std::get<2>(std::move(result)));
      case 3: // Completed with set_done.
        return std::nullopt;
    }
  }
};
inline constexpr sync_wait_t sync_wait{};


// Lazily transfer using a scheduler.
// The schedule_from is implemented so that the target scheduler can provide optional optimizations.
struct lazy_schedule_from_t {
  template<typename> friend struct _generic_operand_base_t;

  template<scheduler Scheduler, sender Sender>
  auto operator()(Scheduler&& sch, Sender&& s) const
  -> sender decltype(auto) {
    if constexpr(tag_invocable<lazy_schedule_from_t, Scheduler, Sender>)
      return tag_invoke(*this, std::forward<Scheduler>(sch), std::forward<Sender>(s));
    else
      return default_impl(std::forward<Scheduler>(sch), std::forward<Sender>(s));
  }

  private:
  // A done-receiver-wrapper.
  //
  // When it receives the set_value signal, it'll invoke set_done.
  template<receiver Receiver>
  class done_receiver_wrapper
  : public _generic_receiver_wrapper<Receiver, set_value_t> {
    public:
    explicit done_receiver_wrapper(Receiver&& r)
    : _generic_receiver_wrapper<Receiver, set_value_t>(std::move(r))
    {}

    friend auto tag_invoke([[maybe_unused]] set_value_t, done_receiver_wrapper&& self) noexcept -> void {
      set_done(std::move(self.r));
    }
  };

  // A error-receiver-wrapper.
  //
  // When it receives the set_value signal, it'll invoke set_error, with some error that was stored at construction time.
  template<receiver Receiver, typename ErrorType>
  requires receiver<Receiver, ErrorType>
  class error_receiver_wrapper
  : public _generic_receiver_wrapper<Receiver, set_value_t> {
    public:
    error_receiver_wrapper(Receiver&& r, ErrorType&& err)
    : _generic_receiver_wrapper<Receiver, set_value_t>(std::move(r)),
      err(std::move(err))
    {}

    error_receiver_wrapper(Receiver&& r, const ErrorType& err)
    : _generic_receiver_wrapper<Receiver, set_value_t>(std::move(r)),
      err(err)
    {}

    friend auto tag_invoke([[maybe_unused]] set_value_t, error_receiver_wrapper&& self) noexcept -> void {
      set_error(std::move(self.r), std::move(self.err));
    }

    private:
    ErrorType err;
  };

  // A value-receiver-wrapper.
  //
  // When it receives the set_value signal, it'll invoke set_value, with some values that were stored at construction time.
  template<receiver Receiver, typename... ValueTypes>
  class value_receiver_wrapper
  : public _generic_receiver_wrapper<Receiver, set_value_t> {
    public:
    template<typename... ValueTypes_>
    value_receiver_wrapper(Receiver&& r, ValueTypes_&&... value_types)
    : _generic_receiver_wrapper<Receiver, set_value_t>(std::move(r)),
      values(std::forward<ValueTypes_>(value_types)...)
    {}

    friend auto tag_invoke([[maybe_unused]] set_value_t, value_receiver_wrapper&& self) -> void {
      std::apply(
          [&self](ValueTypes&&... values) -> void {
            set_value(std::move(self.r), std::move(values)...);
          },
          std::move(self.values));
    }

    private:
    [[no_unique_address]] std::tuple<ValueTypes...> values;
  };

  template<template<typename...> class SignalReceiverWrapper, scheduler Scheduler, receiver Receiver, typename... Args>
  class op_state
  : public operation_state_base_
  {
    public:
    // Declare the operation-state type that this op_state will wrap.
    using op_state_type = decltype(
        ::earnest::execution::connect(
            ::earnest::execution::schedule(std::declval<Scheduler>()),
            std::declval<SignalReceiverWrapper<Receiver, Args...>>()));

    template<typename... Args_>
    op_state(Scheduler&& sch, Receiver&& r, Args_&&... args)
    : impl(::earnest::execution::connect(
            ::earnest::execution::schedule(std::move(sch)),
            SignalReceiverWrapper<Receiver, Args...>(std::move(r), std::forward<Args_>(args)...)))
    {}

    void start() noexcept {
      ::earnest::execution::start(impl);
    }

    private:
    op_state_type impl;
  };

  // The receiver wrapper.
  //
  // This takes the receiver that is after this transfer, and wraps it.
  //
  // For each signal that this receiver-wrapper receives,
  // it'll execute `scheduler(sch) | <signal>_receiver_wrapper | Receiver`.
  // <signal>_receiver_wrapper is one of the above wrappers, that propagates whatever arguments came into the handler.
  // (Except that <signal>_receiver_wrapper isn't an adapter, but it's just a wrapper around Receiver.)
  template<scheduler Scheduler, receiver Receiver, typename... OpStates>
  class receiver_impl
  : public _generic_receiver_wrapper<Receiver, set_value_t, set_error_t, set_done_t>
  {
    public:
    receiver_impl(Scheduler&& sch, Receiver&& r)
    : _generic_receiver_wrapper<Receiver, set_value_t, set_error_t, set_done_t>(std::move(r)),
      sch(std::move(sch))
    {}

    receiver_impl(receiver_impl&& other)
    noexcept(std::is_nothrow_move_constructible_v<Receiver> && std::is_nothrow_move_constructible_v<Scheduler>)
    : _generic_receiver_wrapper<Receiver, set_value_t, set_error_t, set_done_t>(std::move(other)),
      sch(std::move(other.sch))
    {
      // We only permit moving the state, if it hasn't yet been started.
      assert(std::holds_alternative<std::monostate>(other.opstates));
    }

    template<typename... Args>
    friend auto tag_invoke([[maybe_unused]] set_value_t, receiver_impl&& self, Args&&... args) noexcept -> void {
      self.template invoke<value_receiver_wrapper>(std::forward<Args>(args)...);
    }

    template<typename ErrorType>
    friend auto tag_invoke([[maybe_unused]] set_error_t, receiver_impl&& self, ErrorType&& err) noexcept -> void {
      self.template invoke<error_receiver_wrapper>(std::forward<ErrorType>(err));
    }

    friend auto tag_invoke([[maybe_unused]] set_done_t, receiver_impl&& self) noexcept -> void {
      self.template invoke<done_receiver_wrapper>();
    }

    private:
    // This function does the transfer to the new scheduler.
    //
    // It uses a signal_receiver_wrapper template, which it'll use to guide execution.
    template<template<typename, typename...> class signal_receiver_wrapper, typename... Args>
    auto invoke(Args&&... args) noexcept -> void {
      try {
        using opstate_type = op_state<signal_receiver_wrapper, Scheduler, _generic_rawptr_receiver<Receiver>, std::remove_cvref_t<Args>...>;

        opstates
            .template emplace<opstate_type>(std::move(sch), make_copy_of_receiver(), std::forward<Args>(args)...)
            .start();
      } catch (...) {
        set_error(std::move(this->r), std::current_exception());
      }
    }

    // Makes a copy of the receiver.
    //
    // We need a copy of the receiver, so that if any of the next steps throws an exception,
    // we can still deliver it to the receiver.
    auto make_copy_of_receiver() noexcept -> _generic_rawptr_receiver<Receiver> {
      return _generic_rawptr_receiver<Receiver>(&this->r);
    }

    Scheduler sch;
    std::variant<std::monostate, OpStates...> opstates;
  };

  // Figure out the correct type of the receiver_impl.
  //
  // It's a bit hard to read, because we require an `op_state` for each invocation that the transfer can require.
  // But the outcome is in the `template<typed_sender Sender> using type = ...` part of the struct.
  template<scheduler Scheduler, receiver Receiver>
  struct select_receiver_type {
    private:
    // Figure out the implementation of op_state, for a given values-invocation.
    template<typename... Args>
    using values_opstate = op_state<value_receiver_wrapper, Scheduler, _generic_rawptr_receiver<Receiver>, std::remove_cvref_t<Args>...>;
    // Figure out the implementation of op_state, for a given error-invocation.
    template<typename ErrorType>
    using error_opstate = op_state<error_receiver_wrapper, Scheduler, _generic_rawptr_receiver<Receiver>, std::remove_cvref_t<ErrorType>>;
    // Figure out the implementation of op_state, for the done-invocation.
    using done_opstate = op_state<done_receiver_wrapper, Scheduler, _generic_rawptr_receiver<Receiver>>;

    // We only receive errors as a single collection.
    // So we need to transform-and-add that collection in a single step.
    template<typename Appender>
    struct many_error_opstate_ {
      template<typename... ErrorTypes>
      using type = typename Appender::template append<error_opstate<ErrorTypes>...>;
    };

    // This template takes a sender and an appender, and adds op_state for each possible values signal.
    // Returns an appender.
    template<typed_sender Sender, typename Appender>
    using add_value_handlers = typename sender_traits<Sender>::template value_types<values_opstate, Appender::template append>;
    // This template takes a sender and an appender, and adds op_state for each possible error signal.
    // Returns an appender.
    template<typed_sender Sender, typename Appender>
    using add_error_handlers = typename sender_traits<Sender>::template error_types<many_error_opstate_<Appender>::template type>;
    // This template takes a sender and an appender, and adds op_state for the done signal.
    // Returns an appender.
    // Even if the done signal isn't advertised, we must accept the done signal.
    template<typed_sender Sender, typename Appender>
    using add_done_handlers = typename Appender::template append<done_opstate>;

    // When we have all the operation-state wrappers figured out, we can declare the type of the receiver.
    template<typename... OpStates>
    using bind_opstates_to_receiver = receiver_impl<Scheduler, Receiver, OpStates...>;

    // This template adds all the possible handlers to the opstate.
    // Note that it is possible they'll contain duplicates, for example if the scheduler is type-erasing.
    template<typed_sender Sender>
    using add_all_handlers =
        add_done_handlers<Sender,
            add_error_handlers<Sender,
                add_value_handlers<Sender, _type_appender<>>>>;

    public:
    // This is the actual computation outcome.
    //
    // It worketh thusly:
    // - `add_all_done_handlers<Sender>` computes all the sender types. This may contain duplicates (we don't want duplicates).
    // - `::template type<_deduplicate<...>::template type>` forwards all these handler into a bound deduplicator, which deduplicates them.
    // - `_deduplicate<bind_opstates_to_receiver>::template type>` is the bound deduplicator. When it receives types, it'll deduplicate them
    //   and then apply them to `bind_opstates_to_receiver`.
    // - `bind_opstates_to_receiver` takes a series of op-states, and creates a `receiver_impl` using them.
    // And that last `receiver_impl` is what we want. :)
    template<typed_sender Sender>
    using type = typename add_all_handlers<Sender>::template type<_deduplicate<bind_opstates_to_receiver>::template type>;
  };

  // Easier-to-use version of the above.
  template<scheduler Scheduler, typed_sender Sender, receiver Receiver>
  using receiver_type = typename select_receiver_type<Scheduler, Receiver>::template type<Sender>;

  // Implementation of the sender.
  // It creates an op_state that does all the work.
  template<sender Sender, scheduler Scheduler>
  class sender_impl
  : public _generic_sender_wrapper<sender_impl<Sender, Scheduler>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>
  {
    template<typename, sender, typename...> friend class ::earnest::execution::_generic_sender_wrapper;

    public:
    template<typename Sender_, typename Scheduler_>
    requires std::constructible_from<Sender, Sender_> && std::constructible_from<Scheduler, Scheduler_>
    sender_impl(Sender_&& sender, Scheduler_&& sch)
    : _generic_sender_wrapper<sender_impl, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>(std::move(sender)),
      sch(std::move(sch))
    {}

    // The heart of the sender: connecting to another receiver.
    //
    // The receiver is wrapped inside a `receiver_impl` (the exact type if computed using `select_receiver_type`).
    // That wrapped receiver will do the work of creating a new operation-state and executing the transfer.
    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> operation_state decltype(auto) {
      return ::earnest::execution::connect(
          std::move(self.s),
          receiver_type<Scheduler, Sender, std::remove_cvref_t<Receiver>>(std::move(self.sch), std::forward<Receiver>(r)));
    }

    friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_value_t>, const sender_impl& self) noexcept -> const Scheduler& {
      return self.sch;
    }

    friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_error_t>, const sender_impl& self) -> const Scheduler& {
      return self.sch;
    }

    friend auto tag_invoke([[maybe_unused]] get_completion_scheduler_t<set_done_t>, const sender_impl& self) -> const Scheduler& {
      return self.sch;
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    -> sender_impl<std::remove_cvref_t<OtherSender>, Scheduler> {
      return sender_impl<std::remove_cvref_t<OtherSender>, Scheduler>(std::forward<OtherSender>(other_sender), std::move(sch));
    }

    Scheduler sch;
  };

  template<scheduler Scheduler, sender Sender>
  constexpr auto default_impl(Scheduler&& sch, Sender&& s) const
  -> sender_impl<std::remove_cvref_t<Sender>, std::remove_cvref_t<Scheduler>> {
    return sender_impl<std::remove_cvref_t<Sender>, std::remove_cvref_t<Scheduler>>(std::forward<Sender>(s), std::forward<Scheduler>(sch));
  }
};
inline constexpr lazy_schedule_from_t lazy_schedule_from{};


// Transfer using a scheduler.
// The schedule_from is implemented so that the target scheduler can provide optional optimizations.
struct schedule_from_t {
  template<typename> friend struct _generic_operand_base_t;

  template<scheduler Scheduler, sender Sender>
  auto operator()(Scheduler&& sch, Sender&& s) const
  -> sender decltype(auto) {
    if constexpr(tag_invocable<lazy_schedule_from_t, Scheduler, Sender>)
      return tag_invoke(*this, std::forward<Scheduler>(sch), std::forward<Sender>(s));
    else
      return default_impl(std::forward<Scheduler>(sch), std::forward<Sender>(s));
  }

  private:
  template<scheduler Scheduler, sender Sender>
  constexpr auto default_impl(Scheduler&& sch, Sender&& s) const
  -> sender decltype(auto) {
    return lazy_schedule_from(std::forward<Scheduler>(sch), std::forward<Sender>(s));
  }
};
inline constexpr schedule_from_t schedule_from{};


// Transfer control to a different scheduler.
struct lazy_transfer_t {
  template<typename> friend struct _generic_operand_base_t;

  template<sender Sender, scheduler Scheduler>
  constexpr auto operator()(Sender&& s, Scheduler&& sch) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<Sender>(s), std::forward<Scheduler>(sch));
  }

  template<scheduler Scheduler>
  constexpr auto operator()(Scheduler&& sch) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Scheduler>(sch));
  }

  private:
  template<sender Sender, scheduler Scheduler>
  constexpr auto default_impl(Sender&& s, Scheduler&& sch) const
  -> sender decltype(auto) {
    return lazy_schedule_from(std::forward<Scheduler>(sch), std::forward<Sender>(s));
  }
};
inline constexpr lazy_transfer_t lazy_transfer;


// Transfer control to a different scheduler.
struct transfer_t {
  template<typename> friend struct _generic_operand_base_t;

  template<sender Sender, scheduler Scheduler>
  constexpr auto operator()(Sender&& s, Scheduler&& sch) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<Sender>(s), std::forward<Scheduler>(sch));
  }

  template<scheduler Scheduler>
  constexpr auto operator()(Scheduler&& sch) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Scheduler>(sch));
  }

  private:
  template<sender Sender, scheduler Scheduler>
  constexpr auto default_impl(Sender&& s, Scheduler&& sch) const
  -> sender decltype(auto) {
    return schedule_from(std::forward<Scheduler>(sch), std::forward<Sender>(s));
  }
};
inline constexpr transfer_t transfer;


// Create a sender that provides the given values, on the scheduler.
struct transfer_just_t {
  template<typename> friend struct _generic_operand_base_t;

  template<scheduler Scheduler, typename... Args>
  constexpr auto operator()(Scheduler&& sch, Args&&... args) const
  -> sender decltype(auto) {
    if constexpr(tag_invocable<transfer_just_t, Scheduler, Args...>) {
      return tag_invoke(*this, std::forward<Scheduler>(sch), std::forward<Args>(args)...);
    } else {
      return transfer(just(std::forward<Args>(args)...), std::forward<Scheduler>(sch));
    }
  }
};
inline constexpr transfer_just_t transfer_just{};


// Common implementation for the let_* family of adapters.
//
// The let_* family of adapters always does the same thing, just in response to different signals.
// It's less work to write one that captures all possible cases.
struct _let_adapter_common_t {
  private:
  // This type adapter applies a Tuple to a _type_appender.
  // We need this, because we often want to transform the inner set of values-types
  // (a `_type_appender<_type_appender<...>, ...>').
  template<template<typename...> class Tuple>
  struct type_appender_to_tuple_ {
    template<typename TypeAdapter>
    using type = typename TypeAdapter::template type<Tuple>;
  };

  // Prepend the Signal type in the front of a type-appender.
  template<typename Signal>
  struct prepend {
    template<typename T>
    using type = typename _type_appender<Signal>::template merge<T>;
  };

  // Create a helper type, that'll figure out the function-arguments,
  // as well as the retained errors and values.
  //
  // Declares:
  // - function_arguments type (`_type_appender<_type_appender<FirstArgs...>, _type_appender<SecondArgs...>, ...>')
  //   describing the possible argument sets passed to the  function.
  //   The outer collection is the variant, the inner collections are the tuples.
  // - retained_value_types type (`_type_appender<_type_appender<FirstArgs...>, _type_appender<SecondArgs...>, ...>')
  //   describing the argument sets that are passed through as-is.
  //   The outer collection is the variant, the inner collections are the tuples.
  // - retained_error_types type (`_type_appender<ErrorType1, ErrorType2, ...>')
  //   describing the errors that are passed through as-is.
  //   The outer collection is the variant.
  // - retained_sends_done bool, describing if the set_done signal is retained.
  //   If the Sender doesn't emit set_done, then this will be false.
  template<typed_sender Sender, typename... Signal>
  requires (sizeof...(Signal) > 0) &&
      ((std::same_as<set_value_t, Signal> || std::same_as<set_error_t, Signal> || std::same_as<set_done_t, Signal>) &&...)
  struct sender_types_helper {
    using function_arguments = _type_appender<>::merge<
        // Add value-invocations, if the set_value is part of the signals.
        std::conditional_t<
            (std::same_as<set_value_t, Signal> ||...),
            typename sender_traits<Sender>::
                template value_types<_type_appender, _type_appender>::   // Take each of the value-types:
                                                                         // `_type_appender<
                                                                         //     _type_appender<T1...>,
                                                                         //     _type_appender<T2...>,
                                                                         //     ...>'
                template transform<prepend<set_value_t>::template type>, // Prepend the set_value_t in front of each:
                                                                         // `_type_appender<
                                                                         //     _type_appender<set_value_t, T1...>,
                                                                         //     _type_appender<set_value_t, T2...>,
                                                                         //     ...>'
            _type_appender<>>,
        // Add error-invocations, if the set_error is part of the signals.
        std::conditional_t<
            (std::same_as<set_error_t, Signal> ||...),
            typename sender_traits<Sender>::
                template error_types<_type_appender>::                   // Take each of the error-types: `_type_appender<E1, E2, ...>'
                template transform<_type_appender>::                     // Wrap each of the errors in its own type-appender:
                                                                         // `_type_appender<
                                                                         //     _type_appender<E1>,
                                                                         //     _type_appender<E2>,
                                                                         //     ...>'
                template transform<prepend<set_error_t>::template type>, // Prepend the set_error_t in front of each:
                                                                         // `_type_appender<
                                                                         //     _type_appender<set_error_t, E1>,
                                                                         //     _type_appender<set_error_t, E2>,
                                                                         //     ...>'
            _type_appender<>>,
        // Add done-invocation, if the set_done is part of the signals.
        std::conditional_t<
            (std::same_as<set_done_t, Signal> ||...),
            _type_appender<_type_appender<set_done_t>>,                  // The only done-invocation is without arguments, and only the set_done_t tag:
                                                                         // `_type_appender<_type_appender<set_done_t>>'
            _type_appender<>>
        >;

    // If set_value_t is not one of the signals, then all value types are retained.
    using retained_value_types = std::conditional_t<
        (std::same_as<set_value_t, Signal> ||...),
        _type_appender<>,
        typename sender_traits<Sender>::template value_types<_type_appender, _type_appender>>;
    // If set_error_t is not one of the signals, then all error types are retained.
    using retained_error_types = std::conditional_t<
        (std::same_as<set_error_t, Signal> ||...),
        _type_appender<>,
        typename sender_traits<Sender>::template error_types<_type_appender>>;
    // If done is intercepted, we don't retain it.
    static inline constexpr bool retained_sends_done = !(std::same_as<set_done_t, Signal> ||...) && sender_traits<Sender>::sends_done;
  };

  // The operation state for a specific invocation of the arguments and the function.
  // Holds on to:
  // - OpState: an operation state. It'll create this in place.
  // - Args: arguments to the let-function. It'll keep a hold of these until the operation-chain completes.
  template<operation_state OpState, typename... Args>
  struct values_and_opstate {
    private:
    template<std::size_t... Idx, typename Fn, typename Receiver, typename... Args_>
    constexpr values_and_opstate([[maybe_unused]] std::index_sequence<Idx...>, Fn&& fn, Receiver&& r, Args_&&... args)
    : args(std::forward<Args_>(args)...),
      opstate(
          connect(
              std::invoke(std::forward<Fn>(fn), std::get<Idx>(this->args)...),
              std::forward<Receiver>(r)))
    {}

    public:
    template<typename Fn, typename Receiver, typename... Args_>
    constexpr values_and_opstate(Fn&& fn, Receiver&& r, Args_&&... args)
    : values_and_opstate(std::index_sequence_for<Args...>(), std::forward<Fn>(fn), std::forward<Receiver>(r), std::forward<Args_>(args)...)
    {}

    // Disallow copy/move, because if the opstate contains references to args,
    // those would break during a copy/move operation.
    values_and_opstate(const values_and_opstate&) = delete;
    values_and_opstate(values_and_opstate&&) = delete;
    values_and_opstate& operator=(const values_and_opstate&) = delete;
    values_and_opstate& operator=(values_and_opstate&&) = delete;

    auto start() noexcept -> void {
      ::earnest::execution::start(opstate);
    }

    private:
    // Order is significant: opstate depends on references to args.
    //
    // We use no-unique-address to allow for empty-member-optimization,
    // which we think is safe because args and opstate should never be
    // identity compared (i.e. nobody will ever call
    // `static_cast<void*>(&args) == static_cast<void*>(&opstate)').
    [[no_unique_address]] std::tuple<Args...> args;
    [[no_unique_address]] OpState opstate;
  };

  // Helper struct, to figure out the correct type of values_and_opstate.
  //
  // By not using the Fn and Receiver in the type of values_and_opstate,
  // we minimize the amount of types the compiler has to generate.
  template<typename Fn, typename Receiver>
  struct values_and_opstate_type {
    public:
    // The let-function will return a sender of this type.
    //
    // Note that if the function returns a reference, we want to preserve that,
    // because it might be significant to the `connect' call.
    template<typename... Args>
    using sender_t = std::invoke_result_t<Fn, std::add_lvalue_reference_t<Args>...>;
    // Connecting the sender from the let-function, with the receiver we have to deliver to next,
    // yields this operation-state type.
    template<typename... Args>
    using opstate_t = decltype(connect(std::declval<sender_t<Args...>>(), std::declval<Receiver>()));

    // Figure out the type for values_and_opstate, based on the given argument types.
    template<typename... Args>
    using type = values_and_opstate<opstate_t<std::remove_cvref_t<Args>...>, std::remove_cvref_t<Args>...>;
  };

  // The op-state for the let-invocation.
  //
  // It knows of all values_and_opstate types needed.
  // Once the `apply' method is called, it'll create the requires values_and_opstate,
  // and then start that.
  template<typename... ValuesAndOpstates>
  requires (sizeof...(ValuesAndOpstates) > 0)
  class op_state {
    public:
    op_state() noexcept = default;

    // Op-state aren't moveable.
    // But, if the operation hasn't been started, we'll hold a std::monostate.
    // And we allow move-construction while that exists.
    op_state([[maybe_unused]] op_state&& other) noexcept {
      assert(std::holds_alternative<std::monostate>(other.opstates));
    }

    // Instantiate an opstate bound to the specific arguments.
    // And then starts that opstate.
    template<typename Fn, typename Receiver, typename... Args>
    auto apply(Fn&& fn, Receiver&& r, Args&&... args) -> void {
      using opstate_type = typename values_and_opstate_type<Fn, Receiver>::template type<Args...>;
      static_assert(std::disjunction_v<std::is_same<ValuesAndOpstates, opstate_type>...>,
          "chosen operation type must be present in the variant");
      assert(std::holds_alternative<std::monostate>(opstates));
      opstates
          .template emplace<opstate_type>(std::forward<Fn>(fn), std::forward<Receiver>(r), std::forward<Args>(args)...)
          .start();
    }

    private:
    // All the operation states that are possible.
    // And monostate, as the first type, so it'll default-construct with that.
    [[no_unique_address]] std::variant<std::monostate, ValuesAndOpstates...> opstates;
  };

  // This captures all variations of the operation state.
  // We collect each possible argument into a values_and_opstate type.
  // We then pass them to the op_state template.
  //
  // We use a struct, to make sure the template cannot result until all arguments
  // are specified. This is because if we use the using-type directly, the compiler
  // will partially apply what it can. And then complain if it can't.
  template<sender Sender, typename Fn, receiver Receiver, typename... Signal>
  struct op_state_type_ {
    static_assert(!std::is_same_v<
        _type_appender<>,
        typename sender_types_helper<Sender, Signal...>::function_arguments>,
        "function must be invoked with non-zero variations on arguments (i.e. is must be invoked)");

    using type = typename sender_types_helper<Sender, Signal...>::function_arguments::
        template transform<type_appender_to_tuple_<values_and_opstate_type<Fn, Receiver>::template type>::template type>::
        template type<op_state>;
  };
  template<sender Sender, typename Fn, receiver Receiver, typename... Signal>
  using op_state_type = typename op_state_type_<Sender, Fn, Receiver, Signal...>::type;

  // Implementation of a let_* receiver.
  //
  // Template arguments:
  // - Tag: one of set_value_t/set_error_t/set_done_t, indicating which let-operation is specialized.
  // - OpState: op_state_type outcome deciding on the operation stateus this receiver will contain.
  // - Receiver: the receiver that is being wrapped. The sender returned by Fn will flow into this Receiver.
  // - Fn: a function type that handles transforming arguments into a sender.
  template<typename OpState, receiver Receiver, typename Fn, typename... Signal>
  class receiver_impl
  : public _generic_receiver_wrapper<Receiver, Signal...>
  {
    public:
    receiver_impl(Receiver&& r, Fn&& fn)
    : _generic_receiver_wrapper<Receiver, Signal...>(std::move(r)),
      fn(std::move(fn))
    {}

    receiver_impl(const Receiver& r, Fn&& fn)
    : _generic_receiver_wrapper<Receiver, Signal...>(r),
      fn(std::move(fn))
    {}

    // tag-invoke specialization for the given Tag.
    template<typename Tag, typename... Args>
    requires
        (std::is_same_v<Tag, Signal> ||...) &&
        (std::is_same_v<set_value_t, Tag> ||
         (std::is_same_v<set_error_t, Tag> && sizeof...(Args) == 1) ||
         (std::is_same_v<set_done_t, Tag> && sizeof...(Args) == 0))
    friend auto tag_invoke(Tag tag, receiver_impl&& self, Args&&... args) noexcept -> void {
      try {
        self.next_state.apply(std::move(self.fn), self.make_copy_of_receiver(), std::move(tag), std::forward<Args>(args)...);
      } catch (...) {
        set_error(std::move(self.r), std::current_exception());
      }
    }

    private:
    // Makes a copy of the receiver.
    //
    // We need a copy of the receiver, so that if any of the steps throws an exception,
    // we can deliver that exception to the receiver.
    auto make_copy_of_receiver() noexcept -> _generic_rawptr_receiver<Receiver> {
      return _generic_rawptr_receiver<Receiver>(&this->r);
    }

    // Operation state for the continuation.
    // We'll fill it in when (if) we get values from the set_value signal.
    [[no_unique_address]] OpState next_state;
    // Function that generates a sender for us.
    [[no_unique_address]] Fn fn;
  };

  // Forward-declare sender implementation.
  template<sender Sender, typename Fn, typename... Signal> class sender_impl;

  // Figure out the sender_traits types for the sender.
  //
  // We must block access to all `get_completion_scheduler's:
  // we cannot query the result from the let-invocation.
  template<sender Sender, typename Fn, typename... Signal>
  struct sender_types_for_impl
  : public _generic_sender_wrapper<sender_impl<Sender, Fn, Signal...>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>
  {
    using _generic_sender_wrapper<sender_impl<Sender, Fn, Signal...>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>::_generic_sender_wrapper;
  };

  // Specialization for the case where the wrapped sender has known value-types.
  template<typed_sender Sender, typename Fn, typename... Signal>
  struct sender_types_for_impl<Sender, Fn, Signal...>
  : public _generic_sender_wrapper<sender_impl<Sender, Fn, Signal...>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>
  {
    private:
    using traits_helper = sender_types_helper<Sender, Signal...>;

    // Helper struct, given a type-appender of function-arguments, will figure out the invoke-result of Fn with those arguments.
    // We need this to compute the all_senders type (since the invoke-result of Fn with Args will produce a sender).
    template<typename> struct figure_out_invoke_result__;
    template<typename... Args>
    requires std::invocable<Fn, Args&...>
    struct figure_out_invoke_result__<_type_appender<Args...>> : std::invoke_result<Fn, Args&...> {};
    template<typename Appender>
    using figure_out_invoke_result_ = typename figure_out_invoke_result__<Appender>::type;
    // Figure out all sender.
    // Creates a _type_appender<sender_traits<sender1_type>, sender_traits<sender2_type>, sender_traits<sender3_type>, ...>.
    using all_senders = typename traits_helper::function_arguments::
        template transform<figure_out_invoke_result_>::          // Compute the invoke-result of Fn with Args.
        template transform<std::remove_cvref_t>::                // Strip off any decorators, so we can apply sender_traits.
        template transform<sender_traits>;                       // And squash them into their sender traits (since that's all we care about).

    // Helper type for collect_value_types.
    // Takes a sender_traits, and computes `_type_appender<_type_appender<...>, _type_appender<...>, ...>' value-types from it.
    template<typename SenderTraits>
    using collect_value_types_ = typename SenderTraits::template value_types<_type_appender, _type_appender>;
    // Collect all types into a `_type_appender<_type_appender<...>, _type_appender<...>, ...>'.
    // The outer _type_appender is the variant, the inner are the tuples.
    using collect_value_types = typename all_senders::
        template transform<collect_value_types_>:: // Produces value-types, but they're all wrapped inside another _type_appender
        template type<_type_appender<>::merge>;    // Unpacks the outer _type_appender.

    // Helper type for collect_error_types.
    // Takes a sender_traits, and computes `_type_appender<E1, E2, ...>' error-types from it.
    template<typename SenderTraits>
    using collect_error_types_ = typename SenderTraits::template error_types<_type_appender>;
    // Collect all the error types into a _type_appender.
    // Creates `_type_appender<ErrorType1, ErrorType2, ...>'.
    using collect_error_types = typename all_senders::
        template transform<collect_error_types_>:: // Produces error-types, but they're all wrapped in another _type_appender
        template type<_type_appender<>::merge>;    // Unpacks the outer _type_appender.

    // Get the sends_done from each sender, and expose it as a std::true_type or std::false_type.
    // (It needs to be a type, because we want to apply it to the all_senders::type<...> template, which can only return types.)
    template<typename SenderTraits>
    using sends_done_as_integral_constant_ = std::integral_constant<bool, SenderTraits::sends_done>;
    // Figure out if the sends_done signal is emitted from any of the created senders.
    static inline constexpr bool collect_sends_done = all_senders::
        template transform<sends_done_as_integral_constant_>:: // Creates `_type_appender<true_type, false_type, ...>'
        template type<std::disjunction>::                      // Creates a single integral-constant (either std::true_type or std::false_type).
        value;

    public:
    // Value-types is the union of retained value-types and collected value-types.
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types =
        typename _type_appender<>::merge<
            typename traits_helper::retained_value_types,
            collect_value_types
        >::                                                                 // value-types, but using _type_appender
        template transform<type_appender_to_tuple_<Tuple>::template type>:: // replace the inner _type_appenders with tuples
        template type<Variant>;                                             // replace the outer _type_appender with variant.

    // Error-types of this sender is the union of retained and collected error-types.
    // And if the function or the connect operation throws an exception, it's also the exception_ptr.
    template<template<typename...> class Variant>
    using error_types =
        typename _type_appender<>::merge<
            _type_appender<std::exception_ptr>,           // If we fail to run the function, we'll have the exception error.
            typename traits_helper::retained_error_types, // Original errors are preserved.
            collect_error_types                           // And all the errors from the returned senders.
        >::template type<Variant>;

    // We send the done signal, if it is sent (without intercept) from the original sender,
    // or is sent by any of the created senders.
    static inline constexpr bool sends_done = traits_helper::retained_sends_done || collect_sends_done;

    using _generic_sender_wrapper<sender_impl<Sender, Fn, Signal...>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>::_generic_sender_wrapper;
  };

  // Sender for the let_value operation.
  // When connected to, it creates a receiver_impl, and connects that to the wrapped sender.
  template<sender Sender, typename Fn, typename... Signal>
  class sender_impl
  : public sender_types_for_impl<Sender, Fn, Signal...>
  {
    template<typename, sender, typename...> friend class ::earnest::execution::_generic_sender_wrapper;

    private:
    template<receiver Receiver>
    using wrapped_receiver_type =
        receiver_impl<
            op_state_type<Sender, Fn, _generic_rawptr_receiver<Receiver>, Signal...>,
            Receiver,
            Fn,
            Signal...>;

    public:
    template<typename Sender_, typename Fn_>
    requires std::constructible_from<Sender, Sender_> && std::constructible_from<Fn, Fn_>
    constexpr sender_impl(Sender_&& s, Fn_&& fn)
    : sender_types_for_impl<Sender, Fn, Signal...>(std::forward<Sender_>(s)),
      fn(std::forward<Fn_>(fn))
    {}

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> operation_state decltype(auto) {
      return ::earnest::execution::connect(
          std::move(self.s),
          wrapped_receiver_type<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r), std::move(self.fn)));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    -> sender_impl<std::remove_cvref_t<OtherSender>, Fn, Signal...> {
      return sender_impl<std::remove_cvref_t<OtherSender>, Fn, Signal...>(std::forward<OtherSender>(other_sender), std::move(fn));
    }

    // The function that will return the sender for the let_value-operation.
    Fn fn;
  };

  public:
  template<typename... Signal, sender Sender, typename Fn>
  requires ((sizeof...(Signal) > 0) &&...&& (std::same_as<Signal, set_value_t> || std::same_as<Signal, set_error_t> || std::same_as<Signal, set_done_t>))
  static constexpr auto impl(Sender&& s, Fn&& fn)
  -> sender decltype(auto) {
    return sender_impl<std::remove_cvref_t<Sender>, std::remove_cvref_t<Fn>, Signal...>(std::forward<Sender>(s), std::forward<Fn>(fn));
  }
};


// Create a lazy-let-value adapter.
// This adapter takes a function argument. When it receives the set_value-signal,
// it'll invoke that function. The function should return a sender.
// It'll then run attach that sender to the receiver.
//
// Note, the documentation appears to suggest that the values from the set_value signal
// are stored in the execution-state, ensuring it's lifetime until the receiver completes.
// It also appears to suggest those values are passed by lvalue-reference, instead of forwarded.
//
// A lazy adapter will never run before the operation change is started.
struct lazy_let_value_t {
  template<typename> friend struct _generic_operand_base_t;

  // When sender produces the set-value signal, invoke fn.
  // Fn will return a sender, and we'll run that next.
  template<sender Sender, typename Fn>
  constexpr auto operator()(Sender&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<Sender>(s), std::forward<Fn>(fn));
  }

  template<typename Fn>
  constexpr auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<typename Fn>
  class fn_wrapper {
    static_assert(!std::is_const_v<Fn> && !std::is_reference_v<Fn>);

    public:
    explicit fn_wrapper(Fn&& fn)
    : fn(std::move(fn))
    {}

    explicit fn_wrapper(const Fn& fn)
    : fn(fn)
    {}

    template<typename... Args>
    auto operator()([[maybe_unused]] set_value_t tag, Args&&... args) & -> decltype(auto) {
      return std::invoke(fn, std::forward<Args>(args)...);
    }

    template<typename... Args>
    auto operator()([[maybe_unused]] set_value_t tag, Args&&... args) const & -> decltype(auto) {
      return std::invoke(fn, std::forward<Args>(args)...);
    }

    template<typename... Args>
    auto operator()([[maybe_unused]] set_value_t tag, Args&&... args) && -> decltype(auto) {
      return std::invoke(std::move(fn), std::forward<Args>(args)...);
    }

    private:
    Fn fn;
  };

  template<sender Sender, typename Fn>
  constexpr auto default_impl(Sender&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _let_adapter_common_t::impl<set_value_t>(std::forward<Sender>(s), fn_wrapper(std::forward<Fn>(fn)));
  }
};
inline constexpr lazy_let_value_t lazy_let_value{};


// Create a let-value adapter.
// This adapter takes a function argument. When it receives the set_value-signal,
// it'll invoke that function. The function should return a sender.
// It'll then run attach that sender to the receiver.
//
// Note, the documentation says that the values from the set_value signal
// are stored in the execution-state, ensuring its lifetime until the receiver completes.
// It also appears to suggest those values are passed by lvalue-reference, instead of forwarded.
struct let_value_t {
  template<typename> friend struct _generic_operand_base_t;

  // When sender produces the set-value signal, invoke fn.
  // Fn will return a sender, and we'll run that next.
  template<sender Sender, typename Fn>
  constexpr auto operator()(Sender&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<Sender>(s), std::forward<Fn>(fn));
  }

  template<typename Fn>
  constexpr auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<sender Sender, typename Fn>
  constexpr auto default_impl(Sender&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return lazy_let_value(std::forward<Sender>(s), std::forward<Fn>(fn));
  }
};
inline constexpr let_value_t let_value;


// Create a lazy-let-error adapter.
// This adapter takes a function argument. When it receives the set_error-signal,
// it'll invoke that function. The function should return a sender.
// It'll then run attach that sender to the receiver.
//
// Note, the documentation appears to suggest that the errors from the set_error signal
// are stored in the execution-state, ensuring it's lifetime until the receiver completes.
// It also appears to suggest those errors are passed by lvalue-reference, instead of forwarded.
//
// A lazy adapter will never run before the operation change is started.
struct lazy_let_error_t {
  template<typename> friend struct _generic_operand_base_t;

  // When sender produces the set-error signal, invoke fn.
  // Fn will return a sender, and we'll run that next.
  template<sender Sender, typename Fn>
  constexpr auto operator()(Sender&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_error_t>(*this, std::forward<Sender>(s), std::forward<Fn>(fn));
  }

  template<typename Fn>
  constexpr auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<typename Fn>
  class fn_wrapper {
    static_assert(!std::is_const_v<Fn> && !std::is_reference_v<Fn>);

    public:
    explicit fn_wrapper(Fn&& fn)
    : fn(std::move(fn))
    {}

    explicit fn_wrapper(const Fn& fn)
    : fn(fn)
    {}

    template<typename... Args>
    auto operator()([[maybe_unused]] set_error_t tag, Args&&... args) & -> decltype(auto) {
      return std::invoke(fn, std::forward<Args>(args)...);
    }

    template<typename... Args>
    auto operator()([[maybe_unused]] set_error_t tag, Args&&... args) const & -> decltype(auto) {
      return std::invoke(fn, std::forward<Args>(args)...);
    }

    template<typename... Args>
    auto operator()([[maybe_unused]] set_error_t tag, Args&&... args) && -> decltype(auto) {
      return std::invoke(std::move(fn), std::forward<Args>(args)...);
    }

    private:
    Fn fn;
  };

  template<sender Sender, typename Fn>
  constexpr auto default_impl(Sender&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _let_adapter_common_t::impl<set_error_t>(std::forward<Sender>(s), fn_wrapper(std::forward<Fn>(fn)));
  }
};
inline constexpr lazy_let_error_t lazy_let_error{};


// Create a let-error adapter.
// This adapter takes a function argument. When it receives the set_error-signal,
// it'll invoke that function. The function should return a sender.
// It'll then run attach that sender to the receiver.
//
// Note, the documentation says that the errors from the set_error signal
// are stored in the execution-state, ensuring its lifetime until the receiver completes.
// It also appears to suggest those errors are passed by lvalue-reference, instead of forwarded.
struct let_error_t {
  template<typename> friend struct _generic_operand_base_t;

  // When sender produces the set-error signal, invoke fn.
  // Fn will return a sender, and we'll run that next.
  template<sender Sender, typename Fn>
  constexpr auto operator()(Sender&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_error_t>(*this, std::forward<Sender>(s), std::forward<Fn>(fn));
  }

  template<typename Fn>
  constexpr auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<sender Sender, typename Fn>
  constexpr auto default_impl(Sender&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return lazy_let_error(std::forward<Sender>(s), std::forward<Fn>(fn));
  }
};
inline constexpr let_error_t let_error;


// Create a lazy-let-done adapter.
// This adapter takes a function argument. When it receives the set_done-signal,
// it'll invoke that function. The function should return a sender.
// It'll then run attach that sender to the receiver.
//
// A lazy adapter will never run before the operation change is started.
struct lazy_let_done_t {
  template<typename> friend struct _generic_operand_base_t;

  // When sender produces the set-done signal, invoke fn.
  // Fn will return a sender, and we'll run that next.
  template<sender Sender, typename Fn>
  constexpr auto operator()(Sender&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_done_t>(*this, std::forward<Sender>(s), std::forward<Fn>(fn));
  }

  template<typename Fn>
  constexpr auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<typename Fn>
  class fn_wrapper {
    static_assert(!std::is_const_v<Fn> && !std::is_reference_v<Fn>);

    public:
    explicit fn_wrapper(Fn&& fn)
    : fn(std::move(fn))
    {}

    explicit fn_wrapper(const Fn& fn)
    : fn(fn)
    {}

    template<typename... Args>
    auto operator()([[maybe_unused]] set_done_t tag, Args&&... args) & -> decltype(auto) {
      return std::invoke(fn, std::forward<Args>(args)...);
    }

    template<typename... Args>
    auto operator()([[maybe_unused]] set_done_t tag, Args&&... args) const & -> decltype(auto) {
      return std::invoke(fn, std::forward<Args>(args)...);
    }

    template<typename... Args>
    auto operator()([[maybe_unused]] set_done_t tag, Args&&... args) && -> decltype(auto) {
      return std::invoke(std::move(fn), std::forward<Args>(args)...);
    }

    private:
    Fn fn;
  };

  template<sender Sender, typename Fn>
  constexpr auto default_impl(Sender&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _let_adapter_common_t::impl<set_done_t>(std::forward<Sender>(s), fn_wrapper(std::forward<Fn>(fn)));
  }
};
inline constexpr lazy_let_done_t lazy_let_done{};


// Create a let-done adapter.
// This adapter takes a function argument. When it receives the set_done-signal,
// it'll invoke that function. The function should return a sender.
// It'll then run attach that sender to the receiver.
struct let_done_t {
  template<typename> friend struct _generic_operand_base_t;

  // When sender produces the set-done signal, invoke fn.
  // Fn will return a sender, and we'll run that next.
  template<sender Sender, std::invocable<> Fn>
  requires typed_sender<std::invoke_result_t<Fn>>
  constexpr auto operator()(Sender&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_done_t>(*this, std::forward<Sender>(s), std::forward<Fn>(fn));
  }

  template<std::invocable<> Fn>
  requires typed_sender<std::invoke_result_t<Fn>>
  constexpr auto operator()(Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Fn>(fn));
  }

  private:
  template<sender Sender, std::invocable<> Fn>
  requires typed_sender<std::invoke_result_t<Fn>>
  constexpr auto default_impl(Sender&& s, Fn&& fn) const
  -> sender decltype(auto) {
    return lazy_let_done(std::forward<Sender>(s), std::forward<Fn>(fn));
  }
};
inline constexpr let_done_t let_done;


// Create a bulk-execution adapter.
// This adapter takes a function argument, and a number N. When it receives the set_value-signal,
// it'll run the function N times, as if by `for (i=0; i<N; ++i) f(i, args...)'.
// Afterwards, it'll forward the orginal values.
//
// Note that the function will be invoked via non-const lvalue-reference.
// The args to the set-value signal will be forwarded to the function as by lvalue-reference.
// The index will be passed by const-reference.
//
// Lazy adapters never execute until start is called on the operation-state.
struct lazy_bulk_t {
  template<typename> friend struct _generic_operand_base_t;

  template<sender Sender, std::integral Shape, typename Fn>
  constexpr auto operator()(Sender&& s, Shape&& shape, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<Sender>(s), std::forward<Shape>(shape), std::forward<Fn>(fn));
  }

  template<std::integral Shape, typename Fn>
  constexpr auto operator()(Shape&& shape, Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Shape>(shape), std::forward<Fn>(fn));
  }

  private:
  // The receiver only intercepts the set_value signal.
  // When it is receiver, it'll do a for-loop on the arguments.
  // And then pass on the orginal arguments from set_value.
  template<receiver Receiver, std::integral Shape, typename Fn>
  class receiver_impl
  : public _generic_receiver_wrapper<Receiver, set_value_t>
  {
    public:
    receiver_impl(Receiver&& r, Shape&& shape, Fn&& fn)
    : _generic_receiver_wrapper<Receiver, set_value_t>(std::move(r)),
      shape(std::move(shape)),
      fn(std::move(fn))
    {}

    template<typename... Args>
    friend auto tag_invoke([[maybe_unused]] set_value_t, receiver_impl&& self, Args&&... args) -> void {
      for (Shape i = Shape(0); i < self.shape; ++i)
        std::invoke(self.fn, std::as_const(i), args...);
      set_value(std::move(self.r), std::forward<Args>(args)...);
    }

    private:
    [[no_unique_address]] Shape shape;
    [[no_unique_address]] Fn fn;
  };

  // The sender simply creates a receiver wrapper when invoked.
  template<sender Sender, std::integral Shape, typename Fn>
  class sender_impl
  : public _generic_sender_wrapper<sender_impl<Sender, Shape, Fn>, Sender, connect_t>
  {
    template<typename, sender, typename...> friend class ::earnest::execution::_generic_sender_wrapper;

    public:
    template<typename Sender_, typename Shape_, typename Fn_>
    requires std::constructible_from<Sender, Sender_> && std::constructible_from<Shape, Shape_> && std::constructible_from<Fn, Fn_>
    sender_impl(Sender_&& s, Shape_&& shape, Fn_&& fn)
    : _generic_sender_wrapper<sender_impl<Sender, Shape, Fn>, Sender, connect_t>(std::forward<Sender_>(s)),
      shape(std::forward<Shape_>(shape)),
      fn(std::forward<Fn_>(fn))
    {}

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> operation_state decltype(auto) {
      return ::earnest::execution::connect(
          std::move(self.s),
          receiver_impl<std::remove_cvref_t<Receiver>, Shape, Fn>(std::forward<Receiver>(r), std::move(self.shape), std::move(self.fn)));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    -> sender_impl<std::remove_cvref_t<OtherSender>, Shape, Fn> {
      return sender_impl<std::remove_cvref_t<OtherSender>, Shape, Fn>(std::forward<OtherSender>(other_sender), std::move(shape), std::move(fn));
    }

    [[no_unique_address]] Shape shape;
    [[no_unique_address]] Fn fn;
  };

  template<sender Sender, std::integral Shape, typename Fn>
  constexpr auto default_impl(Sender&& s, Shape&& shape, Fn&& fn) const
  -> sender_impl<std::remove_cvref_t<Sender>, std::remove_cvref_t<Shape>, std::remove_cvref_t<Fn>> {
    return sender_impl<std::remove_cvref_t<Sender>, std::remove_cvref_t<Shape>, std::remove_cvref_t<Fn>>(
        std::forward<Sender>(s), std::forward<Shape>(shape), std::forward<Fn>(fn));
  }
};
inline constexpr lazy_bulk_t lazy_bulk;


// Create a bulk-execution adapter.
// This adapter takes a function argument, and a number N. When it receives the set_value-signal,
// it'll run the function N times, as if by `for (i=0; i<N; ++i) f(i, args...)'.
// Afterwards, it'll forward the orginal values.
//
// Note that the function will be invoked via non-const lvalue-reference.
// The args to the set-value signal will be forwarded to the function as by lvalue-reference.
// The index will be passed by const-reference.
struct bulk_t {
  template<typename> friend struct _generic_operand_base_t;

  // When sender produces the set-done signal, invoke fn.
  // Fn will return a sender, and we'll run that next.
  template<sender Sender, std::integral Shape, typename Fn>
  constexpr auto operator()(Sender&& s, Shape&& shape, Fn&& fn) const
  -> sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<Sender>(s), std::forward<Shape>(shape), std::forward<Fn>(fn));
  }

  template<std::integral Shape, typename Fn>
  constexpr auto operator()(Shape&& shape, Fn&& fn) const
  -> decltype(auto) {
    return _generic_adapter(*this, std::forward<Shape>(shape), std::forward<Fn>(fn));
  }

  private:
  template<sender Sender, std::integral Shape, typename Fn>
  constexpr auto default_impl(Sender&& s, Shape&& shape, Fn&& fn) const
  -> sender decltype(auto) {
    return lazy_bulk(std::forward<Sender>(s), std::forward<Shape>(shape), std::forward<Fn>(fn));
  }
};
inline constexpr bulk_t bulk;


// Create the ensure_started adapter.
// This adapter starts a sender chain early, but still allows attaching receiver.
//
// There is no lazy version of ensure_started. Furthermore, an `ensure_started' adapter
// cannot be part of an incomplete adapter chain. (Because how would we start that?)
// Furthermore, `ensure_started' may cause the next operation in the chain to run
// either on the scheduler of the preceding chain, or inside the function context
// of the start operation. So you should treat it as a `| transfer(<undefined>)'.
//
// The implementation creates an intermediate_operation_state, which it holds on to
// using a shared_ptr. (The intermediate_operation_state derives from outcome_handler,
// and that's the type used in the chain.)
//
// The intermediate_operation_state holds on to
// - an `outcome_handler' which will hold on to the values until a receiver is attached,
// - a `Sender | ...'-operation state, which will deliver into the `outcome_handler'.
//
// When a receiver is attached and started, the `outcome_handler' is informed of its
// existence.
//
// The `outcome_handler' is a type that:
// - holds a variant of all the possible outcome types
// - has a (late-installed) next_receiver_fn, which will pick up the values in the variant
// - a lock, so that the attachment is safe across threads.
struct ensure_started_t {
  template<typename> friend struct _generic_operand_base_t;

  template<typed_sender Sender>
  constexpr auto operator()(Sender&& s) const
  -> typed_sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<Sender>(s));
  }

  constexpr auto operator()() const
  -> decltype(auto) {
    return _generic_adapter(*this);
  }

  private:
  // The outcome of the early-started sender.
  //
  // An outcome has an `void apply(Receiver&& r) noexcept' function that'll forward its values to a receiver.
  template<typename Signal, typename...> class outcome;
  // Outcome for the set-value signal.
  template<typename... T>
  class outcome<set_value_t, T...> {
    public:
    template<template<typename...> class Tuple> using value_types = _type_appender<Tuple<T...>>;
    using error_types = _type_appender<>;

    template<typename... T_>
    explicit outcome(T_&&... values)
    : values(std::forward<T_>(values)...)
    {}

    template<receiver_of<T...> Receiver>
    auto apply(Receiver&& r) noexcept -> void {
      try {
        std::apply(
            [&r]<typename... Args>(Args&&... args) {
              ::earnest::execution::set_value(std::forward<Receiver>(r), std::forward<Args>(args)...);
            },
            std::move(values));
      } catch (...) {
        ::earnest::execution::set_error(std::forward<Receiver>(r), std::current_exception());
      }
    }

    private:
    [[no_unique_address]] std::tuple<T...> values;
  };
  // Outcome for the set-error signal.
  template<typename Error>
  class outcome<set_error_t, Error> {
    public:
    template<template<typename...> class Tuple> using value_types = _type_appender<>;
    using error_types = _type_appender<Error>;

    template<typename Error_>
    explicit outcome(Error_&& error)
    : error(std::forward<Error_>(error))
    {}

    template<receiver<Error> Receiver>
    auto apply(Receiver&& r) noexcept -> void {
      ::earnest::execution::set_error(std::forward<Receiver>(r), std::move(error));
    }

    private:
    [[no_unique_address]] Error error;
  };

  // Helper type, that creates an outcome for a given signal.
  template<typename Signal>
  struct outcome_selector {
    // The actual application of outcome.
    // We strip away and const/volative/references, so that we have clean types
    // that we can assign to a tuple.
    template<typename... T>
    using type = outcome<Signal, std::remove_cvref_t<T>...>;
  };

  // Figure out all the outcomes that a given sender can produce.
  template<typed_sender Sender>
  struct all_outcomes_ {
    // Compute all the value-outcomes.
    // Yields `_type_appender<outcome<set_value_t, ...>, outcome<set_value_t, ...>, ...>'.
    using computed_value_outcomes = typename sender_traits<Sender>::template value_types<outcome_selector<set_value_t>::template type, _type_appender>;

    // Compute all the error-outcomes from the sender.
    // Yields `_type_appender<outcome<set_error_t, Error1>, outcome<set_error_t, Error2>, ...>'.
    using computed_error_outcomes_from_sender =
        typename sender_traits<Sender>::template error_types<_type_appender>:: // collect all errors (`_type_appender<Error1, Error2, ...>')
        template transform<outcome_selector<set_error_t>::template type>;      // convert each error-type into an error-outcome.

    // We have the std::exception_ptr error type,
    // in addition to all the error types from the sender type.
    using computed_error_outcomes =
        typename _type_appender<>::merge<                             // Union of outcome-for-exception_ptr, and outcomes-from-sender.
            _type_appender<outcome<set_error_t, std::exception_ptr>>, // (Note that we may have a duplicate, if sender advertised exception_ptr.
            computed_error_outcomes_from_sender                       //
        >::                                                           //
        template type<_deduplicate<_type_appender>::template type>;   // Deduplicate the error types into a _type_appender.

    // Compute the done-outcomes.
    // There are no done-outcomes, if the caller doesn't emit the done-signal.
    using computed_done_outcomes = std::conditional_t<
        sender_traits<Sender>::sends_done,
        _type_appender<outcome<set_done_t>>,
        _type_appender<>>;

    public:
    // Take the union of the different outcome types.
    //
    // We're not deduplicating any, because we're counting on the type-deduplication done by sender_traits.
    using types = typename _type_appender<>::merge<
        computed_value_outcomes,
        computed_error_outcomes,
        computed_done_outcomes>;
  };
  // Figure out all the outcomes that a given sender can produce.
  // Something of the form: `_type_appender<outcome<...>, outcome<...>, ...>'
  template<typed_sender Sender>
  using all_outcomes = typename all_outcomes_<Sender>::types;

  template<template<typename...> class Tuple>
  struct _select_value_types_from_outcome_ {
    template<typename Outcome>
    using type = typename Outcome::template value_types<Tuple>;
  };
  struct _select_error_types_from_outcome_ {
    template<typename Outcome>
    using type = typename Outcome::error_types;
  };

  // An outcome selector.
  //
  // AllOutcomes: the type `all_outcomes<Sender>'.
  // We provide value_types, error_types, and sends_done, so you can request sender_traits on this outcome_handler.
  //
  // The selector implements the lockable concept, and methods should be called with the lock held.
  template<typename AllOutcomes, bool SendsDone>
  class outcome_handler {
    private:
    using variant_type = typename _type_appender<std::monostate>::merge<AllOutcomes>::template type<std::variant>;
    using next_receiver_fn = void(void*, outcome_handler&) noexcept;

    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = typename AllOutcomes::
        template transform<_select_value_types_from_outcome_<Tuple>::template type>:: // Each outcome is converted to a collection of zero or one tuples
                                                                                      // which are then all bundled together in a collection:
                                                                                      // `_type_appender<
                                                                                      //   _type_appender<Tuple<...>>,
                                                                                      //   _type_appender<>,
                                                                                      //   ...>'
        template type<_type_appender<>::merge>::                                      // Squash the double collections down to a single collection
        template type<Variant>;                                                       // And then turn it into a variant.

    template<template<typename...> class Variant>
    using error_types = typename AllOutcomes::
        template transform<_select_error_types_from_outcome_::type>::                 // Similar to the value_types, each outcome is transformed
                                                                                      // into a collection of zero or one errors, and all that is
                                                                                      // wrapped in a collection:
                                                                                      // `_type_appender<
                                                                                      //   _type_appender<Error1>,
                                                                                      //   _type_appender<>,
                                                                                      //   ...>'
        template type<_type_appender<>::merge>::                                      // Squash the double collections down to a single collection
        template type<Variant>;                                                       // And then turn it into a variant.

    static inline constexpr bool sends_done = SendsDone;

    // Receiver that delivers its signals into a matching outcome_selector.
    class receiver_impl {
      public:
      explicit receiver_impl(std::shared_ptr<outcome_handler> handler) noexcept
      : handler(handler)
      {}

      // Handler of the done signal.
      template<typename... Args>
      friend auto tag_invoke([[maybe_unused]] set_value_t, receiver_impl&& self, Args&&... args) noexcept -> void {
        using outcome_type = outcome<set_value_t, std::remove_cvref_t<Args>...>;

        const auto handler = self.clear();
        std::lock_guard lck{*handler};

        assert(handler->impl.index() == 0 || handler->impl.valueless_by_exception());
        try {
          handler->impl.template emplace<outcome_type>(std::forward<Args>(args)...);
        } catch (...) {
          handler->impl.template emplace<outcome<set_error_t, std::exception_ptr>>(std::current_exception());
        }

        // Notify the handler.
        handler->notify();
      }

      // Handler of the error signal.
      template<typename Error>
      friend auto tag_invoke([[maybe_unused]] set_error_t, receiver_impl&& self, Error&& error) noexcept -> void {
        using outcome_type = outcome<set_error_t, std::remove_cvref_t<Error>>;

        const auto handler = self.clear();
        std::lock_guard lck{*handler};

        assert(handler->impl.index() == 0 || handler->impl.valueless_by_exception());
        handler->impl.template emplace<outcome_type>(std::forward<Error>(error));

        // Notify the handler.
        handler->notify();
      }

      // Handler for the done-signal.
      // If the sender advertises no sending of done, then this function will trip undefined behaviour
      // (in debug mode, we'll assert).
      friend auto tag_invoke([[maybe_unused]] set_done_t, receiver_impl&& self) noexcept -> void {
        using outcome_type = outcome<set_done_t>;

        const auto handler = self.clear();
        std::lock_guard lck{*handler};

        assert(handler->impl.index() == 0 || handler->impl.valueless_by_exception());
        assert(SendsDone); // If not advertised as sending a done-signal, we optimize out the done-outcome.
        if constexpr(SendsDone) handler->impl.template emplace<outcome_type>();

        // Notify the handler.
        handler->notify();
      }

      private:
      auto clear() noexcept -> std::shared_ptr<outcome_handler> {
        // We need to clear selector.
        //
        // But since this is a self-referential pointer, clearing the pointer will cause
        // this to be destroyed. I'm a little concerned if the final writes to `selector'
        // will happen before or after the destructor.
        //
        // So in order to be safe, I use exchange:
        // this way, `outcome_selector' will exist until after `selector = nullptr' has
        // completed.
        return std::exchange(handler, nullptr);
      }

      // The handler into which to install the values.
      // The handler pointer also holds a reference to the shared state (which contains this receiver).
      // So once the values have been set, the handler should be cleared, or a memory leak will result.
      std::shared_ptr<outcome_handler> handler;
    };

    constexpr outcome_handler() noexcept = default;

    // Move/copy are not permitted.
    outcome_handler(outcome_handler&&) = delete;
    outcome_handler(const outcome_handler&) = delete;
    outcome_handler& operator=(outcome_handler&&) = delete;
    outcome_handler& operator=(const outcome_handler&) = delete;

    // Propagate state to the receiver.
    //
    // Should be called as part of the `next_receiver' function.
    //
    // Don't acquire a lock on this: the `next_receiver' is always called with the lock held.
    template<receiver Receiver>
    auto apply(Receiver&& r) noexcept -> void {
      std::visit(
          [&](auto&& oc) {
            assert((!std::is_same_v<std::monostate, std::remove_cvref_t<decltype(oc)>>));

            if constexpr(!std::is_same_v<std::monostate, std::remove_cvref_t<decltype(oc)>>)
              oc.apply(std::forward<Receiver>(r));
          },
          std::move(impl));
    }

    // Install the next receiver.
    // Must be called with the handler lock held.
    template<receiver Receiver>
    auto install_next_receiver(Receiver* r) -> void {
      assert(next_receiver == nullptr && next_receiver_arg == nullptr);
      next_receiver_arg = r;
      next_receiver = [](void* void_r, outcome_handler& handler) noexcept {
        handler.apply(std::move(*static_cast<Receiver*>(void_r)));
      };
      notify();
    }

    // Mark the outcome as a failure.
    // Must be called with the handler lock held.
    auto fail(std::exception_ptr ex) noexcept -> void {
      impl.template emplace<outcome<set_error_t, std::exception_ptr>>(std::move(ex));
      notify();
    }

    // Allow the handler to be locked.
    auto lock() -> void {
      mtx.lock();
    }

    // Allow the handler to be unlocked.
    auto unlock() -> void {
      mtx.unlock();
    }

    private:
    // Notify a change in impl or next_receiver.
    //
    // If both impl and next_receiver are ready to propagate a signal,
    // the signal shall be propagated.
    auto notify() noexcept -> void {
      if (!std::holds_alternative<std::monostate>(impl) && next_receiver != nullptr)
        std::invoke(next_receiver, next_receiver_arg, *this);
    }

    std::mutex mtx;
    variant_type impl;

    // We use two pointers for the next-receiver invocation:
    // - next_receiver_arg: the void* for the receiver
    // - next_receiver_fn: a function which handles applying the outcome_handler to the receiver.
    //   The receiver is passed through as the void* argument.
    //
    // The reason for this is that we'll not need to run a constructor.
    // (Aka it's cheaper this way than using a move_only_function.)
    void* next_receiver_arg{nullptr};
    next_receiver_fn* next_receiver{nullptr};
  };

  // Hold on to the operation-state of the sender,
  // and to the outcomes.
  template<typed_sender Sender>
  class intermediate_operation_state
  : public std::enable_shared_from_this<intermediate_operation_state<Sender>>
  {
    private:
    using outcome_handler_type = outcome_handler<all_outcomes<Sender>, sender_traits<Sender>::sends_done>;
    using nested_operation_state_type = std::remove_cvref_t<decltype(::earnest::execution::connect(std::declval<Sender>(), std::declval<typename outcome_handler_type::receiver_impl>()))>;

    public:
    intermediate_operation_state() = default;

    // Operation state is not copyable/moveable.
    intermediate_operation_state(const intermediate_operation_state&) = delete;
    intermediate_operation_state(intermediate_operation_state&&) = delete;
    intermediate_operation_state& operator=(const intermediate_operation_state&) = delete;
    intermediate_operation_state& operator=(intermediate_operation_state&&) = delete;

    // Create the nested operation state and start execution.
    auto start(Sender&& s) noexcept -> void {
      try {
        assert(!nested_operation_state.has_value());
        receiver auto r = typename outcome_handler_type::receiver_impl(handler_ptr()); // Never throws
        nested_operation_state.emplace(std::forward<Sender>(s), std::move(r));
        ::earnest::execution::start(*nested_operation_state);
      } catch (...) {
        // The spec says we must pass on the exception to the next step in the chain,
        // by calling `set_error(r, current_exception())'.
        // This is the equivalent call.
        std::lock_guard lck{handler};
        handler.fail(std::current_exception());
      }
    }

    // Returns an alias pointer to the handler.
    auto handler_ptr() noexcept -> std::shared_ptr<outcome_handler_type> {
      return std::shared_ptr<outcome_handler_type>(this->shared_from_this(), &handler);
    }

    private:
    outcome_handler_type handler;
    optional_operation_state_<nested_operation_state_type> nested_operation_state;
  };

  // Create an operation state and handler.
  // Starts the operation state.
  // And returns only the handler.
  template<typed_sender Sender>
  static auto make_handler(Sender&& s)
  -> std::shared_ptr<outcome_handler<all_outcomes<std::remove_cvref_t<Sender>>, sender_traits<std::remove_cvref_t<Sender>>::sends_done>> {
    auto intermediate_opstate_ptr = std::make_shared<intermediate_operation_state<std::remove_cvref_t<Sender>>>();
    intermediate_opstate_ptr->start(std::forward<Sender>(s));
    return intermediate_opstate_ptr->handler_ptr();
  }

  // The operation state that we will expose.
  //
  // This operation state holds on to the outcome handler,
  // and will at some point, install the next_receiver.
  template<typename OutcomeHandlerType, receiver Receiver>
  class opstate
  : public operation_state_base_
  {
    public:
    explicit opstate(std::shared_ptr<OutcomeHandlerType>&& outcome_handler, Receiver&& r)
    : outcome_handler(std::move(outcome_handler)),
      next_receiver(std::move(r))
    {}

    friend auto tag_invoke([[maybe_unused]] start_t, opstate& self) noexcept -> void {
      { // Use a scope, to limit the life-time of the lock.
        std::lock_guard lck{*self.outcome_handler};
        self.outcome_handler->install_next_receiver(&self.next_receiver);
      }

      // The spec does not mandate we keep a hold of the execution state.
      // So... why not shed it right now, and allow the system to reclaim some memory?
      //
      // (Requires we don't hold the lock.)
      self.outcome_handler.reset();
    }

    private:
    std::shared_ptr<OutcomeHandlerType> outcome_handler;
    Receiver next_receiver;
  };

  // Implementation of the sender.
  //
  // Unlike regular sender-implementations, this time we don't wrap the nested sender.
  // This is because we have to pass it on, so it can be started, instead.
  // In addition, we couldn't forward any queries to that sender anyway:
  // - connect: we would have to override that anyway
  // - get_completion_scheduler: we cannot know if the next operation in the chain
  //   will execute on the scheduler of the nested Sender, or inside the function
  //   context of the calling function.
  //
  // As an advantage, this means we don't need to know anything about the sender except
  // the value_types/error_types/sends_done. And we can get those from the outcome_handler.
  // So we get a lot of type-erase.
  template<typename OutcomeHandlerType>
  class sender_impl {
    private:
    using outcome_handler_type = OutcomeHandlerType;

    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = typename outcome_handler_type::template value_types<Tuple, Variant>;

    template<template<typename...> class Variant>
    using error_types = typename outcome_handler_type::template error_types<Variant>;

    static inline constexpr bool sends_done = outcome_handler_type::sends_done;

    explicit sender_impl(std::shared_ptr<outcome_handler_type>&& handler)
    noexcept
    : handler(std::move(handler))
    {}

    sender_impl(sender_impl&&) noexcept = default;

    // Disallow copies, because we can only attach once.
    sender_impl(const sender_impl&) = delete;

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> opstate<outcome_handler_type, std::remove_cvref_t<Receiver>> {
      return opstate<outcome_handler_type, std::remove_cvref_t<Receiver>>(std::move(self.handler), std::forward<Receiver>(r));
    }

    private:
    std::shared_ptr<outcome_handler_type> handler;
  };

  // Creates the sender.
  //
  // Note that this is never a constexpr function,
  // because the `make_handler' function performs a heap allocation.
  template<typed_sender Sender>
  auto default_impl(Sender&& s) const
  -> typed_sender decltype(auto) {
    using outcome_handler_type = outcome_handler<all_outcomes<std::remove_cvref_t<Sender>>, sender_traits<std::remove_cvref_t<Sender>>::sends_done>;
    return sender_impl<outcome_handler_type>(make_handler(std::forward<Sender>(s)));
  }
};
inline constexpr ensure_started_t ensure_started{};

// Outcome for the set-done signal.
template<>
class ensure_started_t::outcome<set_done_t> {
  public:
  template<template<typename...> class Tuple> using value_types = _type_appender<>;
  using error_types = _type_appender<>;

  explicit outcome() noexcept {}

  template<receiver Receiver>
  auto apply(Receiver&& r) noexcept -> void {
    ::earnest::execution::set_done(std::forward<Receiver>(r));
  }
};


// The when-all adapter takes multiple senders, and concatenates their values.
//
// Note:
// - When zero senders are specified, it returns the `just()' sender.
//   I don't think that's in violation of the spec.
//   This makes implementing the receiver easier, because we don't have to care for the
//   "zero-out-of-zero ready".
// - When one sender is specified, it returns only that sender.
//   I don't think that's in violation of the spec, as the observed behaviour
//   of the pipeline is no different.
struct when_all_t {
  template<typename> friend struct _generic_operand_base_t;

  template<typed_sender... Sender>
  requires ((std::variant_size_v<typename sender_traits<Sender>::template value_types<std::tuple, std::variant>> == 1) &&...)
  constexpr auto operator()(Sender&&... s) const
  -> typed_sender decltype(auto) {
    return _generic_operand_base<>(*this, std::forward<Sender>(s)...);
  }

  private:
  // A local receiver is a receiver that'll accept the values for a single sender.
  //
  // It plays nice with `local_opstate', which'll provide the storage for its outcome.
  template<receiver_of<> Receiver, typename TupleType>
  class local_receiver_impl
  : public _generic_receiver_wrapper<Receiver, set_value_t>
  {
    public:
    explicit local_receiver_impl(Receiver&& r, std::optional<TupleType>* outcome)
    : _generic_receiver_wrapper<Receiver, set_value_t>(std::move(r)),
      outcome(outcome)
    {}

    // Only permit move construction.
    local_receiver_impl(local_receiver_impl&&) = default;
    local_receiver_impl(const local_receiver_impl&) = delete;

    // When we receive the value-signal:
    // - save the values in the opstate
    // - and then notify the wrapped receiver
    template<typename... Args>
    requires std::constructible_from<TupleType, Args...>
    friend auto tag_invoke([[maybe_unused]] set_value_t, local_receiver_impl&& self, Args&&... args)
    -> void {
      assert(!self.outcome->has_value());
      self.outcome->emplace(std::forward<Args>(args)...); // Save our value (may throw).
      ::earnest::execution::set_value(std::move(self.r)); // And invoke our wrapped sender (without arguments).
    }

    private:
    std::optional<TupleType>*const outcome;
  };

  // A local operation-state, that stores the value-signal of the Sender.
  template<typed_sender Sender, receiver_of<> Receiver, typename = void>
  class local_opstate
  : public operation_state_base_
  {
    public:
    using tuple_type = typename sender_traits<Sender>::template value_types<std::tuple, std::type_identity_t>;

    private:
    using receiver_impl = local_receiver_impl<Receiver, tuple_type>;
    using nested_opstate = decltype(::earnest::execution::connect(std::declval<Sender>(), std::declval<receiver_impl>()));

    public:
    explicit local_opstate(Sender&& s, Receiver&& r)
    : impl(::earnest::execution::connect(std::forward<Sender>(s), receiver_impl(std::move(r), &this->values)))
    {}

    // Initialize local-opstate from a tuple of sender and receiver.
    // This because we like to initialize tuples of these things,
    // and tuples don't have a `std::tuple(std::inplace, tuple-of-args...)'
    // style constructor.
    explicit local_opstate(std::tuple<Sender&&, Receiver&&> tpl)
    : local_opstate(std::get<0>(std::move(tpl)), std::get<1>(std::move(tpl)))
    {}

    friend auto tag_invoke([[maybe_unused]] start_t, local_opstate& self) noexcept -> void {
      ::earnest::execution::start(self.impl);
    }

    auto get_outcome() const & noexcept -> const tuple_type& {
      assert(values.has_value());
      return *values;
    }

    auto get_outcome() & noexcept -> tuple_type& {
      assert(values.has_value());
      return *values;
    }

    auto get_outcome() && noexcept -> tuple_type&& {
      assert(values.has_value());
      return *std::move(values);
    }

    private:
    std::optional<tuple_type> values;
    [[no_unique_address]] nested_opstate impl;
  };

  // A specialization for local_opstate, for when the sender sends zero values.
  //
  // In this case, we can cut out the outcome-tuple, and thus don't need to wrap a local_receiver_impl.
  template<typed_sender Sender, receiver_of<> Receiver>
  class local_opstate<
      Sender, Receiver,
      std::enable_if_t<std::is_same_v<
          std::variant<std::tuple<>>,
          typename sender_traits<Sender>::template value_types<std::tuple, std::variant>>>>
  : public operation_state_base_
  {
    public:
    using tuple_type = std::tuple<>;

    private:
    using receiver_impl = Receiver;
    using nested_opstate = decltype(::earnest::execution::connect(std::declval<Sender>(), std::declval<receiver_impl>()));

    public:
    explicit local_opstate(Sender&& s, Receiver&& r)
    : impl(::earnest::execution::connect(std::forward<Sender>(s), std::move(r)))
    {}

    // Initialize local-opstate from a tuple of sender and receiver.
    // This because we like to initialize tuples of these things,
    // and tuples don't have a `std::tuple(std::inplace, tuple-of-args...)'
    // style constructor.
    explicit local_opstate(std::tuple<Sender&&, Receiver&&> tpl)
    : local_opstate(std::get<0>(std::move(tpl)), std::get<1>(std::move(tpl)))
    {}

    friend auto tag_invoke([[maybe_unused]] start_t, local_opstate& self) noexcept -> void {
      ::earnest::execution::start(self.impl);
    }

    constexpr auto get_outcome() const noexcept -> tuple_type {
      return {};
    }

    private:
    [[no_unique_address]] nested_opstate impl;
  };

  // A functor, which, when invoked, will request-stop on a stop-source.
  struct cascade_stop {
    public:
    explicit cascade_stop(in_place_stop_source& stop_source) noexcept
    : stop_source(stop_source)
    {}

    auto operator()() const noexcept -> void {
      stop_source.request_stop();
    }

    private:
    in_place_stop_source& stop_source;
  };

  // Summary that indicates all senders completed with a value-signal.
  class value_signal_summary {
    public:
    template<typename... LocalOpstates, receiver Receiver>
    auto apply(std::tuple<LocalOpstates...>&& local_states, Receiver&& r) noexcept -> void {
      try {
        apply_(std::index_sequence_for<LocalOpstates...>(), std::move(local_states), std::forward<Receiver>(r));
      } catch (...) {
        ::earnest::execution::set_error(std::forward<Receiver>(r), std::current_exception());
      }
    }

    private:
    template<std::size_t... Idx, typename... LocalOpstates, receiver Receiver>
    auto apply_([[maybe_unused]] std::index_sequence<Idx...>, std::tuple<LocalOpstates...>&& local_states, Receiver&& r)
    -> void {
      std::apply(
          [&r](auto&&... args) {
            ::earnest::execution::set_value(std::forward<Receiver>(r), std::move(args)...);
          },
          // All the values in the outcomes, by rvalue-reference, in one giant tuple.
          std::tuple_cat(tuple_for_forwarding(std::get<Idx>(std::move(local_states)).get_outcome())...));
    }

    // Turn a tuple of values into a tuple of rvalue-references.
    // We need rvalue-references, so we won't make copies (or moves) when we call tuple-cat.
    template<typename... T>
    static auto tuple_for_forwarding(std::tuple<T...>&& tpl) noexcept -> std::tuple<std::add_rvalue_reference_t<T>...> {
      return std::apply(
          [](std::add_rvalue_reference_t<T>... args) -> std::tuple<std::add_rvalue_reference_t<T>...> {
            return std::tuple<std::add_rvalue_reference_t<T>...>(std::forward<std::add_rvalue_reference_t<T>>(args)...);
          },
          std::move(tpl));
    }
  };

  // Summary that indicates at least one sender completed with an error-signal.
  template<typename Error>
  class error_signal_summary {
    public:
    explicit error_signal_summary(const Error& error)
    : error(error)
    {}

    explicit error_signal_summary(Error&& error)
    : error(std::move(error))
    {}

    template<typename... LocalOpstates, receiver Receiver>
    auto apply([[maybe_unused]] std::tuple<LocalOpstates...>&&, Receiver&& r) noexcept -> void {
      ::earnest::execution::set_error(std::forward<Receiver>(r), std::move(error));
    }

    private:
    Error error;
  };

  // Summary that indicates at least one sender completed with a done-signal.
  class done_signal_summary {
    public:
    template<typename... LocalOpstates, receiver Receiver>
    auto apply([[maybe_unused]] std::tuple<LocalOpstates...>&&, Receiver&& r) noexcept -> void {
      ::earnest::execution::set_done(std::forward<Receiver>(r));
    }
  };

  // The operation state for when-all, has a few things in common.
  // We split them off into a basic_opstate.
  template<bool SendsDone, typename... Error>
  class basic_opstate {
    private:
    // Summaries describe how the when-all operation completed.
    // - value_signal_summary: used to indicate that all senders completed with a value-signal
    // - error_signal_summary: used to indicate that a sender completed with an error-signal
    // - done_signal_summary: used to indicate that a sender completed with a done-signal
    using summary_type =
        typename _type_appender<>::merge<                                                        // The union of
            _type_appender<value_signal_summary>,                                                // the value-summary
            _type_appender<error_signal_summary<Error>...>,                                      // each of the error-summaries,
            std::conditional_t<SendsDone, _type_appender<done_signal_summary>, _type_appender<>> // and the done-summary (iff sends-done)
        >::                                                                                      //
        template type<std::variant>;                                                             // combined into a variant.

    protected:
    // Implementation of the receiver.
    // This receiver implementation accesses basic_opstate, through opstate.
    //
    // Note: we have to use `typename' here, because OpState cannot become a complete type
    // until it's figured out receiver_impl type.
    template<receiver Receiver, typename OpState>
    class receiver_impl {
      public:
      explicit receiver_impl(OpState& state) noexcept
      : state(state)
      {}

      receiver_impl(const receiver_impl&) = delete;
      receiver_impl(receiver_impl&&) = default;

      // Accept the value-signal.
      friend auto tag_invoke([[maybe_unused]] set_value_t, receiver_impl&& self) noexcept -> void {
        if (self.state.basic_opstate::on_value_completion_())
          self.state.notify();
      }

      // Accept the error-signal.
      // Updates state with an error-summary.
      template<typename ErrorArgument>
      friend auto tag_invoke([[maybe_unused]] set_error_t, receiver_impl&& self, ErrorArgument&& error) noexcept -> void {
        if (self.state.basic_opstate::on_error_completion_(std::forward<ErrorArgument>(error)))
          self.state.notify();
      }

      // Accept the done-signal.
      // Updates state with a done-summary.
      friend auto tag_invoke([[maybe_unused]] set_done_t, receiver_impl&& self) noexcept -> void {
        if (self.state.basic_opstate::on_done_completion_())
          self.state.notify();
      }

      // Expose the local cancelation state.
      friend auto tag_invoke([[maybe_unused]] get_stop_token_t, const receiver_impl& self) noexcept -> in_place_stop_token {
        return self.state.basic_opstate::local_cancelation.get_token();
      }

      // Any other receiver queries are forwarded.
      //
      // This tag_invoke will be used when receiver_impl is a const-reference.
      template<typename Tag, typename... Args,
          typename = std::enable_if_t<
              _is_forwardable_receiver_tag<Tag> &&
              std::negation_v<
                  std::disjunction<
                      std::is_same<set_value_t, Tag>,
                      std::is_same<set_error_t, Tag>,
                      std::is_same<set_done_t, Tag>,
                      std::is_same<get_stop_token_t, Tag>>>>>
      friend auto tag_invoke(Tag tag, const receiver_impl& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, const Receiver&, Args...>)
      -> tag_invoke_result_t<Tag, const Receiver&, Args...> {
        return ::earnest::execution::tag_invoke(std::move(tag), std::as_const(self.state.r), std::forward<Args>(args)...);
      }

      // Any other receiver queries are forwarded.
      //
      // This tag_invoke will be used when receiver_impl is a non-const-reference.
      template<typename Tag, typename... Args,
          typename = std::enable_if_t<
              _is_forwardable_receiver_tag<Tag> &&
              std::negation_v<
                  std::disjunction<
                      std::is_same<set_value_t, Tag>,
                      std::is_same<set_error_t, Tag>,
                      std::is_same<set_done_t, Tag>,
                      std::is_same<get_stop_token_t, Tag>>>>>
      friend auto tag_invoke(Tag tag, receiver_impl& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, Receiver&, Args...>)
      -> tag_invoke_result_t<Tag, Receiver&, Args...> {
        return ::earnest::execution::tag_invoke(std::move(tag), self.state.r, std::forward<Args>(args)...);
      }

      // Any other receiver queries are forwarded.
      //
      // This tag_invoke will be used when receiver_impl is an rvalue-reference.
      template<typename Tag, typename... Args,
          typename = std::enable_if_t<
              _is_forwardable_receiver_tag<Tag> &&
              std::negation_v<
                  std::disjunction<
                      std::is_same<set_value_t, Tag>,
                      std::is_same<set_error_t, Tag>,
                      std::is_same<set_done_t, Tag>,
                      std::is_same<get_stop_token_t, Tag>>>>>
      friend auto tag_invoke(Tag tag, receiver_impl&& self, Args&&... args)
      noexcept(nothrow_tag_invocable<Tag, Receiver&&, Args...>)
      -> tag_invoke_result_t<Tag, Receiver&&, Args...> {
        return ::earnest::execution::tag_invoke(std::move(tag), std::move(self.state.r), std::forward<Args>(args)...);
      }

      private:
      OpState& state;
    };

    protected:
    explicit basic_opstate(std::size_t pending) noexcept
    : pending(pending)
    {}

    public:
    basic_opstate(basic_opstate&&) = delete;
    basic_opstate(const basic_opstate&) = delete;

    protected:
    ~basic_opstate() = default;

    private:
    // Internal implementation of on_value_completion.
    // Updates internal structures.
    // Returns true if all the local opstates have completed.
    //
    // This function holds the lock, so it may not call notify.
    [[nodiscard]]
    auto on_value_completion_() noexcept -> bool {
      std::lock_guard lck{mtx};
      return --pending == 0;
    }

    // Internal implementation of on_error_completion.
    // Updates internal structures.
    // Returns true if all the local opstates have completed.
    //
    // This function holds the lock, so it may not call notify.
    template<typename ErrorArgument>
    [[nodiscard]]
    auto on_error_completion_(ErrorArgument&& error) noexcept -> bool {
      std::lock_guard lck{mtx};

      // Only update the state, if this is the first non-value-signal we encounter.
      if (std::holds_alternative<value_signal_summary>(summary))
        summary.template emplace<error_signal_summary<std::remove_cvref_t<ErrorArgument>>>(std::forward<ErrorArgument>(error));
      // Signal any in-progress senders that they can abort early (since we'll discard the value/error it produces anyway).
      local_cancelation.request_stop();

      return --pending == 0;
    }

    // Internal implementation of on_done_completion.
    // Updates internal structures.
    // Returns true if all the local opstates have completed.
    //
    // This function holds the lock, so it may not call notify.
    [[nodiscard]]
    auto on_done_completion_() noexcept -> bool {
      std::lock_guard lck{mtx};

      assert(SendsDone);
      if constexpr(SendsDone) {
        // Only update the state, if this is the first non-value-signal we encounter.
        if (std::holds_alternative<value_signal_summary>(summary))
          summary.template emplace<done_signal_summary>();
        // Signal any in-progress senders that they can abort early (since we'll discard the value/error it produces anyway).
        local_cancelation.request_stop();
      }

      return --pending == 0;
    }

    protected:
    template<typename LocalStates, receiver Receiver>
    auto deliver_signal(LocalStates&& local_states, Receiver&& r) noexcept {
#ifndef NDEBUG
      {
        std::lock_guard lck{mtx};
        assert(pending == 0);
      }
#endif

      std::visit(
          [&local_states, &r](auto& summary) {
            summary.apply(std::forward<LocalStates>(local_states), std::forward<Receiver>(r));
          },
          summary);
    }

    private:
    std::mutex mtx; // Protect updating local structures.
    std::size_t pending;

    // Summarizes how the local-opstates completed.
    //
    // By default, we expect to pass on a value-signal.
    // But if any of the local-opstates emits a different signal, it'll update the summary.
    // (The summary is only updated for the first non-value signal.)
    std::variant<value_signal_summary, error_signal_summary<Error>..., done_signal_summary> summary;

    protected:
    in_place_stop_source local_cancelation; // We cancel all the invocations, if we receive a done-signal or error-signal.
  };

  // Helper type, that computes
  // `_type_appender<ErrorsForFirstSender..., ErrorsForSecondSender, ...>'
  // with any duplicates removed.
  template<typed_sender... Sender>
  using compute_error_types_for_senders =
      typename _type_appender<>::merge<
          _type_appender<std::exception_ptr>,
          typename sender_traits<Sender>::template error_types<_type_appender>... // create `_type_appender<Error1, Error2, ...>' per sender
      >::                                                                         // and merge them all together into a single _type_appender
      template type<_deduplicate<_type_appender>::template type>;                 // and then remove any duplicates

  // Helper type for basic_opstate_for_senders.
  template<bool SendsDone>
  struct basic_opstate_for_senders_ {
    template<typename... Error>
    using type = basic_opstate<SendsDone, Error...>;
  };
  // The `basic_opstate<SendsDone, Error...>' type, filled in correctly for the given Senders.
  template<typed_sender... Sender>
  using basic_opstate_for_senders = typename compute_error_types_for_senders<Sender...>::template type<
      basic_opstate_for_senders_<(sender_traits<Sender>::sends_done ||...)>::template type>;

  template<receiver Receiver, typed_sender... Sender>
  class opstate
  : private basic_opstate_for_senders<Sender...>,
    public operation_state_base_
  {
    template<bool, typename...> friend class basic_opstate;

    private:
    // Figure out the stop-token-type for the receiver.
    using receiver_stop_token_type = std::remove_cvref_t<decltype(::earnest::execution::get_stop_token(std::declval<Receiver>()))>;
    // Local senders are completing using the local-receiver-type.
    using local_receiver_type = typename basic_opstate_for_senders<Sender...>::template receiver_impl<Receiver, opstate>;

    public:
    opstate(Receiver&& r, Sender&&... s)
    : basic_opstate_for_senders<Sender...>(sizeof...(Sender)),
      r(std::move(r)),
      local_states(
          std::forward_as_tuple(std::move(s), local_receiver_type(*this))...
          ),
      cancelation_cascader(::earnest::execution::get_stop_token(this->r), this->local_cancelation)
    {}

    // Prevent copy/move, so we can rely on our pointer-identities.
    opstate(const opstate&) = delete;
    opstate(opstate&&) = delete;

    friend auto tag_invoke([[maybe_unused]] start_t, opstate& self) noexcept -> void {
      self.start_all_states_(std::index_sequence_for<Sender...>());
    }

    private:
    template<std::size_t Idx, std::size_t... Tail>
    auto start_all_states_([[maybe_unused]] std::index_sequence<Idx, Tail...>) noexcept -> void {
      ::earnest::execution::start(std::get<Idx>(local_states));
      start_all_states_(std::index_sequence<Tail...>());
    }

    auto start_all_states_([[maybe_unused]] std::index_sequence<>) noexcept -> void {
      // Nothing to do
    }

    auto notify() noexcept -> void {
      this->basic_opstate_for_senders<Sender...>::deliver_signal(std::move(local_states), std::move(r));
    }

    Receiver r;
    std::tuple<local_opstate<Sender, local_receiver_type>...> local_states; // Operation state for each of the senders.

    [[no_unique_address]]
    [[maybe_unused]]
    typename receiver_stop_token_type::template callback_type<cascade_stop> cancelation_cascader; // Glue, so that the Receiver will cascade
                                                                                                  // its stop-state into our local_cancelation.
  };

  template<typed_sender... Sender>
  class sender_impl {
    public:
    // The value-types is the concatenation of each of the value-types:
    // `Variant<Tuple<TypesOfFirstSender..., TypesOfSecondSender..., ...>'
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types =
        Variant<
            typename _type_appender<>::merge<
                typename sender_traits<Sender>::template value_types<_type_appender, _type_appender>... // create
                                                                                                        // `_type_appender<
                                                                                                        //     _type_appender<T0, T1, ...>,
                                                                                                        //     ...>'
                                                                                                        // per sender
            >::                                                                                         // and merge them all together into
                                                                                                        // a single _type_appender
            template type<_type_appender<>::merge>::                                                    // concatenate all the type-appenders
                                                                                                        // `_type_appender<
                                                                                                        //     TypesOfFirstSender...,
                                                                                                        //     TypesOfSecondSender...,
                                                                                                        //     ...>'
            template type<Tuple>>;                                                                      // and then apply the tuple (and everything
                                                                                                        // is wrapped inside the one Variant)

    // The error-types is the union of the error-types of each sender.
    template<template<typename...> class Variant>
    using error_types = typename compute_error_types_for_senders<Sender...>::template type<Variant>;

    // We send a done-signal, if any of our senders sends one.
    static inline constexpr bool sends_done = (sender_traits<Sender>::sends_done ||...);

    template<typename... Sender_>
    explicit constexpr sender_impl(Sender_&&... s)
    : senders(std::forward<Sender_>(s)...)
    {}

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> operation_state decltype(auto) {
      return std::apply(
          [&](auto&&... s) {
            return opstate<std::remove_cvref_t<Receiver>, Sender...>(std::forward<Receiver>(r), std::move(s)...);
          },
          std::move(self.senders));
    }

    private:
    std::tuple<Sender...> senders;
  };

  // When at least two senders are specified, we'll do the work of concatenating them
  // into the when-all operation.
  template<typed_sender... Sender, typename = std::enable_if_t<(sizeof...(Sender) >= 2)>>
  requires ((std::variant_size_v<typename sender_traits<Sender>::template value_types<std::tuple, std::variant>> == 1) &&...)
  constexpr auto default_impl(Sender&&... s) const
  -> sender_impl<std::remove_cvref_t<Sender>...> {
    return sender_impl<std::remove_cvref_t<Sender>...>(std::forward<Sender>(s)...);
  }

  // If no senders are specified, we'll return the `just()' sender.
  // Having the zero-senders case covered, makes implementing the when_all operation a little easier.
  auto default_impl() const noexcept -> typed_sender decltype(auto) {
    return just();
  }

  // If there is one sender specified, when-all is just a very expensive way of creating an identity-operation.
  // In this case, we can just forward the sender back, and spare the compiler (and runtime) some work.
  template<typed_sender Sender>
  constexpr auto default_impl(Sender&& s) noexcept -> std::add_rvalue_reference_t<Sender> {
    return std::forward<Sender>(s);
  }
};
inline constexpr when_all_t when_all{};


// The when-all-with-variant adapter, takes each sender it is given,
// squashes each of their value-types into a `std::variant<std::tuple<...>, ...>'
// and then does the same as when_all.
//
// The default-implementation is `when_all(into_variant(s1), into_variant(s2), ...)'.
struct when_all_with_variant_t {
  template<typename> friend struct _generic_operand_base_t;

  template<typed_sender... Sender>
  constexpr auto operator()(Sender&&... s) const
  -> typed_sender decltype(auto) {
    return _generic_operand_base<>(*this, std::forward<Sender>(s)...);
  }

  private:
  template<typed_sender... Sender>
  constexpr auto default_impl(Sender&&... s) const
  -> typed_sender decltype(auto) {
    return ::earnest::execution::when_all(::earnest::execution::into_variant(std::forward<Sender>(s))...);
  }
};
inline constexpr when_all_with_variant_t when_all_with_variant{};


// A when-all, that transfers to a specific scheduler upon completion.
struct transfer_when_all_t {
  template<typename> friend struct _generic_operand_base_t;

  template<scheduler Scheduler, typed_sender... Sender>
  constexpr auto operator()(Scheduler&& sch, Sender&&... s) const
  -> typed_sender decltype(auto) {
    return _generic_operand_base<>(*this, std::forward<Scheduler>(sch), std::forward<Sender>(s)...);
  }

  private:
  template<scheduler Scheduler, typed_sender... Sender>
  constexpr auto default_impl(Scheduler&& sch, Sender&&... s) const
  -> typed_sender decltype(auto) {
    return ::earnest::execution::transfer(
        ::earnest::execution::when_all(std::forward<Sender>(s)...),
        std::forward<Scheduler>(sch));
  }
};
inline constexpr transfer_when_all_t transfer_when_all{};


// A when-all, that lazily transfers to a specific scheduler upon completion.
struct lazy_transfer_when_all_t {
  template<typename> friend struct _generic_operand_base_t;

  template<scheduler Scheduler, typed_sender... Sender>
  constexpr auto operator()(Scheduler&& sch, Sender&&... s) const
  -> typed_sender decltype(auto) {
    return _generic_operand_base<>(*this, std::forward<Scheduler>(sch), std::forward<Sender>(s)...);
  }

  private:
  template<scheduler Scheduler, typed_sender... Sender>
  constexpr auto default_impl(Scheduler&& sch, Sender&&... s) const
  -> typed_sender decltype(auto) {
    return ::earnest::execution::lazy_transfer(
        ::earnest::execution::when_all(std::forward<Sender>(s)...),
        std::forward<Scheduler>(sch));
  }
};
inline constexpr lazy_transfer_when_all_t lazy_transfer_when_all{};


// A when-all-with-variant, that transfers to a specific scheduler upon completion.
struct transfer_when_all_with_variant_t {
  template<typename> friend struct _generic_operand_base_t;

  template<scheduler Scheduler, typed_sender... Sender>
  constexpr auto operator()(Scheduler&& sch, Sender&&... s) const
  -> typed_sender decltype(auto) {
    return _generic_operand_base<>(*this, std::forward<Scheduler>(sch), std::forward<Sender>(s)...);
  }

  private:
  template<scheduler Scheduler, typed_sender... Sender>
  constexpr auto default_impl(Scheduler&& sch, Sender&&... s) const
  -> typed_sender decltype(auto) {
    return ::earnest::execution::transfer(
        ::earnest::execution::when_all(::earnest::execution::into_variant(std::forward<Sender>(s))...),
        std::forward<Scheduler>(sch));
  }
};
inline constexpr transfer_when_all_with_variant_t transfer_when_all_with_variant{};


// A when-all-with-variant, that lazily transfers to a specific scheduler upon completion.
struct lazy_transfer_when_all_with_variant_t {
  template<typename> friend struct _generic_operand_base_t;

  template<scheduler Scheduler, typed_sender... Sender>
  constexpr auto operator()(Scheduler&& sch, Sender&&... s) const
  -> typed_sender decltype(auto) {
    return _generic_operand_base<>(*this, std::forward<Scheduler>(sch), std::forward<Sender>(s)...);
  }

  private:
  template<scheduler Scheduler, typed_sender... Sender>
  constexpr auto default_impl(Scheduler&& sch, Sender&&... s) const
  -> typed_sender decltype(auto) {
    return ::earnest::execution::lazy_transfer(
        ::earnest::execution::when_all(::earnest::execution::into_variant(std::forward<Sender>(s))...),
        std::forward<Scheduler>(sch));
  }
};
inline constexpr lazy_transfer_when_all_with_variant_t lazy_transfer_when_all_with_variant{};


// Split a sender, allowing for multiple chains to be attached, all sharing the same
// chain leading up to the split.
struct lazy_split_t {
  template<typename> friend struct _generic_operand_base_t;

  template<typed_sender Sender>
  constexpr auto operator()(Sender&& s) const
  -> typed_sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<Sender>(s));
  }

  private:
  template<typename... Args>
  class value_outcome {
    public:
    template<typename... Args_>
    explicit constexpr value_outcome(Args_&&... args)
    : values(std::forward<Args_>(args)...)
    {}

    template<receiver Receiver>
    auto apply(Receiver&& r) const noexcept -> void {
      std::apply(
          [&r](const auto&... args) -> void {
            try {
              ::earnest::execution::set_value(std::forward<Receiver>(r), args...);
            } catch (...) {
              ::earnest::execution::set_error(std::forward<Receiver>(r), std::current_exception());
            }
          },
          values);
    }

    private:
    std::tuple<Args...> values;
  };

  template<typename Error>
  class error_outcome {
    public:
    explicit constexpr error_outcome(Error&& error) noexcept(std::is_nothrow_move_constructible_v<Error>)
    : error(std::move(error))
    {}

    explicit constexpr error_outcome(const Error& error) noexcept(std::is_nothrow_copy_constructible_v<Error>)
    : error(error)
    {}

    template<receiver Receiver>
    auto apply(Receiver&& r) const noexcept -> void {
      ::earnest::execution::set_error(std::forward<Receiver>(r), error);
    }

    private:
    Error error;
  };

  class done_outcome {
    public:
    constexpr done_outcome() noexcept = default;

    template<receiver Receiver>
    auto apply(Receiver&& r) const noexcept -> void {
      ::earnest::execution::set_done(std::forward<Receiver>(r));
    }
  };

  // Helper type to compute all the outcome-types for this sender.
  template<typed_sender Sender>
  struct outcome_types_ {
    private:
    // Create a value_outcome for given types.
    template<typename... T>
    using computed_value_type_ = value_outcome<std::remove_cvref_t<T>...>;
    // Figure out all the value_outcomes we need.
    using computed_value_types = typename sender_traits<Sender>::template value_types<computed_value_type_, _type_appender>;

    // Create an error_outcome for given error type.
    template<typename... T>
    using computed_error_type_ = error_outcome<std::remove_cvref_t<T>...>;
    // Figure out all the error_outcomes we need.
    using computed_error_types = typename sender_traits<Sender>::template error_types<_type_appender>::template transform<computed_error_type_>;

    // Figure out all the done_outcomes we need.
    using computed_done_types = std::conditional_t<sender_traits<Sender>::sends_done, _type_appender<done_outcome>, _type_appender<>>;

    public:
    // Create a type-appender containing all the required outcome-types.
    using type = typename _type_appender<>::merge<                  // We want the union of
            computed_value_types,                                   // all value-outcome types
            computed_error_types,                                   // all error-outcome types
            computed_done_types                                     // and the done-outcome type (if we need one)
        >::                                                         //
        template type<_deduplicate<_type_appender>::template type>; // and remove any duplicates.
  };
  // Collect all the outcome-types we need.
  template<typed_sender Sender>
  using outcome_types = typename outcome_types_<Sender>::type;

  // A marker type, to indicate the operation has not been started.
  // We use a specific type, so we can encode it inside the shared_upstate_outcomes variant,
  // without requiring an extra 4+ bytes of memory.
  struct unstarted {};
  // A marker type, to indicate the operation has started.
  // We use a specific type, so we can encode it inside the shared_upstate_outcomes variant,
  // without requiring an extra 4+ bytes of memory.
  struct started {};

  // We use embedded double-linked-list, that way, we can link (and unlink)
  // without requiring additional memory (and thus also without additional
  // exceptions).
  struct opstate_link {
    using callback_fn = void(opstate_link&) noexcept;

    protected:
    constexpr opstate_link(callback_fn* callback) noexcept
    : callback(callback)
    {}

#ifdef NDEBUG
    ~opstate_link() {
      // Confirm we aren't linked.
      assert(succ == nullptr && pred == nullptr);
    }
#else
    ~opstate_link() = default;
#endif

    public:
    opstate_link* succ = nullptr;
    opstate_link* pred = nullptr;
    callback_fn* const callback;
  };

  // The outcomes from the sender chain.
  template<bool SendsDone, typename OutcomeTypes>
  class shared_opstate_outcomes {
    public:
    // Variant with all the outcome types.
    // The first type is `unstarted', and it gets initialized to that.
    //
    // The `unstarted' and `started' marker type are used to indicate what state
    // the system is in. Both indicate that there is no outcome available.
    using variant_type = typename _type_appender<unstarted, started>::merge<OutcomeTypes>::template type<std::variant>;

    // Implementation of the receiver that'll populate this opstate.
    class receiver_impl {
      public:
      receiver_impl(shared_opstate_outcomes& state) noexcept
      : state(state)
      {}

      // Consume the value-signal.
      template<typename... Args>
      friend auto tag_invoke([[maybe_unused]] set_value_t, receiver_impl&& self, Args&&... args) noexcept -> void {
        using outcome_type = value_outcome<std::remove_cvref_t<Args>...>;

        {
          std::lock_guard lck{self.state.mtx};
          try {
            self.state.outcome.template emplace<outcome_type>(std::forward<Args>(args)...);
          } catch (...) {
            self.state.outcome.template emplace<error_outcome<std::exception_ptr>>(std::current_exception());
          }
        }

        self.state.notify();
      }

      // Consume the error-signal.
      template<typename Error>
      friend auto tag_invoke([[maybe_unused]] set_error_t, receiver_impl&& self, Error&& error) noexcept -> void {
        using outcome_type = error_outcome<std::remove_cvref_t<Error>>;

        {
          std::lock_guard lck{self.state.mtx};
          if constexpr(std::same_as<std::remove_cvref_t<Error>, std::exception_ptr>) {
            self.state.outcome.template emplace<outcome_type>(std::forward<Error>(error));
          } else {
            try {
              self.state.outcome.template emplace<outcome_type>(std::forward<Error>(error));
            } catch (...) {
              self.state.outcome.template emplace<error_outcome<std::exception_ptr>>(std::current_exception());
            }
          }
        }

        self.state.notify();
      }

      // Consume the done-signal.
      friend auto tag_invoke([[maybe_unused]] set_done_t, receiver_impl&& self) noexcept -> void {
        using outcome_type = done_outcome;

        {
          std::lock_guard lck{self.state.mtx};
          assert(SendsDone);
          if constexpr(SendsDone) self.state.outcome.template emplace<outcome_type>();
        }

        // Notify any queued operations.
        // Must be run outside the lock.
        self.state.notify();
      }

      private:
      shared_opstate_outcomes& state;
    };

    protected:
    shared_opstate_outcomes() = default;

    // Prevent copy/move (so that pointer can be used).
    shared_opstate_outcomes(const shared_opstate_outcomes&) = delete;
    shared_opstate_outcomes(shared_opstate_outcomes&&) = delete;
    shared_opstate_outcomes& operator=(const shared_opstate_outcomes&) = delete;
    shared_opstate_outcomes& operator=(shared_opstate_outcomes&&) = delete;

    ~shared_opstate_outcomes() = default;

    public:
    // Apply the outcome to a receiver.
    //
    // The outcome must have been populated.
    // (If the outcome hasn't been populated, we fail the call.)
    //
    // You should never call this with the lock held:
    // the completion of the receiver may destroy *this.
    template<receiver Receiver>
    auto apply(Receiver&& r) const noexcept -> void {
#ifndef NDEBUG
      {
        std::lock_guard lck{mtx};
        assert(!std::holds_alternative<unstarted>(outcome) &&
            !std::holds_alternative<started>(outcome));
      }
#endif
      std::visit(
          [&r](const auto& oc) noexcept {
            if constexpr(std::is_same_v<std::remove_cvref_t<decltype(oc)>, unstarted>) {
              // Skip, should never happen.
            } else if constexpr(std::is_same_v<std::remove_cvref_t<decltype(oc)>, started>) {
              // Skip, should never happen.
            } else {
              oc.apply(std::forward<Receiver>(r));
            }
          },
          outcome);
    }

    // Link an opstate to the completion of this.
    //
    // If the operation-state has already completed,
    // it'll invoke the link immediately.
    // Otherwise, it'll link it into the list of pending-completions,
    // which will be invoked once the state ends.
    auto attach(opstate_link& link) noexcept -> void {
      {
        std::lock_guard lck{mtx};
        if (std::holds_alternative<unstarted>(this->outcome) || std::holds_alternative<unstarted>(this->outcome)) {
          // We only link, if the nested-opstate hasn't completed yet.
          link.succ = std::exchange(pending_completions, &link);
          return;
        }
      }

      // Only reached if the state was complete.
      // Invokes the link-callback, which initiates the next operation-state.
      //
      // Must be called outside the lock, because it may destroy *this.
      std::invoke(link.callback, link);
    }

    private:
    // Start any pending completions.
    //
    // Must be called without lock.
    auto notify() noexcept -> void {
      bool last = false;
      while (!last) {
        opstate_link* lnk;
        std::tie(lnk, last) = pop_link();
        if (lnk != nullptr) std::invoke(lnk->callback, *lnk);
      }
    }

    // Unlink and return an opstate-link.
    // Handles locking by itsef.
    auto pop_link() noexcept -> std::tuple<opstate_link*, bool> {
      std::lock_guard lck{mtx};
      opstate_link*const lnk = pending_completions;
      if (lnk == nullptr) return std::make_tuple(nullptr, true);

      assert(lnk->pred == nullptr);
      pending_completions = std::exchange(lnk->succ, nullptr);
      if (pending_completions != nullptr) pending_completions->pred = nullptr;
      lnk->succ = nullptr;
      return std::make_tuple(lnk, pending_completions == nullptr);
    }

    protected:
    mutable std::mutex mtx;
    variant_type outcome;

    private:
    opstate_link* pending_completions = nullptr;
  };
  // Returns the outcome-types corresponding to this sender type.
  template<typed_sender Sender>
  using shared_opstate_outcomes_for_sender = shared_opstate_outcomes<sender_traits<Sender>::sends_done, outcome_types<Sender>>;

  // Shared operation state.
  //
  // Runs the preceding chain, and then keeps a hold of the outcomes.
  template<typed_sender Sender>
  class shared_opstate
  : public shared_opstate_outcomes_for_sender<Sender>
  {
    private:
    using receiver_type = typename shared_opstate_outcomes_for_sender<Sender>::receiver_impl;
    using nested_opstate_type = decltype(::earnest::execution::connect(std::declval<Sender>(), std::declval<receiver_type>()));

    public:
    explicit shared_opstate(Sender&& s)
    : shared_opstate_outcomes_for_sender<Sender>(),
      nested_opstate(::earnest::execution::connect(std::move(s), receiver_type(*this)))
    {}

    // Ensure the shared_opstate is started.
    // You should not hold the lock during this call:
    // the function managed it itself.
    auto ensure_started() noexcept -> void {
      {
        std::lock_guard lck{this->mtx};
        if (!std::holds_alternative<unstarted>(this->outcome)) return; // already started
        this->outcome.template emplace<started>();
      }
      // Ensure we start the operation outside the mutex.
      ::earnest::execution::start(nested_opstate);
    }

    private:
    [[no_unique_address]] nested_opstate_type nested_opstate;
  };

  // The operation state for successive operations.
  template<typed_sender Sender, receiver Receiver>
  class opstate
  : private opstate_link,
    public operation_state_base_
  {
    public:
    explicit opstate(std::shared_ptr<shared_opstate<Sender>>&& shared_state, Receiver&& r)
    : opstate_link(&callback_impl),
      r(std::move(r)),
      shared_state(std::move(shared_state))
    {}

    explicit opstate(const std::shared_ptr<shared_opstate<Sender>>& shared_state, Receiver&& r)
    : opstate_link(&callback_impl),
      r(std::move(r)),
      shared_state(shared_state)
    {}

    friend auto tag_invoke([[maybe_unused]] start_t, opstate& self) noexcept -> void {
      self.shared_state->ensure_started();

      // Attach last: `self' may destroy this opstate upon completion, and that would
      // invalidate `self.shared_state'.
      self.shared_state->attach(self);
    }

    private:
    static auto callback_impl(opstate_link& self_link) noexcept -> void {
      opstate& self = static_cast<opstate&>(self_link);
      self.shared_state->apply(std::move(self.r));
    }

    Receiver r;
    std::shared_ptr<shared_opstate<Sender>> shared_state;
  };

  // Implementation of the sender.
  template<typed_sender Sender>
  class sender_impl {
    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = typename sender_traits<Sender>::template value_types<Tuple, Variant>;

    template<template<typename...> class Variant>
    using error_types = typename sender_traits<Sender>::template error_types<_type_appender<std::exception_ptr>::template append>::template type<Variant>;

    static inline constexpr bool sends_done = sender_traits<Sender>::sends_done;

    template<typename Sender_>
    requires std::constructible_from<shared_opstate<Sender>, Sender_>
    explicit constexpr sender_impl(Sender_&& s)
    : shared_state(std::make_shared<shared_opstate<Sender>>(std::forward<Sender_>(s)))
    {}

    // Connect to a receiver.
    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> opstate<Sender, std::remove_cvref_t<Receiver>> {
      return opstate<Sender, std::remove_cvref_t<Receiver>>(std::move(self.shared_state), std::forward<Receiver>(r));
    }

    // Connect to a receiver.
    // This connect takes `sender_impl' as by const-reference, permitting re-use.
    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, const sender_impl& self, Receiver&& r)
    -> opstate<Sender, std::remove_cvref_t<Receiver>> {
      return opstate<Sender, std::remove_cvref_t<Receiver>>(self.shared_state, std::forward<Receiver>(r));
    }

    private:
    std::shared_ptr<shared_opstate<Sender>> shared_state;
  };

  template<typed_sender Sender>
  constexpr auto default_impl(Sender&& s) const
  -> sender_impl<std::remove_cvref_t<Sender>> {
    return sender_impl<std::remove_cvref_t<Sender>>(std::forward<Sender>(s));
  }
};
inline constexpr lazy_split_t lazy_split{};


// Split a sender, allowing for multiple chains to be attached, all sharing the same
// chain leading up to the split.
struct split_t {
  template<typename> friend struct _generic_operand_base_t;

  template<typed_sender Sender>
  constexpr auto operator()(Sender&& s) const
  -> typed_sender decltype(auto) {
    return _generic_operand_base<set_value_t>(*this, std::forward<Sender>(s));
  }

  private:
  template<typed_sender Sender>
  constexpr auto default_impl(Sender&& s) const
  -> typed_sender decltype(auto) {
    return lazy_split(std::forward<Sender>(s));
  }
};
inline constexpr split_t split{};


struct lazy_on_t {
  template<scheduler Scheduler, typed_sender Sender>
  constexpr auto operator()(Scheduler&& sch, Sender&& s) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_on_t, Scheduler, Sender>)
      return ::earnest::execution::tag_invoke(*this, std::forward<Scheduler>(sch), std::forward<Sender>(s));
    else
      return this->default_impl(std::forward<Scheduler>(sch), std::forward<Sender>(s));
  }

  private:
  // Operation-state for the lazy-on operation.
  //
  // Upon connect, the `scheduler_sender' will invoke:
  // `schedule(Scheduler) | lazy_let_value(returning Sender) | scheduler_receiver(Receiver)'
  // The trailing receiver of is stored in the opstate, and assigned to
  // by the scheduler_receiver.
  //
  // The scheduler_receiver takes care of delivering the completion-signal into the receiver,
  // and the completing the receiver from `start_detached'.
  //
  // When this opstate is started, it'll run `start_detached(scheduler_sender)'.
  template<scheduler Scheduler, sender Sender, receiver Receiver>
  class opstate
  : public operation_state_base_
  {
    private:
    template<receiver LocalReceiver>
    class scheduler_receiver
    : public _generic_receiver_wrapper<LocalReceiver, set_value_t, set_error_t, set_done_t, get_scheduler_t>
    {
      public:
      explicit scheduler_receiver(LocalReceiver&& r, opstate& state)
      : _generic_receiver_wrapper<LocalReceiver, set_value_t, set_error_t, set_done_t, get_scheduler_t>(std::move(r)),
        state(state)
      {}

      template<typename... Args>
      friend auto tag_invoke([[maybe_unused]] set_value_t, scheduler_receiver&& self, Args&&... args) noexcept -> void {
        // Forward value-signal to the receiver in opstate.
        try {
          ::earnest::execution::set_value(std::move(self.state.r), std::forward<Args>(args)...);
        } catch (...) {
          ::earnest::execution::set_error(std::move(self.state.r), std::current_exception());
        }

        self.complete();
      }

      template<typename Error>
      friend auto tag_invoke([[maybe_unused]] set_error_t, scheduler_receiver&& self, Error&& error) noexcept -> void {
        // Forward the error-signal to the receiver in opstate.
        ::earnest::execution::set_error(std::move(self.state.r), std::forward<Error>(error));
        self.complete();
      }

      friend auto tag_invoke([[maybe_unused]] set_done_t, scheduler_receiver&& self) noexcept -> void {
        // Forward the done-signal to the receiver in opstate.
        ::earnest::execution::set_done(std::move(self.state.r));
        self.complete();
      }

      friend auto tag_invoke([[maybe_unused]] get_scheduler_t, const scheduler_receiver& self) noexcept -> auto {
        return self.state.sch;
      }

      private:
      // Inform wrapped receiver that we're done (with no values).
      auto complete() noexcept -> void {
        try {
          ::earnest::execution::set_value(std::move(this->r));
        } catch (...) {
          ::earnest::execution::set_error(std::move(this->r), std::current_exception());
        }
      }

      opstate& state;
    };

    class scheduler_sender {
      public:
      template<template<typename...> class Tuple, template<typename...> class Variant>
      using value_types = Variant<Tuple<>>;

      template<template<typename...> class Variant>
      using error_types = Variant<std::exception_ptr>;

      static inline constexpr bool sends_done = false;

      explicit scheduler_sender(opstate& state) noexcept
      : state(state)
      {}

      template<receiver LocalReceiver>
      friend auto tag_invoke([[maybe_unused]] connect_t, scheduler_sender&& self, LocalReceiver&& r)
      -> decltype(auto) {
        return ::earnest::execution::connect(
            ::earnest::execution::schedule(self.state.sch)
            | lazy_let_value(
                [state=&self.state]() noexcept -> decltype(auto) {
                  return std::move(state->s);
                }),
            scheduler_receiver<std::remove_cvref_t<LocalReceiver>>(std::forward<LocalReceiver>(r), self.state));
      }

      private:
      opstate& state;
    };

    public:
    opstate(Scheduler&& sch, Sender&& s, Receiver&& r)
    : sch(std::move(sch)),
      s(std::move(s)),
      r(std::move(r))
    {}

    friend auto tag_invoke([[maybe_unused]] start_t, opstate& self) noexcept -> void {
      try {
        ::earnest::execution::start_detached(scheduler_sender(self));
      } catch (...) {
        ::earnest::execution::set_error(std::move(self.r), std::current_exception());
      }
    }

    private:
    Scheduler sch;
    Sender s;
    Receiver r;
  };

  template<scheduler Scheduler, typed_sender Sender>
  class sender_impl
  : public _generic_sender_wrapper<sender_impl<Scheduler, Sender>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>
  {
    public:
    static inline constexpr bool sends_done =
        sender_traits<Sender>::sends_done ||
        sender_traits<decltype(::earnest::execution::schedule(std::declval<Scheduler>()))>::sends_done;

    template<typename Scheduler_, typename Sender_>
    requires std::constructible_from<Scheduler, Scheduler_> && std::constructible_from<Sender, Sender_>
    constexpr sender_impl(Scheduler_&& sch, Sender_&& s)
    : _generic_sender_wrapper<sender_impl<Scheduler, Sender>, Sender, connect_t, get_completion_scheduler_t<set_value_t>, get_completion_scheduler_t<set_error_t>, get_completion_scheduler_t<set_done_t>>(std::forward<Sender_>(s)),
      sch(std::forward<Scheduler_>(sch))
    {}

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> opstate<Scheduler, Sender, std::remove_cvref_t<Receiver>> {
      return opstate<Scheduler, Sender, std::remove_cvref_t<Receiver>>(std::move(self.sch), std::move(self.s), std::forward<Receiver>(r));
    }

    private:
    Scheduler sch;
  };

  template<scheduler Scheduler, typed_sender Sender>
  constexpr auto default_impl(Scheduler&& sch, Sender&& s) const
  -> sender_impl<std::remove_cvref_t<Scheduler>, std::remove_cvref_t<Sender>> {
    return sender_impl<std::remove_cvref_t<Scheduler>, std::remove_cvref_t<Sender>>(std::forward<Scheduler>(sch), std::forward<Sender>(s));
  }
};
inline constexpr lazy_on_t lazy_on{};


struct on_t {
  template<scheduler Scheduler, typed_sender Sender>
  constexpr auto operator()(Scheduler&& sch, Sender&& s) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<on_t, Scheduler, Sender>)
      return ::earnest::execution::tag_invoke(*this, std::forward<Scheduler>(sch), std::forward<Sender>(s));
    else
      return lazy_on(std::forward<Scheduler>(sch), std::forward<Sender>(s));
  }
};
inline constexpr on_t on{};


} /* namespace earnest::execution */
