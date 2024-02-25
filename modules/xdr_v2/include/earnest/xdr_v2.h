/* XDR encoding/decoding.
 *
 * `readable_object' and `writable_object' are types that we can create an XDR encoder/decoder for.
 * Those are called `read_operation' or `write_operation'.
 *
 * The following read and write operations exist:
 * 1. read/write into a buffer directly on a writable/readable object
 * 2. read into a temporary object (or buffer) and then invoke a function
 * 3. write from a temporary buffer
 * 4. skip some bytes to maintain padding requirements
 * 5. invoke a pre-encoding/pre-decoding function
 * 6. invoke a post-encoding/post-decoding function
 * 7. encode/decode a nested object
 * 8. manual
 *
 * A sequence of read_operations is a read_operation.
 * And a sequence of write_operations is a write_operation.
 *
 * In order to have the encoder/decoder run efficient, we want to maximize
 * the amount of data transferred per system call. To do this, we re-organize
 * a sequence of read/write operations:
 * - all pre-encoding/pre-decoding functions are moved to the front
 *   (so they can all be run together).
 * - all post-encoding/post-decoding functions are moved to the back
 *   (so they can all be run together).
 * - this leave in the middle, a number of buffers, which we'll combine together
 *   into a vectored write operation.
 * This does lead to an issue where dependent functions could break
 * (for example a pre-decoding read that allocates memory for data).
 * In order to support those, we introduce the notion of a sequence-point.
 * No pre-* or post-* function can cross the sequence-point.
 */
#pragma once

#include <algorithm>
#include <bit>
#include <concepts>
#include <cstddef>
#include <limits>
#include <optional>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <type_traits>

#include <earnest/execution.h>
#include <earnest/execution_util.h>
#include <earnest/execution_io.h>

namespace earnest::xdr_v2 {


struct reader_t {
  template<typename T>
  auto operator()(T& v) const noexcept(execution::nothrow_tag_invocable<reader_t, T&>) -> execution::tag_invoke_result_t<reader_t, T&> {
    return execution::tag_invoke(*this, v);
  }
};
struct writer_t {
  template<typename T>
  auto operator()(const T& v) const noexcept(execution::nothrow_tag_invocable<writer_t, const T&>) -> execution::tag_invoke_result_t<writer_t, const T&> {
    return execution::tag_invoke(*this, v);
  }
};
inline constexpr reader_t reader{};
inline constexpr writer_t writer{};


class xdr_error
: public std::runtime_error
{
  using std::runtime_error::runtime_error;
};


namespace operation {


template<typename T>
concept with_buffer_shift = requires(std::remove_cvref_t<T>& v, std::size_t sz) {
  { v.buffer_shift(sz) };
};

template<typename T>
concept with_temporaries_shift = requires(std::remove_cvref_t<T>&& v) {
  { std::move(v).template temporaries_shift<std::size_t(0)>() };
};

template<typename T>
concept with_constexpr_extent = requires {
  { std::remove_cvref_t<T>::extent } -> std::same_as<const std::size_t&>;
  { std::bool_constant<(std::remove_cvref_t<T>::extent, true)>() } -> std::same_as<std::true_type>; // Confirm extent is constexpr.
};

template<typename T>
concept buffer_operation =
    with_buffer_shift<T> &&
    with_temporaries_shift<T> &&
    with_constexpr_extent<T> &&
    // We require a get_buffer operation, which accepts at least one of
    // `std::span<std::byte>' (when reading) or
    // `std::span<const std::byte>' (when writing)
    ( requires (std::remove_cvref_t<T> v, const std::span<std::byte> s) {
        { v.get_buffer(s) } -> std::convertible_to<std::span<std::byte>>;
      }
      ||
      requires (std::remove_cvref_t<T> v, const std::span<const std::byte> s) {
        { v.get_buffer(s) } -> std::convertible_to<std::span<const std::byte>>;
      });

template<typename T>
concept invocable_operation =
    with_buffer_shift<T> &&
    with_temporaries_shift<T>;
template<typename T, typename State>
concept invocable_operation_for_state =
    invocable_operation<T> &&
    std::invocable<T, State&>;

template<typename T>
concept operation_block_element =
    with_buffer_shift<T> &&
    with_temporaries_shift<T> &&
    with_constexpr_extent<T> &&
    requires (std::remove_cvref_t<T> v) {
      { std::remove_cvref_t<T>::for_writing } -> std::same_as<const bool&>;
      { std::remove_cvref_t<T>::for_reading } -> std::same_as<const bool&>;
      { std::bool_constant<std::remove_cvref_t<T>::for_writing>() }; // Confirm for_writing is constexpr.
      { std::bool_constant<std::remove_cvref_t<T>::for_reading>() }; // Confirm for_reading is constexpr.
    } &&
    ( !std::remove_cvref_t<T>::for_writing || requires (std::remove_cvref_t<T> v) {
        { std::move(v).template make_sender_chain<true>() } -> execution::sender;
      }
    ) &&
    ( !std::remove_cvref_t<T>::for_reading || requires (std::remove_cvref_t<T> v) {
        { std::move(v).template make_sender_chain<false>() } -> execution::sender;
      }
    );

template<typename T, typename Temporaries>
concept unresolved_operation_block_element =
    with_constexpr_extent<T> &&
    requires(std::remove_cvref_t<T> v, Temporaries& temporaries) {
      { std::remove_cvref_t<T>::for_writing } -> std::same_as<const bool&>;
      { std::remove_cvref_t<T>::for_reading } -> std::same_as<const bool&>;
      { std::bool_constant<std::remove_cvref_t<T>::for_writing>() }; // Confirm for_writing is constexpr.
      { std::bool_constant<std::remove_cvref_t<T>::for_reading>() }; // Confirm for_reading is constexpr.
      { std::move(v).resolve(temporaries).sender_chain() } -> execution::sender;
    };

template<typename T>
concept readable_object = execution::tag_invocable<reader_t, std::remove_cvref_t<T>&>;

template<typename T>
concept writable_object = execution::tag_invocable<writer_t, const std::remove_cvref_t<T>&>;


template<bool IsConst, typename Temporaries, unresolved_operation_block_element<Temporaries>... Elements>
class unresolved_operation_block;

template<typename>
struct is_unresolved_operation_block_ : std::false_type {};

template<bool IsConst, typename Temporaries, unresolved_operation_block_element<Temporaries>... Elements>
struct is_unresolved_operation_block_<unresolved_operation_block<IsConst, Temporaries, Elements...>>
: std::true_type
{};

template<typename T>
concept is_unresolved_operation_block = is_unresolved_operation_block_<T>::value;


template<typename Invocation, typename T>
concept read_invocation_for =
    std::copy_constructible<std::remove_cvref_t<Invocation>> &&
    requires(const std::remove_cvref_t<Invocation> invocation, T& v) {
      // Must have a read method.
      { invocation.read(v) } -> is_unresolved_operation_block;
      // Confirm extent is defined and is a constant-time expression.
      typename std::integral_constant<std::size_t, decltype(invocation.read(v))::extent>;
      // Confirm the invocation can produce a sender.
      { invocation.read(v).sender_chain() } -> execution::sender;
    };

template<typename Invocation, typename T>
concept write_invocation_for =
    std::copy_constructible<std::remove_cvref_t<Invocation>> &&
    requires(const std::remove_cvref_t<Invocation> invocation, const T& const_v) {
      // Must have a write method.
      { invocation.write(const_v) } -> is_unresolved_operation_block;
      // Confirm extent is defined and is a constant-time expression.
      typename std::integral_constant<std::size_t, decltype(invocation.write(const_v))::extent>;
      // Confirm the invocation can produce a sender.
      { invocation.write(const_v).sender_chain() } -> execution::sender;
    };

template<typename Invocation, typename T>
concept invocation_for = read_invocation_for<Invocation, T> && write_invocation_for<Invocation, T>;


template<typename = std::tuple<>, typename = std::tuple<>, typename = std::tuple<>> class operation_sequence;

// Describe an operation-block.
//
// Operation-blocks contain sequences, describing how to decode/encode data.
// The operation-block tracks:
// - a shared buffer, for anything that gets encoded early.
// - the type of temporaries (not instantiated), tracking what scratch-variables are required during the operation.
//
// Template parameters:
// - IsConst: if `true', all buffer-references are constants, meaning the operation-block is for writing.
//   Otherwise, the buffer-references are non-const, meaning the operation-block is for reading.
// - Extent: the extent of the temporary buffer.
// - Temporaries: a tuple of default-initialized types, that are used during the operation. Basically scratch-space.
// - Sequences...: the actual steps, that are to be executed sequentially.
template<bool IsConst, std::size_t Extent = 0, operation_block_element... Sequences>
class operation_block;


template<std::size_t... XIdx, typename XByte, std::size_t X, std::size_t... YIdx, typename YByte, std::size_t Y>
requires std::same_as<std::remove_cvref_t<XByte>, std::byte> && std::same_as<std::remove_cvref_t<YByte>, std::byte>
constexpr auto array_cat_(
    [[maybe_unused]] std::index_sequence<XIdx...>, std::span<XByte, X> x,
    [[maybe_unused]] std::index_sequence<YIdx...>, std::span<YByte, Y> y)
noexcept
-> std::array<std::byte, sizeof...(XIdx) + sizeof...(YIdx)> {
  return std::array<std::byte, sizeof...(XIdx) + sizeof...(YIdx)>{x[XIdx]..., y[YIdx]...};
}

template<typename XByte, std::size_t X, typename YByte, std::size_t Y>
requires std::same_as<std::remove_cvref_t<XByte>, std::byte> &&
    std::same_as<std::remove_cvref_t<YByte>, std::byte> &&
    (X != std::dynamic_extent) && (Y != std::dynamic_extent)
constexpr auto array_cat(std::span<XByte, X> x, std::span<YByte, Y> y)
noexcept
-> std::array<std::byte, X + Y> {
  return array_cat_(std::make_index_sequence<X>(), x, std::make_index_sequence<Y>(), y);
}


struct io_barrier {
  static inline constexpr std::size_t extent = 0;
  static inline constexpr bool for_writing = true;
  static inline constexpr bool for_reading = true;

  auto buffer_shift([[maybe_unused]] std::size_t) noexcept {}

  template<std::size_t N>
  auto temporaries_shift() noexcept -> io_barrier&& {
    return std::move(*this);
  }

  template<bool IsConst>
  auto make_sender_chain() {
    return execution::noop();
  }
};
static_assert(operation_block_element<io_barrier>);


template<typename Fn, std::size_t... TemporariesIndices>
class pre_buffer_invocation {
  template<bool, std::size_t, operation_block_element...> friend class operation_block;

  private:
  explicit pre_buffer_invocation(Fn&& fn, std::size_t off, std::size_t len) noexcept(std::is_nothrow_move_constructible_v<Fn>)
  : fn(std::move(fn)),
    off(off),
    len(len)
  {}

  public:
  explicit pre_buffer_invocation(Fn&& fn) noexcept(std::is_nothrow_move_constructible_v<Fn>)
  : fn(std::move(fn))
  {}

  explicit pre_buffer_invocation(const Fn& fn) noexcept(std::is_nothrow_copy_constructible_v<Fn>)
  : fn(fn)
  {}

  auto operator()() && {
    return execution::lazy_then(
        [self=std::move(*this)]<typename State>(State st) mutable -> State {
          assert(st.shared_buf().size() >= self.off + self.len);
          assert(self.len != 0);
          std::invoke(std::move(self.fn), st.shared_buf().subspan(self.off, self.len), st.template get_temporary<TemporariesIndices>()...);
          return st;
        });
  }

  auto buffer_shift(std::size_t increase) noexcept -> void {
    off += increase;
  }

  template<std::size_t N>
  auto temporaries_shift() && {
    return pre_buffer_invocation<Fn, TemporariesIndices + N...>(std::move(fn), off, len);
  }

  private:
  auto assign(std::size_t off, std::size_t len) noexcept {
    this->off = off;
    this->len = len;
  }

  Fn fn;
  std::size_t off = 0, len = 0;
};

template<typename Fn, std::size_t... TemporariesIndices>
class post_buffer_invocation {
  template<bool, std::size_t, operation_block_element...> friend class operation_block;

  private:
  explicit post_buffer_invocation(Fn&& fn, std::size_t off, std::size_t len) noexcept(std::is_nothrow_move_constructible_v<Fn>)
  : fn(std::move(fn)),
    off(off),
    len(len)
  {}

  public:
  explicit post_buffer_invocation(Fn&& fn) noexcept(std::is_nothrow_move_constructible_v<Fn>)
  : fn(std::move(fn))
  {}

  explicit post_buffer_invocation(const Fn& fn) noexcept(std::is_nothrow_copy_constructible_v<Fn>)
  : fn(fn)
  {}

  auto operator()() && {
    return execution::lazy_then(
        [self=std::move(*this)]<typename State>(State st) mutable -> State {
          assert(st.shared_buf().size() >= self.off + self.len);
          assert(self.len != 0);
          std::invoke(std::move(self.fn), st.shared_buf().subspan(self.off, self.len), st.template get_temporary<TemporariesIndices>()...);
          return st;
        });
  }

  auto buffer_shift(std::size_t increase) noexcept -> void {
    off += increase;
  }

  template<std::size_t N>
  auto temporaries_shift() && {
    return post_buffer_invocation<Fn, TemporariesIndices + N...>(std::move(fn), off, len);
  }

  private:
  auto assign(std::size_t off, std::size_t len) noexcept {
    this->off = off;
    this->len = len;
  }

  Fn fn;
  std::size_t off = 0, len = 0;
};

template<typename Fn, std::size_t... TemporariesIndices>
class pre_invocation {
  public:
  explicit pre_invocation(Fn&& fn) noexcept(std::is_nothrow_move_constructible_v<Fn>)
  : fn(std::move(fn))
  {}

  explicit pre_invocation(const Fn& fn) noexcept(std::is_nothrow_copy_constructible_v<Fn>)
  : fn(fn)
  {}

  auto operator()() && {
    return execution::lazy_then(
        [self=std::move(*this)]<typename State>(State st) mutable -> State {
          std::invoke(std::move(self.fn), st.template get_temporary<TemporariesIndices>()...);
          return st;
        });
  }

  auto buffer_shift([[maybe_unused]] std::size_t increase) noexcept -> void {}

  template<std::size_t N>
  auto temporaries_shift() && {
    return pre_invocation<Fn, (TemporariesIndices + N)...>(std::move(fn));
  }

  private:
  Fn fn;
};

template<typename Fn, std::size_t... TemporariesIndices>
class post_invocation {
  public:
  explicit post_invocation(Fn&& fn) noexcept(std::is_nothrow_move_constructible_v<Fn>)
  : fn(std::move(fn))
  {}

  explicit post_invocation(const Fn& fn) noexcept(std::is_nothrow_copy_constructible_v<Fn>)
  : fn(fn)
  {}

  auto operator()() && {
    return execution::lazy_then(
        [self=std::move(*this)]<typename State>(State st) mutable -> State {
          std::invoke(std::move(self.fn), st.template get_temporary<TemporariesIndices>()...);
          return st;
        });
  }

  auto buffer_shift([[maybe_unused]] std::size_t increase) noexcept -> void {}

  template<std::size_t N>
  auto temporaries_shift() && {
    return post_invocation<Fn, (TemporariesIndices + N)...>(std::move(fn));
  }

  private:
  Fn fn;
};

template<typename Pred, std::size_t... TemporariesIndices>
class pre_validation {
  public:
  explicit pre_validation(Pred&& pred)
  noexcept(std::is_nothrow_move_constructible_v<Pred>)
  : pred(std::move(pred))
  {}

  explicit pre_validation(const Pred& pred)
  noexcept(std::is_nothrow_copy_constructible_v<Pred>)
  : pred(pred)
  {}

  auto operator()() && {
    return execution::lazy_validation(
        [pred=std::move(pred)]<typename State>(const State& st) mutable {
          return std::invoke(std::move(pred), st.template get_temporary<TemporariesIndices>()...);
        });
  }

  auto buffer_shift([[maybe_unused]] std::size_t increase) noexcept -> void {}

  template<std::size_t N>
  auto temporaries_shift() && {
    return pre_validation<Pred, (TemporariesIndices + N)...>(std::move(pred));
  }

  private:
  Pred pred;
};

template<typename Pred, std::size_t... TemporariesIndices>
class post_validation {
  public:
  explicit post_validation(Pred&& pred)
  noexcept(std::is_nothrow_move_constructible_v<Pred>)
  : pred(std::move(pred))
  {}

  explicit post_validation(const Pred& pred)
  noexcept(std::is_nothrow_copy_constructible_v<Pred>)
  : pred(pred)
  {}

  auto operator()() && {
    return execution::lazy_validation(
        [pred=std::move(pred)]<typename State>(const State& st) mutable {
          return std::invoke(std::move(pred), st.template get_temporary<TemporariesIndices>()...);
        });
  }

  auto buffer_shift([[maybe_unused]] std::size_t increase) noexcept -> void {}

  template<std::size_t N>
  auto temporaries_shift() && {
    return post_validation<Pred, (TemporariesIndices + N)...>(std::move(pred));
  }

  private:
  Pred pred;
};

// Refers to space in the temporary buffer.
//
// Offset and length are stored, because the temporary buffer may be re-allocated.
template<bool IsConst, std::size_t Extent = std::dynamic_extent>
class temporary_buffer_reference {
  public:
  static inline constexpr std::size_t extent = Extent;
  static inline constexpr bool for_writing = IsConst;
  static inline constexpr bool for_reading = !IsConst;

  explicit temporary_buffer_reference(std::size_t off) noexcept
  : off(off)
  {}

  explicit temporary_buffer_reference(std::size_t off, std::size_t len) noexcept
  : off(off)
  {
    assert(len == Extent);
  }

  auto get_buffer(std::span<std::conditional_t<IsConst, const std::byte, std::byte>> shared_buf) const {
    assert(shared_buf.size() >= off + Extent);
    return shared_buf.subspan(off, Extent);
  }

  auto buffer_shift(std::size_t increase) -> void {
    if (std::numeric_limits<std::size_t>::max() - off - Extent < increase)
      throw std::range_error("xdr buffer shift overflow");
    off += increase;
  }

  template<std::size_t N>
  auto temporaries_shift() && -> temporary_buffer_reference&& {
    return std::move(*this);
  }

  auto get_off() const noexcept -> std::size_t { return off; }
  auto get_len() const noexcept -> std::size_t { return Extent; }

  private:
  std::size_t off;
};

template<bool IsConst>
class temporary_buffer_reference<IsConst, std::dynamic_extent> {
  public:
  static inline constexpr std::size_t extent = std::dynamic_extent;
  static inline constexpr bool for_writing = IsConst;
  static inline constexpr bool for_reading = !IsConst;

  explicit temporary_buffer_reference(std::size_t off, std::size_t len) noexcept
  : off(off),
    len(len)
  {}

  auto get_buffer(std::span<std::conditional_t<IsConst, const std::byte, std::byte>> shared_buf) const {
    assert(shared_buf.size() >= off + len);
    return shared_buf.subspan(off, len);
  }

  auto buffer_shift(std::size_t increase) -> void {
    if (std::numeric_limits<std::size_t>::max() - off - len < increase)
      throw std::range_error("xdr buffer shift overflow");
    off += increase;
  }

  template<std::size_t N>
  auto temporaries_shift() && -> temporary_buffer_reference&& {
    return std::move(*this);
  }

  auto get_off() const noexcept -> std::size_t { return off; }
  auto get_len() const noexcept -> std::size_t { return len; }

  private:
  std::size_t off, len;
};

template<bool IsConst, std::size_t Extent = std::dynamic_extent>
class span_reference {
  public:
  static inline constexpr std::size_t extent = Extent;
  static inline constexpr bool for_writing = IsConst;
  static inline constexpr bool for_reading = !IsConst;

  explicit span_reference(std::span<std::conditional_t<IsConst, const std::byte, std::byte>, Extent> data) noexcept
  : data(data)
  {}

  auto get_buffer([[maybe_unused]] std::span<std::conditional_t<IsConst, const std::byte, std::byte>> shared_buf) const noexcept {
    return data;
  }

  auto buffer_shift([[maybe_unused]] std::size_t increase) noexcept -> void {}

  template<std::size_t N>
  auto temporaries_shift() && -> span_reference&& {
    return std::move(*this);
  }

  private:
  std::span<std::conditional_t<IsConst, const std::byte, std::byte>, Extent> data;
};

template<std::invocable Fn>
class dynamic_span_reference {
  private:
  using span_type = std::remove_cvref_t<std::invoke_result_t<Fn>>;

  public:
  static inline constexpr std::size_t extent = span_type::extent;
  static inline constexpr bool for_writing = true;
  static inline constexpr bool for_reading = !std::is_const_v<typename span_type::element_type>;

  explicit dynamic_span_reference(Fn&& fn)
  : fn(std::move(fn))
  {}

  explicit dynamic_span_reference(const Fn& fn)
  : fn(fn)
  {}

  template<typename Span>
  auto get_buffer([[maybe_unused]] const Span& shared_buf) const {
    return std::invoke(fn);
  }

  auto buffer_shift([[maybe_unused]] std::size_t increase) noexcept -> void {}

  template<std::size_t N>
  auto temporaries_shift() && -> dynamic_span_reference&& {
    return std::move(*this);
  }

  private:
  Fn fn;
};


struct buffer_operation_appender_ {
  template<typename... T, typename Op>
  requires (sizeof...(T) > 0)
  auto operator()(std::tuple<T...>&& list, Op&& op) const {
    return std::apply(
        []<typename... X>(X&&... x) {
          return std::make_tuple(std::forward<X>(x)...);
        },
        std::tuple_cat(
            all_but_last(std::move(list)),
            maybe_squash(only_last(std::move(list)), std::forward<Op>(op))));
  }

  template<typename Op>
  auto operator()([[maybe_unused]] std::tuple<>&& list, Op&& op) const {
    return std::make_tuple(std::forward<Op>(op));
  }

  private:
  template<typename... T>
  requires (sizeof...(T) > 0)
  auto all_but_last(std::tuple<T...>&& list) const {
    return all_but_last_(std::make_index_sequence<sizeof...(T) - 1u>(), std::move(list));
  }

  template<std::size_t... Idx, typename... T>
  auto all_but_last_([[maybe_unused]] std::index_sequence<Idx...>, std::tuple<T...>&& list) const {
    return std::forward_as_tuple(std::get<Idx>(std::move(list))...);
  }

  template<typename... T>
  requires (sizeof...(T) > 0)
  auto only_last(std::tuple<T...>&& list) const -> decltype(auto) {
    return std::get<sizeof...(T) - 1u>(std::move(list));
  }

  template<bool IsConst, std::size_t Extent, typename Op>
  auto maybe_squash(span_reference<IsConst, Extent>&& s, Op&& op) const {
    return std::forward_as_tuple(std::move(s), std::forward<Op>(op));
  }

  template<typename Fn, typename Op>
  auto maybe_squash(dynamic_span_reference<Fn>&& s, Op&& op) const {
    return std::forward_as_tuple(std::move(s), std::forward<Op>(op));
  }

  template<bool IsConst, std::size_t Extent1, std::size_t Extent2>
  auto maybe_squash(temporary_buffer_reference<IsConst, Extent1>&& s, span_reference<IsConst, Extent2>&& op) const {
    return std::forward_as_tuple(std::move(s), std::move(op));
  }

  template<bool IsConst, std::size_t Extent1, typename Fn>
  auto maybe_squash(temporary_buffer_reference<IsConst, Extent1>&& s, dynamic_span_reference<Fn>&& op) const {
    return std::forward_as_tuple(std::move(s), std::move(op));
  }

  template<bool IsConst, std::size_t Extent1, std::size_t Extent2>
  auto maybe_squash(temporary_buffer_reference<IsConst, Extent1>&& s, temporary_buffer_reference<IsConst, Extent2>&& op) const {
    if constexpr(Extent1 == std::dynamic_extent || Extent2 == std::dynamic_extent) {
      return std::make_tuple(temporary_buffer_reference<IsConst, std::dynamic_extent>(s.off, s.len + op.len));
    } else {
      return std::make_tuple(temporary_buffer_reference<IsConst, Extent1 + Extent2>(s.off, s.len + op.len));
    }
  }
};
inline constexpr buffer_operation_appender_ buffer_operation_appender{};


template<typename FD, typename Buffer, typename Temporaries>
class state_ {
  template<typename, typename, typename> friend class state_;

  public:
  state_(FD&& fd, Buffer&& buffer, Temporaries* temporaries)
  : fd(std::move(fd)),
    buffer(std::move(buffer)),
    temporaries(std::move(temporaries))
  {}

  auto shared_buf() noexcept {
    return std::span<std::byte>(buffer.data(), buffer.size());
  }

  template<typename OtherFD>
  auto rebind(OtherFD&& other_fd) && {
    return state_<std::remove_cvref_t<OtherFD>, Buffer, Temporaries>(
        std::forward<OtherFD>(other_fd),
        std::move(buffer),
        std::move(temporaries));
  }

  template<std::size_t I>
  requires (I < std::tuple_size_v<Temporaries>)
  auto get_temporary() const -> decltype(auto) {
    return std::get<I>(*temporaries);
  }

  FD fd;

  private:
  Buffer buffer;
  Temporaries* temporaries;
};

template<typename FD, std::size_t N, typename Temporaries>
class state_<FD, std::array<std::byte, N>, Temporaries> {
  template<typename, typename, typename> friend class state_;

  public:
  state_(FD&& fd, std::array<std::byte, N>&& buffer, Temporaries* temporaries)
  : fd(std::move(fd)),
    buffer(std::move(buffer)),
    temporaries(std::move(temporaries))
  {}

  auto shared_buf() noexcept {
    return std::span<std::byte, N>(buffer.data(), buffer.size());
  }

  template<typename OtherFD>
  auto rebind(OtherFD&& other_fd) && {
    return state_<std::remove_cvref_t<OtherFD>, std::array<std::byte, N>, Temporaries>(
        std::forward<OtherFD>(other_fd),
        std::move(buffer),
        std::move(temporaries));
  }

  template<std::size_t I>
  requires (I < std::tuple_size_v<Temporaries>)
  auto get_temporary() -> decltype(auto) {
    return std::get<I>(*temporaries);
  }

  template<std::size_t I>
  requires (I < std::tuple_size_v<Temporaries>)
  auto get_temporary() const -> decltype(auto) {
    return std::get<I>(*temporaries);
  }

  FD fd;

  private:
  std::array<std::byte, N> buffer;
  Temporaries* temporaries;
};

template<typename FD, typename Buffer, typename Temporaries>
inline auto make_state_(FD&& fd, Buffer&& buffer, Temporaries* temporaries) -> state_<std::remove_cvref_t<FD>, std::remove_cvref_t<Buffer>, Temporaries> {
  return state_<std::remove_cvref_t<FD>, std::remove_cvref_t<Buffer>, Temporaries>(
      std::forward<FD>(fd),
      std::forward<Buffer>(buffer),
      std::move(temporaries));
}


template<std::size_t Extent = 0>
class temporary_buffer;

template<>
class temporary_buffer<std::dynamic_extent> {
  template<std::size_t> friend class temporary_buffer;

  public:
  static inline constexpr std::size_t extent = std::dynamic_extent;

  private:
  explicit temporary_buffer(std::size_t sz)
  : sz(sz)
  {}

  public:
  auto size() const noexcept -> std::size_t {
    return sz;
  }

  auto get_data() && -> std::vector<std::byte> {
    return std::vector<std::byte>(size());
  }

  template<std::size_t N>
  auto append() && {
    sz += N;
    return std::move(*this);
  }

  auto append(std::size_t n) && {
    sz += n;
    return std::move(*this);
  }

  template<std::size_t OtherExtent>
  auto merge(temporary_buffer<OtherExtent>&& other) && {
    return temporary_buffer(size() + other.size());
  }

  private:
  std::size_t sz = 0;
};

template<std::size_t Extent>
class temporary_buffer {
  template<std::size_t> friend class temporary_buffer;

  public:
  static inline constexpr std::size_t extent = Extent;

  temporary_buffer() = default;

  auto size() const noexcept -> std::size_t {
    return Extent;
  }

  auto get_data() && -> std::array<std::byte, Extent> {
    return {};
  }

  template<std::size_t N>
  auto append() && {
    return temporary_buffer<Extent + N>{};
  }

  auto append(std::size_t n) && {
    return temporary_buffer<std::dynamic_extent>(Extent + n);
  }

  template<std::size_t OtherExtent>
  auto merge(temporary_buffer<OtherExtent>&& other) && {
    if constexpr(OtherExtent == std::dynamic_extent) {
      return temporary_buffer<std::dynamic_extent>(Extent + other.size());
    } else {
      return temporary_buffer<Extent + OtherExtent>();
    }
  }
};


template<>
class operation_sequence<std::tuple<>, std::tuple<>, std::tuple<>> {
  template<typename, typename, typename> friend class operation_sequence;

  public:
  static inline constexpr bool for_writing = true;
  static inline constexpr bool for_reading = true;
  static inline constexpr std::size_t extent = 0;

  operation_sequence() = default;
  operation_sequence(operation_sequence&&) = default;
  operation_sequence(const operation_sequence&) = delete;

  template<bool IsConst>
  requires (IsConst ? for_writing : for_reading)
  auto make_sender_chain() && {
    return execution::noop();
  }

  template<typename Temporaries>
  auto resolve([[maybe_unused]] Temporaries& temporaries) && noexcept -> operation_sequence&& {
    return std::move(*this);
  }

  auto buffer_shift(std::size_t increase) noexcept -> void {}

  template<std::size_t N>
  auto temporaries_shift() && -> operation_sequence&& {
    return std::move(*this);
  }

  auto append_operation() && -> operation_sequence&& {
    return std::move(*this);
  }

  template<typename Op0, typename... Ops>
  requires (sizeof...(Ops) > 0)
  auto append_operation(Op0&& op0, Ops&&... ops) && {
    return std::move(*this).append_operation(std::forward<Op0>(op0)).append_operation(std::forward<Ops>(ops)...);
  }

  template<typename Fn, std::size_t... TemporariesIndices>
  auto append_operation(pre_buffer_invocation<Fn, TemporariesIndices...>&& op) && {
    using result_type = operation_sequence<
        std::tuple<pre_buffer_invocation<Fn, TemporariesIndices...>>,
        std::tuple<>,
        std::tuple<>>;
    return result_type(
        std::forward_as_tuple(std::move(op)),
        std::tuple<>(),
        std::tuple<>());
  }

  template<typename Fn, std::size_t... TemporariesIndices>
  auto append_operation(post_buffer_invocation<Fn, TemporariesIndices...>&& op) && {
    using result_type = operation_sequence<
        std::tuple<>,
        std::tuple<>,
        std::tuple<post_buffer_invocation<Fn, TemporariesIndices...>>>;
    return result_type(
        std::tuple<>(),
        std::tuple<>(),
        std::forward_as_tuple(std::move(op)));
  }

  template<typename Fn, std::size_t... TemporariesIndices>
  auto append_operation(pre_invocation<Fn, TemporariesIndices...>&& op) && {
    using result_type = operation_sequence<
        std::tuple<pre_invocation<Fn, TemporariesIndices...>>,
        std::tuple<>,
        std::tuple<>>;
    return result_type(
        std::forward_as_tuple(std::move(op)),
        std::tuple<>(),
        std::tuple<>());
  }

  template<typename Fn, std::size_t... TemporariesIndices>
  auto append_operation(post_invocation<Fn, TemporariesIndices...>&& op) && {
    using result_type = operation_sequence<
        std::tuple<>,
        std::tuple<>,
        std::tuple<post_invocation<Fn, TemporariesIndices...>>>;
    return result_type(
        std::tuple<>(),
        std::tuple<>(),
        std::forward_as_tuple(std::move(op)));
  }

  template<typename Pred, std::size_t... TemporariesIndices>
  auto append_operation(pre_validation<Pred, TemporariesIndices...>&& op) && {
    using result_type = operation_sequence<
        std::tuple<pre_validation<Pred, TemporariesIndices...>>,
        std::tuple<>,
        std::tuple<>>;
    return result_type(
        std::forward_as_tuple(std::move(op)),
        std::tuple<>(),
        std::tuple<>());
  }

  template<typename Pred, std::size_t... TemporariesIndices>
  auto append_operation(post_validation<Pred, TemporariesIndices...>&& op) && {
    using result_type = operation_sequence<
        std::tuple<>,
        std::tuple<>,
        std::tuple<post_validation<Pred, TemporariesIndices...>>>;
    return result_type(
        std::tuple<>(),
        std::tuple<>(),
        std::forward_as_tuple(std::move(op)));
  }

  template<bool IsConst, std::size_t Extent = std::dynamic_extent>
  auto append_operation(temporary_buffer_reference<IsConst, Extent>&& op) && {
    using result_type = operation_sequence<
        std::tuple<>,
        std::tuple<temporary_buffer_reference<IsConst, Extent>>,
        std::tuple<>>;
    return result_type(
        std::tuple<>(),
        std::forward_as_tuple(std::move(op)),
        std::tuple<>());
  }

  template<bool IsConst, std::size_t Extent = std::dynamic_extent>
  auto append_operation(span_reference<IsConst, Extent>&& op) && {
    using result_type = operation_sequence<
        std::tuple<>,
        std::tuple<span_reference<IsConst, Extent>>,
        std::tuple<>>;
    return result_type(
        std::tuple<>(),
        std::forward_as_tuple(std::move(op)),
        std::tuple<>());
  }

  template<typename Fn>
  auto append_operation(dynamic_span_reference<Fn>&& op) && {
    using result_type = operation_sequence<
        std::tuple<>,
        std::tuple<dynamic_span_reference<Fn>>,
        std::tuple<>>;
    return result_type(
        std::tuple<>(),
        std::forward_as_tuple(std::move(op)),
        std::tuple<>());
  }

  template<typename OtherPreInvocationOperations, typename OtherBufferOperations, typename OtherPostInvocationOperations>
  auto merge(operation_sequence<OtherPreInvocationOperations, OtherBufferOperations, OtherPostInvocationOperations>&& other)
  -> operation_sequence<OtherPreInvocationOperations, OtherBufferOperations, OtherPostInvocationOperations>&& {
    return std::move(other);
  }
};

static_assert(operation_block_element<operation_sequence<>>);

// Helper type, that takes zero or more buffers, and transforms it into a sender adapter.
template<bool IsConst>
struct make_read_write_op_ {
  static inline constexpr bool for_writing = IsConst;
  static inline constexpr bool for_reading = !IsConst;

  template<typename... Buffer>
  auto operator()(Buffer&&... buffer) const {
    return execution::lazy_let_value(
        // lazy_let_value will ensure the lifetime of `auto& st' is long enough for us to maintain a reference.
        [...buffer=std::forward<Buffer>(buffer)](auto& st) -> execution::typed_sender auto {
          if constexpr(for_writing) {
            return execution::io::lazy_write_ec(std::ref(st.fd), maybe_wrap_in_array_for_writing(buffer.get_buffer(st.shared_buf())...))
            | execution::lazy_then(
                [&st]([[maybe_unused]] std::size_t wlen) -> decltype(auto) {
                  return std::move(st);
                });
          } else {
            return execution::io::lazy_read_ec(std::ref(st.fd), maybe_wrap_in_array_for_reading(buffer.get_buffer(st.shared_buf())...))
            | execution::lazy_then(
                [&st]([[maybe_unused]] std::size_t rlen) -> decltype(auto) {
                  return std::move(st);
                });
          }
        });
  }

  private:
  template<typename... Spans>
  static auto maybe_wrap_in_array_for_reading(Spans&&... s) {
    if constexpr(sizeof...(s) == 1) {
      return std::span<std::byte>(std::forward<Spans>(s)...);
    } else {
      return std::array<std::span<std::byte>, sizeof...(s)>{ std::forward<Spans>(s)... };
    }
  }

  template<typename... Spans>
  static auto maybe_wrap_in_array_for_writing(Spans&&... s) {
    if constexpr(sizeof...(s) == 1) {
      return std::span<const std::byte>(std::forward<Spans>(s)...);
    } else {
      return std::array<std::span<const std::byte>, sizeof...(s)>{ std::forward<Spans>(s)... };
    }
  }
};

template<invocable_operation... PreInvocationOperations, buffer_operation... BufferOperations, invocable_operation... PostInvocationOperations>
class operation_sequence<
    std::tuple<PreInvocationOperations...>,
    std::tuple<BufferOperations...>,
    std::tuple<PostInvocationOperations...>>
{
  template<typename, typename, typename> friend class operation_sequence;

  public:
  static inline constexpr bool for_writing = (BufferOperations::for_writing &&...);
  static inline constexpr bool for_reading = (BufferOperations::for_reading &&...);
  static inline constexpr std::size_t extent =
      ((BufferOperations::extent == std::dynamic_extent) ||...) ?
      std::dynamic_extent :
      (0u +...+ BufferOperations::extent);

  private:
  operation_sequence(
      std::tuple<PreInvocationOperations...>&& pre_invocations,
      std::tuple<BufferOperations...>&& buffers,
      std::tuple<PostInvocationOperations...>&& post_invocations)
  noexcept(
      std::is_nothrow_move_constructible_v<std::tuple<PreInvocationOperations...>> &&
      std::is_nothrow_move_constructible_v<std::tuple<BufferOperations...>> &&
      std::is_nothrow_move_constructible_v<std::tuple<PostInvocationOperations...>>)
  : pre_invocations(std::move(pre_invocations)),
    buffers(std::move(buffers)),
    post_invocations(std::move(post_invocations))
  {}

  public:
  operation_sequence(operation_sequence&&) = default;
  operation_sequence(const operation_sequence&) = delete;

  template<bool IsConst>
  requires (IsConst ? for_writing : for_reading)
  auto make_sender_chain() && {
    return std::apply(
        [](auto&&... x) {
          return (execution::noop() |...| std::invoke(std::move(x)));
        },
        std::move(pre_invocations))
    | std::apply(
        [&](auto&&... x) {
          if constexpr(sizeof...(x) == 0) {
            return execution::noop();
          } else {
            return make_read_write_op_<IsConst>{}(std::move(x)...);
          }
        },
        std::move(buffers))
    | std::apply(
        [](auto&&... x) {
          return (execution::noop() |...| std::invoke(std::move(x)));
        },
        std::move(post_invocations));
  }

  template<typename Temporaries>
  auto resolve(Temporaries& temporaries) && {
    auto resolved_pre_invocations = std::apply(
        [&temporaries](auto&&... x) {
          return std::make_tuple(std::move(x).resolve(temporaries)...);
        },
        std::move(pre_invocations));
    auto resolved_buffers = std::apply(
        [&temporaries](auto&&... x) {
          return std::make_tuple(std::move(x).resolve(temporaries)...);
        },
        std::move(buffers));
    auto resolved_post_invocations = std::apply(
        [&temporaries](auto&&... x) {
          return std::make_tuple(std::move(x).resolve(temporaries)...);
        },
        std::move(post_invocations));
    return operation_sequence<decltype(resolved_pre_invocations), decltype(resolved_buffers), decltype(resolved_post_invocations)>(
        std::move(resolved_pre_invocations), std::move(resolved_buffers), std::move(resolved_post_invocations));
  }

  auto buffer_shift(std::size_t increase) -> void {
    std::apply(
        [&](auto&... x) {
          (x.buffer_shift(increase), ...);
        },
        pre_invocations);
    std::apply(
        [&](auto&... x) {
          (x.buffer_shift(increase), ...);
        },
        buffers);
    std::apply(
        [&](auto&... x) {
          (x.buffer_shift(increase), ...);
        },
        post_invocations);
  }

  template<std::size_t N>
  auto temporaries_shift() && {
    auto shifted_pre_invocations = std::apply(
        [](auto&&... x) {
          return std::make_tuple(std::move(x).template temporaries_shift<N>()...);
        },
        std::move(pre_invocations));
    auto shifted_buffers = std::apply(
        [](auto&&... x) {
          return std::make_tuple(std::move(x).template temporaries_shift<N>()...);
        },
        std::move(buffers));
    auto shifted_post_invocations = std::apply(
        [](auto&&... x) {
          return std::make_tuple(std::move(x).template temporaries_shift<N>()...);
        },
        std::move(post_invocations));
    return operation_sequence<decltype(shifted_pre_invocations), decltype(shifted_buffers), decltype(shifted_post_invocations)>(
        std::move(shifted_pre_invocations), std::move(shifted_buffers), std::move(shifted_post_invocations));
  }

  auto append_operation() && -> operation_sequence&& {
    return std::move(*this);
  }

  template<typename Op0, typename... Ops>
  requires (sizeof...(Ops) > 0)
  auto append_operation(Op0&& op0, Ops&&... ops) && {
    return std::move(*this).append_operation(std::forward<Op0>(op0)).append_operation(std::forward<Ops>(ops)...);
  }

  template<typename Fn, std::size_t... TemporariesIndices>
  auto append_operation(pre_buffer_invocation<Fn, TemporariesIndices...>&& op) && {
    using result_type = operation_sequence<
        std::tuple<PreInvocationOperations..., pre_buffer_invocation<Fn, TemporariesIndices...>>,
        std::tuple<BufferOperations...>,
        std::tuple<PostInvocationOperations...>>;
    return result_type(
        std::tuple_cat(std::move(pre_invocations), std::forward_as_tuple(std::move(op))),
        std::move(buffers),
        std::move(post_invocations));
  }

  template<typename Fn, std::size_t... TemporariesIndices>
  auto append_operation(post_buffer_invocation<Fn, TemporariesIndices...>&& op) && {
    using result_type = operation_sequence<
        std::tuple<PreInvocationOperations...>,
        std::tuple<BufferOperations...>,
        std::tuple<PostInvocationOperations..., post_buffer_invocation<Fn, TemporariesIndices...>>>;
    return result_type(
        std::move(pre_invocations),
        std::move(buffers),
        std::tuple_cat(std::move(post_invocations), std::forward_as_tuple(std::move(op))));
  }

  template<typename Fn, std::size_t... TemporariesIndices>
  auto append_operation(pre_invocation<Fn, TemporariesIndices...>&& op) && {
    using result_type = operation_sequence<
        std::tuple<PreInvocationOperations..., pre_invocation<Fn, TemporariesIndices...>>,
        std::tuple<BufferOperations...>,
        std::tuple<PostInvocationOperations...>>;
    return result_type(
        std::tuple_cat(std::move(pre_invocations), std::forward_as_tuple(std::move(op))),
        std::move(buffers),
        std::move(post_invocations));
  }

  template<typename Fn, std::size_t... TemporariesIndices>
  auto append_operation(post_invocation<Fn, TemporariesIndices...>&& op) && {
    using result_type = operation_sequence<
        std::tuple<PreInvocationOperations...>,
        std::tuple<BufferOperations...>,
        std::tuple<PostInvocationOperations..., post_invocation<Fn, TemporariesIndices...>>>;
    return result_type(
        std::move(pre_invocations),
        std::move(buffers),
        std::tuple_cat(std::move(post_invocations), std::forward_as_tuple(std::move(op))));
  }

  template<typename Fn, std::size_t... TemporariesIndices>
  auto append_operation(pre_validation<Fn, TemporariesIndices...>&& op) && {
    using result_type = operation_sequence<
        std::tuple<PreInvocationOperations..., pre_validation<Fn>>,
        std::tuple<BufferOperations...>,
        std::tuple<PostInvocationOperations...>>;
    return result_type(
        std::tuple_cat(std::move(pre_invocations), std::forward_as_tuple(std::move(op))),
        std::move(buffers),
        std::move(post_invocations));
  }

  template<typename Fn, std::size_t... TemporariesIndices>
  auto append_operation(post_validation<Fn, TemporariesIndices...>&& op) && {
    using result_type = operation_sequence<
        std::tuple<PreInvocationOperations...>,
        std::tuple<BufferOperations...>,
        std::tuple<PostInvocationOperations..., post_validation<Fn, TemporariesIndices...>>>;
    return result_type(
        std::move(pre_invocations),
        std::move(buffers),
        std::tuple_cat(std::move(post_invocations), std::forward_as_tuple(std::move(op))));
  }

  template<bool IsConst, std::size_t Extent = std::dynamic_extent>
  auto append_operation(temporary_buffer_reference<IsConst, Extent>&& op) && {
    using result_type = operation_sequence<
        std::tuple<PreInvocationOperations...>,
        decltype(buffer_operation_appender(std::move(buffers), std::move(op))),
        std::tuple<PostInvocationOperations...>>;
    return result_type(
        std::move(pre_invocations),
        buffer_operation_appender(std::move(buffers), std::move(op)),
        std::move(post_invocations));
  }

  template<bool IsConst, std::size_t Extent = std::dynamic_extent>
  auto append_operation(span_reference<IsConst, Extent>&& op) && {
    using result_type = operation_sequence<
        std::tuple<PreInvocationOperations...>,
        decltype(buffer_operation_appender(std::move(buffers), std::move(op))),
        std::tuple<PostInvocationOperations...>>;
    return result_type(
        std::move(pre_invocations),
        buffer_operation_appender(std::move(buffers), std::move(op)),
        std::move(post_invocations));
  }

  template<typename Fn>
  auto append_operation(dynamic_span_reference<Fn>&& op) && {
    using result_type = operation_sequence<
        std::tuple<PreInvocationOperations...>,
        decltype(buffer_operation_appender(std::move(buffers), std::move(op))),
        std::tuple<PostInvocationOperations...>>;
    return result_type(
        std::move(pre_invocations),
        buffer_operation_appender(std::move(buffers), std::move(op)),
        std::move(post_invocations));
  }

  template<typename OtherPreInvocationOperations, typename OtherBufferOperations, typename OtherPostInvocationOperations>
  auto merge(operation_sequence<OtherPreInvocationOperations, OtherBufferOperations, OtherPostInvocationOperations> other) {
    auto new_pre = std::tuple_cat(std::move(pre_invocations), std::move(other.pre_invocations));
    auto new_buf = std::tuple_cat(std::move(buffers), std::move(other.buffers));
    auto new_post = std::tuple_cat(std::move(post_invocations), std::move(other.post_invocations));

    return operation_sequence<decltype(new_pre), decltype(new_buf), decltype(new_post)>(std::move(new_pre), std::move(new_buf), std::move(new_post));
  }

  private:
  std::tuple<PreInvocationOperations...> pre_invocations;
  std::tuple<BufferOperations...> buffers;
  std::tuple<PostInvocationOperations...> post_invocations;
};


// An invocation that takes manual control over the file descriptor.
//
// The function can do whatever it wants, as long as it produces a sender
// of file-descriptor. Doesn't have to be the same file-descriptor.
template<typename Fn, std::size_t Extent = std::dynamic_extent, std::size_t... TemporariesIndices>
class manual_invocation {
  public:
  static inline constexpr bool for_writing = true;
  static inline constexpr bool for_reading = true;
  static inline constexpr std::size_t extent = Extent;

  explicit manual_invocation(Fn&& fn)
  : fn(std::move(fn))
  {}

  explicit manual_invocation(const Fn& fn)
  : fn(fn)
  {}

  template<bool>
  auto make_sender_chain() && {
    return execution::lazy_let_value(
        [fn=std::move(this->fn)](auto& st) mutable {
          return std::invoke(std::move(fn), st.fd, st.template get_temporary<TemporariesIndices>()...)
          | execution::lazy_then(
              [&st]<typename FD>(FD&& fd) {
                return std::move(st).rebind(std::forward<FD>(fd));
              });
        });
  }

  template<typename Temporaries>
  auto resolve([[maybe_unused]] Temporaries& temporaries) && noexcept -> manual_invocation&& {
    return std::move(*this);
  }

  auto buffer_shift([[maybe_unused]] std::size_t) noexcept {}

  template<std::size_t N>
  auto temporaries_shift() && {
    return manual_invocation<Fn, Extent, TemporariesIndices + N...>(std::move(fn));
  }

  private:
  Fn fn;
};

// Confirm that manual operation is a block-element.
// (We use a temporary lambda to complete the type, just for testing.)
static_assert(operation_block_element<manual_invocation<decltype([](auto) { /* do nothing */ })>>);


// Helper type to figure out what the last type in a tuple is.
//
// Exposes `void' if the tuple is empty.
template<typename Tuple>
struct tuple_last_type__ {
  using type = std::tuple_element_t<std::tuple_size_v<Tuple> - 1u, Tuple>;
};

template<>
struct tuple_last_type__<std::tuple<>> {
  using type = void;
};

template<typename Tuple>
using tuple_last_type_ = typename tuple_last_type__<Tuple>::type;


template<typename>
struct is_operation_sequence_
: std::false_type
{};
template<typename PreInvocationOperations, typename BufferOperations, typename PostInvocationOperations>
struct is_operation_sequence_<operation_sequence<PreInvocationOperations, BufferOperations, PostInvocationOperations>>
: std::true_type
{};

template<typename T>
static inline constexpr bool is_operation_sequence = is_operation_sequence_<T>::value;


template<operation_block_element... Sequences>
class operation_sequence_tuple
: public std::tuple<Sequences...>
{
  public:
  using std::tuple<Sequences...>::tuple;

  auto buffer_shift(std::size_t increase) -> void {
    apply(
        [increase](auto&... x) {
          (x.buffer_shift(increase), ...);
        });
  }

  template<std::size_t N>
  auto temporaries_shift() && {
    return std::move(*this).apply(
        [](auto&&... x) {
          return operation_sequence_tuple<std::remove_cvref_t<decltype(std::move(x).template temporaries_shift<N>())>...>(
              std::move(x).template temporaries_shift<N>()...);
        });
  }

  template<typename Op0, typename... Ops>
  requires (sizeof...(Ops) > 0)
  auto append_operation(Op0&& op0, Ops&&... ops) && {
    return std::move(*this).append_operation(std::forward<Op0>(op0)).append_operation(std::forward<Ops>(ops)...);
  }

  template<typename Op>
  auto append_operation(Op&& op) && {
    if constexpr(last_is_operation_sequence) {
      return std::apply(
          []<typename... T>(T&&... v) {
            return operation_sequence_tuple<std::remove_cvref_t<T>...>(std::forward<T>(v)...);
          },
          std::tuple_cat(
              std::move(*this).all_but_last_as_rvalue_tuple(),
              std::forward_as_tuple(std::get<sizeof...(Sequences) - 1u>(std::move(*this)).append_operation(std::forward<Op>(op)))));
    } else {
      return std::move(*this).append_sequence(operation_sequence<>{}.append_operation(std::forward<Op>(op)));
    }
  }

  auto append_sequence() && -> operation_sequence_tuple&& {
    return std::move(*this);
  }

  template<typename Seq0, typename... Seqs>
  requires (sizeof...(Seqs) > 0)
  auto append_sequence(Seq0&& seq0, Seqs&&... seqs) && {
    return std::move(*this).append_sequence(std::forward<Seq0>(seq0)).append_sequence(std::forward<Seqs>(seqs)...);
  }

  template<operation_block_element Seq>
  auto append_sequence(Seq&& seq) && {
    if constexpr(last_is_operation_sequence && is_operation_sequence<std::remove_cvref_t<Seq>>) {
      return std::apply(
          []<typename... T>(T&&... v) {
            return operation_sequence_tuple<std::remove_cvref_t<T>...>(std::forward<T>(v)...);
          },
          std::tuple_cat(
              std::move(*this).all_but_last_as_rvalue_tuple(),
              std::forward_as_tuple(std::get<sizeof...(Sequences) - 1u>(std::move(*this)).merge(std::forward<Seq>(seq)))));
    } else {
      return operation_sequence_tuple<Sequences..., std::remove_cvref_t<Seq>>(
          std::tuple_cat(
              std::move(*this).all_as_rvalue_tuple(),
              std::forward_as_tuple(std::forward<Seq>(seq))));
    }
  }

  auto merge() && -> operation_sequence_tuple&& {
    return std::move(*this);
  }

  template<typename Seq0, typename... Seqs>
  requires (sizeof...(Seqs) > 0)
  auto merge(Seq0&& seq0, Seqs&&... seqs) && {
    return std::move(*this).merge(std::forward<Seq0>(seq0)).merge(std::forward<Seqs>(seqs)...);
  }

  template<typename... OtherSequences>
  auto merge(operation_sequence_tuple<OtherSequences...>&& other) && {
    return std::move(other).apply(
        [this]<typename... X>(X&&... x) {
          return std::move(*this).append_sequence(std::forward<X>(x)...);
        });
  }

  template<typename Fn>
  auto apply(Fn&& fn) && {
    std::tuple<Sequences...>& self = *this;
    return std::apply(std::forward<Fn>(fn), std::move(self));
  }

  template<typename Fn>
  auto apply(Fn&& fn) & {
    std::tuple<Sequences...>& self = *this;
    return std::apply(std::forward<Fn>(fn), self);
  }

  template<typename Fn>
  auto apply(Fn&& fn) const & {
    const std::tuple<Sequences...>& self = *this;
    return std::apply(std::forward<Fn>(fn), self);
  }

  private:
  inline auto all_as_rvalue_tuple() && {
    return std::move(*this).apply(
        [](Sequences&&... v) {
          return std::forward_as_tuple(std::move(v)...);
        });
  }

  inline auto all_but_last_as_rvalue_tuple() && {
    return all_but_last_as_rvalue_tuple_(std::make_index_sequence<sizeof...(Sequences) - 1u>());
  }

  template<std::size_t... Idx>
  inline auto all_but_last_as_rvalue_tuple_([[maybe_unused]] std::index_sequence<Idx...>) {
    return std::forward_as_tuple(std::get<Idx>(std::move(*this))...);
  }

  static inline constexpr bool last_is_operation_sequence = is_operation_sequence<tuple_last_type_<std::tuple<Sequences...>>>;
};

template<bool IsConst, std::size_t Extent, typename... Sequences>
auto make_operation_block(
      temporary_buffer<Extent>&& tmpbuf,
      operation_sequence_tuple<Sequences...>&& sequences)
-> operation_block<IsConst, Extent, Sequences...> {
  return operation_block<IsConst, Extent, Sequences...>(std::move(tmpbuf), std::move(sequences));
}

template<bool IsConst, std::size_t Extent, operation_block_element... Sequences>
class operation_block {
  template<bool, std::size_t, operation_block_element...> friend class operation_block;

  static_assert(IsConst ? (Sequences::for_writing &&...) : (Sequences::for_reading &&...));

  public:
  static inline constexpr bool for_writing = IsConst;
  static inline constexpr bool for_reading = !IsConst;

  // Figure out how many bytes we require to encode this block.
  // We use std::dynamic_extent if the size is unknown at compile time.
  static inline constexpr std::size_t extent =
      ((Sequences::extent == std::dynamic_extent) ||...) ?
      std::dynamic_extent :
      (0u +...+ Sequences::extent);

  operation_block(
      temporary_buffer<Extent>&& tmpbuf,
      operation_sequence_tuple<Sequences...>&& sequences)
  : tmpbuf(std::move(tmpbuf)),
    sequences(std::move(sequences))
  {}

  auto sender_chain() && {
    return execution::then(
        [tmpbuf=std::move(this->tmpbuf)]<typename FD, typename Temporaries>(FD&& fd, Temporaries* temporaries) mutable {
          return make_state_(std::forward<FD>(fd), std::move(tmpbuf).get_data(), temporaries);
        })
    | std::move(sequences).apply(
        [](auto&&... sequence) {
          return (execution::noop() |...| std::move(sequence).template make_sender_chain<IsConst>());
        })
    | execution::lazy_then(
        [](auto st) {
          return std::move(st.fd);
        });
  }

  template<typename Temporaries>
  auto resolve(Temporaries& temporaries) && -> operation_block&& {
    return std::move(*this);
  }

  auto buffer_shift(std::size_t increase) -> void {
    sequences.buffer_shift(increase);
  }

  template<std::size_t N>
  auto temporaries_shift() && {
    return make_operation_block<IsConst>(
        std::move(tmpbuf),
        std::move(sequences).template temporaries_shift<N>());
  }

  template<typename Op>
  auto append(Op&& op) && {
    return make_operation_block<IsConst>(
        std::move(tmpbuf),
        std::move(sequences).append_operation(std::forward<Op>(op)));
  }

  template<typename Seq>
  auto append_sequence(Seq&& seq) && {
    return make_operation_block<IsConst>(
        std::move(tmpbuf),
        std::move(sequences).append_sequence(std::forward<Seq>(seq)));
  }

  template<std::size_t N, typename... Op>
  requires (N != std::dynamic_extent)
  auto buffer(Op&&... op) && {
    const auto off = tmpbuf.size();
    (op.assign(tmpbuf.size(), N), ...);
    return make_operation_block<IsConst>(
        std::move(tmpbuf).template append<N>(),
        std::move(sequences).append_operation(temporary_buffer_reference<IsConst, N>(off, N), std::forward<Op>(op)...));
  }

  template<typename... Op>
  auto buffer(std::size_t n, Op&&... op) && {
    const auto off = tmpbuf.size();
    (op.assign(tmpbuf.size(), n), ...);
    return make_operation_block<IsConst>(
        std::move(tmpbuf).append(n),
        std::move(sequences).append_operation(temporary_buffer_reference<IsConst>(off, n), std::forward<Op>(op)...));
  }

  template<std::size_t OtherExtent, typename... OtherSequences>
  auto merge(operation_block<IsConst, OtherExtent, OtherSequences...>&& other) && {
    other.buffer_shift(tmpbuf.size());
    return make_operation_block<IsConst>(
        std::move(tmpbuf).merge(std::move(other.tmpbuf)),
        std::move(sequences).merge(std::move(other.sequences)));
  }

  private:
  temporary_buffer<Extent> tmpbuf;
  operation_sequence_tuple<Sequences...> sequences;
};

template<bool IsConst>
class operation_block<IsConst, 0> {
  template<bool, std::size_t, operation_block_element...> friend class operation_block;

  public:
  static inline constexpr bool for_writing = IsConst;
  static inline constexpr bool for_reading = !IsConst;

  // We require exactly zero bytes to encode this block.
  static inline constexpr std::size_t extent = 0;

  operation_block() = default;

  auto sender_chain() && {
    return execution::then(
        []<typename FD, typename Temporaries>(FD&& fd, Temporaries* temporaries) -> decltype(auto) {
          return std::forward<FD>(fd);
        });
  }

  template<typename Temporaries>
  auto resolve([[maybe_unused]] Temporaries& temporaries) && -> operation_block&& {
    return std::move(*this);
  }

  auto buffer_shift([[maybe_unused]] std::size_t increase) -> void {}

  template<std::size_t N>
  auto temporaries_shift() && -> operation_block&& {
    return std::move(*this);
  }

  template<typename Op>
  auto append(Op&& op) && {
    return make_operation_block<IsConst>(
        temporary_buffer<>(),
        operation_sequence_tuple<operation_sequence<>>().append_operation(std::forward<Op>(op)));
  }

  template<operation_block_element Seq>
  auto append_sequence(Seq&& seq) && {
    return make_operation_block<IsConst>(
        temporary_buffer<>(),
        operation_sequence_tuple<>().append_sequence(std::forward<Seq>(seq)));
  }

  template<std::size_t N, typename... Op>
  requires (N != std::dynamic_extent)
  auto buffer(Op&&... op) && {
    (op.assign(0, N), ...);
    return make_operation_block<IsConst>(
        temporary_buffer<>().template append<N>(),
        operation_sequence_tuple<operation_sequence<>>().append_operation(temporary_buffer_reference<IsConst, N>(0, N), std::forward<Op>(op)...));
  }

  template<typename... Op>
  requires (sizeof...(Op) > 0)
  auto buffer(std::size_t n, Op&&... op) && {
    (op.assign(0, n), ...);
    return make_operation_block<IsConst>(
        temporary_buffer<IsConst>().append(n),
        operation_sequence_tuple<operation_sequence<>>().append_operation(temporary_buffer_reference<IsConst>(0, n), std::forward<Op>(op)...));
  }

  template<std::size_t OtherExtent, typename... OtherSequences>
  auto merge(operation_block<IsConst, OtherExtent, OtherSequences...>&& other) &&
  -> operation_block<IsConst, OtherExtent, OtherSequences...>&& {
    return std::move(other);
  }
};


template<typename T>
struct unresolved_operation_block_is_const_;
template<bool IsConst, typename Temporaries, typename... Elements>
struct unresolved_operation_block_is_const_<unresolved_operation_block<IsConst, Temporaries, Elements...>> : std::bool_constant<IsConst> {};

template<bool IsConst, typename... Temporaries, typename... Elements>
class unresolved_operation_block<IsConst, std::tuple<Temporaries...>, Elements...> {
  template<bool, typename OtherTemporaries, unresolved_operation_block_element<OtherTemporaries>...>
  friend class unresolved_operation_block;

  static_assert((std::default_initializable<Temporaries> &&...));
  static_assert(((IsConst ? Elements::for_writing : Elements::for_reading) &&...));

  public:
  template<typename... Elements_>
  explicit unresolved_operation_block(Elements_&&... elements)
  : elements(std::forward<Elements_>(elements)...)
  {}

  static inline constexpr bool for_writing = (Elements::for_writing &&...);
  static inline constexpr bool for_reading = (Elements::for_reading &&...);

  using temporaries = std::tuple<Temporaries...>;
  static inline constexpr std::size_t extent =
      ((Elements::extent == std::dynamic_extent) || ...) ?
      std::dynamic_extent :
      (0u +...+ Elements::extent);

  auto sender_chain() && {
    if constexpr(std::is_same_v<temporaries, std::tuple<>>) {
      temporaries no_temporaries;
      return execution::lazy_then(
          []<typename FD>(FD&& fd) {
            std::tuple<>* no_temporaries = nullptr;
            return std::tuple<FD, temporaries*>(std::forward<FD>(fd), no_temporaries);
          })
      | execution::lazy_explode_tuple()
      | std::move(*this).resolve(no_temporaries).sender_chain();
    } else {
      return execution::lazy_then(
          []<typename FD>(FD&& fd) -> std::tuple<FD, temporaries> {
            return std::tuple<FD, temporaries>(std::forward<FD>(fd), temporaries{});
          })
      | execution::lazy_explode_tuple()
      | execution::lazy_let_value(
          [self=std::move(*this)]<typename FD>(FD& fd, temporaries& t) mutable {
            return execution::just(std::move(fd), &t)
            | std::move(self).resolve(t).sender_chain();
          });
    }
  }

  auto resolve(temporaries& t) && {
    return std::move(*this).resolve_(std::index_sequence_for<Elements...>{}, operation_block<IsConst>{}, t);
  }

  template<typename... OtherTemporaries, typename... OtherElements>
  auto merge(unresolved_operation_block<IsConst, std::tuple<OtherTemporaries...>, OtherElements...>&& other) && {
    return std::apply(
        []<typename... X>(X&&... x) {
          return unresolved_operation_block<
              IsConst,
              std::tuple<Temporaries..., OtherTemporaries...>,
              std::remove_cvref_t<X>...>(std::forward<X>(x)...);
        },
        std::tuple_cat(
            std::move(elements),
            std::apply(
                []<typename... X>(X&&... x) {
                  return std::make_tuple(std::forward<X>(x).template temporaries_shift<sizeof...(Temporaries)>()...);
                },
                std::move(other.elements))));
  }

  template<unresolved_operation_block_element<std::tuple<Temporaries...>> Block>
  auto append_block(Block&& block) && {
    static_assert(!is_unresolved_operation_block<Block>);
    return std::move(*this).append_block_(std::forward<Block>(block), std::index_sequence_for<Elements...>());
  }

  private:
  template<typename OperationBlock>
  auto resolve_([[maybe_unused]] std::index_sequence<>, OperationBlock&& op_block, [[maybe_unused]] temporaries& t) && {
    return std::forward<OperationBlock>(op_block);
  }

  template<std::size_t Idx0, std::size_t... Idx, typename OperationBlock>
  auto resolve_([[maybe_unused]] std::index_sequence<Idx0, Idx...>, OperationBlock&& op_block, temporaries& t) && {
    auto next_block = std::get<Idx0>(std::move(elements)).resolve(t);
    if constexpr(is_unresolved_operation_block<decltype(next_block)>) {
      static_assert(std::is_void_v<std::tuple_element_t<Idx0, std::tuple<Elements...>>>);
    }
    return std::move(*this).resolve_(
        std::index_sequence<Idx...>(),
        std::move(op_block).merge(std::move(next_block)),
        t);
  }

  template<unresolved_operation_block_element<temporaries> Block, std::size_t... Idx>
  auto append_block_(Block&& block, [[maybe_unused]] std::index_sequence<Idx...>) && {
    return unresolved_operation_block<IsConst, std::tuple<Temporaries...>, Elements..., std::remove_cvref_t<Block>>(std::get<Idx>(std::move(elements))..., std::forward<Block>(block));
  }

  std::tuple<Elements...> elements;
};


template<is_unresolved_operation_block X, is_unresolved_operation_block Y>
auto operator|(X&& x, Y&& y) {
  return std::forward<X>(x).merge(std::forward<Y>(y));
}


template<typename Op, typename... Options>
class operation_with_options {
  public:
  template<typename Op_, typename... Options_>
  explicit constexpr operation_with_options(Op_&& op, Options_&&... options)
  : op(std::forward<Op_>(op)),
    options(std::forward<Options_>(options)...)
  {}

  template<typename T>
  auto read(T&& v) const {
    return std::apply(
        [&v, this](const auto&... options) {
          return this->op.read(std::forward<T>(v), options...);
        },
        options);
  }

  template<typename T>
  auto write(T&& v) const {
    return std::apply(
        [&v, this](const auto&... options) {
          return this->op.write(std::forward<T>(v), options...);
        },
        options);
  }

  private:
  Op op;
  std::tuple<Options...> options;
};

template<typename Op, typename... Options>
operation_with_options(Op&& op, Options&&... options) -> operation_with_options<std::remove_cvref_t<Op>, std::remove_cvref_t<Options>...>;


template<typename Derived>
struct basic_operation {
  constexpr auto operator()() const {
    return Derived{};
  }

  template<typename... Options>
  constexpr auto operator()(Options&&... options) const {
    return operation_with_options(static_cast<const Derived&>(*this), std::forward<Options>(options)...);
  }
};


struct uint8_t
: basic_operation<uint8_t>
{
  template<std::unsigned_integral T>
  auto read(T& v) const {
    return unresolved_operation_block<false, std::tuple<>>{}
        .append_block(operation_block<false>{}
            .buffer<4>(
                post_buffer_invocation(
                    [&v](std::span<const std::byte> tmpbuf) {
                      assert(tmpbuf.size() == 4);
                      if (tmpbuf[0] != std::byte{0} ||
                          tmpbuf[1] != std::byte{0} ||
                          tmpbuf[2] != std::byte{0})
                        throw xdr_error("integer overflow for 8-bit value");
                      v = static_cast<std::uint8_t>(tmpbuf[3]);
                    })));
  }

  template<std::unsigned_integral T>
  auto write(const T& v) const {
    return unresolved_operation_block<true, std::tuple<>>{}
        .append_block(operation_block<true>{}
            .template buffer<4>(
                pre_buffer_invocation(
                    [&v](std::span<std::byte> tmpbuf) {
                      if (v > std::numeric_limits<std::uint8_t>::max())
                        throw xdr_error("integer overflow for 8-bit value");

                      std::array<std::byte, 4> bytes{ std::byte{}, std::byte{}, std::byte{}, static_cast<std::byte>(static_cast<std::uint8_t>(v)) };
                      assert(tmpbuf.size() == bytes.size());
                      std::copy_n(bytes.begin(), bytes.size(), tmpbuf.begin());
                    })));
  }

  template<std::signed_integral T>
  auto read(T& v) const {
    return unresolved_operation_block<false, std::tuple<>>{}
        .append_block(operation_block<false>{}
            .template buffer<4>(
                post_buffer_invocation(
                    [&v](std::span<const std::byte> tmpbuf) {
                      std::uint32_t v32;

                      const auto src = std::span<const std::byte, 4>(tmpbuf);
                      const auto dst = std::as_writable_bytes(std::span<std::uint32_t, 1>(&v32, 1));
                      assert(src.size() == dst.size());
                      std::copy(src.begin(), src.end(), dst.begin());
                      if constexpr(std::endian::native == std::endian::little) {
                        v32 = (v32 & 0xff000000u) >> 24
                            | (v32 & 0x00ff0000u) >>  8
                            | (v32 & 0x0000ff00u) <<  8
                            | (v32 & 0x000000ffu) << 24;
                      }

                      if (v32 > std::numeric_limits<std::uint8_t>::max() || v32 > std::numeric_limits<T>::max())
                        throw xdr_error("integer value too big to read");
                      v = v32;
                    })));
  }

  template<std::signed_integral T>
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    return unresolved_operation_block<true, std::tuple<>>{}
        .append_block(operation_block<true>{}
            .template buffer<4>(
                pre_buffer_invocation(
                    [&v](std::span<std::byte> tmpbuf) {
                      if (v < 0)
                        throw xdr_error("integer value too small to write");
                      if (v > std::numeric_limits<std::uint8_t>::max())
                        throw xdr_error("integer value too big to write");

                      std::uint32_t v32 = v;
                      if constexpr(std::endian::native == std::endian::little) {
                        v32 = (v32 & 0xff000000u) >> 24
                            | (v32 & 0x00ff0000u) >>  8
                            | (v32 & 0x0000ff00u) <<  8
                            | (v32 & 0x000000ffu) << 24;
                      }

                      auto bytes = std::as_bytes(std::span<std::uint32_t, 1>(&v32, 1));
                      assert(tmpbuf.size() == bytes.size());
                      std::copy_n(bytes.begin(), bytes.size(), tmpbuf.begin());
                    })));
  }
};

static_assert(invocation_for<uint8_t, std::uint8_t>);
static_assert(invocation_for<uint8_t, std::uint16_t>);
static_assert(invocation_for<uint8_t, std::uint32_t>);
static_assert(invocation_for<uint8_t, std::uint64_t>);
static_assert(invocation_for<uint8_t, std::uintmax_t>);
static_assert(invocation_for<uint8_t, std::int8_t>);
static_assert(invocation_for<uint8_t, std::int16_t>);
static_assert(invocation_for<uint8_t, std::int32_t>);
static_assert(invocation_for<uint8_t, std::int64_t>);
static_assert(invocation_for<uint8_t, std::intmax_t>);


struct uint16_t
: basic_operation<uint16_t>
{
  template<std::unsigned_integral T>
  auto read(T& v) const {
    return unresolved_operation_block<false, std::tuple<>>{}
        .append_block(operation_block<false>{}
            .buffer<4>(
                post_buffer_invocation(
                    [&v](std::span<const std::byte> tmpbuf) {
                      assert(tmpbuf.size() == 4);
                      if (tmpbuf[0] != std::byte{0} ||
                          tmpbuf[1] != std::byte{0})
                        throw xdr_error("integer overflow for 16-bit value");
                      v = 0x100u * static_cast<std::uint8_t>(tmpbuf[2]) + static_cast<std::uint8_t>(tmpbuf[3]);
                    })));
  }

  template<std::unsigned_integral T>
  auto write(T& v) const {
    return unresolved_operation_block<true, std::tuple<>>{}
        .append_block(operation_block<true>{}
            .template buffer<4>(
                pre_buffer_invocation(
                    [&v](std::span<std::byte> tmpbuf) {
                      if (v > std::numeric_limits<std::uint16_t>::max())
                        throw xdr_error("integer overflow for 16-bit value");

                      std::array<std::byte, 4> bytes{ std::byte{}, std::byte{}, static_cast<std::byte>(static_cast<std::uint8_t>(v >> 8 & 0xffu)), static_cast<std::byte>(static_cast<std::uint8_t>(v & 0xffu)) };
                      assert(tmpbuf.size() == bytes.size());
                      std::copy_n(bytes.begin(), bytes.size(), tmpbuf.begin());
                    })));
  }

  template<std::signed_integral T>
  auto read(T& v) const {
    return unresolved_operation_block<false, std::tuple<>>{}
        .append_block(operation_block<false>{}
            .template buffer<4>(
                post_buffer_invocation(
                    [&v](std::span<const std::byte> tmpbuf) {
                      std::uint32_t v32;

                      const auto src = std::span<const std::byte, 4>(tmpbuf);
                      const auto dst = std::as_writable_bytes(std::span<std::uint32_t, 1>(&v32, 1));
                      assert(src.size() == dst.size());
                      std::copy(src.begin(), src.end(), dst.begin());
                      if constexpr(std::endian::native == std::endian::little) {
                        v32 = (v32 & 0xff000000u) >> 24
                            | (v32 & 0x00ff0000u) >>  8
                            | (v32 & 0x0000ff00u) <<  8
                            | (v32 & 0x000000ffu) << 24;
                      }

                      if (v32 > std::numeric_limits<std::uint16_t>::max() || v32 > std::numeric_limits<T>::max())
                        throw xdr_error("integer value too big to read");
                      v = v32;
                    })));
  }

  template<std::signed_integral T>
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    return unresolved_operation_block<true, std::tuple<>>{}
        .append_block(operation_block<true>{}
            .template buffer<4>(
                pre_buffer_invocation(
                    [&v](std::span<std::byte> tmpbuf) {
                      if (v < 0)
                        throw xdr_error("integer value too small to write");
                      if (v > std::numeric_limits<std::uint16_t>::max())
                        throw xdr_error("integer value too big to write");

                      std::uint32_t v32 = v;
                      if constexpr(std::endian::native == std::endian::little) {
                        v32 = (v32 & 0xff000000u) >> 24
                            | (v32 & 0x00ff0000u) >>  8
                            | (v32 & 0x0000ff00u) <<  8
                            | (v32 & 0x000000ffu) << 24;
                      }

                      auto bytes = std::as_bytes(std::span<std::uint32_t, 1>(&v32, 1));
                      assert(tmpbuf.size() == bytes.size());
                      std::copy_n(bytes.begin(), bytes.size(), tmpbuf.begin());
                    })));
  }
};

static_assert(invocation_for<uint16_t, std::uint8_t>);
static_assert(invocation_for<uint16_t, std::uint16_t>);
static_assert(invocation_for<uint16_t, std::uint32_t>);
static_assert(invocation_for<uint16_t, std::uint64_t>);
static_assert(invocation_for<uint16_t, std::uintmax_t>);
static_assert(invocation_for<uint16_t, std::int8_t>);
static_assert(invocation_for<uint16_t, std::int16_t>);
static_assert(invocation_for<uint16_t, std::int32_t>);
static_assert(invocation_for<uint16_t, std::int64_t>);
static_assert(invocation_for<uint16_t, std::intmax_t>);


struct uint32_t
: basic_operation<uint32_t>
{
  template<std::unsigned_integral T>
  requires (sizeof(T) == 4)
  auto read(T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    if constexpr(std::endian::native == std::endian::big) {
      return unresolved_operation_block<false, std::tuple<>>{}
          .append_block(operation_block<false>{}
              .append(span_reference<false, 4>(std::as_writable_bytes(std::span<T, 1>(&v, 1)))));
    } else {
      return unresolved_operation_block<false, std::tuple<>>{}
          .append_block(operation_block<false>{}
              .append(span_reference<false, 4>(std::as_writable_bytes(std::span<T, 1>(&v, 1))))
              .append(
                  post_invocation(
                      [&v]() noexcept {
                        v = (v & 0xff000000u) >> 24
                          | (v & 0x00ff0000u) >>  8
                          | (v & 0x0000ff00u) <<  8
                          | (v & 0x000000ffu) << 24;
                      })));
    }
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) == 4)
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    if constexpr(std::endian::native == std::endian::big) {
      return unresolved_operation_block<true, std::tuple<>>{}
          .append_block(operation_block<true>{}
              .append(span_reference<true, 4>(std::as_bytes(std::span<T, 1>(&v, 1)))));
    } else if constexpr(std::endian::native == std::endian::little) {
      return unresolved_operation_block<true, std::tuple<>>{}
          .append_block(operation_block<true>{}
              .template buffer<4>(
                  pre_buffer_invocation(
                      [&v](std::span<std::byte> tmpbuf) {
                        const std::uint32_t x = (v & 0xff000000u) >> 24
                                              | (v & 0x00ff0000u) >>  8
                                              | (v & 0x0000ff00u) <<  8
                                              | (v & 0x000000ffu) << 24;

                        auto bytes = std::as_bytes(std::span<const std::uint32_t, 1>(&x, 1));
                        assert(tmpbuf.size() == bytes.size());
                        std::copy_n(bytes.begin(), bytes.size(), tmpbuf.begin());
                      })));
    }
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) < 4)
  auto read(T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    return unresolved_operation_block<false, std::tuple<>>{}
        .append_block(operation_block<false>{}
            .template buffer<4>(
                post_buffer_invocation(
                    [&v](std::span<const std::byte> tmpbuf) {
                      std::uint32_t v32;

                      const auto src = std::span<const std::byte, 4>(tmpbuf);
                      const auto dst = std::as_writable_bytes(std::span<std::uint32_t, 1>(&v32, 1));
                      assert(src.size() == dst.size());
                      std::copy(src.begin(), src.end(), dst.begin());
                      if constexpr(std::endian::native == std::endian::little) {
                        v32 = (v32 & 0xff000000u) >> 24
                            | (v32 & 0x00ff0000u) >>  8
                            | (v32 & 0x0000ff00u) <<  8
                            | (v32 & 0x000000ffu) << 24;
                      }
                      if (v32 > std::numeric_limits<T>::max())
                        throw xdr_error("integer value too big to read");
                      v = v32;
                    })));
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) < 4)
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    return unresolved_operation_block<true, std::tuple<>>{}
        .append_block(operation_block<true>{}
            .template buffer<4>(
                pre_buffer_invocation(
                    [&v](std::span<std::byte> tmpbuf) {
                      std::uint32_t v32 = v;
                      if constexpr(std::endian::native == std::endian::little) {
                        v32 = (v32 & 0xff000000u) >> 24
                            | (v32 & 0x00ff0000u) >>  8
                            | (v32 & 0x0000ff00u) <<  8
                            | (v32 & 0x000000ffu) << 24;
                      }

                      auto bytes = std::as_bytes(std::span<std::uint32_t, 1>(&v32, 1));
                      assert(tmpbuf.size() == bytes.size());
                      std::copy_n(bytes.begin(), bytes.size(), tmpbuf.begin());
                    })));
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) > 4)
  auto read(T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    return unresolved_operation_block<false, std::tuple<>>{}
        .append_block(operation_block<false>{}
            .template buffer<4>(
                post_buffer_invocation(
                    [&v](std::span<const std::byte> tmpbuf) {
                      std::uint32_t v32;

                      const auto src = std::span<const std::byte, 4>(tmpbuf);
                      const auto dst = std::as_writable_bytes(std::span<std::uint32_t, 1>(&v32, 1));
                      assert(src.size() == dst.size());
                      std::copy(src.begin(), src.end(), dst.begin());
                      if constexpr(std::endian::native == std::endian::little) {
                        v32 = (v32 & 0xff000000u) >> 24
                            | (v32 & 0x00ff0000u) >>  8
                            | (v32 & 0x0000ff00u) <<  8
                            | (v32 & 0x000000ffu) << 24;
                      }
                      v = v32;
                    })));
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) > 4)
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    return unresolved_operation_block<true, std::tuple<>>{}
        .append_block(operation_block<true>{}
            .template buffer<4>(
                pre_buffer_invocation(
                    [&v](std::span<std::byte> tmpbuf) {
                      if (v > std::numeric_limits<std::uint32_t>::max())
                        throw xdr_error("integer value too big to write");

                      std::uint32_t v32 = v;
                      if constexpr(std::endian::native == std::endian::little) {
                        v32 = (v32 & 0xff000000u) >> 24
                            | (v32 & 0x00ff0000u) >>  8
                            | (v32 & 0x0000ff00u) <<  8
                            | (v32 & 0x000000ffu) << 24;
                      }

                      auto bytes = std::as_bytes(std::span<std::uint32_t, 1>(&v32, 1));
                      assert(tmpbuf.size() == bytes.size());
                      std::copy_n(bytes.begin(), bytes.size(), tmpbuf.begin());
                    })));
  }

  template<std::signed_integral T>
  auto read(T& v) const {
    return unresolved_operation_block<false, std::tuple<>>{}
        .append_block(operation_block<false>{}
            .template buffer<4>(
                post_buffer_invocation(
                    [&v](std::span<const std::byte> tmpbuf) {
                      std::uint32_t v32;

                      const auto src = std::span<const std::byte, 4>(tmpbuf);
                      const auto dst = std::as_writable_bytes(std::span<std::uint32_t, 1>(&v32, 1));
                      assert(src.size() == dst.size());
                      std::copy(src.begin(), src.end(), dst.begin());
                      if constexpr(std::endian::native == std::endian::little) {
                        v32 = (v32 & 0xff000000u) >> 24
                            | (v32 & 0x00ff0000u) >>  8
                            | (v32 & 0x0000ff00u) <<  8
                            | (v32 & 0x000000ffu) << 24;
                      }

                      if (v32 > std::numeric_limits<T>::max())
                        throw xdr_error("integer value too big to read");
                      v = v32;
                    })));
  }

  template<std::signed_integral T>
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    return unresolved_operation_block<true, std::tuple<>>{}
        .append_block(operation_block<true>{}
            .template buffer<4>(
                pre_buffer_invocation(
                    [&v](std::span<std::byte> tmpbuf) {
                      if (v < 0)
                        throw xdr_error("integer value too small to write");
                      if (v > std::numeric_limits<std::uint32_t>::max())
                        throw xdr_error("integer value too big to write");

                      std::uint32_t v32 = v;
                      if constexpr(std::endian::native == std::endian::little) {
                        v32 = (v32 & 0xff000000u) >> 24
                            | (v32 & 0x00ff0000u) >>  8
                            | (v32 & 0x0000ff00u) <<  8
                            | (v32 & 0x000000ffu) << 24;
                      }

                      auto bytes = std::as_bytes(std::span<std::uint32_t, 1>(&v32, 1));
                      assert(tmpbuf.size() == bytes.size());
                      std::copy_n(bytes.begin(), bytes.size(), tmpbuf.begin());
                    })));
  }
};

static_assert(invocation_for<uint32_t, std::uint8_t>);
static_assert(invocation_for<uint32_t, std::uint16_t>);
static_assert(invocation_for<uint32_t, std::uint32_t>);
static_assert(invocation_for<uint32_t, std::uint64_t>);
static_assert(invocation_for<uint32_t, std::uintmax_t>);
static_assert(invocation_for<uint32_t, std::int8_t>);
static_assert(invocation_for<uint32_t, std::int16_t>);
static_assert(invocation_for<uint32_t, std::int32_t>);
static_assert(invocation_for<uint32_t, std::int64_t>);
static_assert(invocation_for<uint32_t, std::intmax_t>);


struct uint64_t
: basic_operation<uint64_t>
{
  template<std::unsigned_integral T>
  requires (sizeof(T) == 8)
  auto read(T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    if constexpr(std::endian::native == std::endian::big) {
      return unresolved_operation_block<false, std::tuple<>>{}
          .append_block(operation_block<false>{}
              .append(span_reference<false, 8>(std::as_writable_bytes(std::span<T, 1>(&v, 1)))));
    } else {
      return unresolved_operation_block<false, std::tuple<>>{}
          .append_block(operation_block<false>{}
              .append(span_reference<false, 8>(std::as_writable_bytes(std::span<T, 1>(&v, 1))))
              .append(
                  post_invocation(
                      [&v]() noexcept {
                        v = (v & 0xff00000000000000ull) >> 56
                          | (v & 0x00ff000000000000ull) >> 40
                          | (v & 0x0000ff0000000000ull) >> 24
                          | (v & 0x000000ff00000000ull) >>  8
                          | (v & 0x00000000ff000000ull) <<  8
                          | (v & 0x0000000000ff0000ull) << 24
                          | (v & 0x000000000000ff00ull) << 40
                          | (v & 0x00000000000000ffull) << 56;
                      })));
    }
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) == 8)
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    if constexpr(std::endian::native == std::endian::big) {
      return unresolved_operation_block<true, std::tuple<>>{}
          .append_block(operation_block<true>{}
              .append(span_reference<true, 8>(std::as_bytes(std::span<T, 1>(&v, 1)))));
    } else if constexpr(std::endian::native == std::endian::little) {
      return unresolved_operation_block<true, std::tuple<>>{}
          .append_block(operation_block<true>{}
              .template buffer<8>(
                  pre_buffer_invocation(
                      [&v](std::span<std::byte> tmpbuf) {
                        const std::uint64_t x = (v & 0xff00000000000000ull) >> 56
                                              | (v & 0x00ff000000000000ull) >> 40
                                              | (v & 0x0000ff0000000000ull) >> 24
                                              | (v & 0x000000ff00000000ull) >>  8
                                              | (v & 0x00000000ff000000ull) <<  8
                                              | (v & 0x0000000000ff0000ull) << 24
                                              | (v & 0x000000000000ff00ull) << 40
                                              | (v & 0x00000000000000ffull) << 56;

                        auto bytes = std::as_bytes(std::span<const std::uint64_t, 1>(&x, 1));
                        assert(tmpbuf.size() == bytes.size());
                        std::copy_n(bytes.begin(), bytes.size(), tmpbuf.begin());
                      })));
    }
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) < 8)
  auto read(T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    return unresolved_operation_block<false, std::tuple<>>{}
        .append_block(operation_block<false>{}
            .template buffer<8>(
                post_buffer_invocation(
                    [&v](std::span<const std::byte> tmpbuf) {
                      std::uint64_t v64;

                      const auto src = std::span<const std::byte, 8>(tmpbuf);
                      const auto dst = std::as_writable_bytes(std::span<std::uint64_t, 1>(&v64, 1));
                      assert(src.size() == dst.size());
                      std::copy(src.begin(), src.end(), dst.begin());
                      if constexpr(std::endian::native == std::endian::little) {
                        v64 = (v64 & 0xff00000000000000ull) >> 56
                            | (v64 & 0x00ff000000000000ull) >> 40
                            | (v64 & 0x0000ff0000000000ull) >> 24
                            | (v64 & 0x000000ff00000000ull) >>  8
                            | (v64 & 0x00000000ff000000ull) <<  8
                            | (v64 & 0x0000000000ff0000ull) << 24
                            | (v64 & 0x000000000000ff00ull) << 40
                            | (v64 & 0x00000000000000ffull) << 56;
                      }
                      if (v64 > std::numeric_limits<T>::max())
                        throw xdr_error("integer value too big to read");
                      v = v64;
                    })));
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) < 8)
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    return unresolved_operation_block<true, std::tuple<>>{}
        .append_block(operation_block<true>{}
            .template buffer<8>(
                pre_buffer_invocation(
                    [&v](std::span<std::byte> tmpbuf) {
                      std::uint64_t v64 = v;
                      if constexpr(std::endian::native == std::endian::little) {
                        v64 = (v64 & 0xff00000000000000ull) >> 56
                            | (v64 & 0x00ff000000000000ull) >> 40
                            | (v64 & 0x0000ff0000000000ull) >> 24
                            | (v64 & 0x000000ff00000000ull) >>  8
                            | (v64 & 0x00000000ff000000ull) <<  8
                            | (v64 & 0x0000000000ff0000ull) << 24
                            | (v64 & 0x000000000000ff00ull) << 40
                            | (v64 & 0x00000000000000ffull) << 56;
                      }

                      auto bytes = std::as_bytes(std::span<std::uint64_t, 1>(&v64, 1));
                      assert(tmpbuf.size() == bytes.size());
                      std::copy_n(bytes.begin(), bytes.size(), tmpbuf.begin());
                    })));
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) > 8)
  auto read(T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    return unresolved_operation_block<false, std::tuple<>>{}
        .append_block(operation_block<false>{}
            .template buffer<8>(
                post_buffer_invocation(
                    [&v](std::span<const std::byte> tmpbuf) {
                      std::uint64_t v64;

                      const auto src = std::span<const std::byte, 8>(tmpbuf);
                      const auto dst = std::as_writable_bytes(std::span<std::uint64_t, 1>(&v64, 1));
                      assert(src.size() == dst.size());
                      std::copy(src.begin(), src.end(), dst.begin());
                      if constexpr(std::endian::native == std::endian::little) {
                        v64 = (v64 & 0xff00000000000000ull) >> 56
                            | (v64 & 0x00ff000000000000ull) >> 40
                            | (v64 & 0x0000ff0000000000ull) >> 24
                            | (v64 & 0x000000ff00000000ull) >>  8
                            | (v64 & 0x00000000ff000000ull) <<  8
                            | (v64 & 0x0000000000ff0000ull) << 24
                            | (v64 & 0x000000000000ff00ull) << 40
                            | (v64 & 0x00000000000000ffull) << 56;
                      }
                      v = v64;
                    })));
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) > 8)
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    return unresolved_operation_block<true, std::tuple<>>{}
        .append_block(operation_block<true>{}
            .template buffer<8>(
                pre_buffer_invocation(
                    [&v](std::span<std::byte> tmpbuf) {
                      if (v > std::numeric_limits<std::uint64_t>::max())
                        throw xdr_error("integer value too big to write");

                      std::uint64_t v64 = v;
                      if constexpr(std::endian::native == std::endian::little) {
                        v64 = (v64 & 0xff00000000000000ull) >> 56
                            | (v64 & 0x00ff000000000000ull) >> 40
                            | (v64 & 0x0000ff0000000000ull) >> 24
                            | (v64 & 0x000000ff00000000ull) >>  8
                            | (v64 & 0x00000000ff000000ull) <<  8
                            | (v64 & 0x0000000000ff0000ull) << 24
                            | (v64 & 0x000000000000ff00ull) << 40
                            | (v64 & 0x00000000000000ffull) << 56;
                      }

                      auto bytes = std::as_bytes(std::span<std::uint64_t, 1>(&v64, 1));
                      assert(tmpbuf.size() == bytes.size());
                      std::copy_n(bytes.begin(), bytes.size(), tmpbuf.begin());
                    })));
  }

  template<std::signed_integral T>
  auto read(T& v) const {
    return unresolved_operation_block<false, std::tuple<>>{}
        .append_block(operation_block<false>{}
            .template buffer<8>(
                post_buffer_invocation(
                    [&v](std::span<const std::byte> tmpbuf) {
                      std::uint64_t v64;

                      const auto src = std::span<const std::byte, 8>(tmpbuf);
                      const auto dst = std::as_writable_bytes(std::span<std::uint64_t, 1>(&v64, 1));
                      assert(src.size() == dst.size());
                      std::copy(src.begin(), src.end(), dst.begin());
                      if constexpr(std::endian::native == std::endian::little) {
                        v64 = (v64 & 0xff00000000000000ull) >> 56
                            | (v64 & 0x00ff000000000000ull) >> 40
                            | (v64 & 0x0000ff0000000000ull) >> 24
                            | (v64 & 0x000000ff00000000ull) >>  8
                            | (v64 & 0x00000000ff000000ull) <<  8
                            | (v64 & 0x0000000000ff0000ull) << 24
                            | (v64 & 0x000000000000ff00ull) << 40
                            | (v64 & 0x00000000000000ffull) << 56;
                      }

                      if (v64 > std::numeric_limits<T>::max())
                        throw xdr_error("integer value too big to read");
                      v = v64;
                    })));
  }

  template<std::signed_integral T>
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    return unresolved_operation_block<true, std::tuple<>>{}
        .append_block(operation_block<true>{}
            .template buffer<8>(
                pre_buffer_invocation(
                    [&v](std::span<std::byte> tmpbuf) {
                      if (v < 0)
                        throw xdr_error("integer value too small to write");
                      if (v > std::numeric_limits<std::uint64_t>::max())
                        throw xdr_error("integer value too big to write");

                      std::uint64_t v64 = v;
                      if constexpr(std::endian::native == std::endian::little) {
                        v64 = (v64 & 0xff00000000000000ull) >> 56
                            | (v64 & 0x00ff000000000000ull) >> 40
                            | (v64 & 0x0000ff0000000000ull) >> 24
                            | (v64 & 0x000000ff00000000ull) >>  8
                            | (v64 & 0x00000000ff000000ull) <<  8
                            | (v64 & 0x0000000000ff0000ull) << 24
                            | (v64 & 0x000000000000ff00ull) << 40
                            | (v64 & 0x00000000000000ffull) << 56;
                      }

                      auto bytes = std::as_bytes(std::span<std::uint64_t, 1>(&v64, 1));
                      assert(tmpbuf.size() == bytes.size());
                      std::copy_n(bytes.begin(), bytes.size(), tmpbuf.begin());
                    })));
  }
};

static_assert(invocation_for<uint64_t, std::uint8_t>);
static_assert(invocation_for<uint64_t, std::uint16_t>);
static_assert(invocation_for<uint64_t, std::uint32_t>);
static_assert(invocation_for<uint64_t, std::uint64_t>);
static_assert(invocation_for<uint64_t, std::uintmax_t>);
static_assert(invocation_for<uint64_t, std::int8_t>);
static_assert(invocation_for<uint64_t, std::int16_t>);
static_assert(invocation_for<uint64_t, std::int32_t>);
static_assert(invocation_for<uint64_t, std::int64_t>);
static_assert(invocation_for<uint64_t, std::intmax_t>);


// Manual control of an operation.
//
// Requires a function, that when called as `fn(a_file_descriptor)'
// returns a typed-sender, that returns a file-descriptor.
// It doesn't have to be the same file-descriptor: it's fine if it's
// a different one, or even changes type.
struct manual_t {
  constexpr auto operator()() const noexcept -> manual_t {
    return *this;
  }

  template<typename Fn>
  auto read(Fn&& fn) const {
    return unresolved_operation_block<false, std::tuple<>>{}
        .append_block(operation_block<false>{}
        .append_sequence(manual_invocation(std::forward<Fn>(fn))));
  }

  template<typename Fn>
  auto write(Fn&& fn) const {
    return unresolved_operation_block<true, std::tuple<>>{}
        .append_block(operation_block<true>{}
        .append_sequence(manual_invocation(std::forward<Fn>(fn))));
  }
};


// Add between 0 and 3 padding bytes.
//
// During reads, confirms the padding bytes are zeroed.
struct padding_t
: basic_operation<padding_t>
{
  struct value {
    std::uint8_t len = 0;
    std::array<std::byte, 3> buf = { std::byte{0}, std::byte{0}, std::byte{0} };
  };

  auto read(value& v) const {
    return unresolved_operation_block<false, std::tuple<>>{}
        .append_block(operation_block<false>{}
            .append(dynamic_span_reference(
                    [&v]() {
                      if (v.len > v.buf.size()) throw std::logic_error("xdr: incorrect padding instruction");
                      return std::span<std::byte>(v.buf.data(), v.buf.size()).subspan(0, v.len);
                    }))
            .append(
                post_invocation(
                    [&v]() {
                      if (std::any_of(v.buf.begin(), v.buf.end(),
                              [](std::byte b) {
                                return b != std::byte{0};
                              }))
                        throw xdr_error("padding with non-zeroed bytes");
                    })));
  }

  auto write(const value& v) const {
    return unresolved_operation_block<true, std::tuple<>>{}
        .append_block(operation_block<true>{}
            .append(dynamic_span_reference(
                    [&v]() {
                      if (v.len > v.buf.size()) throw std::logic_error("xdr: incorrect padding instruction");
                      return std::span<const std::byte>(v.buf.data(), v.buf.size()).subspan(0, v.len);
                    })));
  }
};

static_assert(invocation_for<padding_t, padding_t::value>);


template<std::size_t Idx, typename T>
struct tuple_element_of_type {
  static inline constexpr std::size_t index = Idx;
  using type = T;

  template<typename Temporaries>
  static auto get(Temporaries& temporaries) -> type& {
    return std::get<index>(temporaries);
  }

  template<typename Temporaries>
  static auto get(const Temporaries& temporaries) -> const type& {
    return std::get<index>(temporaries);
  }
};


template<typename Fn, typename... TupleElementOfType>
class unresolved_operation;

template<typename Fn, std::size_t... Idx, typename... T>
class unresolved_operation<Fn, tuple_element_of_type<Idx, T>...> {
  static_assert(std::invocable<Fn&&, T&...>);

  private:
  using resolved_block_type = std::invoke_result_t<Fn, T&...>;

  public:
  static inline constexpr std::size_t extent = resolved_block_type::extent;
  static inline constexpr bool for_writing = resolved_block_type::for_writing;
  static inline constexpr bool for_reading = resolved_block_type::for_reading;

  template<typename Fn_>
  explicit unresolved_operation(Fn_&& fn)
  noexcept(std::is_nothrow_constructible_v<Fn, Fn_>)
  : fn(std::forward<Fn_>(fn))
  {}

  template<typename Temporaries>
  auto resolve(Temporaries& temporaries) && {
    return maybe_insulate_(std::invoke(std::move(fn), tuple_element_of_type<Idx, T>::get(temporaries)...));
  }

  template<std::size_t N>
  auto temporaries_shift() && -> unresolved_operation<Fn, tuple_element_of_type<Idx + N, T>...> {
    return unresolved_operation<Fn, tuple_element_of_type<Idx + N, T>...>(std::move(fn));
  }

  private:
  // When this operation is resolved, all temporaries have already been declared (and bound).
  // So if the `op' requires its own temporaries (aka, it's an unresolved operation),
  // then it needs to be insulated:
  // its operation needs to be run separately from the containing operation.
  template<typename Op>
  static auto maybe_insulate_(Op&& op) {
    if constexpr(!is_unresolved_operation_block<Op>) {
      return std::forward<Op>(op);
    } else if constexpr(std::tuple_size_v<typename std::remove_cvref_t<Op>::temporaries> == 0) {
      // If the unresolved-operation doesn't use temporaries, it's always embeddable,
      // and we need no insulation.
      std::tuple<> no_temporaries;
      return std::move(op).resolve(no_temporaries);
    } else {
      constexpr bool is_const = unresolved_operation_block_is_const_<std::remove_cvref_t<Op>>::value;
      return operation_block<is_const>{}
          .append_sequence(
              manual_invocation(
                  [op=std::forward<Op>(op)](auto& fd) mutable {
                    return execution::just(std::move(fd))
                    | std::move(op).sender_chain();
                  }));
    }
  }

  Fn fn;
};


// Read/write a temporary variable.
template<typename TupleElementOfType>
struct readwrite_temporary_variable_t;

template<std::size_t Idx, typename T>
struct readwrite_temporary_variable_t<tuple_element_of_type<Idx, T>> {
  private:
  template<typename Invocation>
  requires read_invocation_for<Invocation, T> || write_invocation_for<Invocation, T>
  struct bound {
    explicit constexpr bound(const Invocation& invocation)
    : invocation(invocation)
    {}

    explicit constexpr bound(Invocation&& invocation)
    : invocation(std::move(invocation))
    {}

    auto read() const {
      return readwrite_temporary_variable_t{}.read(invocation);
    }

    auto write() const {
      return readwrite_temporary_variable_t{}.write(invocation);
    }

    private:
    [[no_unique_address]] Invocation invocation;
  };

  public:
  template<typename Invocation>
  constexpr auto operator()(Invocation&& invocation) const {
    return bound<std::remove_cvref_t<Invocation>>(std::forward<Invocation>(invocation));
  }

  template<typename Invocation>
  requires requires(Invocation&& invocation, T& v) {
    { std::move(invocation).read(v) };
  }
  auto read(Invocation&& invocation) const {
    auto fn = [invocation=std::forward<Invocation>(invocation)](auto& temporary) mutable {
      return std::move(invocation).read(temporary);
    };

    return unresolved_operation<decltype(fn), tuple_element_of_type<Idx, T>>(std::move(fn));
  }

  template<typename Invocation>
  requires requires(Invocation&& invocation, T& v) {
    { std::move(invocation).write(v) };
  }
  auto write(Invocation&& invocation) const {
    auto fn = [invocation=std::forward<Invocation>(invocation)](auto& temporary) mutable {
      return std::move(invocation).write(temporary);
    };

    return unresolved_operation<decltype(fn), tuple_element_of_type<Idx, T>>(std::move(fn));
  }
};
template<typename TupleElementOfType>
inline constexpr readwrite_temporary_variable_t<TupleElementOfType> readwrite_temporary_variable{};


// Read/write a constant value.
struct constant_t
: basic_operation<constant_t>
{
  template<typename T, typename Invocation>
  auto read(T&& v, Invocation&& invocation) const {
    auto confirmation_fn = [expected=std::forward<T>(v)](std::remove_cvref_t<T>& decoded) {
      if (decoded != expected) throw xdr_error("mismatched constant");
    };

    return unresolved_operation_block<false, std::tuple<std::remove_cvref_t<T>>>{}
        .append_block(readwrite_temporary_variable<tuple_element_of_type<0, std::remove_cvref_t<T>>>(std::forward<Invocation>(invocation)).read())
        .append_block(operation_block<false>{}.append(post_invocation<decltype(confirmation_fn), 0>(std::move(confirmation_fn))));
  }

  template<typename T, typename Invocation>
  auto write(T&& v, Invocation&& invocation) const {
    auto initialize_fn = [initial_value=std::forward<T>(v)](std::remove_cvref_t<T>& to_be_encoded) mutable {
      to_be_encoded = std::move(initial_value);
    };

    return unresolved_operation_block<true, std::tuple<std::remove_cvref_t<T>>>{}
        .append_block(operation_block<true>{}.append(pre_invocation<decltype(initialize_fn), 0>(std::move(initialize_fn))))
        .append_block(readwrite_temporary_variable<tuple_element_of_type<0, std::remove_cvref_t<T>>>(std::forward<Invocation>(invocation)).write());
  }
};


template<typename S, std::size_t ElementSize>
concept string =
    (std::integral<typename std::remove_cvref_t<S>::value_type> || std::same_as<std::byte, typename std::remove_cvref_t<S>::value_type>) &&
    (sizeof(typename std::remove_cvref_t<S>::value_type) == ElementSize) &&
    requires(S s) {
      { s.data() } -> std::convertible_to<const typename std::remove_cvref_t<S>::value_type*>;
      { s.size() } -> std::convertible_to<std::size_t>;
    };

template<typename S, std::size_t ElementSize>
concept variable_string =
    string<S, ElementSize> &&
    requires(std::remove_cvref_t<S> s, std::size_t sz) {
      { s.resize(sz) };
    };


// Read a byte string of fixed size.
struct fixed_byte_string_t
: basic_operation<fixed_byte_string_t>
{
  private:
  template<string<1> String>
  static auto fix_padding_fn(const String& v) noexcept {
    return [&v](padding_t::value& padding) {
      padding.len = (std::size_t(0) - static_cast<std::size_t>(v.size())) % 4u;
    };
  }

  public:
  template<string<1> String>
  requires (sizeof(typename String::value_type) == 1)
  auto read(String& v) const {
    using value_type = typename std::remove_cvref_t<String>::value_type;

    return unresolved_operation_block<false, std::tuple<padding_t::value>>{}
        .append_block(operation_block<false>{}
            .append(pre_invocation<decltype(fix_padding_fn(v)), 0>(std::move(fix_padding_fn(v))))
            .append(
                dynamic_span_reference(
                    [&v]() {
                      return std::as_writable_bytes(std::span<value_type>(v.data(), v.size()));
                    })))
        .append_block(readwrite_temporary_variable<tuple_element_of_type<0, padding_t::value>>.read(padding_t{}));
  }

  template<string<1> String>
  requires (sizeof(typename String::value_type) == 1)
  auto write(const String& v) const {
    using value_type = typename std::remove_cvref_t<String>::value_type;

    return unresolved_operation_block<true, std::tuple<padding_t::value>>{}
        .append_block(operation_block<true>{}
            .append(pre_invocation<decltype(fix_padding_fn(v)), 0>(std::move(fix_padding_fn(v))))
            .append(
                dynamic_span_reference(
                    [&v]() {
                      return std::as_bytes(std::span<const value_type>(v.data(), v.size()));
                    })))
        .append_block(readwrite_temporary_variable<tuple_element_of_type<0, padding_t::value>>.write(padding_t{}));
  }
};

static_assert(invocation_for<fixed_byte_string_t, std::string>);
static_assert(invocation_for<fixed_byte_string_t, std::array<std::uint8_t, 12>>);

// Read a byte string of variable size.
struct byte_string_t
: basic_operation<byte_string_t>
{
  template<variable_string<1> String>
  requires (sizeof(typename String::value_type) == 1)
  auto read(String& v, std::optional<std::size_t> max_size = std::nullopt) const {
    using size_type = typename String::size_type;

    auto apply_size_fn = [&v, max_size](auto sz) {
      if (max_size.has_value() && sz > max_size)
        throw xdr_error("string too large");
      v.resize(sz);
    };

    return unresolved_operation_block<false, std::tuple<size_type>>{}
        .append_block(readwrite_temporary_variable<tuple_element_of_type<0, size_type>>.read(uint32_t{}))
        .append_block(operation_block<false>{}
            .append(post_invocation<decltype(apply_size_fn), 0>(std::move(apply_size_fn)))
            .append_sequence(io_barrier{}))
        .merge(fixed_byte_string_t{}.read(v));
  }

  template<string<1> String>
  requires (sizeof(typename String::value_type) == 1)
  auto write(const String& v, std::optional<std::size_t> max_size = std::nullopt) const {
    using size_type = typename String::size_type;

    auto set_size_fn = [&v, max_size](auto& sz) {
      if (max_size.has_value() && v.size() > max_size.value())
        throw xdr_error("string too large");
      sz = v.size();
    };

    return unresolved_operation_block<true, std::tuple<size_type>>{}
        .append_block(operation_block<true>{}
            .append(pre_invocation<decltype(set_size_fn), 0>(std::move(set_size_fn))))
        .append_block(readwrite_temporary_variable<tuple_element_of_type<0, size_type>>.write(uint32_t{}))
        .merge(fixed_byte_string_t{}.write(v));
  }
};

static_assert(invocation_for<byte_string_t, std::string>);
static_assert(invocation_for<byte_string_t, std::vector<std::uint8_t>>);
static_assert(invocation_for<decltype(byte_string_t{}(12)), std::string>);
static_assert(invocation_for<decltype(byte_string_t{}(12)), std::vector<std::uint8_t>>);

// Read/write a 7-bit ascii string value.
struct fixed_ascii_string_t
: basic_operation<fixed_ascii_string_t>
{
  template<string<1> String>
  requires (sizeof(typename String::value_type) == 1)
  auto read(String& v) const {
    return fixed_byte_string_t{}.read(v)
        .append_block(operation_block<false>{}
            .append(
                post_invocation(
                    [&v]() {
                      if (std::any_of(v.begin(), v.end(),
                              [](const auto& c) {
                                return c < typename String::value_type(0) || c > typename String::value_type(0x7f);
                              }))
                        throw xdr_error("character outside ascii range");
                    })));
  }

  template<string<1> String>
  requires (sizeof(typename String::value_type) == 1)
  auto write(const String& v) const {
    return unresolved_operation_block<true, std::tuple<>>{}
        .append_block(operation_block<true>{}
            .append(
                pre_invocation(
                    [&v]() {
                      if (std::any_of(v.begin(), v.end(),
                              [](const auto& c) {
                                return c < typename String::value_type(0) || c > typename String::value_type(0x7f);
                              }))
                        throw xdr_error("character outside ascii range");
                    })))
        .merge(fixed_byte_string_t{}.write(v));
  }
};

static_assert(invocation_for<fixed_ascii_string_t, std::string>);
static_assert(invocation_for<fixed_ascii_string_t, std::array<std::uint8_t, 12>>);

// Read/write a 7-bit ascii string value.
struct ascii_string_t
: basic_operation<ascii_string_t>
{
  template<variable_string<1> String>
  requires (sizeof(typename String::value_type) == 1)
  auto read(String& v, std::optional<std::size_t> max_size = std::nullopt) const {
    return byte_string_t{}.read(v, max_size)
        .append_block(operation_block<false>{}
            .append(
                post_invocation(
                    [&v]() {
                      if (std::any_of(v.begin(), v.end(),
                              [](const auto& c) {
                                return c < typename String::value_type(0) || c > typename String::value_type(0x7f);
                              }))
                        throw xdr_error("character outside ascii range");
                    })));
  }

  template<string<1> String>
  requires (sizeof(typename String::value_type) == 1)
  auto write(const String& v, std::optional<std::size_t> max_size = std::nullopt) const {
    return unresolved_operation_block<true, std::tuple<>>{}
        .append_block(operation_block<true>{}
            .append(
                pre_invocation(
                    [&v]() {
                      if (std::any_of(v.begin(), v.end(),
                              [](const auto& c) {
                                return c < typename String::value_type(0) || c > typename String::value_type(0x7f);
                              }))
                        throw xdr_error("character outside ascii range");
                    })))
        .merge(byte_string_t{}.write(v, max_size));
  }
};

static_assert(invocation_for<ascii_string_t, std::string>);
static_assert(invocation_for<ascii_string_t, std::vector<std::uint8_t>>);
static_assert(invocation_for<decltype(ascii_string_t{}(12)), std::string>);
static_assert(invocation_for<decltype(ascii_string_t{}(12)), std::vector<std::uint8_t>>);


// Read/write a collection of fixed size.
struct fixed_collection_t
: basic_operation<fixed_collection_t>
{
  template<typename Collection, read_invocation_for<typename Collection::value_type> Invocation>
  auto read(Collection& collection, Invocation invocation) const {
    return manual_t{}.read(
        [&collection, invocation=std::move(invocation)](auto& fd) {
          using std::begin;
          using std::end;
          using execution::just;
          using execution::then;
          using execution::lazy_then;
          using execution::repeat;

          auto factory = [invocation=std::move(invocation)](auto fd, auto& iter) {
            return just(std::move(fd))
            | invocation.read(*iter).sender_chain()
            | lazy_then(
                [&iter]([[maybe_unused]] auto post_invocation_fd) {
                  ++iter;
                });
          };

          return just(std::move(fd), begin(collection), end(collection))
          | repeat(
              [factory]([[maybe_unused]] std::size_t idx, auto& fd, auto& iter, auto& end) {
                using result_type = decltype(factory(std::ref(fd), iter));
                if (iter != end)
                  return std::make_optional(factory(std::ref(fd), iter));
                else
                  return std::optional<result_type>{};
              })
          | then(
              []<typename FD>(FD&& fd, [[maybe_unused]] auto&& iter, [[maybe_unused]] auto&& end) -> decltype(auto) {
                return std::forward<FD>(fd);
              });
        });
  }

  template<typename Collection, write_invocation_for<typename Collection::value_type> Invocation>
  auto write(const Collection& collection, Invocation invocation) const {
    return manual_t{}.write(
        [&collection, invocation=std::move(invocation)](auto& fd) {
          using std::begin;
          using std::end;
          using execution::just;
          using execution::then;
          using execution::repeat;

          auto factory = [invocation=std::move(invocation)](auto fd, auto& iter) {
            return just(std::move(fd))
            | invocation.write(*iter).sender_chain()
            | then(
                [&iter]([[maybe_unused]] auto post_invocation_fd) {
                  ++iter;
                });
          };

          return just(std::move(fd), begin(collection), end(collection))
          | repeat(
              [factory]([[maybe_unused]] std::size_t idx, auto& fd, auto& iter, auto& end) {
                using result_type = decltype(factory(std::ref(fd), iter));
                if (iter != end)
                  return std::make_optional(factory(std::ref(fd), iter));
                else
                  return std::optional<result_type>{};
              })
          | then(
              []<typename FD>(FD&& fd, [[maybe_unused]] auto&& iter, [[maybe_unused]] auto&& end) -> decltype(auto) {
                return std::forward<FD>(fd);
              });
        });
  }
};

static_assert(invocation_for<decltype(fixed_collection_t{}(ascii_string_t{})), std::array<std::string, 12>>);
static_assert(invocation_for<decltype(fixed_collection_t{}(ascii_string_t{}(12))), std::array<std::string, 12>>);


// Read/write a collection of arbitrary size.
struct collection_t
: basic_operation<collection_t>
{
  template<typename Collection, read_invocation_for<typename Collection::value_type> Invocation>
  requires requires(Collection collection, std::size_t sz) {
    { collection.resize(sz) };
  }
  auto read(Collection& collection, Invocation invocation, std::optional<std::size_t> max_size = std::nullopt) const {
    using size_type = typename Collection::size_type;

    auto apply_size_fn = [&collection, max_size](auto sz) {
      if (max_size.has_value() && sz > max_size)
        throw xdr_error("collection too large");
      collection.resize(sz);
    };

    return unresolved_operation_block<false, std::tuple<size_type>>{}
        .append_block(readwrite_temporary_variable<tuple_element_of_type<0, size_type>>.read(uint32_t{}))
        .append_block(operation_block<false>{}
            .append(post_invocation<decltype(apply_size_fn), 0>(std::move(apply_size_fn)))
            .append_sequence(io_barrier{}))
        .merge(fixed_collection_t{}.read(collection, std::move(invocation)));
  }

  // When we cannot resize the collection,
  // we'll need to read elements one-at-a-time and insert them.
  template<typename Collection, read_invocation_for<typename Collection::value_type> Invocation>
  requires (!requires(Collection collection, std::size_t sz) {
        { collection.resize(sz) };
      })
  auto read(Collection& collection, Invocation invocation, std::optional<std::size_t> max_size = std::nullopt) const {
    using size_type = typename Collection::size_type;
    using value_type = typename Collection::value_type;

    auto apply_size_fn = [&collection, max_size](auto sz) {
      if (max_size.has_value() && sz > max_size)
        throw xdr_error("collection too large");
      collection.clear();
    };

    auto confirm_size_fn = [&collection](auto sz) {
      if (collection.size() != sz)
        throw xdr_error("collection size mismatch after reading");
    };

    auto reader = [&collection, invocation=std::move(invocation)](auto& fd, size_type sz) {
      using std::begin;
      using std::end;
      using execution::just;
      using execution::then;
      using execution::repeat;

      auto factory = [&collection, invocation=std::move(invocation)](auto fd) {
        auto inserter = [&collection](value_type& value) {
          if constexpr(requires(Collection c, value_type v) { c.push_back(v); })
            collection.push_back(std::move(value));
          else
            collection.insert(collection.end(), std::move(value));
        };

        auto read_1 = unresolved_operation_block<false, std::tuple<value_type>>{}
            .append_block(
                readwrite_temporary_variable<tuple_element_of_type<0, value_type>>
                    .read(invocation))
            .append_block(operation_block<false>{}
                .append(post_invocation<decltype(inserter), 0>(inserter)));

        return just(std::move(fd))
        | std::move(read_1).sender_chain();
      };

      return just(std::move(fd), sz)
      | repeat(
          [factory]([[maybe_unused]] std::size_t idx, auto& fd, size_type sz) {
            using result_type = decltype(factory(std::ref(fd)));
            if (idx != sz)
              return std::make_optional(factory(std::ref(fd)));
            else
              return std::optional<result_type>{};
          })
      | then(
          [&collection](auto fd, size_type sz) {
            // Confirm the collection actually is the intended size.
            // We only expect this to cause a mismatch, if the collection is a set or map (etc)
            // and there were duplicate elements.
            if (collection.size() != sz)
              throw xdr_error("reading N items did not result in collection of N items");

            return fd;
          });
    };

    return unresolved_operation_block<false, std::tuple<size_type>>{}
        .append_block(readwrite_temporary_variable<tuple_element_of_type<0, size_type>>.read(uint32_t{}))
        .append_block(operation_block<false>{}
            .append(post_invocation<decltype(apply_size_fn), 0>(std::move(apply_size_fn)))
            .append_sequence(io_barrier{})
            .append_sequence(manual_invocation<decltype(reader), std::dynamic_extent, 0>(std::move(reader)))
            .append(post_invocation<decltype(confirm_size_fn), 0>(std::move(confirm_size_fn))));
  }

  template<typename Collection, write_invocation_for<typename Collection::value_type> Invocation>
  auto write(const Collection& collection, Invocation invocation, std::optional<std::size_t> max_size = std::nullopt) const {
    using size_type = typename Collection::size_type;

    auto set_size_fn = [&collection, max_size](auto& sz) {
      if (max_size.has_value() && collection.size() > max_size.value())
        throw xdr_error("collection too large");
      sz = collection.size();
    };

    return unresolved_operation_block<true, std::tuple<size_type>>{}
        .append_block(operation_block<true>{}
            .append(pre_invocation<decltype(set_size_fn), 0>(std::move(set_size_fn))))
        .append_block(readwrite_temporary_variable<tuple_element_of_type<0, size_type>>.write(uint32_t{}))
        .merge(fixed_collection_t{}.write(collection, std::move(invocation)));
  }
};

static_assert(invocation_for<decltype(collection_t{}(ascii_string_t{})), std::vector<std::string>>);
static_assert(invocation_for<decltype(collection_t{}(ascii_string_t{}, 12)), std::vector<std::string>>);


// Read/write a std::optional.
struct optional_t
: basic_operation<optional_t>
{
  template<typename T, typename Invocation>
  auto read(std::optional<T>& opt, Invocation&& invocation) const {
    using execution::just;
    using execution::noop;
    using execution::let_variant;

    auto make_sender = [invocation=std::forward<Invocation>(invocation)](auto& fd, std::optional<T>& opt, const std::uint32_t& discriminant) {
      auto reader_when_set = [&]() {
        return just(std::move(fd)) | invocation.read(opt.value()).sender_chain();
      };
      auto reader_when_clear = [&]() {
        return just(std::move(fd));
      };
      using variant_type = std::variant<decltype(reader_when_set()), decltype(reader_when_clear())>;

      switch (discriminant) {
        default:
          throw xdr_error("optional discriminant out-of-range");
        case 1:
          opt.emplace();
          return variant_type(std::in_place_index<0>, reader_when_set());
        case 0:
          opt.reset();
          return variant_type(std::in_place_index<1>, reader_when_clear());
      }
    };

    auto reader = [&opt, make_sender](auto& fd, std::uint32_t discriminant) {
      return just(std::move(fd), std::ref(opt), discriminant)
      | let_variant(make_sender);
    };

    constexpr std::size_t nested_extent = (decltype(invocation.read(std::declval<T&>()))::extent == 0 ? 0u : std::dynamic_extent);

    return unresolved_operation_block<false, std::tuple<std::uint32_t>>{}
        .append_block(readwrite_temporary_variable<tuple_element_of_type<0, std::uint32_t>>(uint32_t{}).read())
        .append_block(operation_block<false>{}
            .append_sequence(io_barrier{})
            .append_sequence(manual_invocation<decltype(reader), nested_extent, 0>(std::move(reader))));
  }

  template<typename T, typename Invocation>
  auto write(const std::optional<T>& opt, Invocation&& invocation) const {
    using execution::just;
    using execution::noop;
    using execution::let_variant;

    auto set_discriminant = [&opt](std::uint32_t& discriminant) {
      discriminant = (opt.has_value() ? 1u : 0u);
    };

    auto make_sender = [invocation=std::forward<Invocation>(invocation)](auto& fd, const std::optional<T>& opt, const std::uint32_t& discriminant) {
      auto writer_when_set = [&]() {
        return just(std::move(fd)) | invocation.write(opt.value()).sender_chain();
      };
      auto writer_when_clear = [&]() {
        return just(std::move(fd));
      };
      using variant_type = std::variant<decltype(writer_when_set()), decltype(writer_when_clear())>;

      if (discriminant != 0)
        return variant_type(std::in_place_index<0>, writer_when_set());
      else
        return variant_type(std::in_place_index<1>, writer_when_clear());
    };

    auto writer = [&opt, make_sender](auto& fd, const std::uint32_t& discriminant) {
      return just(std::move(fd), std::cref(opt), discriminant)
      | let_variant(make_sender);
    };

    constexpr std::size_t nested_extent = (decltype(invocation.write(std::declval<const T&>()))::extent == 0 ? 0u : std::dynamic_extent);

    return unresolved_operation_block<true, std::tuple<std::uint32_t>>{}
        .append_block(operation_block<true>{}
            .append(pre_invocation<decltype(set_discriminant), 0>(std::move(set_discriminant))))
        .append_block(readwrite_temporary_variable<tuple_element_of_type<0, std::uint32_t>>(uint32_t{}).write())
        .append_block(operation_block<true>{}
            .append_sequence(manual_invocation<decltype(writer), nested_extent, 0>(std::move(writer))));
  }
};

static_assert(invocation_for<decltype(optional_t{}(uint32_t{})), std::optional<int>>);


struct variant_t
: basic_operation<variant_t>
{
  template<std::uint32_t Discriminant, typename Op>
  struct discriminant_t
  : Op
  {
    static inline constexpr std::uint32_t discriminant_value = Discriminant;

    explicit discriminant_t(const Op& op)
    : Op(op)
    {}

    explicit discriminant_t(Op&& op)
    : Op(std::move(op))
    {}
  };

  private:
  template<typename T>
  struct is_discriminant_t : std::false_type {};
  template<std::uint32_t Discriminant, typename Op>
  struct is_discriminant_t<discriminant_t<Discriminant, Op>> : std::true_type {};
  template<typename T>
  static inline constexpr bool is_discriminant = is_discriminant_t<T>::value;

  template<typename Op>
  static constexpr auto figure_out_discriminant([[maybe_unused]] std::size_t fallback) noexcept -> std::size_t {
    if constexpr(is_discriminant<Op>)
      return Op::discriminant_value;
    else
      return fallback;
  }

  // Constexpr expression to check if a sequence of std::size_t is unique.
  // This specializations handles the case where there are no arguments.
  static constexpr auto is_unique() noexcept -> bool {
    return true;
  }

  // Constexpr expression to check if a sequence of std::size_t is unique.
  // Bugs: this is quadratic complexity.
  template<typename... Tail>
  static constexpr auto is_unique(std::size_t x, Tail... tail) noexcept -> bool {
    return ((x != tail) &&...&& is_unique(tail...));
  }

  // translate-discriminant does an in-place translation of the discriminant, to its variant-index.
  template<std::size_t IndexOfValue0, std::size_t...> struct translate_discriminant;
  // translate-discriminant, when it doesn't find a translation, will throw xdr-error.
  template<std::size_t IndexOfValue0>
  struct translate_discriminant<IndexOfValue0> {
    auto operator()([[maybe_unused]] std::size_t discriminant) const -> std::size_t {
      throw xdr_error("variant-discriminant not recognized");
    }
  };
  // recursive implementation of translate-discriminant.
  // If we find a match, we perform in-place translation of the discriminant.
  template<std::size_t IndexOfValue0, std::size_t Value0, std::size_t... Tail>
  struct translate_discriminant<IndexOfValue0, Value0, Tail...> {
    auto operator()(std::size_t discriminant) const -> std::size_t {
      if (discriminant == Value0) {
        return IndexOfValue0;
      } else {
        return translate_discriminant<IndexOfValue0 + 1u, Tail...>{}(discriminant);
      }
    }
  };

  template<std::size_t... Values>
  requires (is_unique(Values...))
  struct discriminant_set {
    static_assert(((Values < 0xffff'ffffU) &&...), "variant-indices must fit in a 32-bit unsigned integer");

    // Retrieve the discriminant at position idx.
    static constexpr auto get(std::size_t idx) -> std::uint32_t {
      return std::array<std::size_t, sizeof...(Values)>{ Values... }.at(idx);
    }

    // Retrieve the index of the given discriminant.
    static constexpr auto find_index_for(std::size_t discriminant) -> std::size_t {
      return translate_discriminant<0, Values...>{}(discriminant);
    }
  };

  template<typename... Tail>
  static constexpr auto decide_on_extent(std::size_t extent0, Tail... extents) noexcept -> std::size_t {
    if (((extent0 == extents) &&...)) // llvm says I can remove the braces, but that's incorrect.
      return extent0;
    else
      return std::dynamic_extent;
  }

  public:
  template<std::uint32_t Discriminant, typename Op>
  auto discriminant(Op&& op) const -> discriminant_t<Discriminant, std::remove_cvref_t<Op>> {
    return discriminant_t<Discriminant, std::remove_cvref_t<Op>>(std::forward<Op>(op));
  }

  template<typename... T, typename... Invocations>
  requires (sizeof...(T) == sizeof...(Invocations)) && (read_invocation_for<Invocations, T> &&...) && (sizeof...(T) > 0)
  auto read(std::variant<T...>& v, Invocations... invocations) const {
    return read_(std::index_sequence_for<T...>{}, v, std::make_tuple(std::move(invocations)...));
  }

  template<typename... T, typename... Invocations>
  requires (sizeof...(T) == sizeof...(Invocations)) && (write_invocation_for<Invocations, T> &&...) && (sizeof...(T) > 0)
  auto write(const std::variant<T...>& v, Invocations... invocations) const {
    return write_(std::index_sequence_for<T...>{}, v, std::make_tuple(std::move(invocations)...));
  }

  private:
  template<std::size_t Idx, typename FD, typename Variant, typename Invocations>
  struct invocation_handler;

  template<std::size_t Idx, typename FD, typename... T, typename... Invocations>
  struct invocation_handler<Idx, FD, std::variant<T...>, std::tuple<Invocations...>> {
    using variant_type = std::variant<T...>;
    using invocations_type = std::tuple<Invocations...>;

    static auto read(FD&& fd, variant_type& v, const invocations_type& invocations) {
      return execution::just(std::move(fd))
      | std::get<Idx>(invocations).read(v.template emplace<Idx>()).sender_chain();
    }

    static auto write(FD&& fd, const variant_type& v, const invocations_type& invocations) {
      return execution::just(std::move(fd))
      | std::get<Idx>(invocations).write(std::get<Idx>(v)).sender_chain();
    }

    template<typename SenderVariant>
    static auto read_into_sender_variant(FD& fd, variant_type& v, const invocations_type& invocations) -> SenderVariant {
      return SenderVariant(std::in_place_index<Idx>, invocation_handler::read(std::move(fd), v, invocations));
    }

    template<typename SenderVariant>
    static auto write_into_sender_variant(FD& fd, const variant_type& v, const invocations_type& invocations) -> SenderVariant {
      return SenderVariant(std::in_place_index<Idx>, invocation_handler::write(std::move(fd), v, invocations));
    }
  };

  template<std::size_t... Idx, typename... T, typename... Invocations>
  auto read_([[maybe_unused]] std::index_sequence<Idx...> indices, std::variant<T...>& v, std::tuple<Invocations...> invocations) const {
    using d_set = discriminant_set<figure_out_discriminant<std::remove_cvref_t<Invocations>>(Idx)...>;
    constexpr std::size_t nested_extent = decide_on_extent(decltype(std::get<Idx>(invocations).read(std::declval<T&>()))::extent...);

    auto nested_operation = [&v, invocations]<typename FD>(FD& fd, std::uint32_t discriminant) {
      using execution::just;
      using execution::let_variant;

      using nested_sender_variant = std::variant<
          std::remove_cvref_t<decltype(
              invocation_handler<Idx, FD, std::variant<T...>, std::tuple<Invocations...>>::read(
                  std::declval<FD>(),
                  std::declval<std::variant<T...>&>(),
                  std::declval<const std::tuple<Invocations...>&>()))>...>;
      using nested_sender_fn = nested_sender_variant (*)(FD&, std::variant<T...>&, const std::tuple<Invocations...>&);

      auto functions = std::array<nested_sender_fn, sizeof...(Idx)>{
        &invocation_handler<Idx, FD, std::variant<T...>, std::tuple<Invocations...>>::template read_into_sender_variant<nested_sender_variant>...
      };

      return just(std::move(fd), std::ref(v), invocations)
      | let_variant(functions.at(d_set::find_index_for(discriminant)));
    };

    return unresolved_operation_block<false, std::tuple<std::uint32_t>>{}
        .append_block(readwrite_temporary_variable<tuple_element_of_type<0, std::uint32_t>>(uint32_t{}).read())
        .append_block(operation_block<false>{}
            .append_sequence(io_barrier{})
            .append_sequence(manual_invocation<decltype(nested_operation), nested_extent, 0>(std::move(nested_operation))));
  }

  template<std::size_t... Idx, typename... T, typename... Invocations>
  auto write_([[maybe_unused]] std::index_sequence<Idx...> indices, const std::variant<T...>& v, std::tuple<Invocations...> invocations) const {
    using d_set = discriminant_set<figure_out_discriminant<std::remove_cvref_t<Invocations>>(Idx)...>;
    constexpr std::size_t nested_extent = decide_on_extent(decltype(std::get<Idx>(invocations).write(std::declval<const T&>()))::extent...);

    auto nested_operation = [&v, invocations]<typename FD>(FD& fd) {
      using execution::just;
      using execution::let_variant;

      using nested_sender_variant = std::variant<
          std::remove_cvref_t<decltype(
              invocation_handler<Idx, FD, std::variant<T...>, std::tuple<Invocations...>>::write(
                  std::declval<FD>(),
                  std::declval<const std::variant<T...>&>(),
                  std::declval<const std::tuple<Invocations...>&>()))>...>;
      using nested_sender_fn = nested_sender_variant (*)(FD&, const std::variant<T...>&, const std::tuple<Invocations...>&);

      auto functions = std::array<nested_sender_fn, sizeof...(Idx)>{
        &invocation_handler<Idx, FD, std::variant<T...>, std::tuple<Invocations...>>::template write_into_sender_variant<nested_sender_variant>...
      };

      return just(std::move(fd), std::cref(v), invocations)
      | let_variant(functions.at(v.index()));
    };

    auto set_discriminant = [&v](std::uint32_t& discriminant) {
      if (v.valueless_by_exception()) throw std::bad_variant_access();
      discriminant = d_set::get(v.index());
    };

    return unresolved_operation_block<true, std::tuple<std::uint32_t>>{}
        .append_block(operation_block<true>{}
            .append(pre_invocation<decltype(set_discriminant), 0>(std::move(set_discriminant))))
        .append_block(readwrite_temporary_variable<tuple_element_of_type<0, std::uint32_t>>(uint32_t{}).write())
        .append_block(operation_block<true>{}
            .append_sequence(manual_invocation<decltype(nested_operation), nested_extent>(std::move(nested_operation))));
  }
};


// Skip reading/writing.
struct skip_t
: basic_operation<skip_t>
{
  template<typename T>
  auto read([[maybe_unused]] T& v) const noexcept {
    return unresolved_operation_block<false, std::tuple<>>{};
  }

  template<typename T>
  auto write([[maybe_unused]] const T& v) const noexcept {
    return unresolved_operation_block<true, std::tuple<>>{};
  }
};

static_assert(invocation_for<skip_t, int>);
static_assert(invocation_for<skip_t, std::monostate>);


// Identity conversion.
struct identity_t
: basic_operation<identity_t>
{
  template<readable_object T>
  auto read(T& v) const {
    return unresolved_operation_block<false, std::tuple<>>{}
    | reader(v);
  }

  template<writable_object T>
  auto write(const T& v) const {
    return unresolved_operation_block<true, std::tuple<>>{}
    | writer(v);
  }
};


struct tuple_t
: basic_operation<tuple_t>
{
  private:
  template<typename>
  static constexpr auto make_identity() {
    return identity_t{};
  }

  public:
  template<typename... T>
  requires (sizeof...(T) > 0)
  auto read(std::tuple<T...>& v) const {
    return this->read(v, make_identity<T>()...);
  }

  template<typename... T, typename... Invocation>
  requires (sizeof...(T) == sizeof...(Invocation))
  auto read(std::tuple<T...>& v, Invocation... invocation) const {
    return read_(std::index_sequence_for<T...>{}, v, std::move(invocation)...);
  }

  template<typename... T>
  requires (sizeof...(T) > 0)
  auto write(const std::tuple<T...>& v) const {
    return this->write(v, make_identity<T>()...);
  }

  template<typename... T, typename... Invocation>
  requires (sizeof...(T) == sizeof...(Invocation))
  auto write(const std::tuple<T...>& v, Invocation... invocation) const {
    return write_(std::index_sequence_for<T...>{}, v, std::move(invocation)...);
  }

  private:
  template<std::size_t... Idx, typename... T, typename... Invocation>
  auto read_([[maybe_unused]] std::index_sequence<Idx...> indices, std::tuple<T...>& v, Invocation... invocation) {
    return (unresolved_operation_block<false, std::tuple<>>{} |...| invocation.read(std::get<Idx>(v)));
  }

  template<std::size_t... Idx, typename... T, typename... Invocation>
  auto write_([[maybe_unused]] std::index_sequence<Idx...> indices, const std::tuple<T...>& v, Invocation... invocation) {
    return (unresolved_operation_block<true, std::tuple<>>{} |...| invocation.write(std::get<Idx>(v)));
  }
};


struct pair_t
: basic_operation<pair_t>
{
  template<typename T, typename U, typename FirstInvocation = identity_t, typename SecondInvocation = identity_t>
  auto read(std::pair<T, U>& v, FirstInvocation first_invocation = FirstInvocation{}, SecondInvocation second_invocation = SecondInvocation{}) const {
    return first_invocation.read(v.first)
    | second_invocation.read(v.second);
  }

  template<typename T, typename U, typename FirstInvocation = identity_t, typename SecondInvocation = identity_t>
  auto write(const std::pair<T, U>& v, FirstInvocation first_invocation = FirstInvocation{}, SecondInvocation second_invocation = SecondInvocation{}) const {
    return first_invocation.write(v.first)
    | second_invocation.write(v.second);
  }
};


// Check a predicate.
// If the predicate doesn't hold, use an error-factory to create an error type.
struct validation_t {
  template<std::invocable Pred>
  auto read(Pred&& pred) const {
    return unresolved_operation_block<false, std::tuple<>>{}
        .append_block(operation_block<false>{}
            .append(post_validation(std::forward<Pred>(pred))));
  }

  template<std::invocable Pred>
  auto write(Pred&& pred) const {
    return unresolved_operation_block<true, std::tuple<>>{}
        .append_block(operation_block<true>{}
            .append(pre_validation(std::forward<Pred>(pred))));
  }
};


// This type indicates we want a read/write operation that takes a file-descriptor from the value-type.
struct no_fd_t {};

// Write operation.
struct write_t {
  template<typename FD, typename T, write_invocation_for<std::remove_cvref_t<T>> Invocation = identity_t>
  requires (!std::same_as<std::remove_cvref_t<FD>, no_fd_t>)
  auto operator()(FD&& fd, T&& v, Invocation invocation = Invocation{}) const {
    return execution::just(std::forward<FD>(fd))
    | (*this)(no_fd_t{}, std::forward<T>(v), std::move(invocation));
  }

  template<typename T, write_invocation_for<std::remove_cvref_t<T>> Invocation = identity_t>
  auto operator()([[maybe_unused]] no_fd_t fd, T&& v, Invocation invocation = Invocation{}) const {
    return invocation.write(std::forward<T>(v)).sender_chain();
  }
};

// Read operation.
struct read_t {
  template<typename FD, typename T, read_invocation_for<std::remove_cvref_t<T>> Invocation = identity_t>
  requires (!std::same_as<std::remove_cvref_t<FD>, no_fd_t>)
  auto operator()(FD&& fd, T&& v, Invocation invocation = Invocation{}) const {
    return execution::just(std::forward<FD>(fd))
    | (*this)(no_fd_t{}, std::forward<T>(v), std::move(invocation));
  }

  template<typename T, read_invocation_for<std::remove_cvref_t<T>> Invocation = identity_t>
  auto operator()([[maybe_unused]] no_fd_t fd, T&& v, Invocation invocation = Invocation{}) const {
    return invocation.read(std::forward<T>(v)).sender_chain();
  }
};

// Read operation that reads into a newly-instantiated type.
template<typename T>
struct read_into_t {
  template<typename FD, read_invocation_for<std::remove_cvref_t<T>> Invocation = identity_t>
  requires (!std::same_as<std::remove_cvref_t<FD>, no_fd_t>)
  auto operator()(FD&& fd, Invocation invocation = Invocation{}) const {
    return execution::just(std::forward<FD>(fd))
    | (*this)(no_fd_t{}, std::move(invocation));
  }

  template<read_invocation_for<std::remove_cvref_t<T>> Invocation = identity_t>
  auto operator()([[maybe_unused]] no_fd_t fd, Invocation invocation = Invocation{}) const {
    return execution::lazy_then(
        []<typename FD>(FD fd) {
          return std::tuple<FD, T>(std::move(fd), T{});
        })
    | execution::explode_tuple()
    | execution::lazy_let_value(
        [invocation](auto& fd, T& v) {
          return read_t{}(std::move(fd), v, invocation)
          | execution::lazy_then(
              [&v]<typename FD>(FD&& fd) {
                return std::tuple<FD, T>(std::forward<FD>(fd), std::move(v));
              })
          | execution::explode_tuple();
        });
  }
};


} /* namespace operation */


inline constexpr operation::uint8_t              uint8{};
inline constexpr operation::uint16_t             uint16{};
inline constexpr operation::uint32_t             uint32{};
inline constexpr operation::uint64_t             uint64{};
inline constexpr operation::manual_t             manual{};
inline constexpr operation::constant_t           constant{};
inline constexpr operation::fixed_byte_string_t  fixed_byte_string{};
inline constexpr operation::byte_string_t        byte_string{};
inline constexpr operation::fixed_ascii_string_t fixed_ascii_string{};
inline constexpr operation::ascii_string_t       ascii_string{};
inline constexpr operation::fixed_collection_t   fixed_collection{};
inline constexpr operation::collection_t         collection{};
inline constexpr operation::optional_t           optional{};
inline constexpr operation::variant_t            variant{};
inline constexpr operation::skip_t               skip{};
inline constexpr operation::identity_t           identity{};
inline constexpr operation::tuple_t              tuple{};
inline constexpr operation::pair_t               pair{};
inline constexpr operation::validation_t         validation{};

inline constexpr operation::no_fd_t              no_fd{}; // Indicate you don't want to bind a file-descriptor to a read/write operation.
inline constexpr operation::read_t               read{};
template<typename T>
inline constexpr operation::read_into_t<T>       read_into{};
inline constexpr operation::write_t              write{};


} /* namespace earnest::xdr */
