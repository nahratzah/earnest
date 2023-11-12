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
#include <ranges>
#include <span>
#include <stdexcept>
#include <system_error>
#include <tuple>
#include <type_traits>

#include <earnest/execution.h>
#include <earnest/execution_util.h>
#include <earnest/execution_io.h>

namespace earnest::xdr {


class reader;
class writer;


class xdr_error
: public std::runtime_error
{
  using std::runtime_error::runtime_error;
};


template<typename T>
concept read_operation =
    true; // XXX

template<typename T>
concept write_operation =
    true; // XXX

template<typename T>
concept readable_object =
    requires(reader r, T& v) {
      { r & v } -> read_operation;
    };

template<typename T>
concept writable_object =
    requires(writer w, const T& v) {
      { w & v } -> write_operation;
    };


namespace operation {


template<bool, typename = std::tuple<>, typename = std::tuple<>, typename = std::tuple<>> class operation_sequence;

template<bool IsConst, std::size_t Extent = 0, typename PreInvocationOperations = std::tuple<>, typename BufferOperations = std::tuple<>, typename PostInvocationOperations = std::tuple<>>
class operation_block;


template<std::size_t... XIdx, std::size_t X, std::size_t... YIdx, std::size_t Y>
constexpr auto array_cat_(
    [[maybe_unused]] std::index_sequence<XIdx...>, std::span<const std::byte, X> x,
    [[maybe_unused]] std::index_sequence<YIdx...>, std::span<const std::byte, Y> y)
noexcept
-> std::array<std::byte, sizeof...(XIdx) + sizeof...(YIdx)> {
  return std::array<std::byte, sizeof...(XIdx) + sizeof...(YIdx)>{x[XIdx]..., y[YIdx]...};
}

template<std::size_t X, std::size_t Y>
requires (X != std::dynamic_extent) && (Y != std::dynamic_extent)
constexpr auto array_cat(std::span<const std::byte, X> x, std::span<const std::byte, Y> y)
noexcept
-> std::array<std::byte, X + Y> {
  return array_cat_(std::make_index_sequence<X>(), x, std::make_index_sequence<Y>(), y);
}


template<typename Fn>
class pre_buffer_invocation {
  template<bool IsConst, std::size_t Extent, typename PreInvocationOperations, typename BufferOperations, typename PostInvocationOperations>
  friend class operation_block;

  public:
  explicit pre_buffer_invocation(Fn&& fn) noexcept(std::is_nothrow_move_constructible_v<Fn>)
  : fn(std::move(fn))
  {}

  explicit pre_buffer_invocation(const Fn& fn) noexcept(std::is_nothrow_copy_constructible_v<Fn>)
  : fn(fn)
  {}

  template<typename Byte, std::size_t N>
  requires std::same_as<std::remove_const_t<Byte>, std::byte>
  auto operator()(std::span<Byte, N> data) && -> void {
    assert(data.size() >= off + len);
    assert(len != 0);
    std::invoke(std::move(fn), data.subspan(off, len));
  }

  auto shift(std::size_t increase) noexcept -> void {
    off += increase;
  }

  private:
  auto assign(std::size_t off, std::size_t len) noexcept {
    this->off = off;
    this->len = len;
  }

  Fn fn;
  std::size_t off = 0, len = 0;
};

template<typename Fn>
class post_buffer_invocation {
  template<bool IsConst, std::size_t Extent, typename PreInvocationOperations, typename BufferOperations, typename PostInvocationOperations>
  friend class operation_block;

  public:
  explicit post_buffer_invocation(Fn&& fn) noexcept(std::is_nothrow_move_constructible_v<Fn>)
  : fn(std::move(fn))
  {}

  explicit post_buffer_invocation(const Fn& fn) noexcept(std::is_nothrow_copy_constructible_v<Fn>)
  : fn(fn)
  {}

  template<typename Byte, std::size_t N>
  requires std::same_as<std::remove_const_t<Byte>, std::byte>
  auto operator()(std::span<Byte, N> data) && -> void {
    assert(data.size() >= off + len);
    assert(len != 0);
    std::invoke(std::move(fn), data.subspan(off, len));
  }

  auto shift(std::size_t increase) noexcept -> void {
    off += increase;
  }

  private:
  auto assign(std::size_t off, std::size_t len) noexcept {
    this->off = off;
    this->len = len;
  }

  Fn fn;
  std::size_t off = 0, len = 0;
};

template<std::invocable Fn>
class pre_invocation {
  public:
  explicit pre_invocation(Fn&& fn) noexcept(std::is_nothrow_move_constructible_v<Fn>)
  : fn(std::move(fn))
  {}

  explicit pre_invocation(const Fn& fn) noexcept(std::is_nothrow_copy_constructible_v<Fn>)
  : fn(fn)
  {}

  template<typename Byte, std::size_t N>
  requires std::same_as<std::remove_const_t<Byte>, std::byte>
  auto operator()([[maybe_unused]] std::span<Byte, N>) && -> void {
    std::invoke(std::move(fn));
  }

  auto shift([[maybe_unused]] std::size_t increase) noexcept -> void {}

  private:
  Fn fn;
};

template<std::invocable Fn>
class post_invocation {
  public:
  explicit post_invocation(Fn&& fn) noexcept(std::is_nothrow_move_constructible_v<Fn>)
  : fn(std::move(fn))
  {}

  explicit post_invocation(const Fn& fn) noexcept(std::is_nothrow_copy_constructible_v<Fn>)
  : fn(fn)
  {}

  template<typename Byte, std::size_t N>
  requires std::same_as<std::remove_const_t<Byte>, std::byte>
  auto operator()([[maybe_unused]] std::span<Byte, N>) && -> void {
    std::invoke(std::move(fn));
  }

  auto shift([[maybe_unused]] std::size_t increase) noexcept -> void {}

  private:
  Fn fn;
};

// Refers to space in the temporary buffer.
//
// Offset and length are stored, because the temporary buffer may be re-allocated.
template<bool IsConst, std::size_t Extent = std::dynamic_extent>
class temporary_buffer_reference {
  public:
  static inline constexpr std::size_t extent = Extent;

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

  auto shift(std::size_t increase) -> void {
    if (std::numeric_limits<std::size_t>::max() - off - Extent < increase)
      throw std::range_error("xdr buffer shift overflow");
    off += increase;
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

  explicit temporary_buffer_reference(std::size_t off, std::size_t len) noexcept
  : off(off),
    len(len)
  {}

  auto get_buffer(std::span<std::conditional_t<IsConst, const std::byte, std::byte>> shared_buf) const {
    assert(shared_buf.size() >= off + len);
    return shared_buf.subspan(off, len);
  }

  auto shift(std::size_t increase) -> void {
    if (std::numeric_limits<std::size_t>::max() - off - len < increase)
      throw std::range_error("xdr buffer shift overflow");
    off += increase;
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

  explicit span_reference(std::span<std::conditional_t<IsConst, const std::byte, std::byte>, Extent> data) noexcept
  : data(data)
  {}

  auto get_buffer([[maybe_unused]] std::span<std::conditional_t<IsConst, const std::byte, std::byte>> shared_buf) const noexcept {
    return data;
  }

  auto shift([[maybe_unused]] std::size_t increase) noexcept -> void {}

  private:
  std::span<std::conditional_t<IsConst, const std::byte, std::byte>, Extent> data;
};


struct buffer_operation_appender_ {
  template<typename... T, typename Op>
  auto operator()(std::tuple<T...>&& list, Op&& op) {
    return std::apply(
        []<typename... X>(X&&... x) {
          return std::make_tuple(std::forward<X>(x)...);
        },
        std::tuple_cat(
            all_but_last(std::move(list)),
            maybe_squash(only_last(std::move(list)), std::forward<Op>(op))));
  }

  private:
  template<typename... T>
  auto all_but_last(std::tuple<T...>&& list) {
    return all_buf_last_(std::make_index_sequence<sizeof...(T) - 1u>(), std::move(list));
  }

  template<std::size_t... Idx, typename... T>
  auto all_but_last_([[maybe_unused]] std::index_sequence<Idx...>, std::tuple<T...>&& list) {
    return std::forward_as_tuple(std::get<Idx>(std::move(list))...);
  }

  template<typename... T>
  auto only_last(std::tuple<T...>&& list) -> decltype(auto) {
    return std::get<sizeof...(T) - 1u>(std::move(list));
  }

  template<bool IsConst, std::size_t Extent, typename Op>
  auto maybe_squash(span_reference<IsConst, Extent>&& s, Op&& op) {
    return std::forward_as_tuple(std::move(s), std::forward<Op>(op));
  }

  template<bool IsConst, std::size_t Extent1, std::size_t Extent2>
  auto maybe_squash(temporary_buffer_reference<IsConst, Extent1>&& s, span_reference<IsConst, Extent2>&& op) {
    return std::forward_as_tuple(std::move(s), std::move(op));
  }

  template<bool IsConst, std::size_t Extent1, std::size_t Extent2>
  auto maybe_squash(temporary_buffer_reference<IsConst, Extent1>&& s, temporary_buffer_reference<IsConst, Extent2>&& op) {
    if constexpr(Extent1 == std::dynamic_extent || Extent2 == std::dynamic_extent) {
      return std::make_tuple(temporary_buffer_reference<IsConst, std::dynamic_extent>(s.off, s.len + op.len));
    } else {
      return std::make_tuple(temporary_buffer_reference<IsConst, Extent1 + Extent2>(s.off, s.len + op.len));
    }
  }
};
inline constexpr buffer_operation_appender_ buffer_operation_appender{};


template<bool IsConst, std::size_t Extent = 0>
class temporary_buffer;

template<>
class temporary_buffer<true, std::dynamic_extent> {
  template<bool, std::size_t> friend class temporary_buffer;

  private:
  explicit temporary_buffer(std::vector<std::byte> data)
  : data(std::move(data))
  {}

  public:
  auto size() const noexcept -> std::size_t {
    return data.size();
  }

  auto get_data() && noexcept -> std::vector<std::byte>&& {
    return std::move(data);
  }

  template<std::size_t N>
  auto append(std::span<const std::byte, N> extra_data) && {
    data.insert(data.end(), extra_data.begin(), extra_data.end());
    return std::move(*this);
  }

  template<std::size_t OtherExtent>
  auto merge(temporary_buffer<true, OtherExtent>&& other) &&
  -> temporary_buffer&& {
    data.insert(data.end(), std::move(other).get_data().begin(), std::move(other).get_data().end());
    return std::move(*this);
  }

  private:
  std::vector<std::byte> data;
};

template<>
class temporary_buffer<false, std::dynamic_extent> {
  template<bool, std::size_t> friend class temporary_buffer;

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
  auto merge(temporary_buffer<false, OtherExtent>&& other) && {
    return temporary_buffer(size() + other.size());
  }

  private:
  std::size_t sz = 0;
};

template<std::size_t Extent>
class temporary_buffer<true, Extent> {
  template<bool, std::size_t> friend class temporary_buffer;

  public:
  temporary_buffer() {}

  private:
  explicit temporary_buffer(std::array<std::byte, Extent> data)
  : data(std::move(data))
  {}

  public:
  auto size() const noexcept -> std::size_t {
    return data.size();
  }

  auto get_data() noexcept -> std::array<std::byte, Extent>&& {
    return std::move(data);
  }

  template<std::size_t N>
  auto append(std::span<const std::byte, N> extra_data) && {
    if constexpr(N == std::dynamic_extent) {
      std::vector<std::byte> tmp(data.size() + extra_data.size());
      auto i = std::copy(data.begin(), data.end(), tmp.begin());
      std::copy(extra_data.begin(), extra_data.end(), i);
      return temporary_buffer<true, std::dynamic_extent>(std::move(tmp));
    } else {
      return temporary_buffer<true, Extent + N>(array_cat(std::span<const std::byte, Extent>(data), extra_data));
    }
  }

  template<std::size_t OtherExtent>
  auto merge(temporary_buffer<true, OtherExtent>&& other) && {
    if constexpr(OtherExtent == std::dynamic_extent) {
      return temporary_buffer<true, std::dynamic_extent>({})
          .merge(std::move(*this))
          .merge(std::move(other));
    } else {
      return temporary_buffer<true, Extent + OtherExtent>(
          array_cat(std::span<std::byte, Extent>(data), std::span<std::byte, Extent>(other.data)));
    }
  }

  private:
  std::array<std::byte, Extent> data;
};

template<std::size_t Extent>
class temporary_buffer<false, Extent> {
  template<bool, std::size_t> friend class temporary_buffer;

  public:
  temporary_buffer() = default;

  auto size() const noexcept -> std::size_t {
    return Extent;
  }

  auto get_data() && -> std::array<std::byte, Extent> {
    return {};
  }

  template<std::size_t N>
  auto append() && {
    return temporary_buffer<false, Extent + N>{};
  }

  auto append(std::size_t n) && {
    return temporary_buffer<false, std::dynamic_extent>(Extent + n);
  }

  template<std::size_t OtherExtent>
  auto merge(temporary_buffer<false, OtherExtent>&& other) && {
    if constexpr(OtherExtent == std::dynamic_extent) {
      return temporary_buffer<false, std::dynamic_extent>(Extent + other.size());
    } else {
      return temporary_buffer<false, Extent + OtherExtent>();
    }
  }
};


template<bool IsConst>
class operation_sequence<IsConst, std::tuple<>, std::tuple<>, std::tuple<>> {
  template<bool, typename, typename, typename> friend class operation_sequence;

  public:
  static inline constexpr bool for_writing = IsConst;
  static inline constexpr bool for_reading = !IsConst;
  static inline constexpr std::size_t size = 0;

  operation_sequence() = default;
  operation_sequence(operation_sequence&&) = default;
  operation_sequence(const operation_sequence&) = delete;

  template<typename FD>
  auto make_sender_chain([[maybe_unused]] FD& fd, [[maybe_unused]] std::span<std::conditional_t<IsConst, const std::byte, std::byte>> shared_buf) && {
    return execution::just(std::move(fd));
  }

  auto shift(std::size_t increase) noexcept -> void {}

  auto append_operation() && -> operation_sequence&& {
    return std::move(*this);
  }

  template<typename Op0, typename... Ops>
  requires (sizeof...(Ops) > 0)
  auto append_operation(Op0&& op0, Ops&&... ops) && {
    return std::move(*this).append_operation(std::forward<Op0>(op0)).append_operation(std::forward<Ops>(ops)...);
  }

  template<typename Fn>
  auto append_operation(pre_buffer_invocation<Fn>&& op) && {
    using result_type = operation_sequence<
        IsConst,
        std::tuple<pre_buffer_invocation<Fn>>,
        std::tuple<>,
        std::tuple<>>;
    return result_type(
        std::forward_as_tuple(std::move(op)),
        std::tuple<>(),
        std::tuple<>());
  }

  template<typename Fn>
  auto append_operation(post_buffer_invocation<Fn>&& op) && {
    using result_type = operation_sequence<
        IsConst,
        std::tuple<>,
        std::tuple<>,
        std::tuple<post_buffer_invocation<Fn>>>;
    return result_type(
        std::tuple<>(),
        std::tuple<>(),
        std::forward_as_tuple(std::move(op)));
  }

  template<typename Fn>
  auto append_operation(pre_invocation<Fn>&& op) && {
    using result_type = operation_sequence<
        IsConst,
        std::tuple<pre_invocation<Fn>>,
        std::tuple<>,
        std::tuple<>>;
    return result_type(
        std::forward_as_tuple(std::move(op)),
        std::tuple<>(),
        std::tuple<>());
  }

  template<typename Fn>
  auto append_operation(post_invocation<Fn>&& op) && {
    using result_type = operation_sequence<
        IsConst,
        std::tuple<>,
        std::tuple<>,
        std::tuple<post_invocation<Fn>>>;
    return result_type(
        std::tuple<>(),
        std::tuple<>(),
        std::forward_as_tuple(std::move(op)));
  }

  template<std::size_t Extent = std::dynamic_extent>
  auto append_operation(temporary_buffer_reference<IsConst, Extent>&& op) && {
    using result_type = operation_sequence<
        IsConst,
        std::tuple<>,
        std::tuple<temporary_buffer_reference<IsConst, Extent>>,
        std::tuple<>>;
    return result_type(
        std::tuple<>(),
        std::forward_as_tuple(std::move(op)),
        std::tuple<>());
  }

  template<std::size_t Extent = std::dynamic_extent>
  auto append_operation(span_reference<IsConst, Extent>&& op) && {
    using result_type = operation_sequence<
        IsConst,
        std::tuple<>,
        std::tuple<span_reference<IsConst, Extent>>,
        std::tuple<>>;
    return result_type(
        std::tuple<>(),
        std::forward_as_tuple(std::move(op)),
        std::tuple<>());
  }

  template<typename OtherPreInvocationOperations, typename OtherBufferOperations, typename OtherPostInvocationOperations>
  auto merge(operation_sequence<IsConst, OtherPreInvocationOperations, OtherBufferOperations, OtherPostInvocationOperations>&& other)
  -> operation_sequence<IsConst, OtherPreInvocationOperations, OtherBufferOperations, OtherPostInvocationOperations>&& {
    return std::move(other);
  }
};

// Figure out the `static constexpr std::size_t size' attribute for an operation-sequence.
//
// The default case is to not have a size attribute (meaning the size is not known at
// compile time).
template<typename BufferOperations, typename = void>
struct _operation_block_size_ {
  static inline constexpr std::size_t size = std::dynamic_extent;

  protected:
  _operation_block_size_() = default;
  _operation_block_size_(const _operation_block_size_&) = default;
  _operation_block_size_(_operation_block_size_&&) = default;
  _operation_block_size_& operator=(const _operation_block_size_&) = default;
  _operation_block_size_& operator=(_operation_block_size_&&) = default;
  ~_operation_block_size_() = default;
};
// Specialization for the size attribute.
// This attribute exists if we can compute the total size, and exposes that total size.
template<typename... BufferOperations>
struct _operation_block_size_<std::tuple<BufferOperations...>, std::enable_if_t<((BufferOperations::extent != std::dynamic_extent) &&...&& true)>> {
  static inline constexpr std::size_t extent = (BufferOperations::extent +...+ 0u);

  protected:
  _operation_block_size_() = default;
  _operation_block_size_(const _operation_block_size_&) = default;
  _operation_block_size_(_operation_block_size_&&) = default;
  _operation_block_size_& operator=(const _operation_block_size_&) = default;
  _operation_block_size_& operator=(_operation_block_size_&&) = default;
  ~_operation_block_size_() = default;
};

// Helper type, that takes zero or more buffers, and transforms it into a sender adapter.
template<bool IsConst>
struct make_read_write_op_ {
  static inline constexpr bool for_writing = IsConst;
  static inline constexpr bool for_reading = !IsConst;

  template<typename... Buffer>
  auto operator()(Buffer&&... buffer) const {
    return execution::lazy_let_value(
        [...buffer=std::forward<Buffer>(buffer)](auto st) -> execution::typed_sender auto {
          if constexpr(for_writing) {
            return execution::io::lazy_write_ec(st.fd, maybe_wrap_in_array_for_writing(buffer.get_buffer(st.shared_buf)...))
            | execution::lazy_then(
                [st]([[maybe_unused]] std::size_t wlen) {
                  return st;
                });
          } else {
            return execution::io::lazy_read_ec(st.fd, maybe_wrap_in_array_for_reading(buffer.get_buffer(st.shared_buf)...))
            | execution::lazy_then(
                [st]([[maybe_unused]] std::size_t rlen) {
                  return st;
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

template<bool IsConst, typename... PreInvocationOperations, typename... BufferOperations, typename... PostInvocationOperations>
class operation_sequence<
    IsConst,
    std::tuple<PreInvocationOperations...>,
    std::tuple<BufferOperations...>,
    std::tuple<PostInvocationOperations...>>
: public _operation_block_size_<std::tuple<BufferOperations...>>
{
  template<bool, typename, typename, typename> friend class operation_sequence;

  public:
  static inline constexpr bool for_writing = IsConst;
  static inline constexpr bool for_reading = !IsConst;

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

  template<typename FD>
  auto make_sender_chain(FD& fd, std::span<std::conditional_t<IsConst, const std::byte, std::byte>> shared_buf) && {
    struct state {
      FD& fd;
      std::span<std::conditional_t<IsConst, const std::byte, std::byte>> shared_buf;
    };

    return execution::just(state{
          .fd=fd,
          .shared_buf=shared_buf
        })
    | std::apply(
        [](auto&&... x) {
          if constexpr(sizeof...(x) == 0) {
            return execution::noop();
          } else {
            return execution::lazy_then(
                [...x=std::move(x)](state& st) mutable {
                  (std::invoke(std::move(x), std::as_const(st.shared_buf)), ...);
                  return st;
                });
          }
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
          if constexpr(sizeof...(x) == 0) {
            return execution::noop();
          } else {
            return execution::lazy_then(
                [...x=std::move(x)](state st) mutable {
                  (std::invoke(std::move(x), std::as_const(st.shared_buf)), ...);
                  return st;
                });
          }
        },
        std::move(post_invocations))
    | execution::lazy_then(
        [](state st) {
          return std::move(st.fd);
        });
  }

  auto shift(std::size_t increase) -> void {
    std::apply(
        [&](auto&... x) {
          (x.shift(increase), ...);
        },
        pre_invocations);
    std::apply(
        [&](auto&... x) {
          (x.shift(increase), ...);
        },
        buffers);
    std::apply(
        [&](auto&... x) {
          (x.shift(increase), ...);
        },
        post_invocations);
  }

  auto append_operation() && -> operation_sequence&& {
    return std::move(*this);
  }

  template<typename Op0, typename... Ops>
  requires (sizeof...(Ops) > 0)
  auto append_operation(Op0&& op0, Ops&&... ops) && {
    return std::move(*this).append(std::forward<Op0>(op0)).append(std::forward<Ops>(ops)...);
  }

  template<typename Fn>
  auto append_operation(pre_buffer_invocation<Fn>&& op) && {
    using result_type = operation_sequence<
        IsConst,
        std::tuple<PreInvocationOperations..., pre_buffer_invocation<Fn>>,
        std::tuple<BufferOperations...>,
        std::tuple<PostInvocationOperations...>>;
    return result_type(
        std::tuple_cat(std::move(pre_invocations), std::forward_as_tuple(std::move(op))),
        std::move(buffers),
        std::move(post_invocations));
  }

  template<typename Fn>
  auto append_operation(post_buffer_invocation<Fn>&& op) && {
    using result_type = operation_sequence<
        IsConst,
        std::tuple<PreInvocationOperations...>,
        std::tuple<BufferOperations...>,
        std::tuple<PostInvocationOperations..., post_buffer_invocation<Fn>>>;
    return result_type(
        std::move(pre_invocations),
        std::move(buffers),
        std::tuple_cat(std::move(post_invocations), std::forward_as_tuple(std::move(op))));
  }

  template<typename Fn>
  auto append_operation(pre_invocation<Fn>&& op) && {
    using result_type = operation_sequence<
        IsConst,
        std::tuple<PreInvocationOperations..., pre_invocation<Fn>>,
        std::tuple<BufferOperations...>,
        std::tuple<PostInvocationOperations...>>;
    return result_type(
        std::tuple_cat(std::move(pre_invocations), std::forward_as_tuple(std::move(op))),
        std::move(buffers),
        std::move(post_invocations));
  }

  template<typename Fn>
  auto append_operation(post_invocation<Fn>&& op) && {
    using result_type = operation_sequence<
        IsConst,
        std::tuple<PreInvocationOperations...>,
        std::tuple<BufferOperations...>,
        std::tuple<PostInvocationOperations..., post_invocation<Fn>>>;
    return result_type(
        std::move(pre_invocations),
        std::move(buffers),
        std::tuple_cat(std::move(post_invocations), std::forward_as_tuple(std::move(op))));
  }

  template<std::size_t Extent = std::dynamic_extent>
  auto append_operation(temporary_buffer_reference<IsConst, Extent>&& op) && {
    using result_type = operation_sequence<
        IsConst,
        std::tuple<PreInvocationOperations...>,
        decltype(buffer_operation_appender(std::move(buffers), std::move(op))),
        std::tuple<PostInvocationOperations...>>;
    return result_type(
        std::move(pre_invocations),
        buffer_operation_appender(std::move(buffers), std::move(op)),
        std::move(post_invocations));
  }

  template<std::size_t Extent = std::dynamic_extent>
  auto append_operation(span_reference<IsConst, Extent>&& op) && {
    using result_type = operation_sequence<
        IsConst,
        std::tuple<PreInvocationOperations...>,
        decltype(buffer_operation_appender(std::move(buffers), std::move(op))),
        std::tuple<PostInvocationOperations...>>;
    return result_type(
        std::move(pre_invocations),
        buffer_operation_appender(std::move(buffers), std::move(op)),
        std::move(post_invocations));
  }

  template<typename OtherPreInvocationOperations, typename OtherBufferOperations, typename OtherPostInvocationOperations>
  auto merge(operation_sequence<IsConst, OtherPreInvocationOperations, OtherBufferOperations, OtherPostInvocationOperations> other) {
    return std::apply(
        [&](auto&&... ops1) {
          return std::apply(
              [&](auto&&... ops2) {
                return std::apply(
                    [&](auto&&... ops3) {
                      return std::move(*this).append_operation(std::move(ops1)..., std::move(ops2)..., std::move(ops3)...);
                    },
                    std::move(other.post_invocations));
              },
              std::move(other.buffers));
        },
        std::move(other.pre_invocations));
  }

  private:
  std::tuple<PreInvocationOperations...> pre_invocations;
  std::tuple<BufferOperations...> buffers;
  std::tuple<PostInvocationOperations...> post_invocations;
};


template<bool IsConst, std::size_t Extent, typename PreInvocationOperations, typename BufferOperations, typename PostInvocationOperations>
auto make_operation_block(
      temporary_buffer<IsConst, Extent>&& tmpbuf,
      operation_sequence<IsConst, PreInvocationOperations, BufferOperations, PostInvocationOperations>&& sequence)
-> operation_block<IsConst, Extent, PreInvocationOperations, BufferOperations, PostInvocationOperations> {
  return operation_block(std::move(tmpbuf), std::move(sequence));
}

template<bool IsConst, std::size_t Extent, typename PreInvocationOperations, typename BufferOperations, typename PostInvocationOperations>
class operation_block
: public _operation_block_size_<BufferOperations>
{
  public:
  operation_block(
      temporary_buffer<IsConst, Extent>&& tmpbuf,
      operation_sequence<IsConst, PreInvocationOperations, BufferOperations, PostInvocationOperations>&& sequence)
  : tmpbuf(std::move(tmpbuf)),
    sequence(std::move(sequence))
  {}

  auto sender_chain() && {
    return execution::then(
        [tmpbuf=std::move(this->tmpbuf)]<typename FD>(FD&& fd) mutable {
          return std::make_tuple(std::forward<FD>(fd), std::move(tmpbuf).get_data());
        })
    | execution::explode_tuple()
    | execution::lazy_let_value(
        [sequence=std::move(this->sequence)](auto& fd, auto& tmpbuf) mutable {
          return std::move(sequence).make_sender_chain(
              fd,
              std::span<std::conditional_t<IsConst, const std::byte, std::byte>>(tmpbuf.data(), tmpbuf.size()));
        });
  }

  auto shift(std::size_t increase) -> void {
    sequence.shift(increase);
  }

  template<typename Fn>
  auto append(pre_invocation<Fn>&& op) && {
    return make_operation_block(
        std::move(tmpbuf),
        std::move(sequence).append_operation(std::move(op)));
  }

  template<typename Fn>
  auto append(post_invocation<Fn>&& op) && {
    return make_operation_block(
        std::move(tmpbuf),
        std::move(sequence).append_operation(std::move(op)));
  }

  template<std::size_t N>
  auto append(span_reference<IsConst, N>&& op) && {
    return make_operation_block(
        std::move(tmpbuf),
        std::move(sequence).append_operation(std::move(op)));
  }

  template<std::size_t N, typename... Op>
  auto buffer(std::span<const std::byte, N> data, Op&&... op) && {
    static_assert(IsConst);

    const auto off = tmpbuf.size();
    const auto len = data.size();
    (op.assign(off, len), ...);
    return make_operation_block(
        std::move(tmpbuf).append(std::move(data)),
        std::move(sequence).append_operation(temporary_buffer_reference<true, N>(off, len), std::forward<Op>(op)...));
  }

  template<std::size_t N, typename... Op>
  requires (N != std::dynamic_extent)
  auto buffer(Op&&... op) && {
    static_assert(!IsConst);

    const auto off = tmpbuf.size();
    (op.assign(tmpbuf.size(), N), ...);
    return make_operation_block(
        std::move(tmpbuf).template append<N>(),
        std::move(sequence).append_operation(temporary_buffer_reference<false, N>(off, N), std::forward<Op>(op)...));
  }

  template<typename... Op>
  auto buffer(std::size_t n, Op&&... op) && {
    static_assert(!IsConst);

    const auto off = tmpbuf.size();
    (op.assign(tmpbuf.size(), n), ...);
    return make_operation_block(
        std::move(tmpbuf).append(n),
        std::move(sequence).append_operation(temporary_buffer_reference<false>(off, n), std::forward<Op>(op)...));
  }

  template<std::size_t OtherExtent, typename OtherPreInvocationOperations, typename OtherBufferOperations, typename OtherPostInvocationOperations>
  auto append(operation_block<IsConst, OtherExtent, OtherPreInvocationOperations, OtherBufferOperations, OtherPostInvocationOperations>&& other) && {
    other.shift(tmpbuf.size());
    return make_operation_block(
        std::move(tmpbuf).merge(std::move(other.tmpbuf)),
        std::move(sequence).merge(std::move(other.sequence)));
  }

  private:
  temporary_buffer<IsConst, Extent> tmpbuf;
  operation_sequence<IsConst, PreInvocationOperations, BufferOperations, PostInvocationOperations> sequence;
};

template<bool IsConst>
class operation_block<IsConst, 0, std::tuple<>, std::tuple<>, std::tuple<>> {
  public:
  static inline constexpr std::size_t extent = 0;

  operation_block() = default;

  auto sender_chain() && {
    return execution::noop();
  }

  auto shift(std::size_t increase) -> void {}

  template<typename Fn>
  auto append(pre_invocation<Fn>&& op) && {
    return make_operation_block(
        temporary_buffer<IsConst>(),
        operation_sequence<IsConst>().append_operation(std::move(op)));
  }

  template<typename Fn>
  auto append(post_invocation<Fn>&& op) && {
    return make_operation_block(
        temporary_buffer<IsConst>(),
        operation_sequence<IsConst>().append_operation(std::move(op)));
  }

  template<std::size_t N>
  auto append(span_reference<IsConst, N>&& op) && {
    return make_operation_block(
        temporary_buffer<IsConst>(),
        operation_sequence<IsConst>().append_operation(std::move(op)));
  }

  template<std::size_t N, typename... Op>
  auto buffer(std::span<const std::byte, N> data, Op&&... op) && {
    static_assert(IsConst);

    const auto len = data.size();
    (op.assign(0, len), ...);
    return make_operation_block(
        temporary_buffer<IsConst>().append(std::move(data)),
        operation_sequence<IsConst>().append_operation(temporary_buffer_reference<true, N>(0, len), std::forward<Op>(op)...));
  }

  template<std::size_t N, typename... Op>
  requires (N != std::dynamic_extent)
  auto buffer(Op&&... op) && {
    static_assert(!IsConst);

    (op.assign(0, N), ...);
    return make_operation_block(
        temporary_buffer<IsConst>().template append<N>(),
        operation_sequence<IsConst>().append_operation(temporary_buffer_reference<false, N>(0, N), std::forward<Op>(op)...));
  }

  template<typename... Op>
  requires (sizeof...(Op) > 0)
  auto buffer(std::size_t n, Op&&... op) && {
    static_assert(!IsConst);

    (op.assign(0, n), ...);
    return make_operation_block(
        temporary_buffer<IsConst>().append(n),
        operation_sequence<IsConst>().append_operation(temporary_buffer_reference<false>(0, n), std::forward<Op>(op)...));
  }

  template<std::size_t OtherExtent, typename OtherPreInvocationOperations, typename OtherBufferOperations, typename OtherPostInvocationOperations>
  auto append(operation_block<IsConst, OtherExtent, OtherPreInvocationOperations, OtherBufferOperations, OtherPostInvocationOperations>&& other) &&
  -> operation_block<IsConst, OtherExtent, OtherPreInvocationOperations, OtherBufferOperations, OtherPostInvocationOperations>&& {
    other.shift(0);
    return std::move(other);
  }
};


struct uint8_t {
  template<std::unsigned_integral T>
  auto read(T& v) const {
    return operation_block<false>{}
        .buffer<4>(
            post_buffer_invocation(
                [&v](std::span<const std::byte> tmpbuf) {
                  assert(tmpbuf.size() == 4);
                  if (tmpbuf[0] != std::byte{0} ||
                      tmpbuf[1] != std::byte{0} ||
                      tmpbuf[2] != std::byte{0})
                    throw xdr_error("integer overflow for 8-bit value");
                  v = static_cast<std::uint8_t>(tmpbuf[3]);
                }));
  }

  template<std::unsigned_integral T>
  auto write(const T& v) const {
    if (v > std::numeric_limits<std::uint8_t>::max())
      throw xdr_error("integer overflow for 8-bit value");

    std::array<std::byte, 4> data{ std::byte{}, std::byte{}, std::byte{}, static_cast<std::byte>(static_cast<std::uint8_t>(v)) };
    return operation_block<true>{}
        .buffer(std::span<const std::byte, 4>(data));
  }

  template<std::signed_integral T>
  auto read(T& v) const {
    return operation_block<false>{}
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
                }));
  }

  template<std::signed_integral T>
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

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
    return operation_block<true>{}
        .buffer(std::as_bytes(std::span<std::uint32_t, 1>(&v32, 1)));
  }
};


struct uint16_t {
  template<std::unsigned_integral T>
  auto read(T& v) const {
    return operation_block<false>{}
        .buffer<4>(
            post_buffer_invocation(
                [&v](std::span<const std::byte> tmpbuf) {
                  assert(tmpbuf.size() == 4);
                  if (tmpbuf[0] != std::byte{0} ||
                      tmpbuf[1] != std::byte{0})
                    throw xdr_error("integer overflow for 16-bit value");
                  v = 0x100u * static_cast<std::uint8_t>(tmpbuf[2]) + static_cast<std::uint8_t>(tmpbuf[3]);
                }));
  }

  template<std::unsigned_integral T>
  auto write(T& v) const {
    if (v > std::numeric_limits<std::uint16_t>::max())
      throw xdr_error("integer overflow for 16-bit value");

    std::array<std::byte, 4> data{ std::byte{}, std::byte{}, static_cast<std::byte>(static_cast<std::uint8_t>(v >> 8 & 0xffu)), static_cast<std::byte>(static_cast<std::uint8_t>(v & 0xffu)) };
    return operation_block<true>{}
        .buffer(std::span<const std::byte, 4>(data));
  }

  template<std::signed_integral T>
  auto read(T& v) const {
    return operation_block<false>{}
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
                }));
  }

  template<std::signed_integral T>
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

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
    return operation_block<true>{}
        .buffer(std::as_bytes(std::span<std::uint32_t, 1>(&v32, 1)));
  }
};


struct uint32_t {
  template<std::unsigned_integral T>
  requires (sizeof(T) == 4)
  auto read(T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    if constexpr(std::endian::native == std::endian::big) {
      return operation_block<false>{}
          .append(span_reference<false, 4>(std::as_writable_bytes(std::span<T, 1>(&v, 1))));
    } else {
      return operation_block<false>{}
          .append(span_reference<false, 4>(std::as_writable_bytes(std::span<T, 1>(&v, 1))))
          .append(
              post_invocation(
                  [&v]() noexcept {
                    v = (v & 0xff000000u) >> 24
                      | (v & 0x00ff0000u) >>  8
                      | (v & 0x0000ff00u) <<  8
                      | (v & 0x000000ffu) << 24;
                  }));
    }
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) == 4)
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    if constexpr(std::endian::native == std::endian::big) {
      return operation_block<true>{}
          .append(span_reference<true, 4>(std::as_bytes(std::span<T, 1>(&v, 1))));
    } else if constexpr(std::endian::native == std::endian::little) {
      const std::uint32_t x = (v & 0xff000000u) >> 24
                            | (v & 0x00ff0000u) >>  8
                            | (v & 0x0000ff00u) <<  8
                            | (v & 0x000000ffu) << 24;
      return operation_block<true>{}
          .buffer(std::as_bytes(std::span<const std::uint32_t, 1>(&x, 1)));
    }
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) < 4)
  auto read(T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    return operation_block<false>{}
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
                }));
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) < 4)
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    std::uint32_t v32 = v;
    if constexpr(std::endian::native == std::endian::little) {
      v32 = (v32 & 0xff000000u) >> 24
          | (v32 & 0x00ff0000u) >>  8
          | (v32 & 0x0000ff00u) <<  8
          | (v32 & 0x000000ffu) << 24;
    }
    return operation_block<true>{}
        .buffer(std::as_bytes(std::span<std::uint32_t, 1>(&v32, 1)));
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) > 4)
  auto read(T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    return operation_block<false>{}
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
                }));
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) > 4)
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    if (v > std::numeric_limits<std::uint32_t>::max())
      throw xdr_error("integer value too big to write");

    std::uint32_t v32 = v;
    if constexpr(std::endian::native == std::endian::little) {
      v32 = (v32 & 0xff000000u) >> 24
          | (v32 & 0x00ff0000u) >>  8
          | (v32 & 0x0000ff00u) <<  8
          | (v32 & 0x000000ffu) << 24;
    }
    return operation_block<true>{}
        .buffer(std::as_bytes(std::span<std::uint32_t, 1>(&v32, 1)));
  }

  template<std::signed_integral T>
  auto read(T& v) const {
    return operation_block<false>{}
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
                }));
  }

  template<std::signed_integral T>
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

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
    return operation_block<true>{}
        .buffer(std::as_bytes(std::span<std::uint32_t, 1>(&v32, 1)));
  }
};


struct uint64_t {
  template<std::unsigned_integral T>
  requires (sizeof(T) == 8)
  auto read(T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    if constexpr(std::endian::native == std::endian::big) {
      return operation_block<false>{}
          .append(span_reference<false, 8>(std::as_writable_bytes(std::span<T, 1>(&v, 1))));
    } else {
      return operation_block<false>{}
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
                  }));
    }
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) == 8)
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    if constexpr(std::endian::native == std::endian::big) {
      return operation_block<true>{}
          .append(span_reference<true, 8>(std::as_bytes(std::span<T, 1>(&v, 1))));
    } else if constexpr(std::endian::native == std::endian::little) {
      const std::uint64_t x = (v & 0xff00000000000000ull) >> 56
                            | (v & 0x00ff000000000000ull) >> 40
                            | (v & 0x0000ff0000000000ull) >> 24
                            | (v & 0x000000ff00000000ull) >>  8
                            | (v & 0x00000000ff000000ull) <<  8
                            | (v & 0x0000000000ff0000ull) << 24
                            | (v & 0x000000000000ff00ull) << 40
                            | (v & 0x00000000000000ffull) << 56;
      return operation_block<true>{}
          .buffer(std::as_bytes(std::span<const std::uint64_t, 1>(&x, 1)));
    }
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) < 8)
  auto read(T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    return operation_block<false>{}
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
                }));
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) < 8)
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

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
    return operation_block<true>{}
        .buffer(std::as_bytes(std::span<std::uint64_t, 1>(&v64, 1)));
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) > 8)
  auto read(T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

    return operation_block<false>{}
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
                }));
  }

  template<std::unsigned_integral T>
  requires (sizeof(T) > 8)
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

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
    return operation_block<true>{}
        .buffer(std::as_bytes(std::span<std::uint64_t, 1>(&v64, 1)));
  }

  template<std::signed_integral T>
  auto read(T& v) const {
    return operation_block<false>{}
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
                }));
  }

  template<std::signed_integral T>
  auto write(const T& v) const {
    static_assert(std::endian::native == std::endian::big || std::endian::native == std::endian::little,
        "implementation only knows little and big endian");

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
    return operation_block<true>{}
        .buffer(std::as_bytes(std::span<std::uint64_t, 1>(&v64, 1)));
  }
};


} /* namespace operation */


inline constexpr operation::uint8_t  uint8{};
inline constexpr operation::uint16_t uint16{};
inline constexpr operation::uint32_t uint32{};
inline constexpr operation::uint64_t uint64{};


} /* namespace earnest::xdr */
