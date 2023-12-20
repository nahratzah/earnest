/* IO executions.
 *
 * The IO executions are all initiating senders.
 *
 * Read-operations yield `(std::size_t bytes, bool eof)'
 * (where `bytes' indicates the number of bytes read, and `eof' indicates if eof was encountered).
 * Write-operations yield `(std::size_t bytes)'
 * (where `bytes' indicates the number of bytes written).
 *
 * Operations with `ec' in the name, will report errors using std::error_code.
 * Operations without `ec' in the name, will report errors using exception-pointer
 * (containing std::system_error).
 *
 * The `read_some' and `write_some' family of operations, will write some number of bytes.
 * (They may write zero bytes.)
 * Their counterparts without `some' in the name, will write all bytes.
 * Or at least `minbytes' bytes if a number of bytes was specified.
 *
 * The read_some/write_some operations may accept a scheduler.
 * If they do, then that scheduler is used as a customization-point:
 * by providing a specialization of
 * `tag_invoke(read_some_ec_t, Scheduler, FD, std::span<const std::byte>, std::optional<std::size_t>)'
 * If no such specialization exists, the code will default to
 * `on(Scheduler, read_some_ec(FD, std::span<const std::byte>, std::optional<std::size_t>))'
 *
 * For read_some/write_some without a bound scheduler, the default implementation
 * will observe if the receiver has a scheduler. And if so, bind to that scheduler
 * if possible.
 *
 * The default implementations of all operations are blocking operations.
 * (Might add non-blocking ones later.)
 */
#pragma once

#include <algorithm>
#include <cerrno>
#include <concepts>
#include <cstddef>
#include <exception>
#include <numeric>
#include <optional>
#include <ranges>
#include <span>
#include <system_error>

#include "execution.h"
#include "execution_util.h"

#include <sys/uio.h>
#include <unistd.h>

namespace earnest::execution::io {


enum class errc {
  eof
};

inline auto category() noexcept -> const std::error_category& {
  class category_impl
  : public std::error_category
  {
    public:
    auto name() const noexcept -> const char* override {
      return "earnest::execution::io";
    }

    auto message(int condition) const -> std::string override {
      switch (errc{condition}) {
        default:
          return "unknown";
        case errc::eof:
          return "end_of_file";
      }
    }
  };

  static const category_impl impl;
  return impl;
}

inline auto make_error_code(errc e) {
  return std::error_code(static_cast<int>(e), category());
}


} /* namespace earnest::execution::io */

namespace std {


template<>
struct is_error_code_enum<::earnest::execution::io::errc> : std::true_type {};


} /* namespace std */

namespace earnest::execution::io {


// Type used for file offsets.
using offset_type = std::uint64_t;


// Multiple spans describing a buffer.
// This is used for vectored writes.
template<typename T>
concept const_buffer_sequence = std::ranges::forward_range<T> && std::convertible_to<std::ranges::range_value_t<T>, std::span<const std::byte>>;

// Multiple spans describing a buffer.
// This is used for vectored reads.
//
// (Note that any mutable_buffer_sequence also satisfies the const_buffer_sequence concept,
// so it's best not to use them as a discriminant for overloading.)
template<typename T>
concept mutable_buffer_sequence = std::ranges::forward_range<T> && std::convertible_to<std::ranges::range_value_t<T>, std::span<std::byte>>;

// Single-span or buffer-sequence buffer.
template<typename T>
concept const_buffers = std::convertible_to<std::remove_cvref_t<T>, std::span<const std::byte>> || const_buffer_sequence<T>;

// Single-span or buffer-sequence buffer.
template<typename T>
concept mutable_buffers = std::convertible_to<std::remove_cvref_t<T>, std::span<std::byte>> || mutable_buffer_sequence<T>;


// Convert a buffer-sequence into a vector of spans.
template<mutable_buffer_sequence Buffers>
inline auto vectorize_mutable_buffers(Buffers&& buf) -> std::vector<std::span<std::byte>> {
#if __cpp_lib_ranges_to_container >= 202202L
  return std::forward<Buffers>(buf)
  | std::ranges::to<std::vector<std::span<std::byte>>>();
#else
  std::vector<std::span<std::byte>> result;
  // Using a raw iterator, so we can play nice with the sentinel.
  // (Ranges can have differently-typed begin and end iterators,
  // whereas <algorithms> always wants the same type for those iterators.)
  for (auto iter = std::ranges::begin(buf);
      iter != std::ranges::end(buf);
      ++iter) {
    result.emplace_back(*iter);
  }
  return result;
#endif
}

// Convert a buffer-sequence into a vector of spans.
template<const_buffer_sequence Buffers>
inline auto vectorize_const_buffers(Buffers&& buf) -> std::vector<std::span<const std::byte>> {
#if __cpp_lib_ranges_to_container >= 202202L
  return std::forward<Buffers>(buf)
  | std::ranges::to<std::vector<std::span<const std::byte>>>();
#else
  std::vector<std::span<const std::byte>> result;
  // Using a raw iterator, so we can play nice with the sentinel.
  // (Ranges can have differently-typed begin and end iterators,
  // whereas <algorithms> always wants the same type for those iterators.)
  for (auto iter = std::ranges::begin(buf);
      iter != std::ranges::end(buf);
      ++iter) {
    result.emplace_back(*iter);
  }
  return result;
#endif
}

// Convert a buffer-sequence into a vector, using some transformation.
template<mutable_buffer_sequence Buffers, std::invocable<std::span<std::byte>> TransformFn>
inline auto vectorize_mutable_buffers(Buffers&& buf, TransformFn&& transform_fn)
-> auto {
  using result_value_type = std::invoke_result_t<std::add_lvalue_reference_t<TransformFn>, std::span<std::byte>>;
  using result_type = std::vector<result_value_type>;

#if __cpp_lib_ranges_to_container >= 202202L
  return std::forward<Buffers>(buf)
  | std::ranges::views::transform(std::forward<TransformFn>(transform_fn))
  | std::ranges::to<std::vector<>();
#else
  result_type result;
  // Using a raw iterator, so we can play nice with the sentinel.
  // (Ranges can have differently-typed begin and end iterators,
  // whereas <algorithms> always wants the same type for those iterators.)
  for (auto iter = std::ranges::begin(buf);
      iter != std::ranges::end(buf);
      ++iter) {
    result.emplace_back(std::invoke(transform_fn, *iter));
  }
  return result;
#endif
}

// Convert a buffer-sequence into a vector, using some transformation.
template<const_buffer_sequence Buffers, std::invocable<std::span<std::byte>> TransformFn>
inline auto vectorize_const_buffers(Buffers&& buf, TransformFn&& transform_fn)
-> auto {
  using result_value_type = std::invoke_result_t<std::add_lvalue_reference_t<TransformFn>, std::span<const std::byte>>;
  using result_type = std::vector<result_value_type>;

#if __cpp_lib_ranges_to_container >= 202202L
  return std::forward<Buffers>(buf)
  | std::ranges::views::transform(std::forward<TransformFn>(transform_fn))
  | std::ranges::to<std::vector<>();
#else
  result_type result;
  // Using a raw iterator, so we can play nice with the sentinel.
  // (Ranges can have differently-typed begin and end iterators,
  // whereas <algorithms> always wants the same type for those iterators.)
  for (auto iter = std::ranges::begin(buf);
      iter != std::ranges::end(buf);
      ++iter) {
    result.emplace_back(std::invoke(transform_fn, *iter));
  }
  return result;
#endif
}


// The buffer aspect of a readwrite-state.
//
// This version uses a std::span.
template<typename Byte>
requires (std::is_same_v<std::remove_const_t<Byte>, std::byte>)
struct readwrite_state_span_ {
  std::span<Byte> buf;

  explicit readwrite_state_span_(std::span<Byte> buf) noexcept
  : buf(buf)
  {}

  protected:
  inline auto update_buf_(std::size_t len) -> void {
    assert(buf.size() >= len);
    buf = buf.subspan(len);
  }

  inline auto size() const noexcept -> std::size_t {
    return buf.size();
  }
};

// The buffer aspect of a readwrite-state.
//
// This version uses a vector of std::span.
template<typename Byte>
requires (std::is_same_v<std::remove_const_t<Byte>, std::byte>)
struct readwrite_state_vectored_ {
  std::vector<std::span<Byte>> buf;

  template<typename Buffers>
  explicit readwrite_state_vectored_(Buffers&& buf)
  : buf(convert_to_buffer_(std::forward<Buffers>(buf)))
  {}

  protected:
  inline auto update_buf_(std::size_t len) -> void {
    auto iter = buf.begin();
    while (iter != buf.end() && iter->size() <= len) {
      len -= iter->size();
      ++iter;
    }
    assert(iter != buf.end() || len == 0); // If this trips, we read/wrote more than we had buffers for.
    if (len != 0) *iter = iter->subspan(len);

    buf.erase(buf.begin(), iter);
  }

  inline auto size() const noexcept -> std::size_t {
    return std::accumulate(buf.begin(), buf.end(), std::size_t(0),
        [](std::size_t partial_sum, std::span<Byte> s) -> std::size_t {
          return partial_sum + s.size();
        });
  }

  private:
  template<typename Buffers>
  inline auto convert_to_buffer_(Buffers&& buf) -> decltype(auto) {
    if constexpr(std::is_const_v<Byte>) {
      return vectorize_const_buffers(std::forward<Buffers>(buf));
    } else {
      return vectorize_mutable_buffers(std::forward<Buffers>(buf));
    }
  }
};

// Readwrite-state aspect for the presence/absence of an offset.
template<bool EnableOffset> struct readwrite_state_offset_;

template<>
struct readwrite_state_offset_<true> {
  offset_type offset;

  explicit readwrite_state_offset_(offset_type offset) noexcept
  : offset(offset)
  {}

  protected:
  inline auto update_offset_(std::size_t len) -> void {
    offset += len;
  }
};

template<>
struct readwrite_state_offset_<false> {
  protected:
  inline auto update_offset_([[maybe_unused]] std::size_t len) -> void {}
};

// A readwrite_state.
//
// We use this during reads and writes, to ensure we write the minimum-requested bytes.
//
// Note: FD may be a reference (this is in fact very likely).
template<typename FD, bool ForReading, bool Vectored, bool EnableOffset>
class readwrite_state_
: public readwrite_state_offset_<EnableOffset>,
  public std::conditional_t<
      Vectored,
      readwrite_state_vectored_<std::conditional_t<ForReading, std::byte, const std::byte>>,
      readwrite_state_span_<std::conditional_t<ForReading, std::byte, const std::byte>>>
{
  public:
  FD fd;
  std::size_t minbytes;
  std::size_t operation_count = 0;

  template<typename FD_, typename Buffers>
  readwrite_state_(FD_&& fd, offset_type offset, Buffers&& buf, std::optional<std::size_t> minbytes)
  : readwrite_state_offset_<EnableOffset>(offset),
    std::conditional_t<
        Vectored,
        readwrite_state_vectored_<std::conditional_t<ForReading, std::byte, const std::byte>>,
        readwrite_state_span_<std::conditional_t<ForReading, std::byte, const std::byte>>>(std::forward<Buffers>(buf)),
    fd(std::forward<FD_>(fd)),
    minbytes(minbytes.value_or(this->size()))
  {
    if (this->minbytes > this->size())
      throw std::logic_error("buffer is too short for requested minimum read/write");
  }

  template<typename FD_, typename Buffers>
  readwrite_state_(FD_&& fd, Buffers&& buf, std::optional<std::size_t> minbytes)
  : std::conditional_t<
        Vectored,
        readwrite_state_vectored_<std::conditional_t<ForReading, std::byte, const std::byte>>,
        readwrite_state_span_<std::conditional_t<ForReading, std::byte, const std::byte>>>(std::forward<Buffers>(buf)),
    fd(std::forward<FD_>(fd)),
    minbytes(minbytes.value_or(this->size()))
  {
    if (this->minbytes > this->size())
      throw std::logic_error("buffer is too short for requested minimum read/write");
  }

  readwrite_state_(const readwrite_state_& other) = delete;
  readwrite_state_(readwrite_state_&& other) = default;

  auto make_updater() & {
    return [this](std::size_t rlen) {
      assert(this->size() >= rlen);
      this->update_buf_(rlen);
      this->update_offset_(rlen);
      this->minbytes -= rlen;
      this->operation_count += rlen;
    };
  }
};

// Make a new readwrite-state.
template<bool ForReading, typename FD, typename Buffers>
auto make_readwrite_state_(FD&& fd, offset_type offset, Buffers&& buf, std::optional<std::size_t> minbytes) {
  if constexpr(std::convertible_to<Buffers, std::span<std::byte>> || std::convertible_to<Buffers, std::span<const std::byte>>) {
    return readwrite_state_<FD, ForReading, false, true>(std::forward<FD>(fd), offset, std::forward<Buffers>(buf), minbytes);
  } else {
    return readwrite_state_<FD, ForReading, true, true>(std::forward<FD>(fd), offset, std::forward<Buffers>(buf), minbytes);
  }
}

// Make a new readwrite-state.
template<bool ForReading, typename FD, typename Buffers>
auto make_readwrite_state_(FD&& fd, Buffers&& buf, std::optional<std::size_t> minbytes) {
  if constexpr(std::convertible_to<Buffers, std::span<std::byte>> || std::convertible_to<Buffers, std::span<const std::byte>>) {
    return readwrite_state_<FD, ForReading, false, false>(std::forward<FD>(fd), std::forward<Buffers>(buf), minbytes);
  } else {
    return readwrite_state_<FD, ForReading, true, false>(std::forward<FD>(fd), std::forward<Buffers>(buf), minbytes);
  }
}


// Adapter that, when the chain completes with an error-signal holding a std::error_code,
// converts it into a std::system_error.
//
// Anything else passes through unaltered.
struct ec_to_exception_t {
  template<typename> friend struct ::earnest::execution::_generic_operand_base_t;

  template<sender Sender>
  auto operator()(Sender&& s) const
  noexcept(noexcept(_generic_operand_base<set_error_t>(std::declval<const ec_to_exception_t&>(), std::declval<Sender>())))
  -> sender decltype(auto) {
    return _generic_operand_base<set_error_t>(*this, std::forward<Sender>(s));
  }

  auto operator()() const noexcept -> sender decltype(auto) {
    return _generic_adapter(*this);
  }

  private:
  template<sender Sender> class sender_impl;

  template<typename T>
  using is_not_error_code = std::negation<std::is_same<std::error_code, T>>;

  template<sender Sender>
  struct sender_types_for_impl
  : _generic_sender_wrapper<sender_impl<Sender>, Sender, connect_t>
  {
    // Inherit constructors
    using _generic_sender_wrapper<sender_impl<Sender>, Sender, connect_t>::_generic_sender_wrapper;
  };

  template<receiver Receiver>
  struct receiver_impl
  : _generic_receiver_wrapper<Receiver, set_value_t, set_error_t>
  {
    public:
    explicit receiver_impl(Receiver&& r)
    noexcept(std::is_nothrow_move_constructible_v<Receiver>)
    : _generic_receiver_wrapper<Receiver, set_value_t, set_error_t>(std::move(r))
    {}

    template<typename... Args>
    friend auto tag_invoke([[maybe_unused]] set_value_t, receiver_impl&& self, Args&&... args) noexcept -> void {
      try {
        execution::set_value(std::move(self.r), std::forward<Args>(args)...);
      } catch (...) {
        execution::set_error(std::move(self.r), std::current_exception());
      }
    }

    template<typename Error>
    friend auto tag_invoke([[maybe_unused]] set_error_t, receiver_impl&& self, Error&& error) noexcept -> decltype(auto) {
      if constexpr(std::is_same_v<std::remove_cvref_t<Error>, std::error_code>)
        return execution::set_error(std::move(self.r), std::make_exception_ptr(std::system_error(std::forward<Error>(error))));
      else
        return execution::set_error(std::move(self.r), std::forward<Error>(error));
    }
  };

  template<typed_sender Sender>
  struct sender_types_for_impl<Sender>
  : _generic_sender_wrapper<sender_impl<Sender>, Sender, connect_t>
  {
    public:
    // The error-types we expose,
    // is all the original error-codes,
    // except for std::error_code (so we remove that)
    // which we turn into std::system_error (so we add std::exception_ptr).
    template<template<typename...> class Variant>
    using error_types =
        typename _type_appender<>::merge<
            _type_appender<std::exception_ptr>,    // We turn error_code into an exception, so we'll need to publish exception_ptr.
            typename sender_traits<Sender>::       // Take the sender traits
            template error_types<_type_appender>:: // and compute the collection of error types
            template retain<is_not_error_code>>::  // and drop the error_code
        template type<Variant>;                    // then squash it into a variant.

    // Inherit constructors
    using _generic_sender_wrapper<sender_impl<Sender>, Sender, connect_t>::_generic_sender_wrapper;
  };

  template<sender Sender>
  class sender_impl
  : public sender_types_for_impl<Sender>
  {
    template<typename, sender, typename...> friend class ::earnest::execution::_generic_sender_wrapper;

    public:
    template<typename Sender_>
    requires std::constructible_from<Sender, Sender_>
    explicit sender_impl(Sender_&& s)
    noexcept(std::is_nothrow_constructible_v<Sender, Sender_>)
    : sender_types_for_impl<Sender>(std::forward<Sender_>(s))
    {}

    template<receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> operation_state decltype(auto) {
      return execution::connect(
          std::move(self.s),
          receiver_impl<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r)));
    }

    private:
    template<sender OtherSender>
    auto rebind(OtherSender&& other_sender) &&
    noexcept(std::is_nothrow_constructible_v<sender_impl<std::remove_cvref_t<OtherSender>>, OtherSender>)
    -> sender_impl<std::remove_cvref_t<OtherSender>> {
      return sender_impl<std::remove_cvref_t<OtherSender>>(std::forward<OtherSender>(other_sender));
    }
  };

  template<sender Sender>
  auto default_impl(Sender&& s) const
  -> sender_impl<std::remove_cvref_t<Sender>> {
    return sender_impl<std::remove_cvref_t<Sender>>(std::forward<Sender>(s));
  }
};
inline constexpr ec_to_exception_t ec_to_exception{};


// Read some bytes from a file-descriptor, at a specific offset.
//
// The default implementation only exists for `int' file-descriptor,
// and invokes the pread(2) system call.
//
// If the read is interrupted (errno == EINTR), the error will be swallowed
// and zero-bytes-written will be reported instead.
//
// If the read ended because of EOF, the eof-error will be emitted.
//
// Errors are communicated as std::error_code.
struct lazy_read_some_at_ec_t {
  template<typename FD>
  requires (!std::same_as<std::remove_cvref_t<FD>, int>)
  auto operator()(FD&& fd, offset_type offset, std::span<std::byte> buf) const
  noexcept(nothrow_tag_invocable<lazy_read_some_at_ec_t, FD, offset_type, std::span<std::byte>>)
  -> typed_sender decltype(auto) {
    return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(offset), std::move(buf));
  }

  template<typename FD, mutable_buffer_sequence Buffers>
  requires (!std::same_as<std::remove_cvref_t<FD>, int>)
  auto operator()(FD&& fd, offset_type offset, Buffers&& buf) const
  noexcept(
      tag_invocable<lazy_read_some_at_ec_t, FD, offset_type, Buffers> ?
      nothrow_tag_invocable<lazy_read_some_at_ec_t, FD, offset_type, Buffers> :
      std::is_nothrow_invocable_v<lazy_read_some_at_ec_t, FD, offset_type, std::span<std::byte>>)
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_read_some_at_ec_t, FD, offset_type, Buffers>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf));
    } else {
      std::span<std::byte> s;
      if (!std::ranges::empty(buf)) s = *std::ranges::begin(buf);
      return (*this)(std::forward<FD>(fd), std::move(offset), std::move(s));
    }
  }

  private:
  template<receiver_of<std::size_t> Receiver>
  class opstate
  : public operation_state_base_
  {
    public:
    explicit opstate(Receiver&& r, int fd, offset_type offset, std::span<std::byte> buf)
    noexcept(std::is_nothrow_move_constructible_v<Receiver>)
    : r(std::move(r)),
      fd(fd),
      offset(offset),
      buf(buf)
    {}

    friend auto tag_invoke([[maybe_unused]] start_t, opstate& self) noexcept -> void {
      auto rlen = ::pread(self.fd, self.buf.data(), self.buf.size(), self.offset);
      bool eof = true;
      if (rlen == -1) {
        auto ec = std::error_code(errno, std::generic_category());
        if (ec == std::make_error_code(std::errc::interrupted)) {
          rlen = 0;
          eof = false;
        } else {
          execution::set_error(std::move(self.r), std::move(ec));
          return;
        }
      }
      if (rlen != 0) eof = false;

      try {
        if (eof)
          execution::set_error(std::move(self.r), make_error_code(errc::eof));
        else
          execution::set_value(std::move(self.r), static_cast<std::size_t>(rlen));
      } catch (...) {
        execution::set_error(std::move(self.r), std::current_exception());
      }
    }

    private:
    Receiver r;
    int fd;
    offset_type offset;
    std::span<std::byte> buf;
  };

  template<receiver_of<std::size_t> Receiver>
  class vectored_opstate
  : public operation_state_base_
  {
    public:
    template<mutable_buffer_sequence Buffers>
    explicit vectored_opstate(Receiver&& r, int fd, offset_type offset, Buffers&& buf)
    noexcept(std::is_nothrow_move_constructible_v<Receiver>)
    : r(std::move(r)),
      fd(fd),
      offset(offset),
      iov(vectorize_mutable_buffers(
              std::forward<Buffers>(buf),
              [](std::span<std::byte> s) {
                using iovec = struct ::iovec;
                return iovec{
                  .iov_base=s.data(),
                  .iov_len=s.size()
                };
              }))
    {}

    friend auto tag_invoke([[maybe_unused]] start_t, vectored_opstate& self) noexcept -> void {
      auto rlen = ::preadv(self.fd, self.iov.data(), self.iov.size(), self.offset);
      bool eof = true;
      if (rlen == -1) {
        auto ec = std::error_code(errno, std::generic_category());
        if (ec == std::make_error_code(std::errc::interrupted)) {
          rlen = 0;
          eof = false;
        } else {
          execution::set_error(std::move(self.r), std::move(ec));
          return;
        }
      }
      if (rlen != 0) eof = false;

      try {
        if (eof)
          execution::set_error(std::move(self.r), make_error_code(errc::eof));
        else
          execution::set_value(std::move(self.r), static_cast<std::size_t>(rlen));
      } catch (...) {
        execution::set_error(std::move(self.r), std::current_exception());
      }
    }

    private:
    Receiver r;
    int fd;
    offset_type offset;
    std::vector<struct ::iovec> iov;
  };

  // Figure out if a given receiver has a scheduler.
  template<receiver, typename = void>
  struct has_scheduler_
  : std::false_type
  {};
  template<receiver T>
  struct has_scheduler_<T, std::void_t<decltype(execution::get_scheduler(std::declval<const T&>()))>>
  : std::true_type
  {};

  // Figure out if a given receiver has a scheduler.
  template<receiver Receiver>
  static inline constexpr bool has_scheduler = has_scheduler_<Receiver>::value;

  class sender_impl {
    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<Tuple<std::size_t>>;

    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr, std::error_code>;

    static inline constexpr bool sends_done = false;

    explicit sender_impl(int fd, offset_type offset, std::span<std::byte> buf) noexcept
    : fd(fd),
      offset(offset),
      buf(buf)
    {}

    // Only permit move operations.
    sender_impl(sender_impl&&) = default;
    sender_impl(const sender_impl&) = delete;

    // Connect this sender with a receiver.
    //
    // If the receiver has a scheduler, we'll try to forward to an implementation of lazy_write_ec,
    // that is specific for the scheduler.
    // But if there isn't one, then we'll use our own implementation (which uses a block write).
    template<receiver_of<std::size_t> Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> operation_state decltype(auto) {
      if constexpr(has_scheduler<std::remove_cvref_t<Receiver>>) {
        if constexpr(tag_invocable<lazy_read_some_at_ec_t, decltype(execution::get_scheduler(r)), int, offset_type, std::span<std::byte>>) {
          return execution::connect(
              execution::tag_invoke(execution::get_scheduler(r), self.fd, self.offset, std::move(self.buf)),
              std::forward<Receiver>(r));
        } else {
          // XXX we have a scheduler, and no specialized implementation
          // we should create a global-configurable implementation
          return opstate<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r), self.fd, self.offset, self.buf);
        }
      } else {
        return opstate<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r), self.fd, self.offset, self.buf);
      }
    }

    private:
    int fd;
    offset_type offset;
    std::span<std::byte> buf;
  };

  class vectored_sender_impl {
    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<Tuple<std::size_t>>;

    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr, std::error_code>;

    static inline constexpr bool sends_done = false;

    template<mutable_buffer_sequence Buffers>
    explicit vectored_sender_impl(int fd, offset_type offset, Buffers&& buf) noexcept
    : fd(fd),
      offset(offset),
      buf(vectorize_mutable_buffers(std::forward<Buffers>(buf)))
    {}

    // Only permit move operations.
    vectored_sender_impl(vectored_sender_impl&&) = default;
    vectored_sender_impl(const vectored_sender_impl&) = delete;

    // Connect this sender with a receiver.
    //
    // If the receiver has a scheduler, we'll try to forward to an implementation of lazy_write_ec,
    // that is specific for the scheduler.
    // But if there isn't one, then we'll use our own implementation (which uses a block write).
    template<receiver_of<std::size_t> Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, vectored_sender_impl&& self, Receiver&& r)
    -> operation_state decltype(auto) {
      if constexpr(has_scheduler<std::remove_cvref_t<Receiver>>) {
        if constexpr(tag_invocable<lazy_read_some_at_ec_t, decltype(execution::get_scheduler(r)), int, offset_type, std::vector<std::span<std::byte>>>) {
          return execution::connect(
              execution::tag_invoke(execution::get_scheduler(r), self.fd, self.offset, std::move(self.buf)),
              std::forward<Receiver>(r));
        } else {
          // XXX we have a scheduler, and no specialized implementation
          // we should create a global-configurable implementation
          return vectored_opstate<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r), self.fd, self.offset, std::move(self.buf));
        }
      } else {
        return vectored_opstate<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r), self.fd, self.offset, std::move(self.buf));
      }
    }

    private:
    int fd;
    offset_type offset;
    std::vector<std::span<std::byte>> buf;
  };

  public:
  auto operator()(int fd, offset_type offset, std::span<std::byte> buf) const
  noexcept
  -> sender_impl {
    return sender_impl(fd, offset, buf);
  }

  template<mutable_buffer_sequence Buffers>
  auto operator()(int fd, offset_type offset, Buffers&& buf) const
  noexcept
  -> vectored_sender_impl {
    return vectored_sender_impl(fd, offset, std::forward<Buffers>(buf));
  }

  template<scheduler Scheduler, typename FD, mutable_buffers Buffers>
  auto operator()([[maybe_unused]] Scheduler&& sch, FD&& fd, offset_type offset, Buffers&& buf) const
  noexcept(
      tag_invocable<lazy_read_some_at_ec_t, Scheduler, FD, offset_type, Buffers> ?
      nothrow_tag_invocable<lazy_read_some_at_ec_t, Scheduler, FD, offset_type, Buffers> :
      std::is_nothrow_invocable_v<lazy_read_some_at_ec_t, FD, offset_type, Buffers>)
  -> decltype(auto) {
    if constexpr(tag_invocable<lazy_read_some_at_ec_t, Scheduler, FD, offset_type, Buffers>)
      return execution::tag_invoke(*this, std::forward<Scheduler>(sch), std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf));
    else
      return execution::lazy_on(std::forward<Scheduler>(sch), (*this)(std::move(fd), std::move(offset), std::forward<Buffers>(buf)));
  }
};
inline constexpr lazy_read_some_at_ec_t lazy_read_some_at_ec{};


// Read some bytes from a file-descriptor.
//
// The default implementation only exists for `int' file-descriptor,
// and invokes the read(2) system call.
//
// If the read is interrupted (errno == EINTR), the error will be swallowed
// and zero-bytes-written will be reported instead.
//
// If the read ended because of EOF, the eof-error will be emitted.
//
// Errors are communicated as std::error_code.
struct lazy_read_some_ec_t {
  template<typename FD>
  requires (!std::same_as<std::remove_cvref_t<FD>, int>)
  auto operator()(FD&& fd, std::span<std::byte> buf) const
  noexcept(nothrow_tag_invocable<lazy_read_some_ec_t, FD, std::span<std::byte>>)
  -> typed_sender decltype(auto) {
    return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(buf));
  }

  template<typename FD, mutable_buffer_sequence Buffers>
  requires (!std::same_as<std::remove_cvref_t<FD>, int>)
  auto operator()(FD&& fd, Buffers&& buf) const
  noexcept(
      tag_invocable<lazy_read_some_ec_t, FD, Buffers> ?
      nothrow_tag_invocable<lazy_read_some_ec_t, FD, Buffers> :
      std::is_nothrow_invocable_v<lazy_read_some_ec_t, FD, std::span<std::byte>>)
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_read_some_ec_t, FD, Buffers>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::forward<Buffers>(buf));
    } else {
      std::span<std::byte> s;
      if (!std::ranges::empty(buf)) s = *std::ranges::begin(buf);
      return (*this)(std::forward<FD>(fd), std::move(s));
    }
  }

  private:
  template<receiver_of<std::size_t> Receiver>
  class opstate
  : public operation_state_base_
  {
    public:
    explicit opstate(Receiver&& r, int fd, std::span<std::byte> buf)
    noexcept(std::is_nothrow_move_constructible_v<Receiver>)
    : r(std::move(r)),
      fd(fd),
      buf(buf)
    {}

    friend auto tag_invoke([[maybe_unused]] start_t, opstate& self) noexcept -> void {
      auto rlen = ::read(self.fd, self.buf.data(), self.buf.size());
      bool eof = true;
      if (rlen == -1) {
        auto ec = std::error_code(errno, std::generic_category());
        if (ec == std::make_error_code(std::errc::interrupted)) {
          rlen = 0;
          eof = false;
        } else {
          execution::set_error(std::move(self.r), std::move(ec));
          return;
        }
      }
      if (rlen != 0) eof = false;

      try {
        if (eof)
          execution::set_error(std::move(self.r), make_error_code(errc::eof));
        else
          execution::set_value(std::move(self.r), static_cast<std::size_t>(rlen));
      } catch (...) {
        execution::set_error(std::move(self.r), std::current_exception());
      }
    }

    private:
    Receiver r;
    int fd;
    std::span<std::byte> buf;
  };

  template<receiver_of<std::size_t> Receiver>
  class vectored_opstate
  : public operation_state_base_
  {
    public:
    template<mutable_buffer_sequence Buffers>
    explicit vectored_opstate(Receiver&& r, int fd, Buffers&& buf)
    noexcept(std::is_nothrow_move_constructible_v<Receiver>)
    : r(std::move(r)),
      fd(fd),
      iov(vectorize_mutable_buffers(
              std::forward<Buffers>(buf),
              [](std::span<std::byte> s) {
                using iovec = struct ::iovec;
                return iovec{
                  .iov_base=s.data(),
                  .iov_len=s.size()
                };
              }))
    {}

    friend auto tag_invoke([[maybe_unused]] start_t, vectored_opstate& self) noexcept -> void {
      auto rlen = ::readv(self.fd, self.iov.data(), self.iov.size());
      bool eof = true;
      if (rlen == -1) {
        auto ec = std::error_code(errno, std::generic_category());
        if (ec == std::make_error_code(std::errc::interrupted)) {
          rlen = 0;
          eof = false;
        } else {
          execution::set_error(std::move(self.r), std::move(ec));
          return;
        }
      }
      if (rlen != 0) eof = false;

      try {
        if (eof)
          execution::set_error(std::move(self.r), make_error_code(errc::eof));
        else
          execution::set_value(std::move(self.r), static_cast<std::size_t>(rlen));
      } catch (...) {
        execution::set_error(std::move(self.r), std::current_exception());
      }
    }

    private:
    Receiver r;
    int fd;
    std::vector<struct ::iovec> iov;
  };

  // Figure out if a given receiver has a scheduler.
  template<receiver, typename = void>
  struct has_scheduler_
  : std::false_type
  {};
  template<receiver T>
  struct has_scheduler_<T, std::void_t<decltype(execution::get_scheduler(std::declval<const T&>()))>>
  : std::true_type
  {};

  // Figure out if a given receiver has a scheduler.
  template<receiver Receiver>
  static inline constexpr bool has_scheduler = has_scheduler_<Receiver>::value;

  class sender_impl {
    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<Tuple<std::size_t>>;

    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr, std::error_code>;

    static inline constexpr bool sends_done = false;

    explicit sender_impl(int fd, std::span<std::byte> buf) noexcept
    : fd(fd),
      buf(buf)
    {}

    // Only permit move operations.
    sender_impl(sender_impl&&) = default;
    sender_impl(const sender_impl&) = delete;

    // Connect this sender with a receiver.
    //
    // If the receiver has a scheduler, we'll try to forward to an implementation of lazy_write_ec,
    // that is specific for the scheduler.
    // But if there isn't one, then we'll use our own implementation (which uses a block write).
    template<receiver_of<std::size_t> Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> operation_state decltype(auto) {
      if constexpr(has_scheduler<std::remove_cvref_t<Receiver>>) {
        if constexpr(tag_invocable<lazy_read_some_ec_t, decltype(execution::get_scheduler(r)), int, std::span<std::byte>>) {
          return execution::connect(
              execution::tag_invoke(execution::get_scheduler(r), self.fd, std::move(self.buf)),
              std::forward<Receiver>(r));
        } else {
          // XXX we have a scheduler, and no specialized implementation
          // we should create a global-configurable implementation
          return opstate<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r), self.fd, self.buf);
        }
      } else {
        return opstate<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r), self.fd, self.buf);
      }
    }

    private:
    int fd;
    std::span<std::byte> buf;
  };

  class vectored_sender_impl {
    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<Tuple<std::size_t>>;

    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr, std::error_code>;

    static inline constexpr bool sends_done = false;

    template<mutable_buffer_sequence Buffers>
    explicit vectored_sender_impl(int fd, Buffers&& buf) noexcept
    : fd(fd),
      buf(vectorize_mutable_buffers(std::forward<Buffers>(buf)))
    {}

    // Only permit move operations.
    vectored_sender_impl(vectored_sender_impl&&) = default;
    vectored_sender_impl(const vectored_sender_impl&) = delete;

    // Connect this sender with a receiver.
    //
    // If the receiver has a scheduler, we'll try to forward to an implementation of lazy_write_ec,
    // that is specific for the scheduler.
    // But if there isn't one, then we'll use our own implementation (which uses a block write).
    template<receiver_of<std::size_t> Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, vectored_sender_impl&& self, Receiver&& r)
    -> operation_state decltype(auto) {
      if constexpr(has_scheduler<std::remove_cvref_t<Receiver>>) {
        if constexpr(tag_invocable<lazy_read_some_ec_t, decltype(execution::get_scheduler(r)), int, std::vector<std::span<std::byte>>>) {
          return execution::connect(
              execution::tag_invoke(execution::get_scheduler(r), self.fd, std::move(self.buf)),
              std::forward<Receiver>(r));
        } else {
          // XXX we have a scheduler, and no specialized implementation
          // we should create a global-configurable implementation
          return vectored_opstate<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r), self.fd, std::move(self.buf));
        }
      } else {
        return vectored_opstate<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r), self.fd, std::move(self.buf));
      }
    }

    private:
    int fd;
    std::vector<std::span<std::byte>> buf;
  };

  public:
  auto operator()(int fd, std::span<std::byte> buf) const
  noexcept
  -> sender_impl {
    return sender_impl(fd, buf);
  }

  template<mutable_buffers Buffers>
  auto operator()(int fd, Buffers&& buf) const
  noexcept
  -> vectored_sender_impl {
    return vectored_sender_impl(fd, std::forward<Buffers>(buf));
  }

  template<scheduler Scheduler, typename FD, mutable_buffers Buffers>
  auto operator()([[maybe_unused]] Scheduler&& sch, FD&& fd, Buffers&& buf) const
  noexcept(
      tag_invocable<lazy_read_some_ec_t, Scheduler, FD, Buffers> ?
      nothrow_tag_invocable<lazy_read_some_ec_t, Scheduler, FD, Buffers> :
      std::is_nothrow_invocable_v<lazy_read_some_ec_t, FD, Buffers>)
  -> decltype(auto) {
    if constexpr(tag_invocable<lazy_read_some_ec_t, Scheduler, FD, Buffers>)
      return execution::tag_invoke(*this, std::forward<Scheduler>(sch), std::forward<FD>(fd), std::forward<Buffers>(buf));
    else
      return execution::lazy_on(std::forward<Scheduler>(sch), (*this)(std::move(fd), std::forward<Buffers>(buf)));
  }
};
inline constexpr lazy_read_some_ec_t lazy_read_some_ec{};


// Write some bytes to a file-descriptor, at a specific offset.
//
// The default implementation only exists for `int' file-descriptor,
// and invokes the pwrite(2) system call.
//
// If the write is interrupted (errno == EINTR), the error will be swallowed
// and zero-bytes-written will be reported instead.
//
// Errors are communicated as std::error_code.
struct lazy_write_some_at_ec_t {
  template<typename FD>
  requires (!std::same_as<std::remove_cvref_t<FD>, int>)
  auto operator()(FD&& fd, offset_type offset, std::span<const std::byte> buf) const
  noexcept(nothrow_tag_invocable<lazy_write_some_at_ec_t, FD, offset_type, std::span<const std::byte>>)
  -> typed_sender decltype(auto) {
    return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(offset), std::move(buf));
  }

  template<typename FD, const_buffer_sequence Buffers>
  requires (!std::same_as<std::remove_cvref_t<FD>, int>)
  auto operator()(FD&& fd, offset_type offset, Buffers&& buf) const
  noexcept(
      tag_invocable<lazy_write_some_at_ec_t, FD, offset_type, Buffers> ?
      nothrow_tag_invocable<lazy_write_some_at_ec_t, FD, offset_type, Buffers> :
      std::is_nothrow_invocable_v<lazy_write_some_at_ec_t, FD, offset_type, std::span<const std::byte>>)
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_write_some_at_ec_t, FD, offset_type, Buffers>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), offset, std::move(buf));
    } else {
      std::span<const std::byte> s;
      if (!std::ranges::empty(buf)) s = *std::ranges::begin(buf);
      return (*this)(std::forward<FD>(fd), offset, std::move(s));
    }
  }

  private:
  template<receiver_of<std::size_t> Receiver>
  class opstate
  : public operation_state_base_
  {
    public:
    explicit opstate(Receiver&& r, int fd, offset_type offset, std::span<const std::byte> buf)
    noexcept(std::is_nothrow_move_constructible_v<Receiver>)
    : r(std::move(r)),
      fd(fd),
      offset(offset),
      buf(buf)
    {}

    friend auto tag_invoke([[maybe_unused]] start_t, opstate& self) noexcept -> void {
      auto wlen = ::pwrite(self.fd, self.buf.data(), self.buf.size(), self.offset);
      if (wlen == -1) {
        auto ec = std::error_code(errno, std::generic_category());
        if (ec == std::make_error_code(std::errc::interrupted)) {
          wlen = 0;
        } else {
          execution::set_error(std::move(self.r), std::move(ec));
          return;
        }
      }

      try {
        execution::set_value(std::move(self.r), static_cast<std::size_t>(wlen));
      } catch (...) {
        execution::set_error(std::move(self.r), std::current_exception());
      }
    }

    private:
    Receiver r;
    int fd;
    offset_type offset;
    std::span<const std::byte> buf;
  };

  template<receiver_of<std::size_t> Receiver>
  class vectored_opstate
  : public operation_state_base_
  {
    public:
    template<const_buffer_sequence Buffers>
    explicit vectored_opstate(Receiver&& r, int fd, offset_type offset, Buffers&& buf)
    noexcept(std::is_nothrow_move_constructible_v<Receiver>)
    : r(std::move(r)),
      fd(fd),
      offset(offset),
      iov(vectorize_const_buffers(
              std::forward<Buffers>(buf),
              [](std::span<const std::byte> s) {
                using iovec = struct ::iovec;
                return iovec{
                  .iov_base=const_cast<std::byte*>(s.data()),
                  .iov_len=s.size()
                };
              }))
    {}

    friend auto tag_invoke([[maybe_unused]] start_t, vectored_opstate& self) noexcept -> void {
      auto wlen = ::pwritev(self.fd, self.iov.data(), self.iov.size(), self.offset);
      if (wlen == -1) {
        auto ec = std::error_code(errno, std::generic_category());
        if (ec == std::make_error_code(std::errc::interrupted)) {
          wlen = 0;
        } else {
          execution::set_error(std::move(self.r), std::move(ec));
          return;
        }
      }

      try {
        execution::set_value(std::move(self.r), static_cast<std::size_t>(wlen));
      } catch (...) {
        execution::set_error(std::move(self.r), std::current_exception());
      }
    }

    private:
    Receiver r;
    int fd;
    offset_type offset;
    std::vector<struct ::iovec> iov;
  };

  // Figure out if a given receiver has a scheduler.
  template<receiver, typename = void>
  struct has_scheduler_
  : std::false_type
  {};
  template<receiver T>
  struct has_scheduler_<T, std::void_t<decltype(execution::get_scheduler(std::declval<const T&>()))>>
  : std::true_type
  {};

  // Figure out if a given receiver has a scheduler.
  template<receiver Receiver>
  static inline constexpr bool has_scheduler = has_scheduler_<Receiver>::value;

  class sender_impl {
    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<Tuple<std::size_t>>;

    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr, std::error_code>;

    static inline constexpr bool sends_done = false;

    explicit sender_impl(int fd, offset_type offset, std::span<const std::byte> buf) noexcept
    : fd(fd),
      offset(offset),
      buf(buf)
    {}

    // Only permit move operations.
    sender_impl(sender_impl&&) = default;
    sender_impl(const sender_impl&) = delete;

    // Connect this sender with a receiver.
    //
    // If the receiver has a scheduler, we'll try to forward to an implementation of lazy_write_ec,
    // that is specific for the scheduler.
    // But if there isn't one, then we'll use our own implementation (which uses a block write).
    template<receiver_of<std::size_t> Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> operation_state decltype(auto) {
      if constexpr(has_scheduler<std::remove_cvref_t<Receiver>>) {
        if constexpr(tag_invocable<lazy_write_some_at_ec_t, decltype(execution::get_scheduler(r)), int, offset_type, std::span<const std::byte>>) {
          return execution::connect(
              execution::tag_invoke(execution::get_scheduler(r), self.fd, self.offset, std::move(self.buf)),
              std::forward<Receiver>(r));
        } else {
          // XXX we have a scheduler, and no specialized implementation
          // we should create a global-configurable implementation
          return opstate<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r), self.fd, self.offset, self.buf);
        }
      } else {
        return opstate<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r), self.fd, self.offset, self.buf);
      }
    }

    private:
    int fd;
    offset_type offset;
    std::span<const std::byte> buf;
  };

  class vectored_sender_impl {
    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<Tuple<std::size_t>>;

    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr, std::error_code>;

    static inline constexpr bool sends_done = false;

    template<const_buffer_sequence Buffers>
    explicit vectored_sender_impl(int fd, offset_type offset, Buffers&& buf) noexcept
    : fd(fd),
      offset(offset),
      buf(vectorize_const_buffers(std::forward<Buffers>(buf)))
    {}

    // Only permit move operations.
    vectored_sender_impl(vectored_sender_impl&&) = default;
    vectored_sender_impl(const vectored_sender_impl&) = delete;

    // Connect this sender with a receiver.
    //
    // If the receiver has a scheduler, we'll try to forward to an implementation of lazy_write_ec,
    // that is specific for the scheduler.
    // But if there isn't one, then we'll use our own implementation (which uses a block write).
    template<receiver_of<std::size_t> Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, vectored_sender_impl&& self, Receiver&& r)
    -> operation_state decltype(auto) {
      if constexpr(has_scheduler<std::remove_cvref_t<Receiver>>) {
        if constexpr(tag_invocable<lazy_write_some_at_ec_t, decltype(execution::get_scheduler(r)), int, offset_type, std::vector<std::span<const std::byte>>>) {
          return execution::connect(
              execution::tag_invoke(execution::get_scheduler(r), self.fd, self.offset, std::move(self.buf)),
              std::forward<Receiver>(r));
        } else {
          // XXX we have a scheduler, and no specialized implementation
          // we should create a global-configurable implementation
          return vectored_opstate<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r), self.fd, self.offset, std::move(self.buf));
        }
      } else {
        return vectored_opstate<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r), self.fd, self.offset, std::move(self.buf));
      }
    }

    private:
    int fd;
    offset_type offset;
    std::vector<std::span<const std::byte>> buf;
  };

  public:
  auto operator()(int fd, offset_type offset, std::span<const std::byte> buf) const
  noexcept
  -> sender_impl {
    return sender_impl(fd, offset, buf);
  }

  template<const_buffer_sequence Buffers>
  auto operator()(int fd, offset_type offset, Buffers&& buf) const
  noexcept
  -> vectored_sender_impl {
    return vectored_sender_impl(fd, offset, std::forward<Buffers>(buf));
  }

  template<scheduler Scheduler, typename FD, const_buffers Buffers>
  auto operator()([[maybe_unused]] Scheduler&& sch, FD&& fd, offset_type offset, Buffers&& buf) const
  noexcept(
      tag_invocable<lazy_write_some_at_ec_t, Scheduler, FD, offset_type, Buffers> ?
      nothrow_tag_invocable<lazy_write_some_at_ec_t, Scheduler, FD, offset_type, Buffers> :
      std::is_nothrow_invocable_v<lazy_write_some_at_ec_t, FD, offset_type, Buffers>)
  -> decltype(auto) {
    if constexpr(tag_invocable<lazy_write_some_at_ec_t, Scheduler, FD, offset_type, Buffers>)
      return execution::tag_invoke(*this, std::forward<Scheduler>(sch), std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf));
    else
      return execution::lazy_on(std::forward<Scheduler>(sch), (*this)(std::move(fd), std::move(offset), std::forward<Buffers>(buf)));
  }
};
inline constexpr lazy_write_some_at_ec_t lazy_write_some_at_ec{};


// Write some bytes to a file-descriptor.
//
// The default implementation only exists for `int' file-descriptor,
// and invokes the write(2) system call.
//
// If the write is interrupted (errno == EINTR), the error will be swallowed
// and zero-bytes-written will be reported instead.
//
// Errors are communicated as std::error_code.
struct lazy_write_some_ec_t {
  template<typename FD>
  requires (!std::same_as<std::remove_cvref_t<FD>, int>)
  auto operator()(FD&& fd, std::span<const std::byte> buf) const
  noexcept(nothrow_tag_invocable<lazy_write_some_ec_t, FD, std::span<const std::byte>>)
  -> typed_sender decltype(auto) {
    return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(buf));
  }

  template<typename FD, const_buffer_sequence Buffers>
  requires (!std::same_as<std::remove_cvref_t<FD>, int>)
  auto operator()(FD&& fd, Buffers&& buf) const
  noexcept(
      tag_invocable<lazy_write_some_ec_t, FD, Buffers> ?
      nothrow_tag_invocable<lazy_write_some_ec_t, FD, Buffers> :
      std::is_nothrow_invocable_v<lazy_write_some_ec_t, FD, std::span<std::byte>>)
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_write_some_ec_t, FD, Buffers>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::forward<Buffers>(buf));
    } else {
      std::span<const std::byte> s;
      if (!std::ranges::empty(buf)) s = *std::ranges::begin(buf);
      return (*this)(std::forward<FD>(fd), std::move(s));
    }
  }

  private:
  template<receiver_of<std::size_t> Receiver>
  class opstate
  : public operation_state_base_
  {
    public:
    explicit opstate(Receiver&& r, int fd, std::span<const std::byte> buf)
    noexcept(std::is_nothrow_move_constructible_v<Receiver>)
    : r(std::move(r)),
      fd(fd),
      buf(buf)
    {}

    friend auto tag_invoke([[maybe_unused]] start_t, opstate& self) noexcept -> void {
      auto wlen = ::write(self.fd, self.buf.data(), self.buf.size());
      if (wlen == -1) {
        auto ec = std::error_code(errno, std::generic_category());
        if (ec == std::make_error_code(std::errc::interrupted)) {
          wlen = 0;
        } else {
          execution::set_error(std::move(self.r), std::move(ec));
          return;
        }
      }

      try {
        execution::set_value(std::move(self.r), static_cast<std::size_t>(wlen));
      } catch (...) {
        execution::set_error(std::move(self.r), std::current_exception());
      }
    }

    private:
    Receiver r;
    int fd;
    std::span<const std::byte> buf;
  };

  template<receiver_of<std::size_t> Receiver>
  class vectored_opstate
  : public operation_state_base_
  {
    public:
    template<const_buffer_sequence Buffers>
    explicit vectored_opstate(Receiver&& r, int fd, Buffers&& buf)
    noexcept(std::is_nothrow_move_constructible_v<Receiver>)
    : r(std::move(r)),
      fd(fd),
      iov(vectorize_const_buffers(
              std::forward<Buffers>(buf),
              [](std::span<const std::byte> s) {
                using iovec = struct ::iovec;
                return iovec{
                  .iov_base=const_cast<std::byte*>(s.data()),
                  .iov_len=s.size()
                };
              }))
    {}

    friend auto tag_invoke([[maybe_unused]] start_t, vectored_opstate& self) noexcept -> void {
      auto wlen = ::writev(self.fd, self.iov.data(), self.iov.size());
      if (wlen == -1) {
        auto ec = std::error_code(errno, std::generic_category());
        if (ec == std::make_error_code(std::errc::interrupted)) {
          wlen = 0;
        } else {
          execution::set_error(std::move(self.r), std::move(ec));
          return;
        }
      }

      try {
        execution::set_value(std::move(self.r), static_cast<std::size_t>(wlen));
      } catch (...) {
        execution::set_error(std::move(self.r), std::current_exception());
      }
    }

    private:
    Receiver r;
    int fd;
    std::vector<struct ::iovec> iov;
  };

  // Figure out if a given receiver has a scheduler.
  template<receiver, typename = void>
  struct has_scheduler_
  : std::false_type
  {};
  template<receiver T>
  struct has_scheduler_<T, std::void_t<decltype(execution::get_scheduler(std::declval<const T&>()))>>
  : std::true_type
  {};

  // Figure out if a given receiver has a scheduler.
  template<receiver Receiver>
  static inline constexpr bool has_scheduler = has_scheduler_<Receiver>::value;

  class sender_impl {
    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<Tuple<std::size_t>>;

    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr, std::error_code>;

    static inline constexpr bool sends_done = false;

    explicit sender_impl(int fd, std::span<const std::byte> buf) noexcept
    : fd(fd),
      buf(buf)
    {}

    // Only permit move operations.
    sender_impl(sender_impl&&) = default;
    sender_impl(const sender_impl&) = delete;

    // Connect this sender with a receiver.
    //
    // If the receiver has a scheduler, we'll try to forward to an implementation of lazy_write_ec,
    // that is specific for the scheduler.
    // But if there isn't one, then we'll use our own implementation (which uses a block write).
    template<receiver_of<std::size_t> Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    -> operation_state decltype(auto) {
      if constexpr(has_scheduler<std::remove_cvref_t<Receiver>>) {
        if constexpr(tag_invocable<lazy_write_some_ec_t, decltype(execution::get_scheduler(r)), int, std::span<const std::byte>>) {
          return execution::connect(
              execution::tag_invoke(execution::get_scheduler(r), self.fd, std::move(self.buf)),
              std::forward<Receiver>(r));
        } else {
          // XXX we have a scheduler, and no specialized implementation
          // we should create a global-configurable implementation
          return opstate<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r), self.fd, self.buf);
        }
      } else {
        return opstate<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r), self.fd, self.buf);
      }
    }

    private:
    int fd;
    std::span<const std::byte> buf;
  };

  class vectored_sender_impl {
    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<Tuple<std::size_t>>;

    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr, std::error_code>;

    static inline constexpr bool sends_done = false;

    template<const_buffer_sequence Buffers>
    explicit vectored_sender_impl(int fd, Buffers&& buf) noexcept
    : fd(fd),
      buf(vectorize_const_buffers(std::forward<Buffers>(buf)))
    {}

    // Only permit move operations.
    vectored_sender_impl(vectored_sender_impl&&) = default;
    vectored_sender_impl(const vectored_sender_impl&) = delete;

    // Connect this sender with a receiver.
    //
    // If the receiver has a scheduler, we'll try to forward to an implementation of lazy_write_ec,
    // that is specific for the scheduler.
    // But if there isn't one, then we'll use our own implementation (which uses a block write).
    template<receiver_of<std::size_t> Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, vectored_sender_impl&& self, Receiver&& r)
    -> operation_state decltype(auto) {
      if constexpr(has_scheduler<std::remove_cvref_t<Receiver>>) {
        if constexpr(tag_invocable<lazy_write_some_ec_t, decltype(execution::get_scheduler(r)), int, std::vector<std::span<const std::byte>>>) {
          return execution::connect(
              execution::tag_invoke(execution::get_scheduler(r), self.fd, std::move(self.buf)),
              std::forward<Receiver>(r));
        } else {
          // XXX we have a scheduler, and no specialized implementation
          // we should create a global-configurable implementation
          return vectored_opstate<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r), self.fd, std::move(self.buf));
        }
      } else {
        return vectored_opstate<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r), self.fd, std::move(self.buf));
      }
    }

    private:
    int fd;
    std::vector<std::span<const std::byte>> buf;
  };

  public:
  auto operator()(int fd, std::span<const std::byte> buf) const
  noexcept
  -> sender_impl {
    return sender_impl(fd, buf);
  }

  template<const_buffer_sequence Buffers>
  auto operator()(int fd, Buffers&& buf) const
  noexcept
  -> vectored_sender_impl {
    return vectored_sender_impl(fd, buf);
  }

  template<scheduler Scheduler, typename FD, const_buffers Buffers>
  auto operator()([[maybe_unused]] Scheduler&& sch, FD&& fd, Buffers&& buf) const
  noexcept(
      tag_invocable<lazy_write_some_ec_t, Scheduler, FD, Buffers> ?
      nothrow_tag_invocable<lazy_write_some_ec_t, Scheduler, FD, Buffers> :
      std::is_nothrow_invocable_v<lazy_write_some_ec_t, FD, Buffers>)
  -> decltype(auto) {
    if constexpr(tag_invocable<lazy_write_some_ec_t, Scheduler, FD, Buffers>)
      return execution::tag_invoke(*this, std::forward<Scheduler>(sch), std::forward<FD>(fd), std::forward<Buffers>(buf));
    else
      return execution::lazy_on(std::forward<Scheduler>(sch), (*this)(std::move(fd), std::forward<Buffers>(buf)));
  }
};
inline constexpr lazy_write_some_ec_t lazy_write_some_ec{};


// Read some bytes from a file-descriptor, at a specific offset.
//
// The default implementation only exists for `int' file-descriptor,
// and invokes the read(2) system call.
//
// If the read is interrupted (errno == EINTR), the error will be swallowed
// and zero-bytes-written will be reported instead.
//
// Errors are communicated as std::system_error.
struct lazy_read_some_at_t {
  template<typename FD>
  requires (!std::same_as<std::remove_cvref_t<FD>, int>)
  auto operator()(FD&& fd, offset_type offset, std::span<std::byte> buf) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_read_some_at_t, FD, offset_type, std::span<std::byte>>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(offset), std::move(buf));
    } else {
      return lazy_read_some_at_ec(std::forward<FD>(fd), std::move(offset), std::move(buf))
      | ec_to_exception();
    }
  }

  template<typename FD, mutable_buffer_sequence Buffers>
  requires (!std::same_as<std::remove_cvref_t<FD>, int>)
  auto operator()(FD&& fd, offset_type offset, Buffers&& buf) const
  noexcept(
      tag_invocable<lazy_read_some_at_t, FD, offset_type, Buffers> ?
      nothrow_tag_invocable<lazy_read_some_at_t, FD, offset_type, Buffers> :
      noexcept(
          lazy_read_some_at_ec(std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf))
          | ec_to_exception()))
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_read_some_at_t, FD, offset_type, Buffers>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf));
    } else if constexpr(tag_invocable<lazy_read_some_at_t, FD, offset_type, std::span<std::byte>>) {
      std::span<std::byte> s;
      if (!std::ranges::empty(buf)) s = *std::ranges::begin(buf);
      return (*this)(std::forward<FD>(fd), offset, std::move(s));
    } else {
      return lazy_read_some_at_ec(std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf))
      | ec_to_exception();
    }
  }

  template<mutable_buffers Buffers>
  auto operator()(int fd, offset_type offset, Buffers&& buf) const
  noexcept(noexcept(
          lazy_read_some_at_ec(fd, offset, std::forward<Buffers>(buf))
          | ec_to_exception()))
  -> typed_sender decltype(auto) {
    return lazy_read_some_at_ec(fd, offset, std::forward<Buffers>(buf))
    | ec_to_exception();
  }

  template<scheduler Scheduler, typename FD, mutable_buffers Buffers>
  auto operator()([[maybe_unused]] Scheduler&& sch, FD&& fd, offset_type offset, Buffers&& buf) const
  noexcept(
      tag_invocable<lazy_read_some_at_t, Scheduler, FD, offset_type, Buffers> ?
      nothrow_tag_invocable<lazy_read_some_at_t, Scheduler, FD, offset_type, Buffers> :
      std::is_nothrow_invocable_v<lazy_read_some_at_t, FD, offset_type, Buffers>)
  -> decltype(auto) {
    if constexpr(tag_invocable<lazy_read_some_at_t, Scheduler, FD, offset_type, Buffers>)
      return execution::tag_invoke(*this, std::forward<Scheduler>(sch), std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf));
    else
      return execution::lazy_on(std::forward<Scheduler>(sch), (*this)(std::move(fd), std::move(offset), std::forward<Buffers>(buf)));
  }
};
inline constexpr lazy_read_some_at_t lazy_read_some_at{};


// Read some bytes from a file-descriptor.
//
// The default implementation only exists for `int' file-descriptor,
// and invokes the read(2) system call.
//
// If the read is interrupted (errno == EINTR), the error will be swallowed
// and zero-bytes-written will be reported instead.
//
// Errors are communicated as std::system_error.
struct lazy_read_some_t {
  template<typename FD>
  requires (!std::same_as<std::remove_cvref_t<FD>, int>)
  auto operator()(FD&& fd, std::span<std::byte> buf) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_read_some_t, FD, std::span<std::byte>>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(buf));
    } else {
      return lazy_read_some_ec(std::forward<FD>(fd), std::move(buf))
      | ec_to_exception();
    }
  }

  template<typename FD, mutable_buffer_sequence Buffers>
  requires (!std::same_as<std::remove_cvref_t<FD>, int>)
  auto operator()(FD&& fd, Buffers&& buf) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_read_some_t, FD, Buffers>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::forward<Buffers>(buf));
    } else if constexpr(tag_invocable<lazy_read_some_t, FD, std::span<std::byte>>) {
      std::span<std::byte> s;
      if (!std::ranges::empty(buf)) s = *std::ranges::begin(buf);
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(s));
    } else {
      return lazy_read_some_ec(std::forward<FD>(fd), std::forward<Buffers>(buf))
      | ec_to_exception();
    }
  }

  template<mutable_buffers Buffers>
  auto operator()(int fd, Buffers&& buf) const
  noexcept(noexcept(
          lazy_read_some_ec(fd, std::forward<Buffers>(buf))
          | ec_to_exception()))
  -> typed_sender decltype(auto) {
    return lazy_read_some_ec(fd, std::forward<Buffers>(buf))
    | ec_to_exception();
  }

  template<scheduler Scheduler, typename FD, mutable_buffers Buffers>
  auto operator()([[maybe_unused]] Scheduler&& sch, FD&& fd, Buffers&& buf) const
  noexcept(
      tag_invocable<lazy_read_some_t, Scheduler, FD, Buffers> ?
      nothrow_tag_invocable<lazy_read_some_t, Scheduler, FD, Buffers> :
      std::is_nothrow_invocable_v<lazy_read_some_t, FD, Buffers>)
  -> decltype(auto) {
    if constexpr(tag_invocable<lazy_read_some_t, Scheduler, FD, Buffers>)
      return execution::tag_invoke(*this, std::forward<Scheduler>(sch), std::forward<FD>(fd), std::forward<Buffers>(buf));
    else
      return execution::lazy_on(std::forward<Scheduler>(sch), (*this)(std::move(fd), std::forward<Buffers>(buf)));
  }
};
inline constexpr lazy_read_some_t lazy_read_some{};


// Write some bytes to a file-descriptor, at a specific offset.
//
// The default implementation only exists for `int' file-descriptor,
// and invokes the write(2) system call.
//
// If the write is interrupted (errno == EINTR), the error will be swallowed
// and zero-bytes-written will be reported instead.
//
// Errors are communicated as std::system_error.
struct lazy_write_some_at_t {
  template<typename FD>
  requires (!std::same_as<std::remove_cvref_t<FD>, int>)
  auto operator()(FD&& fd, offset_type offset, std::span<const std::byte> buf) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_write_some_at_t, FD, offset_type, std::span<const std::byte>>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), offset, std::move(buf));
    } else {
      return lazy_write_some_at_ec(std::forward<FD>(fd), offset, std::move(buf))
      | ec_to_exception();
    }
  }

  template<typename FD, const_buffer_sequence Buffers>
  requires (!std::same_as<std::remove_cvref_t<FD>, int>)
  auto operator()(FD&& fd, offset_type offset, Buffers&& buf) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_write_some_at_t, FD, offset_type, Buffers>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), offset, std::forward<Buffers>(buf));
    } else if constexpr(tag_invocable<lazy_write_some_at_t, FD, offset_type, std::span<const std::byte>>) {
      std::span<const std::byte> s;
      if (!std::ranges::empty(buf)) s = *std::ranges::begin(buf);
      return (*this)(std::forward<FD>(fd), offset, std::move(s));
    } else {
      return lazy_write_some_at_ec(std::forward<FD>(fd), offset, std::forward<Buffers>(buf))
      | ec_to_exception();
    }
  }

  template<const_buffers Buffers>
  auto operator()(int fd, offset_type offset, Buffers&& buf) const
  noexcept(noexcept(
          lazy_write_some_at_ec(fd, offset, std::forward<Buffers>(buf))
          | ec_to_exception()))
  -> typed_sender decltype(auto) {
    return lazy_write_some_at_ec(fd, offset, std::forward<Buffers>(buf))
    | ec_to_exception();
  }

  template<scheduler Scheduler, typename FD, const_buffers Buffers>
  auto operator()([[maybe_unused]] Scheduler&& sch, FD&& fd, offset_type offset, Buffers&& buf) const
  noexcept(
      tag_invocable<lazy_write_some_at_t, Scheduler, FD, offset_type, Buffers> ?
      nothrow_tag_invocable<lazy_write_some_at_t, Scheduler, FD, offset_type, Buffers> :
      std::is_nothrow_invocable_v<lazy_write_some_at_t, FD, offset_type, Buffers>)
  -> decltype(auto) {
    if constexpr(tag_invocable<lazy_write_some_at_t, Scheduler, FD, offset_type, Buffers>)
      return execution::tag_invoke(*this, std::forward<Scheduler>(sch), std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf));
    else
      return execution::lazy_on(std::forward<Scheduler>(sch), (*this)(std::move(fd), std::move(offset), std::forward<Buffers>(buf)));
  }
};
inline constexpr lazy_write_some_at_t lazy_write_some_at{};


// Write some bytes to a file-descriptor.
//
// The default implementation only exists for `int' file-descriptor,
// and invokes the write(2) system call.
//
// If the write is interrupted (errno == EINTR), the error will be swallowed
// and zero-bytes-written will be reported instead.
//
// Errors are communicated as std::system_error.
struct lazy_write_some_t {
  template<typename FD>
  requires (!std::same_as<std::remove_cvref_t<FD>, int>)
  auto operator()(FD&& fd, std::span<const std::byte> buf) const
  noexcept(
      tag_invocable<lazy_write_some_t, FD, std::span<const std::byte>> ?
      nothrow_tag_invocable<lazy_write_some_t, FD, std::span<const std::byte>> :
      noexcept(
          lazy_write_some_ec(std::forward<FD>(fd), std::move(buf))
          | ec_to_exception()))
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_write_some_t, FD, std::span<const std::byte>>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(buf));
    } else {
      return lazy_write_some_ec(std::forward<FD>(fd), std::move(buf))
      | ec_to_exception();
    }
  }

  template<typename FD, const_buffer_sequence Buffers>
  requires (!std::same_as<std::remove_cvref_t<FD>, int>)
  auto operator()(FD&& fd, Buffers&& buf) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_write_some_t, FD, Buffers>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(buf));
    } else if constexpr(tag_invocable<lazy_write_some_t, FD, std::span<const std::byte>>) {
      std::span<const std::byte> s;
      if (!std::ranges::empty(buf)) s = *std::ranges::begin(buf);
      return (*this)(std::forward<FD>(fd), std::move(s));
    } else {
      return lazy_write_some_ec(std::forward<FD>(fd), std::move(buf))
      | ec_to_exception();
    }
  }

  template<const_buffers Buffers>
  auto operator()(int fd, Buffers&& buf) const
  noexcept(noexcept(
          lazy_write_some_ec(fd, std::forward<Buffers>(buf))
          | ec_to_exception()))
  -> typed_sender decltype(auto) {
    return lazy_write_some_ec(fd, std::forward<Buffers>(buf))
    | ec_to_exception();
  }

  template<scheduler Scheduler, typename FD, const_buffers Buffers>
  auto operator()([[maybe_unused]] Scheduler&& sch, FD&& fd, Buffers&& buf) const
  noexcept(
      tag_invocable<lazy_write_some_t, Scheduler, FD, Buffers> ?
      nothrow_tag_invocable<lazy_write_some_t, Scheduler, FD, Buffers> :
      std::is_nothrow_invocable_v<lazy_write_some_t, FD, Buffers>)
  -> decltype(auto) {
    if constexpr(tag_invocable<lazy_write_some_t, Scheduler, FD, Buffers>)
      return execution::tag_invoke(*this, std::forward<Scheduler>(sch), std::forward<FD>(fd), std::forward<Buffers>(buf));
    else
      return execution::lazy_on(std::forward<Scheduler>(sch), (*this)(std::move(fd), std::forward<Buffers>(buf)));
  }
};
inline constexpr lazy_write_some_t lazy_write_some{};


// Read bytes from a file-descriptor, at a specific offset.
//
// Usage:
// `lazy_read_at_ec(file_descriptor, offset, std::span<std::byte>(...))' to read all bytes of the span.
// `lazy_read_at_ec(file_descriptor, offset, std::span<std::byte>(...), 100)' to read at least the first 100 bytes of the span.
//
// The default implementation repeatedly uses `lazy_read_some_at_ec' until sufficient data has been read.
struct lazy_read_at_ec_t {
  template<typename FD, mutable_buffers Buffers>
  auto operator()(FD&& fd, offset_type offset, Buffers&& buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_read_at_ec_t, FD, offset_type, Buffers, std::optional<std::size_t>>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf), std::move(minbytes));
    } else {
      return just(make_readwrite_state_<true>(std::forward<FD>(fd), offset, std::forward<Buffers>(buf), minbytes))
      | lazy_repeat(
          []([[maybe_unused]] std::size_t idx, auto& st) {
            auto generator = [&]() {
              return std::make_optional(
                  lazy_read_some_at_ec(st.fd, st.offset, st.buf)
                  | lazy_then(st.make_updater()));
            };
            using opt_sender = std::invoke_result_t<decltype(generator)>;

            if (st.buf.empty() || st.minbytes <= 0)
              return opt_sender(std::nullopt);
            else
              return generator();
          })
      | lazy_then(
          [](auto st) noexcept {
            return st.operation_count;
          });
    }
  }
};
inline constexpr lazy_read_at_ec_t lazy_read_at_ec{};


// Read bytes from a file-descriptor.
//
// Usage:
// `lazy_read_ec(file_descriptor, std::span<std::byte>(...))' to read all bytes of the span.
// `lazy_read_ec(file_descriptor, std::span<std::byte>(...), 100)' to read at least the first 100 bytes of the span.
//
// The default implementation repeatedly uses `lazy_read_some_ec' until sufficient data has been read.
struct lazy_read_ec_t {
  template<typename FD, mutable_buffers Buffers>
  auto operator()(FD&& fd, Buffers buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_read_ec_t, FD, Buffers, std::optional<std::size_t>>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::forward<Buffers>(buf), std::move(minbytes));
    } else {
      return just(make_readwrite_state_<true>(std::forward<FD>(fd), std::forward<Buffers>(buf), minbytes))
      | lazy_repeat(
          []([[maybe_unused]] std::size_t idx, auto& st) {
            auto generator = [&]() {
              return std::make_optional(
                  lazy_read_some_ec(st.fd, st.buf)
                  | lazy_then(st.make_updater()));
            };
            using opt_sender = std::invoke_result_t<decltype(generator)>;

            if (st.buf.empty() || st.minbytes <= 0)
              return opt_sender(std::nullopt);
            else
              return generator();
          })
      | lazy_then(
          [](auto st) noexcept {
            return st.operation_count;
          });
    }
  }
};
inline constexpr lazy_read_ec_t lazy_read_ec{};


// Write bytes to a file-descriptor, at a specific offset.
//
// Usage:
// `lazy_write_at_ec(file_descriptor, offset, std::span<const std::byte>(...))' to write all bytes of the span.
// `lazy_write_at_ec(file_descriptor, offset, std::span<const std::byte>(...), 100)' to write at least the first 100 bytes of the span.
//
// The default implementation repeatedly uses `lazy_write_some_ec' until sufficient data has been written.
struct lazy_write_at_ec_t {
  template<typename FD, const_buffers Buffers>
  auto operator()(FD&& fd, offset_type offset, Buffers&& buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_write_at_ec_t, FD, offset_type, Buffers, std::optional<std::size_t>>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf), std::move(minbytes));
    } else {
      return just(make_readwrite_state_<false>(std::forward<FD>(fd), offset, std::forward<Buffers>(buf), minbytes))
      | lazy_repeat(
          []([[maybe_unused]] std::size_t idx, auto& st) {
            auto generator = [&]() {
              return std::make_optional(
                  lazy_write_some_at_ec(st.fd, st.offset, st.buf)
                  | lazy_then(st.make_updater()));
            };
            using opt_sender = std::invoke_result_t<decltype(generator)>;

            if (st.buf.empty() || st.minbytes <= 0)
              return opt_sender(std::nullopt);
            else
              return generator();
          })
      | lazy_then(
          [](auto st) noexcept {
            return st.operation_count;
          });
    }
  }
};
inline constexpr lazy_write_at_ec_t lazy_write_at_ec{};


// Write bytes to a file-descriptor.
//
// Usage:
// `lazy_write_ec(file_descriptor, std::span<const std::byte>(...))' to write all bytes of the span.
// `lazy_write_ec(file_descriptor, std::span<const std::byte>(...), 100)' to write at least the first 100 bytes of the span.
//
// The default implementation repeatedly uses `lazy_write_some_ec' until sufficient data has been written.
struct lazy_write_ec_t {
  template<typename FD, const_buffers Buffers>
  auto operator()(FD&& fd, Buffers&& buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_write_ec_t, FD, Buffers, std::optional<std::size_t>>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::forward<Buffers>(buf), std::move(minbytes));
    } else {
      return just(make_readwrite_state_<false>(std::forward<FD>(fd), std::forward<Buffers>(buf), minbytes))
      | lazy_repeat(
          []([[maybe_unused]] std::size_t idx, auto& st) {
            auto generator = [&]() {
              return std::make_optional(
                  lazy_write_some_ec(st.fd, st.buf)
                  | lazy_then(st.make_updater()));
            };
            using opt_sender = std::invoke_result_t<decltype(generator)>;

            if (st.buf.empty() || st.minbytes <= 0)
              return opt_sender(std::nullopt);
            else
              return generator();
          })
      | lazy_then(
          [](auto st) noexcept {
            return st.operation_count;
          });
    }
  }
};
inline constexpr lazy_write_ec_t lazy_write_ec{};


// Read bytes from a file-descriptor, at a specific offset.
//
// Usage:
// `lazy_read_at(file_descriptor, offset, std::span<std::byte>(...))' to read all bytes of the span.
// `lazy_read_at(file_descriptor, offset, std::span<std::byte>(...), 100)' to read at least the first 100 bytes of the span.
//
// The default implementation repeatedly uses `lazy_read_some_at' until sufficient data has been read.
struct lazy_read_at_t {
  template<typename FD, mutable_buffers Buffers>
  auto operator()(FD&& fd, offset_type offset, Buffers&& buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_read_at_t, FD, offset_type, Buffers, std::optional<std::size_t>>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf), std::move(minbytes));
    } else {
      return just(make_readwrite_state_<true>(std::forward<FD>(fd), offset, std::forward<Buffers>(buf), minbytes))
      | lazy_repeat(
          []([[maybe_unused]] std::size_t idx, auto& st) {
            auto generator = [&]() {
              return std::make_optional(
                  lazy_read_some_at(st.fd, st.offset, st.buf)
                  | lazy_then(st.make_updater()));
            };
            using opt_sender = std::invoke_result_t<decltype(generator)>;

            if (st.buf.empty() || st.minbytes <= 0)
              return opt_sender(std::nullopt);
            else
              return generator();
          })
      | lazy_then(
          [](auto st) noexcept {
            return st.operation_count;
          });
    }
  }
};
inline constexpr lazy_read_at_t lazy_read_at{};


// Read bytes from a file-descriptor.
//
// Usage:
// `lazy_read(file_descriptor, std::span<std::byte>(...))' to read all bytes of the span.
// `lazy_read(file_descriptor, std::span<std::byte>(...), 100)' to read at least the first 100 bytes of the span.
//
// The default implementation repeatedly uses `lazy_read_some' until sufficient data has been read.
struct lazy_read_t {
  template<typename FD, mutable_buffers Buffers>
  auto operator()(FD&& fd, Buffers&& buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_read_t, FD, Buffers, std::optional<std::size_t>>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::forward<Buffers>(buf), std::move(minbytes));
    } else {
      return just(make_readwrite_state_<true>(std::forward<FD>(fd), std::forward<Buffers>(buf), minbytes))
      | lazy_repeat(
          []([[maybe_unused]] std::size_t idx, auto& st) {
            auto generator = [&]() {
              return std::make_optional(
                  lazy_read_some(st.fd, st.buf)
                  | lazy_then(st.make_updater()));
            };
            using opt_sender = std::invoke_result_t<decltype(generator)>;

            if (st.buf.empty() || st.minbytes <= 0)
              return opt_sender(std::nullopt);
            else
              return generator();
          })
      | lazy_then(
          [](auto st) noexcept {
            return st.operation_count;
          });
    }
  }
};
inline constexpr lazy_read_t lazy_read{};


// Write bytes to a file-descriptor, at a specific offset.
//
// Usage:
// `lazy_write(file_descriptor, std::span<const std::byte>(...))' to write all bytes of the span.
// `lazy_write(file_descriptor, std::span<const std::byte>(...), 100)' to write at least the first 100 bytes of the span.
//
// The default implementation repeatedly uses `lazy_write_some' until sufficient data has been written.
struct lazy_write_at_t {
  template<typename FD, const_buffers Buffers>
  auto operator()(FD&& fd, offset_type offset, Buffers&& buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_write_at_t, FD, offset_type, Buffers, std::optional<std::size_t>>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf), std::move(minbytes));
    } else {
      return just(make_readwrite_state_<false>(std::forward<FD>(fd), offset, std::forward<Buffers>(buf), minbytes))
      | lazy_repeat(
          []([[maybe_unused]] std::size_t idx, auto& st) {
            auto generator = [&]() {
              return std::make_optional(
                  lazy_write_some_at(st.fd, st.offset, st.buf)
                  | lazy_then(st.make_updater()));
            };
            using opt_sender = std::invoke_result_t<decltype(generator)>;

            if (st.buf.empty() || st.minbytes <= 0)
              return opt_sender(std::nullopt);
            else
              return generator();
          })
      | lazy_then(
          [](auto st) noexcept {
            return st.operation_count;
          });
    }
  }
};
inline constexpr lazy_write_at_t lazy_write_at{};


// Write bytes to a file-descriptor.
//
// Usage:
// `lazy_write(file_descriptor, std::span<const std::byte>(...))' to write all bytes of the span.
// `lazy_write(file_descriptor, std::span<const std::byte>(...), 100)' to write at least the first 100 bytes of the span.
//
// The default implementation repeatedly uses `lazy_write_some' until sufficient data has been written.
struct lazy_write_t {
  template<typename FD, const_buffers Buffers>
  auto operator()(FD&& fd, Buffers&& buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_write_t, FD, Buffers, std::optional<std::size_t>>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::forward<Buffers>(buf), std::move(minbytes));
    } else {
      return just(make_readwrite_state_<false>(std::forward<FD>(fd), std::forward<Buffers>(buf), minbytes))
      | lazy_repeat(
          []([[maybe_unused]] std::size_t idx, auto& st) {
            auto generator = [&]() {
              return std::make_optional(
                  lazy_write_some(st.fd, st.buf)
                  | lazy_then(st.make_updater()));
            };
            using opt_sender = std::invoke_result_t<decltype(generator)>;

            if (st.buf.empty() || st.minbytes <= 0)
              return opt_sender(std::nullopt);
            else
              return generator();
          })
      | lazy_then(
          [](auto st) noexcept {
            return st.operation_count;
          });
    }
  }
};
inline constexpr lazy_write_t lazy_write{};


struct read_at_ec_t {
  template<typename FD, mutable_buffers Buffers>
  auto operator()(FD&& fd, offset_type offset, Buffers&& buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<read_at_ec_t, FD, offset_type, Buffers, std::optional<std::size_t>>)
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf), std::move(minbytes));
    else
      return lazy_read_at_ec(std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf), std::move(minbytes));
  }
};
inline constexpr read_at_ec_t read_at_ec{};


struct read_ec_t {
  template<typename FD, mutable_buffers Buffers>
  auto operator()(FD&& fd, Buffers&& buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<read_ec_t, FD, Buffers, std::optional<std::size_t>>)
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::forward<Buffers>(buf), std::move(minbytes));
    else
      return lazy_read_ec(std::forward<FD>(fd), std::forward<Buffers>(buf), std::move(minbytes));
  }
};
inline constexpr read_ec_t read_ec{};


struct write_at_ec_t {
  template<typename FD, const_buffers Buffers>
  auto operator()(FD&& fd, offset_type offset, Buffers&& buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<write_at_ec_t, FD, offset_type, Buffers, std::optional<std::size_t>>)
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf), std::move(minbytes));
    else
      return lazy_write_at_ec(std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf), std::move(minbytes));
  }
};
inline constexpr write_at_ec_t write_at_ec{};


struct write_ec_t {
  template<typename FD, const_buffers Buffers>
  auto operator()(FD&& fd, Buffers&& buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<write_ec_t, FD, Buffers, std::optional<std::size_t>>)
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::forward<Buffers>(buf), std::move(minbytes));
    else
      return lazy_write_ec(std::forward<FD>(fd), std::forward<Buffers>(buf), std::move(minbytes));
  }
};
inline constexpr write_ec_t write_ec{};


struct read_at_t {
  template<typename FD, mutable_buffers Buffers>
  auto operator()(FD&& fd, offset_type offset, Buffers&& buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<read_at_t, FD, offset_type, Buffers, std::optional<std::size_t>>)
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf), std::move(minbytes));
    else
      return lazy_read_at(std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf), std::move(minbytes));
  }
};
inline constexpr read_at_t read_at{};


struct read_t {
  template<typename FD, mutable_buffers Buffers>
  auto operator()(FD&& fd, Buffers&& buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<read_t, FD, Buffers, std::optional<std::size_t>>)
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::forward<Buffers>(buf), std::move(minbytes));
    else
      return lazy_read(std::forward<FD>(fd), std::forward<Buffers>(buf), std::move(minbytes));
  }
};
inline constexpr read_t read{};


struct write_at_t {
  template<typename FD, const_buffers Buffers>
  auto operator()(FD&& fd, offset_type offset, Buffers&& buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<write_at_t, FD, offset_type, Buffers, std::optional<std::size_t>>)
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf), std::move(minbytes));
    else
      return lazy_write_at(std::forward<FD>(fd), std::move(offset), std::forward<Buffers>(buf), std::move(minbytes));
  }
};
inline constexpr write_at_t write_at{};


struct write_t {
  template<typename FD, const_buffers Buffers>
  auto operator()(FD&& fd, Buffers&& buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<write_t, FD, Buffers, std::optional<std::size_t>>)
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::forward<Buffers>(buf), std::move(minbytes));
    else
      return lazy_write(std::forward<FD>(fd), std::forward<Buffers>(buf), std::move(minbytes));
  }
};
inline constexpr write_t write{};


struct read_some_at_ec_t {
  template<typename FD>
  auto operator()(FD&& fd, offset_type offset, std::span<std::byte> buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<read_some_at_ec_t, FD, offset_type, std::span<std::byte>, std::optional<std::size_t>>)
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(offset), std::move(buf), std::move(minbytes));
    else
      return lazy_read_some_at_ec(std::forward<FD>(fd), std::move(offset), std::move(buf), std::move(minbytes));
  }
};
inline constexpr read_some_at_ec_t read_some_at_ec{};


struct read_some_ec_t {
  template<typename FD>
  auto operator()(FD&& fd, std::span<std::byte> buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<read_some_ec_t, FD, std::span<std::byte>, std::optional<std::size_t>>)
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(buf), std::move(minbytes));
    else
      return lazy_read_some_ec(std::forward<FD>(fd), std::move(buf), std::move(minbytes));
  }
};
inline constexpr read_some_ec_t read_some_ec{};


struct write_some_at_ec_t {
  template<typename FD>
  auto operator()(FD&& fd, offset_type offset, std::span<const std::byte> buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<write_some_at_ec_t, FD, offset_type, std::span<const std::byte>, std::optional<std::size_t>>)
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(offset), std::move(buf), std::move(minbytes));
    else
      return lazy_write_some_at_ec(std::forward<FD>(fd), std::move(offset), std::move(buf), std::move(minbytes));
  }
};
inline constexpr write_some_at_ec_t write_some_at_ec{};


struct write_some_ec_t {
  template<typename FD>
  auto operator()(FD&& fd, std::span<const std::byte> buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<write_some_ec_t, FD, std::span<const std::byte>, std::optional<std::size_t>>)
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(buf), std::move(minbytes));
    else
      return lazy_write_some_ec(std::forward<FD>(fd), std::move(buf), std::move(minbytes));
  }
};
inline constexpr write_some_ec_t write_some_ec{};


struct read_some_at_t {
  template<typename FD>
  auto operator()(FD&& fd, offset_type offset, std::span<std::byte> buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<read_some_at_t, FD, offset_type, std::span<std::byte>, std::optional<std::size_t>>)
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(offset), std::move(buf), std::move(minbytes));
    else
      return lazy_read_some_at(std::forward<FD>(fd), std::move(offset), std::move(buf), std::move(minbytes));
  }
};
inline constexpr read_some_at_t read_some_at{};


struct read_some_t {
  template<typename FD>
  auto operator()(FD&& fd, std::span<std::byte> buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<read_some_t, FD, std::span<std::byte>, std::optional<std::size_t>>)
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(buf), std::move(minbytes));
    else
      return lazy_read_some(std::forward<FD>(fd), std::move(buf), std::move(minbytes));
  }
};
inline constexpr read_some_t read_some{};


struct write_some_at_t {
  template<typename FD>
  auto operator()(FD&& fd, offset_type offset, std::span<const std::byte> buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<write_some_at_t, FD, offset_type, std::span<const std::byte>, std::optional<std::size_t>>)
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(offset), std::move(buf), std::move(minbytes));
    else
      return lazy_write_some_at(std::forward<FD>(fd), std::move(offset), std::move(buf), std::move(minbytes));
  }
};
inline constexpr write_some_at_t write_some_at{};


struct write_some_t {
  template<typename FD>
  auto operator()(FD&& fd, std::span<const std::byte> buf, std::optional<std::size_t> minbytes = std::nullopt) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<write_some_t, FD, std::span<const std::byte>, std::optional<std::size_t>>)
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(buf), std::move(minbytes));
    else
      return lazy_write_some(std::forward<FD>(fd), std::move(buf), std::move(minbytes));
  }
};
inline constexpr write_some_t write_some{};


struct lazy_truncate_ec_t {
  template<typename FD>
  requires (!std::same_as<std::remove_cvref_t<FD>, int>)
  auto operator()(FD&& fd, offset_type len) const
  -> typed_sender decltype(auto) {
    return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(len));
  }

  private:
  template<receiver_of<> Receiver>
  class opstate
  : public operation_state_base_
  {
    public:
    explicit opstate(int fd, offset_type len, Receiver&& r)
    noexcept(std::is_nothrow_move_constructible_v<Receiver>)
    : fd(fd),
      len(len),
      r(std::move(r))
    {}

    friend auto tag_invoke([[maybe_unused]] start_t, opstate& self) noexcept -> void {
      if (::ftruncate(self.fd, self.len) == -1) {
        execution::set_error(std::move(self.r), std::error_code(errno, std::generic_category()));
      } else {
        try {
          execution::set_value(std::move(self.r));
        } catch (...) {
          execution::set_error(std::move(self.r), std::current_exception());
        }
      }
    }

    private:
    int fd;
    offset_type len;
    Receiver r;
  };

  class sender_impl {
    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<Tuple<>>;

    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr, std::error_code>;

    static inline constexpr bool sends_done = false;

    explicit sender_impl(int fd, offset_type len) noexcept
    : fd(fd),
      len(len)
    {}

    // Only permit move operations.
    sender_impl(sender_impl&&) = default;
    sender_impl(const sender_impl&) = delete;

    template<receiver_of<> Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    noexcept(std::is_nothrow_constructible_v<
        opstate<std::remove_cvref_t<Receiver>>,
        int, offset_type, Receiver>)
    -> opstate<std::remove_cvref_t<Receiver>> {
      return opstate<std::remove_cvref_t<Receiver>>(self.fd, self.len, std::forward<Receiver>(r));
    }

    private:
    int fd;
    offset_type len;
  };

  public:
  auto operator()(int fd, offset_type len) const
  -> sender_impl {
    return sender_impl(fd, len);
  }
};
inline constexpr lazy_truncate_ec_t lazy_truncate_ec{};


struct lazy_truncate_t {
  template<typename FD>
  auto operator()(FD&& fd, offset_type len) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_truncate_t, FD, offset_type>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(len));
    } else {
      return lazy_truncate_ec(std::forward<FD>(fd), std::move(len))
      | ec_to_exception();
    }
  }
};
inline constexpr lazy_truncate_t lazy_truncate{};


struct truncate_ec_t {
  template<typename FD>
  auto operator()(FD&& fd, offset_type len) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<truncate_ec_t, FD, offset_type>)
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(len));
    else
      return lazy_truncate_ec(std::forward<FD>(fd), std::move(len));
  }
};
inline constexpr truncate_ec_t truncate_ec{};


struct truncate_t {
  template<typename FD>
  auto operator()(FD&& fd, offset_type len) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<truncate_t, FD, offset_type>)
      return execution::tag_invoke(*this, std::forward<FD>(fd), std::move(len));
    else
      return lazy_truncate(std::forward<FD>(fd), std::move(len));
  }
};
inline constexpr truncate_t truncate{};


struct lazy_datasync_ec_t {
  template<typename FD>
  requires (!std::same_as<std::remove_cvref_t<FD>, int>)
  auto operator()(FD&& fd) const
  -> typed_sender decltype(auto) {
    return execution::tag_invoke(*this, std::forward<FD>(fd));
  }

  private:
  template<receiver_of<> Receiver>
  class opstate
  : public operation_state_base_
  {
    public:
    explicit opstate(int fd, Receiver&& r)
    noexcept(std::is_nothrow_move_constructible_v<Receiver>)
    : fd(fd),
      r(std::move(r))
    {}

    friend auto tag_invoke([[maybe_unused]] start_t, opstate& self) noexcept -> void {
      if (::fdatasync(self.fd) == -1) {
        execution::set_error(std::move(self.r), std::error_code(errno, std::generic_category()));
      } else {
        try {
          execution::set_value(std::move(self.r));
        } catch (...) {
          execution::set_error(std::move(self.r), std::current_exception());
        }
      }
    }

    private:
    int fd;
    Receiver r;
  };

  class sender_impl {
    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<Tuple<>>;

    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr, std::error_code>;

    static inline constexpr bool sends_done = false;

    explicit sender_impl(int fd) noexcept
    : fd(fd)
    {}

    // Only permit move operations.
    sender_impl(sender_impl&&) = default;
    sender_impl(const sender_impl&) = delete;

    template<receiver_of<> Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    noexcept(std::is_nothrow_constructible_v<
        opstate<std::remove_cvref_t<Receiver>>,
        int, offset_type, Receiver>)
    -> opstate<std::remove_cvref_t<Receiver>> {
      return opstate<std::remove_cvref_t<Receiver>>(self.fd, std::forward<Receiver>(r));
    }

    private:
    int fd;
  };

  public:
  auto operator()(int fd) const
  -> sender_impl {
    return sender_impl(fd);
  }
};
inline constexpr lazy_datasync_ec_t lazy_datasync_ec{};


struct lazy_datasync_t {
  template<typename FD>
  auto operator()(FD&& fd) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_datasync_t, FD>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd));
    } else {
      return lazy_datasync_ec(std::forward<FD>(fd))
      | ec_to_exception();
    }
  }
};
inline constexpr lazy_datasync_t lazy_datasync{};


struct datasync_ec_t {
  template<typename FD>
  auto operator()(FD&& fd) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<datasync_ec_t, FD>)
      return execution::tag_invoke(*this, std::forward<FD>(fd));
    else
      return lazy_datasync_ec(std::forward<FD>(fd));
  }
};
inline constexpr datasync_ec_t datasync_ec{};


struct datasync_t {
  template<typename FD>
  auto operator()(FD&& fd) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<datasync_t, FD>)
      return execution::tag_invoke(*this, std::forward<FD>(fd));
    else
      return lazy_datasync(std::forward<FD>(fd));
  }
};
inline constexpr datasync_t datasync{};


struct lazy_sync_ec_t {
  template<typename FD>
  requires (!std::same_as<std::remove_cvref_t<FD>, int>)
  auto operator()(FD&& fd) const
  -> typed_sender decltype(auto) {
    return execution::tag_invoke(*this, std::forward<FD>(fd));
  }

  private:
  template<receiver_of<> Receiver>
  class opstate
  : public operation_state_base_
  {
    public:
    explicit opstate(int fd, Receiver&& r)
    noexcept(std::is_nothrow_move_constructible_v<Receiver>)
    : fd(fd),
      r(std::move(r))
    {}

    friend auto tag_invoke([[maybe_unused]] start_t, opstate& self) noexcept -> void {
      if (::fsync(self.fd) == -1) {
        execution::set_error(std::move(self.r), std::error_code(errno, std::generic_category()));
      } else {
        try {
          execution::set_value(std::move(self.r));
        } catch (...) {
          execution::set_error(std::move(self.r), std::current_exception());
        }
      }
    }

    private:
    int fd;
    Receiver r;
  };

  class sender_impl {
    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<Tuple<>>;

    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr, std::error_code>;

    static inline constexpr bool sends_done = false;

    explicit sender_impl(int fd) noexcept
    : fd(fd)
    {}

    // Only permit move operations.
    sender_impl(sender_impl&&) = default;
    sender_impl(const sender_impl&) = delete;

    template<receiver_of<> Receiver>
    friend auto tag_invoke([[maybe_unused]] connect_t, sender_impl&& self, Receiver&& r)
    noexcept(std::is_nothrow_constructible_v<
        opstate<std::remove_cvref_t<Receiver>>,
        int, offset_type, Receiver>)
    -> opstate<std::remove_cvref_t<Receiver>> {
      return opstate<std::remove_cvref_t<Receiver>>(self.fd, std::forward<Receiver>(r));
    }

    private:
    int fd;
  };

  public:
  auto operator()(int fd) const
  -> sender_impl {
    return sender_impl(fd);
  }
};
inline constexpr lazy_sync_ec_t lazy_sync_ec{};


struct lazy_sync_t {
  template<typename FD>
  auto operator()(FD&& fd) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<lazy_sync_t, FD>) {
      return execution::tag_invoke(*this, std::forward<FD>(fd));
    } else {
      return lazy_sync_ec(std::forward<FD>(fd))
      | ec_to_exception();
    }
  }
};
inline constexpr lazy_sync_t lazy_sync{};


struct sync_ec_t {
  template<typename FD>
  auto operator()(FD&& fd) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<sync_ec_t, FD>)
      return execution::tag_invoke(*this, std::forward<FD>(fd));
    else
      return lazy_sync_ec(std::forward<FD>(fd));
  }
};
inline constexpr sync_ec_t sync_ec{};


struct sync_t {
  template<typename FD>
  auto operator()(FD&& fd) const
  -> typed_sender decltype(auto) {
    if constexpr(tag_invocable<sync_t, FD>)
      return execution::tag_invoke(*this, std::forward<FD>(fd));
    else
      return lazy_sync(std::forward<FD>(fd));
  }
};
inline constexpr sync_t sync{};


} /* namespace earnest::execution::io */
