#pragma once

#include <type_traits>
#include <variant>

#include <earnest/file_id.h>
#include <earnest/xdr.h>
#include <earnest/fd.h>

namespace earnest::detail {


template<typename> struct record_write_type_;

template<typename T>
using record_write_type_t = typename record_write_type_<T>::type;

template<typename... T>
struct record_write_type_<std::variant<T...>> {
  using type = std::variant<record_write_type_t<T>...>;
};

template<typename... T>
struct record_write_type_<std::tuple<T...>> {
  using type = std::tuple<record_write_type_t<T>...>;
};


template<std::size_t Idx>
struct wal_record_reserved {
  auto operator<=>(const wal_record_reserved& y) const noexcept = default;
};
template<std::size_t Idx> struct record_write_type_<wal_record_reserved<Idx>> { using type = wal_record_reserved<Idx>; };

template<typename X, std::size_t Idx>
inline auto operator&(xdr<X>&& x, [[maybe_unused]] const wal_record_reserved<Idx>& noop) -> xdr<X>&& {
  throw std::logic_error("bug: shouldn't be serializing/deserializing reserved elements");
  return std::move(x);
}


struct wal_record_end_of_records {
  auto operator<=>(const wal_record_end_of_records& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_end_of_records> { using type = wal_record_end_of_records; };

template<typename X>
inline auto operator&(xdr<X>&& x, [[maybe_unused]] const wal_record_end_of_records& eor) noexcept -> xdr<X>&& {
  return std::move(x);
}


struct wal_record_noop {
  auto operator<=>(const wal_record_noop& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_noop> { using type = wal_record_noop; };

template<typename X>
inline auto operator&(xdr<X>&& x, [[maybe_unused]] const wal_record_noop& noop) noexcept -> xdr<X>&& {
  return std::move(x);
}


struct wal_record_skip32 {
  std::uint32_t bytes;

  auto operator<=>(const wal_record_skip32& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_skip32> { using type = wal_record_skip32; };

struct wal_record_skip64 {
  std::uint64_t bytes;

  auto operator<=>(const wal_record_skip64& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_skip64> { using type = wal_record_skip64; };

template<typename... X>
inline auto operator&(::earnest::xdr_reader<X...>&& x, wal_record_skip32& skip) {
  return std::move(x) & xdr_uint32(skip.bytes) & xdr_manual(
      [&skip](auto& stream, auto callback, [[maybe_unused]] std::string& s) {
        if constexpr(requires { stream.skip(skip.bytes); }) {
          stream.skip(skip.bytes);
          std::invoke(callback, std::error_code());
        } else {
          s.resize(skip.bytes, '\0');
          asio::async_read(
              stream,
              asio::buffer(s),
              [callback=std::move(callback)](std::error_code ec, [[maybe_unused]] std::size_t nbytes) mutable {
                std::invoke(callback, ec);
              });
        }
      }).template with_temporary<std::string>();
}

template<typename... X>
inline auto operator&(::earnest::xdr_writer<X...>&& x, const wal_record_skip32& skip) {
  return std::move(x) & xdr_uint32(skip.bytes) & xdr_manual(
      [&skip](auto& stream, auto callback, [[maybe_unused]] std::string& s) {
        if constexpr(requires { stream.skip(skip.bytes); }) {
          stream.skip(skip.bytes);
        } else {
          s.resize(skip.bytes, '\0');
          asio::async_write(
              stream,
              asio::buffer(s),
              [callback=std::move(callback)](std::error_code ec, [[maybe_unused]] std::size_t nbytes) mutable {
                std::invoke(callback, ec);
              });
        }
      }).template with_temporary<std::string>();
}

template<typename... X>
inline auto operator&(::earnest::xdr_reader<X...>&& x, wal_record_skip64& skip) {
  return std::move(x) & xdr_uint64(skip.bytes) & xdr_manual(
      [&skip](auto& stream, auto callback, [[maybe_unused]] std::string& s) {
        if constexpr(requires { stream.skip(skip.bytes); }) {
          stream.skip(skip.bytes);
          std::invoke(callback, std::error_code());
        } else {
          s.resize(skip.bytes, '\0');
          asio::async_read(
              stream,
              asio::buffer(s),
              [callback=std::move(callback)](std::error_code ec, [[maybe_unused]] std::size_t nbytes) mutable {
                std::invoke(callback, ec);
              });
        }
      }).template with_temporary<std::string>();
}

template<typename... X>
inline auto operator&(::earnest::xdr_writer<X...>&& x, const wal_record_skip64& skip) {
  return std::move(x) & xdr_uint64(skip.bytes) & xdr_manual(
      [&skip](auto& stream, auto callback, [[maybe_unused]] std::string& s) {
        if constexpr(requires { stream.skip(skip.bytes); }) {
          stream.skip(skip.bytes);
        } else {
          s.resize(skip.bytes, '\0');
          asio::async_write(
              stream,
              asio::buffer(s),
              [callback=std::move(callback)](std::error_code ec, [[maybe_unused]] std::size_t nbytes) mutable {
                std::invoke(callback, ec);
              });
        }
      }).template with_temporary<std::string>();
}


struct wal_record_seal {
  auto operator<=>(const wal_record_seal& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_seal> { using type = wal_record_seal; };

template<typename X>
inline auto operator&(::earnest::xdr<X>&& x, [[maybe_unused]] const wal_record_seal& r) -> ::earnest::xdr<X>&& {
  return std::move(x);
}


// Indicates a specific WAL file has been archived.
struct wal_record_wal_archived {
  std::uint64_t sequence;

  auto operator<=>(const wal_record_wal_archived& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_wal_archived> { using type = wal_record_wal_archived; };

template<typename X>
inline auto operator&(xdr<X>&& x, typename xdr<X>::template typed_function_arg<wal_record_wal_archived> r) {
  return std::move(x) & xdr_uint64(r.sequence);
}


// Declare intent of writing the next wal-file.
struct wal_record_rollover_intent {
  std::string filename;

  auto operator<=>(const wal_record_rollover_intent& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_rollover_intent> { using type = wal_record_rollover_intent; };

template<typename X>
inline auto operator&(xdr<X>&& x, typename xdr<X>::template typed_function_arg<wal_record_rollover_intent> r) {
  return std::move(x) & xdr_bytes(r.filename);
}


// Declare that the next wal-file was successfully created.
struct wal_record_rollover_ready {
  std::string filename;

  auto operator<=>(const wal_record_rollover_ready& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_rollover_ready> { using type = wal_record_rollover_ready; };

template<typename X>
inline auto operator&(xdr<X>&& x, typename xdr<X>::template typed_function_arg<wal_record_rollover_ready> r) {
  return std::move(x) & xdr_bytes(r.filename);
}


struct wal_record_create_file {
  file_id file;

  auto operator<=>(const wal_record_create_file& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_create_file> { using type = wal_record_create_file; };

template<typename X>
inline auto operator&(::earnest::xdr<X>&& x, typename xdr<X>::template typed_function_arg<wal_record_create_file> r) {
  return std::move(x) & r.file;
}


struct wal_record_erase_file {
  file_id file;

  auto operator<=>(const wal_record_erase_file& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_erase_file> { using type = wal_record_erase_file; };

template<typename X>
inline auto operator&(::earnest::xdr<X>&& x, typename xdr<X>::template typed_function_arg<wal_record_erase_file> r) {
  return std::move(x) & r.file;
}


struct wal_record_truncate_file {
  file_id file;
  std::uint64_t new_size;

  auto operator<=>(const wal_record_truncate_file& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_truncate_file> { using type = wal_record_truncate_file; };

template<typename X>
inline auto operator&(::earnest::xdr<X>&& x, typename ::earnest::xdr<X>::template typed_function_arg<wal_record_truncate_file> r) {
  return std::move(x) & r.file & xdr_uint64(r.new_size);
}


template<typename Executor, typename Reactor = aio::reactor>
struct wal_record_modify_file32 {
  file_id file;
  std::uint64_t file_offset;
  std::uint64_t wal_offset;
  std::uint32_t wal_len;
  std::shared_ptr<const fd<Executor, Reactor>> wal_file;

  auto operator<=>(const wal_record_modify_file32& y) const noexcept = default;
};

struct wal_record_modify_file_write32 {
  file_id file;
  std::uint64_t file_offset;
  std::vector<std::byte> data;
};

template<typename Executor, typename Reactor> struct record_write_type_<wal_record_modify_file32<Executor, Reactor>> { using type = wal_record_modify_file_write32; };

template<typename... X, typename Executor, typename Reactor>
inline auto operator&(::earnest::xdr_reader<X...>&& x, wal_record_modify_file32<Executor, Reactor>& r) {
  return std::move(x)
      & r.file
      & xdr_uint64(r.file_offset)
      & xdr_uint32(r.wal_len)
      & xdr_manual(
          [&r](auto& stream, auto callback) {
            r.wal_offset = stream.position();
            stream.skip(r.wal_len);
            if (r.wal_len % 4u != 0u) stream.skip(4u - (r.wal_len % 4u));
            std::invoke(callback, std::error_code());
          });
}

template<typename... X>
inline auto operator&(::earnest::xdr_writer<X...>&& x, typename ::earnest::xdr_writer<X...>::template typed_function_arg<wal_record_modify_file_write32> r) {
  return std::move(x)
      & r.file
      & xdr_uint64(r.file_offset)
      & xdr_bytes(r.data);
}


template<typename Executor, typename Reactor = aio::reactor>
using wal_record_variant = std::variant<
    wal_record_end_of_records, // 0
    wal_record_noop, // 1
    wal_record_skip32, // 2
    wal_record_skip64, // 3
    wal_record_seal, // 4
    wal_record_wal_archived, // 5
    wal_record_rollover_intent, // 6
    wal_record_rollover_ready, // 7
    wal_record_reserved<8>,
    wal_record_reserved<9>,
    wal_record_reserved<10>,
    wal_record_reserved<11>,
    wal_record_reserved<12>,
    wal_record_reserved<13>,
    wal_record_reserved<14>,
    wal_record_reserved<15>,
    wal_record_reserved<16>,
    wal_record_reserved<17>,
    wal_record_reserved<18>,
    wal_record_reserved<19>,
    wal_record_reserved<20>,
    wal_record_reserved<21>,
    wal_record_reserved<22>,
    wal_record_reserved<23>,
    wal_record_reserved<24>,
    wal_record_reserved<25>,
    wal_record_reserved<26>,
    wal_record_reserved<27>,
    wal_record_reserved<28>,
    wal_record_reserved<29>,
    wal_record_reserved<30>,
    wal_record_reserved<31>,
    wal_record_create_file, // 32
    wal_record_erase_file, // 33
    wal_record_truncate_file, // 34
    wal_record_modify_file32<Executor, Reactor> // 35
    >;

// Indices 0..31 (inclusive) are reserved for bookkeeping of the WAL.
inline constexpr auto wal_record_is_bookkeeping(std::size_t idx) noexcept -> bool {
  return idx < 32;
}

template<typename Executor, typename Reactor>
inline auto wal_record_is_bookkeeping(const wal_record_variant<Executor, Reactor>& r) noexcept -> bool {
  return wal_record_is_bookkeeping(r.index());
}


namespace record_support_ {


template<bool Include, typename T> struct filtered_type;

template<typename Variant, typename T> struct prepend_variant_type;
template<typename... VT, typename T>
struct prepend_variant_type<std::variant<VT...>, T> {
  using type = std::variant<T, VT...>;
};


template<typename... FilteredType> struct filter;

template<>
struct filter<> {
  using type = std::variant<>;
};

template<typename FilteredType, typename... Tail>
struct filter<filtered_type<false, FilteredType>, Tail...> {
  using type = typename filter<Tail...>::type;
};

template<typename FilteredType, typename... Tail>
struct filter<filtered_type<true, FilteredType>, Tail...> {
  using type = typename prepend_variant_type<typename filter<Tail...>::type, FilteredType>::type;
};


template<typename... Functors>
struct overrides_t
: public Functors...
{
  overrides_t(Functors... functors)
  : Functors(std::move(functors))...
  {}

  using Functors::operator()...;
};

template<typename... Functors>
auto overrides(Functors&&... functors) -> overrides_t<std::remove_cvref_t<Functors>...> {
  return overrides_t<std::remove_cvref_t<Functors>...>(std::forward<Functors>(functors)...);
}


template<typename FilteredType, typename Result> struct converter;

template<typename FilteredType, typename Result>
struct converter<filtered_type<true, FilteredType>, Result> {
  auto operator()(const FilteredType& t) -> Result {
    return t;
  }

  auto operator()(FilteredType&& t) -> Result {
    return std::move(t);
  }
};

template<typename FilteredType, typename Result>
struct converter<filtered_type<false, FilteredType>, Result> {
  auto operator()([[maybe_unused]] const FilteredType& t) -> Result {
    throw std::logic_error("type is excluded from non-bookkeeping wal-records");
  }
};


template<typename Variant, typename Indices = std::make_index_sequence<std::variant_size_v<Variant>>>
struct without_bookkeeping_variant;

template<typename Variant, size_t... Idx>
struct without_bookkeeping_variant<Variant, std::index_sequence<Idx...>>
: filter<filtered_type<!wal_record_is_bookkeeping(Idx), std::variant_alternative_t<Idx, Variant>>...>
{
  public:
  template<typename R>
  static auto convert(R&& r) -> typename without_bookkeeping_variant::type {
    return std::visit(
        overrides(
            converter<filtered_type<!wal_record_is_bookkeeping(Idx), std::variant_alternative_t<Idx, Variant>>, typename without_bookkeeping_variant::type>()...
        ),
        std::forward<R>(r));
  }
};


} /* namespace earnest::detail::record_support_ */


template<typename WalRecordVariant>
using wal_record_no_bookkeeping = typename record_support_::without_bookkeeping_variant<WalRecordVariant>::type;

template<typename WalRecordVariant>
auto make_wal_record_no_bookkeeping(WalRecordVariant&& v) -> wal_record_no_bookkeeping<std::remove_cvref_t<WalRecordVariant>> {
  return record_support_::without_bookkeeping_variant<std::remove_cvref_t<WalRecordVariant>>::convert(std::forward<WalRecordVariant>(v));
}


} /* namespace earnest::detail */
