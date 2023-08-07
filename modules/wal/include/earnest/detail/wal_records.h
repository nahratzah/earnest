#pragma once

#include <type_traits>
#include <variant>

#include <earnest/detail/overload.h>
#include <earnest/fd.h>
#include <earnest/file_id.h>
#include <earnest/xdr.h>

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


struct wal_record_end_of_records {
  static constexpr std::uint32_t opcode = 0;

  auto operator<=>(const wal_record_end_of_records& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_end_of_records> { using type = wal_record_end_of_records; };

template<typename X>
inline auto operator&(xdr<X>&& x, [[maybe_unused]] const wal_record_end_of_records& eor) noexcept -> xdr<X>&& {
  return std::move(x);
}


struct wal_record_noop {
  static constexpr std::uint32_t opcode = 1;

  auto operator<=>(const wal_record_noop& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_noop> { using type = wal_record_noop; };

template<typename X>
inline auto operator&(xdr<X>&& x, [[maybe_unused]] const wal_record_noop& noop) noexcept -> xdr<X>&& {
  return std::move(x);
}


struct wal_record_skip32 {
  static constexpr std::uint32_t opcode = 2;

  std::uint32_t bytes;

  auto operator<=>(const wal_record_skip32& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_skip32> { using type = wal_record_skip32; };

struct wal_record_skip64 {
  static constexpr std::uint32_t opcode = 3;

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
  static constexpr std::uint32_t opcode = 4;

  auto operator<=>(const wal_record_seal& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_seal> { using type = wal_record_seal; };

template<typename X>
inline auto operator&(::earnest::xdr<X>&& x, [[maybe_unused]] const wal_record_seal& r) -> ::earnest::xdr<X>&& {
  return std::move(x);
}


// Indicates a specific WAL file has been archived.
struct wal_record_wal_archived {
  static constexpr std::uint32_t opcode = 5;

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
  static constexpr std::uint32_t opcode = 6;

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
  static constexpr std::uint32_t opcode = 7;

  std::string filename;

  auto operator<=>(const wal_record_rollover_ready& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_rollover_ready> { using type = wal_record_rollover_ready; };

template<typename X>
inline auto operator&(xdr<X>&& x, typename xdr<X>::template typed_function_arg<wal_record_rollover_ready> r) {
  return std::move(x) & xdr_bytes(r.filename);
}


struct wal_record_create_file {
  static constexpr std::uint32_t opcode = 32;

  file_id file;

  auto operator<=>(const wal_record_create_file& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_create_file> { using type = wal_record_create_file; };

template<typename X>
inline auto operator&(::earnest::xdr<X>&& x, typename xdr<X>::template typed_function_arg<wal_record_create_file> r) {
  return std::move(x) & r.file;
}


struct wal_record_erase_file {
  static constexpr std::uint32_t opcode = 33;

  file_id file;

  auto operator<=>(const wal_record_erase_file& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_erase_file> { using type = wal_record_erase_file; };

template<typename X>
inline auto operator&(::earnest::xdr<X>&& x, typename xdr<X>::template typed_function_arg<wal_record_erase_file> r) {
  return std::move(x) & r.file;
}


struct wal_record_truncate_file {
  static constexpr std::uint32_t opcode = 34;

  file_id file;
  std::uint64_t new_size;

  auto operator<=>(const wal_record_truncate_file& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_truncate_file> { using type = wal_record_truncate_file; };

template<typename X>
inline auto operator&(::earnest::xdr<X>&& x, typename ::earnest::xdr<X>::template typed_function_arg<wal_record_truncate_file> r) {
  return std::move(x) & r.file & xdr_uint64(r.new_size);
}


struct wal_record_modify_file_write32 {
  static constexpr std::uint32_t opcode = 35;

  static inline constexpr std::size_t data_offset = 4u;

  file_id file;
  std::uint64_t file_offset;
  std::vector<std::byte> data;
};

template<typename Executor, typename Reactor = aio::reactor>
struct wal_record_modify_file32 {
  static constexpr std::uint32_t opcode = wal_record_modify_file_write32::opcode;
  static inline constexpr std::size_t data_offset = wal_record_modify_file_write32::data_offset;

  file_id file;
  std::uint64_t file_offset;
  std::uint64_t wal_offset;
  std::uint32_t wal_len;
  std::shared_ptr<const fd<Executor, Reactor>> wal_file;

  auto operator<=>(const wal_record_modify_file32& y) const noexcept = default;
};

template<typename Executor, typename Reactor> struct record_write_type_<wal_record_modify_file32<Executor, Reactor>> { using type = wal_record_modify_file_write32; };

template<typename... X, typename Executor, typename Reactor>
inline auto operator&(::earnest::xdr_reader<X...>&& x, wal_record_modify_file32<Executor, Reactor>& r) {
  return std::move(x)
      & xdr_uint32(r.wal_len)
      & xdr_manual(
          [&r](auto& stream, auto callback) {
            r.wal_offset = stream.position();
            stream.skip(r.wal_len);
            if (r.wal_len % 4u != 0u) stream.skip(4u - (r.wal_len % 4u));
            std::invoke(callback, std::error_code());
          })
      & r.file
      & xdr_uint64(r.file_offset);
}

template<typename... X>
inline auto operator&(::earnest::xdr_writer<X...>&& x, typename ::earnest::xdr_writer<X...>::template typed_function_arg<wal_record_modify_file_write32> r) {
  return std::move(x)
      & xdr_bytes(r.data)
      & r.file
      & xdr_uint64(r.file_offset);
}


template<typename Executor, typename Reactor>
using wal_record_variant_ = std::variant<
    wal_record_end_of_records, // 0
    wal_record_noop, // 1
    wal_record_skip32, // 2
    wal_record_skip64, // 3
    wal_record_seal, // 4
    wal_record_wal_archived, // 5
    wal_record_rollover_intent, // 6
    wal_record_rollover_ready, // 7
    wal_record_create_file, // 32
    wal_record_erase_file, // 33
    wal_record_truncate_file, // 34
    wal_record_modify_file32<Executor, Reactor> // 35
    >;

// This type simply wraps the underlying std::variant.
// But having it as a struct makes reading types easier.
template<typename Executor, typename Reactor = aio::reactor>
struct wal_record_variant
: public wal_record_variant_<Executor, Reactor>
{
  using wal_record_variant_<Executor, Reactor>::wal_record_variant_;

  wal_record_variant(const wal_record_variant_<Executor, Reactor>& r)
      noexcept(std::is_nothrow_copy_constructible_v<wal_record_variant_<Executor, Reactor>>)
  : wal_record_variant_<Executor, Reactor>(r)
  {}

  wal_record_variant(wal_record_variant_<Executor, Reactor>&& r)
      noexcept(std::is_nothrow_move_constructible_v<wal_record_variant_<Executor, Reactor>>)
  : wal_record_variant_<Executor, Reactor>(std::move(r))
  {}

  private:
  template<typename Stream, typename Callback>
  auto decode_with_discriminants_([[maybe_unused]] std::uint32_t discriminant, [[maybe_unused]] Stream& stream, Callback&& callback, [[maybe_unused]] std::index_sequence<>) -> void {
    std::invoke(callback, make_error_code(xdr_errc::decoding_error));
  }

  template<typename Stream, typename Callback, std::size_t VariantIndex0, std::size_t... VariantIndices>
  auto decode_with_discriminants_(std::uint32_t discriminant, Stream& stream, Callback&& callback, [[maybe_unused]] std::index_sequence<VariantIndex0, VariantIndices...>) -> void {
    using type = std::variant_alternative_t<VariantIndex0, wal_record_variant>;

    if (discriminant == type::opcode) {
      ::earnest::async_read(
          stream,
          ::earnest::xdr_reader<>() & this->template emplace<VariantIndex0>(),
          std::forward<Callback>(callback));
    } else {
      decode_with_discriminants_(discriminant, stream, std::forward<Callback>(callback), std::index_sequence<VariantIndices...>{});
    }
  }

  auto decoder_() {
    return earnest::xdr_manual(
        [this](std::uint32_t discriminant, auto& stream, auto callback) -> void {
          this->decode_with_discriminants_(discriminant, stream, std::move(callback), std::make_index_sequence<std::variant_size_v<wal_record_variant>>());
        }).template with_lead<std::uint32_t>(xdr_uint32);
  }

  public:
  template<typename... X>
  friend inline auto operator&(::earnest::xdr_reader<X...>&& x, wal_record_variant& v) {
    return std::move(x) & v.decoder_();
  }
};

// This type simply wraps the underlying std::variant.
// But having it as a struct makes reading types easier.
template<typename Executor, typename Reactor>
struct wal_record_write_variant
: public record_write_type_t<wal_record_variant_<Executor, Reactor>>
{
  using record_write_type_t<wal_record_variant_<Executor, Reactor>>::record_write_type_t;

  wal_record_write_variant(const record_write_type_t<wal_record_variant_<Executor, Reactor>>& r)
      noexcept(std::is_nothrow_copy_constructible_v<record_write_type_t<wal_record_variant_<Executor, Reactor>>>)
  : record_write_type_t<wal_record_variant_<Executor, Reactor>>(r)
  {}

  wal_record_write_variant(record_write_type_t<wal_record_variant_<Executor, Reactor>>&& r)
      noexcept(std::is_nothrow_move_constructible_v<record_write_type_t<wal_record_variant_<Executor, Reactor>>>)
  : record_write_type_t<wal_record_variant_<Executor, Reactor>>(std::move(r))
  {}

  private:
  auto encoder_() const {
    return earnest::xdr_manual(
        [this](auto& stream, auto callback) -> void {
          std::visit(
              [&stream, &callback](const auto& v) -> void {
                ::earnest::async_write(
                    stream,
                    ::earnest::xdr_writer<>() & xdr_uint32(v.opcode) & v,
                    std::move(callback));
              },
              *this);
        });
  }

  public:
  template<typename... X>
  friend inline auto operator&(::earnest::xdr_writer<X...>&& x, const wal_record_write_variant& v) {
    return std::move(x) & v.encoder_();
  }
};

template<typename Executor, typename Reactor>
struct record_write_type_<wal_record_variant<Executor, Reactor>>
{
  using type = wal_record_write_variant<Executor, Reactor>;
};

// Indices 0..31 (inclusive) are reserved for bookkeeping of the WAL.
inline constexpr auto wal_record_is_bookkeeping(std::size_t idx) noexcept -> bool {
  return idx < 32;
}

template<typename Executor, typename Reactor>
inline auto wal_record_is_bookkeeping(const wal_record_variant<Executor, Reactor>& r) noexcept -> bool {
  return wal_record_is_bookkeeping(std::visit([](const auto& r) { return r.opcode; }, r));
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
: filter<filtered_type<!wal_record_is_bookkeeping(std::variant_alternative_t<Idx, Variant>::opcode), std::variant_alternative_t<Idx, Variant>>...>
{
  public:
  template<typename R>
  static auto convert(R&& r) -> typename without_bookkeeping_variant::type {
    return std::visit(
        overrides(
            converter<filtered_type<!wal_record_is_bookkeeping(std::variant_alternative_t<Idx, Variant>::opcode), std::variant_alternative_t<Idx, Variant>>, typename without_bookkeeping_variant::type>()...
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


template<typename FdType, typename VariantType> struct wal_record_write_to_read_converter;

template<typename Executor, typename Reactor, typename... T>
struct wal_record_write_to_read_converter<fd<Executor, Reactor>, std::variant<T...>> {
  using variant_type = std::variant<T...>;
  using write_variant_type = record_write_type_t<variant_type>;
  static inline constexpr std::size_t variant_discriminant_bytes = 4;

  wal_record_write_to_read_converter(std::shared_ptr<const fd<Executor, Reactor>> wal_file, std::uint64_t offset) noexcept
  : wal_file(std::move(wal_file)),
    offset(std::move(offset))
  {}

  auto operator()(const write_variant_type& wrecord) -> variant_type {
    return std::visit(
        overload(
            [](const wal_record_end_of_records& r) -> variant_type { return r; },
            [](const wal_record_noop& r) -> variant_type { return r; },
            [](const wal_record_skip32& r) -> variant_type { return r; },
            [](const wal_record_skip64& r) -> variant_type { return r; },
            [](const wal_record_seal& r) -> variant_type { return r; },
            [](const wal_record_wal_archived& r) -> variant_type { return r; },
            [](const wal_record_rollover_intent& r) -> variant_type { return r; },
            [](const wal_record_rollover_ready& r) -> variant_type { return r; },
            [](const wal_record_create_file& r) -> variant_type { return r; },
            [](const wal_record_erase_file& r) -> variant_type { return r; },
            [](const wal_record_truncate_file& r) -> variant_type { return r; },
            [this](const record_write_type_t<wal_record_modify_file32<Executor, Reactor>>& r) -> variant_type {
              if (r.data.size() > 0xffff'ffffU) throw std::logic_error("too much data");
              return wal_record_modify_file32<Executor, Reactor>{
                .file=r.file,
                .file_offset=r.file_offset,
                .wal_offset=this->offset + record_write_type_t<wal_record_modify_file32<Executor, Reactor>>::data_offset + variant_discriminant_bytes,
                .wal_len=static_cast<std::uint32_t>(r.data.size()),
                .wal_file=this->wal_file,
              };
            }),
        wrecord);
  }

  private:
  std::shared_ptr<const fd<Executor, Reactor>> wal_file;
  std::uint64_t offset;
};

template<typename Executor, typename Reactor>
struct wal_record_write_to_read_converter<fd<Executor, Reactor>, wal_record_variant<Executor, Reactor>> {
  using variant_type = wal_record_variant<Executor, Reactor>;
  using write_variant_type = wal_record_write_variant<Executor, Reactor>;

  wal_record_write_to_read_converter(std::shared_ptr<const fd<Executor, Reactor>> wal_file, std::uint64_t offset) noexcept
  : converter_(std::move(wal_file), std::move(offset))
  {}

  auto operator()(const write_variant_type& wrecord) -> variant_type {
    return variant_type(converter_(wrecord));
  }

  private:
  wal_record_write_to_read_converter<fd<Executor, Reactor>, wal_record_variant_<Executor, Reactor>> converter_;
};


} /* namespace earnest::detail */

namespace std {


template<typename Executor, typename Reactor>
struct variant_size<::earnest::detail::wal_record_variant<Executor, Reactor>>
: variant_size<::earnest::detail::wal_record_variant_<Executor, Reactor>>
{};

template<typename Executor, typename Reactor>
struct variant_size<::earnest::detail::wal_record_write_variant<Executor, Reactor>>
: variant_size<::earnest::detail::record_write_type_t<::earnest::detail::wal_record_variant_<Executor, Reactor>>>
{};


template<std::size_t I, typename Executor, typename Reactor>
struct variant_alternative<I, ::earnest::detail::wal_record_variant<Executor, Reactor>>
: variant_alternative<I, ::earnest::detail::wal_record_variant_<Executor, Reactor>>
{};

template<std::size_t I, typename Executor, typename Reactor>
struct variant_alternative<I, ::earnest::detail::wal_record_write_variant<Executor, Reactor>>
: variant_alternative<I, ::earnest::detail::record_write_type_t<::earnest::detail::wal_record_variant_<Executor, Reactor>>>
{};


}
