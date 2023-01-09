#pragma once

#include <variant>

#include <earnest/file_id.h>
#include <earnest/xdr.h>

namespace earnest::detail {


template<typename> struct record_write_type_;
template<> struct record_write_type_<std::monostate> { using type = std::monostate; };

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


struct wal_record_noop {};
template<> struct record_write_type_<wal_record_noop> { using type = wal_record_noop; };

template<typename X>
inline auto operator&(xdr<X>&& x, const wal_record_noop& noop) noexcept -> xdr<X>&&;


struct wal_record_skip32 {
  std::uint32_t bytes;
};
template<> struct record_write_type_<wal_record_skip32> { using type = wal_record_skip32; };

struct wal_record_skip64 {
  std::uint64_t bytes;
};
template<> struct record_write_type_<wal_record_skip64> { using type = wal_record_skip64; };

template<typename... X>
inline auto operator&(::earnest::xdr_reader<X...>&& x, wal_record_skip32& skip);
template<typename... X>
inline auto operator&(::earnest::xdr_writer<X...>&& x, const wal_record_skip32& skip);

template<typename... X>
inline auto operator&(::earnest::xdr_reader<X...>&& x, wal_record_skip64& skip);
template<typename... X>
inline auto operator&(::earnest::xdr_writer<X...>&& x, const wal_record_skip64& skip);


struct wal_record_create_file {
  file_id file;
};
template<> struct record_write_type_<wal_record_create_file> { using type = wal_record_create_file; };

template<typename X>
inline auto operator&(::earnest::xdr<X>&& x, typename xdr<X>::template typed_function_arg<wal_record_create_file> r) {
  return std::move(x) & r.file;
}


struct wal_record_erase_file {
  file_id file;
};
template<> struct record_write_type_<wal_record_erase_file> { using type = wal_record_erase_file; };

template<typename X>
inline auto operator&(::earnest::xdr<X>&& x, typename xdr<X>::template typed_function_arg<wal_record_erase_file> r) {
  return std::move(x) & r.file;
}


struct wal_record_truncate_file {
  file_id file;
  std::uint64_t new_size;
};
template<> struct record_write_type_<wal_record_truncate_file> { using type = wal_record_truncate_file; };

template<typename X>
inline auto operator&(::earnest::xdr<X>&& x, typename xdr<X>::template typed_function_arg<wal_record_truncate_file> r) {
  return std::move(x) & r.file & xdr_uint64(r.new_size);
}


struct wal_record_modify_file32 {
  file_id file;
  std::uint64_t file_offset;
  std::uint64_t wal_offset;
  std::uint32_t wal_len;
};

struct wal_record_modify_file_write32 {
  file_id file;
  std::uint64_t file_offset;
  std::vector<std::byte> data;
};

template<> struct record_write_type_<wal_record_modify_file32> { using type = wal_record_modify_file_write32; };

template<typename X>
inline auto operator&(::earnest::xdr<X>&& x, typename xdr<X>::template typed_function_arg<wal_record_modify_file32> r) {
  return std::move(x)
      & r.file
      & xdr_uint64(r.file_offset)
      & xdr_uint32(r.wal_len)
      & xdr_manual(
          [&r](auto& stream, auto callback) {
            static_assert(::earnest::xdr<X>::is_reader);
            r.wal_offset = stream.position();
            std::invoke(callback, std::error_code());
          });
}

template<typename X>
inline auto operator&(::earnest::xdr<X>&& x, typename xdr<X>::template typed_function_arg<wal_record_modify_file_write32> r) {
  return std::move(x)
      & r.file
      & xdr_uint64(r.file_offset)
      & xdr_bytes(r.data);
}


} /* namespace earnest::detail */
