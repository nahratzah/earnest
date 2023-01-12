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


struct wal_record_reserved {
  auto operator<=>(const wal_record_reserved& y) const noexcept = default;
};
template<> struct record_write_type_<wal_record_reserved> { using type = wal_record_reserved; };

template<typename X>
inline auto operator&(xdr<X>&& x, [[maybe_unused]] const wal_record_reserved& noop) -> xdr<X>&& {
  throw std::logic_error("bug: shouldn't be serializing/deserializing reserved elements");
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


struct wal_record_modify_file32 {
  file_id file;
  std::uint64_t file_offset;
  std::uint64_t wal_offset;
  std::uint32_t wal_len;

  auto operator<=>(const wal_record_modify_file32& y) const noexcept = default;
};

struct wal_record_modify_file_write32 {
  file_id file;
  std::uint64_t file_offset;
  std::vector<std::byte> data;
};

template<> struct record_write_type_<wal_record_modify_file32> { using type = wal_record_modify_file_write32; };

template<typename... X>
inline auto operator&(::earnest::xdr_reader<X...>&& x, typename ::earnest::xdr_reader<X...>::template typed_function_arg<wal_record_modify_file32> r) {
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


} /* namespace earnest::detail */
