#pragma once

#include <functional>
#include <iosfwd>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <spdlog/spdlog.h>

#include <earnest/xdr.h>

namespace earnest {


struct file_id {
  using allocator_type = std::string::allocator_type;

  file_id() noexcept(std::is_nothrow_default_constructible_v<std::string>) = default;

  explicit file_id(allocator_type alloc)
  : ns(alloc),
    filename(alloc)
  {}

  file_id(std::string ns, std::string filename) noexcept
  : ns(std::move(ns)),
    filename(std::move(filename))
  {}

  auto get_allocator() const -> allocator_type { return ns.get_allocator(); }

  auto operator<=>(const file_id& y) const noexcept = default;
  auto operator==(const file_id& y) const noexcept -> bool = default;

  auto to_string() const -> std::string;

  std::string ns; // Namespace in which the file is located.
  std::string filename; // Name of the file.
};

template<typename X>
inline auto operator&(xdr<X>&& x, typename xdr<X>::template typed_function_arg<file_id> fid) {
  return std::move(x)
      & xdr_bytes(fid.ns)
      & xdr_bytes(fid.filename);
}

template<typename CharT, typename Traits>
inline auto operator<<(std::basic_ostream<CharT, Traits>& out, const file_id& fid) -> std::basic_ostream<CharT, Traits>& {
  using namespace std::string_view_literals;

  return out << "file_id{ns=\""sv << fid.ns << "\", filename=\""sv << fid.filename << "\"}"sv;
}

inline auto file_id::to_string() const -> std::string {
  std::ostringstream s;
  s << *this;
  return std::move(s).str();
}


} /* namespace earnest */

namespace std {


template<>
struct hash<::earnest::file_id> {
  auto operator()(const ::earnest::file_id& fid) const noexcept -> std::size_t {
    std::hash<std::string_view> sv_hash;
    return 10001u * sv_hash(fid.ns) + sv_hash(fid.filename);
  }
};


} /* namespace std */

namespace fmt {


template<>
struct formatter<earnest::file_id>
: formatter<std::string>
{
  auto format(const earnest::file_id& fid, format_context& ctx) -> decltype(ctx.out()) {
    return format_to(ctx.out(), "{}/{}", fid.ns, fid.filename);
  }
};


} /* namespace spdlog::fmt */
