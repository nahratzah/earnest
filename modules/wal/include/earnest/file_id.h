#pragma once

#include <string>
#include <type_traits>
#include <utility>
#include <asio/buffer.hpp>

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

  std::string ns; // Namespace in which the file is located.
  std::string filename; // Name of the file.
};

template<typename X>
inline auto operator&(xdr<X>&& x, typename xdr<X>::template typed_function_arg<file_id> fid) {
  return std::move(x)
      & xdr_bytes(fid.ns)
      & xdr_bytes(fid.filename);
}


} /* namespace earnest */
