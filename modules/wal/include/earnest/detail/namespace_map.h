#pragma once

#include <cstddef>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <earnest/file_id.h>
#include <earnest/xdr.h>

namespace earnest::detail {


template<std::size_t Version = 0>
struct namespace_map {
  std::unordered_map<std::string, std::string> namespaces;
  std::unordered_set<file_id> files;

  auto operator<=>(const namespace_map& y) const noexcept = default;
};

template<typename X>
inline auto operator&(xdr<X>&& x, typename xdr<X>::template typed_function_arg<namespace_map<0>> ns_map) {
  return std::move(x)
      & xdr_constant(0u).as(earnest::xdr_uint32)
      & xdr_map(ns_map.namespaces).of(xdr_bytes, xdr_bytes)
      & xdr_set(ns_map.files);
}


} /* namespace earnest::detail */
