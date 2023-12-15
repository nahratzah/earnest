#pragma once

#include <cstddef>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <earnest/file_id.h>
#include <earnest/xdr.h>
#include <earnest/xdr_v2.h>

namespace earnest::detail {


template<std::size_t Version = 0>
struct namespace_map {
  std::unordered_map<std::string, std::string> namespaces;
  std::unordered_set<file_id> files;

  auto operator<=>(const namespace_map& y) const noexcept = default;

  friend auto tag_invoke([[maybe_unused]] xdr_v2::reader_t tag, namespace_map& m) {
    return xdr_v2::constant.read(0u, xdr_v2::uint32)
    | xdr_v2::collection(xdr_v2::pair(xdr_v2::ascii_string, xdr_v2::ascii_string)).read(m.namespaces)
    | xdr_v2::collection(xdr_v2::identity).read(m.files);
  }

  friend auto tag_invoke([[maybe_unused]] xdr_v2::writer_t tag, const namespace_map& m) {
    return xdr_v2::constant.write(0u, xdr_v2::uint32)
    | xdr_v2::collection(xdr_v2::pair(xdr_v2::ascii_string, xdr_v2::ascii_string)).write(m.namespaces)
    | xdr_v2::collection(xdr_v2::identity).write(m.files);
  }
};

template<typename X>
inline auto operator&(xdr<X>&& x, typename xdr<X>::template typed_function_arg<namespace_map<0>> ns_map) {
  return std::move(x)
      & xdr_constant(0u).as(earnest::xdr_uint32)
      & xdr_map(ns_map.namespaces).of(xdr_bytes, xdr_bytes)
      & xdr_set(ns_map.files);
}


} /* namespace earnest::detail */
