#pragma once

#include <cstdint>
#include <sstream>
#include <type_traits>
#include <utility>

#include <earnest/file_id.h>

namespace earnest {


struct db_address {
  db_address() = default;

  db_address(file_id file, std::uint64_t offset) noexcept(std::is_nothrow_move_constructible_v<file_id>)
  : file(std::move(file)),
    offset(std::move(offset))
  {}

  auto operator<=>(const db_address& y) const noexcept = default;

  auto to_string() const -> std::string;

  file_id file;
  std::uint64_t offset = 0;
};


template<typename CharT, typename Traits>
inline auto operator<<(std::basic_ostream<CharT, Traits>& out, const db_address& address) -> std::basic_ostream<CharT, Traits>& {
  return out << address.offset << "@" << address.file;
}

inline auto db_address::to_string() const -> std::string {
  std::ostringstream s;
  s << *this;
  return std::move(s).str();
}


} /* namespace earnest */
