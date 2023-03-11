#pragma once

#include <cstdint>
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

  file_id file;
  std::uint64_t offset = 0;
};


} /* namespace earnest */
