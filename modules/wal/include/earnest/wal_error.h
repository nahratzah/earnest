#pragma once

#include <string>
#include <system_error>


namespace earnest {


enum class wal_errc {
  bad_version,
  memory_exhausted,
  bad_state,
  bad_read
};

auto earnest_category() -> const std::error_category& {
  class category_impl
  : public std::error_category
  {
    public:
    constexpr category_impl() noexcept = default;

    auto name() const noexcept -> const char* override {
      return "earnest::wal";
    }

    auto message(int condition) const -> std::string override {
      using namespace std::string_literals;

      switch (static_cast<wal_errc>(condition)) {
        default:
          return "unrecognized condition"s;
        case wal_errc::bad_version:
          return "bad WAL version"s;
        case wal_errc::memory_exhausted:
          return "WAL memory exhausted"s;
        case wal_errc::bad_state:
          return "WAL bad state"s;
        case wal_errc::bad_read:
          return "WAL read error"s;
      }
    }
  };

  static category_impl impl;
  return impl;
}

auto make_error_code(wal_errc e) -> std::error_code {
  return std::error_code(static_cast<int>(e), earnest_category());
}


} /* namespace earnest */

namespace std {


template<>
struct is_error_code_enum<::earnest::wal_errc>
: std::true_type
{};


} /* namespace std */
