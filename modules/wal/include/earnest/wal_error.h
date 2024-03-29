#pragma once

#include <string>
#include <system_error>


namespace earnest {


enum class wal_errc {
  bad_version=1,
  memory_exhausted,
  bad_state,
  bad_read,
  no_data_files,
  unable_to_lock,
  recovery_failure,
  unrecoverable
};

inline auto earnest_category() -> const std::error_category& {
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
        case wal_errc::no_data_files:
          return "WAL has no data files"s;
        case wal_errc::unable_to_lock:
          return "WAL cannot be locked"s;
        case wal_errc::recovery_failure:
          return "WAL recovery failed"s;
        case wal_errc::unrecoverable:
          return "WAL is unrecoverable"s;
      }
    }
  };

  static category_impl impl;
  return impl;
}

inline auto make_error_code(wal_errc e) noexcept -> std::error_code {
  return std::error_code(static_cast<int>(e), earnest_category());
}


} /* namespace earnest */

namespace std {


template<>
struct is_error_code_enum<::earnest::wal_errc>
: std::true_type
{};


} /* namespace std */
