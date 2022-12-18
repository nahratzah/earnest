#pragma once

#include <system_error>


namespace earnest {


enum class wal_errc {
  bad_version,
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
      switch (static_cast<wal_errc>(condition)) {
        default:
          return "unrecognized condition";
        case wal_errc::bad_version:
          return "bad WAL version";
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
