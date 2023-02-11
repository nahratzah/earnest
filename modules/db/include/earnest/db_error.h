#pragma once

namespace earnest {


enum class db_errc {
  cache_collision,
};

inline auto db_category() -> const std::error_category& {
  class category_impl
  : public std::error_category
  {
    public:
    constexpr category_impl() noexcept = default;

    auto name() const noexcept -> const char* override {
      return "earnest::db";
    }

    auto message(int condition) const -> std::string override {
      using namespace std::string_literals;

      switch (static_cast<db_errc>(condition)) {
        default:
          return "unrecognized condition"s;
        case db_errc::cache_collision:
          return "DB cache collision"s;
      }
    }
  };

  static category_impl impl;
  return impl;
}

inline auto make_error_code(db_errc e) noexcept -> std::error_code {
  return std::error_code(static_cast<int>(e), db_category());
}


} /* namespace earnest */

namespace std {


template<>
struct is_error_code_enum<::earnest::db_errc>
: std::true_type
{};


} /* namespace std */
