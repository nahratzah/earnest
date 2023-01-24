#pragma once

#include <string>
#include <system_error>


namespace earnest {


enum class file_db_errc {
  unrecoverable,
};

auto file_db_category() -> const std::error_category& {
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

      switch (static_cast<file_db_errc>(condition)) {
        default:
          return "unrecognized condition"s;
        case file_db_errc::unrecoverable:
          return "File-DB is unrecoverable"s;
      }
    }
  };

  static category_impl impl;
  return impl;
}

auto make_error_code(file_db_errc e) -> std::error_code {
  return std::error_code(static_cast<int>(e), file_db_category());
}


} /* namespace earnest */

namespace std {


template<>
struct is_error_code_enum<::earnest::file_db_errc>
: std::true_type
{};


} /* namespace std */
