#pragma once

#include <string>
#include <system_error>


namespace earnest {


enum class file_db_errc {
  unrecoverable,
  lock_failure,
  read_not_permitted,
  write_not_permitted,
  write_past_eof,
  closing,
};

inline auto file_db_category() -> const std::error_category& {
  class category_impl
  : public std::error_category
  {
    public:
    constexpr category_impl() noexcept = default;

    auto name() const noexcept -> const char* override {
      return "earnest::file_db";
    }

    auto message(int condition) const -> std::string override {
      using namespace std::string_literals;

      switch (static_cast<file_db_errc>(condition)) {
        default:
          return "unrecognized condition"s;
        case file_db_errc::unrecoverable:
          return "File-DB is unrecoverable"s;
        case file_db_errc::lock_failure:
          return "File-DB unable to lock"s;
        case file_db_errc::read_not_permitted:
          return "File-DB transaction does not have read permission"s;
        case file_db_errc::write_not_permitted:
          return "File-DB transaction does not have write permission"s;
        case file_db_errc::write_past_eof:
          return "File-DB write past extends past end-of-file"s;
        case file_db_errc::closing:
          return "File-DB closing"s;
      }
    }
  };

  static category_impl impl;
  return impl;
}

inline auto make_error_code(file_db_errc e) -> std::error_code {
  return std::error_code(static_cast<int>(e), file_db_category());
}


} /* namespace earnest */

namespace std {


template<>
struct is_error_code_enum<::earnest::file_db_errc>
: std::true_type
{};


} /* namespace std */
