#pragma once

#include <iosfwd>
#include <string_view>

#include <spdlog/spdlog.h>

namespace earnest {


/**
 * \brief Transaction isolation levels.
 * \details
 * Describes the isolation level of a transaction.
 */
enum class isolation {
  /**
   * \brief Read-commited transaction isolation.
   * \details
   * Read commited data. New commits become visible immediately.
   */
  read_commited,

  /**
   * \brief Repeatable-read transaction isolation.
   * \details
   * Point-in-time read. New commits won't be visible.
   */
  repeatable_read,

  /**
   * \brief Serializable transaction isolation.
   * \details
   * Point-in-time read. New commits won't be visible.
   *
   * Additionally, the transaction will fail to commit,
   * if any of its reads or writes has been written to
   * by another transaction.
   */
  serializable
};


template<typename Char, typename Traits>
auto operator<<(std::basic_ostream<Char, Traits>& out, isolation i) -> std::basic_ostream<Char, Traits>& {
  switch (i) {
    case isolation::read_commited:
      out << "read-committed";
      break;
    case isolation::repeatable_read:
      out << "repeatable-read";
      break;
    case isolation::serializable:
      out << "serializable";
      break;
  }
  return out;
}


} /* namespace earnest */

namespace fmt {


template<>
struct formatter<earnest::isolation>
: formatter<std::string>
{
  auto format(earnest::isolation i, format_context& ctx) -> decltype(ctx.out()) {
    using namespace std::literals;

    std::string_view isv;
    switch (i) {
      default: [[unlikely]]
        return format_to(ctx.out(), "isolation:{}", static_cast<std::underlying_type_t<earnest::isolation>>(i));
      case earnest::isolation::read_commited:
        isv = "read-committed"sv;
        break;
      case earnest::isolation::repeatable_read:
        isv = "repeatable-read"sv;
        break;
      case earnest::isolation::serializable:
        isv = "serializable"sv;
        break;
    }
    return format_to(ctx.out(), "{}", isv);
  }
};


} /* namespace spdlog::fmt */
