#pragma once

#include <type_traits>
#include <string_view>

#include <spdlog/spdlog.h>

namespace earnest {


/**
 * \brief Transaction read/write mode.
 * \details
 * Controls if a transaction is read-only, write-only, or read-write.
 */
enum class tx_mode {
  /**
   * \brief Decline transaction mode.
   * \details
   * Reads will fail.
   * Writes will fail.
   */
  decline = 0,

  /**
   * \brief Read-only transaction mode.
   * \details
   * Reads are permitted.
   * Writes will fail.
   */
  read_only = 1,

  /**
   * \brief Write-only transaction mode.
   * \details
   * Reads will fail.
   * Writes are permitted.
   */
  write_only = 2,

  /**
   * \brief Write-only transaction mode.
   * \details
   * Reads are permitted.
   * Writes are permitted.
   */
  read_write = 3
};


template<typename Traits>
inline auto operator<<(std::basic_ostream<char, Traits>& o, tx_mode m) -> std::basic_ostream<char, Traits>& {
  using namespace std::literals;

  switch (m) {
    case tx_mode::decline:
      o << "tx_mode::decline"sv;
      break;
    case tx_mode::read_only:
      o << "tx_mode::read_only"sv;
      break;
    case tx_mode::write_only:
      o << "tx_mode::write_only"sv;
      break;
    case tx_mode::read_write:
      o << "tx_mode::read_write"sv;
      break;
  }
  return o;
}

template<typename Traits>
inline auto operator<<(std::basic_ostream<wchar_t, Traits>& o, tx_mode m) -> std::basic_ostream<wchar_t, Traits>& {
  using namespace std::literals;

  switch (m) {
    case tx_mode::decline:
      o << L"tx_mode::decline"sv;
      break;
    case tx_mode::read_only:
      o << L"tx_mode::read_only"sv;
      break;
    case tx_mode::write_only:
      o << L"tx_mode::write_only"sv;
      break;
    case tx_mode::read_write:
      o << L"tx_mode::read_write"sv;
      break;
  }
  return o;
}

template<typename Traits>
inline auto operator<<(std::basic_ostream<char8_t, Traits>& o, tx_mode m) -> std::basic_ostream<char8_t, Traits>& {
  using namespace std::literals;

  switch (m) {
    case tx_mode::decline:
      o << u8"tx_mode::decline"sv;
      break;
    case tx_mode::read_only:
      o << u8"tx_mode::read_only"sv;
      break;
    case tx_mode::write_only:
      o << u8"tx_mode::write_only"sv;
      break;
    case tx_mode::read_write:
      o << u8"tx_mode::read_write"sv;
      break;
  }
  return o;
}

template<typename Traits>
inline auto operator<<(std::basic_ostream<char16_t, Traits>& o, tx_mode m) -> std::basic_ostream<char16_t, Traits>& {
  using namespace std::literals;

  switch (m) {
    case tx_mode::decline:
      o << u"tx_mode::decline"sv;
      break;
    case tx_mode::read_only:
      o << u"tx_mode::read_only"sv;
      break;
    case tx_mode::write_only:
      o << u"tx_mode::write_only"sv;
      break;
    case tx_mode::read_write:
      o << u"tx_mode::read_write"sv;
      break;
  }
  return o;
}

template<typename Traits>
inline auto operator<<(std::basic_ostream<char32_t, Traits>& o, tx_mode m) -> std::basic_ostream<char32_t, Traits>& {
  using namespace std::literals;

  switch (m) {
    case tx_mode::decline:
      o << U"tx_mode::decline"sv;
      break;
    case tx_mode::read_only:
      o << U"tx_mode::read_only"sv;
      break;
    case tx_mode::write_only:
      o << U"tx_mode::write_only"sv;
      break;
    case tx_mode::read_write:
      o << U"tx_mode::read_write"sv;
      break;
  }
  return o;
}


///\brief Test if \p m allows for reading.
constexpr auto read_permitted(tx_mode m) noexcept -> bool {
  return m == tx_mode::read_only || m == tx_mode::read_write;
}

///\brief Test if \p m allows for writing.
constexpr auto write_permitted(tx_mode m) noexcept -> bool {
  return m == tx_mode::write_only || m == tx_mode::read_write;
}

constexpr auto operator&(tx_mode x, tx_mode y) noexcept -> tx_mode {
  std::underlying_type_t<tx_mode> x_val = static_cast<std::underlying_type_t<tx_mode>>(x);
  std::underlying_type_t<tx_mode> y_val = static_cast<std::underlying_type_t<tx_mode>>(y);
  return static_cast<tx_mode>(x_val & y_val);
}

constexpr auto operator|(tx_mode x, tx_mode y) noexcept -> tx_mode {
  std::underlying_type_t<tx_mode> x_val = static_cast<std::underlying_type_t<tx_mode>>(x);
  std::underlying_type_t<tx_mode> y_val = static_cast<std::underlying_type_t<tx_mode>>(y);
  return static_cast<tx_mode>(x_val | y_val);
}

constexpr auto operator~(tx_mode x) noexcept -> tx_mode {
  std::underlying_type_t<tx_mode> x_val = static_cast<std::underlying_type_t<tx_mode>>(x);
  std::underlying_type_t<tx_mode> mask = 3;
  return static_cast<tx_mode>(mask & ~x_val);
}


} /* namespace earnest */

namespace fmt {


template<>
struct formatter<earnest::tx_mode>
: formatter<std::string>
{
  auto format(earnest::tx_mode i, format_context& ctx) -> decltype(ctx.out()) {
    using namespace std::literals;

    std::string_view isv;
    switch (i) {
      default: [[unlikely]]
        return fmt::format_to(ctx.out(), "tx-mode:0x{:x}", static_cast<std::underlying_type_t<earnest::tx_mode>>(i));
      case earnest::tx_mode::decline:
        isv = "tx-mode-decline"sv;
        break;
      case earnest::tx_mode::write_only:
        isv = "write-only"sv;
        break;
      case earnest::tx_mode::read_only:
        isv = "read-only"sv;
        break;
      case earnest::tx_mode::read_write:
        isv = "read-write"sv;
        break;
    }
    return fmt::format_to(ctx.out(), "{}", isv);
  }
};


} /* namespace spdlog::fmt */
