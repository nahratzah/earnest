#pragma once

#include <array>
#include <cstddef>
#include <iosfwd>
#include <span>
#include <string_view>
#include <type_traits>

namespace earnest {


class byte_span_printer {
  public:
  template<std::size_t Extent>
  explicit byte_span_printer(std::span<const std::byte, Extent> span) noexcept
  : span(span)
  {}

  template<std::size_t Extent>
  explicit byte_span_printer(std::span<std::byte, Extent> span) noexcept
  : span(span)
  {}

  template<typename Traits>
  friend auto operator<<(std::basic_ostream<char, Traits>& out, const byte_span_printer& p) -> std::basic_ostream<char, Traits>& {
    using namespace std::literals;

    static constexpr std::string_view prefix = "0x"sv;
    static constexpr std::array<std::string_view, 16> identifiers = {
      "0"sv, "1"sv, "2"sv, "3"sv, "4"sv, "5"sv, "6"sv, "7"sv,
      "8"sv, "9"sv, "a"sv, "b"sv, "c"sv, "d"sv, "e"sv, "f"sv,
    };

    auto print = [&out](std::byte x) -> void {
      out << prefix
          << identifiers[static_cast<std::underlying_type_t<std::byte>>(x) / 16u]
          << identifiers[static_cast<std::underlying_type_t<std::byte>>(x) % 16u];
    };

    if (p.span.empty()) {
      out << "[]"sv;
    } else {
      out << "[ "sv;
      print(p.span[0]);
      for (const std::byte& b : p.span.subspan(1)) {
        out << ", "sv;
        print(b);
      }
      out << " ]"sv;
    }

    return out;
  }

  template<typename Traits>
  friend auto operator<<(std::basic_ostream<wchar_t, Traits>& out, const byte_span_printer& p) -> std::basic_ostream<wchar_t, Traits>& {
    using namespace std::literals;

    static constexpr std::wstring_view prefix = L"0x"sv;
    static constexpr std::array<std::wstring_view, 16> identifiers = {
      L"0"sv, L"1"sv, L"2"sv, L"3"sv, L"4"sv, L"5"sv, L"6"sv, L"7"sv,
      L"8"sv, L"9"sv, L"a"sv, L"b"sv, L"c"sv, L"d"sv, L"e"sv, L"f"sv,
    };

    auto print = [&out](std::byte x) -> void {
      out << prefix
          << identifiers[static_cast<std::underlying_type_t<std::byte>>(x) / 16u]
          << identifiers[static_cast<std::underlying_type_t<std::byte>>(x) % 16u];
    };

    if (p.span.empty()) {
      out << L"[]"sv;
    } else {
      out << L"[ "sv;
      print(p.span[0]);
      for (const std::byte& b : p.span.subspan(1)) {
        out << L", "sv;
        print(b);
      }
      out << L" ]"sv;
    }

    return out;
  }

  template<typename Traits>
  friend auto operator<<(std::basic_ostream<char8_t, Traits>& out, const byte_span_printer& p) -> std::basic_ostream<char8_t, Traits>& {
    using namespace std::literals;

    static constexpr std::u8string_view prefix = u8"0x"sv;
    static constexpr std::array<std::u8string_view, 16> identifiers = {
      u8"0"sv, u8"1"sv, u8"2"sv, u8"3"sv, u8"4"sv, u8"5"sv, u8"6"sv, u8"7"sv,
      u8"8"sv, u8"9"sv, u8"a"sv, u8"b"sv, u8"c"sv, u8"d"sv, u8"e"sv, u8"f"sv,
    };

    auto print = [&out](std::byte x) -> void {
      out << prefix
          << identifiers[static_cast<std::underlying_type_t<std::byte>>(x) / 16u]
          << identifiers[static_cast<std::underlying_type_t<std::byte>>(x) % 16u];
    };

    if (p.span.empty()) {
      out << u8"[]"sv;
    } else {
      out << u8"[ "sv;
      print(p.span[0]);
      for (const std::byte& b : p.span.subspan(1)) {
        out << u8", "sv;
        print(b);
      }
      out << u8" ]"sv;
    }

    return out;
  }

  template<typename Traits>
  friend auto operator<<(std::basic_ostream<char16_t, Traits>& out, const byte_span_printer& p) -> std::basic_ostream<char16_t, Traits>& {
    using namespace std::literals;

    static constexpr std::u16string_view prefix = u"0x"sv;
    static constexpr std::array<std::u16string_view, 16> identifiers = {
      u"0"sv, u"1"sv, u"2"sv, u"3"sv, u"4"sv, u"5"sv, u"6"sv, u"7"sv,
      u"8"sv, u"9"sv, u"a"sv, u"b"sv, u"c"sv, u"d"sv, u"e"sv, u"f"sv,
    };

    auto print = [&out](std::byte x) -> void {
      out << prefix
          << identifiers[static_cast<std::underlying_type_t<std::byte>>(x) / 16u]
          << identifiers[static_cast<std::underlying_type_t<std::byte>>(x) % 16u];
    };

    if (p.span.empty()) {
      out << u"[]"sv;
    } else {
      out << u"[ "sv;
      print(p.span[0]);
      for (const std::byte& b : p.span.subspan(1)) {
        out << u", "sv;
        print(b);
      }
      out << u" ]"sv;
    }

    return out;
  }

  template<typename Traits>
  friend auto operator<<(std::basic_ostream<char32_t, Traits>& out, const byte_span_printer& p) -> std::basic_ostream<char32_t, Traits>& {
    using namespace std::literals;

    static constexpr std::u32string_view prefix = U"0x"sv;
    static constexpr std::array<std::u32string_view, 16> identifiers = {
      U"0"sv, U"1"sv, U"2"sv, U"3"sv, U"4"sv, U"5"sv, U"6"sv, U"7"sv,
      U"8"sv, U"9"sv, U"a"sv, U"b"sv, U"c"sv, U"d"sv, U"e"sv, U"f"sv,
    };

    auto print = [&out](std::byte x) -> void {
      out << prefix
          << identifiers[static_cast<std::underlying_type_t<std::byte>>(x) / 16u]
          << identifiers[static_cast<std::underlying_type_t<std::byte>>(x) % 16u];
    };

    if (p.span.empty()) {
      out << U"[]"sv;
    } else {
      out << U"[ "sv;
      print(p.span[0]);
      for (const std::byte& b : p.span.subspan(1)) {
        out << U", "sv;
        print(b);
      }
      out << U" ]"sv;
    }

    return out;
  }

  private:
  std::span<const std::byte> span;
};


} /* namespace earnest */
