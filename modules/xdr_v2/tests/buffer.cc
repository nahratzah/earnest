#include "buffer.h"

#include <algorithm>
#include <ostream>

buffer::buffer(std::initializer_list<std::byte> il)
: data(il)
{}

buffer::buffer(std::initializer_list<std::uint8_t> il)
: data(il.size(), std::byte{})
{
  std::transform(il.begin(), il.end(), data.begin(),
      [](std::uint8_t x) -> std::byte {
        return static_cast<std::byte>(x);
      });
}

auto buffer::operator==(const buffer& y) const noexcept -> bool {
  return std::equal(data.begin(), data.end(), y.data.begin(), y.data.end());
}

auto buffer::print_byte_span(std::ostream& out, std::span<const std::byte> data) -> void {
  static constexpr std::array<char, 16> hex_chars = { '0', '1', '2', '3',
                                                      '4', '5', '6', '7',
                                                      '8', '9', 'a', 'b',
                                                      'c', 'd', 'e', 'f' };
  bool first = true;
  std::for_each(data.begin(), data.end(),
      [&](std::byte b) {
        out.put(std::exchange(first, false) ? '[' : ' ');
        const std::uint8_t bval = static_cast<std::uint8_t>(b);
        out.put(hex_chars[bval / hex_chars.size()]);
        out.put(hex_chars[bval % hex_chars.size()]);
      });
  if (first) out.put('[');
  out.put(']');
}
