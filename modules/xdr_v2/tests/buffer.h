#pragma once

#include <earnest/execution.h>
#include <earnest/execution_util.h>
#include <earnest/execution_io.h>

#include <ostream>
#include <vector>

class buffer {
  public:
  buffer() = default;

  buffer(std::initializer_list<std::byte> il)
  : data(il)
  {}

  buffer(std::initializer_list<std::uint8_t> il)
  : data(il.size(), std::byte{})
  {
    std::transform(il.begin(), il.end(), data.begin(),
        [](std::uint8_t x) -> std::byte {
          return static_cast<std::byte>(x);
        });
  }

  // Prohibit copying.
  buffer(const buffer&) = delete;
  buffer(buffer&&) = default;

  auto operator==(const buffer& y) const noexcept -> bool {
    return std::equal(data.begin(), data.end(), y.data.begin(), y.data.end());
  }

  friend auto operator<<(std::ostream& out, const buffer& buf) -> std::ostream& {
    print_byte_span(out, {buf.data.data(), buf.data.size()});
    return out;
  }

  friend auto tag_invoke([[maybe_unused]] earnest::execution::io::lazy_write_some_ec_t, buffer& self, std::span<const std::byte> buf) {
    return earnest::execution::just()
    | earnest::execution::lazy_then(
        [&self, buf]() {
          self.data.insert(self.data.end(), buf.begin(), buf.end());
          return buf.size();
        });
  }

  friend auto tag_invoke([[maybe_unused]] earnest::execution::io::lazy_read_some_ec_t, buffer& self, std::span<std::byte> buf) {
    return earnest::execution::just()
    | earnest::execution::lazy_then(
        [&self, buf]() {
          const auto count = std::min(self.data.size(), buf.size());
          std::copy_n(self.data.begin(), count, buf.begin());
          self.data.erase(self.data.begin(), self.data.begin() + count);
          return std::make_tuple(count, count < buf.size());
        })
    | earnest::execution::lazy_explode_tuple();
  }

  private:
  static auto print_byte_span(std::ostream& out, std::span<const std::byte> data) -> void {
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

  std::vector<std::byte> data;
};
