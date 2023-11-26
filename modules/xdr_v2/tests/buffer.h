#pragma once

#include <earnest/execution.h>
#include <earnest/execution_util.h>
#include <earnest/execution_io.h>

#include <iosfwd>
#include <vector>

class buffer {
  public:
  buffer() = default;
  buffer(std::initializer_list<std::byte> il);
  buffer(std::initializer_list<std::uint8_t> il);

  // Prohibit copying.
  buffer(const buffer&) = delete;
  buffer(buffer&&) = default;
  buffer& operator=(const buffer&) = delete;
  buffer& operator=(buffer&&) = default;

  auto operator==(const buffer& y) const noexcept -> bool;
  auto operator!=(const buffer& y) const noexcept -> bool { return !(*this == y); }

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
          if (self.data.empty() && buf.size() != 0)
            throw std::system_error(std::error_code(::earnest::execution::io::errc::eof));

          const auto count = std::min(self.data.size(), buf.size());
          std::copy_n(self.data.begin(), count, buf.begin());
          self.data.erase(self.data.begin(), self.data.begin() + count);
          return count;
        })
    | earnest::execution::system_error_to_error_code();
  }

  friend auto tag_invoke([[maybe_unused]] earnest::execution::io::lazy_write_ec_t, buffer& self, std::span<const std::byte> buf, [[maybe_unused]] std::optional<std::size_t> minbytes) {
    return earnest::execution::just()
    | earnest::execution::lazy_then(
        [&self, buf, minbytes]() {
          self.data.insert(self.data.end(), buf.begin(), buf.end());
          return buf.size();
        });
  }

  friend auto tag_invoke([[maybe_unused]] earnest::execution::io::lazy_read_ec_t, buffer& self, std::span<std::byte> buf, std::optional<std::size_t> minbytes) {
    return earnest::execution::just()
    | earnest::execution::lazy_then(
        [&self, buf, minbytes=minbytes.value_or(buf.size())]() {
          if (self.data.size() < minbytes)
            throw std::system_error(std::error_code(::earnest::execution::io::errc::eof));

          const auto count = std::min(self.data.size(), buf.size());
          std::copy_n(self.data.begin(), count, buf.begin());
          self.data.erase(self.data.begin(), self.data.begin() + count);
          return count;
        })
    | earnest::execution::system_error_to_error_code();
  }

  private:
  static auto print_byte_span(std::ostream& out, std::span<const std::byte> data) -> void;

  std::vector<std::byte> data;
};
