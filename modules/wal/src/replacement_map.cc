#include <earnest/detail/replacement_map.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstring>
#include <span>

namespace earnest::detail {


auto replacement_map_value::cmp_eq(std::span<const replacement_map_value> x, std::span<const replacement_map_value> y) -> bool {
  using namespace execution;

  static constexpr std::size_t bufsiz = 65536;
  auto x_iter = x.begin(), y_iter = y.begin();
  const auto x_end = x.end(), y_end = y.end();

  if (x_iter == x_end || y_iter == y_end)
    return x_iter == x_end && y_iter == y_end;

  auto x_off = x_iter->offset(), y_off = y_iter->offset();
  std::array<std::byte, bufsiz> x_buf_spc, y_buf_spc;
  std::span<std::byte> x_buf, y_buf;

  do {
    bool skip = false;
    // Fill x-buffer.
    if (x_buf.size() == 0) {
      if (x_off == x_iter->end_offset()) {
        ++x_iter;
        if (x_iter != x_end) x_off = x_iter->offset();
        skip = true;
      } else {
        x_buf = x_buf_spc;
        if (x_buf.size() > x_iter->end_offset() - x_off) x_buf.subspan(0, x_iter->end_offset() - x_off);
        auto wal_offset = x_iter->wal_offset() + (x_off - x_iter->offset());
        auto [rlen] = sync_wait(io::read_some_at(x_iter->wal_file(), wal_offset, x_buf)).value();
        x_buf = x_buf.subspan(0, rlen);
      }
    }
    // Fill y-buffer.
    if (y_buf.size() == 0) {
      if (y_off == y_iter->end_offset()) {
        ++y_iter;
        if (y_iter != y_end) y_off = y_iter->offset();
        skip = true;
      } else {
        y_buf = y_buf_spc;
        if (y_buf.size() > y_iter->end_offset() - y_off) y_buf = y_buf.subspan(0, y_iter->end_offset() - y_off);
        auto wal_offset = y_iter->wal_offset() + (y_off - y_iter->offset());
        auto [rlen] = sync_wait(io::read_some_at(y_iter->wal_file(), wal_offset, y_buf)).value();
        y_buf = y_buf.subspan(0, rlen);
      }
    }
    // Restart the loop if we changed iterator position (and thus didn't fill the buffers).
    if (skip) continue;

    assert(x_buf.size() > 0 && y_buf.size() > 0);

    const auto buflen = std::min(x_buf.size(), y_buf.size());
    if (x_off != y_off) return false;
    if (std::memcmp(x_buf.data(), y_buf.data(), buflen) != 0) return false;

    x_off += buflen;
    y_off += buflen;
    x_buf = x_buf.subspan(buflen);
    y_buf = y_buf.subspan(buflen);
  } while (x_iter != x_end && y_iter != y_end);

  return x_buf.empty() && y_buf.empty() && x_iter == x_end && y_iter == y_end;
}


} /* namespace earnest::detail */
