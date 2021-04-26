#include <earnest/detail/replacement_map.h>

#include <algorithm>
#include <cstring>

namespace earnest::detail {


auto equal_(
  const boost::intrusive::set<
      replacement_map_value,
      boost::intrusive::base_hook<replacement_map_value_hook_>,
      boost::intrusive::constant_time_size<false>,
      boost::intrusive::key_of_value<rmv_key_extractor_>>& x,
  const boost::intrusive::set<
      replacement_map_value,
      boost::intrusive::base_hook<replacement_map_value_hook_>,
      boost::intrusive::constant_time_size<false>,
      boost::intrusive::key_of_value<rmv_key_extractor_>>& y) noexcept
-> bool {
  auto x_iter = x.begin(), y_iter = y.begin();
  const auto x_end = x.end(), y_end = y.end();

  if (x_iter == x_end || y_iter == y_end)
    return x_iter == x_end && y_iter == y_end;

  auto x_off = x_iter->offset(), y_off = y_iter->offset();
  asio::const_buffer x_buf = x_iter->buffer(), y_buf = y_iter->buffer();

  do {
    assert(x_buf.size() > 0 && y_buf.size() > 0);

    const auto buflen = std::min(x_buf.size(), y_buf.size());
    if (x_off != y_off) return false;
    if (std::memcmp(x_buf.data(), y_buf.data(), buflen) != 0) return false;

    x_off += buflen;
    y_off += buflen;
    x_buf += buflen;
    y_buf += buflen;

    if (x_buf.size() == 0) {
      ++x_iter;
      if (x_iter != x_end) {
        x_off = x_iter->offset();
        x_buf = x_iter->buffer();
      }
    }

    if (y_buf.size() == 0) {
      ++y_iter;
      if (y_iter != y_end) {
        y_off = y_iter->offset();
        y_buf = y_iter->buffer();
      }
    }
  } while (x_iter != x_end && y_iter != y_end);

  return x_buf.size() == 0 && y_buf.size() == 0 && x_iter == x_end && y_iter == y_end;
}


template class replacement_map<>;


} /* namespace earnest::detail */
