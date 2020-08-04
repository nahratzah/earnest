#ifndef EARNEST_TXFILE_INL_H
#define EARNEST_TXFILE_INL_H

#include <boost/system/system_error.hpp>
#include <algorithm>

namespace earnest {


inline auto txfile::get_allocator() const -> allocator_type {
  if (pimpl_ != nullptr) return pimpl_->wal_.get_allocator();
  return {};
}


template<typename MB>
inline auto txfile::transaction::read_some_at(offset_type off, MB&& mb) const -> std::size_t {
  boost::system::error_code ec;
  std::size_t s = read_some_at(off, std::forward<MB>(mb), ec);
  if (ec) throw boost::system::system_error(ec, "read");
  return s;
}

template<typename MB>
inline auto txfile::transaction::read_some_at(offset_type off, MB&& mb, boost::system::error_code& ec) const -> std::size_t {
  ec.clear();

  auto non_empty_buf = std::find_if(
      boost::asio::buffer_sequence_begin(mb),
      boost::asio::buffer_sequence_end(mb),
      [](const boost::asio::mutable_buffer& buf) -> bool {
        return buf.size() > 0;
      });
  if (non_empty_buf == boost::asio::buffer_sequence_end(mb)) return 0;

  return read_at_(off, non_empty_buf->data(), non_empty_buf->size(), ec);
}

template<typename MB>
inline auto txfile::transaction::write_some_at(offset_type off, MB&& mb) -> std::size_t {
  boost::system::error_code ec;
  std::size_t s = write_some_at(off, std::forward<MB>(mb), ec);
  if (ec) throw boost::system::system_error(ec, "read");
  return s;
}

template<typename MB>
inline auto txfile::transaction::write_some_at(offset_type off, MB&& mb, boost::system::error_code& ec) -> std::size_t {
  ec.clear();

  auto non_empty_buf = std::find_if(
      boost::asio::buffer_sequence_begin(mb),
      boost::asio::buffer_sequence_end(mb),
      [](const boost::asio::const_buffer& buf) -> bool {
        return buf.size() > 0;
      });
  if (non_empty_buf == boost::asio::buffer_sequence_end(mb)) return 0;

  // XXX pass ec forward
  return write_at(off, non_empty_buf->data(), non_empty_buf->size());
}


} /* namespace earnest */

#endif /* EARNEST_TXFILE_INL_H */
