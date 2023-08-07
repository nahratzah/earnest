#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>

#include <asio/async_result.hpp>
#include <asio/buffer.hpp>
#include <asio/read_at.hpp>
#include <boost/intrusive/set.hpp>

#include <earnest/detail/wal_records.h>
#include <earnest/fd.h>

namespace earnest::detail {


using replacement_map_value_hook_ = boost::intrusive::set_base_hook<boost::intrusive::optimize_size<true>>;


template<typename FD>
class replacement_map_value
: public replacement_map_value_hook_
{
  public:
  using offset_type = std::uint64_t;
  using size_type = std::uint64_t;

  replacement_map_value() noexcept = default;

  template<typename Executor, typename Reactor>
  replacement_map_value(const wal_record_modify_file32<Executor, Reactor>& record) noexcept
  : file_offset(record.file_offset),
    wal_offset_(record.wal_offset),
    len(record.wal_len),
    wal_file_(record.wal_file)
  {}

  template<typename Executor, typename Reactor>
  replacement_map_value(wal_record_modify_file32<Executor, Reactor>&& record) noexcept
  : file_offset(record.file_offset),
    wal_offset_(record.wal_offset),
    len(record.wal_len),
    wal_file_(std::move(record.wal_file))
  {}

  replacement_map_value(offset_type file_offset, offset_type wal_offset, size_type len, std::shared_ptr<const FD> wal_file) noexcept
  : file_offset(file_offset),
    wal_offset_(wal_offset),
    len(len),
    wal_file_(std::move(wal_file))
  {}

  auto offset() const noexcept -> offset_type { return file_offset; }
  auto size() const noexcept -> size_type { return len; }
  auto end_offset() const noexcept -> offset_type { return offset() + size(); }
  auto wal_offset() const noexcept -> offset_type { return wal_offset_; }
  auto wal_file() const noexcept -> const FD& { return *wal_file_; }

  void start_at(offset_type new_off) {
    if (new_off < offset() || new_off > end_offset())
      throw std::invalid_argument("replacement_map start_at invalid position");

    const size_type shift = new_off - file_offset;
    len -= shift;
    file_offset += shift;
    wal_offset_ += shift;
  }

  void truncate(size_type new_sz) {
    if (new_sz > size())
      throw std::invalid_argument("replacement truncate may not grow size");

    len = new_sz;
  }

  private:
  offset_type file_offset = 0, wal_offset_ = 0;
  size_type len = 0;
  std::shared_ptr<const FD> wal_file_;
};


/**
 * \brief Record file changes.
 * \details
 * A replacement map holds on to file changes in memory and allows for them to
 * be applied during reads.
 *
 * \tparam Alloc An allocator type.
 */
template<typename FD, typename Alloc = std::allocator<std::byte>>
class replacement_map {
  public:
  using fd_type = FD;
  using value_type = replacement_map_value<fd_type>;
  using reference = value_type&;
  using const_reference = const value_type&;
  using pointer = value_type*;
  using const_pointer = const value_type*;
  using allocator_type = typename std::allocator_traits<Alloc>::template rebind_alloc<value_type>;

  using offset_type = typename value_type::offset_type;
  using size_type = typename value_type::size_type;

  private:
  using alloc_traits = std::allocator_traits<allocator_type>;
  using map_type = std::vector<value_type, typename alloc_traits::template rebind_alloc<value_type>>;

  public:
  using iterator = typename map_type::const_iterator;
  using const_iterator = iterator;

  explicit replacement_map(allocator_type alloc = allocator_type())
      noexcept(std::is_nothrow_move_constructible_v<allocator_type>)
  : map_(std::move(alloc))
  {}

  replacement_map(const replacement_map& other)
      noexcept(std::is_nothrow_copy_constructible_v<map_type>)
  : map_(other.map_)
  {}

  replacement_map(replacement_map&& other)
      noexcept(std::is_nothrow_move_constructible_v<map_type>)
  : map_(std::move(other.map_))
  {}

  auto operator=(const replacement_map& other) -> replacement_map& {
    map_ = other.map_;
    return *this;
  }

  auto operator=(replacement_map&& other) noexcept -> replacement_map& {
    map_ = std::move(other.map_);
    return *this;
  }

  void swap(replacement_map& other) noexcept {
    map_.swap(other.map_);
  }

  auto get_allocator() const -> allocator_type { return map_.get_allocator(); }
  auto empty() const noexcept -> bool { return map_.empty(); }

  void clear() { map_.clear(); }
  template<typename... Args>
  auto insert(Args&&... args) -> iterator { return insert_(value_type(std::forward<Args>(args)...)); }

  auto erase(offset_type begin_off, offset_type end_off) -> iterator {
    if (begin_off > end_off)
      throw std::invalid_argument("range must go from low to high offsets");
    return erase_(begin_off, end_off);
  }

  void truncate(offset_type new_sz) noexcept { erase(new_sz, std::numeric_limits<offset_type>::max()); }

  auto begin() -> iterator { return map_.begin(); }
  auto end() -> iterator { return map_.end(); }
  auto begin() const -> const_iterator { return map_.begin(); }
  auto end() const -> const_iterator { return map_.end(); }
  auto cbegin() const -> const_iterator { return begin(); }
  auto cend() const -> const_iterator { return end(); }

  template<typename OtherAlloc>
  auto merge(const replacement_map<fd_type, OtherAlloc>& other) -> replacement_map& {
    std::for_each(other.begin(), other.end(),
        [this](const_reference v) {
          this->insert_(v);
        });
    return *this;
  }

  auto merge(replacement_map&& other) noexcept -> replacement_map& {
    std::for_each(
        std::make_move_iterator(other.begin()), std::make_move_iterator(other.end()),
        [this](value_type&& value) {
          this->insert_(std::move(value));
        });
    other.map_.clear();
    return *this;
  }

  ///\brief Find the first element at-or-after offset \p off .
  auto find(offset_type off) -> iterator {
    const auto comparator = overload(
        [](const value_type& x, offset_type y) -> bool {
          return x.offset() < y;
        },
        [](offset_type x, const value_type& y) -> bool {
          return x < y.offset();
        });

    auto iter = std::lower_bound(map_.begin(), map_.end(), off, comparator);
    if (iter != map_.begin()) {
      auto before = std::prev(iter);
      if (before->end_offset() > off) return before;
    }
    return iter;
  }

  ///\brief Find the first element at-or-after offset \p off .
  auto find(offset_type off) const -> const_iterator {
    const auto comparator = overload(
        [](const value_type& x, offset_type y) -> bool {
          return x.offset() < y;
        },
        [](offset_type x, const value_type& y) -> bool {
          return x < y.offset();
        });

    auto iter = std::lower_bound(map_.begin(), map_.end(), off, comparator);
    if (iter != map_.begin()) {
      auto before = std::prev(iter);
      if (before->end_offset() > off) return before;
    }
    return iter;
  }

  private:
  auto insert_(value_type value) -> iterator {
    if (value.size() == 0) return map_.end(); // skip insert of empty entries

    erase_(value.offset(), value.end_offset());
    auto insert_position = std::upper_bound(
        map_.cbegin(), map_.cend(),
        value,
        [](const value_type& x, const value_type& y) -> bool {
          return x.offset() < y.offset();
        });

    return map_.insert(insert_position, std::move(value));
  }

  auto erase_(offset_type begin_offset, offset_type end_offset) -> iterator {
    assert(begin_offset <= end_offset);

    const auto comparator = overload(
        [](const value_type& x, offset_type y) -> bool {
          return x.offset() < y;
        },
        [](offset_type x, const value_type& y) -> bool {
          return x < y.offset();
        });

    auto begin = std::lower_bound(map_.begin(), map_.end(), begin_offset, comparator);
    if (begin != map_.begin()) {
      auto before = std::prev(begin);
      if (before->end_offset() > begin_offset) {
        value_type sibling = *before;
        sibling.start_at(begin_offset); // if this throws, this function is buggy

        begin = map_.insert(begin, sibling); // might throw if we run out of memory
        std::prev(begin)->truncate(begin_offset - before->offset()); // if this throws, this function is buggy
      }
    }

    auto end = std::lower_bound(map_.begin(), map_.end(), end_offset, comparator);
    if (end != map_.begin()) {
      auto before = std::prev(end);
      if (before->end_offset() > end_offset) {
        before->start_at(end_offset);
        end = before;
      }
    }

    return map_.erase(begin, end);
  }

  map_type map_;
};


template<typename FD, typename Alloc>
void swap(replacement_map<FD, Alloc>& x, replacement_map<FD, Alloc>& y) noexcept {
  x.swap(y);
}


template<typename XFD, typename XAlloc, typename YFD, typename YAlloc>
inline auto operator==(const replacement_map<XFD, XAlloc>& x, const replacement_map<YFD, YAlloc>& y) -> bool {
  auto x_iter = x.begin(), y_iter = y.begin();
  const auto x_end = x.end(), y_end = y.end();

  if (x_iter == x_end || y_iter == y_end)
    return x_iter == x_end && y_iter == y_end;

  auto x_off = x_iter->offset(), y_off = y_iter->offset();
  std::vector<std::byte> x_buf_spc(65536), y_buf_spc(65536);
  asio::mutable_buffer x_buf = asio::buffer(x_buf_spc, 0), y_buf = asio::buffer(y_buf_spc, 0);

  do {
    bool skip = false;
    // Fill x-buffer.
    if (x_buf.size() == 0) {
      if (x_off == x_iter->end_offset()) {
        ++x_iter;
        if (x_iter != x_end) x_off = x_iter->offset();
        skip = true;
      } else {
        x_buf = asio::buffer(x_buf_spc, x_iter->end_offset() - x_off);
        asio::read_at(*x_iter, x_off, x_buf, asio::transfer_at_least(1));
      }
    }
    // Fill y-buffer.
    if (y_buf.size() == 0) {
      if (y_off == y_iter->end_offset()) {
        ++y_iter;
        if (y_iter != y_end) y_off = y_iter->offset();
        skip = true;
      } else {
        y_buf = asio::buffer(y_buf_spc, y_iter->end_offset() - y_off);
        asio::read_at(*y_iter, y_off, y_buf, asio::transfer_at_least(1));
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
    x_buf += buflen;
    y_buf += buflen;
  } while (x_iter != x_end && y_iter != y_end);

  return x_buf.size() == 0 && y_buf.size() == 0 && x_iter == x_end && y_iter == y_end;
}

template<typename XFD, typename XAlloc, typename YFD, typename YAlloc>
inline auto operator!=(const replacement_map<XFD, XAlloc>& x, const replacement_map<YFD, YAlloc>& y) -> bool {
  return !(x == y);
}


} /* namespace earnest::detail */
