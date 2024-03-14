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

#include <earnest/detail/wal_records.h>
#include <earnest/execution_io.h>

namespace earnest::detail {


class replacement_map_value {
  public:
  using offset_type = std::uint64_t;
  using size_type = std::uint64_t;

  replacement_map_value() noexcept = default;

  replacement_map_value(offset_type file_offset, offset_type wal_offset, size_type len, gsl::not_null<std::shared_ptr<execution::io::type_erased_at_readable>> wal_file) noexcept
  : file_offset(file_offset),
    wal_offset_(wal_offset),
    len(len),
    wal_file_(std::move(wal_file))
  {}

  replacement_map_value(const wal_record_modify_file32& record) noexcept
  : replacement_map_value(record.file_offset, record.wal_offset, record.wal_len, record.wal_file)
  {}

  auto offset() const noexcept -> offset_type { return file_offset; }
  auto size() const noexcept -> size_type { return len; }
  auto end_offset() const noexcept -> offset_type { return offset() + size(); }
  auto wal_offset() const noexcept -> offset_type { return wal_offset_; }

  auto wal_file() const noexcept -> execution::io::type_erased_at_readable& {
    assert(wal_file_ != nullptr);
    return *wal_file_;
  }

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

  static auto cmp_eq(std::span<const replacement_map_value> x, std::span<const replacement_map_value> y) -> bool;

  private:
  offset_type file_offset = 0, wal_offset_ = 0;
  size_type len = 0;
  std::shared_ptr<execution::io::type_erased_at_readable> wal_file_;
};


/**
 * \brief Record file changes.
 * \details
 * A replacement map holds on to file changes in memory and allows for them to
 * be applied during reads.
 */
template<typename Alloc = std::allocator<std::byte>>
class replacement_map {
  public:
  using value_type = replacement_map_value;
  using reference = value_type&;
  using const_reference = const value_type&;
  using pointer = value_type*;
  using const_pointer = const value_type*;
  using allocator_type = Alloc;

  using offset_type = typename value_type::offset_type;
  using size_type = typename value_type::size_type;

  private:
  using alloc_traits = std::allocator_traits<allocator_type>;
  using map_type = std::vector<value_type, typename alloc_traits::template rebind_alloc<value_type>>;

  public:
  using iterator = typename map_type::const_iterator;
  using const_iterator = iterator;

  explicit replacement_map(allocator_type alloc = allocator_type{})
  : map_(alloc)
  {}

  replacement_map(const replacement_map& y, allocator_type alloc)
  : map_(y.map_, alloc)
  {}

  replacement_map(replacement_map&& y, allocator_type alloc)
  : map_(std::move(y.map_), alloc)
  {}

  template<typename OtherAlloc>
  requires (!std::same_as<Alloc, OtherAlloc>)
  replacement_map(const replacement_map<OtherAlloc>& y, allocator_type alloc = allocator_type{})
  : map_(y.map_.begin(), y.map_.end(), alloc)
  {}

  void swap(replacement_map& other) noexcept {
    map_.swap(other.map_);
  }

  template<typename OtherAlloc>
  auto operator==(const replacement_map<OtherAlloc>& y) const -> bool {
    return value_type::cmp_eq(map_, y.map_);
  }

  template<typename OtherAlloc>
  auto operator!=(const replacement_map<OtherAlloc>& y) const -> bool {
    return !(*this == y);
  }

  auto get_allocator() const -> allocator_type { return map_.get_allocator(); }
  auto empty() const noexcept -> bool { return map_.empty(); }
  void clear() { map_.clear(); }

  template<typename... Args>
  requires std::constructible_from<value_type, Args...>
  auto insert(Args&&... args) -> iterator {
    return insert_(value_type(std::forward<Args>(args)...));
  }

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

  auto merge(const replacement_map& other) -> replacement_map& {
    std::for_each(other.map_.begin(), other.map_.end(),
        [this](const_reference v) {
          this->insert_(v);
        });
    return *this;
  }

  auto merge(replacement_map&& other) noexcept -> replacement_map& {
    std::for_each(
        std::make_move_iterator(other.map_.begin()), std::make_move_iterator(other.map_.end()),
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


template<typename Alloc>
inline void swap(replacement_map<Alloc>& x, replacement_map<Alloc>& y) noexcept {
  x.swap(y);
}


} /* namespace earnest::detail */
