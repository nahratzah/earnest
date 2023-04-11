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


struct rmv_key_extractor_ {
  using type = std::uint64_t;

  template<typename FD>
  auto operator()(const replacement_map_value<FD>& rmv) const noexcept -> type {
    return rmv.offset();
  }
};


template<typename FD1, typename FD2>
auto equal_(
  const boost::intrusive::set<
      replacement_map_value<FD1>,
      boost::intrusive::base_hook<replacement_map_value_hook_>,
      boost::intrusive::constant_time_size<false>,
      boost::intrusive::key_of_value<rmv_key_extractor_>>& x,
  const boost::intrusive::set<
      replacement_map_value<FD2>,
      boost::intrusive::base_hook<replacement_map_value_hook_>,
      boost::intrusive::constant_time_size<false>,
      boost::intrusive::key_of_value<rmv_key_extractor_>>& y) noexcept
-> bool {
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


/**
 * \brief Record file changes.
 * \details
 * A replacement map holds on to file changes in memory and allows for them to
 * be applied during reads.
 *
 * \tparam Alloc An allocator type.
 */
template<typename FD, typename Alloc = std::allocator<std::byte>>
class replacement_map
: private std::allocator_traits<Alloc>::template rebind_alloc<replacement_map_value<FD>>
{
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
  using map_type = boost::intrusive::set<
      value_type,
      boost::intrusive::base_hook<replacement_map_value_hook_>,
      boost::intrusive::constant_time_size<false>,
      boost::intrusive::key_of_value<rmv_key_extractor_>>;

  class disposer_ {
    public:
    explicit disposer_(replacement_map& self) noexcept
    : self_(&self)
    {}

    auto operator()(value_type* ptr) const noexcept {
      alloc_traits::destroy(*self_, ptr);
      alloc_traits::deallocate(*self_, ptr, 1);
    }

    auto owner() const noexcept -> const allocator_type* { return self_; }

    private:
    allocator_type* self_ = nullptr;
  };

  public:
  using iterator = typename map_type::const_iterator;
  using const_iterator = iterator;

  explicit replacement_map(allocator_type alloc = allocator_type())
      noexcept(std::is_nothrow_move_constructible_v<allocator_type>)
  : allocator_type(std::move(alloc))
  {}

  replacement_map(const replacement_map& other)
  : allocator_type(other.get_allocator())
  {
#if 0 // Somehow this trips a SIGBUS, but only sometimes.
    map_.clone_from(
        other.map_,
        [this](const_reference v) {
          return make_value_(v);
        },
        disposer_(*this));
#else
    for (const auto& e : other) insert(e);
#endif
  }

  replacement_map(replacement_map&& other)
      noexcept(std::is_nothrow_move_constructible_v<allocator_type> && std::is_nothrow_move_constructible_v<map_type>)
  : allocator_type(other.get_allocator()),
    map_(std::move(other.map_))
  {}

  auto operator=(const replacement_map& other) -> replacement_map& {
    clear();

    if constexpr(alloc_traits::propagate_on_container_copy_assignment::value) {
      allocator_type& my_alloc = *this;
      my_alloc = other.get_allocator();
    }

    map_.clone_from(
        other.map_,
        [this](const_reference v) {
          return make_value_(v);
        },
        disposer_(*this));
    return *this;
  }

  auto operator=(replacement_map&& other) noexcept -> replacement_map& {
    clear();

    if constexpr(alloc_traits::propagate_on_container_swap::value) {
      allocator_type& my_alloc = *this;
      my_alloc = other.get_allocator();
    } else {
#ifndef NDEBUG
      allocator_type& my_alloc = *this;
      allocator_type& other_alloc = other;
      assert(my_alloc == other_alloc);
#endif
    }

    map_ = std::move(other.map_);
    return *this;
  }

  ~replacement_map() { clear(); }

  void swap(replacement_map& other) noexcept {
    using std::swap;

    if constexpr(alloc_traits::propagate_on_container_swap::value) {
      allocator_type& my_alloc = *this;
      allocator_type& other_alloc = other;
      swap(my_alloc, other_alloc);
    } else {
#ifndef NDEBUG
      allocator_type& my_alloc = *this;
      allocator_type& other_alloc = other;
      assert(my_alloc == other_alloc);
#endif
    }

    swap(map_, other.map_);
  }

  auto get_allocator() const -> allocator_type { return *this; }
  auto operator==(const replacement_map& y) const noexcept -> bool { return equal_(map_, y.map_); }
  auto operator!=(const replacement_map& y) const noexcept -> bool { return !(*this == y); }
  auto empty() const noexcept -> bool { return map_.empty(); }

  void clear() { map_.clear_and_dispose(disposer_(*this)); }
  template<typename... Args>
  auto insert(Args&&... args) -> iterator { return insert_(make_value_(std::forward<Args>(args)...)); }

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
          this->insert_(this->make_value_(v));
        });
    return *this;
  }

  auto merge(replacement_map&& other) noexcept -> replacement_map& {
#ifndef NDEBUG
    allocator_type& my_alloc = *this;
    allocator_type& other_alloc = other;
    assert(my_alloc == other_alloc);
#endif

    other.map_.clear_and_dispose(
        [this](pointer v) {
          auto ptr = std::unique_ptr<value_type, disposer_>(nullptr, disposer_(*this));
          ptr.reset(v);
          insert_(std::move(ptr));
        });
    return *this;
  }

  ///\brief Find the first element at-or-after offset \p off .
  auto find(offset_type off) -> iterator {
    auto iter = map_.lower_bound(off);
    if (iter != map_.begin()) {
      auto before = std::prev(iter);
      if (before->end_offset() > off) return before;
    }
    return iter;
  }

  ///\brief Find the first element at-or-after offset \p off .
  auto find(offset_type off) const -> const_iterator {
    auto iter = map_.lower_bound(off);
    if (iter != map_.begin()) {
      auto before = std::prev(iter);
      if (before->end_offset() > off) return before;
    }
    return iter;
  }

  private:
  template<typename... Args>
  auto make_value_(Args&&... args) -> std::unique_ptr<value_type, disposer_> {
    auto ptr = std::unique_ptr<value_type, disposer_>(nullptr, disposer_(*this));

    auto raw_ptr = alloc_traits::allocate(*this, 1);
    try {
      alloc_traits::construct(*this, raw_ptr, std::forward<Args>(args)...);
    } catch (...) {
      try {
        alloc_traits::deallocate(*this, raw_ptr, 1);
      } catch (...) {
        // If the deallocate throws, we want to keep our original exception.
        // And so, we drop this exception and probably leak the pointer.
        std::clog << "during recovery, deallocation failed, may leak memory\n";
      }
      throw;
    }

    ptr.reset(raw_ptr); // Never throws.
    return ptr;
  }

  auto insert_(std::unique_ptr<value_type, disposer_>&& ptr) noexcept -> iterator {
    assert(ptr.get_deleter().owner() == this);
    assert(ptr != nullptr);
    if (ptr->size() == 0) return map_.end(); // skip insert of empty entries

    erase_(ptr->offset(), ptr->end_offset());

    iterator ins_iter;
    bool inserted;
    std::tie(ins_iter, inserted) = map_.insert(*ptr);
    assert(inserted);
    ptr.release();
    return ins_iter;
  }

  auto erase_(offset_type begin_offset, offset_type end_offset) noexcept -> iterator {
    assert(begin_offset <= end_offset);

    auto begin = map_.lower_bound(begin_offset);
    if (begin != map_.begin()) {
      auto before = std::prev(begin);
      if (before->end_offset() > begin_offset) {
        auto sibling = make_value_(*before);
        sibling->start_at(begin_offset); // if this throws, this function is buggy

        before->truncate(begin_offset - before->offset()); // if this throws, this function is buggy

        bool insert_success;
        std::tie(begin, insert_success) = map_.insert(*sibling);
        assert(insert_success);
        sibling.release();
      }
    }

    auto end = map_.lower_bound(end_offset);
    if (end != map_.begin()) {
      auto before = std::prev(end);
      if (before->end_offset() > end_offset) {
        before->start_at(end_offset);
        end = before;
      }
    }

    return map_.erase_and_dispose(begin, end, disposer_(*this));
  }

  map_type map_;
};


template<typename FD, typename Alloc>
void swap(replacement_map<FD, Alloc>& x, replacement_map<FD, Alloc>& y) noexcept {
  x.swap(y);
}


} /* namespace earnest::detail */
