#ifndef EARNEST_DETAIL_REPLACEMENT_MAP_INL_H
#define EARNEST_DETAIL_REPLACEMENT_MAP_INL_H

#include <iterator>
#include <limits>
#include <stdexcept>
#include <utility>

namespace earnest::detail {


using replacement_map_value_hook_ = boost::intrusive::set_base_hook<boost::intrusive::optimize_size<true>>;


template<typename Alloc>
inline replacement_map_value::replacement_map_value(offset_type off, asio::const_buffer buf, Alloc&& alloc) {
  asio::mutable_buffer my_buf;
  std::tie(*this, my_buf) = allocate(off, buf.size(), std::forward<Alloc>(alloc));
  asio::buffer_copy(my_buf, buf);
}

inline replacement_map_value::replacement_map_value(offset_type off, asio::const_buffer buf)
: replacement_map_value(off, buf, std::allocator<std::byte>())
{}

inline replacement_map_value::replacement_map_value(offset_type off, const replacement_map_value& buf)
: off_(off),
  len_(buf.len_),
  data_(buf.data_)
{
  if (len_ > std::numeric_limits<offset_type>::max() - off_)
    throw std::length_error("replacement_map buffer-end-offset overflow");
}

template<typename Alloc>
inline auto replacement_map_value::allocate(offset_type off, size_type len, Alloc&& alloc) -> std::tuple<replacement_map_value, asio::mutable_buffer> {
  std::tuple<replacement_map_value, asio::mutable_buffer> result;

  if (len > std::numeric_limits<offset_type>::max() - off)
    throw std::length_error("replacement_map buffer-end-offset overflow");

  using alloc_traits = typename std::allocator_traits<std::decay_t<Alloc>>::template rebind_traits<std::byte>;
  typename alloc_traits::allocator_type a = std::forward<Alloc>(alloc);
#if __cpp_lib_shared_ptr_arrays >= 201707L
  auto dptr = std::allocate_shared<std::byte[]>(a, len);
#else
  std::shared_ptr<std::byte> dptr;
  {
    auto raw_ptr = alloc_traits::allocate(a, len);
    dptr.reset(
        raw_ptr,
        [a, len](std::byte* ptr) mutable noexcept -> void {
          alloc_traits::deallocate(a, ptr, len);
        },
        a);
  }
#endif
  std::get<1>(result) = asio::buffer(dptr.get(), len);

  std::get<0>(result).off_ = off;
  std::get<0>(result).len_ = len;
  std::get<0>(result).data_ = std::move(dptr);

  return result;
}

inline auto replacement_map_value::offset() const noexcept -> offset_type {
  return off_;
}

inline auto replacement_map_value::size() const noexcept -> size_type {
  return len_;
}

inline auto replacement_map_value::end_offset() const noexcept -> offset_type {
  return offset() + size();
}

inline auto replacement_map_value::data() const noexcept -> const void* {
  return data_.get();
}

inline auto replacement_map_value::buffer() const noexcept -> asio::const_buffer {
  return asio::buffer(data(), size());
}

inline auto replacement_map_value::shared_data() const noexcept -> std::shared_ptr<const void> {
  return data_;
}

inline void replacement_map_value::start_at(offset_type new_off) {
  if (new_off < offset() || new_off > end_offset())
    throw std::invalid_argument("replacement_map start_at invalid position");

  const size_type shift = new_off - off_;
  data_ = std::shared_ptr<const void>(data_, reinterpret_cast<const std::byte*>(data_.get()) + shift);
  len_ -= shift;
  off_ += shift;
}

inline void replacement_map_value::truncate(size_type new_sz) {
  if (new_sz > len_)
    throw std::invalid_argument("replacement_map truncate may not grow size");

  len_ = new_sz;
}


inline auto rmv_key_extractor_::operator()(const replacement_map_value& rmv) const noexcept -> type {
  return rmv.offset();
}


template<typename Alloc>
replacement_map<Alloc>::replacement_map(allocator_type alloc)
    noexcept(std::is_nothrow_move_constructible_v<allocator_type>)
: allocator_type(std::move(alloc))
{}

template<typename Alloc>
replacement_map<Alloc>::replacement_map(const replacement_map& other)
: allocator_type(alloc_traits::select_on_container_copy_construction(other))
{
  map_.clone_from(
      other.map_,
      [this](const_reference v) {
        return make_value_(v);
      },
      disposer_(*this));
}

template<typename Alloc>
replacement_map<Alloc>::replacement_map(replacement_map&& other) noexcept(std::is_nothrow_move_constructible_v<allocator_type> && std::is_nothrow_move_constructible_v<map_type>)
: allocator_type(other.get_allocator()),
  map_(std::move(other.map_))
{}

template<typename Alloc>
auto replacement_map<Alloc>::operator=(const replacement_map& other) -> replacement_map& {
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

template<typename Alloc>
auto replacement_map<Alloc>::operator=(replacement_map&& other) noexcept -> replacement_map& {
  clear();

  if constexpr(alloc_traits::propagate_on_container_move_assignment::value) {
    allocator_type& my_alloc = *this;
    my_alloc = std::move(other.get_allocator());
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

template<typename Alloc>
replacement_map<Alloc>::~replacement_map() {
  clear();
}

template<typename Alloc>
void replacement_map<Alloc>::swap(replacement_map& other) noexcept {
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

template<typename Alloc>
auto replacement_map<Alloc>::get_allocator() const -> allocator_type {
  return *this;
}

template<typename Alloc>
auto replacement_map<Alloc>::operator==(const replacement_map& y) const noexcept -> bool {
  return equal_(map_, y.map_);
}

template<typename Alloc>
auto replacement_map<Alloc>::operator!=(const replacement_map& y) const noexcept -> bool {
  return !(*this == y);
}

template<typename Alloc>
auto replacement_map<Alloc>::empty() const noexcept -> bool {
  return map_.empty();
}

template<typename Alloc>
void replacement_map<Alloc>::clear() {
  map_.clear_and_dispose(disposer_(*this));
}

template<typename Alloc>
auto replacement_map<Alloc>::insert(offset_type off, asio::const_buffer buf) -> iterator {
  return insert_(make_value_(off, buf, get_allocator()));
}

template<typename Alloc>
[[nodiscard]]
auto replacement_map<Alloc>::insert(offset_type off, size_type len) -> std::pair<iterator, asio::mutable_buffer> {
  value_type vt;
  asio::mutable_buffer buf;
  std::tie(vt, buf) = value_type::allocate(off, len, get_allocator());

  return std::make_pair(insert_(make_value_(std::move(vt))), buf);
}

template<typename Alloc>
template<typename OffsetIter>
auto replacement_map<Alloc>::insert_many(OffsetIter b, OffsetIter e, asio::const_buffer buf) -> std::shared_ptr<const void> {
  if (b == e) return nullptr;

  const auto source = insert_(make_value_(*b, buf, get_allocator()));
  for (++b; b != e; ++b) insert_(make_value_(*b, *source));
  return source.shared_data();
}

template<typename Alloc>
template<typename OffsetIter>
[[nodiscard]]
auto replacement_map<Alloc>::insert_many(OffsetIter b, OffsetIter e, size_type len) -> std::pair<std::shared_ptr<const void>, asio::mutable_buffer> {
  replacement_map_value template_value;
  asio::mutable_buffer buf;
  std::tie(template_value, buf) = replacement_map_value::allocate(0, len, get_allocator());

  for (++b; b != e; ++b) insert_(make_value_(*b, template_value));
  return std::make_pair(template_value.shared_data(), buf);
}

template<typename Alloc>
auto replacement_map<Alloc>::erase(offset_type begin_off, offset_type end_off) -> iterator {
  if (begin_off > end_off)
    throw std::invalid_argument("range must go from low to high offsets");
  return erase_(begin_off, end_off);
}

template<typename Alloc>
void replacement_map<Alloc>::truncate(offset_type new_sz) noexcept {
  erase(new_sz, std::numeric_limits<offset_type>::max());
}

template<typename Alloc>
auto replacement_map<Alloc>::begin() -> iterator {
  return map_.begin();
}

template<typename Alloc>
auto replacement_map<Alloc>::end() -> iterator {
  return map_.end();
}

template<typename Alloc>
auto replacement_map<Alloc>::begin() const -> const_iterator {
  return map_.begin();
}

template<typename Alloc>
auto replacement_map<Alloc>::end() const -> const_iterator {
  return map_.end();
}

template<typename Alloc>
auto replacement_map<Alloc>::cbegin() const -> const_iterator {
  return begin();
}

template<typename Alloc>
auto replacement_map<Alloc>::cend() const -> const_iterator {
  return end();
}

template<typename Alloc>
template<typename OtherAlloc>
auto replacement_map<Alloc>::merge(const replacement_map<OtherAlloc>& other) -> replacement_map& {
  std::for_each(other.begin(), other.end(),
      [this](const replacement_map_value& v) {
        insert_(make_value_(v.offset(), v));
      });
  return *this;
}

template<typename Alloc>
auto replacement_map<Alloc>::merge(replacement_map&& other) noexcept -> replacement_map& {
#ifndef NDEBUG
  allocator_type& my_alloc = *this;
  allocator_type& other_alloc = other;
  assert(my_alloc == other_alloc);
#endif

  other.map_.clear_and_dispose(
      [this](replacement_map_value* v) {
        auto ptr = std::unique_ptr<value_type, disposer_>(nullptr, disposer_(*this));
        ptr.reset(v);
        insert_(std::move(ptr));
      });
  return *this;
}

template<typename Alloc>
auto replacement_map<Alloc>::find(offset_type off) -> iterator {
  auto iter = map_.lower_bound(off);
  if (iter != map_.begin()) {
    auto before = std::prev(iter);
    if (before->end_offset() > off) return before;
  }
  return iter;
}

template<typename Alloc>
auto replacement_map<Alloc>::find(offset_type off) const -> const_iterator {
  auto iter = map_.lower_bound(off);
  if (iter != map_.begin()) {
    auto before = std::prev(iter);
    if (before->end_offset() > off) return before;
  }
  return iter;
}

template<typename Alloc>
template<typename... Args>
auto replacement_map<Alloc>::make_value_(Args&&... args) -> std::unique_ptr<value_type, disposer_> {
  auto ptr = std::unique_ptr<value_type, disposer_>(nullptr, disposer_(*this));

  auto raw_ptr = alloc_traits::allocate(*this, 1);
  try {
    alloc_traits::construct(*this, raw_ptr, std::forward<Args>(args)...);
  } catch (...) {
    alloc_traits::deallocate(*this, raw_ptr, 1);
    throw;
  }

  ptr.reset(raw_ptr); // Never throws.
  return ptr;
}

template<typename Alloc>
auto replacement_map<Alloc>::insert_(std::unique_ptr<value_type, disposer_>&& ptr) noexcept -> iterator {
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

template<typename Alloc>
auto replacement_map<Alloc>::erase_(offset_type begin_offset, offset_type end_offset) noexcept -> iterator {
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


template<typename Alloc>
void swap(replacement_map<Alloc>& x, replacement_map<Alloc>& y) noexcept {
  x.swap(y);
}


template<typename Alloc>
replacement_map<Alloc>::disposer_::disposer_(replacement_map& self) noexcept
: self_(&self)
{}

template<typename Alloc>
auto replacement_map<Alloc>::disposer_::operator()(value_type* ptr) const noexcept {
  alloc_traits::destroy(*self_, ptr);
  alloc_traits::deallocate(*self_, ptr, 1);
}

template<typename Alloc>
auto replacement_map<Alloc>::disposer_::owner() const noexcept -> const allocator_type* {
  return self_;
}


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_REPLACEMENT_MAP_INL_H */
