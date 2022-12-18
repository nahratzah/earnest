#ifndef EARNEST_DETAIL_REPLACEMENT_MAP_H
#define EARNEST_DETAIL_REPLACEMENT_MAP_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <utility>

#include <asio/async_result.hpp>
#include <asio/buffer.hpp>
#include <boost/intrusive/set.hpp>

namespace earnest::detail {


using replacement_map_value_hook_ = boost::intrusive::set_base_hook<boost::intrusive::optimize_size<true>>;


class replacement_map_value
: public replacement_map_value_hook_
{
  public:
  using offset_type = std::uint64_t;
  using size_type = std::size_t;

  replacement_map_value() noexcept = default;

  template<typename Alloc>
  replacement_map_value(offset_type off, asio::const_buffer buf, Alloc&& alloc);

  replacement_map_value(offset_type off, asio::const_buffer buf);
  replacement_map_value(offset_type off, const replacement_map_value& buf);

  template<typename Alloc>
  static auto allocate(offset_type off, size_type len, Alloc&& alloc) -> std::tuple<replacement_map_value, asio::mutable_buffer>;

  auto offset() const noexcept -> offset_type;
  auto size() const noexcept -> size_type;
  auto end_offset() const noexcept -> offset_type;
  auto data() const noexcept -> const void*;
  auto buffer() const noexcept -> asio::const_buffer;
  auto shared_data() const noexcept -> std::shared_ptr<const void>;

  void start_at(offset_type new_off);
  void truncate(size_type new_sz);

  private:
  offset_type off_ = 0;
  size_type len_ = 0;
  std::shared_ptr<const void> data_;
};


struct rmv_key_extractor_ {
  using type = std::uint64_t;

  auto operator()(const replacement_map_value& rmv) const noexcept -> type;
};


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
-> bool;


/**
 * \brief Record file changes.
 * \details
 * A replacement map holds on to file changes in memory and allows for them to
 * be applied during reads.
 *
 * \tparam Alloc An allocator type.
 */
template<typename Alloc = std::allocator<std::byte>>
class replacement_map
: private std::allocator_traits<Alloc>::template rebind_alloc<replacement_map_value>
{
  public:
  using value_type = replacement_map_value;
  using reference = value_type&;
  using const_reference = const value_type&;
  using pointer = value_type*;
  using const_pointer = const value_type*;
  using allocator_type = typename std::allocator_traits<Alloc>::template rebind_alloc<value_type>;

  using offset_type = value_type::offset_type;
  using size_type = value_type::size_type;

  private:
  using alloc_traits = std::allocator_traits<allocator_type>;
  using map_type = boost::intrusive::set<
      replacement_map_value,
      boost::intrusive::base_hook<replacement_map_value_hook_>,
      boost::intrusive::constant_time_size<false>,
      boost::intrusive::key_of_value<rmv_key_extractor_>>;

  class disposer_ {
    public:
    explicit disposer_(replacement_map& self) noexcept;
    auto operator()(value_type* ptr) const noexcept;
    auto owner() const noexcept -> const allocator_type*;

    private:
    allocator_type* self_ = nullptr;
  };

  public:
  using iterator = map_type::const_iterator;
  using const_iterator = iterator;

  explicit replacement_map(allocator_type alloc = allocator_type())
      noexcept(std::is_nothrow_move_constructible_v<allocator_type>);
  replacement_map(const replacement_map& other);
  replacement_map(replacement_map&& other)
      noexcept(std::is_nothrow_move_constructible_v<allocator_type> && std::is_nothrow_move_constructible_v<map_type>);

  auto operator=(const replacement_map& other) -> replacement_map&;
  auto operator=(replacement_map&& other) noexcept -> replacement_map&;

  ~replacement_map();

  void swap(replacement_map& other) noexcept;
  auto get_allocator() const -> allocator_type;

  auto operator==(const replacement_map& y) const noexcept -> bool;
  auto operator!=(const replacement_map& y) const noexcept -> bool;
  auto empty() const noexcept -> bool;

  void clear();
  auto insert(offset_type off, asio::const_buffer buf) -> iterator;
  [[nodiscard]] auto insert(offset_type off, size_type len) -> std::pair<iterator, asio::mutable_buffer>;

  template<typename OffsetIter>
  auto insert_many(OffsetIter b, OffsetIter e, asio::const_buffer buf) -> std::shared_ptr<const void>;

  template<typename OffsetIter>
  [[nodiscard]] auto insert_many(OffsetIter b, OffsetIter e, size_type len) -> std::pair<std::shared_ptr<const void>, asio::mutable_buffer>;

  auto erase(offset_type begin_off, offset_type end_off) -> iterator;
  void truncate(offset_type new_sz) noexcept;

  auto begin() -> iterator;
  auto end() -> iterator;
  auto begin() const -> const_iterator;
  auto end() const -> const_iterator;
  auto cbegin() const -> const_iterator;
  auto cend() const -> const_iterator;

  template<typename OtherAlloc>
  auto merge(const replacement_map<OtherAlloc>& other) -> replacement_map&;
  auto merge(replacement_map&& other) noexcept -> replacement_map&;

  ///\brief Find the first element at-or-after offset \p off .
  auto find(offset_type off) -> iterator;
  ///\brief Find the first element at-or-after offset \p off .
  auto find(offset_type off) const -> const_iterator;

  /**
   * \brief Insert data from the given random-access-device.
   * \details
   * Reads \p len bytes from the \p device starting at offset \p device_off .
   * The data is inserted at position \p map_off .
   *
   * The region is inserted before the function completes, so it's
   * deterministic to use overlapping regions.
   *
   * \attention Until the read completes successfully, the replacement_map will contain uninitialized memory.
   * If the read fails, then the memory will remain in the map, and remain uninitialized.
   *
   * \param device An asynchronous random-access read device.
   * \param device_off Offset at which the read on \p device happens.
   * \param map_off Offset in this replacement_map at which the read data is to be inserted.
   * \param len The length (in bytes) of data that is to be read.
   * \param token Completion token.
   */
  template<
      typename AsyncRandomAccessReadDevice,
      typename CompletionToken = typename asio::default_completion_token<typename AsyncRandomAccessReadDevice::executor_type>::type>
  auto async_read_at_and_insert(AsyncRandomAccessReadDevice& device, std::uint64_t device_off, offset_type map_off, size_type len, CompletionToken&& token = typename asio::default_completion_token<typename AsyncRandomAccessReadDevice::executor_type>::type{})
  -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code)>::return_type;

  /**
   * \brief Insert data at many positions, from the given random-access-device.
   * \details
   * Reads \p len bytes from the \p device starting at offset \p device_off .
   * The data is inserted many times, at the offsets described by iterator pairs \p map_off_begin and \p map_off_end .
   *
   * The regions are inserted in order of iteration, before the function completes, so it's
   * deterministic to use overlapping regions.
   *
   * \attention Until the read completes successfully, the replacement_map will contain uninitialized memory.
   * If the read fails, then the memory will remain in the map, and remain uninitialized.
   *
   * \param device An asynchronous random-access read device.
   * \param device_off Offset at which the read on \p device happens.
   * \param map_off_begin,map_off_end Iterator-range of offsets in this replacement_map at which the read data is to be inserted.
   * \param len The length (in bytes) of data that is to be read.
   * \param token Completion token.
   */
  template<
      typename AsyncRandomAccessReadDevice,
      typename OffsetIter,
      typename CompletionToken = typename asio::default_completion_token<typename AsyncRandomAccessReadDevice::executor_type>::type>
  auto async_read_at_and_insert_many(AsyncRandomAccessReadDevice& device, std::uint64_t device_off, OffsetIter map_off_begin, OffsetIter map_off_end, size_type len, CompletionToken&& token = typename asio::default_completion_token<typename AsyncRandomAccessReadDevice::executor_type>::type{})
  -> typename asio::async_result<std::decay_t<CompletionToken>, void(std::error_code)>::return_type;

  private:
  template<typename... Args>
  auto make_value_(Args&&... args) -> std::unique_ptr<value_type, disposer_>;

  auto insert_(std::unique_ptr<value_type, disposer_>&& ptr) noexcept -> iterator;
  auto erase_(offset_type begin_offset, offset_type end_offset) noexcept -> iterator;

  map_type map_;
};


template<typename Alloc>
void swap(replacement_map<Alloc>& x, replacement_map<Alloc>& y) noexcept;


extern template class replacement_map<>;


} /* namespace earnest::detail */

#include "replacement_map.ii"

#endif /* EARNEST_DETAIL_REPLACEMENT_MAP_H */
