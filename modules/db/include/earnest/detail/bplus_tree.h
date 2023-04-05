#pragma once

#include <algorithm>
#include <atomic>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iterator>
#include <iostream>
#include <memory>
#include <numeric>
#include <scoped_allocator>
#include <span>
#include <system_error>
#include <typeinfo>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <asio/append.hpp>
#include <asio/async_result.hpp>
#include <asio/buffer.hpp>
#include <asio/deferred.hpp>
#include <asio/io_context.hpp>
#include <boost/endian.hpp>
#include <boost/polymorphic_pointer_cast.hpp>

#include <earnest/byte_span_printer.h>
#include <earnest/db_address.h>
#include <earnest/db_cache.h>
#include <earnest/detail/completion_barrier.h>
#include <earnest/detail/completion_handler_fun.h>
#include <earnest/detail/completion_wrapper.h>
#include <earnest/detail/constexpr_rounding.h>
#include <earnest/detail/move_only_function.h>
#include <earnest/detail/overload.h>
#include <earnest/raw_db.h>
#include <earnest/xdr.h>

namespace earnest::detail {


// Error codes used internally for the b+tree.
// Mostly used for signaling.
enum class bplus_tree_errc {
  restart=1,
  allocator_file_mismatch,
  root_page_present,
  bad_tree,
  page_too_small_for_split,
};

inline auto bplus_tree_category() -> const std::error_category& {
  class category_impl
  : public std::error_category
  {
    public:
    constexpr category_impl() noexcept = default;

    auto name() const noexcept -> const char* override {
      return "earnest::detail::bplus_tree";
    }

    auto message(int condition) const -> std::string override {
      using namespace std::string_literals;

      switch (static_cast<bplus_tree_errc>(condition)) {
        default:
          return "unrecognized condition"s;
        case bplus_tree_errc::restart:
          return "B+tree restart operation"s;
        case bplus_tree_errc::allocator_file_mismatch:
          return "B+tree allocator allocated in a different file"s;
        case bplus_tree_errc::root_page_present:
          return "B+tree already has a root page"s;
        case bplus_tree_errc::bad_tree:
          return "B+tree is incorrect"s;
        case bplus_tree_errc::page_too_small_for_split:
          return "B+tree page-split on too-small-page"s;
      }
    }
  };

  static category_impl impl;
  return impl;
}

inline auto make_error_code(bplus_tree_errc e) noexcept -> std::error_code {
  return std::error_code(static_cast<int>(e), bplus_tree_category());
}


} /* namespace earnest::detail */

namespace std {


template<>
struct is_error_code_enum<::earnest::detail::bplus_tree_errc>
: std::true_type
{};


} /* namespace std */

namespace earnest::detail {


struct bplus_tree_header {
  static inline constexpr std::uint32_t magic = 0x72bd'8d1aU;
  std::uint64_t root;

  template<typename X>
  friend auto operator&(::earnest::xdr<X>&& x, typename ::earnest::xdr<X>::template typed_function_arg<bplus_tree_header> y) {
    return std::move(x)
        & xdr_constant(magic).as(xdr_uint32)
        & xdr_constant(0).as(xdr_uint32) // reserved
        & xdr_uint64(y.root);
  }
};

struct bplus_tree_page_header {
  static constexpr std::size_t augment_propagation_required_offset = 4u;
  static constexpr std::size_t parent_offset = 24u;

  std::uint32_t magic;
  bool augment_propagation_required = false;
  std::uint64_t parent;

  template<typename X>
  friend auto operator&(::earnest::xdr<X>&& x, typename ::earnest::xdr<X>::template typed_function_arg<bplus_tree_page_header> y) {
    return std::move(x)
        & xdr_uint32(y.magic)
        & xdr_bool(y.augment_propagation_required)
        & xdr_constant(0).as(xdr_uint64) // reserved
        & xdr_constant(0).as(xdr_uint64) // reserved
        & xdr_uint64(y.parent);
  }
};

struct bplus_tree_intr_header {
  std::uint32_t size = 0;

  template<typename X>
  friend auto operator&(::earnest::xdr<X>&& x, [[maybe_unused]] typename ::earnest::xdr<X>::template typed_function_arg<bplus_tree_intr_header> y) {
    return std::move(x) & xdr_uint32(y.size);
  }
};

struct bplus_tree_leaf_header {
  std::uint64_t successor_page, predecessor_page;

  template<typename X>
  friend auto operator&(::earnest::xdr<X>&& x, typename ::earnest::xdr<X>::template typed_function_arg<bplus_tree_leaf_header> y) {
    return std::move(x)
        & xdr_uint64(y.predecessor_page)
        & xdr_uint64(y.successor_page);
  }
};


// Different states for leaf elements.
enum class bplus_tree_leaf_use_element : std::uint8_t {
  unused = 0,
  used = 1,
  before_first = 0x70,
  after_last = 0x71,
  ghost_create = 0x80,
  ghost_delete = 0x81,
  ghost_iterator_before = 0x72,
  ghost_iterator_after = 0x73,
};


template<typename Traits>
inline auto operator<<(std::basic_ostream<char, Traits>& out, bplus_tree_leaf_use_element elem) -> std::basic_ostream<char, Traits>& {
  using namespace std::literals;

  switch (elem) {
    default:
      {
        unsigned int elem_value = static_cast<std::underlying_type_t<bplus_tree_leaf_use_element>>(elem);
        out << "earnest::detail::bplus_tree_leaf_use_element{"sv << elem_value << "}"sv;
      }
      break;
    case bplus_tree_leaf_use_element::unused:
      out << "unused"sv;
      break;
    case bplus_tree_leaf_use_element::used:
      out << "used"sv;
      break;
    case bplus_tree_leaf_use_element::before_first:
      out << "before_first"sv;
      break;
    case bplus_tree_leaf_use_element::after_last:
      out << "after_last"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_create:
      out << "ghost_create"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_delete:
      out << "ghost_delete"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_iterator_before:
      out << "ghost_iterator_before"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_iterator_after:
      out << "ghost_iterator_after"sv;
      break;
  }

  return out;
}

template<typename Traits>
inline auto operator<<(std::basic_ostream<wchar_t, Traits>& out, bplus_tree_leaf_use_element elem) -> std::basic_ostream<wchar_t, Traits>& {
  using namespace std::literals;

  switch (elem) {
    default:
      {
        unsigned int elem_value = static_cast<std::underlying_type_t<bplus_tree_leaf_use_element>>(elem);
        out << L"earnest::detail::bplus_tree_leaf_use_element{"sv << elem_value << L"}"sv;
      }
      break;
    case bplus_tree_leaf_use_element::unused:
      out << L"unused"sv;
      break;
    case bplus_tree_leaf_use_element::used:
      out << L"used"sv;
      break;
    case bplus_tree_leaf_use_element::before_first:
      out << L"before_first"sv;
      break;
    case bplus_tree_leaf_use_element::after_last:
      out << L"after_last"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_create:
      out << L"ghost_create"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_delete:
      out << L"ghost_delete"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_iterator_before:
      out << L"ghost_iterator_before"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_iterator_after:
      out << L"ghost_iterator_after"sv;
      break;
  }

  return out;
}

template<typename Traits>
inline auto operator<<(std::basic_ostream<char8_t, Traits>& out, bplus_tree_leaf_use_element elem) -> std::basic_ostream<char8_t, Traits>& {
  using namespace std::literals;

  switch (elem) {
    default:
      {
        unsigned int elem_value = static_cast<std::underlying_type_t<bplus_tree_leaf_use_element>>(elem);
        out << u8"earnest::detail::bplus_tree_leaf_use_element{"sv << elem_value << u8"}"sv;
      }
      break;
    case bplus_tree_leaf_use_element::unused:
      out << u8"unused"sv;
      break;
    case bplus_tree_leaf_use_element::used:
      out << u8"used"sv;
      break;
    case bplus_tree_leaf_use_element::before_first:
      out << u8"before_first"sv;
      break;
    case bplus_tree_leaf_use_element::after_last:
      out << u8"after_last"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_create:
      out << u8"ghost_create"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_delete:
      out << u8"ghost_delete"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_iterator_before:
      out << u8"ghost_iterator_before"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_iterator_after:
      out << u8"ghost_iterator_after"sv;
      break;
  }

  return out;
}

template<typename Traits>
inline auto operator<<(std::basic_ostream<char16_t, Traits>& out, bplus_tree_leaf_use_element elem) -> std::basic_ostream<char16_t, Traits>& {
  using namespace std::literals;

  switch (elem) {
    default:
      {
        unsigned int elem_value = static_cast<std::underlying_type_t<bplus_tree_leaf_use_element>>(elem);
        out << u"earnest::detail::bplus_tree_leaf_use_element{"sv << elem_value << u"}"sv;
      }
      break;
    case bplus_tree_leaf_use_element::unused:
      out << u"unused"sv;
      break;
    case bplus_tree_leaf_use_element::used:
      out << u"used"sv;
      break;
    case bplus_tree_leaf_use_element::before_first:
      out << u"before_first"sv;
      break;
    case bplus_tree_leaf_use_element::after_last:
      out << u"after_last"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_create:
      out << u"ghost_create"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_delete:
      out << u"ghost_delete"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_iterator_before:
      out << u"ghost_iterator_before"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_iterator_after:
      out << u"ghost_iterator_after"sv;
      break;
  }

  return out;
}

template<typename Traits>
inline auto operator<<(std::basic_ostream<char32_t, Traits>& out, bplus_tree_leaf_use_element elem) -> std::basic_ostream<char32_t, Traits>& {
  using namespace std::literals;

  switch (elem) {
    default:
      {
        unsigned int elem_value = static_cast<std::underlying_type_t<bplus_tree_leaf_use_element>>(elem);
        out << U"earnest::detail::bplus_tree_leaf_use_element{"sv << elem_value << U"}"sv;
      }
      break;
    case bplus_tree_leaf_use_element::unused:
      out << U"unused"sv;
      break;
    case bplus_tree_leaf_use_element::used:
      out << U"used"sv;
      break;
    case bplus_tree_leaf_use_element::before_first:
      out << U"before_first"sv;
      break;
    case bplus_tree_leaf_use_element::after_last:
      out << U"after_last"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_create:
      out << U"ghost_create"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_delete:
      out << U"ghost_delete"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_iterator_before:
      out << U"ghost_iterator_before"sv;
      break;
    case bplus_tree_leaf_use_element::ghost_iterator_after:
      out << U"ghost_iterator_after"sv;
      break;
  }

  return out;
}


enum class iterator_direction {
  forward,
  reverse
};


struct bplus_tree_versions {
  using type = std::uint64_t;

  type page_split = 0, page_merge = 0, augment = 0;
};


struct bplus_tree_value_spec {
  bool byte_equality_enabled, byte_order_enabled;
  std::size_t bytes;
  std::function<std::strong_ordering(std::span<const std::byte>, std::span<const std::byte>)> compare;

  auto padded_bytes() const noexcept -> std::size_t {
    return round_up(bytes, 4u);
  }

  auto padding_bytes() const noexcept -> std::size_t {
    return padded_bytes() - bytes;
  }

  auto do_compare(std::span<const std::byte> x, std::span<const std::byte> y) const -> std::strong_ordering {
    if (byte_order_enabled) {
      // I don't have the `std::lexicographical_compare_three_way` function. :'(
      auto byte_comparison_fn = [](std::span<const std::byte> x, std::span<const std::byte> y) -> std::strong_ordering {
        auto [x_iter, y_iter] = std::mismatch(x.begin(), x.end(), y.begin(), y.end());
        if (x_iter == x.end())
          return (y_iter == y.end() ? std::strong_ordering::equal : std::strong_ordering::less);
        else if (y_iter == y.end())
          return std::strong_ordering::greater;
        else
          return (*x_iter <=> *y_iter);
      };

      std::strong_ordering o = byte_comparison_fn(x, y);
      if (o == std::strong_ordering::equal && !byte_equality_enabled)
        o = this->compare(x, y);
      return o;
    }

    if (byte_equality_enabled && std::equal(x.begin(), x.end(), y.begin(), y.end()))
      return std::strong_ordering::equal;

    return this->compare(x, y);
  }

  auto do_equal(std::span<const std::byte> x, std::span<const std::byte> y) const -> bool {
    if (byte_equality_enabled)
      return std::equal(x.begin(), x.end(), y.begin(), y.end());
    else
      return this->compare(x, y) == std::strong_ordering::equal;
  }
};


struct bplus_tree_augment_spec
: public bplus_tree_value_spec
{
  std::function<void(std::span<std::byte>, std::vector<std::span<const std::byte>>)> augment_merge;
  std::function<void(std::span<std::byte>, std::span<const std::byte>, std::span<const std::byte>)> augment_value;
};


struct bplus_tree_element_spec {
  bplus_tree_value_spec key;
  bplus_tree_value_spec value;
  std::vector<bplus_tree_augment_spec> augments;

  auto padded_augment_bytes() const noexcept -> std::size_t {
    return std::accumulate(
        augments.begin(), augments.end(),
        std::size_t(0),
        [](std::size_t x, const bplus_tree_augment_spec& y) noexcept -> std::size_t {
          return x + y.padded_bytes();
        });
  }

  auto key_compare(std::span<const std::byte> x, std::span<const std::byte> y) const -> std::strong_ordering {
    return key.do_compare(x, y);
  }

  auto key_equal(std::span<const std::byte> x, std::span<const std::byte> y) const -> bool {
    return key.do_equal(x, y);
  }
};


struct bplus_tree_spec {
  bplus_tree_element_spec element;
  std::size_t elements_per_leaf;
  std::size_t child_pages_per_intr;

  auto payload_bytes_per_leaf() const -> std::size_t {
    const auto use_list_bytes = round_up(elements_per_leaf, 4u);
    const auto bytes_per_value = element.key.padded_bytes() + element.value.padded_bytes();
    const auto payload_bytes = elements_per_leaf * bytes_per_value;
    return use_list_bytes + payload_bytes;
  }

  auto payload_bytes_per_intr() const -> std::size_t {
    const auto num_keys = child_pages_per_intr - 1u;
    const auto bytes_per_pointer = sizeof(std::uint64_t) + element.padded_augment_bytes();
    const auto payload_bytes = num_keys * element.key.padded_bytes() + child_pages_per_intr * bytes_per_pointer;
    return payload_bytes;
  }

  auto bytes_per_leaf() const -> std::size_t {
    using xdr_page_hdr = std::remove_cvref_t<decltype(::earnest::xdr_reader<>() & std::declval<bplus_tree_page_header&>())>;
    using xdr_leaf_hdr = std::remove_cvref_t<decltype(::earnest::xdr_reader<>() & std::declval<bplus_tree_leaf_header&>())>;
    constexpr std::size_t header_bytes = xdr_page_hdr::bytes.value() + xdr_leaf_hdr::bytes.value();

    return header_bytes + payload_bytes_per_leaf();
  }

  auto bytes_per_intr() const -> std::size_t {
    using xdr_page_hdr = std::remove_cvref_t<decltype(::earnest::xdr_reader<>() & std::declval<bplus_tree_page_header&>())>;
    using xdr_intr_hdr = std::remove_cvref_t<decltype(::earnest::xdr_reader<>() & std::declval<bplus_tree_intr_header&>())>;
    constexpr std::size_t header_bytes = xdr_page_hdr::bytes.value() + xdr_intr_hdr::bytes.value();

    return header_bytes + payload_bytes_per_intr();
  }

  auto key_compare(std::span<const std::byte> x, std::span<const std::byte> y) const -> std::strong_ordering {
    return element.key_compare(x, y);
  }

  auto key_equal(std::span<const std::byte> x, std::span<const std::byte> y) const -> bool {
    return element.key_equal(x, y);
  }
};


template<typename RawDbType> class bplus_tree_page;
template<typename RawDbType> class bplus_tree_intr;
template<typename RawDbType> class bplus_tree_leaf;
template<typename RawDbType, typename DBAllocator> class bplus_tree;


template<typename PageType>
struct bplus_tree_siblings {
  using pointer_type = cycle_ptr::cycle_gptr<PageType>;

  bplus_tree_siblings(pointer_type predecessor, pointer_type self, pointer_type successor) noexcept
  : predecessor(std::move(predecessor)),
    self(std::move(self)),
    successor(std::move(successor))
  {}

  template<bool ForWrite>
  auto lock_op([[maybe_unused]] std::bool_constant<ForWrite> for_write) const {
    using lock_type = std::conditional_t<ForWrite,
          typename monitor<typename PageType::executor_type, typename PageType::allocator_type>::upgrade_lock,
          typename monitor<typename PageType::executor_type, typename PageType::allocator_type>::shared_lock>;

    auto lock_op = [](pointer_type ptr, auto... locks) {
      return asio::deferred.when(ptr == nullptr)
          .then(asio::deferred.values(lock_type{}, locks...))
          .otherwise(
              [ ptr,
                deferred=asio::append(asio::deferred, locks...)
              ]() mutable {
                if constexpr(ForWrite)
                  return ptr->page_lock.dispatch_upgrade(std::move(deferred), __FILE__, __LINE__);
                else
                  return ptr->page_lock.dispatch_shared(std::move(deferred), __FILE__, __LINE__);
              });
    };

    return lock_op(predecessor)
    | asio::deferred(
        [self=this->self, lock_op](auto predecessor_lock) {
          return lock_op(self, std::move(predecessor_lock));
        })
    | asio::deferred(
        [successor=this->successor, lock_op](auto self_lock, auto predecessor_lock) {
          return lock_op(successor, std::move(self_lock), std::move(predecessor_lock));
        })
    | asio::deferred(
        [](auto successor_lock, auto self_lock, auto predecessor_lock) {
          return asio::deferred.values(
              std::array<lock_type, 3>{
                std::move(predecessor_lock),
                std::move(self_lock),
                std::move(successor_lock),
              });
        });
  }

  pointer_type predecessor, self, successor;
};


template<typename RawDbType, bool IsConst>
class bplus_element_reference {
  template<typename, bool> friend class bplus_element_reference;

  public:
  using value_type = std::conditional_t<
      IsConst,
      const typename bplus_tree_leaf<RawDbType>::element,
      typename bplus_tree_leaf<RawDbType>::element>;
  using pointer = cycle_ptr::cycle_gptr<value_type>;
  using reference = std::add_lvalue_reference_t<value_type>;

  constexpr bplus_element_reference() noexcept = default;

  explicit bplus_element_reference(pointer elem, const typename monitor<typename RawDbType::executor_type, typename RawDbType::allocator_type>::shared_lock& lock) noexcept
  : elem_(std::move(elem))
  {
    if (this->elem_ != nullptr) {
      assert(lock.holds_monitor(this->elem_->element_lock));
      this->elem_->use_count.fetch_add(1u, std::memory_order_relaxed); // relaxed memory order, because we hold the lock
    }
  }

  explicit bplus_element_reference(pointer elem, const typename monitor<typename RawDbType::executor_type, typename RawDbType::allocator_type>::upgrade_lock& lock) noexcept
  : elem_(std::move(elem))
  {
    if (this->elem_ != nullptr) {
      assert(lock.holds_monitor(this->elem_->element_lock));
      this->elem_->use_count.fetch_add(1u, std::memory_order_relaxed); // relaxed memory order, because we hold the lock
    }
  }

  explicit bplus_element_reference(pointer elem, const typename monitor<typename RawDbType::executor_type, typename RawDbType::allocator_type>::exclusive_lock& lock) noexcept
  : elem_(std::move(elem))
  {
    if (this->elem_ != nullptr) {
      assert(lock.holds_monitor(this->elem_->element_lock));
      this->elem_->use_count.fetch_add(1u, std::memory_order_relaxed); // relaxed memory order, because we hold the lock
    }
  }

  bplus_element_reference(const bplus_element_reference& y) noexcept
  : elem_(y.elem_)
  {
    if (this->elem_ != nullptr) {
      [[maybe_unused]] const auto old_use_count = elem_->use_count.fetch_add(1u, std::memory_order_relaxed);
      assert(old_use_count > 0);
    }
  }

  // Initialize const-reference from non-const-reference.
  template<bool Enable = IsConst, std::enable_if_t<Enable, int> = 0>
  bplus_element_reference(const bplus_element_reference<RawDbType, false>& y) noexcept
  : elem_(y.elem_)
  {
    if (this->elem_ != nullptr) {
      [[maybe_unused]] const auto old_use_count = elem_->use_count.fetch_add(1u, std::memory_order_relaxed);
      assert(old_use_count > 0);
    }
  }

  // Initialize const-reference from non-const-reference.
  template<bool Enable = IsConst, std::enable_if_t<Enable, int> = 0>
  bplus_element_reference(bplus_element_reference<RawDbType, false>&& y) noexcept
  : elem_(std::move(y.elem_))
  {
    if (this->elem_ != nullptr) assert(this->elem_->use_count.load() > 0);
  }

  bplus_element_reference(bplus_element_reference&& y) noexcept
  : elem_(std::move(y.elem_))
  {}

  auto operator=(bplus_element_reference y) noexcept -> bplus_element_reference& {
    this->swap(y);
    return *this;
  }

  ~bplus_element_reference() {
    reset();
  }

  template<typename Alloc, typename... Args>
  static auto allocate(Alloc&& alloc, Args&&... args) -> bplus_element_reference {
    bplus_element_reference r;
    pointer p = cycle_ptr::allocate_cycle<typename bplus_tree_leaf<RawDbType>::element>(std::forward<Alloc>(alloc), std::forward<Args>(args)...);

    [[maybe_unused]] auto old_use_count = p->use_count.fetch_add(1u, std::memory_order_relaxed);
    assert(old_use_count == 0);
    r.elem_ = std::move(p);

    return r;
  }

  auto operator*() const noexcept -> reference {
    return *elem_;
  }

  auto operator->() const noexcept -> const pointer& {
    return elem_;
  }

  auto get() const noexcept -> const pointer& {
    return elem_;
  }

  template<bool YIsConst>
  auto operator==(const bplus_element_reference<RawDbType, YIsConst>& y) const noexcept -> bool {
    return elem_ == y.elem_;
  }

  template<bool YIsConst>
  auto operator!=(const bplus_element_reference<RawDbType, YIsConst>& y) const noexcept -> bool {
    return elem_ != y.elem_;
  }

  auto reset() noexcept -> void {
    if (elem_ != nullptr) {
      [[maybe_unused]] const auto old_use_count = elem_->use_count.fetch_sub(1u, std::memory_order_release);
      assert(old_use_count > 0);
      elem_.reset();
    }
  }

  auto swap(bplus_element_reference& y) noexcept -> void {
    elem_.swap(y.elem_);
  }

  private:
  pointer elem_;
};

template<typename RawDbType, bool IsConst>
inline auto swap(bplus_element_reference<RawDbType, IsConst>& x, bplus_element_reference<RawDbType, IsConst>& y) noexcept -> void {
  x.swap(y);
}


template<typename RawDbType>
class bplus_tree_page
: public db_cache_value,
  protected cycle_ptr::cycle_base
{
  template<typename> friend struct bplus_tree_siblings;
  template<typename, typename> friend class bplus_tree;

  public:
  static inline constexpr std::uint64_t nil_page = -1;
  using raw_db_type = RawDbType;
  using executor_type = typename raw_db_type::executor_type;
  using allocator_type = typename raw_db_type::cache_allocator_type;

  private:
  using intr_type = bplus_tree_intr<RawDbType>;
  using leaf_type = bplus_tree_leaf<RawDbType>;

  public:
  explicit bplus_tree_page(std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address, const bplus_tree_page_header& h = bplus_tree_page_header{.parent=nil_page}) noexcept(std::is_nothrow_move_constructible_v<db_address>)
  : address(address),
    raw_db(raw_db),
    spec(std::move(spec)),
    page_lock(raw_db->get_executor(), "earnest::detail::bplus_tree_page{" + address.to_string() + "}", raw_db->get_allocator()),
    augment_propagation_required(h.augment_propagation_required),
    ex_(raw_db->get_executor()),
    alloc_(raw_db->get_cache_allocator()),
    parent_offset(h.parent)
  {}

  ~bplus_tree_page() override = default;

  auto get_executor() const -> executor_type { return ex_; }
  auto get_allocator() const -> allocator_type { return alloc_; }

  template<typename AcceptanceFn>
  static auto async_load_op(std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address, AcceptanceFn&& acceptance_fn) {
    return raw_db->async_get(
        typename raw_db_type::template key_type<bplus_tree_page>{.file=address.file, .offset=address.offset},
        asio::deferred,
        [](auto stream, std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address) {
          struct state {
            explicit state(decltype(stream)&& s)
            : s(std::move(s))
            {}

            decltype(stream) s;
          };

          auto state_ptr = std::make_shared<state>(std::move(stream));
          return raw_load_op(state_ptr->s, std::move(spec), std::move(raw_db), std::move(address))
          | asio::deferred(
              [state_ptr](std::error_code ec, cycle_ptr::cycle_gptr<bplus_tree_page> page) mutable {
                state_ptr.reset(); // Using `state_ptr` like this silences compiler warnings.
                return asio::deferred.values(std::move(ec), std::move(page));
              });
        },
        std::forward<AcceptanceFn>(acceptance_fn),
        spec, raw_db, address)
    | asio::deferred(
        [](std::error_code ec, cycle_ptr::cycle_gptr<bplus_tree_page> page) {
          using variant_type = std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>>;

          variant_type casted_page;
          if (!ec) {
            if (typeid(*page) == typeid(leaf_type)) {
              casted_page.template emplace<cycle_ptr::cycle_gptr<leaf_type>>(boost::polymorphic_pointer_downcast<leaf_type>(page));
            } else if (typeid(*page) == typeid(intr_type)) {
              casted_page.template emplace<cycle_ptr::cycle_gptr<intr_type>>(boost::polymorphic_pointer_downcast<intr_type>(page));
            } else [[unlikely]] {
              // Can only happen if there is a third type.
              ec = make_error_code(db_errc::cache_collision);
            }
          }

          return asio::deferred.values(ec, std::move(casted_page));
        });
  }

  // Write augment_propagation_required marker to disk, but don't update in-memory representation.
  template<typename TxAlloc, typename CompletionToken>
  auto async_set_augment_propagation_required_diskonly_op(typename raw_db_type::template fdb_transaction<TxAlloc> tx, CompletionToken&& token) {
    using tx_file_type = typename raw_db_type::template fdb_transaction<TxAlloc>::file;
    using stream_type = positional_stream_adapter<tx_file_type>;

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, std::shared_ptr<stream_type> fptr) {
          async_write(
              *fptr,
              ::earnest::xdr_writer<>() & xdr_constant(true).as(xdr_bool),
              completion_wrapper<void(std::error_code)>(
                  std::move(handler),
                  [fptr](auto handler, std::error_code ec) mutable {
                    fptr.reset();
                    return std::invoke(handler, ec);
                  }));
        },
        token, std::allocate_shared<stream_type>(tx.get_allocator(), tx[address.file], address.offset + bplus_tree_page_header::augment_propagation_required_offset));
  }

  // Write parent address to disk, but don't update in-memory representation.
  template<typename TxAlloc, typename CompletionToken>
  auto async_set_parent_diskonly_op(typename raw_db_type::template fdb_transaction<TxAlloc> tx, std::uint64_t parent, CompletionToken&& token) {
    using tx_file_type = typename raw_db_type::template fdb_transaction<TxAlloc>::file;
    using stream_type = positional_stream_adapter<tx_file_type>;

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, std::shared_ptr<stream_type> fptr, std::uint64_t parent) {
          async_write(
              *fptr,
              ::earnest::xdr_writer<>() & xdr_constant(parent).as(xdr_uint64),
              completion_wrapper<void(std::error_code)>(
                  std::move(handler),
                  [fptr](auto handler, std::error_code ec) mutable {
                    fptr.reset();
                    return std::invoke(handler, ec);
                  }));
        },
        token, std::allocate_shared<stream_type>(tx.get_allocator(), tx[address.file], address.offset + bplus_tree_page_header::parent_offset), std::move(parent));
  }

  private:
  template<typename Stream>
  static auto raw_load_op(Stream& stream, std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address) {
    auto result = std::make_shared<cycle_ptr::cycle_gptr<bplus_tree_page>>();
    return async_read(
        stream,
        ::earnest::xdr_reader<>() & xdr_manual(
            [spec, raw_db, address, result](bplus_tree_page_header h, auto& stream, auto callback) {
              switch (h.magic) {
                default:
                  std::invoke(callback, make_error_code(xdr_errc::decoding_error));
                  break;
                case intr_type::magic:
                  {
                    const auto ptr = cycle_ptr::allocate_cycle<intr_type>(raw_db->get_cache_allocator(), std::move(spec), raw_db, std::move(address), h);
                    *result = ptr;
                    ptr->continue_load_(stream, raw_db, std::move(callback));
                  }
                  break;
                case leaf_type::magic:
                  {
                    const auto ptr = cycle_ptr::allocate_cycle<leaf_type>(raw_db->get_cache_allocator(), std::move(spec), raw_db, std::move(address), h);
                    *result = ptr;
                    ptr->continue_load_(stream, raw_db, std::move(callback));
                  }
                  break;
              }
            }).template with_lead<bplus_tree_page_header>(),
        asio::deferred)
    | asio::deferred(
        [result](std::error_code ec) {
          return asio::deferred.values(ec, *result);
        });
  }

  public:
  auto parent_page_offset() const -> std::uint64_t {
    return this->parent_offset;
  }

  auto parent_page_address() const -> db_address {
    return db_address(this->address.file, parent_page_offset());
  }

  private:
  template<bool ForWrite>
  auto parent_page_impl_([[maybe_unused]] std::bool_constant<ForWrite> for_write, move_only_function<void(std::error_code, cycle_ptr::cycle_gptr<intr_type>, std::conditional_t<ForWrite, typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock>)> handler) const -> void {
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;
    using monitor_uplock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock;
    using bplus_tree_intr_ptr = cycle_ptr::cycle_gptr<intr_type>;
    using bplus_tree_leaf_ptr = cycle_ptr::cycle_gptr<leaf_type>;
    using ptr_type = std::variant<bplus_tree_intr_ptr, bplus_tree_leaf_ptr>;
    using target_lock_type = std::conditional_t<ForWrite, monitor_uplock_type, monitor_shlock_type>;

    // This is the same code as page_variant_op(),
    // except the lock order is reversed.
    struct op
    : public std::enable_shared_from_this<op>
    {
      op(cycle_ptr::cycle_gptr<const bplus_tree_page> self, move_only_function<void(std::error_code, bplus_tree_intr_ptr, target_lock_type)> handler)
      : self(std::move(self)),
        handler(std::move(handler))
      {}

      // Prevent move/copy, to prevent accidentally introducing bugs.
      op(const op&) = delete;
      op(op&&) = delete;

      auto operator()() -> void {
        self->page_lock.dispatch_shared(
            [self_op=this->shared_from_this()](monitor_shlock_type lock) mutable -> void {
              self_op->load_target_page(std::move(lock));
            },
            __FILE__, __LINE__);
      }

      private:
      auto load_target_page(monitor_shlock_type self_shlock) -> void {
        cycle_ptr::cycle_gptr<raw_db_type> raw_db = self->raw_db.lock();
        if (raw_db == nullptr || self->erased_) [[unlikely]] {
          error_invoke(make_error_code(db_errc::data_expired));
          return;
        }

        db_address target_addr = self->parent_page_address();
        self_shlock.reset(); // Release lock.

        // There is no parent, so we return without an error, and with a nullptr.
        if (target_addr.offset == nil_page) {
          std::invoke(handler, std::error_code{}, nullptr, target_lock_type{});
          return;
        }

        async_load_op(
            self->spec,
            std::move(raw_db),
            target_addr,
            [self_op=this->shared_from_this(), target_addr]() {
              return self_op->self->page_lock.dispatch_shared(asio::deferred, __FILE__, __LINE__)
              | asio::deferred(
                  [self_op, target_addr]([[maybe_unused]] monitor_shlock_type self_lock) {
                    std::error_code ec;
                    if (self_op->self->erased_ || target_addr != self_op->self->parent_page_address())
                      ec = make_error_code(bplus_tree_errc::restart);
                    return asio::deferred.values(ec);
                  });
            })
        | [self_op=this->shared_from_this()](std::error_code ec, ptr_type target_page) -> void {
            if (ec == make_error_code(bplus_tree_errc::restart)) {
              // Using asio::post, to prevent recursion from overflowing the stack space.
              asio::post(
                  self_op->self->get_executor(),
                  [self_op]() {
                    std::invoke(*self_op);
                  });
              return;
            }

            if (ec) {
              self_op->error_invoke(ec);
              return;
            }

            self_op->lock_target(std::move(target_page));
          };
      }

      auto lock_target(ptr_type target_page) -> void {
        const bplus_tree_page*const raw_ptr = get_raw_ptr(target_page);
        if constexpr(ForWrite) {
          raw_ptr->page_lock.dispatch_upgrade(
              [ target_page=std::move(target_page),
                self_op=this->shared_from_this()
              ](monitor_uplock_type target_lock) mutable {
                self_op->validate(std::move(target_page), std::move(target_lock));
              },
              __FILE__, __LINE__);
        } else {
          raw_ptr->page_lock.dispatch_shared(
              [ target_page=std::move(target_page),
                self_op=this->shared_from_this()
              ](monitor_shlock_type target_lock) mutable {
                self_op->validate(std::move(target_page), std::move(target_lock));
              },
              __FILE__, __LINE__);
        }
      }

      auto validate(ptr_type target_page, target_lock_type target_lock) -> void {
        self->page_lock.dispatch_shared(
            [ self_op=this->shared_from_this(),
              target_page=std::move(target_page),
              target_lock=std::move(target_lock)
            ](monitor_shlock_type self_lock) mutable {
              if (self_op->self->erased_) [[unlikely]] {
                self_op->error_invoke(make_error_code(db_errc::data_expired));
                return;
              }

              if (get_raw_ptr(target_page)->address != self_op->self->parent_page_address()) {
                // Restart.
                asio::post(
                    self_op->self->get_executor(),
                    [self_op]() {
                      std::invoke(*self_op);
                    });
                return;
              }

              self_lock.reset(); // No longer need this lock.
              std::visit(
                  overload(
                      [&](bplus_tree_intr_ptr&& target_page) {
                        std::invoke(self_op->handler, std::error_code(), std::move(target_page), std::move(target_lock));
                      },
                      [&](bplus_tree_leaf_ptr&& target_page) {
                        // Parent page cannot be a leaf, so this would be bad.
                        std::clog << "bplus-tree: bad-tree, parent page is a leaf\n";
                        target_page->maybe_log_dump("parent page");
                        self_op->error_invoke(make_error_code(bplus_tree_errc::bad_tree));
                      }),
                  std::move(target_page));
            },
            __FILE__, __LINE__);
      }

      static auto get_raw_ptr(const ptr_type& p) -> bplus_tree_page* {
        return std::visit(
            [](const auto& ptr) -> bplus_tree_page* {
              return ptr.get();
            },
            p);
      }

      auto error_invoke(std::error_code ec) -> void {
        std::invoke(handler, ec, nullptr, target_lock_type{});
      }

      const cycle_ptr::cycle_gptr<const bplus_tree_page> self;
      const move_only_function<void(std::error_code, bplus_tree_intr_ptr, target_lock_type)> handler;
    };

    std::invoke(*std::make_shared<op>(this->shared_from_this(this), std::move(handler)));
  }

  public:
  template<bool ForWrite, typename CompletionToken>
  auto async_parent_page([[maybe_unused]] std::bool_constant<ForWrite> for_write, CompletionToken&& token) {
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;
    using monitor_uplock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock;
    using bplus_tree_intr_ptr = cycle_ptr::cycle_gptr<intr_type>;
    using target_lock_type = std::conditional_t<ForWrite, monitor_uplock_type, monitor_shlock_type>;

    return asio::async_initiate<CompletionToken, void(std::error_code, bplus_tree_intr_ptr, target_lock_type)>(
        [](auto handler, cycle_ptr::cycle_gptr<const bplus_tree_page> self) {
          if constexpr(handler_has_executor_v<decltype(handler)>)
            self->parent_page_impl_(std::bool_constant<ForWrite>(), completion_handler_fun(std::move(handler), self->get_executor()));
          else
            self->parent_page_impl_(std::bool_constant<ForWrite>(), std::move(handler));
        },
        token, this->shared_from_this(this));
  }

  template<bool ParentForWrite, typename SiblingPageType = bplus_tree_page>
  auto sibling_page_op(std::bool_constant<ParentForWrite> parent_for_write) const {
    using siblings_type = bplus_tree_siblings<SiblingPageType>;
    using page_ptr = cycle_ptr::cycle_gptr<bplus_tree_page>;

    return async_parent_page(std::move(parent_for_write), asio::deferred)
    | asio::deferred(
        [self=this->shared_from_this(this)](std::error_code ec, cycle_ptr::cycle_gptr<intr_type> parent, auto parent_lock) {
          return asio::deferred.when(!ec && parent != nullptr)
              .then(
                  [self, parent, parent_lock]() {
                    assert(parent->contains(self->address));

                    const std::optional<std::size_t> self_idx = parent->find_index(self->address);
                    assert(self_idx.has_value());

                    std::optional<db_address> predecessor_address, successor_address;
                    if (self_idx.value() > 0u)
                      predecessor_address = parent->get_index(self_idx.value() - 1u);
                    successor_address = parent->get_index(self_idx.value() + 1u);

                    return asio::deferred.values(std::error_code(), parent, parent_lock, self, std::move(predecessor_address), std::move(successor_address));
                  })
              .otherwise(asio::deferred.values(ec, parent, parent_lock, self, std::optional<db_address>(), std::optional<db_address>()));
        })
    | asio::deferred(
        [](std::error_code ec, cycle_ptr::cycle_gptr<intr_type> parent, auto parent_lock, page_ptr self, std::optional<db_address> predecessor_address, std::optional<db_address> successor_address) {
          const bool need_to_load_things = (predecessor_address.has_value() || successor_address.has_value());

          cycle_ptr::cycle_gptr<raw_db_type> raw_db;
          if (!ec) {
            raw_db = self->raw_db.lock();
            if (raw_db == nullptr) [[unlikely]] ec = make_error_code(db_errc::data_expired);
          }

          const std::shared_ptr<std::array<page_ptr, 3>> state;
          if (!ec && need_to_load_things) {
            state = std::make_shared<std::array<page_ptr, 3>>();
            (*state)[1] = self;
          }

          return asio::deferred.when(!ec && need_to_load_things)
              .then(
                  asio::async_initiate<decltype(asio::deferred), void()>(
                      [state](auto handler, std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, std::optional<db_address> predecessor_address, std::optional<db_address> successor_address) -> void {
                        // We load the predecessor and successor in parallel.
                        auto barrier = make_completion_barrier(std::move(handler));

                        if (predecessor_address.has_value()) {
                          async_load_op(spec, raw_db, predecessor_address.value())
                          | completion_wrapper<void(std::error_code, std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>>)>(
                              ++barrier,
                              [state](auto handler, std::error_code ec, std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>> variant) {
                                if (!ec) {
                                  std::visit(
                                      [&state](auto ptr) {
                                        (*state)[0] = std::move(ptr);
                                      },
                                      std::move(variant));
                                }
                                std::invoke(handler, ec);
                              });
                        }
                        if (successor_address.has_value()) {
                          async_load_op(spec, raw_db, successor_address.value())
                          | completion_wrapper<void(std::error_code, std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>>)>(
                              ++barrier,
                              [state](auto handler, std::error_code ec, std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>> variant) {
                                if (!ec) {
                                  std::visit(
                                      [&state](auto ptr) {
                                        (*state)[2] = std::move(ptr);
                                      },
                                      std::move(variant));
                                }
                                std::invoke(handler, ec);
                              });
                        }
                        std::invoke(barrier, std::error_code());
                      },
                      asio::deferred,
                      self->spec, std::move(raw_db), predecessor_address, successor_address)
                  | asio::deferred(
                      [state, parent, parent_lock, self](std::error_code ec) mutable {
                        return asio::deferred.values(ec, std::move(parent), std::move(parent_lock), (*state)[0], (*state)[1], (*state)[2]);
                      }))
              .otherwise(asio::deferred.values(ec, parent, parent_lock, page_ptr(nullptr), self, page_ptr(nullptr)));
        })
    | asio::deferred(
        [](std::error_code ec, cycle_ptr::cycle_gptr<intr_type> parent, auto parent_lock, page_ptr predecessor, page_ptr self, page_ptr successor) {
          auto cast = [](std::error_code& ec, page_ptr ptr) {
            if constexpr(std::is_same_v<std::remove_cv_t<SiblingPageType>, bplus_tree_page>) {
              return ptr;
            } else {
              auto casted_ptr = std::dynamic_pointer_cast<SiblingPageType>(ptr);
              if (!ec && casted_ptr == nullptr && ptr != nullptr)
                ec = make_error_code(db_errc::cache_collision);
              return casted_ptr;
            }
          };

          auto siblings = siblings_type(cast(ec, predecessor), cast(ec, self), cast(ec, successor));
          assert(siblings.self != nullptr);

          return asio::deferred.values(std::move(ec), std::move(parent), std::move(parent_lock), std::move(siblings));
        });
  }

  // Must lock:
  // - this read-locked
  template<typename CompletionToken>
  auto async_compute_page_augments(CompletionToken&& token) const {
    return asio::async_initiate<CompletionToken, void(std::vector<std::byte>)>(
        [](auto handler, cycle_ptr::cycle_gptr<const bplus_tree_page> self) -> void {
          return self->async_compute_page_augments_impl_(std::move(handler));
        },
        token, this->shared_from_this(this));
  }

  auto async_fix_augment_background() -> void {
    auto raw_db = this->raw_db.lock();
    if (raw_db == nullptr) return;

    // We use the raw_db allocator, so that the memory usage is tracked,
    // but doesn't count against cache-memory
    // (similar to how what we do with the monitors).
    auto raw_db_allocator = raw_db->get_allocator();

    asio::defer(
        get_executor(),
        [self=this->shared_from_this(this), raw_db_allocator]() {
          self->page_lock.async_shared(
              [self, raw_db_allocator](auto lock) {
                self->async_fix_augment_impl_(
                    [](std::error_code ec) {
                      if (ec && ec != make_error_code(db_errc::data_expired))
                        std::clog << "bplus-tree: background augmentation update failed: " << ec.message() << "\n";
                    },
                    false,
                    raw_db_allocator,
                    std::move(lock));
              },
              __FILE__, __LINE__);
        });
  }

  template<typename TxAlloc, typename CompletionToken>
  auto async_fix_augment(TxAlloc tx_alloc, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock page_lock, CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree_page> self, TxAlloc tx_alloc, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock page_lock) {
          self->async_fix_augment_impl_(std::move(handler), true, std::move(tx_alloc), std::move(page_lock));
        },
        token, this->shared_from_this(this), std::move(tx_alloc), std::move(page_lock));
  }

  protected:
  auto on_load() noexcept -> void override {
    this->db_cache_value::on_load(); // Call parent.

    // Since we're not yet handed out, we can forego locks.
    if (augment_propagation_required) async_fix_augment_background();
  }

  private:
  template<typename TxAlloc>
  auto async_fix_augment_impl_(move_only_function<void(std::error_code)> handler, bool must_complete, TxAlloc tx_alloc, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock page_lock) -> void {
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;
    using monitor_uplock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock;
    using monitor_exlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock;
    using tx_type = typename raw_db_type::template fdb_transaction<TxAlloc>;

    assert(page_lock.holds_monitor(this->page_lock));

    struct op
    : public std::enable_shared_from_this<op>
    {
      op(cycle_ptr::cycle_gptr<bplus_tree_page> self, move_only_function<void(std::error_code)> handler, TxAlloc tx_alloc, bool must_complete)
      : self(std::move(self)),
        tx_alloc(std::move(tx_alloc)),
        handler(std::move(handler)),
        must_complete(must_complete)
      {}

      auto operator()() -> void {
        self->page_lock.dispatch_shared(
            [self_op=this->shared_from_this()](monitor_shlock_type lock) {
              std::invoke(*self_op, std::move(lock));
            },
            __FILE__, __LINE__);
      }

      auto operator()(monitor_shlock_type lock) -> void {
        if (self->erased_) [[unlikely]] {
          error_invoke(make_error_code(db_errc::data_expired));
        } else if (!self->augment_propagation_required) [[unlikely]] {
          lock.reset();
          self->async_parent_page(std::false_type(), asio::deferred)
          | [self_op=this->shared_from_this()](std::error_code ec, cycle_ptr::cycle_gptr<bplus_tree_page> parent, [[maybe_unused]] monitor_shlock_type parent_lock) {
              parent_lock.reset();
              if (ec)
                self_op->error_invoke(ec);
              else
                self_op->complete_invoke(parent);
            };
        } else {
          compute_augmentation(std::move(lock));
        }
      }

      private:
      auto compute_augmentation(monitor_shlock_type lock) -> void {
        self->async_compute_page_augments(asio::append(asio::deferred, std::move(lock)))
        | asio::deferred(
            [self_op=this->shared_from_this()](std::vector<std::byte> augment, monitor_shlock_type lock) {
              self_op->augment = std::move(augment);
              self_op->augment_version = self_op->self->versions.augment;
              lock.reset(); // No longer need the lock.
              return asio::deferred.values();
            })
        | [self_op=this->shared_from_this()]() {
            auto raw_db = self_op->self->raw_db.lock();
            if (raw_db == nullptr) [[unlikely]] {
              self_op->error_invoke(make_error_code(db_errc::data_expired));
              return;
            }

            self_op->self->async_parent_page(std::true_type(), asio::deferred)
            | [self_op](std::error_code ec, cycle_ptr::cycle_gptr<bplus_tree_intr<RawDbType>> parent, monitor_uplock_type parent_lock) mutable {
                if (ec) {
                  self_op->error_invoke(ec);
                  return;
                }
                if (parent == nullptr) {
                  self_op->self->page_lock.dispatch_exclusive(
                      [self_op](monitor_exlock_type self_lock) {
                        // We don't clear this bit if the parent has acquired a parent
                        // between our earlier conclusion of not having a parent, and now.
                        if (self_op->self->parent_page_offset() == nil_page)
                          self_op->self->augment_propagation_required = false;

                        // We're done. Even if the page acquired a parent in the meantime.
                        self_lock.reset();
                        self_op->complete_invoke(nullptr);
                      },
                      __FILE__, __LINE__);
                  return;
                }

                self_op->store_augment(std::move(parent), std::move(parent_lock));
              };
          };
      }

      auto store_augment(cycle_ptr::cycle_gptr<bplus_tree_intr<RawDbType>> parent, monitor_uplock_type parent_lock) -> void {
        auto raw_db = self->raw_db.lock();
        if (raw_db == nullptr) [[unlikely]] {
          error_invoke(make_error_code(db_errc::data_expired));
          return;
        }

        const std::optional<std::size_t> offset = parent->find_index(self->address);
        if (!offset.has_value()) {
          std::clog << "bplus-tree: bad-tree, child-page not found within parent page\n";
          self->maybe_log_dump("child page");
          parent->maybe_log_dump("parent page");
          error_invoke(make_error_code(bplus_tree_errc::bad_tree));
          return;
        }

        const auto existing_augment = parent->augment_span(offset.value());
        if (std::equal(existing_augment.begin(), existing_augment.end(), this->augment.cbegin(), this->augment.cend())) {
          self->page_lock.dispatch_exclusive(
              [parent=std::move(parent), parent_lock=std::move(parent_lock), self_op=this->shared_from_this()](monitor_exlock_type self_lock) mutable {
                if (self_op->self->versions.augment != self_op->augment_version) {
                  // Restart, because augment got updated from under us.
                  self_lock.reset();
                  parent_lock.reset();
                  std::invoke(*self_op);
                } else {
                  self_op->self->augment_propagation_required = false;
                  self_lock.reset();
                  parent_lock.reset();
                  self_op->complete_invoke(parent);
                }
              },
              __FILE__, __LINE__);
          return;
        }

        tx_type tx = raw_db->fdb_tx_begin(isolation::read_commited, tx_mode::read_write, tx_alloc);
        parent->async_set_augments_diskonly_op(
            tx, offset.value(), std::span<const std::byte>(this->augment),
            [ self_op=this->shared_from_this(),
              parent_lock=std::move(parent_lock),
              offset=offset.value(),
              parent, tx
            ](std::error_code ec) {
              if (ec) {
                self_op->error_invoke(ec);
              } else {
                self_op->self->page_lock.dispatch_upgrade(
                    [ parent_lock=std::move(parent_lock),
                      self_op, tx, parent, offset
                    ](monitor_uplock_type self_uplock) {
                      self_op->commit(tx, std::move(parent), std::move(parent_lock), std::move(self_uplock), offset);
                    },
                    __FILE__, __LINE__);
              }
            });
      }

      auto commit(tx_type tx, cycle_ptr::cycle_gptr<bplus_tree_intr<RawDbType>> parent, monitor_uplock_type parent_lock, monitor_uplock_type self_lock, std::size_t offset) -> void {
        if (self->versions.augment != augment_version) {
          // Restart, because the augment got altered from under us.
          asio::post(
              self->get_executor(),
              [self_op=this->shared_from_this()]() {
                std::invoke(*self_op);
              });
          return;
        }

        tx.async_commit(asio::deferred)
        | asio::deferred(
            [parent_lock=std::move(parent_lock)](std::error_code ec) {
              return asio::deferred.when(!ec)
                  .then(std::move(parent_lock).dispatch_exclusive(asio::append(asio::deferred, ec)))
                  .otherwise(asio::deferred.values(monitor_exlock_type{}, ec));
            })
        | asio::deferred(
            [self_lock=std::move(self_lock)](monitor_exlock_type parent_exlock, std::error_code ec) {
              return asio::deferred.when(!ec)
                  .then(self_lock.dispatch_exclusive(asio::append(asio::deferred, std::move(parent_exlock), ec)))
                  .otherwise(asio::deferred.values(monitor_exlock_type{}, monitor_exlock_type{}, ec));
            })
        | [self_op=this->shared_from_this(), offset, parent=std::move(parent)](monitor_exlock_type self_exlock, monitor_exlock_type parent_exlock, std::error_code ec) {
            if (ec) {
              self_op->error_invoke(ec);
              return;
            }

            const auto existing_augment = parent->augment_span(offset);
            assert(self_op->augment.size() == existing_augment.size());
            std::copy(self_op->augment.cbegin(), self_op->augment.cend(), existing_augment.begin());
            ++parent->versions.augment;
            parent_exlock.reset();

            self_op->self->augment_propagation_required = false;
            self_exlock.reset();

            self_op->complete_invoke(parent);
          };
      }

      auto error_invoke(std::error_code ec) -> void {
        std::invoke(handler, ec);
      }

      auto complete_invoke(cycle_ptr::cycle_gptr<bplus_tree_page> parent) -> void {
        if (!must_complete || parent == nullptr) {
          std::invoke(handler, std::error_code());
          return;
        }

        this->self = std::move(parent);
        // Restart, but at the parent.
        asio::post(
            self->get_executor(),
            [self_op=this->shared_from_this()]() {
              std::invoke(*self_op);
            });
      }

      cycle_ptr::cycle_gptr<bplus_tree_page> self;
      TxAlloc tx_alloc;
      const move_only_function<void(std::error_code)> handler;
      std::vector<std::byte> augment;
      bplus_tree_versions::type augment_version;
      const bool must_complete;
    };

    std::invoke(*std::allocate_shared<op>(tx_alloc, this->shared_from_this(this), std::move(handler), tx_alloc, must_complete), std::move(page_lock));
  }

  virtual auto async_compute_page_augments_impl_(move_only_function<void(std::vector<std::byte>)> callback) const -> void = 0;

  protected:
  virtual auto log_dump_impl_(std::ostream& out) const -> void {
    using namespace std::literals;

    out << "  address: "sv << address << "\n"sv
        << "  versions: {\n"sv
        << "    page_split: "sv << versions.page_split << "\n"sv
        << "    page_merge: "sv << versions.page_merge << "\n"sv
        << "    augment:    "sv << versions.augment << "\n"sv
        << "  }\n"sv
        << "  raw_db: "sv << raw_db.lock() << "\n"sv
        << "  spec{\n"sv
        << "    elements_per_leaf: "sv << spec->elements_per_leaf << "\n"sv
        << "    child_pages_per_intr: "sv << spec->child_pages_per_intr << "\n"sv
        << "    element: {\n"sv
        << "      key: {\n"sv
        << "        bytes: "sv << spec->element.key.bytes << "\n"sv
        << "        byte_equality_enabled: "sv << spec->element.key.byte_equality_enabled << "\n"sv
        << "        byte_order_enabled: "sv << spec->element.key.byte_order_enabled << "\n"sv
        << "      }\n"sv
        << "      value: {\n"sv
        << "        bytes: "sv << spec->element.value.bytes << "\n"sv
        << "        byte_equality_enabled: "sv << spec->element.value.byte_equality_enabled << "\n"sv
        << "        byte_order_enabled: "sv << spec->element.value.byte_order_enabled << "\n"sv
        << "      }\n"sv;

    if (spec->element.augments.empty()) {
      out << "      augments: []\n"sv;
    } else {
      out << "      augments: [\n"sv;
      for (const auto& augment : spec->element.augments) {
        out << "        { bytes: "sv << augment.bytes << "\n"sv
            << "          byte_equality_enabled: "sv << augment.byte_equality_enabled << "\n"sv
            << "          byte_order_enabled: "sv << augment.byte_order_enabled << "\n"sv
            << "        }\n"sv;
      }
      out << "      ]\n"sv;
    }

    out << "    }\n"sv
        << "    bytes_per_leaf: "sv << spec->bytes_per_leaf() << "\n"sv
        << "    bytes_per_intr: "sv << spec->bytes_per_intr() << "\n"sv
        << "  }\n"sv
        << "  erased: " << erased_ << "\n"sv
        << "  augment_propagation_required: " << augment_propagation_required << "\n"sv
        << "  parent_offset: " << parent_offset << "\n"sv;
  }

  public:
  auto log_dump(std::ostream& out) const -> void {
    log_dump_impl_(out);
  }

  auto maybe_log_dump([[maybe_unused]] std::string_view prefix = std::string_view()) -> void {
#ifndef NDEBUG
    using namespace std::literals;

    if (!prefix.empty()) std::clog << prefix << " "sv;
    log_dump(std::clog);
#endif
  }

  protected:
  template<bool ForWrite>
  auto intr_page_op(std::bool_constant<ForWrite> for_write, move_only_function<std::variant<std::error_code, db_address>()> get_target_addr_fn) const {
    return page_variant_op(std::move(for_write), std::move(get_target_addr_fn))
    | variant_to_pointer_op<intr_type>();
  }

  template<typename PageType>
  static auto variant_to_pointer_op() {
    return asio::deferred(
        [](std::error_code ec, auto ptr_variant, auto... args) {
          cycle_ptr::cycle_gptr<PageType> ptr;
          if (!ec) {
            if (std::holds_alternative<cycle_ptr::cycle_gptr<PageType>>(ptr_variant))
              ptr = std::get<cycle_ptr::cycle_gptr<PageType>>(ptr_variant);
            else
              ec = make_error_code(xdr_errc::decoding_error);
          }
          return asio::deferred.values(std::move(ec), std::move(ptr), std::move(args)...);
        });
  }

  template<bool ForWrite>
  auto page_variant_op([[maybe_unused]] std::bool_constant<ForWrite> for_write, move_only_function<std::variant<std::error_code, db_address>()> get_target_addr_fn) const {
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;
    using monitor_uplock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock;
    using bplus_tree_intr_ptr = cycle_ptr::cycle_gptr<intr_type>;
    using bplus_tree_leaf_ptr = cycle_ptr::cycle_gptr<leaf_type>;
    using ptr_type = std::variant<bplus_tree_intr_ptr, bplus_tree_leaf_ptr>;
    using target_lock_type = std::conditional_t<ForWrite, monitor_uplock_type, monitor_shlock_type>;

    struct op
    : public std::enable_shared_from_this<op>
    {
      op(cycle_ptr::cycle_gptr<bplus_tree_page> self,
          move_only_function<std::variant<std::error_code, db_address>()> get_target_addr_fn,
          move_only_function<void(std::error_code, ptr_type, target_lock_type)> handler)
      : self(std::move(self)),
        get_target_addr_fn(std::move(get_target_addr_fn)),
        handler(std::move(handler))
      {}

      // Prevent move/copy, to prevent accidentally introducing bugs.
      op(const op&) = delete;
      op(op&&) = delete;

      auto operator()() -> void {
        self->page_lock.dispatch_shared(
            [self_op=this->shared_from_this()](monitor_shlock_type lock) mutable -> void {
              self_op->load_target_page(std::move(lock));
            },
            __FILE__, __LINE__);
      }

      private:
      auto load_target_page(monitor_shlock_type self_shlock) -> void {
        cycle_ptr::cycle_gptr<raw_db_type> raw_db = this->raw_db.lock();
        if (raw_db == nullptr || self->erased_) [[unlikely]] {
          error_invoke(make_error_code(db_errc::data_expired));
          return;
        }

        std::variant<std::error_code, db_address> addr_or_error = get_target_addr_fn();
        if (std::holds_alternative<std::error_code>(addr_or_error)) {
          assert(!!std::get<std::error_code>(addr_or_error));
          error_invoke(std::get<std::error_code>(addr_or_error));
          return;
        }
        db_address target_addr = std::get<db_address>(std::move(addr_or_error));
        self_shlock.reset();

        async_load_op(
            self->spec,
            std::move(raw_db),
            target_addr,
            [self_op=this->shared_from_this(), target_addr]() mutable {
              return self_op->self->page_lock.dispatch_shared(asio::append(asio::deferred, std::move(self_op), std::move(target_addr)), __FILE__, __LINE__)
              | asio::deferred(
                  []([[maybe_unused]] monitor_shlock_type self_lock, std::shared_ptr<op> self_op, db_address target_addr) {
                    std::error_code ec;
                    if (self_op->self->erased_) {
                      ec = make_error_code(bplus_tree_errc::restart);
                    } else {
                      std::visit(
                          overload(
                              [&ec]([[maybe_unused]] const std::error_code& reload_ec) {
                                ec = make_error_code(bplus_tree_errc::restart);
                              },
                              [&ec, &target_addr](const db_address& reload_addr) {
                                if (reload_addr != target_addr)
                                  ec = make_error_code(bplus_tree_errc::restart);
                              }),
                          self_op->get_target_addr_fn());
                    }
                    return asio::deferred.values(ec);
                  });
            })
        | [self_op=this->shared_from_this()](std::error_code ec, ptr_type target_page) -> void {
            if (ec == make_error_code(bplus_tree_errc::restart)) {
              // Using asio::post, to prevent recursion from overflowing the stack space.
              asio::post(
                  self_op->get_executor(),
                  [self_op]() {
                    std::invoke(self_op);
                  });
              return;
            }

            if (ec) {
              self_op->error_invoke(ec);
              return;
            }

            self_op->lock_target(std::move(target_page));
          };
      }

      auto lock_target(ptr_type target_page) -> void {
        self->page_lock.dispatch_shared(
            [ target_page=std::move(target_page),
              self_op=this->shared_from_this()
            ](monitor_shlock_type self_lock) mutable {
              const bplus_tree_page*const raw_ptr = get_raw_ptr(target_page);
              if constexpr(ForWrite) {
                raw_ptr->page_lock.dispatch_upgrade(
                    [ target_page=std::move(target_page),
                      self_lock=std::move(self_lock),
                      self_op=std::move(self_op)
                    ](monitor_uplock_type target_lock) mutable {
                      self_op->validate(std::move(target_page), std::move(target_lock), std::move(self_lock));
                    });
              } else {
                raw_ptr->page_lock.dispatch_shared(
                    [ target_page=std::move(target_page),
                      self_lock=std::move(self_lock),
                      self_op=std::move(self_op)
                    ](monitor_shlock_type target_lock) mutable {
                      self_op->validate(std::move(target_page), std::move(target_lock), std::move(self_lock));
                    });
              }
            },
            __FILE__, __LINE__);
      }

      auto validate(ptr_type target_page, target_lock_type target_lock, monitor_shlock_type self_lock) -> void {
        if (self->erased_) [[unlikely]] {
          error_invoke(make_error_code(db_errc::data_expired));
          return;
        }

        bool need_restart = false;
        std::error_code ec = std::visit(
            overload(
                []([[maybe_unused]] const std::error_code& reload_ec) -> std::error_code {
                  assert(!!reload_ec);
                  return reload_ec;
                },
                [&need_restart, &target_page](const db_address& reload_addr) -> std::error_code {
                  if (reload_addr != get_raw_ptr(target_page)->address)
                    need_restart = true;
                  return {};
                }),
            get_target_addr_fn());
        if (ec) {
          error_invoke(ec);
          return;
        }

        if (need_restart) {
          // Restart.
          asio::post(
              self->get_executor(),
              [self_op=this->shared_from_this()]() {
                std::invoke(self_op);
              });
          return;
        }

        self_lock.reset(); // No longer need this lock.
        std::invoke(handler, std::error_code(), std::move(target_page), std::move(target_lock));
      }

      static auto get_raw_ptr(const ptr_type& p) -> bplus_tree_page* {
        return std::visit(
            [](const auto& ptr) -> bplus_tree_page* {
              return ptr;
            },
            p);
      }

      auto error_invoke(std::error_code ec) -> void {
        std::invoke(handler, ec, ptr_type{}, target_lock_type{});
      }

      const cycle_ptr::cycle_gptr<const bplus_tree_page> self;
      const move_only_function<std::variant<std::error_code, db_address>()> get_target_addr_fn;
      const move_only_function<void(std::error_code, ptr_type, target_lock_type)> handler;
    };

    return asio::async_initiate<decltype(asio::deferred), void(std::error_code, ptr_type, target_lock_type)>(
        [](auto handler, cycle_ptr::cycle_gptr<const bplus_tree_page> self, auto get_target_addr_fn) {
          if constexpr(handler_has_executor_v<decltype(handler)>)
            std::invoke(std::make_shared<op>(self, std::move(get_target_addr_fn), completion_handler_fun(std::move(handler), self->get_executor())));
          else
            std::invoke(std::make_shared<op>(std::move(self), std::move(get_target_addr_fn), std::move(handler)));
        },
        asio::deferred, this->shared_from_this(this), std::move(get_target_addr_fn));
  }

  auto combine_augments(std::vector<std::span<const std::byte>> augments) const -> std::vector<std::byte> {
    auto combined_augments = std::vector<std::byte>(this->spec->element.padded_augment_bytes(), std::byte(0));
    std::span<std::byte> data_span = std::span(combined_augments);

    for (const bplus_tree_augment_spec& spec : this->spec->element.augments) {
      auto fn_arg = std::vector<std::span<const std::byte>>(augments.size());
      std::transform(
          augments.begin(), augments.end(),
          fn_arg.begin(),
          [&spec](std::span<const std::byte>& input) {
            assert(input.size() >= spec.padded_bytes());
            auto this_augment = input.subspan(0, spec.bytes);
            input = input.subspan(spec.padded_bytes());
            return this_augment;
          });
      spec.augment_merge(data_span.subspan(spec.bytes), std::move(fn_arg));
      data_span = data_span.subspan(spec.padded_bytes());
    }
    assert(data_span.empty());
    assert(std::all_of(augments.begin(), augments.end(), [](const std::span<const std::byte>& s) { return s.empty(); }));

    return combined_augments;
  }

  public:
  const db_address address;
  bplus_tree_versions versions;

  protected:
  const cycle_ptr::cycle_weak_ptr<raw_db_type> raw_db;
  const std::shared_ptr<const bplus_tree_spec> spec;
  mutable monitor<executor_type, typename raw_db_type::allocator_type> page_lock;
  bool erased_ = false;
  bool augment_propagation_required = false;

  private:
  executor_type ex_;
  allocator_type alloc_;
  std::uint64_t parent_offset = nil_page;
};


/* Interior page.
 *
 * Points at more pages.
 * Has a size >= 1.
 * For each elements:
 * - has a child-page offset.
 * - has a augmentation for the pointed-to page.
 *
 * Has size-1 keys.
 * For key[N]:
 * - elements in child[0..N] (inclusive) have keys <= key[N]
 * - elements in child[N..size-1] have keys >= key[N]
 */
template<typename RawDbType>
class bplus_tree_intr final
: public bplus_tree_page<RawDbType>
{
  template<typename> friend class bplus_tree_page;
  template<typename, typename> friend class bplus_tree;

  public:
  static inline constexpr std::uint32_t magic = 0x237d'bf9aU;
  using raw_db_type = typename bplus_tree_page<RawDbType>::raw_db_type;
  using executor_type = typename bplus_tree_page<RawDbType>::executor_type;
  using allocator_type = typename bplus_tree_page<RawDbType>::allocator_type;

  private:
  template<typename T> using rebind_alloc = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;

  public:
  bplus_tree_intr(std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address, const bplus_tree_page_header& h = bplus_tree_page_header{.parent=bplus_tree_page<RawDbType>::nil_page})
  : bplus_tree_page<RawDbType>(spec, std::move(raw_db), std::move(address), h),
    bytes_(spec->payload_bytes_per_intr(), this->get_allocator())
  {
    assert(spec->payload_bytes_per_intr() == key_offset(spec->child_pages_per_intr - 1u));
  }

  ~bplus_tree_intr() override = default;

  template<typename TxAlloc, typename DbAlloc, typename CompletionToken>
  static auto async_new_page(
      typename raw_db_type::template fdb_transaction<TxAlloc> tx,
      DbAlloc db_alloc,
      std::shared_ptr<const bplus_tree_spec> spec,
      cycle_ptr::cycle_gptr<raw_db_type> raw_db,
      std::uint64_t parent_offset,
      std::uint64_t child_offset,
      std::span<const std::byte> child_augment,
      CompletionToken&& token) {
    using tx_file_type = typename raw_db_type::template fdb_transaction<TxAlloc>::file;
    using stream_type = positional_stream_adapter<tx_file_type>;
    using page_type = bplus_tree_page<RawDbType>;

    return asio::async_initiate<CompletionToken, void(std::error_code, cycle_ptr::cycle_gptr<bplus_tree_intr>)>(
        [](auto handler, typename raw_db_type::template fdb_transaction<TxAlloc> tx, DbAlloc db_alloc, std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, std::uint64_t parent_offset, std::uint64_t child_offset, std::span<const std::byte> child_augment) -> void {
          db_alloc.async_allocate(tx, spec->bytes_per_intr(), asio::deferred)
          | asio::deferred(
              [tx, raw_db, spec, parent_offset, child_offset, child_augment](std::error_code ec, db_address addr) mutable {
                cycle_ptr::cycle_gptr<bplus_tree_intr> new_page;
                if (!ec) {
                  new_page = cycle_ptr::allocate_cycle<bplus_tree_intr>(raw_db->get_cache_allocator(), spec, raw_db, std::move(addr), bplus_tree_page_header{.parent=parent_offset});
                  new_page->hdr_.size = 0u;

                  if (child_offset != bplus_tree_page<RawDbType>::nil_page) {
                    new_page->hdr_.size = 1u;

                    { // Copy child offset into the new page.
                      std::uint64_t child_offset_be = child_offset;
                      boost::endian::native_to_big_inplace(child_offset_be);
                      auto child_offset_be_span = std::as_bytes(std::span<std::uint64_t, 1>(&child_offset_be, 1));
                      auto new_element_span = new_page->element_span(0);
                      if (new_element_span.size() != child_offset_be_span.size())
                        ec = make_error_code(xdr_errc::encoding_error);
                      else
                        std::copy(child_offset_be_span.begin(), child_offset_be_span.end(), new_element_span.begin());
                    }

                    { // Copy augment into the new page.
                      auto new_augment_span = new_page->augment_span(0);
                      if (new_augment_span.size() != child_augment.size()) [[unlikely]]
                        ec = make_error_code(xdr_errc::encoding_error);
                      else
                        std::copy(child_augment.begin(), child_augment.end(), new_augment_span.begin());
                    }
                  }

                  tx.on_commit(
                      [raw_db, new_page]() {
                        raw_db->template emplace<page_type>(
                            typename raw_db_type::template key_type<page_type>{
                              .file=new_page->address.file,
                              .offset=new_page->address.offset
                            },
                            new_page);
                      });
                }

                return asio::deferred.when(!ec)
                    .then(
                        asio::deferred.values(tx, new_page)
                        | asio::deferred(
                            [](typename raw_db_type::template fdb_transaction<TxAlloc> tx, cycle_ptr::cycle_gptr<bplus_tree_intr> new_page) {
                              auto stream = std::allocate_shared<stream_type>(tx.get_allocator(), tx[new_page->address.file], new_page->address.offset);
                              return async_write(
                                  *stream,
                                  (::earnest::xdr_writer<>() & xdr_constant(bplus_tree_page_header{.magic=magic, .parent=new_page->parent_page_offset()}))
                                  + bplus_tree_intr::continue_xdr_(::earnest::xdr_writer<>(), *new_page),
                                  asio::append(asio::deferred, new_page, stream))
                              | asio::deferred(
                                  [](std::error_code ec, cycle_ptr::cycle_gptr<bplus_tree_intr> new_page, [[maybe_unused]] auto stream) {
                                    return asio::deferred.values(std::move(ec), std::move(new_page));
                                  });
                            }))
                    .otherwise(asio::deferred.values(ec, new_page));
              })
          | std::move(handler);
        },
        token, std::move(tx), std::move(db_alloc), std::move(spec), std::move(raw_db), std::move(parent_offset), std::move(child_offset), std::move(child_augment));
  }

  static constexpr std::size_t xdr_page_hdr_bytes = std::remove_cvref_t<decltype(::earnest::xdr_reader<>() & std::declval<bplus_tree_page_header&>())>::bytes.value();
  static constexpr std::size_t xdr_hdr_bytes = std::remove_cvref_t<decltype(::earnest::xdr_reader<>() & std::declval<bplus_tree_intr_header&>())>::bytes.value();

  // Write header to disk, but don't update in-memory representation.
  template<typename TxAlloc, typename CompletionToken>
  auto async_set_hdr_diskonly_op(typename raw_db_type::template fdb_transaction<TxAlloc> tx, bplus_tree_intr_header hdr, CompletionToken&& token) {
    using tx_file_type = typename raw_db_type::template fdb_transaction<TxAlloc>::file;
    using stream_type = positional_stream_adapter<tx_file_type>;

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, std::shared_ptr<stream_type> fptr, bplus_tree_intr_header hdr) -> void {
          async_write(
              *fptr,
              ::earnest::xdr_writer<>() & xdr_constant(std::move(hdr)),
              completion_wrapper<void(std::error_code)>(
                  std::move(handler),
                  [fptr](auto handler, std::error_code ec) mutable {
                    fptr.reset();
                    return std::invoke(handler, ec);
                  }));
        },
        token, std::allocate_shared<stream_type>(tx.get_allocator(), tx[this->address.file], this->address.offset + xdr_page_hdr_bytes), std::move(hdr));
  }

  // Write elements to disk, but don't update in-memory representation.
  template<typename TxAlloc, typename CompletionToken>
  auto async_set_elements_diskonly_op(typename raw_db_type::template fdb_transaction<TxAlloc> tx, std::size_t offset, std::span<const std::byte> elements, CompletionToken&& token) {
    using tx_file_type = typename raw_db_type::template fdb_transaction<TxAlloc>::file;

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree_intr> self, std::shared_ptr<tx_file_type> fptr, std::size_t offset, std::span<const std::byte> elements) -> void {
          if (offset >= self->spec->child_pages_per_intr)
            throw std::invalid_argument("bad offset for element write");
          if (elements.size() > (self->spec->child_pages_per_intr - offset) * self->element_length())
            throw std::invalid_argument("too many bytes for element write");

          return asio::async_write_at(
              *fptr,
              self->address.offset + xdr_page_hdr_bytes + xdr_hdr_bytes + self->element_offset(offset),
              asio::buffer(elements),
              completion_wrapper<void(std::error_code, std::size_t)>(
                  std::move(handler),
                  [fptr](auto handler, std::error_code ec, [[maybe_unused]] std::size_t nbytes) mutable {
                    fptr.reset();
                    return std::invoke(handler, ec);
                  }));
        },
        token, this->shared_from_this(this), std::allocate_shared<tx_file_type>(tx.get_allocator(), tx[this->address.file]), std::move(offset), std::move(elements));
  }

  // Write augments to disk, but don't update in-memory representation.
  template<typename TxAlloc, typename CompletionToken>
  auto async_set_augments_diskonly_op(typename raw_db_type::template fdb_transaction<TxAlloc> tx, std::size_t offset, std::span<const std::byte> augments, CompletionToken&& token) {
    using tx_file_type = typename raw_db_type::template fdb_transaction<TxAlloc>::file;

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree_intr> self, std::shared_ptr<tx_file_type> fptr, std::size_t offset, std::span<const std::byte> augments) -> void {
          if (offset >= self->spec->child_pages_per_intr)
            throw std::invalid_argument("bad offset for augment write");
          if (augments.size() > (self->spec->child_pages_per_intr - offset) * self->augment_length())
            throw std::invalid_argument("too many bytes for augment write");

          return asio::async_write_at(
              *fptr,
              self->address.offset + xdr_page_hdr_bytes + xdr_hdr_bytes + self->augment_offset(offset),
              asio::buffer(augments),
              completion_wrapper<void(std::error_code, std::size_t)>(
                  std::move(handler),
                  [fptr](auto handler, std::error_code ec, [[maybe_unused]] std::size_t nbytes) mutable {
                    fptr.reset();
                    return std::invoke(handler, ec);
                  }));
        },
        token, this->shared_from_this(this), std::allocate_shared<tx_file_type>(tx.get_allocator(), tx[this->address.file]), std::move(offset), std::move(augments));
  }

  // Write keys to disk, but don't update in-memory representation.
  template<typename TxAlloc, typename CompletionToken>
  auto async_set_keys_diskonly_op(typename raw_db_type::template fdb_transaction<TxAlloc> tx, std::size_t offset, std::span<const std::byte> keys, CompletionToken&& token) {
    using tx_file_type = typename raw_db_type::template fdb_transaction<TxAlloc>::file;

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree_intr> self, std::shared_ptr<tx_file_type> fptr, std::size_t offset, std::span<const std::byte> keys) {
          if (offset >= self->spec->child_pages_per_intr)
            throw std::invalid_argument("bad offset for key write");
          if (keys.size() > (self->spec->child_pages_per_intr - offset) * self->key_length())
            throw std::invalid_argument("too many bytes for key write");

          return asio::async_write_at(
              *fptr,
              self->address.offset + xdr_page_hdr_bytes + xdr_hdr_bytes + self->key_offset(offset),
              asio::buffer(keys),
              completion_wrapper<void(std::error_code, std::size_t)>(
                  std::move(handler),
                  [fptr](auto handler, std::error_code ec, [[maybe_unused]] std::size_t nbytes) mutable {
                    fptr.reset();
                    return std::invoke(handler, ec);
                  }));
        },
        token, this->shared_from_this(this), std::allocate_shared<tx_file_type>(tx.get_allocator(), tx[this->address.file]), std::move(offset), std::move(keys));
  }

  // Must hold page_lock.
  auto contains(const db_address& child) const noexcept -> bool {
    for (std::size_t i = 0; i < hdr_.size; ++i) {
      if (element(i) == child.offset)
        return child.file == this->address.file;
    }
    return false;
  }

  // Must hold page_lock.
  auto find_index(const db_address& child) const noexcept -> std::optional<std::size_t> {
    if (child.file != this->address.file) [[unlikely]] return std::nullopt;
    for (std::size_t i = 0; i < hdr_.size; ++i) {
      if (element(i) == child.offset)
        return i;
    }
    return std::nullopt;
  }

  auto get_index(std::size_t idx) const -> std::optional<db_address> {
    if (idx >= hdr_.size) return std::nullopt;
    return db_address(this->address.file, element(idx));
  }

  auto key_find_index(std::span<const std::byte> key) const -> std::optional<std::size_t> {
    if (hdr_.size == 0) [[unlikely]] return std::nullopt;

    for (std::size_t i = 0; i < hdr_.size - 1u; ++i) {
      const std::span<const std::byte> key_i = key_span(i);
      const std::strong_ordering o = this->spec->key_compare(key, key_i);
      if (o == std::strong_ordering::less) return i;
    }
    return hdr_.size - 1u;
  }

  auto key_find(std::span<const std::byte> key) const -> std::optional<db_address> {
    const std::optional<std::size_t> index = key_find_index(key);
    if (index.has_value())
      return get_index(*index);
    else
      return std::nullopt;
  }

  auto index_size() const noexcept -> std::size_t {
    return hdr_.size;
  }

  private:
  template<typename Stream, typename CompletionToken>
  auto continue_load_(Stream& stream, [[maybe_unused]] cycle_ptr::cycle_gptr<raw_db_type> raw_db, CompletionToken&& token) {
    return async_read(
        stream,
        bplus_tree_intr::continue_xdr_(::earnest::xdr_reader<>(), *this),
        std::forward<CompletionToken>(token));
  }

  template<typename X>
  static auto continue_xdr_(::earnest::xdr<X>&& x, typename ::earnest::xdr<X>::template typed_function_arg<bplus_tree_intr> y) {
    return std::move(x) & y.hdr_ & xdr_dynamic_raw_bytes(asio::buffer(y.bytes_));
  }

  public:
  auto element0_offset() const noexcept -> std::size_t { return 0u; }
  auto element_length() const noexcept -> std::size_t { return sizeof(std::uint64_t); }
  auto element_offset(std::size_t idx) const noexcept -> std::size_t { return element0_offset() + idx * element_length(); }
  auto augment0_offset() const noexcept -> std::size_t { return element_offset(this->spec->child_pages_per_intr); }
  auto augment_length() const noexcept -> std::size_t { return this->spec->element.padded_augment_bytes(); }
  auto augment_offset(std::size_t idx) const noexcept -> std::size_t { return augment0_offset() + idx * augment_length(); }
  auto key0_offset() const noexcept -> std::size_t { return augment_offset(this->spec->child_pages_per_intr); }
  auto key_length() const noexcept -> std::size_t { return this->spec->element.key.padded_bytes(); }
  auto key_offset(std::size_t idx) const noexcept -> std::size_t { return key0_offset() + idx * key_length(); }

  auto element_span(std::size_t idx) const noexcept -> std::span<const std::byte> {
    assert(idx < this->spec->child_pages_per_intr);
    assert(bytes_.size() >= element_offset(this->spec->child_pages_per_intr));
    return std::span<const std::byte>(&bytes_[element_offset(idx)], element_length());
  }

  auto element_span(std::size_t idx) noexcept -> std::span<std::byte> {
    assert(idx < this->spec->child_pages_per_intr);
    assert(bytes_.size() >= element_offset(this->spec->child_pages_per_intr));
    return std::span<std::byte>(&bytes_[element_offset(idx)], element_length());
  }

  auto element_multispan(std::size_t b, std::size_t e) noexcept -> std::span<std::byte> {
    assert(b <= e);
    assert(e <= this->spec->child_pages_per_intr);
    return std::span<std::byte>(&bytes_[element_offset(b)], &bytes_[element_offset(e)]);
  }

  auto element(std::size_t idx) const noexcept -> std::uint64_t {
    std::uint64_t offset;
    auto e_span = element_span(idx);
    assert(e_span.size() == sizeof(offset));
    std::memcpy(&offset, e_span.data(), e_span.size());
    boost::endian::big_to_native_inplace(offset);
    return offset;
  }

  auto augment_span(std::size_t idx) const noexcept -> std::span<const std::byte> {
    assert(idx < this->spec->child_pages_per_intr);
    assert(bytes_.size() >= augment_offset(this->spec->child_pages_per_intr));
    return std::span<const std::byte>(&bytes_[augment_offset(idx)], augment_length());
  }

  auto augment_span(std::size_t idx) noexcept -> std::span<std::byte> {
    assert(idx < this->spec->child_pages_per_intr);
    assert(bytes_.size() >= augment_offset(this->spec->child_pages_per_intr));
    return std::span<std::byte>(&bytes_[augment_offset(idx)], augment_length());
  }

  auto augment_multispan(std::size_t b, std::size_t e) noexcept -> std::span<std::byte> {
    assert(b <= e);
    assert(e <= this->spec->child_pages_per_intr);
    return std::span<std::byte>(&bytes_[augment_offset(b)], &bytes_[augment_offset(e)]);
  }

  auto key_span(std::size_t idx) const noexcept -> std::span<const std::byte> {
    assert(idx < this->spec->child_pages_per_intr - 1u);
    assert(bytes_.size() >= key_offset(this->spec->child_pages_per_intr - 1u));
    return std::span<const std::byte>(&bytes_[key_offset(idx)], key_length());
  }

  auto key_span(std::size_t idx) noexcept -> std::span<std::byte> {
    assert(idx < this->spec->child_pages_per_intr - 1u);
    assert(bytes_.size() >= key_offset(this->spec->child_pages_per_intr - 1u));
    return std::span<std::byte>(&bytes_[key_offset(idx)], key_length());
  }

  auto key_multispan(std::size_t b, std::size_t e) noexcept -> std::span<std::byte> {
    assert(b <= e);
    assert(e <= this->spec->child_pages_per_intr - 1u);
    return std::span<std::byte>(&bytes_[key_offset(b)], &bytes_[key_offset(e)]);
  }

  private:
  auto async_compute_page_augments_impl_(move_only_function<void(std::vector<std::byte>)> callback) const -> void override {
    std::vector<std::span<const std::byte>> inputs;
    inputs.reserve(hdr_.size);
    for (std::size_t i = 0; i < hdr_.size; ++i)
      inputs.push_back(augment_span(i));
    callback(this->combine_augments(std::move(inputs)));
  }

  protected:
  auto log_dump_impl_(std::ostream& out) const -> void override {
    using namespace std::literals;

    out << "earnest::detail::bplus_tree_intr: {\n"sv;
    this->bplus_tree_page<RawDbType>::log_dump_impl_(out);
    out << "  index_size: "sv << index_size() << "\n"sv;

    for (std::size_t i = 0; i < index_size(); ++i) {
      if (i != 0)
        out << "  key["sv <<  (i - 1u) << "]: "sv << byte_span_printer(key_span(i - 1u)) << "\n"sv;
      out << "  element["sv << i << "]: "sv << element(i) << "\n"sv;
      out << "  augment["sv << i << "]: "sv << byte_span_printer(augment_span(i)) << "\n"sv;
    }

    out << "}\n"sv;
  }

  private:
  std::vector<std::byte, rebind_alloc<std::byte>> bytes_;
  bplus_tree_intr_header hdr_;
};


template<typename RawDbType>
class bplus_tree_leaf final
: public bplus_tree_page<RawDbType>
{
  template<typename> friend class bplus_tree_page;
  template<typename, typename> friend class bplus_tree;

  public:
  static inline constexpr std::uint32_t magic = 0x65cc'612dU;
  using raw_db_type = typename bplus_tree_page<RawDbType>::raw_db_type;
  using executor_type = typename bplus_tree_page<RawDbType>::executor_type;
  using allocator_type = typename bplus_tree_page<RawDbType>::allocator_type;

  private:
  template<typename T> using rebind_alloc = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;

  public:
  struct element
  : protected cycle_ptr::cycle_base
  {
    using executor_type = typename bplus_tree_leaf::executor_type;

    element(executor_type ex, std::size_t index, cycle_ptr::cycle_gptr<bplus_tree_leaf> owner, typename raw_db_type::allocator_type alloc)
    : index(std::move(index)),
      element_lock(std::move(ex), lock_name_(this), std::move(alloc)),
      owner(std::move(owner))
    {}

    auto get_executor() const -> executor_type {
      return this->element_lock.get_executor();
    }

    auto type() const -> bplus_tree_leaf_use_element {
      return owner->use_list_span()[index];
    }

    auto key_span() const -> std::span<const std::byte> {
      return owner->key_span(index);
    }

    auto key_span() -> std::span<std::byte> {
      return owner->key_span(index);
    }

    auto value_span() const -> std::span<const std::byte> {
      return owner->value_span(index);
    }

    auto value_span() -> std::span<std::byte> {
      return owner->value_span(index);
    }

    auto key_compare(std::span<const std::byte> key) const -> std::strong_ordering {
      const auto& spec = *owner->spec;
      std::strong_ordering o = std::strong_ordering::equivalent;

      switch (type()) {
        default:
          throw std::system_error(make_error_code(bplus_tree_errc::bad_tree), "element type not recognized");
        case bplus_tree_leaf_use_element::unused:
          throw std::system_error(make_error_code(bplus_tree_errc::bad_tree), "element type 'unused' may not be active for element");
        case bplus_tree_leaf_use_element::before_first:
          o = std::strong_ordering::less;
          break;
        case bplus_tree_leaf_use_element::after_last:
          o = std::strong_ordering::greater;
          break;
        case bplus_tree_leaf_use_element::used:
        case bplus_tree_leaf_use_element::ghost_create:
        case bplus_tree_leaf_use_element::ghost_delete:
          o = spec.key_compare(key_span(), key);
          break;
        case bplus_tree_leaf_use_element::ghost_iterator_before:
          o = spec.key_compare(key_span(), key);
          if (o == std::strong_ordering::equal) o = std::strong_ordering::less;
          break;
        case bplus_tree_leaf_use_element::ghost_iterator_after:
          o = spec.key_compare(key_span(), key);
          if (o == std::strong_ordering::equal) o = std::strong_ordering::greater;
          break;
      }

      return o;
    }

    auto key_equal(std::span<const std::byte> key, bool iterator_equal = false) const -> bool {
      const auto& spec = *owner->spec;
      bool eq;

      switch (type()) {
        default: [[unlikely]]
          eq = false;
          break;
        case bplus_tree_leaf_use_element::used:
        case bplus_tree_leaf_use_element::ghost_create:
        case bplus_tree_leaf_use_element::ghost_delete:
          eq = spec.key_equal(key_span(), key);
          break;
        case bplus_tree_leaf_use_element::ghost_iterator_before:
        case bplus_tree_leaf_use_element::ghost_iterator_after:
          eq = iterator_equal && spec.key_equal(key_span(), key);
          break;
      }

      return eq;
    }

    template<typename LeafLockType = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock, typename TxAlloc, typename CompletionToken>
    requires std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock> || std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock> || std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock>
    auto async_lock_owner(TxAlloc tx_alloc, CompletionToken token) const {
      return asio::async_initiate<CompletionToken, void(cycle_ptr::cycle_gptr<bplus_tree_leaf>, LeafLockType)>(
          [](auto handler, cycle_ptr::cycle_gptr<const element> self, TxAlloc tx_alloc) {
            self->template async_lock_owner_<LeafLockType>(std::move(tx_alloc), std::move(handler));
          },
          token, this->shared_from_this(this), std::move(tx_alloc));
    }

    private:
    template<typename LeafLockType = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock, typename TxAlloc>
    requires std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock> || std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock> || std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock>
    auto async_lock_owner_(TxAlloc tx_alloc, move_only_function<void(cycle_ptr::cycle_gptr<bplus_tree_leaf>, LeafLockType)> handler) const -> void {
      using monitor_exlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock;
      using monitor_uplock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock;
      using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;

      struct op
      : public std::enable_shared_from_this<op>
      {
        explicit op(cycle_ptr::cycle_gptr<const element> self, move_only_function<void(cycle_ptr::cycle_gptr<bplus_tree_leaf>, LeafLockType)> handler)
        : elem(std::move(self)),
          handler(std::move(handler))
        {}

        auto operator()() -> void {
          elem->element_lock.async_shared(asio::deferred, __FILE__, __LINE__)
          | asio::deferred(
              [self_op=this->shared_from_this()]([[maybe_unused]] monitor_shlock_type elem_shlock) {
                return asio::deferred.values(cycle_ptr::cycle_gptr<bplus_tree_leaf>(self_op->elem->owner));
              })
          | asio::deferred(
              [](cycle_ptr::cycle_gptr<bplus_tree_leaf> owner) {
                if constexpr(std::is_same_v<monitor_exlock_type, LeafLockType>) {
                  return owner->page_lock.async_exclusive(asio::append(asio::deferred, owner), __FILE__, __LINE__);
                } else if constexpr(std::is_same_v<monitor_uplock_type, LeafLockType>) {
                  return owner->page_lock.async_upgrade(asio::append(asio::deferred, owner), __FILE__, __LINE__);
                } else {
                  static_assert(std::is_same_v<monitor_shlock_type, LeafLockType>);
                  return owner->page_lock.async_shared(asio::append(asio::deferred, owner), __FILE__, __LINE__);
                }
              })
          | [self_op=this->shared_from_this()](LeafLockType leaf_lock, cycle_ptr::cycle_gptr<bplus_tree_leaf> owner) {
              bool success = std::any_of(owner->elements().begin(), owner->elements().end(),
                  [&self_op](const auto& owner_elem_ptr) -> bool {
                    return self_op->elem == owner_elem_ptr;
                  });

              if (success) {
                std::invoke(self_op->handler, std::move(owner), std::move(leaf_lock));
              } else {
                // Using asio::post, to prevent recursion from overflowing the stack space.
                asio::post(
                    self_op->elem->get_executor(),
                    [self_op]() {
                      std::invoke(*self_op);
                    });
              }
            };
        }

        private:
        const cycle_ptr::cycle_gptr<const element> elem;
        move_only_function<void(cycle_ptr::cycle_gptr<bplus_tree_leaf>, LeafLockType)> handler;
      };

      std::invoke(*std::allocate_shared<op>(tx_alloc, this->shared_from_this(this), std::move(handler)));
    }

    static auto lock_name_(const void* this_ptr) -> std::string {
      std::ostringstream s;
      s << "earnest::detail::bplus_tree_leaf::element{" << this_ptr << "}";
      return std::move(s).str();
    }

    public:
    std::size_t index;
    mutable monitor<executor_type, typename raw_db_type::allocator_type> element_lock;
    cycle_ptr::cycle_member_ptr<bplus_tree_leaf> owner;

    // Use-count counts how many references depend on this element.
    // If use-count is zero, the element may be safely deleted.
    // Use-count requires a lock to be held, when increasing from 0.
    // Use-count does not require a lock to be held, otherwise.
    mutable std::atomic<std::size_t> use_count{std::size_t(0)};
  };

  private:
  using element_ptr = cycle_ptr::cycle_member_ptr<element>;

  public:
  using element_vector = std::vector<
      element_ptr,
      cycle_ptr::cycle_allocator<rebind_alloc<element_ptr>>>;

  bplus_tree_leaf(std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address, const bplus_tree_page_header& h = bplus_tree_page_header{.parent=bplus_tree_page<RawDbType>::nil_page}, std::uint64_t predecessor_offset = bplus_tree_page<RawDbType>::nil_page, std::uint64_t successor_offset = bplus_tree_page<RawDbType>::nil_page)
  : bplus_tree_page<RawDbType>(spec, std::move(raw_db), std::move(address), h),
    bytes_(spec->payload_bytes_per_leaf(), this->get_allocator()),
    elements_(typename element_vector::allocator_type(*this, this->get_allocator())),
    hdr_{
      .successor_page=successor_offset,
      .predecessor_page=predecessor_offset,
    }
  {
    assert(spec->payload_bytes_per_leaf() == element_offset(spec->elements_per_leaf));
    elements_.reserve(spec->payload_bytes_per_leaf());
  }

  ~bplus_tree_leaf() override = default;

  template<typename TxAlloc, typename DbAlloc, typename CompletionToken>
  static auto async_new_page(
      typename raw_db_type::template fdb_transaction<TxAlloc> tx,
      DbAlloc db_alloc,
      std::shared_ptr<const bplus_tree_spec> spec,
      cycle_ptr::cycle_gptr<raw_db_type> raw_db,
      std::uint64_t parent_offset,
      std::uint64_t pred_sibling,
      std::uint64_t succ_sibling,
      bool create_before_first_and_after_last,
      CompletionToken&& token) {
    using tx_file_type = typename raw_db_type::template fdb_transaction<TxAlloc>::file;
    using stream_type = positional_stream_adapter<tx_file_type>;
    using page_type = bplus_tree_page<RawDbType>;

    return asio::async_initiate<CompletionToken, void(std::error_code, cycle_ptr::cycle_gptr<bplus_tree_leaf>)>(
        [](auto handler, typename raw_db_type::template fdb_transaction<TxAlloc> tx, DbAlloc db_alloc, std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, std::uint64_t parent_offset, std::uint64_t pred_sibling, std::uint64_t succ_sibling, bool create_before_first_and_after_last) -> void {
          db_alloc.async_allocate(tx, spec->bytes_per_leaf(), asio::deferred)
          | asio::deferred(
              [tx, raw_db, spec, parent_offset, pred_sibling, succ_sibling, create_before_first_and_after_last](std::error_code ec, db_address addr) mutable {
                cycle_ptr::cycle_gptr<bplus_tree_leaf> new_page;
                if (!ec) {
                  new_page = cycle_ptr::allocate_cycle<bplus_tree_leaf>(raw_db->get_cache_allocator(), spec, raw_db, std::move(addr), bplus_tree_page_header{.parent=parent_offset}, pred_sibling, succ_sibling);

                  if (create_before_first_and_after_last) [[unlikely]] { // Unlikey, because only first root page needs this.
                    new_page->use_list_span().front() = bplus_tree_leaf_use_element::before_first;
                    new_page->use_list_span().back() = bplus_tree_leaf_use_element::after_last;

                    assert(new_page->elements_.empty());
                    new_page->elements_.emplace_back(cycle_ptr::allocate_cycle<element>(new_page->get_allocator(), new_page->get_executor(), 0u, new_page, new_page->get_allocator()));
                    new_page->elements_.emplace_back(cycle_ptr::allocate_cycle<element>(new_page->get_allocator(), new_page->get_executor(), new_page->use_list_span().size() - 1u, new_page, new_page->get_allocator()));
                  }

                  tx.on_commit(
                      [raw_db, new_page]() {
                        raw_db->template emplace<page_type>(
                            typename raw_db_type::template key_type<page_type>{
                              .file=new_page->address.file,
                              .offset=new_page->address.offset
                            },
                            new_page);
                      });
                }

                return asio::deferred.when(!ec)
                    .then(
                        asio::deferred.values(tx, new_page)
                        | asio::deferred(
                            [](typename raw_db_type::template fdb_transaction<TxAlloc> tx, cycle_ptr::cycle_gptr<bplus_tree_leaf> new_page) {
                              auto stream = std::allocate_shared<stream_type>(tx.get_allocator(), tx[new_page->address.file], new_page->address.offset);
                              return async_write(
                                  *stream,
                                  (::earnest::xdr_writer<>() & xdr_constant(bplus_tree_page_header{.magic=magic, .parent=new_page->parent_page_offset()}))
                                  + bplus_tree_leaf::continue_xdr_(::earnest::xdr_writer<>(), *new_page),
                                  asio::append(asio::deferred, new_page, stream));
                            })
                        | asio::deferred(
                            [](std::error_code ec, cycle_ptr::cycle_gptr<bplus_tree_leaf> new_page, [[maybe_unused]] auto stream) {
                              return asio::deferred.values(std::move(ec), std::move(new_page));
                            }))
                    .otherwise(asio::deferred.values(ec, new_page));
              })
          | std::move(handler);
        },
        token, std::move(tx), std::move(db_alloc), std::move(spec), std::move(raw_db), std::move(parent_offset), std::move(pred_sibling), std::move(succ_sibling), std::move(create_before_first_and_after_last));
  }

  auto elements() const noexcept -> const element_vector& {
    return elements_;
  }

  auto next_page_address() const -> db_address {
    db_address result = this->address;
    result.offset = hdr_.successor_page;
    return result;
  }

  auto prev_page_address() const -> db_address {
    db_address result = this->address;
    result.offset = hdr_.predecessor_page;
    return result;
  }

  private:
  template<typename LeafLockType = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock, int Direction, typename TxAlloc>
  requires std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock> || std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock> || std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock>
  auto async_prevnext_page_impl_(
      [[maybe_unused]] std::integral_constant<int, Direction> direction,
      TxAlloc tx_alloc,
      move_only_function<void(std::error_code, cycle_ptr::cycle_gptr<bplus_tree_leaf>, LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock)> handler)
  -> std::enable_if_t<Direction == -1 || Direction == 1> {
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;
    using monitor_uplock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock;
    using monitor_exlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock;

    struct op
    : public std::enable_shared_from_this<op>
    {
      op(cycle_ptr::cycle_gptr<bplus_tree_leaf> self, move_only_function<void(std::error_code, cycle_ptr::cycle_gptr<bplus_tree_leaf>, LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock)> handler)
      : self(std::move(self)),
        handler(std::move(handler))
      {}

      auto operator()() -> void {
        self->page_lock.dispatch_shared(
            [self_op=this->shared_from_this()](monitor_shlock_type self_lock) -> void {
              if (self_op->self->erased_) {
                self_op->error_invoke(make_error_code(db_errc::data_expired));
                return;
              }

              db_address target;
              if constexpr(Direction < 0)
                target = self_op->self->prev_page_address();
              else
                target = self_op->self->next_page_address();

              self_lock.reset(); // No longer need the lock.
              self_op->load_page(std::move(target));
            },
            __FILE__, __LINE__);
      }

      private:
      auto load_page(db_address address) -> void {
        if (address.offset == bplus_tree_page<raw_db_type>::nil_page) {
          std::invoke(handler, std::error_code{}, nullptr, LeafLockType{}, monitor_shlock_type{});
          return;
        }

        auto raw_db = self->raw_db.lock();
        if (raw_db == nullptr) {
          error_invoke(db_errc::data_expired);
          return;
        }

        bplus_tree_page<raw_db_type>::async_load_op(
            self->spec, raw_db, address,
            [self_page=this->self, address]() {
              return self_page->page_lock.dispatch_shared(asio::deferred, __FILE__, __LINE__)
              | asio::deferred(
                  [self_page, address]([[maybe_unused]] monitor_shlock_type self_lock) {
                    std::error_code ec;

                    if (self_page->erased_) {
                      ec = bplus_tree_errc::restart;
                    } else {
                      if constexpr(Direction < 0) {
                        if (address != self_page->prev_page_address())
                          ec = bplus_tree_errc::restart;
                      } else {
                        if (address != self_page->next_page_address())
                          ec = bplus_tree_errc::restart;
                      }
                    }

                    return asio::deferred.values(ec);
                  });
            })
        | bplus_tree_page<raw_db_type>::template variant_to_pointer_op<bplus_tree_leaf>()
        | [self_op=this->shared_from_this()](std::error_code ec, cycle_ptr::cycle_gptr<bplus_tree_leaf> leaf) -> void {
            if (ec == make_error_code(bplus_tree_errc::restart)) {
              std::invoke(*self_op);
              return;
            }

            self_op->validate(std::move(leaf));
          };
      }

      auto validate(cycle_ptr::cycle_gptr<bplus_tree_leaf> other_page) -> void {
        assert(other_page != nullptr);

        auto other_lock_op = [other_page](monitor_shlock_type self_lock) {
          if constexpr(std::is_same_v<LeafLockType, monitor_exlock_type>) {
            return other_page->page_lock.dispatch_exclusive(asio::append(asio::deferred, other_page, std::move(self_lock)), __FILE__, __LINE__);
          } else if constexpr(std::is_same_v<LeafLockType, monitor_uplock_type>) {
            return other_page->page_lock.dispatch_upgrade(asio::append(asio::deferred, other_page, std::move(self_lock)), __FILE__, __LINE__);
          } else {
            static_assert(std::is_same_v<LeafLockType, monitor_shlock_type>);
            return other_page->page_lock.dispatch_shared(asio::append(asio::deferred, other_page, std::move(self_lock)), __FILE__, __LINE__);
          }
        };

        asio::deferred.values(this->shared_from_this())
        | asio::deferred(
            [](std::shared_ptr<op> self_op) {
              return asio::deferred.when(Direction > 0)
                  .then(self_op->self->page_lock.async_shared(asio::deferred, __FILE__, __LINE__))
                  .otherwise(asio::deferred.values(monitor_shlock_type{}));
            })
        | asio::deferred(std::move(other_lock_op))
        | asio::deferred(
            [self_op=this->shared_from_this()](LeafLockType other_lock, cycle_ptr::cycle_gptr<bplus_tree_leaf> other_leaf, monitor_shlock_type self_lock) {
              return asio::deferred.when(Direction < 0)
                  .then(self_op->self->page_lock.async_shared(asio::append(asio::deferred, other_lock, other_leaf), __FILE__, __LINE__))
                  .otherwise(asio::deferred.values(std::move(self_lock), other_lock, other_leaf));
            })
        | [self_op=this->shared_from_this()](monitor_shlock_type self_lock, LeafLockType other_lock, cycle_ptr::cycle_gptr<bplus_tree_leaf> other_leaf) -> void {
            bool valid = !other_leaf->erased_;
            if constexpr(Direction < 0) {
              if (other_leaf->next_page_address() != self_op->self->address)
                valid = false;
            } else {
              if (other_leaf->prev_page_address() != self_op->self->address)
                valid = false;
            }

            if (valid)
              std::invoke(self_op->handler, std::error_code(), std::move(other_leaf), std::move(other_lock), std::move(self_lock));
            else
              std::invoke(*self_op); // restart
          };
      }

      auto error_invoke(std::error_code ec) -> void {
        std::invoke(handler, ec, nullptr, LeafLockType{}, monitor_shlock_type{});
      }

      cycle_ptr::cycle_gptr<bplus_tree_leaf> self;
      move_only_function<void(std::error_code, cycle_ptr::cycle_gptr<bplus_tree_leaf>, LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock)> handler;
    };

    std::invoke(*std::allocate_shared<op>(tx_alloc, this->shared_from_this(this), std::move(handler)));
  }

  public:
  template<typename LeafLockType = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock, typename TxAlloc, typename CompletionToken>
  requires std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock> || std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock> || std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock>
  auto async_next_page(TxAlloc tx_alloc, CompletionToken&& token) {
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;

    return asio::async_initiate<CompletionToken, void(std::error_code, cycle_ptr::cycle_gptr<bplus_tree_leaf>, LeafLockType, monitor_shlock_type)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree_leaf> self, TxAlloc tx_alloc) {
          if constexpr(handler_has_executor_v<decltype(handler)>)
            self->template async_prevnext_page_impl_<LeafLockType>(std::integral_constant<int, 1>(), std::move(tx_alloc), completion_handler_fun(std::move(handler), self->get_executor()));
          else
            self->template async_prevnext_page_impl_<LeafLockType>(std::integral_constant<int, 1>(), std::move(tx_alloc), std::move(handler));
        },
        token, this->shared_from_this(this), std::move(tx_alloc));
  }

  template<typename LeafLockType = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock, typename TxAlloc, typename CompletionToken>
  requires std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock> || std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock> || std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock>
  auto async_prev_page(TxAlloc tx_alloc, CompletionToken&& token) {
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;

    return asio::async_initiate<CompletionToken, void(std::error_code, cycle_ptr::cycle_gptr<bplus_tree_leaf>, LeafLockType, monitor_shlock_type)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree_leaf> self, TxAlloc tx_alloc) {
          if constexpr(handler_has_executor_v<decltype(handler)>)
            self->template async_prevnext_page_impl_<LeafLockType>(std::integral_constant<int, -1>(), std::move(tx_alloc), completion_handler_fun(std::move(handler), self->get_executor()));
          else
            self->template async_prevnext_page_impl_<LeafLockType>(std::integral_constant<int, -1>(), std::move(tx_alloc), std::move(handler));
        },
        token, this->shared_from_this(this), std::move(tx_alloc));
  }

  // Accepts a completion-handler of the form:
  // void(std::error_code, cycle_ptr::cycle_gptr<bplus_tree_leaf>, monitor::{shared|exclusive}_lock)
  template<bool ForWrite>
  [[deprecated]]
  auto next_page_op(std::bool_constant<ForWrite> for_write) const {
    return nextprev_page_op_(std::move(for_write), &bplus_tree_leaf::next_page_address, &bplus_tree_leaf::prev_page_address);
  }

  // Accepts a completion-handler of the form:
  // void(std::error_code, cycle_ptr::cycle_gptr<bplus_tree_leaf>, monitor::{shared|exclusive}_lock)
  template<bool ForWrite, typename Lock>
  [[deprecated]]
  auto next_page_op(std::bool_constant<ForWrite> for_write, Lock lock) const {
    return this->page_variant_op(
        for_write, lock,
        // get-address function:
        [](const auto& self) {
          return boost::polymorphic_pointer_downcast<cycle_ptr::cycle_gptr<const bplus_tree_leaf>>(self)->next_page_address();
        },
        // validation function:
        [](const auto& target_ptr, const auto& self_ptr) {
          return std::invoke(
              overload(
                  [](const bplus_tree_leaf& leaf, const auto& self_ptr) {
                    return leaf.prev_page_address() == self_ptr->address;
                  },
                  []([[maybe_unused]] const bplus_tree_intr<RawDbType>& intr, [[maybe_unused]] const auto& self_ptr) {
                    return true; // It's actually the wrong type, but that page (intr) was loaded while this page (self_ptr) was locked, so we know it was the actual pointer.
                                 // Next, the variant_to_pointer_op will cause this to be rejected.
                  }),
              *target_ptr, self_ptr);
        },
        // restart function:
        [self=this->shared_from_this(this), for_write, lock]() mutable {
          return self->next_page_op(std::move(for_write), std::move(lock));
        })
    | bplus_tree_page<RawDbType>::template variant_to_pointer_op<bplus_tree_leaf>();
  }

  // Accepts a completion-handler of the form:
  // void(std::error_code, cycle_ptr::cycle_gptr<bplus_tree_leaf>, monitor::{shared|exclusive}_lock)
  template<bool ForWrite>
  [[deprecated]]
  auto prev_page_op(std::bool_constant<ForWrite> for_write) const {
    return nextprev_page_op_(std::move(for_write), &bplus_tree_leaf::prev_page_address, &bplus_tree_leaf::next_page_address);
  }

  static constexpr std::size_t xdr_page_hdr_bytes = std::remove_cvref_t<decltype(::earnest::xdr_reader<>() & std::declval<bplus_tree_page_header&>())>::bytes.value();
  static constexpr std::size_t xdr_hdr_bytes = std::remove_cvref_t<decltype(::earnest::xdr_reader<>() & std::declval<bplus_tree_leaf_header&>())>::bytes.value();

  // Write header to disk, but don't update in-memory representation.
  template<typename TxAlloc, typename CompletionToken>
  auto async_set_hdr_diskonly_op(typename raw_db_type::template fdb_transaction<TxAlloc> tx, bplus_tree_leaf_header hdr, CompletionToken&& token) {
    using tx_file_type = typename raw_db_type::template fdb_transaction<TxAlloc>::file;
    using stream_type = positional_stream_adapter<tx_file_type>;

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, std::shared_ptr<stream_type> fptr, bplus_tree_leaf_header hdr) {
          async_write(
              *fptr,
              ::earnest::xdr_writer<>() & xdr_constant(std::move(hdr)),
              completion_wrapper<void(std::error_code)>(
                  std::move(handler),
                  [fptr](auto handler, std::error_code ec) mutable {
                    fptr.reset();
                    return std::invoke(handler, ec);
                  }));
        },
        token, std::allocate_shared<stream_type>(tx.get_allocator(), tx[this->address.file], this->address.offset + xdr_page_hdr_bytes), std::move(hdr));
  }

  template<typename TxAlloc, typename CompletionToken>
  auto async_set_use_list_diskonly_op(typename raw_db_type::template fdb_transaction<TxAlloc> tx, std::size_t offset, bplus_tree_leaf_use_element type, CompletionToken&& token) {
    using tx_file_type = typename raw_db_type::template fdb_transaction<TxAlloc>::file;
    struct state {
      state(tx_file_type f, bplus_tree_leaf_use_element type)
      : f(std::move(f)),
        type(std::move(type))
      {}

      tx_file_type f;
      bplus_tree_leaf_use_element type;
    };

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree_leaf> self, std::shared_ptr<state> state_ptr, std::size_t offset) {
          if (offset >= self->spec->elements_per_leaf)
            throw std::invalid_argument("bad offset for element write");

          const auto span = std::as_bytes(std::span<bplus_tree_leaf_use_element, 1>(&state_ptr->type, 1));
          return asio::async_write_at(
              state_ptr->f,
              self->address.offset + xdr_page_hdr_bytes + xdr_hdr_bytes + self->element_offset(offset),
              asio::buffer(span),
              completion_wrapper<void(std::error_code, std::size_t)>(
                  std::move(handler),
                  [state_ptr](auto handler, std::error_code ec, [[maybe_unused]] std::size_t nbytes) mutable {
                    if (!ec) assert(nbytes == 1);
                    state_ptr.reset();
                    return std::invoke(handler, ec);
                  }));
        },
        token, this->shared_from_this(this), std::allocate_shared<state>(tx.get_allocator(), tx[this->address.file], type), std::move(offset));
  }

  template<typename TxAlloc, typename CompletionToken>
  auto async_set_use_list_diskonly_op(typename raw_db_type::template fdb_transaction<TxAlloc> tx, std::size_t offset, std::span<const bplus_tree_leaf_use_element> type, CompletionToken&& token) {
    using tx_file_type = typename raw_db_type::template fdb_transaction<TxAlloc>::file;
    struct state {
      explicit state(tx_file_type f)
      : f(std::move(f))
      {}

      tx_file_type f;
    };

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree_leaf> self, std::shared_ptr<state> state_ptr, std::size_t offset, std::span<const bplus_tree_leaf_use_element> type) {
          if (offset + type.size() > self->spec->elements_per_leaf)
            throw std::invalid_argument("bad offset for element write");

          const auto span = std::as_bytes(type);
          return asio::async_write_at(
              state_ptr->f,
              self->address.offset + xdr_page_hdr_bytes + xdr_hdr_bytes + self->element_offset(offset),
              asio::buffer(span),
              completion_wrapper<void(std::error_code, std::size_t)>(
                  std::move(handler),
                  [state_ptr, type](auto handler, std::error_code ec, [[maybe_unused]] std::size_t nbytes) mutable {
                    if (!ec) assert(nbytes == type.size());
                    state_ptr.reset();
                    return std::invoke(handler, ec);
                  }));
        },
        token, this->shared_from_this(this), std::allocate_shared<state>(tx.get_allocator(), tx[this->address.file]), std::move(offset), std::move(type));
  }

  // Write elements to disk, but don't update in-memory representation.
  template<typename TxAlloc, typename CompletionToken>
  auto async_set_elements_diskonly_op(typename raw_db_type::template fdb_transaction<TxAlloc> tx, std::size_t offset, std::span<const std::byte> elements, CompletionToken&& token) {
    using tx_file_type = typename raw_db_type::template fdb_transaction<TxAlloc>::file;

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree_leaf> self, std::shared_ptr<tx_file_type> fptr, std::size_t offset, std::span<const std::byte> elements) {
          if (offset >= self->spec->elements_per_leaf)
            throw std::invalid_argument("bad offset for element write");
          if (elements.size() > (self->spec->elements_per_leaf - offset) * self->element_length())
            throw std::invalid_argument("too many bytes for element write");

          return asio::async_write_at(
              *fptr,
              self->address.offset + xdr_page_hdr_bytes + xdr_hdr_bytes + self->element_offset(offset),
              asio::buffer(elements),
              completion_wrapper<void(std::error_code, std::size_t)>(
                  std::move(handler),
                  [fptr](auto handler, std::error_code ec, [[maybe_unused]] std::size_t nbytes) mutable {
                    fptr.reset();
                    return std::invoke(handler, ec);
                  }));
        },
        token, this->shared_from_this(this), std::allocate_shared<tx_file_type>(tx.get_allocator(), tx[this->address.file]), std::move(offset), std::move(elements));
  }

  protected:
  auto on_load() noexcept -> void override {
    const bool cleanup_needed = std::any_of(elements_.cbegin(), elements_.cend(),
        [](const auto& elem_ptr) {
          switch (elem_ptr->type()) {
            default:
              return false;
            case bplus_tree_leaf_use_element::ghost_create:
            case bplus_tree_leaf_use_element::ghost_delete:
              return true;
          }
        });
    if (cleanup_needed) this->background_clean_up_use_list();

    this->bplus_tree_page<RawDbType>::on_load();
  }

  public:
  auto background_clean_up_use_list() -> void {
    using monitor_uplock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock;
    using monitor_exlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock;

    struct cleanup_data {
      explicit cleanup_data(cycle_ptr::cycle_gptr<element> elem, monitor_uplock_type elem_uplock) noexcept
      : elem(std::move(elem)),
        elem_uplock(std::move(elem_uplock))
      {}

      cycle_ptr::cycle_gptr<element> elem;
      monitor_uplock_type elem_uplock;
      monitor_exlock_type elem_exlock;
    };
    using cleanup_data_vector = std::vector<
        cleanup_data,
        typename std::allocator_traits<typename raw_db_type::allocator_type>::template rebind_alloc<cleanup_data>>;
    using tx_type = typename raw_db_type::template fdb_transaction<typename cleanup_data_vector::allocator_type>;

    struct op
    : public std::enable_shared_from_this<op>
    {
      op(cycle_ptr::cycle_gptr<bplus_tree_leaf> page, cycle_ptr::cycle_gptr<raw_db_type> raw_db) noexcept
      : page(page),
        cleanups(raw_db->get_allocator())
      {}

      auto operator()() -> void {
        page->page_lock.dispatch_upgrade(
            [self_op=this->shared_from_this()](monitor_uplock_type lock) {
              self_op->page_uplock = std::move(lock);
              self_op->gather_cleanups();
            },
            __FILE__, __LINE__);
      }

      auto get_allocator() const -> typename cleanup_data_vector::allocator_type { return cleanups.get_allocator(); }
      auto get_executor() const -> executor_type { return page->get_executor(); }

      private:
      auto gather_cleanups(std::size_t idx = 0) -> void {
        assert(page_uplock.is_locked());

        for (; idx < page->elements_.size(); ++idx) {
          const element_ptr& elem_ptr = page->elements_[idx];
          std::optional<monitor_uplock_type> opt_elem_uplock = elem_ptr->element_lock.try_upgrade(__FILE__, __LINE__);
          if (opt_elem_uplock.has_value()) {
            switch (elem_ptr->type()) {
              default:
                /* skip */
                break;
              case bplus_tree_leaf_use_element::ghost_create:
              case bplus_tree_leaf_use_element::ghost_delete:
                this->cleanups.emplace_back(elem_ptr, std::move(opt_elem_uplock).value());
                break;
            }
          } else {
            elem_ptr->element_lock.async_upgrade(
                [self_op=this->shared_from_this(), elem_ptr=cycle_ptr::cycle_gptr<element>(elem_ptr), idx](monitor_uplock_type elem_uplock) {
                  switch (elem_ptr->type()) {
                    default:
                      /* skip */
                      break;
                    case bplus_tree_leaf_use_element::ghost_create:
                    case bplus_tree_leaf_use_element::ghost_delete:
                      self_op->cleanups.emplace_back(elem_ptr, std::move(elem_uplock));
                      break;
                  }
                  self_op->gather_cleanups(idx + 1u);
                },
                __FILE__, __LINE__);
            return; // Callback will restart the loop.
          }
        }
        if (cleanups.empty()) return; // Nothing to do.

        update_on_disk();
      }

      auto update_on_disk() -> void {
        auto raw_db = page->raw_db.lock();
        if (raw_db == nullptr) {
          error_complete(make_error_code(db_errc::data_expired));
          return;
        }

        tx_type tx = raw_db->fdb_tx_begin(isolation::read_commited, tx_mode::read_write, get_allocator());
        auto barrier = make_completion_barrier(
            [self_op=this->shared_from_this(), tx](std::error_code ec) {
              if (ec)
                self_op->error_complete(ec);
              else
                self_op->commit(tx);
            },
            get_executor());

        std::for_each(cleanups.cbegin(), cleanups.cend(),
            [&](const auto& c) {
              page->async_set_use_list_diskonly_op(tx, c.elem->index, bplus_tree_leaf_use_element::unused, ++barrier);
            });
        page->async_set_augment_propagation_required_diskonly_op(tx, std::move(barrier));
      }

      auto commit(tx_type tx) -> void {
        tx.async_commit(
            [self_op=this->shared_from_this()](std::error_code ec) {
              if (ec)
                self_op->error_complete(ec);
              else
                self_op->exlock_page();
            });
      }

      auto exlock_page() -> void {
        std::move(page_uplock).dispatch_exclusive(
            [self_op=this->shared_from_this()](monitor_exlock_type page_exlock) {
              self_op->page_exlock = std::move(page_exlock);
              self_op->exlock_elements();
            });
      }

      auto exlock_elements(std::size_t idx = 0) -> void {
        for (/*skip*/; idx < cleanups.size(); ++idx) {
          auto opt_elem_exlock = std::move(cleanups[idx].elem_uplock).try_exclusive();
          if (opt_elem_exlock.has_value()) {
            cleanups[idx].elem_exlock = std::move(opt_elem_exlock).value();
          } else {
            std::move(cleanups[idx].elem_uplock).async_exclusive(
                [self_op=this->shared_from_this(), idx](monitor_exlock_type elem_exlock) {
                  self_op->cleanups[idx].elem_exlock = std::move(elem_exlock);
                  self_op->exlock_elements(idx + 1u);
                });
            return; // Callback will continue the loop.
          }
        }

        update_in_memory();
      }

      auto update_in_memory() -> void {
        assert(page_exlock.is_locked());
        for (const auto& c : cleanups) assert(c.elem_exlock.is_locked());

        page->augment_propagation_required = true;
        page->async_fix_augment_background();

        std::for_each(cleanups.cbegin(), cleanups.cend(),
            [this](const auto& c) mutable {
              page->use_list_span()[c.elem->index] = bplus_tree_leaf_use_element::unused;

              // We hold the lock, so use_count can't be turned from zero to non-zero,
              // so we don't care about atomic memory-order.
              if (c.elem->use_count.load(std::memory_order_relaxed) == 0u) {
                const auto elem_iter = std::find(this->page->elements_.cbegin(), this->page->elements_.cend(), c.elem);
                assert(elem_iter != this->page->elements_.cend());
                this->page->elements_.erase(elem_iter);
              }
            });

        /* Done! */
      }

      auto error_complete(std::error_code ec) -> void {
        if (ec == make_error_code(db_errc::data_expired)) return; // Don't log data-expired.
        std::clog << "bplus-tree: error during background cleanup of use-list: " << ec.message() << "\n";
      }

      cycle_ptr::cycle_gptr<bplus_tree_leaf> page;
      monitor_uplock_type page_uplock;
      monitor_exlock_type page_exlock;
      cleanup_data_vector cleanups;
    };

    cycle_ptr::cycle_gptr<raw_db_type> raw_db = this->raw_db.lock();
    if (raw_db == nullptr) return;
    std::invoke(*std::allocate_shared<op>(raw_db->get_allocator(), this->shared_from_this(this), raw_db));
  }

  template<typename TxAlloc>
  static auto async_visit(
      bplus_element_reference<raw_db_type, true> begin,
      bplus_element_reference<raw_db_type, true> end,
      TxAlloc tx_alloc,
      move_only_function<void(std::span<const bplus_element_reference<raw_db_type, true>>, move_only_function<void(std::error_code)>)> acceptor,
      move_only_function<void(std::error_code)> handler) {
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;

    struct element_ptr {
      element_ptr() noexcept = default;

      explicit element_ptr(cycle_ptr::cycle_gptr<element> elem) noexcept
      : elem(std::move(elem))
      {}

      cycle_ptr::cycle_gptr<element> elem;
      monitor_shlock_type lock;
    };
    using element_ptr_vector = std::vector<
        element_ptr,
        typename std::allocator_traits<TxAlloc>::template rebind_alloc<element_ptr>>;
    using reference_vector = std::vector<
        bplus_element_reference<raw_db_type, true>,
        typename std::allocator_traits<TxAlloc>::template rebind_alloc<bplus_element_reference<raw_db_type, true>>>;

    struct op
    : public std::enable_shared_from_this<op>
    {
      public:
      using allocator_type = TxAlloc;

      op(bplus_element_reference<raw_db_type, true> end, TxAlloc tx_alloc, move_only_function<void(std::span<const bplus_element_reference<raw_db_type, true>>, move_only_function<void(std::error_code)>)> acceptor, move_only_function<void(std::error_code)> handler)
      : end(std::move(end)),
        ptr_vector(tx_alloc),
        refs(tx_alloc),
        acceptor(std::move(acceptor)),
        handler(std::move(handler))
      {}

      auto get_allocator() const -> allocator_type { return ptr_vector.get_allocator(); }

      auto operator()(bplus_element_reference<raw_db_type, true> begin) -> void {
        begin->template async_lock_owner<monitor_shlock_type>(
            get_allocator(),
            [self_op=this->shared_from_this(), begin](cycle_ptr::cycle_gptr<bplus_tree_leaf> leaf, monitor_shlock_type leaf_lock) mutable -> void {
              self_op->gather(std::move(begin), false, std::move(leaf), std::move(leaf_lock));
            });
      }

      private:
      auto gather(bplus_element_reference<raw_db_type, true> begin, bool skip_first, cycle_ptr::cycle_gptr<bplus_tree_leaf> leaf, monitor_shlock_type leaf_lock) -> void {
        assert(ptr_vector.empty());
        assert(refs.empty());

        const auto leaf_elems_end = leaf->elements().end();
        for (auto iter = (begin.get() == nullptr ? leaf->elements().begin() : std::find(leaf->elements().begin(), leaf_elems_end, begin.get()));
            iter != leaf_elems_end;
            ++iter) {
          ptr_vector.emplace_back(*iter);
          if (*iter == end.get()) {
            fin = true;
            break;
          }
        }

        // We want to allow elements to be re-organized across leaves during the acceptor run.
        // So we'll release leaf locks, and only retain some tracking information.
        weak_leaf = leaf;
        leaf_versions = leaf->versions;
        leaf.reset();
        leaf_lock.reset();

        lock_all(skip_first);
      }

      auto lock_all(bool skip_first) -> void {
        lock_all(ptr_vector.begin(), skip_first);
      }

      auto lock_all(typename element_ptr_vector::iterator iter, bool skip_first) -> void {
        for (/* skip */; iter != ptr_vector.end(); ++iter) {
          auto opt_elem_lock = iter->elem->element_lock.try_shared(__FILE__, __LINE__);
          if (opt_elem_lock.has_value()) {
            iter->lock = std::move(opt_elem_lock).value();
          } else {
            iter->elem->element_lock.async_shared(
                [self_op=this->shared_from_this(), iter, skip_first](monitor_shlock_type elem_lock) {
                  iter->lock = std::move(elem_lock);
                  self_op->lock_all(std::next(iter), skip_first);
                },
                __FILE__, __LINE__);
            return; // Callback will resume the loop.
          }
        }

        // Now that everything is locked, we want to save the rear element.
        // We know the ptr_vector cannot be empty, since it contained the begin-lookup-element.
        assert(!ptr_vector.empty());
        last_elem = bplus_element_reference<raw_db_type, true>(ptr_vector.back().elem, ptr_vector.back().lock);

        // If we have to skip the first element, pop it now.
        assert(!ptr_vector.empty());
        if (skip_first) ptr_vector.erase(ptr_vector.begin());

        // Now that we have our continuation bookkeeping done, there's no more need for ptr_vector
        // to hold elements we don't care about.
        // So only retain "used" elements.
        ptr_vector.erase(
            std::remove_if(
                ptr_vector.begin(), ptr_vector.end(),
                [](const element_ptr& e) {
                  return e.elem->type() != bplus_tree_leaf_use_element::used;
                }),
            ptr_vector.end());

        invoke_acceptor();
      }

      auto invoke_acceptor() -> void {
        assert(refs.empty());
        std::transform(
            std::make_move_iterator(ptr_vector.begin()), std::make_move_iterator(ptr_vector.end()),
            std::back_inserter(refs),
            [](element_ptr&& e) {
              return bplus_element_reference<raw_db_type, true>(std::move(e.elem), std::move(e.lock));
            });
        ptr_vector.clear(); // No longer needed, and we like to release locks early.

        if (refs.empty()) {
          // refs can be empty, if there were no "used" elements within the ptr_vector.
          if (fin)
            std::invoke(handler, std::error_code{});
          else
            resume();
        } else {
          std::invoke(
              acceptor,
              std::span<const bplus_element_reference<raw_db_type, true>>(refs.data(), refs.size()),
              [self_op=this->shared_from_this()](std::error_code ec) {
#ifndef NDEBUG
                // Release memory for refs, so that if acceptor is using a dangling reference, it'll crash, hopefully.
                // Should make debugging easier.
                // Sadly the only way to force this, is to re-create the vector, because C++ vectors don't have any
                // binding ways of shrinking.
                reference_vector(self_op->refs.get_allocator()).swap(self_op->refs);
#else
                self_op->refs.clear(); // No longer needed.
#endif

                if (ec || self_op->fin)
                  std::invoke(self_op->handler, ec);
                else
                  self_op->resume();
              });
        }
      }

      auto resume() -> void {
        assert(last_elem.get() != nullptr);
        last_elem->template async_lock_owner<monitor_shlock_type>(
            get_allocator(),
            [self_op=this->shared_from_this()](cycle_ptr::cycle_gptr<bplus_tree_leaf> leaf, monitor_shlock_type leaf_lock) mutable -> void {
              if (leaf != self_op->weak_leaf.lock() || leaf->versions.page_split != self_op->leaf_versions.page_split || leaf->versions.page_merge != self_op->leaf_versions.page_merge) {
                // The leaf was split or merged from under us, or just completely rewritten.
                // Either way, it's no longer the same leaf, so we can't be sure we visited all elements.
                // So the restart, right after the last-considered-element.
                self_op->gather(std::move(self_op->last_elem), true, std::move(leaf), std::move(leaf_lock));
              } else {
                leaf->async_next_page(self_op->get_allocator(), asio::deferred)
                | [self_op, leaf](std::error_code ec, cycle_ptr::cycle_gptr<bplus_tree_leaf> next_page, monitor_shlock_type next_page_lock, monitor_shlock_type leaf_lock) {
                    if (ec == make_error_code(db_errc::data_expired)) {
                      self_op->last_elem->element_lock.async_shared(asio::deferred, __FILE__, __LINE__)
                      | [self_op, leaf, ec]([[maybe_unused]] monitor_shlock_type last_elem_lock) {
                          if (self_op->last_elem->owner == leaf) {
                            std::invoke(self_op->handler, ec);
                          } else {
                            last_elem_lock.reset();
                            self_op->resume(); // retry: page got moved from under us
                          }
                        };
                      return;
                    } else if (ec) {
                      std::invoke(self_op->handler, ec);
                      return;
                    }

                    if (leaf->versions.page_split != self_op->leaf_versions.page_split || leaf->versions.page_merge != self_op->leaf_versions.page_merge) {
                      self_op->gather(std::move(self_op->last_elem), true, std::move(leaf), std::move(leaf_lock));
                    } else if (next_page == nullptr) {
                      std::clog << "bplus-tree: iteration running out of pages without finding iteration end point...\n";
                      leaf->maybe_log_dump("most recent page");
                      std::invoke(self_op->handler, std::error_code{});
                    } else {
                      leaf_lock.reset();
                      self_op->gather(bplus_element_reference<raw_db_type, true>{}, false, std::move(next_page), std::move(next_page_lock));
                    }
                  };
              }
            });
      }

      bplus_element_reference<raw_db_type, true> end;
      element_ptr_vector ptr_vector;
      reference_vector refs;
      move_only_function<void(std::span<const bplus_element_reference<raw_db_type, true>>, move_only_function<void(std::error_code)>)> acceptor;
      move_only_function<void(std::error_code)> handler;

      bool fin = false;
      cycle_ptr::cycle_weak_ptr<bplus_tree_leaf> weak_leaf;
      bplus_tree_versions leaf_versions;
      bplus_element_reference<raw_db_type, true> last_elem;
    };

    std::invoke(*std::allocate_shared<op>(tx_alloc, std::move(end), tx_alloc, std::move(acceptor), std::move(handler)), std::move(begin));
  }

  template<typename Stream, typename CompletionToken>
  auto continue_load_(Stream& stream, cycle_ptr::cycle_gptr<raw_db_type> raw_db, CompletionToken&& token) {
    return async_read(
        stream,
        bplus_tree_leaf::continue_xdr_(::earnest::xdr_reader<>(), *this)
        & xdr_processor(
            [this, raw_db]() -> std::error_code {
              assert(this->elements_.empty());
              const auto use_list = this->use_list_span();
              for (std::size_t i = 0; i < use_list.size(); ++i) {
                switch (use_list[i]) {
                  case bplus_tree_leaf_use_element::ghost_create:
                  case bplus_tree_leaf_use_element::ghost_delete:
                    // We'll handle cleaning these up during the on-load call.
                    [[fallthrough]];
                  default:
                    this->elements_.emplace_back(
                        cycle_ptr::allocate_cycle<element>(
                            this->get_allocator(),
                            this->get_executor(),
                            i,
                            this->shared_from_this(this),
                            raw_db->get_allocator()));
                    break;
                  case bplus_tree_leaf_use_element::unused:
                    /* skip */
                    break;
                }
              }
              return {};
            }),
        std::forward<CompletionToken>(token));
  }

  template<typename X>
  static auto continue_xdr_(::earnest::xdr<X>&& x, typename ::earnest::xdr<X>::template typed_function_arg<bplus_tree_leaf> y) {
    return std::move(x) & y.hdr_ & xdr_dynamic_raw_bytes(asio::buffer(y.bytes_));
  }

  template<bool ForWrite, typename GetAddrFn, typename InverseGetAddrFn>
  [[deprecated]]
  auto nextprev_page_op_(std::bool_constant<ForWrite> for_write, GetAddrFn get_addr_fn, InverseGetAddrFn inverse_get_addr_fn) {
    return this->page_lock.async_shared(asio::deferred, __FILE__, __LINE__)
    | asio::deferred(
        [ self=this->shared_from_this(this),
          for_write=std::move(for_write),
          get_addr_fn=std::move(get_addr_fn),
          inverse_get_addr_fn=std::move(inverse_get_addr_fn)
        ](auto lock) mutable {
          return self->page_variant_op(
              for_write, std::move(lock),
              // get-address function:
              [get_addr_fn](const auto& self) {
                return std::invoke(get_addr_fn, boost::polymorphic_pointer_downcast<cycle_ptr::cycle_gptr<bplus_tree_leaf>>(self));
              },
              // validation function:
              [inverse_get_addr_fn](const auto& target_ptr, const auto& self_ptr) {
                return std::invoke(
                    overload(
                        [&inverse_get_addr_fn](const bplus_tree_leaf& leaf, const auto& self_ptr) -> bool {
                          return std::invoke(inverse_get_addr_fn, &leaf) == self_ptr->address;
                        },
                        []([[maybe_unused]] const bplus_tree_intr<RawDbType>& intr, [[maybe_unused]] const auto& self_ptr) -> bool {
                          return true; // It's actually the wrong type, but that page (intr) was loaded while this page (self_ptr) was locked, so we know it was the actual pointer.
                                       // Next, the variant_to_pointer_op will cause this to be rejected.
                        }),
                    *target_ptr, self_ptr);
              },
              // restart function:
              [self, for_write, get_addr_fn, inverse_get_addr_fn]() mutable {
                return self->nextprev_page_op_(std::move(for_write), std::move(get_addr_fn), std::move(inverse_get_addr_fn));
              });
        })
    | bplus_tree_page<RawDbType>::template variant_to_pointer_op<bplus_tree_leaf>();
  }

  auto use_list_offset() const noexcept -> std::size_t { return 0u; }
  auto use_list_length() const noexcept -> std::size_t { return this->spec->elements_per_leaf; }
  auto element0_offset() const noexcept -> std::size_t { return use_list_offset() + round_up(use_list_length(), 4u); }
  auto element_length() const noexcept -> std::size_t { return this->spec->element.key.padded_bytes() + this->spec->element.value.padded_bytes(); }
  auto element_offset(std::size_t idx) const noexcept -> std::size_t { return element0_offset() + idx * element_length(); }

  auto use_list_span() const noexcept -> std::span<const bplus_tree_leaf_use_element> {
    assert(bytes_.size() >= use_list_offset() + use_list_length());
    const auto use_list_bytes = std::span<const std::byte>(bytes_).subspan(use_list_offset(), use_list_length());
    return {reinterpret_cast<const bplus_tree_leaf_use_element*>(use_list_bytes.data()), use_list_bytes.size()};
  }

  auto use_list_span() noexcept -> std::span<bplus_tree_leaf_use_element> {
    assert(bytes_.size() >= use_list_offset() + use_list_length());
    const auto use_list_bytes = std::span<std::byte>(bytes_).subspan(use_list_offset(), use_list_length());
    return {reinterpret_cast<bplus_tree_leaf_use_element*>(use_list_bytes.data()), use_list_bytes.size()};
  }

  auto element_span(std::size_t idx) const noexcept -> std::span<const std::byte> {
    assert(idx < this->spec->elements_per_leaf);
    assert(bytes_.size() >= element_offset(this->spec->elements_per_leaf));
    return { &bytes_[element_offset(idx)], element_length() };
  }

  auto element_span(std::size_t idx) noexcept -> std::span<std::byte> {
    assert(idx < this->spec->elements_per_leaf);
    assert(bytes_.size() >= element_offset(this->spec->elements_per_leaf));
    return { &bytes_[element_offset(idx)], element_length() };
  }

  auto element_multispan(std::size_t b, std::size_t e) const noexcept -> std::span<const std::byte> {
    assert(b < this->spec->elements_per_leaf);
    assert(e <= this->spec->elements_per_leaf);
    assert(bytes_.size() >= element_offset(this->spec->elements_per_leaf));
    return { &bytes_[element_offset(b)], element_offset(e) - element_offset(b) };
  }

  auto element_multispan(std::size_t b, std::size_t e) noexcept -> std::span<std::byte> {
    assert(b < this->spec->elements_per_leaf);
    assert(e <= this->spec->elements_per_leaf);
    assert(bytes_.size() >= element_offset(this->spec->elements_per_leaf));
    return { &bytes_[element_offset(b)], element_offset(e) - element_offset(b) };
  }

  auto key_span(std::size_t idx) const noexcept -> std::span<const std::byte> {
    assert(idx < this->spec->elements_per_leaf);
    assert(bytes_.size() >= element_offset(this->spec->elements_per_leaf));
    return { &bytes_[element_offset(idx)], this->spec->element.key.bytes };
  }

  auto key_span(std::size_t idx) noexcept -> std::span<std::byte> {
    assert(idx < this->spec->elements_per_leaf);
    assert(bytes_.size() >= element_offset(this->spec->elements_per_leaf));
    return { &bytes_[element_offset(idx)], this->spec->element.key.bytes };
  }

  auto value_span(std::size_t idx) const noexcept -> std::span<const std::byte> {
    assert(idx < this->spec->elements_per_leaf);
    assert(bytes_.size() >= element_offset(this->spec->elements_per_leaf));
    return { &bytes_[element_offset(idx) + this->spec->element.key.padded_bytes()], this->spec->element.value.bytes };
  }

  auto value_span(std::size_t idx) noexcept -> std::span<std::byte> {
    assert(idx < this->spec->elements_per_leaf);
    assert(bytes_.size() >= element_offset(this->spec->elements_per_leaf));
    return { &bytes_[element_offset(idx) + this->spec->element.key.padded_bytes()], this->spec->element.value.bytes };
  }

  auto async_compute_page_augments_impl_(move_only_function<void(std::vector<std::byte>)> callback) const -> void override {
    using byte_vector = std::vector<std::byte>;
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;

    const auto augments = std::make_shared<std::vector<byte_vector>>();
    augments->reserve(this->elements_.size());
    auto barrier = make_completion_barrier(
        [ augments, callback=std::move(callback),
          self=this->shared_from_this(this)
        ](std::error_code ec) {
          assert(!ec);

          auto augment_spans = std::vector<std::span<const std::byte>>(augments->size());
          std::transform(
              augments->begin(), augments->end(),
              augment_spans.begin(),
              [](const byte_vector& v) { return std::span<const std::byte>(v); });
          callback(self->combine_augments(std::move(augment_spans)));
        },
        this->get_executor());

    for (const auto& eptr : this->elements_) {
      const bplus_tree_leaf_use_element eptr_type = eptr->type();
      if (eptr_type == bplus_tree_leaf_use_element::used) {
        const auto i = augments->size();
        augments->emplace_back();
        eptr->element_lock.dispatch_shared(
            completion_wrapper<void(monitor_shlock_type)>(
                ++barrier,
                [ i, augments,
                  eptr=cycle_ptr::cycle_gptr<const element>(eptr),
                  self=this->shared_from_this(this)
                ](auto handler, monitor_shlock_type lock) -> void {
                  (*augments)[i] = self->augments_for_value_(*eptr);
                  lock.reset();
                  std::invoke(handler, std::error_code());
                }),
            __FILE__, __LINE__);
      }
    }

    std::invoke(barrier, std::error_code());
  }

  auto augments_for_value_(const element& e) const -> std::vector<std::byte> {
    using byte_vector = std::vector<std::byte>;

    auto augments = byte_vector(this->spec->element.padded_augment_bytes(), std::byte(0));
    std::span<std::byte> data_span = std::span(augments);

    for (const bplus_tree_augment_spec& spec : this->spec->element.augments) {
      assert(data_span.size() >= spec.padded_bytes());
      spec.augment_value(data_span.subspan(0, spec.bytes), e.key_span(), e.value_span());
      data_span = data_span.subspan(spec.padded_bytes());
    }
    assert(data_span.empty());

    return augments;
  }

  protected:
  auto log_dump_impl_(std::ostream& out) const -> void override {
    using namespace std::literals;

    out << "earnest::detail::bplus_tree_leaf: {\n"sv;
    this->bplus_tree_page<RawDbType>::log_dump_impl_(out);
    out << "  next_page_address: "sv << next_page_address() << "\n"sv;
    out << "  prev_page_address: "sv << prev_page_address() << "\n"sv;
    out << "  elements_size: "sv << elements_.size() << "\n"sv;

    if (elements_.empty()) {
      out << "  elements: []\n"sv;
    } else {
      out << "  elements: [\n"sv;
      for (const auto& elem_ptr : elements_) {
        if (elem_ptr == nullptr) {
          out << "    nullptr\n"sv;
          continue;
        }

        out << "    { index: "sv << elem_ptr->index << ", owner: "sv;
        if (elem_ptr->owner == nullptr)
          out << "nullptr"sv;
        else
          out << elem_ptr->owner->address;
        out << " }\n"sv;
      }
      out << "  ]\n"sv;
    }

    for (std::size_t i = 0; i < this->spec->elements_per_leaf; ++i) {
      out << "  use_list["sv << i << "]: "sv << use_list_span()[i] << "\n"sv;
      if (use_list_span()[i] == bplus_tree_leaf_use_element::unused) continue;

      out << "  key["sv << i << "]: "sv << byte_span_printer(key_span(i)) << "\n"sv;
      out << "  value["sv << i << "]: "sv << byte_span_printer(value_span(i)) << "\n"sv;
    }

    out << "}\n"sv;
  }

  private:
  std::vector<std::byte, rebind_alloc<std::byte>> bytes_;
  element_vector elements_;
  bplus_tree_leaf_header hdr_;
};


/*
 * A B+ tree.
 *
 * RawDbType: raw database interface. B+ tree operates directly on raw storage.
 *   B+ tree will use the executor and allocators from RawDbType.
 * DBAllocator: allocator for the B+ tree. Because B+ tree can be used in allocators,
 *   the allocation is always done before acquiring locks on the tree.
 */
template<typename RawDbType, typename DBAllocator>
class bplus_tree
: public db_cache_value,
  private cycle_ptr::cycle_base
{
  public:
  static inline constexpr std::uint32_t magic = bplus_tree_header::magic;
  using raw_db_type = RawDbType;
  using executor_type = typename raw_db_type::executor_type;
  using allocator_type = typename raw_db_type::cache_allocator_type;

  private:
  using page_type = bplus_tree_page<RawDbType>;
  using intr_type = bplus_tree_intr<RawDbType>;
  using leaf_type = bplus_tree_leaf<RawDbType>;

  public:
  using element = typename leaf_type::element;

  static inline constexpr std::uint64_t nil_page = page_type::nil_page;

  bplus_tree(std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address, DBAllocator db_allocator) noexcept(std::is_nothrow_move_constructible_v<db_address>)
  : address(address),
    raw_db(raw_db),
    spec(std::move(spec)),
    page_lock(raw_db->get_executor(), "earnest::detail::bplus_tree{" + address.to_string() + "}", raw_db->get_allocator()),
    ex_(raw_db->get_executor()),
    alloc_(raw_db->get_cache_allocator()),
    db_alloc_(std::move(db_allocator))
  {}

  ~bplus_tree() override = default;

  auto get_executor() const -> executor_type { return ex_; }
  auto get_allocator() const -> allocator_type { return alloc_; }

  template<typename AcceptanceFn>
  static auto async_load_op(std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address, DBAllocator db_allocator, AcceptanceFn&& acceptance_fn) {
    return raw_db->async_get(
        typename raw_db_type::template key_type<bplus_tree>{.file=address.file, .offset=address.offset},
        asio::deferred,
        [](auto stream, std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address, DBAllocator db_allocator) {
          auto stream_ptr = std::make_shared<decltype(stream)>(std::move(stream));
          return raw_load_op(*stream_ptr, std::move(spec), std::move(raw_db), std::move(address), std::move(db_allocator))
          | asio::deferred(
              [stream_ptr](std::error_code ec, cycle_ptr::cycle_gptr<bplus_tree> tree) mutable {
                stream_ptr.reset(); // Using `stream_ptr` like this silences compiler warnings.
                return asio::deferred.values(std::move(ec), std::move(tree));
              });
        },
        std::forward<AcceptanceFn>(acceptance_fn),
        spec, raw_db, address, std::move(db_allocator));
  }

  template<typename BytesAllocator = std::allocator<std::byte>>
  static auto creation_bytes(BytesAllocator allocator = BytesAllocator()) -> std::vector<std::byte, typename std::allocator_traits<BytesAllocator>::template rebind_alloc<std::byte>> {
    asio::io_context ioctx;
    auto stream = byte_stream<asio::io_context::executor_type, typename std::allocator_traits<BytesAllocator>::template rebind_alloc<std::byte>>(ioctx.get_executor(), allocator);
    std::error_code error;
    async_write(
        stream,
        ::earnest::xdr_writer<>() & xdr_constant(bplus_tree_header{.root=nil_page}),
        [&error](std::error_code ec) { error = ec; });
    ioctx.run();
    if (error) throw std::system_error(error);
    return std::move(stream).data();
  }

  private:
  template<typename T, std::size_t TExtent, typename U, std::size_t UExtent>
  requires std::assignable_from<T&, U&>
  static auto span_copy(std::span<T, TExtent> dst, std::span<U, UExtent> src) -> void {
    static_assert(TExtent == std::dynamic_extent || UExtent == std::dynamic_extent || TExtent == UExtent, "extents are of unequal length");

    if (dst.size() != src.size()) throw std::invalid_argument("span size mismatch during page-split");
    if (&*dst.begin() >= &*src.begin() && &*dst.begin() < &*src.end())
      std::copy_backward(src.begin(), src.end(), dst.end());
    else
      std::copy(src.begin(), src.end(), dst.begin());
  }

  template<typename TxAlloc>
  struct tree_path {
    using allocator_type = TxAlloc;

    template<typename PageType>
    struct versioned_page {
      constexpr versioned_page() noexcept = default;

      explicit versioned_page(cycle_ptr::cycle_gptr<PageType> page) noexcept
      : page(std::move(page)),
        versions(this->page->versions)
      {}

      auto assign(cycle_ptr::cycle_gptr<PageType> page) noexcept -> void {
        *this = versioned_page(std::move(page));
      }

      cycle_ptr::cycle_gptr<PageType> page;
      bplus_tree_versions versions;
    };

    tree_path() = default;

    explicit tree_path(cycle_ptr::cycle_gptr<bplus_tree> tree, allocator_type alloc = allocator_type())
    : tree(std::move(tree)),
      interior_pages(std::move(alloc))
    {}

    auto get_allocator() const -> allocator_type { return interior_pages.get_allocator(); }

    auto clear() -> void {
      tree.reset();
      interior_pages.clear();
      leaf_page.page.reset();
    }

    cycle_ptr::cycle_gptr<bplus_tree> tree;
    std::vector<versioned_page<intr_type>, typename std::allocator_traits<TxAlloc>::template rebind_alloc<versioned_page<intr_type>>> interior_pages;
    versioned_page<leaf_type> leaf_page;
  };

  template<typename Stream>
  static auto raw_load_op(Stream& stream, std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address, DBAllocator db_allocator) {
    auto result = cycle_ptr::allocate_cycle<bplus_tree>(raw_db->get_cache_allocator(), std::move(spec), raw_db, std::move(address), std::move(db_allocator));
    return async_read(
        stream,
        ::earnest::xdr_reader<>() & result->hdr_,
        asio::append(asio::deferred, result));
  }

  // Install a root page.
  // Callback arguments:
  // - std::error_code ec: error code
  // - cycle_ptr::cycle_gptr<leaf_type> leaf: newly installed root page
  // - monitor<executor_type, raw_db_type::allocator_type>::upgrade_lock exlock: exclusive lock on the tree
  //
  // Call may fail with bplus_tree_errc::root_page_present, in which case a root page already exists.
  // In this case, the lock will be held.
  template<typename TxAlloc, typename CompletionToken>
  auto install_root_page_(TxAlloc tx_alloc, CompletionToken&& token) {
    using tx_file_type = typename raw_db_type::template fdb_transaction<TxAlloc>::file;
    using stream_type = positional_stream_adapter<tx_file_type>;
    using monitor_exlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock;
    using monitor_uplock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock;

    return asio::async_initiate<CompletionToken, void(std::error_code, cycle_ptr::cycle_gptr<leaf_type>, monitor_uplock_type)>(
        [](auto handler, TxAlloc tx_alloc, cycle_ptr::cycle_gptr<bplus_tree> self) -> void {
          cycle_ptr::cycle_gptr<raw_db_type> raw_db = self->raw_db.lock();
          if (raw_db == nullptr) {
            std::invoke(handler, make_error_code(db_errc::data_expired), nullptr, monitor_uplock_type{});
            return;
          }
          auto tx = raw_db->fdb_tx_begin(isolation::read_commited, tx_mode::read_write, tx_alloc);

          leaf_type::async_new_page(tx, self->db_alloc_, self->spec, raw_db, nil_page, nil_page, nil_page, true, asio::deferred)
          | asio::deferred(
              // Verify the allocator didn't accidentally allocate in a different file.
              [self](std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> leaf) {
                if (!ec && self->address.file != leaf->address.file) [[unlikely]] {
                  std::clog << "bplus-tree: tree lives in " << self->address.file << ", but allocator allocates to " << leaf->address.file << "; will fail root-page construction since bplus-tree cannot span multiple files.\n";
                  ec = make_error_code(bplus_tree_errc::allocator_file_mismatch);
                }

                return asio::deferred.values(std::move(ec), std::move(leaf));
              })
          | asio::deferred(
              // Write an updated bplus_tree record.
              [tx, self](std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> leaf) {
                return asio::deferred.when(!ec)
                    .then(
                        asio::deferred.values(tx, leaf)
                        | asio::deferred(
                            [self](auto tx, cycle_ptr::cycle_gptr<leaf_type> leaf) {
                              auto stream = std::allocate_shared<stream_type>(tx.get_allocator(), tx[self->address.file], self->address.offset);
                              auto new_hdr = self->hdr_;
                              new_hdr.root = leaf->address.offset;
                              return async_write(
                                  *stream,
                                  ::earnest::xdr_writer<>() & xdr_constant(std::move(new_hdr)),
                                  asio::append(asio::deferred, leaf, stream))
                              | asio::deferred(
                                  // We need to capture stream, to ensure it remains valid until the write-call completes.
                                  [](std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> leaf, [[maybe_unused]] auto stream) {
                                    return asio::deferred.values(std::move(ec), std::move(leaf));
                                  });
                            }))
                    .otherwise(asio::deferred.values(ec, leaf));
              })
          | asio::deferred(
              [self](std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> leaf) {
                return asio::deferred.when(!ec)
                    .then(self->page_lock.dispatch_upgrade(asio::append(asio::deferred, ec, leaf), __FILE__, __LINE__))
                    .otherwise(asio::deferred.values(monitor_uplock_type(), ec, leaf));
              })
          | asio::deferred(
              // Re-order arguments, because having `ec` not be the first argument is weird.
              [](monitor_uplock_type lock, std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> leaf) {
                return asio::deferred.values(std::move(ec), std::move(leaf), std::move(lock));
              })
          | asio::deferred(
              [self](std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> leaf, monitor_uplock_type lock) {
                if (!ec && self->erased_) [[unlikely]]
                  ec = make_error_code(db_errc::data_expired);
                if (!ec && self->hdr_.root != nil_page)
                  ec = make_error_code(bplus_tree_errc::root_page_present);

                return asio::deferred.values(std::move(ec), std::move(leaf), std::move(lock));
              })
          | asio::deferred(
              [tx](std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> leaf, monitor_uplock_type lock) mutable {
                return asio::deferred.when(!ec)
                    .then(tx.async_commit(asio::append(asio::deferred, leaf, lock)))
                    .otherwise(asio::deferred.values(ec, leaf, lock));
              })
          | asio::deferred(
              [](std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> leaf, monitor_uplock_type lock) {
                return asio::deferred.when(!ec)
                    .then(std::move(lock).dispatch_exclusive(asio::append(asio::deferred, ec, leaf)))
                    .otherwise(asio::deferred.values(monitor_exlock_type{}, ec, leaf));
              })
          | asio::deferred(
              [self](monitor_exlock_type lock, std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> leaf) {
                monitor_uplock_type uplock;
                if (!ec) {
                  self->hdr_.root = leaf->address.offset;
                  uplock = lock.as_upgrade_lock();
                }
                return asio::deferred.values(std::move(ec), std::move(leaf), std::move(uplock));
              })
          | std::move(handler);
        },
        token, std::move(tx_alloc), this->shared_from_this(this));
  }

  auto maybe_load_root_page_nocreate_op_() {
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;
    using page_variant = std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>>;

    auto self = this->shared_from_this(this);

    return self->page_lock.dispatch_shared(asio::deferred, __FILE__, __LINE__)
    | asio::deferred(
        [self](monitor_shlock_type tree_lock) {
          const auto load_offset = self->hdr_.root;
          tree_lock.reset();

          std::error_code ec;
          auto raw_db = self->raw_db.lock();
          if (raw_db == nullptr) ec = make_error_code(db_errc::data_expired);

          return asio::deferred.when(!ec && load_offset != nil_page)
              .then(
                  page_type::async_load_op(
                      self->spec,
                      raw_db,
                      db_address(self->address.file, load_offset),
                      [self, load_offset]() {
                        return self->page_lock.dispatch_shared(asio::deferred, __FILE__, __LINE__)
                        | asio::deferred(
                            [self, load_offset]([[maybe_unused]] monitor_shlock_type tree_lock) {
                              std::error_code ec;
                              if (self->erased_ || self->hdr_.root != load_offset)
                                ec = make_error_code(bplus_tree_errc::restart);
                              return asio::deferred.values(ec);
                            });
                      })
                  | asio::deferred(
                      [self, load_offset](std::error_code ec, page_variant page) {
                        return asio::deferred.when(!ec)
                            .then(
                                self->page_lock.dispatch_shared(asio::append(asio::deferred, page), __FILE__, __LINE__)
                                | asio::deferred(
                                    [self, load_offset](monitor_shlock_type tree_lock, page_variant page) {
                                      std::error_code ec;
                                      if (self->erased_) [[unlikely]]
                                        ec = make_error_code(db_errc::data_expired);
                                      else if (self->hdr_.root != load_offset)
                                        ec = make_error_code(bplus_tree_errc::restart);

                                      return asio::deferred.values(std::move(ec), std::move(page), std::move(tree_lock));
                                    }))
                            .otherwise(asio::deferred.values(ec, page, monitor_shlock_type{}));
                      }))
              .otherwise(asio::deferred.values(std::error_code(), page_variant(), tree_lock));
        });
  }

  auto load_root_page_nocreate_op_() {
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;
    using page_variant = std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>>;

    return asio::async_initiate<decltype(asio::deferred), void(std::error_code, page_variant, monitor_shlock_type)>(
        []<typename Handler>(Handler handler, cycle_ptr::cycle_gptr<bplus_tree> self) -> void {
          struct op {
            explicit op(cycle_ptr::cycle_gptr<bplus_tree> self) noexcept
            : self(std::move(self))
            {}

            auto operator()(Handler handler) const -> void {
              self->maybe_load_root_page_nocreate_op_()
              | completion_wrapper<void(std::error_code, page_variant, monitor_shlock_type)>(
                  std::move(handler),
                  [op=*this](auto handler, std::error_code ec, page_variant page, monitor_shlock_type tree_lock) mutable {
                    if (ec == make_error_code(bplus_tree_errc::restart))
                      return std::invoke(op, std::move(handler));
                    else
                      std::invoke(handler, std::move(ec), std::move(page), std::move(tree_lock));
                  });
            }

            private:
            cycle_ptr::cycle_gptr<bplus_tree> self;
          };

          std::invoke(op(std::move(self)), std::move(handler));
        },
        asio::deferred, this->shared_from_this(this));
  }

  template<typename TxAlloc>
  auto install_or_get_root_page_op_(TxAlloc tx_alloc) {
    using monitor_uplock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock;
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;
    using lock_variant = std::variant<monitor_uplock_type, monitor_shlock_type>;
    using page_variant = std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>>;

    return asio::async_initiate<decltype(asio::deferred), void(std::error_code, page_variant, lock_variant /*tree_lock*/)>(
        []<typename HandlerType>(HandlerType handler, cycle_ptr::cycle_gptr<bplus_tree> self, TxAlloc tx_alloc) -> void {
          using handler_type = decltype(completion_handler_fun(std::declval<HandlerType>(), std::declval<executor_type>()));

          struct op {
            explicit op(cycle_ptr::cycle_gptr<bplus_tree> tree, handler_type handler, TxAlloc tx_alloc)
            : handler(std::move(handler)),
              tx_alloc(std::move(tx_alloc)),
              tree(std::move(tree))
            {}

            auto operator()() -> void {
              const cycle_ptr::cycle_gptr<bplus_tree> tree = this->tree; // copy, so we can move self_op.
              tree->load_root_page_nocreate_op_()
              | [ self_op=std::move(*this)
                ](std::error_code ec, page_variant root_page, monitor_shlock_type tree_lock) mutable {
                  if (ec || std::visit([](const auto& pg) { return pg != nullptr; }, root_page)) {
                    std::invoke(self_op.handler, std::move(ec), std::move(root_page), lock_variant(std::move(tree_lock)));
                  } else {
                    const cycle_ptr::cycle_gptr<bplus_tree> tree = self_op.tree; // copy, so we can move self_op.
                    TxAlloc tx_alloc = self_op.tx_alloc; // copy, so we can move self_op.
                    tree->install_root_page_(
                        tx_alloc,
                        [ self_op=std::move(self_op)
                        ](std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> leaf, monitor_uplock_type tree_lock) mutable {
                          if (ec == make_error_code(bplus_tree_errc::root_page_present))
                            std::invoke(self_op);
                          else
                            std::invoke(self_op.handler, std::move(ec), page_variant(std::move(leaf)), lock_variant(std::move(tree_lock)));
                        });
                  }
                };
            }

            private:
            handler_type handler;
            TxAlloc tx_alloc;
            cycle_ptr::cycle_gptr<bplus_tree> tree;
          };

          std::invoke(op(self, completion_handler_fun(std::move(handler), self->get_executor()), std::move(tx_alloc)));
        },
        asio::deferred, this->shared_from_this(this), std::move(tx_alloc));
  }

  template<typename LeafLockType = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock, typename TxAlloc>
  requires std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock> || std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock> || std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock>
  static auto tree_walk_impl_(move_only_function<std::variant<db_address, std::error_code>(cycle_ptr::cycle_gptr<intr_type> intr)> address_selector_fn, tree_path<TxAlloc> path, move_only_function<void(std::error_code, tree_path<TxAlloc>, LeafLockType)> handler) -> void {
    using address_selector_fn_type = move_only_function<std::variant<db_address, std::error_code>(cycle_ptr::cycle_gptr<intr_type> intr)>;
    using monitor_uplock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock;
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;
    using lock_variant = std::variant<monitor_uplock_type, monitor_shlock_type>;
    using page_variant = std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>>;
    using handler_type = move_only_function<void(std::error_code, tree_path<TxAlloc>, LeafLockType)>;

    struct op {
      explicit op(address_selector_fn_type address_selector_fn, handler_type handler)
      : handler(std::move(handler)),
        address_selector_fn(std::move(address_selector_fn))
      {}

      auto start(tree_path<TxAlloc> path) -> void {
        if (path.interior_pages.empty()) {
          auto tx_alloc = path.get_allocator(); // Make a copy, since we'll move path.
          auto tree = path.tree; // Make a copy, since we'll move path.
          tree->install_or_get_root_page_op_(tx_alloc)
          | [ self_op=std::move(*this),
              path=std::move(path)
            ](std::error_code ec, page_variant page, lock_variant parent_lock) mutable -> void {
              if (ec) {
                self_op.error_invoke_(std::move(ec));
              } else {
                std::visit(
                    [&](auto page) -> void {
                      self_op.search(std::move(path), std::move(page), std::move(parent_lock));
                    },
                    std::move(page));
              }
            };
        } else { // !path.interior_pages.empty()
          auto page = path.interior_pages.back().page; // Make a copy, since we'll move path.
          page->page_lock.dispatch_shared(asio::deferred, __FILE__, __LINE__)
          | [ self_op=std::move(*this),
              path=std::move(path)
            ](monitor_shlock_type last_page_lock) mutable -> void {
              auto& last_page = path.interior_pages.back();
              if (last_page.page->erased_ || last_page.page->versions.page_split != last_page.versions.page_split) {
                // We want to restart the search from the most recent page that hasn't been split/merged since we last observed it.
                path.interior_pages.pop_back();
                self_op.start(std::move(path));
              } else {
                auto page = last_page.page;
                self_op.search_with_page_locked(std::move(path), std::move(page), std::move(last_page_lock));
              }
            };
        }
      }

      private:
      auto search(tree_path<TxAlloc> path, cycle_ptr::cycle_gptr<leaf_type> page, lock_variant parent_lock) -> void {
        auto callback =
            [ self_op=std::move(*this),
              path=std::move(path),
              page,
              parent_lock=std::move(parent_lock)
            ](auto page_lock) mutable -> void {
              std::visit([](auto& lock) { lock.reset(); }, parent_lock); // No longer needed, now that `page_lock` is locked.
              path.leaf_page.assign(page);
              self_op.search_with_page_locked(std::move(path), std::move(page), std::move(page_lock));
            };
        if constexpr(std::is_same_v<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock>) {
          page->page_lock.dispatch_shared(std::move(callback), __FILE__, __LINE__);
        } else if constexpr(std::is_same_v<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock>) {
          page->page_lock.dispatch_upgrade(std::move(callback), __FILE__, __LINE__);
        } else {
          static_assert(std::is_same_v<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock>);
          page->page_lock.dispatch_exclusive(std::move(callback), __FILE__, __LINE__);
        }
      }

      auto search(tree_path<TxAlloc> path, cycle_ptr::cycle_gptr<intr_type> page, lock_variant parent_lock) -> void {
        page->page_lock.dispatch_shared(
            [ self_op=std::move(*this),
              path=std::move(path),
              page,
              parent_lock=std::move(parent_lock)
            ](auto page_lock) mutable -> void {
              std::visit([](auto& lock) { lock.reset(); }, parent_lock); // No longer needed, now that `page_lock` is locked.
              path.interior_pages.emplace_back(page);
              self_op.search_with_page_locked(std::move(path), std::move(page), std::move(page_lock));
            },
            __FILE__, __LINE__);
      }

      auto search_with_page_locked(tree_path<TxAlloc> path, [[maybe_unused]] cycle_ptr::cycle_gptr<leaf_type> page, LeafLockType page_lock) -> void {
        assert(path.leaf_page.page == page);
        std::invoke(handler, std::error_code(), std::move(path), std::move(page_lock));
      }

      auto search_with_page_locked(tree_path<TxAlloc> path, cycle_ptr::cycle_gptr<intr_type> intr, monitor_shlock_type page_lock) -> void {
        auto intr_version = intr->versions.page_split;
        db_address child_page_address;
        cycle_ptr::cycle_gptr<raw_db_type> raw_db;
        {
          std::error_code ec;
          std::visit(
              overload(
                  [&ec](std::error_code addr_selection_ec) {
                    if (!ec) throw std::logic_error("address selector returned an error without error state");
                    ec = addr_selection_ec;
                  },
                  [&child_page_address](db_address addr) {
                    child_page_address = std::move(addr);
                  }),
              std::invoke(address_selector_fn, intr));
          page_lock.reset(); // No longer need the lock.

          if (!ec) [[likely]] {
            raw_db = intr->raw_db.lock();
            if (raw_db == nullptr) [[unlikely]] ec = make_error_code(db_errc::data_expired);
          }

          if (ec) {
            error_invoke_(ec);
            return;
          }
        }

        page_type::async_load_op(
            intr->spec, std::move(raw_db), child_page_address,
            [intr, child_page_address]() {
              return intr->page_lock.dispatch_shared(asio::deferred, __FILE__, __LINE__)
              | asio::deferred(
                  [intr, child_page_address]([[maybe_unused]] monitor_shlock_type intr_lock) {
                    std::error_code ec;
                    if (!intr->contains(child_page_address))
                      ec = make_error_code(bplus_tree_errc::restart);
                    return asio::deferred.values(ec);
                  });
            })
        | asio::deferred(
            [intr](std::error_code ec, page_variant child_page) {
              return asio::deferred.when(!ec)
                  .then(intr->page_lock.dispatch_shared(asio::append(asio::deferred, ec, child_page), __FILE__, __LINE__))
                  .otherwise(asio::deferred.values(monitor_shlock_type{}, ec, child_page));
            })
        | [ self_op=std::move(*this),
            path=std::move(path),
            intr, intr_version
          ](monitor_shlock_type intr_lock, std::error_code ec, page_variant child_page) mutable -> void {
            if (!ec) [[likely]] {
              if (intr->erased_ || intr->versions.page_split != intr_version) {
                ec = make_error_code(bplus_tree_errc::restart);
              } else {
                std::visit(
                    overload(
                        [&ec](std::error_code addr_selection_ec) {
                          if (!ec) throw std::logic_error("address selector returned an error without error state");
                          ec = addr_selection_ec;
                        },
                        [&ec, &child_page](const db_address& addr) {
                          const db_address& child_address = std::visit<const db_address&>(
                              [](const auto& child_ptr) -> const db_address& {
                                return child_ptr->address;
                              },
                              child_page);
                          if (addr != child_address) [[unlikely]]
                            ec = make_error_code(bplus_tree_errc::restart);
                        }),
                    std::invoke(self_op.address_selector_fn, intr));
              }
            }

            if (ec == make_error_code(bplus_tree_errc::restart)) {
              self_op.start(std::move(path));
            } else if (ec) {
              self_op.error_invoke_(std::move(ec));
            } else {
              std::visit(
                  [&](auto page) -> void {
                    self_op.search(std::move(path), std::move(page), std::move(intr_lock));
                  },
                  std::move(child_page));
            }
          };
      }

      auto error_invoke_(std::error_code ec) -> void {
        std::invoke(handler, ec, tree_path<TxAlloc>(nullptr, std::move(tx_alloc)), LeafLockType{});
      }

      TxAlloc tx_alloc;
      handler_type handler;
      address_selector_fn_type address_selector_fn;
    };

    op(std::move(address_selector_fn), std::move(handler)).start(std::move(path));
  }

  template<typename LeafLockType = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock, typename TxAlloc, typename CompletionToken>
  requires std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock> || std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock> || std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock>
  static auto tree_walk_(move_only_function<std::variant<db_address, std::error_code>(cycle_ptr::cycle_gptr<intr_type> intr)> address_selector_fn, tree_path<TxAlloc> path, CompletionToken&& token) {
    using address_selector_fn_type = move_only_function<std::variant<db_address, std::error_code>(cycle_ptr::cycle_gptr<intr_type> intr)>;
    using handler_type = move_only_function<void(std::error_code, tree_path<TxAlloc>, LeafLockType)>;

    return asio::async_initiate<CompletionToken, void(std::error_code, tree_path<TxAlloc>, LeafLockType)>(
        [](auto handler, address_selector_fn_type address_selector_fn, tree_path<TxAlloc> path) -> void {
          if constexpr(detail::handler_has_executor_v<decltype(handler)>) {
            auto ex = path.tree->get_executor();
            tree_walk_impl_<LeafLockType>(std::move(address_selector_fn), std::move(path), handler_type(completion_handler_fun(std::move(handler), std::move(ex))));
          } else {
            tree_walk_impl_<LeafLockType>(std::move(address_selector_fn), std::move(path), handler_type(std::move(handler)));
          }
        },
        token, std::move(address_selector_fn), std::move(path));
  }

  template<typename LeafLockType = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock, typename TxAlloc>
  requires std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock> || std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock> || std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock>
  auto find_leaf_page_for_insert_op_(tree_path<TxAlloc> path, std::span<const std::byte> key) {
    assert(path.tree == this->shared_from_this(this));
    return tree_walk_<LeafLockType>(
        [key](cycle_ptr::cycle_gptr<intr_type> intr) -> std::variant<db_address, std::error_code> {
          std::optional<db_address> opt_child_page_address = intr->key_find(key);
          if (opt_child_page_address.has_value()) {
            return std::move(opt_child_page_address).value();
          } else {
            std::clog << "bplus-tree: bad-tree, page has no child-pages\n";
            intr->maybe_log_dump();
            return make_error_code(bplus_tree_errc::bad_tree);
          }
        },
        std::move(path),
        asio::deferred);
  }

  template<typename TxAlloc>
  auto ensure_parent_op_(cycle_ptr::cycle_gptr<page_type> page, TxAlloc tx_alloc) {
    using monitor_exlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock;
    using monitor_uplock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock;
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;
    using tx_type = typename raw_db_type::template fdb_transaction<TxAlloc>;
    using tx_file_type = typename tx_type::file;
    using handler_type = move_only_function<void(std::error_code, cycle_ptr::cycle_gptr<intr_type>)>;

    struct op {
      op(cycle_ptr::cycle_gptr<bplus_tree> tree, cycle_ptr::cycle_gptr<page_type> page, TxAlloc tx_alloc, handler_type handler)
      : tx_alloc(std::move(tx_alloc)),
        page(std::move(page)),
        tree(std::move(tree)),
        handler(std::move(handler))
      {}

      auto operator()() -> void {
        auto tree = this->tree; // Make a copy, since we'll move *this.
        auto page = this->page; // Make a copy, since we'll move *this.
        page->async_parent_page(std::false_type(), asio::deferred)
        | [self_op=std::move(*this)](std::error_code ec, cycle_ptr::cycle_gptr<intr_type> parent, [[maybe_unused]] monitor_shlock_type parent_lock) mutable {
            if (ec || parent != nullptr)
              std::invoke(self_op.handler, std::move(ec), std::move(parent));
            else
              self_op.create_new_parent();
          };
      }

      private:
      auto create_new_parent() -> void {
        cycle_ptr::cycle_gptr<raw_db_type> raw_db = page->raw_db.lock();
        if (raw_db == nullptr) [[unlikely]] {
          error_invoke(make_error_code(db_errc::data_expired));
          return;
        }

        auto tree = this->tree; // Make a copy, since we'll move *this.
        auto page = this->page; // Make a copy, since we'll move *this.
        auto tx_alloc = this->tx_alloc; // Make a copy, since we'll move *this.
        page->page_lock.dispatch_shared(asio::deferred, __FILE__, __LINE__)
        | asio::deferred(
            [page](monitor_shlock_type page_shlock) {
              return page->async_compute_page_augments(asio::append(asio::deferred, page->versions, std::move(page_shlock)));
            })
        | asio::deferred(
            [tx_alloc, tree, page, raw_db](std::vector<std::byte> augment, bplus_tree_versions page_versions, monitor_shlock_type page_shlock) {
              page_shlock.reset(); // No longer needed.
              tx_type tx = raw_db->fdb_tx_begin(isolation::read_commited, tx_mode::read_write, tx_alloc);
              const auto augment_ptr = std::allocate_shared<std::vector<std::byte, typename std::allocator_traits<TxAlloc>::template rebind_alloc<std::byte>>>(
                  tx.get_allocator(),
                  augment.begin(), augment.end(), tx.get_allocator());
              return intr_type::async_new_page(
                  tx, tree->db_alloc_, page->spec, raw_db, nil_page,
                  page->address.offset, *augment_ptr,
                  asio::append(asio::deferred, augment_ptr, std::move(page_versions), tx));
            })
        | [self_op=std::move(*this)](std::error_code ec, cycle_ptr::cycle_gptr<intr_type> parent_page, [[maybe_unused]] auto augment_ptr, bplus_tree_versions page_versions, tx_type tx) mutable {
            if (ec) {
              self_op.error_invoke(ec);
              return;
            }

            self_op.verify_augment_and_install_page(std::move(tx), std::move(parent_page), std::move(page_versions));
          };
      }

      auto verify_augment_and_install_page(tx_type tx, cycle_ptr::cycle_gptr<intr_type> parent_page, bplus_tree_versions page_versions) -> void {
        auto tree = this->tree; // Make a copy, since we'll move *this.
        auto page = this->page; // Make a copy, since we'll move *this.
        tree->page_lock.dispatch_upgrade(asio::append(asio::deferred, std::move(parent_page), std::move(page_versions)), __FILE__, __LINE__)
        | asio::deferred(
            [page](monitor_uplock_type tree_uplock, cycle_ptr::cycle_gptr<intr_type> parent_page, bplus_tree_versions page_versions) {
              return page->page_lock.dispatch_upgrade(asio::append(asio::deferred, std::move(tree_uplock), std::move(parent_page), std::move(page_versions)), __FILE__, __LINE__);
            })
        | [tx, self_op=std::move(*this)](monitor_uplock_type page_uplock, monitor_uplock_type tree_uplock, cycle_ptr::cycle_gptr<intr_type> parent_page, bplus_tree_versions page_versions) mutable {
            if (self_op.page->erased_ || self_op.tree->erased_) [[unlikely]] {
              page_uplock.reset();
              tree_uplock.reset();
              self_op.error_invoke(make_error_code(db_errc::data_expired));
              return;
            }

            if (self_op.page->versions.augment != page_versions.augment) {
              page_uplock.reset();
              tree_uplock.reset();
              self_op.fix_augmentation(std::move(tx), std::move(parent_page));
              return;
            }

            if (self_op.page->parent_page_offset() != nil_page) {
              page_uplock.reset();
              tree_uplock.reset();
              std::invoke(self_op); // restart
              return;
            }

            if (self_op.tree->address.file != self_op.page->address.file) [[unlikely]] {
              page_uplock.reset();
              tree_uplock.reset();
              std::clog << "bplus-tree: bad-tree, tree and page are in different files (tree file: " << self_op.tree->address.file << ", page file: " << self_op.page->address.file << ")\n";
              self_op.page->maybe_log_dump();
              self_op.error_invoke(make_error_code(bplus_tree_errc::bad_tree));
              return;
            }

            if (self_op.tree->hdr_.root != self_op.page->address.offset) {
              // XXX maybe this should be bplus_tree_errc::bad_tree,
              // because:
              // - if this page had acquired a root, it would have shown above
              // - if this page was erased, it would have been caught above
              page_uplock.reset();
              tree_uplock.reset();
              std::invoke(self_op); // restart
              return;
            }

            tx.async_commit(
                completion_wrapper<void(std::error_code)>(
                    std::move(self_op),
                    [tree_uplock, page_uplock, parent_page](op self_op, std::error_code ec) {
                      if (ec) {
                        std::invoke(self_op.handler, ec, nullptr);
                        return;
                      }

                      tree_uplock.dispatch_exclusive(asio::deferred)
                      | asio::deferred(
                          [page_uplock](monitor_exlock_type tree_exlock) {
                            return page_uplock.dispatch_exclusive(asio::append(asio::deferred, std::move(tree_exlock)));
                          })
                      | asio::deferred(
                          [page=self_op.page, parent_page, tree=self_op.tree](monitor_exlock_type page_exlock, monitor_exlock_type tree_exlock) {
                            page->parent_offset = tree->hdr_.root = parent_page->address.offset;
                            page_exlock.reset();
                            tree_exlock.reset();
                            return asio::deferred.values(std::error_code{}, parent_page);
                          })
                      | std::move(self_op.handler);
                    }));
          };
      }

      auto fix_augmentation(tx_type tx, cycle_ptr::cycle_gptr<intr_type> parent_page) -> void {
        auto page = this->page; // Make a copy, since we'll move *this.
        page->page_lock.dispatch_shared(asio::deferred, __FILE__, __LINE__)
        | asio::deferred(
            [page](monitor_shlock_type page_shlock) {
              return page->async_compute_page_augments(asio::append(asio::deferred, page->versions, std::move(page_shlock)));
            })
        | asio::deferred(
            [parent_page, tx](std::vector<std::byte> child_augment, bplus_tree_versions page_versions, monitor_shlock_type page_shlock) mutable {
              page_shlock.reset(); // No longer needed.

              std::error_code ec;
              auto new_augment_span = parent_page->augment_span(0);
              if (new_augment_span.size() != child_augment.size())
                ec = make_error_code(xdr_errc::encoding_error);
              else
                std::copy(child_augment.begin(), child_augment.end(), new_augment_span.begin());
              auto file_ptr = std::allocate_shared<tx_file_type>(tx.get_allocator(), tx[parent_page->address.file]);

              return asio::deferred.when(!ec)
                  .then(
                      asio::async_write_at(
                          *file_ptr,
                          parent_page->address.offset + parent_page->augment_offset(0),
                          asio::buffer(new_augment_span),
                          asio::append(asio::deferred, page_versions, file_ptr)))
                  .otherwise(asio::deferred.values(ec, std::size_t(0), page_versions, file_ptr));
            })
        | [self_op=std::move(*this), tx, parent_page](std::error_code ec, [[maybe_unused]] std::size_t nbytes, bplus_tree_versions page_versions, [[maybe_unused]] auto file_ptr) mutable {
            if (ec)
              self_op.error_invoke(ec);
            else
              self_op.verify_augment_and_install_page(std::move(tx), std::move(parent_page), std::move(page_versions));
          };
      }

      auto error_invoke(std::error_code ec) -> void {
        std::invoke(handler, ec, nullptr);
      }

      TxAlloc tx_alloc;
      cycle_ptr::cycle_gptr<page_type> page;
      cycle_ptr::cycle_gptr<bplus_tree> tree;
      handler_type handler;
    };

    return asio::async_initiate<decltype(asio::deferred), void(std::error_code, cycle_ptr::cycle_gptr<intr_type>)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree> tree, cycle_ptr::cycle_gptr<page_type> page, TxAlloc tx_alloc) {
          if constexpr(handler_has_executor_v<decltype(handler)>)
            std::invoke(op(tree, std::move(page), std::move(tx_alloc), completion_handler_fun(std::move(handler), tree->get_executor())));
          else
            std::invoke(op(std::move(tree), std::move(page), std::move(tx_alloc), std::move(handler)));
        },
        asio::deferred, this->shared_from_this(this), std::move(page), std::move(tx_alloc));
  }

  template<typename PageType, typename TxAlloc>
  requires (std::is_same_v<PageType, intr_type> || std::is_same_v<PageType, leaf_type>)
  auto split_page_impl_(cycle_ptr::cycle_gptr<PageType> page, bplus_tree_versions::type page_split, TxAlloc tx_alloc, move_only_function<void(std::error_code)> handler) -> void {
    using monitor_exlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock;
    using monitor_uplock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock;
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;
    using tx_type = typename raw_db_type::template fdb_transaction<TxAlloc>;
    static constexpr std::size_t num_page_locks_needed = (std::is_same_v<PageType, intr_type> ? 3u : 4u);

    struct element_rebalancer_for_leaf {
      struct rebalance_item {
        using allocator_type = typename std::allocator_traits<TxAlloc>::template rebind_alloc<std::byte>;

        explicit rebalance_item(allocator_type tx_alloc)
        : elem(),
          which_page(0),
          new_index(0),
          kv_bytes(std::move(tx_alloc))
        {}

        rebalance_item(const rebalance_item& y, allocator_type tx_alloc)
        : elem(y.elem),
          uplock(y.uplock),
          exlock(y.exlock),
          which_page(y.which_page),
          new_index(y.new_index),
          use_type(y.use_type),
          kv_bytes(y.kv_bytes, std::move(tx_alloc))
        {}

        rebalance_item(cycle_ptr::cycle_gptr<typename leaf_type::element> elem, allocator_type tx_alloc)
        : elem(std::move(elem)),
          which_page(0),
          new_index(0),
          kv_bytes(std::move(tx_alloc))
        {
          if (which_page != this->which_page)
            throw std::out_of_range("bug: wrong page selection during split");
        }

        auto get_allocator() const -> allocator_type {
          return kv_bytes.get_allocator();
        }

        cycle_ptr::cycle_gptr<typename leaf_type::element> elem;
        monitor_uplock_type uplock;
        monitor_exlock_type exlock;
        std::uint32_t which_page : 1;
        std::size_t new_index : std::numeric_limits<std::size_t>::digits - 9u;
        bplus_tree_leaf_use_element use_type;
        std::vector<std::byte, allocator_type> kv_bytes;
      };

      using rebalance_vector = std::vector<rebalance_item, std::scoped_allocator_adaptor<typename std::allocator_traits<TxAlloc>::template rebind_alloc<rebalance_item>>>;

      protected:
      explicit element_rebalancer_for_leaf(const TxAlloc& tx_alloc)
      : rebalance(tx_alloc)
      {}

      ~element_rebalancer_for_leaf() = default;

      public:
      auto populate_rebalance(const leaf_type& leaf) -> void {
        for (cycle_ptr::cycle_gptr<typename leaf_type::element> element_ptr : leaf.elements())
          rebalance.emplace_back(std::move(element_ptr));
      }

      protected:
      auto compute_rebalance(const leaf_type& leaf, std::size_t shift) -> void {
        assert(rebalance.size() == leaf.elements().size());
        for (const auto& rebalance_item : rebalance) assert(rebalance_item.which_page == 0);

        if (rebalance.size() < shift) throw std::out_of_range("bug: shifting too many elements during split");
        const auto keep_in_page_0 = rebalance.size() - shift;
        const auto elements_per_leaf = leaf.spec->elements_per_leaf;

        std::for_each(
            std::next(rebalance.begin(), keep_in_page_0), rebalance.end(),
            [](auto& rebalance_item) {
              rebalance_item.which_page = 1;
            });
        redistribute(elements_per_leaf, rebalance.begin(), std::next(rebalance.begin(), keep_in_page_0));
        redistribute(elements_per_leaf, std::next(rebalance.begin(), keep_in_page_0), rebalance.end());
      }

      private:
      static auto redistribute(std::size_t elements_per_leaf, typename rebalance_vector::iterator b, typename rebalance_vector::iterator e) -> void {
        assert(e - b >= 0);
        const std::size_t count = e - b;
        if (count == 0) return;
        const std::size_t between = elements_per_leaf / count;
        assert(between > 0);
        const std::size_t offset0 = (count == elements_per_leaf ? 0 : (between + 1u) / 2u);

        std::size_t offset = offset0;
        std::for_each(b, e,
            [&offset, between, elements_per_leaf](rebalance_item& item) {
              assert(offset < elements_per_leaf);

              item.new_index = offset;
              if (item.new_index != offset) // Verify we didn't get a truncation.
                throw std::out_of_range("bug: wrong index selection during split");
              offset += between;
            });
      }

      public:
      rebalance_vector rebalance;
    };

    struct element_rebalancer_for_intr {
      struct reparent_item {
        reparent_item() = default;

        explicit reparent_item(cycle_ptr::cycle_gptr<page_type> page)
        : page(std::move(page))
        {}

        cycle_ptr::cycle_gptr<page_type> page;
        monitor_uplock_type uplock;
        monitor_exlock_type exlock;
      };

      using reparent_vector = std::vector<reparent_item, typename std::allocator_traits<TxAlloc>::template rebind_alloc<reparent_item>>;

      protected:
      explicit element_rebalancer_for_intr(const TxAlloc& tx_alloc)
      : reparent(tx_alloc)
      {}

      ~element_rebalancer_for_intr() = default;

      auto compute_rebalance([[maybe_unused]] const intr_type& intr, [[maybe_unused]] std::size_t shift) -> void {
        assert(false); // Should never be called.
      }

      public:
      auto load_child_pages(cycle_ptr::cycle_gptr<const intr_type> parent, std::size_t shift) {
        return asio::async_initiate<decltype(asio::deferred), void(std::error_code)>(
            [](auto handler, element_rebalancer_for_intr* self, cycle_ptr::cycle_gptr<const intr_type> parent, std::size_t shift) {
              using handler_type = decltype(handler);

              struct op
              : public std::enable_shared_from_this<op>
              {
                op(element_rebalancer_for_intr* self,
                    cycle_ptr::cycle_gptr<const intr_type> parent,
                    std::size_t shift,
                    handler_type handler)
                : self(std::move(self)),
                  parent(std::move(parent)),
                  shift(std::move(shift)),
                  handler(std::move(handler))
                {}

                auto operator()() -> void {
                  auto raw_db = parent->raw_db.lock();
                  if (raw_db == nullptr) {
                    std::invoke(handler, make_error_code(db_errc::data_expired));
                    return;
                  }

                  while (self->reparent.size() < shift) {
                    page_type::async_load_op(
                        parent->spec, raw_db,
                        db_address(parent->address.file, parent->element(parent->index_size() - shift + self->reparent.size())),
                        []() {
                          // We hold the lock on parent, so there's no way this address can be invalidated.
                          return asio::deferred.values(std::error_code());
                        })
                    | asio::deferred(
                        [](std::error_code ec, std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>> page) {
                          return asio::deferred.values(
                              ec,
                              std::visit(
                                  [](cycle_ptr::cycle_gptr<page_type> page_ptr) -> cycle_ptr::cycle_gptr<page_type> {
                                    return page_ptr;
                                  },
                                  std::move(page)));
                        })
                    | [self_op=this->shared_from_this()](std::error_code ec, cycle_ptr::cycle_gptr<page_type> page) -> void {
                        if (ec == make_error_code(bplus_tree_errc::restart)) {
                          std::invoke(*self_op);
                        } else if (ec) {
                          std::invoke(self_op->handler, ec);
                        } else {
                          self_op->self->reparent.emplace_back(std::move(page));
                          std::invoke(*self_op);
                        }
                      };
                    return; // callback will restart the loop.
                  }

                  lock_for_upgrade();
                }

                private:
                auto lock_for_upgrade(std::size_t i = 0) -> void {
                  for (/* skip */; i < shift; ++i) {
                    assert(!self->reparent[i].uplock.is_locked());

                    auto opt_uplock = self->reparent[i].page->page_lock.try_upgrade(__FILE__, __LINE__);
                    if (opt_uplock.has_value()) {
                      self->reparent[i].uplock = std::move(opt_uplock).value();
                    } else {
                      self->reparent[i].page->page_lock.async_upgrade(
                          [self_op=this->shared_from_this(), i](monitor_uplock_type uplock) -> void {
                            self_op->self->reparent[i].uplock = std::move(uplock);
                            self_op->lock_for_upgrade(i + 1u);
                          },
                          __FILE__, __LINE__);
                    }
                  }

                  std::invoke(handler, std::error_code());
                }

                element_rebalancer_for_intr*const self;
                const cycle_ptr::cycle_gptr<const intr_type> parent;
                const std::size_t shift;
                handler_type handler;
              };

              std::invoke(*std::allocate_shared<op>(self->reparent.get_allocator(), self, std::move(parent), std::move(shift), std::move(handler)));
            },
            asio::deferred, this, std::move(parent), std::move(shift));
      }

      auto exlock_reparent() {
        return asio::async_initiate<decltype(asio::deferred), void()>(
            [](auto handler, element_rebalancer_for_intr* self) {
              using handler_type = decltype(handler);

              struct op
              : public std::enable_shared_from_this<op>
              {
                op(element_rebalancer_for_intr* self, handler_type handler)
                : self(std::move(self)),
                  handler(std::move(handler))
                {}

                auto operator()() -> void {
                  (*this)(0);
                }

                auto operator()(std::size_t i) -> void {
                  for (/* skip */; i < self->reparent.size(); ++i) {
                    auto opt_exlock = std::move(self->reparent[i].uplock).try_exclusive();
                    if (opt_exlock.has_value()) {
                      self->reparent[i].exlock = std::move(opt_exlock).value();
                    } else {
                      std::move(self->reparent[i].uplock).async_exclusive(
                          [self_op=this->shared_from_this(), i](monitor_exlock_type exlock) -> void {
                            self_op->self->reparent[i].exlock = std::move(exlock);
                            std::invoke(*self_op, i + 1u);
                          });
                      return; // callback will resume the loop
                    }
                  }

                  std::invoke(handler);
                }

                private:
                element_rebalancer_for_intr*const self;
                handler_type handler;
              };

              std::invoke(*std::allocate_shared<op>(self->reparent.get_allocator(), self, std::move(handler)));
            },
            asio::deferred, this);
      }

      reparent_vector reparent;
    };

    using element_rebalancer = std::conditional_t<std::is_same_v<PageType, intr_type>, element_rebalancer_for_intr, element_rebalancer_for_leaf>;

    struct page_selection
    : element_rebalancer
    {
      page_selection(cycle_ptr::cycle_gptr<intr_type> parent, std::initializer_list<cycle_ptr::cycle_gptr<PageType>> init_level_pages, const TxAlloc& tx_alloc)
      : element_rebalancer(tx_alloc),
        parent(std::move(parent))
      {
        assert(init_level_pages.size() == 2 || init_level_pages.size() == level_pages.size());
        std::copy(
            std::make_move_iterator(init_level_pages.begin()),
            std::make_move_iterator(init_level_pages.end()),
            level_pages.begin());
      }

      page_selection(const page_selection&) = delete;
      page_selection(page_selection&&) = delete;

      auto reset() -> void {
        for (auto& l : exlocks) l.reset();
        for (auto& l : uplocks) l.reset();
        for (auto& p : level_pages) p.reset();
        parent.reset();
      }

      auto compute_rebalance(std::size_t shift) -> void {
        assert(level_pages[0] != nullptr);
        this->element_rebalancer::compute_rebalance(*level_pages[0], shift);
      }

      cycle_ptr::cycle_gptr<intr_type> parent;
      std::array<cycle_ptr::cycle_gptr<PageType>, num_page_locks_needed - 1u> level_pages;
      std::array<monitor_uplock_type, num_page_locks_needed> uplocks;
      std::array<monitor_exlock_type, num_page_locks_needed> exlocks;
    };

    struct op {
      op(cycle_ptr::cycle_gptr<bplus_tree> tree, cycle_ptr::cycle_gptr<PageType> page, bplus_tree_versions::type page_split, TxAlloc tx_alloc, move_only_function<void(std::error_code)> handler) noexcept
      : tx_alloc(std::move(tx_alloc)),
        tree(std::move(tree)),
        page(std::move(page)),
        page_split(std::move(page_split)),
        handler(std::move(handler))
      {}

      auto operator()() -> void {
        auto tree = this->tree; // Make a copy, since we'll move this.
        auto page = this->page; // Make a copy, since we'll move this.
        auto tx_alloc = this->tx_alloc; // Make a copy, since we'll move this.

        tree->ensure_parent_op_(page, tx_alloc)
        | [self_op=std::move(*this)](std::error_code ec, cycle_ptr::cycle_gptr<intr_type> parent) mutable {
            cycle_ptr::cycle_gptr<raw_db_type> raw_db;
            if (!ec) {
              raw_db = self_op.tree->raw_db.lock();
              if (raw_db == nullptr) [[unlikely]] ec = make_error_code(db_errc::data_expired);
            }

            if (ec)
              self_op.error_invoke(ec);
            else
              self_op.create_sibling(std::move(raw_db), std::move(parent));
          };
      }

      private:
      auto create_sibling(cycle_ptr::cycle_gptr<raw_db_type> raw_db, cycle_ptr::cycle_gptr<intr_type> parent) -> void {
        auto tree = this->tree; // Make a copy, since we'll move this.
        tx_type tx = raw_db->fdb_tx_begin(isolation::read_commited, tx_mode::read_write, tx_alloc);

        if constexpr(std::is_same_v<intr_type, PageType>) {
          intr_type::async_new_page(
              tx, tree->db_alloc_, tree->spec, raw_db, parent->address.offset,
              nil_page, {},
              [ self_op=std::move(*this),
                tx, parent
              ](std::error_code ec, cycle_ptr::cycle_gptr<intr_type> sibling_page) mutable {
                if (ec)
                  self_op.error_invoke(ec);
                else
                  self_op.have_sibling(tx, parent, std::move(sibling_page));
              });
        } else {
          static_assert(std::is_same_v<leaf_type, PageType>);
          leaf_type::async_new_page(
              tx, tree->db_alloc_, tree->spec, raw_db, parent->address.offset,
              nil_page, nil_page, false,
              [ self_op=std::move(*this),
                tx, parent
              ](std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> sibling_page) mutable {
                if (ec)
                  self_op.error_invoke(ec);
                else
                  self_op.have_sibling(tx, parent, std::move(sibling_page));
              });
        }
      }

      static auto make_ps(cycle_ptr::cycle_gptr<intr_type> parent, cycle_ptr::cycle_gptr<PageType> page, cycle_ptr::cycle_gptr<PageType> sibling_page, TxAlloc tx_alloc) {
        auto ps = std::allocate_shared<page_selection>(tx_alloc,
            parent,
            std::initializer_list<cycle_ptr::cycle_gptr<PageType>>{page, std::move(sibling_page)},
            tx_alloc);

        if constexpr(std::is_same_v<PageType, intr_type>) {
          return asio::deferred.values(std::error_code(), std::move(ps));
        } else {
          return page->page_lock.dispatch_shared(asio::deferred, __FILE__, __LINE__)
          | asio::deferred(
              [page](monitor_shlock_type lock) {
                db_address next_page_address = page->next_page_address();
                lock.reset();

                std::error_code ec;
                auto raw_db = page->raw_db.lock();
                if (raw_db == nullptr) [[unlikely]] ec = make_error_code(db_errc::data_expired);

                return asio::deferred.when(ec || next_page_address.offset == nil_page)
                    .then(asio::deferred.values(ec, cycle_ptr::cycle_gptr<leaf_type>()))
                    .otherwise(
                        page_type::async_load_op(
                            page->spec, raw_db, next_page_address,
                            [page, next_page_address]() {
                              return page->page_lock.dispatch_shared(asio::deferred, __FILE__, __LINE__)
                              | asio::deferred(
                                  [=]([[maybe_unused]] auto lock) {
                                    std::error_code ec;
                                    if (page->erased_ || next_page_address != page->next_page_address())
                                      ec = make_error_code(bplus_tree_errc::restart);
                                    return asio::deferred.values(ec);
                                  });
                            })
                        | page_type::template variant_to_pointer_op<leaf_type>());
              })
          | asio::deferred(
              [ps](std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> next_page) mutable {
                ps->level_pages[2] = std::move(next_page);
                return asio::deferred.values(ec, std::move(ps));
              });
        }
      }

      static auto lock_ps_for_upgrade(std::shared_ptr<page_selection> ps, std::size_t start_idx = 0) {
        struct uplock_op {
          explicit uplock_op(std::shared_ptr<page_selection> ps)
          : ps(std::move(ps))
          {}

          auto operator()(move_only_function<void(std::shared_ptr<page_selection>)> handler, std::size_t idx) -> void {
            do_page_lock(std::move(handler), idx);
          }

          private:
          auto do_page_lock(move_only_function<void(std::shared_ptr<page_selection>)> handler, std::size_t idx) -> void {
            assert(idx <= ps->uplocks.size());
            for (/*skip*/; idx < num_page_locks_needed; ++idx) {
              const bplus_tree_page<RawDbType>* next_page_to_lock; // Can be nullptr.
              switch (idx) {
                default:
                  next_page_to_lock = ps->level_pages[idx - 1u].get();
                  break;
                case 0u:
                  next_page_to_lock = ps->parent.get();
                  break;
              }

              if (next_page_to_lock != nullptr) {
                auto opt_lock = next_page_to_lock->page_lock.try_upgrade(__FILE__, __LINE__);
                if (opt_lock.has_value()) {
                  ps->uplocks[idx] = std::move(opt_lock).value();
                } else {
                  next_page_to_lock->page_lock.async_upgrade(
                      completion_wrapper<void(monitor_uplock_type)>(
                          std::move(handler),
                          [self_op=std::move(*this), idx](auto handler, monitor_uplock_type lock) mutable {
                            self_op.ps->uplocks[idx] = std::move(lock);
                            self_op.do_page_lock(std::move(handler), idx + 1u);
                          }),
                      __FILE__, __LINE__);
                  return; // Callback will restart this loop.
                }
              }
            }

            if constexpr(std::is_same_v<PageType, leaf_type>) ps->populate_rebalance(*ps->level_pages[0]);
            do_elem_lock(std::move(handler));
          }

          auto do_elem_lock(move_only_function<void(std::shared_ptr<page_selection>)> handler, [[maybe_unused]] std::size_t idx = 0) -> void {
            if constexpr(std::is_same_v<PageType, intr_type>) {
              invoke_complete(std::move(handler));
            } else {
              assert(idx <= ps->rebalance.size());
              for (/*skip*/; idx < ps->rebalance.size(); ++idx) {
                const typename leaf_type::element& elem_to_lock = *ps->rebalance[idx].elem;

                auto opt_lock = elem_to_lock.element_lock.try_upgrade(__FILE__, __LINE__);
                if (opt_lock.has_value()) {
                  ps->rebalance[idx].uplock = std::move(opt_lock).value();
                } else {
                  // We use `async_upgrade` (as opposed to `dispatch_upgrade`),
                  // because there could be thousands of page elements,
                  // and dispatch recursion could cause the stack to throw huge.
                  elem_to_lock.element_lock.async_upgrade(
                      completion_wrapper<void(monitor_uplock_type)>(
                          std::move(handler),
                          [self_op=*this, idx](auto handler, monitor_uplock_type lock) mutable {
                            self_op.ps->rebalance[idx].uplock = std::move(lock);
                            self_op.do_elem_lock(std::move(handler), idx + 1u);
                          }),
                      __FILE__, __LINE__);
                  return; // Callback will restart this loop.
                }
              }
              invoke_complete(std::move(handler));
            }
          }

          auto invoke_complete(move_only_function<void(std::shared_ptr<page_selection>)> handler) -> void {
            std::invoke(handler, ps);
          }

          std::shared_ptr<page_selection> ps;
        };

        for (std::size_t i = start_idx; i < ps->uplocks.size(); ++i)
          assert(!ps->uplocks[i].is_locked());
        for (const auto& locks : ps->exlocks)
          assert(!locks.is_locked());

        return asio::async_initiate<decltype(asio::deferred), void(std::shared_ptr<page_selection>)>(
            [](auto handler, std::shared_ptr<page_selection> ps, std::size_t start_idx) -> void {
              std::invoke(uplock_op(std::move(ps)), move_only_function<void(std::shared_ptr<page_selection>)>(std::move(handler)), std::move(start_idx));
            },
            asio::deferred, std::move(ps), std::move(start_idx));
      }

      static auto lock_ps_for_exclusive(std::shared_ptr<page_selection> ps) {
        struct exlock_op {
          explicit exlock_op(std::shared_ptr<page_selection> ps)
          : ps(std::move(ps))
          {}

          auto operator()(move_only_function<void(std::shared_ptr<page_selection>)> handler) -> void {
            do_page_lock(std::move(handler), 0);
          }

          private:
          auto do_page_lock(move_only_function<void(std::shared_ptr<page_selection>)> handler, std::size_t idx) -> void {
            assert(idx <= ps->uplocks.size());

            for (std::size_t i = 0; i < ps->uplocks.size(); ++i) {
              if (i == 0 && ps->parent == nullptr) continue;
              if (i != 0 && ps->level_pages[i - 1u] == nullptr) continue;

              if (i < idx)
                assert(ps->exlocks[i].is_locked());
              else
                assert(ps->uplocks[i].is_locked());
            }

            for (/*skip*/; idx < num_page_locks_needed; ++idx) {
              if (ps->uplocks[idx].is_locked()) {
                monitor_uplock_type lck = std::move(ps->uplocks[idx]);
                auto opt_lock = lck.try_exclusive();
                if (opt_lock.has_value()) {
                  ps->exlocks[idx] = std::move(opt_lock).value();
                } else {
                  std::move(lck).dispatch_exclusive(
                      completion_wrapper<void(monitor_exlock_type)>(
                          std::move(handler),
                          [self_op=std::move(*this), idx](auto handler, monitor_exlock_type lock) mutable {
                            self_op.ps->exlocks[idx] = std::move(lock);
                            self_op.do_page_lock(std::move(handler), idx + 1u);
                          }));
                  return; // Callback will restart this loop.
                }
              }
            }
            do_elem_lock(std::move(handler), 0);
          }

          auto do_elem_lock(move_only_function<void(std::shared_ptr<page_selection>)> handler, std::size_t idx) -> void {
            if constexpr(std::is_same_v<PageType, intr_type>) {
              invoke_complete(std::move(handler));
            } else {
              assert(idx <= ps->rebalance.size());

              for (/*skip*/; idx < ps->rebalance.size(); ++idx) {
                monitor_uplock_type uplock = std::move(ps->rebalance[idx].uplock);
                auto opt_lock = std::move(uplock).try_exclusive();
                if (opt_lock.has_value()) {
                  ps->rebalance[idx].exlock = std::move(opt_lock).value();
                } else {
                  // We use `async_exclusive` (as opposed to `dispatch_upgrade`),
                  // because there could be thousands of page elements,
                  // and dispatch recursion could cause the stack to throw huge.
                  std::move(uplock).async_exclusive(
                      completion_wrapper<void(monitor_exlock_type)>(
                          std::move(handler),
                          [self_op=*this, idx](auto handler, monitor_exlock_type lock) mutable {
                            self_op.ps->rebalance[idx].exlock = std::move(lock);
                            self_op.do_elem_lock(std::move(handler), idx + 1u);
                          }));
                  return; // Callback will restart this loop.
                }
              }
              invoke_complete(std::move(handler));
            }
          }

          auto invoke_complete(move_only_function<void(std::shared_ptr<page_selection>)> handler) -> void {
            std::invoke(handler, std::move(ps));
          }

          std::shared_ptr<page_selection> ps;
        };

        for (std::size_t i = 0; i < ps->uplocks.size(); ++i) {
          if (i == 0 && ps->parent == nullptr) continue;
          if (i != 0 && ps->level_pages[i - 1u] == nullptr) continue;
          assert(ps->uplocks[i].is_locked());
        }
        for (const auto& locks : ps->exlocks)
          assert(!locks.is_locked());

        auto deferred_op = asio::async_initiate<decltype(asio::deferred), void(std::shared_ptr<page_selection>)>(
            [](auto handler, std::shared_ptr<page_selection> ps) -> void {
              std::invoke(exlock_op(std::move(ps)), move_only_function<void(std::shared_ptr<page_selection>)>(std::move(handler)));
            },
            asio::deferred, std::move(ps));

        if constexpr(std::is_same_v<PageType, intr_type>) {
          return std::move(deferred_op)
          | asio::deferred(
              [](std::shared_ptr<page_selection> ps) {
                return ps->exlock_reparent()
                | asio::deferred(
                    [ps]() {
                      return asio::deferred.values(ps);
                    });
              });
        } else {
          return std::move(deferred_op);
        }
      }

      auto have_sibling(tx_type tx, cycle_ptr::cycle_gptr<intr_type> parent, cycle_ptr::cycle_gptr<PageType> sibling_page) {
        auto page = this->page; // Make a copy, since we'll move *this.
        auto tx_alloc = this->tx_alloc; // Make a copy, since we'll move *this.
        make_ps(std::move(parent), std::move(page), std::move(sibling_page), std::move(tx_alloc))
        | [self_op=std::move(*this), tx=std::move(tx)](std::error_code ec, std::shared_ptr<page_selection> ps) mutable -> void {
            if (ec)
              self_op.error_invoke(ec);
            else
              self_op.lock_parent_and_ensure_enough_space(tx, std::move(ps));
          };
      }

      auto lock_parent_and_ensure_enough_space(tx_type tx, std::shared_ptr<page_selection> ps) -> void {
        auto parent = ps->parent; // Make a copy, so we can move ps.
        parent->page_lock.dispatch_upgrade(
            [self_op=std::move(*this), ps=std::move(ps), tx=std::move(tx)](monitor_uplock_type parent_uplock) mutable -> void {
              ps->uplocks[0] = std::move(parent_uplock);

              if (ps->parent->erased_) {
                ps->reset();
                std::invoke(self_op);
                return;
              }

              if (ps->parent->index_size() == ps->parent->spec->child_pages_per_intr) {
                // Parent is full, and cannot accept a new page.
                // So split it.
                auto tree = self_op.tree; // Make a copy, since we'll move self_op.
                auto tx_alloc = self_op.tx_alloc; // Make a copy, since we'll move self_op.
                tree->split_page_impl_(ps->parent, ps->parent->versions.page_split, tx_alloc,
                    completion_wrapper<void(std::error_code)>(
                        std::move(self_op),
                        [](op self_op, std::error_code ec) {
                          if (!ec)
                            std::invoke(self_op); // restart
                          else if (ec == make_error_code(db_errc::data_expired)) // data_expired could happen if the parent page was cleaned up
                            std::invoke(self_op); // restart
                          else
                            self_op.error_invoke(ec);
                        }));
                return;
              }

              self_op.continue_with_locked_parent(std::move(tx), std::move(ps));
            },
            __FILE__, __LINE__);
      }

      auto continue_with_locked_parent([[maybe_unused]] tx_type tx, std::shared_ptr<page_selection> ps) -> void {
        assert(ps->uplocks[0].is_locked());
        lock_ps_for_upgrade(std::move(ps), 1)
        | [self_op=std::move(*this), tx=std::move(tx)](std::shared_ptr<page_selection> ps) mutable -> void {
            if (self_op.page->versions.page_split != self_op.page_split) {
              // The job of this function is to ensure the page gets split,
              // and another thread has just done so for us.
              // So we'll just say we're done.
              self_op.done_invoke();
              return;
            }

            if constexpr(std::is_same_v<PageType, leaf_type>) {
              std::for_each(ps->rebalance.begin(), ps->rebalance.end(),
                  [ps](auto& rebalance_item) {
                    const std::span<const std::byte> elem_span = ps->level_pages[0]->element_span(rebalance_item.elem->index);
                    rebalance_item.kv_bytes.assign(elem_span.begin(), elem_span.end());
                    rebalance_item.use_type = rebalance_item.elem->type();
                  });
            }

            self_op.decide_shift_and_lock_shifted_children(std::move(tx), std::move(ps));
          };
      }

      auto decide_shift_and_lock_shifted_children(tx_type tx, std::shared_ptr<page_selection> ps) -> void {
        auto get_shift = [this]() -> std::uint32_t {
          if constexpr(std::is_same_v<PageType, intr_type>)
            return this->page->index_size() / 2u;
          else
            return this->page->elements().size() / 2u;
        };
        const std::uint32_t shift = get_shift();

        if constexpr(std::is_same_v<PageType, intr_type>) {
          auto page = this->page; // Make a copy, since we'll move *this.
          ps->load_child_pages(page, shift)
          | [self_op=std::move(*this), tx, ps, shift](std::error_code ec) mutable {
              if (ec)
                self_op.error_invoke(ec);
              else
                self_op.update_disk_representation(tx, ps, shift);
            };
        } else {
          update_disk_representation(std::move(tx), std::move(ps), std::move(shift));
        }
      }

      auto update_disk_representation(tx_type tx, std::shared_ptr<page_selection> ps, std::uint32_t shift) -> void {
        assert(ps->uplocks[0].is_locked());
        assert(ps->uplocks[1].is_locked());
        assert(ps->uplocks[2].is_locked());

        auto page = this->page; // Make a copy, since we'll move *this.
        auto tree = this->tree; // Make a copy, since we'll move *this.
        [[maybe_unused]] auto tx_alloc = this->tx_alloc; // Make a copy, since we'll move *this.
        if (shift == 0u) {
          // Not enough elements to shift.
          error_invoke(make_error_code(bplus_tree_errc::page_too_small_for_split));
          return;
        }

        auto barrier = make_completion_barrier(
            [self_op=std::move(*this), tx, ps, shift](std::error_code ec) mutable -> void {
              if (ec)
                self_op.error_invoke(ec);
              else
                self_op.commit_to_disk(tx, std::move(ps), shift);
            },
            tree->get_executor());

        // Find index in parent page.
        const std::optional<std::size_t> index_in_parent = ps->parent->find_index(ps->level_pages[0]->address);
        if (!index_in_parent.has_value()) [[unlikely]] {
          std::clog << "bplus-tree: bad-tree, page not found in parent\n";
          ps->parent->maybe_log_dump("parent");
          ps->level_pages[0]->maybe_log_dump("ps->level_pages[0]");
          std::invoke(barrier, make_error_code(bplus_tree_errc::bad_tree));
          return;
        }
        const std::size_t new_sibling_index_in_parent = index_in_parent.value() + 1u;

        if constexpr(std::is_same_v<PageType, intr_type>) {
          // Truncate old page.
          ps->level_pages[0]->async_set_hdr_diskonly_op(tx, {.size=static_cast<std::uint32_t>(page->index_size()-shift)}, ++barrier);

          // Copy old page elements to new page, and set the size.
          const std::size_t copy_begin = page->index_size() - shift;
          const std::size_t copy_end = page->index_size();
          ps->level_pages[1]->async_set_hdr_diskonly_op(tx, {.size=shift}, ++barrier);
          ps->level_pages[1]->async_set_elements_diskonly_op(tx, 0, page->element_multispan(copy_begin, copy_end), ++barrier);
          ps->level_pages[1]->async_set_augments_diskonly_op(tx, 0, page->augment_multispan(copy_begin, copy_end), ++barrier);
          ps->level_pages[1]->async_set_keys_diskonly_op(tx, 0, page->key_multispan(copy_begin, copy_end - 1u), ++barrier);

          for (const auto& child_page : ps->reparent)
            child_page.page->async_set_parent_diskonly_op(tx, ps->level_pages[1]->address.offset, ++barrier);
        } else {
          static_assert(std::is_same_v<PageType, leaf_type>);

          ps->compute_rebalance(shift);
          std::for_each(
              ps->rebalance.begin(), ps->rebalance.end(),
              [&tx, &barrier, ps](auto& rebalance_item) {
                assert(rebalance_item.kv_bytes.size() == ps->level_pages[0]->spec->element.key.padded_bytes() + ps->level_pages[0]->spec->element.value.padded_bytes());
                ps->level_pages[rebalance_item.which_page]->async_set_use_list_diskonly_op(tx, rebalance_item.new_index, rebalance_item.elem->type(), ++barrier);
                ps->level_pages[rebalance_item.which_page]->async_set_elements_diskonly_op(tx, rebalance_item.new_index, std::span<const std::byte>(rebalance_item.kv_bytes), ++barrier);
              });

          { // Use set-difference to decide which elements are to be marked `unused`.
            auto all_old_indices = std::vector<std::size_t, typename std::allocator_traits<TxAlloc>::template rebind_alloc<std::size_t>>(tx_alloc);
            std::transform(ps->rebalance.begin(), ps->rebalance.end(), std::back_inserter(all_old_indices),
                [](const auto& rebalance_item) {
                  return rebalance_item.elem->index;
                });
            auto new_indices_on_page_0 = std::vector<std::size_t, typename std::allocator_traits<TxAlloc>::template rebind_alloc<std::size_t>>(tx_alloc);
            std::transform(ps->rebalance.begin(), ps->rebalance.end(), std::back_inserter(new_indices_on_page_0),
                [](const auto& rebalance_item) {
                  return rebalance_item.new_index;
                });
            auto elems_to_clear = std::vector<std::size_t, typename std::allocator_traits<TxAlloc>::template rebind_alloc<std::size_t>>(tx_alloc);
            std::set_difference(
                all_old_indices.begin(), all_old_indices.end(),
                new_indices_on_page_0.begin(), new_indices_on_page_0.end(),
                std::back_inserter(elems_to_clear));
            std::for_each(
                elems_to_clear.begin(), elems_to_clear.end(),
                [&tx, &barrier, ps](std::size_t index) {
                  ps->level_pages[0]->async_set_use_list_diskonly_op(tx, index, bplus_tree_leaf_use_element::unused, ++barrier);
                });
          }

          { // Update predecessor/successor references.
            std::array<std::uint64_t, 3> offsets;
            offsets[0] = ps->level_pages[0]->address.offset;
            offsets[1] = ps->level_pages[1]->address.offset;
            offsets[2] = (ps->level_pages[2] != nullptr
                ? ps->level_pages[2]->address.offset
                : nil_page);
            std::array<bplus_tree_leaf_header, 3> h;
            h[0] = ps->level_pages[0]->hdr_;
            h[1] = ps->level_pages[1]->hdr_;
            if (ps->level_pages[2] != nullptr) h[2] = ps->level_pages[2]->hdr_;
            h[0].successor_page = offsets[1];
            h[1].predecessor_page = offsets[0];
            h[1].successor_page = offsets[2];
            h[2].predecessor_page = offsets[1];
            ps->level_pages[0]->async_set_hdr_diskonly_op(tx, h[0], ++barrier);
            ps->level_pages[1]->async_set_hdr_diskonly_op(tx, h[1], ++barrier);
            if (ps->level_pages[2] != nullptr) ps->level_pages[2]->async_set_hdr_diskonly_op(tx, h[2], ++barrier);
          }
        }

        // Update parent page: shift existing elements.
        ps->parent->async_set_hdr_diskonly_op(tx, {.size=static_cast<std::uint32_t>(ps->parent->index_size()+1u)}, ++barrier);
        if (new_sibling_index_in_parent < ps->parent->index_size()) {
          ps->parent->async_set_elements_diskonly_op(tx, new_sibling_index_in_parent + 1u, ps->parent->element_multispan(new_sibling_index_in_parent, ps->parent->index_size()), ++barrier);
          ps->parent->async_set_augments_diskonly_op(tx, new_sibling_index_in_parent + 1u, ps->parent->augment_multispan(new_sibling_index_in_parent, ps->parent->index_size()), ++barrier);
          ps->parent->async_set_keys_diskonly_op(tx, new_sibling_index_in_parent, ps->parent->key_multispan(new_sibling_index_in_parent - 1u, ps->parent->index_size() - 1u), ++barrier);
        }

        // Update parent page: install new page.
        if constexpr(boost::endian::order::native == boost::endian::order::big) {
          // No byte order conversion needed, so we write things directly.
          ps->parent->async_set_elements_diskonly_op(tx, new_sibling_index_in_parent, std::as_bytes(std::span<const std::uint64_t, 1>(&ps->level_pages[1]->address.offset, 1)), ++barrier);
        } else {
          // We'll need a temporary buffer, to hold the endian-reversed type.
          std::shared_ptr<std::uint64_t> new_sibling_offset_be = std::allocate_shared<std::uint64_t>(tx.get_allocator(), boost::endian::native_to_big(ps->level_pages[1]->address.offset));
          ps->parent->async_set_elements_diskonly_op(tx, new_sibling_index_in_parent, std::as_bytes(std::span<const std::uint64_t, 1>(new_sibling_offset_be.get(), 1)),
              completion_wrapper<void(std::error_code)>(
                  ++barrier,
                  [new_sibling_offset_be](auto handler, std::error_code ec) mutable {
                    new_sibling_offset_be.reset();
                    std::invoke(handler, ec);
                  }));
        }
        // We copy the augment and key from the current page.
        ps->parent->async_set_augments_diskonly_op(tx, new_sibling_index_in_parent, ps->parent->augment_span(*index_in_parent), ++barrier);
        if constexpr(std::is_same_v<PageType, intr_type>) {
          ps->parent->async_set_keys_diskonly_op(tx, new_sibling_index_in_parent - 1u, ps->level_pages[0]->key_span(page->index_size() - shift - 1u), ++barrier);
        } else {
          ps->parent->async_set_keys_diskonly_op(tx, new_sibling_index_in_parent - 1u, (ps->rebalance.end() - shift)->elem->key_span(), ++barrier);
        }

        // Mark both siblings as needing an update to their augmentation.
        ps->level_pages[0]->async_set_augment_propagation_required_diskonly_op(tx, ++barrier);
        ps->level_pages[1]->async_set_augment_propagation_required_diskonly_op(tx, ++barrier);

        // Update parent pointer in our new page.
        ps->level_pages[1]->async_set_parent_diskonly_op(tx, ps->parent->address.offset, ++barrier);

        // All writes have been queued.
        std::invoke(barrier, std::error_code());
      }

      auto commit_to_disk(tx_type tx, std::shared_ptr<page_selection> ps, std::size_t shift) -> void {
        tx.async_commit(
            [self_op=std::move(*this), ps=std::move(ps), shift](std::error_code ec) mutable -> void {
              if (ec) {
                self_op.error_invoke(ec);
              } else {
                lock_ps_for_exclusive(std::move(ps))
                | completion_wrapper<void(std::shared_ptr<page_selection>)>(
                    std::move(self_op),
                    [shift](op self_op, std::shared_ptr<page_selection> ps) {
                      self_op.update_memory_representation(std::move(ps), shift);
                    });
              }
            });
      }

      auto update_memory_representation(std::shared_ptr<page_selection> ps, std::uint32_t shift) -> void {
        const std::optional<std::size_t> index_in_parent = ps->parent->find_index(ps->level_pages[0]->address);
        assert(index_in_parent.has_value()); // Already accounted for in update_disk_representation.
        const std::size_t new_sibling_index_in_parent = index_in_parent.value() + 1u;

        auto new_key = std::vector<std::byte, typename std::allocator_traits<TxAlloc>::template rebind_alloc<std::byte>>(tx_alloc);
        if constexpr(std::is_same_v<PageType, intr_type>) {
          auto span = ps->level_pages[0]->key_span(page->index_size() - shift - 1u);
          new_key.resize(span.size());
          std::copy(span.begin(), span.end(), new_key.begin());
        } else {
          auto span = (ps->rebalance.end() - shift)->elem->key_span();
          new_key.resize(span.size());
          std::copy(span.begin(), span.end(), new_key.begin());
        }

        if constexpr(std::is_same_v<PageType, intr_type>) {
          // Truncate old page.
          ps->level_pages[0]->hdr_.size -= shift;

          // Copy old page elements to new page, and set the size.
          const std::size_t copy_begin = page->index_size();
          const std::size_t copy_end = copy_begin + shift;
          ps->level_pages[1]->hdr_.size = shift;
          span_copy(ps->level_pages[1]->element_multispan(0, shift), page->element_multispan(copy_begin, copy_end));
          span_copy(ps->level_pages[1]->augment_multispan(0, shift), page->augment_multispan(copy_begin, copy_end));
          span_copy(ps->level_pages[1]->key_multispan(0, shift - 1u), page->key_multispan(copy_begin, copy_end - 1u));

          for (const auto& child_page : ps->reparent)
            child_page.page->parent_offset = ps->level_pages[1]->address.offset;
        } else {
          static_assert(std::is_same_v<PageType, leaf_type>);

          for (const auto& rebalance_item : ps->rebalance) assert(rebalance_item.exlock.is_locked());
          std::array<std::span<bplus_tree_leaf_use_element>, 2> use_lists{
            ps->level_pages[0]->use_list_span(),
            ps->level_pages[1]->use_list_span(),
          };
          std::for_each(use_lists.begin(), use_lists.end(),
              [](auto& l) {
                std::fill(l.begin(), l.end(), bplus_tree_leaf_use_element::unused);
              });
          std::for_each(ps->rebalance.begin(), ps->rebalance.end(),
              [&use_lists, ps](const auto& rebalance_item) {
                use_lists[rebalance_item.which_page][rebalance_item.new_index] = rebalance_item.use_type;
                span_copy(ps->level_pages[rebalance_item.which_page]->element_span(rebalance_item.new_index),
                    std::span<const std::byte>(rebalance_item.kv_bytes.data(), rebalance_item.kv_bytes.size()));
              });

          // Update elements.
          std::for_each(ps->rebalance.begin(), ps->rebalance.end(),
              [ps](const auto& rebalance_item) {
                if (rebalance_item.which_page != 0)
                  rebalance_item.elem->owner = ps->level_pages[rebalance_item.which_page];
                rebalance_item.elem->index = rebalance_item.new_index;
              });
          // Link shifted elements into the new page.
          ps->level_pages[1]->elements_.insert(
              ps->level_pages[1]->elements_.end(),
              std::make_move_iterator(ps->level_pages[0]->elements_.end()) - shift,
              std::make_move_iterator(ps->level_pages[0]->elements_.end()));
          ps->level_pages[0]->elements_.erase(
              ps->level_pages[0]->elements_.end() - shift,
              ps->level_pages[0]->elements_.end());

          // Update predecessor/successor references.
          ps->level_pages[0]->hdr_.successor_page = ps->level_pages[1]->address.offset;
          ps->level_pages[1]->hdr_.predecessor_page = ps->level_pages[0]->address.offset;
          if (ps->level_pages[2] != nullptr) {
            ps->level_pages[1]->hdr_.successor_page = ps->level_pages[2]->address.offset;
            ps->level_pages[2]->hdr_.predecessor_page = ps->level_pages[1]->address.offset;
          }
        }

        // Update parent page: shift existing elements.
        const auto parent_old_size = ps->parent->hdr_.size++;
        if (new_sibling_index_in_parent < parent_old_size) {
          span_copy(ps->parent->element_multispan(new_sibling_index_in_parent + 1u, ps->parent->hdr_.size), ps->parent->element_multispan(new_sibling_index_in_parent, parent_old_size));
          span_copy(ps->parent->augment_multispan(new_sibling_index_in_parent + 1u, ps->parent->hdr_.size), ps->parent->augment_multispan(new_sibling_index_in_parent, parent_old_size));
          span_copy(ps->parent->key_multispan(new_sibling_index_in_parent, ps->parent->hdr_.size - 1u), ps->parent->key_multispan(new_sibling_index_in_parent - 1u, parent_old_size - 1u));
        }

        // Update parent page: install new page.
        const std::uint64_t new_sibling_offset_be = boost::endian::native_to_big(ps->level_pages[1]->address.offset);
        span_copy(ps->parent->element_span(new_sibling_index_in_parent), std::as_bytes(std::span<const std::uint64_t, 1>(&new_sibling_offset_be, 1)));
        // We copy the augment and key from the current page.
        span_copy(ps->parent->augment_span(new_sibling_index_in_parent), ps->parent->augment_span(*index_in_parent));
        span_copy(ps->parent->key_span(new_sibling_index_in_parent - 1u), std::span<const std::byte>(new_key.data(), new_key.size()));

        // Update the splitted pages that they'll need to update augmentation.
        ps->level_pages[0]->augment_propagation_required = true;
        ps->level_pages[1]->augment_propagation_required = true;

        ps->level_pages[1]->parent_offset = ps->parent->address.offset;

        // Update versions.
        ++ps->level_pages[0]->versions.page_split;
        ++ps->level_pages[0]->versions.augment;
        ++ps->level_pages[1]->versions.augment;

        // We're done \o/
        done_invoke();

        // We want the augmentations to start updating, so we'll kick that off.
        // These are deferred, so they shouldn't grab any locks until we're done.
        ps->level_pages[0]->async_fix_augment_background();
        ps->level_pages[1]->async_fix_augment_background();
      }

      auto done_invoke() -> void {
        std::invoke(handler, std::error_code());
      }

      auto error_invoke(std::error_code ec) -> void {
        std::invoke(handler, ec);
      }

      TxAlloc tx_alloc;
      cycle_ptr::cycle_gptr<bplus_tree> tree;
      cycle_ptr::cycle_gptr<PageType> page;
      bplus_tree_versions::type page_split;
      move_only_function<void(std::error_code)> handler;
    };

    std::invoke(op(
            this->shared_from_this(this),
            std::move(page),
            std::move(page_split),
            std::move(tx_alloc),
            std::move(handler)));
  }

  template<typename PageType, typename TxAlloc>
  requires (std::is_same_v<PageType, intr_type> || std::is_same_v<PageType, leaf_type>)
  auto split_page_op_(cycle_ptr::cycle_gptr<PageType> page, bplus_tree_versions::type page_split, TxAlloc tx_alloc) {
    return asio::async_initiate<decltype(asio::deferred), void(std::error_code)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree> tree, cycle_ptr::cycle_gptr<PageType> page, bplus_tree_versions::type page_split, TxAlloc tx_alloc) {
          // Always use post to invoke the handler:
          // the split_page_impl_() function ends with many locks held,
          // and we want those to be released as soon as possible.
          tree->split_page_impl_(std::move(page), std::move(page_split), std::move(tx_alloc), completion_handler_fun(std::move(handler), tree->get_executor()));
        },
        asio::deferred, this->shared_from_this(this), std::move(page), std::move(page_split), std::move(tx_alloc));
  }

  template<typename TxAlloc>
  struct insert_op
  : std::enable_shared_from_this<insert_op<TxAlloc>>
  {
    using byte_vector = std::vector<std::byte, typename std::allocator_traits<TxAlloc>::template rebind_alloc<std::byte>>;
    using monitor_exlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock;
    using monitor_uplock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock;
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;
    using element_vector = typename bplus_tree_leaf<raw_db_type>::element_vector;
    using tx_type = typename raw_db_type::template fdb_transaction<TxAlloc>;

    struct shift_elem {
      cycle_ptr::cycle_gptr<typename bplus_tree_leaf<raw_db_type>::element> elem;
      monitor_uplock_type uplock;
      monitor_exlock_type exlock;
    };
    using shift_elem_vector = std::vector<shift_elem, typename std::allocator_traits<TxAlloc>::template rebind_alloc<shift_elem>>;

    insert_op(cycle_ptr::cycle_gptr<bplus_tree> tree, std::span<const std::byte> key, std::span<const std::byte> value, TxAlloc tx_alloc)
    : tree(tree),
      key_and_value(make_key_and_value(*tree->spec, key, value, tx_alloc)),
      tx_alloc(tx_alloc),
      path(tree, tx_alloc)
    {}

    private:
    static auto make_key_and_value(const bplus_tree_spec& spec, std::span<const std::byte> key, std::span<const std::byte> value, TxAlloc tx_alloc) -> byte_vector {
      auto bytes = byte_vector(spec.element.key.padded_bytes() + spec.element.value.padded_bytes(), std::byte{0}, tx_alloc);
      span_copy(
          std::span(bytes.data(), bytes.size()).subspan(0, spec.element.key.bytes),
          key);
      span_copy(
          std::span(bytes.data(), bytes.size()).subspan(spec.element.key.padded_bytes(), spec.element.value.bytes),
          value);
      return bytes;
    }

    public:
    auto get_allocator() const -> TxAlloc { return tx_alloc; }
    auto element_span() const noexcept -> std::span<const std::byte> { return {key_and_value.data(), key_and_value.size()}; }

    auto key_span() const noexcept -> std::span<const std::byte> {
      const auto& spec = *tree->spec;
      return element_span().subspan(0, spec.element.key.bytes);
    }

    auto value_span() const noexcept -> std::span<const std::byte> {
      const auto& spec = *tree->spec;
      return element_span().subspan(spec.element.key.padded_bytes(), spec.element.value.bytes);
    }

    auto operator()(move_only_function<void(std::error_code, bplus_element_reference<raw_db_type, false>)> handler) -> void {
      assert(!this->handler);
      this->handler = std::move(handler);

      get_insertion_page();
    }

    private:
    auto get_insertion_page() -> void {
      tree->template find_leaf_page_for_insert_op_<monitor_uplock_type>(std::move(path), key_span())
      | [self_op=this->shared_from_this()](std::error_code ec, tree_path<TxAlloc> path, monitor_uplock_type leaf_lock) {
          if (ec) {
            self_op->error_invoke(ec);
          } else {
            self_op->path = std::move(path);
            self_op->use_target_page(std::move(leaf_lock));
          }
        };
    }

    auto use_target_page(monitor_uplock_type leaf_lock) -> void {
      // Ensure page has enough space.
      if (path.leaf_page.page->elements().size() >= tree->spec->elements_per_leaf) {
        leaf_lock.reset();
        tree->split_page_op_(path.leaf_page.page, path.leaf_page.versions.page_split, get_allocator())
        | [self_op=this->shared_from_this()](std::error_code ec) {
            if (ec && ec != make_error_code(db_errc::data_expired))
              self_op->error_invoke(ec);
            else
              self_op->get_insertion_page();
          };
        return;
      }

      find_insert_position(path.leaf_page.page->elements().begin(), path.leaf_page.page->elements().end(), std::move(leaf_lock));
    }

    auto find_insert_position(typename element_vector::const_iterator b, typename element_vector::const_iterator e, monitor_uplock_type leaf_lock) -> void {
      while (b != e) {
        const auto delta = e - b;
        assert(delta >= 1u);
        const typename element_vector::const_iterator m = b + delta / 2u;
        assert(b <= m && m < e);

        auto opt_elem_shlock = (*m)->element_lock.try_shared(__FILE__, __LINE__);
        if (opt_elem_shlock.has_value()) {
          if ((*m)->key_compare(key_span()) == std::strong_ordering::greater)
            e = m;
          else
            b = m + 1;
        } else {
          (*m)->element_lock.async_shared(
              [b, e, m, leaf_lock, self_op=this->shared_from_this()](auto elem_shlock) mutable -> void {
                if ((*m)->key_compare(self_op->key_span()) == std::strong_ordering::greater) {
                  elem_shlock.reset();
                  self_op->find_insert_position(b, m, std::move(leaf_lock));
                } else {
                  elem_shlock.reset();
                  self_op->find_insert_position(m + 1, e, std::move(leaf_lock));
                }
              },
              __FILE__, __LINE__);
          return; // Callback will restart the loop.
        }
      }

      insert_at(b, std::move(leaf_lock));
    }

    auto insert_at(const typename element_vector::const_iterator insert_before_pos, monitor_uplock_type leaf_lock) -> void {
      const auto& elements = path.leaf_page.page->elements();
      const std::size_t elements_per_leaf = path.leaf_page.page->spec->elements_per_leaf;

      // Figure out the start of the range to shift, if we shift things down.
      typename element_vector::const_iterator forw_shift_begin = insert_before_pos;
      while (forw_shift_begin != elements.begin()
          && (*std::prev(forw_shift_begin))->index + 1u == (forw_shift_begin == elements.end() ? path.leaf_page.page->spec->elements_per_leaf : (*forw_shift_begin)->index))
        --forw_shift_begin;

      // Figure out the end of the range to shift, if we shift things up.
      typename element_vector::const_iterator back_shift_end = insert_before_pos;
      if (back_shift_end != elements.end()) {
        ++back_shift_end;
        while (back_shift_end != elements.end() && (*std::prev(back_shift_end))->index + 1u == (*back_shift_end)->index)
          ++back_shift_end;
      }

      const auto back_shift_count = back_shift_end - insert_before_pos;
      const auto forw_shift_count = insert_before_pos - forw_shift_begin;
      const auto can_forw_shift = (forw_shift_begin == elements.begin()
          ? elements.empty() || elements.front()->index > 0u
          : true);
      const auto can_back_shift = (back_shift_end == elements.end()
          ? elements.empty() || elements.back()->index + 1u < elements_per_leaf
          : true);
      assert(can_forw_shift || can_back_shift);

      if (can_forw_shift && forw_shift_count == 0) {
        // No need for shifting, so skip the process entirely.
        do_ghost_insert(insert_before_pos, std::move(leaf_lock));
      } else if (can_forw_shift && (forw_shift_count <= back_shift_count || !can_back_shift)) {
        shift_(forw_shift_begin, insert_before_pos, leaf_lock,
            std::integral_constant<int, -1>(),
            [ self_op=this->shared_from_this(),
              insert_before_pos
            ](std::error_code ec, monitor_exlock_type leaf_exlock) mutable {
              if (ec)
                self_op->error_invoke(ec);
              else
                self_op->do_ghost_insert(insert_before_pos, std::move(leaf_exlock).as_upgrade_lock());
            });
      } else {
        shift_(insert_before_pos, back_shift_end, leaf_lock,
            std::integral_constant<int, 1>(),
            [ self_op=this->shared_from_this(),
              insert_before_pos
            ](std::error_code ec, monitor_exlock_type leaf_exlock) mutable {
              if (ec)
                self_op->error_invoke(ec);
              else
                self_op->do_ghost_insert(insert_before_pos, std::move(leaf_exlock).as_upgrade_lock());
            });
      }
    }

    template<int Direction, typename Handler>
    auto shift_(const typename element_vector::const_iterator b, const typename element_vector::const_iterator e, monitor_uplock_type leaf_lock, [[maybe_unused]] std::integral_constant<int, Direction> direction, Handler&& handler) -> std::enable_if_t<Direction == -1 || Direction == 1> {
      assert(shift_elems.empty());
      assert(b != path.leaf_page.page->elements().end());

      std::transform(b, e, std::back_inserter(shift_elems),
          [](const auto& elem_ptr) {
            return shift_elem{ .elem=elem_ptr };
          });
      const auto source_index = (*b)->index;
      const auto target_index = (Direction < 0 ? source_index - 1u : source_index + 1u);
      const auto shift_count = (*std::prev(e))->index + 1u - (*b)->index;

      auto raw_db = tree->raw_db.lock();
      if (raw_db == nullptr) [[unlikely]] {
        error_invoke(make_error_code(db_errc::data_expired));
        return;
      }
      tx_type tx = raw_db->fdb_tx_begin(isolation::read_commited, tx_mode::write_only, get_allocator());

      shift_lock_all_for_upgrade_(asio::deferred)
      | asio::deferred(
          [self_op=this->shared_from_this(), tx, target_index, source_index, shift_count]() {
            return asio::async_initiate<decltype(asio::deferred), void(std::error_code)>(
                [target_index, source_index, shift_count](auto handler, std::shared_ptr<insert_op> self_op, tx_type tx) {
                  auto& leaf_page = *self_op->path.leaf_page.page;
                  auto barrier = make_completion_barrier(std::move(handler), self_op->tree->get_executor());

                  leaf_page.async_set_use_list_diskonly_op(
                      tx, target_index,
                      leaf_page.use_list_span().subspan(source_index, shift_count),
                      ++barrier);
                  leaf_page.async_set_elements_diskonly_op(
                      tx, target_index,
                      leaf_page.element_multispan(source_index, source_index + shift_count),
                      ++barrier);

                  if constexpr(Direction < 0) {
                    leaf_page.async_set_use_list_diskonly_op(
                        tx, source_index + shift_count - 1u,
                        bplus_tree_leaf_use_element::unused,
                        ++barrier);
                  } else {
                    leaf_page.async_set_use_list_diskonly_op(
                        tx, source_index,
                        bplus_tree_leaf_use_element::unused,
                        ++barrier);
                  }

                  std::invoke(barrier, std::error_code());
                },
                asio::deferred, self_op, tx);
          })
      | asio::deferred(
          [tx](std::error_code ec) mutable {
            return asio::deferred.when(!ec)
                .then(tx.async_commit(asio::deferred))
                .otherwise(asio::deferred.values(ec));
          })
      | asio::deferred(
          [leaf_lock](std::error_code ec) {
            return asio::deferred.when(!ec)
                .then(leaf_lock.dispatch_exclusive(asio::append(asio::deferred, ec)))
                .otherwise(asio::deferred.values(monitor_exlock_type{}, ec));
          })
      | asio::deferred(
          [self_op=this->shared_from_this()](monitor_exlock_type leaf_exlock, std::error_code ec) {
            return asio::deferred.when(!ec)
                .then(self_op->shift_lock_all_for_ex_(asio::append(asio::deferred, ec, leaf_exlock)))
                .otherwise(asio::deferred.values(ec, leaf_exlock));
          })
      | asio::deferred(
          [ self_op=this->shared_from_this(),
            source_index, target_index, shift_count
          ](std::error_code ec, monitor_exlock_type leaf_exlock) {
            auto& leaf_page = *self_op->path.leaf_page.page;

            if (!ec) {
              span_copy(
                  leaf_page.use_list_span().subspan(target_index, shift_count),
                  leaf_page.use_list_span().subspan(source_index, shift_count));
              span_copy(
                  leaf_page.element_multispan(target_index, target_index + shift_count),
                  leaf_page.element_multispan(source_index, source_index + shift_count));

              if constexpr(Direction < 0)
                leaf_page.use_list_span()[source_index + shift_count - 1u] = bplus_tree_leaf_use_element::unused;
              else
                leaf_page.use_list_span()[source_index] = bplus_tree_leaf_use_element::unused;

              std::for_each(self_op->shift_elems.begin(), self_op->shift_elems.end(),
                  [](const auto& sh_elem) {
                    if constexpr(Direction < 0)
                      --sh_elem.elem->index;
                    else
                      ++sh_elem.elem->index;
                  });
            }

            self_op->shift_elems.clear();
            return asio::deferred.values(ec, std::move(leaf_exlock));
          })
      | std::forward<Handler>(handler);
    }

    template<typename CompletionToken>
    auto shift_lock_all_for_upgrade_(CompletionToken&& token) {
      return asio::async_initiate<CompletionToken, void()>(
          [](auto handler, std::shared_ptr<insert_op> self_op) -> void {
            self_op->shift_lock_all_for_upgrade_impl_(self_op->shift_elems.begin(), std::move(handler));
          },
          token, this->shared_from_this());
    }

    template<typename Handler>
    auto shift_lock_all_for_upgrade_impl_(typename shift_elem_vector::iterator i, Handler&& handler) -> void {
      for (/* skip */; i != shift_elems.end(); ++i) {
        auto opt_elem_uplock = i->elem->element_lock.try_upgrade(__FILE__, __LINE__);
        if (opt_elem_uplock.has_value()) {
          i->uplock = std::move(opt_elem_uplock).value();
        } else {
          i->elem->element_lock.async_upgrade(
              [ self_op=this->shared_from_this(),
                handler=std::forward<Handler>(handler),
                i
              ](monitor_uplock_type elem_uplock) mutable {
                i->uplock = std::move(elem_uplock);
                self_op->shift_lock_all_for_upgrade_impl_(std::next(i), std::move(handler));
              },
              __FILE__, __LINE__);
          return; // Callback will resume the loop.
        }
      }

      std::invoke(handler);
    }

    template<typename CompletionToken>
    auto shift_lock_all_for_ex_(CompletionToken&& token) {
      return asio::async_initiate<CompletionToken, void()>(
          [](auto handler, std::shared_ptr<insert_op> self_op) -> void {
            self_op->shift_lock_all_for_ex_impl_(self_op->shift_elems.begin(), std::move(handler));
          },
          token, this->shared_from_this());
    }

    template<typename Handler>
    auto shift_lock_all_for_ex_impl_(typename shift_elem_vector::iterator i, Handler&& handler) -> void {
      for (/* skip */; i != shift_elems.end(); ++i) {
        auto opt_elem_exlock = std::move(i->uplock).try_exclusive();
        if (opt_elem_exlock.has_value()) {
          i->exlock = std::move(opt_elem_exlock).value();
        } else {
          std::move(i->uplock).async_exclusive(
              [ self_op=this->shared_from_this(),
                handler=std::forward<Handler>(handler),
                i
              ](monitor_exlock_type elem_exlock) mutable {
                i->exlock = std::move(elem_exlock);
                self_op->shift_lock_all_for_ex_impl_(std::next(i), std::move(handler));
              });
          return; // Callback will resume the loop.
        }
      }

      std::invoke(handler);
    }

    auto do_ghost_insert(const typename element_vector::const_iterator insert_before_pos, monitor_uplock_type leaf_lock) -> void {
      // We want the new element to in the middle of the free space.
      // That way, hopefully future inserts won't require any shifting.
      const auto max_index = (insert_before_pos == path.leaf_page.page->elements().end()
          ? path.leaf_page.page->spec->elements_per_leaf
          : (*insert_before_pos)->index) - 1u;
      const auto min_index = (insert_before_pos == path.leaf_page.page->elements().begin()
          ? 0u
          : (*std::prev(insert_before_pos))->index + 1u);
      const auto insert_index = (min_index + max_index + 1u) / 2u;
      assert(min_index <= insert_index && insert_index <= max_index);

      auto new_element = bplus_element_reference<raw_db_type, false>::allocate(path.leaf_page.page->get_allocator(), path.leaf_page.page->get_executor(), insert_index, path.leaf_page.page, path.leaf_page.page->get_allocator());

      auto raw_db = tree->raw_db.lock();
      if (raw_db == nullptr) [[unlikely]] {
        error_invoke(make_error_code(db_errc::data_expired));
        return;
      }
      tx_type tx = raw_db->fdb_tx_begin(isolation::read_commited, tx_mode::write_only, get_allocator());

      asio::async_initiate<decltype(asio::deferred), void(std::error_code)>(
          [](auto handler, std::shared_ptr<insert_op> self_op, tx_type tx, std::size_t insert_index) -> void {
            auto& leaf_page = *self_op->path.leaf_page.page;
            auto barrier = make_completion_barrier(std::move(handler), self_op->tree->get_executor());

            leaf_page.async_set_augment_propagation_required_diskonly_op(tx, ++barrier);
            leaf_page.async_set_use_list_diskonly_op(tx, insert_index, bplus_tree_leaf_use_element::ghost_create, ++barrier);
            leaf_page.async_set_elements_diskonly_op(tx, insert_index, self_op->element_span(), ++barrier);

            std::invoke(barrier, std::error_code());
          },
          asio::deferred, this->shared_from_this(), tx, insert_index)
      | asio::deferred(
          [tx](std::error_code ec) mutable {
            return asio::deferred.when(!ec)
                .then(tx.async_commit(asio::deferred))
                .otherwise(asio::deferred.values(ec));
          })
      | asio::deferred(
          [leaf_lock](std::error_code ec) {
            return asio::deferred.when(!ec)
                .then(leaf_lock.async_exclusive(asio::append(asio::deferred, ec)))
                .otherwise(asio::deferred.values(monitor_exlock_type{}, ec));
          })
      | asio::deferred(
          [self_op=this->shared_from_this(), insert_before_pos, insert_index, new_element]([[maybe_unused]] monitor_exlock_type leaf_exlock, std::error_code ec) {
            if (!ec) {
              self_op->path.leaf_page.page->use_list_span()[insert_index] = bplus_tree_leaf_use_element::ghost_create;
              span_copy(self_op->path.leaf_page.page->element_span(insert_index), self_op->element_span());
              self_op->path.leaf_page.page->elements_.emplace(insert_before_pos, new_element.get());
              self_op->path.leaf_page.page->augment_propagation_required = true;
            }

            return asio::deferred.values(ec);
          })
      | asio::deferred(
          [self_op=this->shared_from_this(), new_element](std::error_code ec) {
            assert(self_op->shift_elems.empty());

            // Clear tree path. Path elements are likely to be invalidated.
            // And in any way, we no longer need them.
            // By releasing them, we allow the cache to forget about them.
            // It's good to be nice to the cache.
            self_op->path.clear();

            return asio::deferred.when(!ec)
                .then(new_element->template async_lock_owner<monitor_shlock_type>(self_op->get_allocator(), asio::append(asio::deferred, ec)))
                .otherwise(asio::deferred.values(cycle_ptr::cycle_gptr<bplus_tree_leaf<raw_db_type>>{}, monitor_shlock_type{}, ec));
          })
      | asio::deferred(
          [self_op=this->shared_from_this()](cycle_ptr::cycle_gptr<bplus_tree_leaf<raw_db_type>> leaf, monitor_shlock_type leaf_lock, std::error_code ec) {
            // We release all our locks while doing the augmentation, which can take a long time.
            // That way, other operations (both read and write) can take place.
            return asio::deferred.when(!ec)
                .then(leaf->async_fix_augment(self_op->get_allocator(), std::move(leaf_lock), asio::deferred))
                .otherwise(asio::deferred.values(ec));
          })
      | [self_op=this->shared_from_this(), new_element](std::error_code ec) {
          if (ec)
            self_op->error_invoke(ec);
          else
            self_op->promote_element(new_element);
        };
    }

    auto promote_element(bplus_element_reference<raw_db_type, false> elem_ref) -> void {
      assert(shift_elems.empty());

      auto raw_db = tree->raw_db.lock();
      if (raw_db == nullptr) [[unlikely]] {
        error_invoke(make_error_code(db_errc::data_expired));
        return;
      }
      tx_type tx = raw_db->fdb_tx_begin(isolation::read_commited, tx_mode::write_only, get_allocator());

      elem_ref->template async_lock_owner<monitor_uplock_type>(this->get_allocator(), asio::deferred)
      | asio::deferred(
          [elem_ref](cycle_ptr::cycle_gptr<bplus_tree_leaf<raw_db_type>> leaf, monitor_uplock_type leaf_lock) {
            return elem_ref->element_lock.async_upgrade(asio::append(asio::deferred, std::move(leaf), std::move(leaf_lock)), __FILE__, __LINE__);
          })
      | asio::deferred(
          [tx, elem_ref](monitor_uplock_type elem_lock, cycle_ptr::cycle_gptr<bplus_tree_leaf<raw_db_type>> leaf, monitor_uplock_type leaf_lock) {
            return leaf->async_set_use_list_diskonly_op(tx, elem_ref->index, bplus_tree_leaf_use_element::used,
                asio::append(asio::deferred, leaf, std::move(leaf_lock), std::move(elem_lock)));
          })
      | asio::deferred(
          [tx](std::error_code ec, cycle_ptr::cycle_gptr<bplus_tree_leaf<raw_db_type>> leaf, monitor_uplock_type leaf_lock, monitor_uplock_type elem_lock) mutable {
            return asio::deferred.when(!ec)
                .then(tx.async_commit(asio::append(asio::deferred, leaf, leaf_lock, elem_lock)))
                .otherwise(asio::deferred.values(ec, leaf, leaf_lock, elem_lock));
          })
      | asio::deferred(
          [](std::error_code ec, cycle_ptr::cycle_gptr<bplus_tree_leaf<raw_db_type>> leaf, monitor_uplock_type leaf_lock, monitor_uplock_type elem_lock) {
            return asio::deferred.when(!ec)
                .then(elem_lock.async_exclusive(asio::append(asio::deferred, ec, leaf, leaf_lock)))
                .otherwise(asio::deferred.values(monitor_exlock_type{}, ec, leaf, leaf_lock));
          })
      | asio::deferred(
          [elem_ref]([[maybe_unused]] monitor_exlock_type elem_lock, std::error_code ec, cycle_ptr::cycle_gptr<bplus_tree_leaf<raw_db_type>> leaf, [[maybe_unused]] monitor_uplock_type leaf_lock) {
            if (!ec) leaf->use_list_span()[elem_ref->index] = bplus_tree_leaf_use_element::used;
            return asio::deferred.values(ec);
          })
      | [self_op=this->shared_from_this(), elem_ref](std::error_code ec) {
          if (ec)
            self_op->error_invoke(ec);
          else
            std::invoke(self_op->handler, ec, std::move(elem_ref));
        };
    }

    auto error_invoke(std::error_code ec) -> void {
      std::invoke(handler, ec, bplus_element_reference<raw_db_type, false>{});
    }

    const cycle_ptr::cycle_gptr<bplus_tree> tree;
    const byte_vector key_and_value;
    shift_elem_vector shift_elems;
    TxAlloc tx_alloc;
    tree_path<TxAlloc> path;
    move_only_function<void(std::error_code, bplus_element_reference<raw_db_type, false>)> handler;
  };

  public:
  template<typename TxAlloc, typename CompletionToken>
  auto insert(std::span<const std::byte> key, std::span<const std::byte> value, TxAlloc tx_alloc, CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(std::error_code, bplus_element_reference<raw_db_type, false>)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree> self, std::shared_ptr<insert_op<TxAlloc>> op) {
          if constexpr(handler_has_executor_v<decltype(handler)>)
            std::invoke(*op, completion_handler_fun(std::move(handler), self->get_executor()));
          else
            std::invoke(*op, std::move(handler));
        },
        token, this->shared_from_this(this), std::allocate_shared<insert_op<TxAlloc>>(tx_alloc, this->shared_from_this(this), std::move(key), std::move(value), tx_alloc));
  }

  template<typename TxAlloc, typename CompletionToken>
  auto async_before_first_element(TxAlloc tx_alloc, CompletionToken&& token) {
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;
    using elem_ref_type = bplus_element_reference<raw_db_type, false>;

    return asio::async_initiate<CompletionToken, void(std::error_code, elem_ref_type)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree> self, TxAlloc tx_alloc) {
          self->tree_walk_(
              [](cycle_ptr::cycle_gptr<intr_type> intr) -> std::variant<db_address, std::error_code> {
                std::optional<db_address> addr = intr->get_index(0);
                if (addr.has_value()) {
                  return std::move(addr).value();
                } else {
                  std::clog << "bplus-tree: bad-tree, page has no child at index 0\n";
                  intr->maybe_log_dump();
                  return make_error_code(bplus_tree_errc::bad_tree);
                }
              },
              tree_path<TxAlloc>(self, std::move(tx_alloc)),
              completion_wrapper<void(std::error_code, tree_path<TxAlloc>, monitor_shlock_type)>(
                  std::move(handler),
                  [](auto handler, std::error_code ec, tree_path<TxAlloc> path, monitor_shlock_type leaf_lock) {
                    cycle_ptr::cycle_gptr<element> elem_ptr;

                    if (!ec && path.leaf_page.page->elements().empty()) [[unlikely]] {
                      std::clog << "bplus-tree: bad-tree, empty leaf-page\n";
                      path.leaf_page.page->maybe_log_dump();
                      ec = make_error_code(bplus_tree_errc::bad_tree);
                    }

                    if (!ec) {
                      elem_ptr = path.leaf_page.page->elements().front();
                      if (elem_ptr->type() != bplus_tree_leaf_use_element::before_first) [[unlikely]] {
                        std::clog << "bplus-tree: bad-tree, last element is supposed to be " << bplus_tree_leaf_use_element::before_first
                            << ", but is " << elem_ptr->type() << "\n";
                        path.leaf_page.page->maybe_log_dump();
                        ec = make_error_code(bplus_tree_errc::bad_tree);
                        elem_ptr.reset();
                      }
                    }

                    if (ec) {
                      leaf_lock.reset(); // No longer need the lock.
                      std::invoke(handler, ec, elem_ref_type{});
                    } else {
                      elem_ptr->element_lock.dispatch_shared(
                          completion_wrapper<void(monitor_shlock_type)>(
                              std::move(handler),
                              [ path=std::move(path),
                                leaf_lock=std::move(leaf_lock),
                                elem_ptr
                              ](auto handler, monitor_shlock_type elem_lock) mutable {
                                leaf_lock.reset(); // No longer need the leaf-lock, now that we hold the element lock.
                                path.clear(); // No longer need the path, now that we hold the element lock.

                                auto elem_ref = elem_ref_type(std::move(elem_ptr), elem_lock);
                                elem_lock.reset(); // No longer need the element-lock.
                                std::invoke(handler, std::error_code{}, std::move(elem_ref));
                              }),
                          __FILE__, __LINE__);
                    }
                  }));
        },
        token, this->shared_from_this(this), std::move(tx_alloc));
  }

  template<typename TxAlloc, typename CompletionToken>
  auto async_after_last_element(TxAlloc tx_alloc, CompletionToken&& token) {
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;
    using elem_ref_type = bplus_element_reference<raw_db_type, false>;

    return asio::async_initiate<CompletionToken, void(std::error_code, elem_ref_type)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree> self, TxAlloc tx_alloc) {
          self->tree_walk_(
              [](cycle_ptr::cycle_gptr<intr_type> intr) -> std::variant<db_address, std::error_code> {
                std::optional<db_address> addr = intr->get_index(intr->index_size() - 1u);
                if (addr.has_value()) {
                  return std::move(addr).value();
                } else {
                  std::clog << "bplus-tree: bad-tree, page has no child at index " << (intr->index_size() - 1u) << "\n";
                  intr->maybe_log_dump();
                  return make_error_code(bplus_tree_errc::bad_tree);
                }
              },
              tree_path<TxAlloc>(self, std::move(tx_alloc)),
              completion_wrapper<void(std::error_code, tree_path<TxAlloc>, monitor_shlock_type)>(
                  std::move(handler),
                  [](auto handler, std::error_code ec, tree_path<TxAlloc> path, monitor_shlock_type leaf_lock) {
                    cycle_ptr::cycle_gptr<element> elem_ptr;

                    if (!ec && path.leaf_page.page->elements().empty()) [[unlikely]] {
                      std::clog << "bplus-tree: bad-tree, empty leaf-page\n";
                      path.leaf_page.page->maybe_log_dump();
                      ec = make_error_code(bplus_tree_errc::bad_tree);
                    }

                    if (!ec) {
                      elem_ptr = path.leaf_page.page->elements().back();
                      if (elem_ptr->type() != bplus_tree_leaf_use_element::after_last) [[unlikely]] {
                        std::clog << "bplus-tree: bad-tree, last element is supposed to be " << bplus_tree_leaf_use_element::after_last
                            << ", but is " << elem_ptr->type() << "\n";
                        path.leaf_page.page->maybe_log_dump();
                        ec = make_error_code(bplus_tree_errc::bad_tree);
                        elem_ptr.reset();
                      }
                    }

                    if (ec) {
                      leaf_lock.reset(); // No longer need the lock.
                      std::invoke(handler, ec, elem_ref_type{});
                    } else {
                      elem_ptr->element_lock.dispatch_shared(
                          completion_wrapper<void(monitor_shlock_type)>(
                              std::move(handler),
                              [ path=std::move(path),
                                leaf_lock=std::move(leaf_lock),
                                elem_ptr
                              ](auto handler, monitor_shlock_type elem_lock) mutable {
                                leaf_lock.reset(); // No longer need the leaf-lock, now that we hold the element lock.
                                path.clear(); // No longer need the path, now that we hold the element lock.

                                auto elem_ref = elem_ref_type(std::move(elem_ptr), elem_lock);
                                elem_lock.reset(); // No longer need the element-lock.
                                std::invoke(handler, std::error_code{}, std::move(elem_ref));
                              }),
                          __FILE__, __LINE__);
                    }
                  }));
        },
        token, this->shared_from_this(this), std::move(tx_alloc));
  }

  template<typename TxAlloc, typename AcceptorFn, typename CompletionToken>
  auto async_visit(bplus_element_reference<raw_db_type, true> begin, bplus_element_reference<raw_db_type, true> end, TxAlloc tx_alloc, AcceptorFn&& acceptor, CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree> self, bplus_element_reference<raw_db_type, true> begin, bplus_element_reference<raw_db_type, true> end, TxAlloc tx_alloc, auto acceptor_fn) -> void {
          if constexpr(handler_has_executor_v<decltype(handler)>)
            leaf_type::async_visit(std::move(begin), std::move(end), std::move(tx_alloc), std::move(acceptor_fn), completion_handler_fun(std::move(handler), self->get_executor()));
          else
            leaf_type::async_visit(std::move(begin), std::move(end), std::move(tx_alloc), std::move(acceptor_fn), std::move(handler));
        },
        token, this->shared_from_this(this), std::move(begin), std::move(end), std::move(tx_alloc), std::forward<AcceptorFn>(acceptor));
  }

  template<typename TxAlloc, typename AcceptorFn, typename CompletionToken>
  auto async_visit_all(TxAlloc tx_alloc, AcceptorFn&& acceptor, CompletionToken&& token) {
    using elem_ref_type = bplus_element_reference<raw_db_type, true>;
    struct state {
      elem_ref_type b, e;
    };

    return async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree> self, TxAlloc tx_alloc, auto acceptor) {
          auto state_ptr = std::allocate_shared<state>(tx_alloc);

          auto barrier = make_completion_barrier(
              completion_wrapper<void(std::error_code)>(
                  std::move(handler),
                  [ acceptor=std::move(acceptor),
                    state_ptr, tx_alloc, self
                  ](auto handler, std::error_code ec) -> void {
                    if (ec)
                      std::invoke(handler, ec);
                    else
                      self->async_visit(std::move(state_ptr->b), std::move(state_ptr->e), std::move(tx_alloc), std::move(acceptor), std::move(handler));
                  }),
              self->get_executor());

          self->async_before_first_element(
              tx_alloc,
              completion_wrapper<void(std::error_code, elem_ref_type)>(
                  ++barrier,
                  [state_ptr](auto handler, std::error_code ec, elem_ref_type b) {
                    state_ptr->b = std::move(b);
                    std::invoke(handler, ec);
                  }));
          self->async_after_last_element(
              tx_alloc,
              completion_wrapper<void(std::error_code, elem_ref_type)>(
                  ++barrier,
                  [state_ptr](auto handler, std::error_code ec, elem_ref_type e) {
                    state_ptr->e = std::move(e);
                    std::invoke(handler, ec);
                  }));

          std::invoke(barrier, std::error_code());
        },
        token, this->shared_from_this(this), std::move(tx_alloc), std::forward<AcceptorFn>(acceptor));
  }

  template<typename TreeLockType, typename CompletionToken>
  requires std::same_as<TreeLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock>
      || std::same_as<TreeLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::upgrade_lock>
      || std::same_as<TreeLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock>
  auto async_log_dump(std::ostream& out, TreeLockType tree_lock, CompletionToken&& token) const {
    using namespace std::literals;

    return asio::async_initiate<CompletionToken, void()>(
        [](auto handler, cycle_ptr::cycle_gptr<const bplus_tree> self, std::ostream& out, TreeLockType tree_lock) -> void {
          if (!tree_lock.holds_monitor(self->page_lock)) throw std::logic_error("tree not locked");

          out << "tree " << self->address << (self->erased_ ? " (erased)"sv : ""sv) << "\n"sv;
          if (self->erased_) {
            std::invoke(handler);
            return;
          }

          cycle_ptr::cycle_gptr<raw_db_type> raw_db = self->raw_db.lock();
          if (raw_db == nullptr) {
            out << "  raw-db is null\n"sv;
            std::invoke(handler);
            return;
          }

          async_log_dump_pg_(
              out,
              std::move(raw_db), self->spec,
              self, std::move(tree_lock),
              db_address(self->address.file, self->hdr_.root),
              db_address(self->address.file, nil_page),
              "  "s,
              std::move(handler));
        },
        token, this->shared_from_this(this), std::ref(out), std::move(tree_lock));
  }

  template<typename CompletionToken>
  auto async_log_dump(std::ostream& out, CompletionToken&& token) const {
    return async_initiate<CompletionToken, void()>(
        [](auto handler, cycle_ptr::cycle_gptr<const bplus_tree> self, std::ostream& out) -> void {
          self->page_lock.async_shared(
              completion_wrapper<void(typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock)>(
                  std::move(handler),
                  [self, &out](auto handler, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock tree_lock) {
                    self->async_log_dump(out, std::move(tree_lock), std::move(handler));
                  }),
              __FILE__, __LINE__);
        },
        token, this->shared_from_this(this), std::ref(out));
  }

  private:
  template<typename LockType>
  static auto async_log_dump_pg_(
      std::ostream& out,
      cycle_ptr::cycle_gptr<raw_db_type> raw_db, std::shared_ptr<const bplus_tree_spec> spec,
      cycle_ptr::cycle_gptr<const void> from_ref, LockType&& from_lock,
      db_address page_addr, db_address parent_addr,
      std::string indent, move_only_function<void()> handler) -> void {
    using namespace std::literals;
    using page_variant = std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>>;

    struct op
    : public std::enable_shared_from_this<op>
    {
      op(std::ostream& out, std::string indent,
          db_address parent_addr,
          cycle_ptr::cycle_gptr<const void> from_ref, std::remove_cvref_t<LockType> from_lock,
          cycle_ptr::cycle_gptr<raw_db_type> raw_db, std::shared_ptr<const bplus_tree_spec> spec,
          move_only_function<void()> handler)
      : out(out),
        raw_db(std::move(raw_db)),
        spec(std::move(spec)),
        indent(std::move(indent)),
        parent_addr(std::move(parent_addr)),
        from_ref(std::move(from_ref)),
        from_lock(std::move(from_lock)),
        handler(std::move(handler))
      {}

      auto operator()(db_address page_addr) -> void {
        // Under normal operation, you shouldn't hold a lock while loading a page.
        // But this is a debug function, so we're kinda-okay with it.
        page_type::async_load_op(
            spec, raw_db, page_addr,
            []() {
              // Because we hold from_ref and from_lock.
              return asio::deferred.values(std::error_code());
            })
        | [self_op=this->shared_from_this(), page_addr](std::error_code ec, page_variant page) -> void {
            if (ec == make_error_code(bplus_tree_errc::restart)) {
              std::invoke(*self_op, page_addr);
            } else if (ec) {
              self_op->out << self_op->first_indent() << "page "sv << page_addr << " error: "sv << ec << "("sv << ec.message() << ")\n"sv;
              self_op->complete();
            } else {
              std::visit(
                  [&](auto page_ptr) {
                    if (page_ptr == nullptr) [[unlikely]] {
                      self_op->out << self_op->first_indent() << "page "sv << page_addr << " nullptr\n"sv;
                      self_op->complete();
                    } else {
                      page_ptr->page_lock.async_shared(
                          [self_op, page_ptr](typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock lock) mutable -> void {
                            self_op->print_page(std::move(page_ptr), std::move(lock));
                          },
                          __FILE__, __LINE__);
                    }
                  },
                  std::move(page));
            }
          };
      }

      private:
      auto print_page(cycle_ptr::cycle_gptr<const intr_type> page, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock lock) -> void {
        print_page_header("interior page"sv, *page);
        print_child_pages(std::move(page), std::move(lock));
      }

      auto print_child_pages(cycle_ptr::cycle_gptr<const intr_type> page, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock lock, std::size_t idx = 0) -> void {
        for (/* skip */; idx < page->index_size(); ++idx) {
          if (idx != 0)
            out << indent << "key["sv <<  (idx - 1u) << "]: "sv << byte_span_printer(page->key_span(idx - 1u)) << "\n"sv;

          if (page->element(idx) == nil_page) [[unlikely]] {
            out << indent << "error: nil page";
          } else {
            std::string next_indent = indent;
            next_indent += "  "sv;

            async_log_dump_pg_(
                out,
                raw_db, spec,
                page, lock,
                db_address(page->address.file, page->element(idx)), page->address,
                std::move(next_indent),
                [self_op=this->shared_from_this(), page, lock, idx]() mutable {
                  self_op->print_child_pages(std::move(page), std::move(lock), idx + 1u);
                });
            return; // callback will resume the loop
          }
        }

        lock.reset();
        complete();
      }

      auto print_page(cycle_ptr::cycle_gptr<const leaf_type> page, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock lock) -> void {
        print_page_header("leaf page"sv, *page);
        out << indent << "next_page_address: "sv << page->next_page_address() << "\n"sv
            << indent << "prev_page_address: "sv << page->prev_page_address() << "\n"sv;
        lock_elems(std::move(page), std::move(lock));
      }

      auto lock_elems(cycle_ptr::cycle_gptr<const leaf_type> page, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock lock) -> void {
        while (elem_locks.size() < page->elements().size()) {
          cycle_ptr::cycle_gptr<typename leaf_type::element> elem_ptr = page->elements().at(elem_locks.size());
          auto opt_elem_lock = elem_ptr->element_lock.try_shared(__FILE__, __LINE__);
          if (opt_elem_lock.has_value()) {
            elem_locks.emplace_back(std::move(opt_elem_lock).value());
          } else {
            elem_ptr->element_lock.async_shared(
                [ self_op=this->shared_from_this(),
                  page=std::move(page),
                  lock=std::move(lock)
                ](typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock elem_lock) mutable {
                  self_op->elem_locks.emplace_back(std::move(elem_lock));
                  self_op->lock_elems(std::move(page), std::move(lock));
                },
                __FILE__, __LINE__);
            return; // Loop will be resumed from callback.
          }
        }

        print_elems(std::move(page), std::move(lock));
      }

      auto print_elems(cycle_ptr::cycle_gptr<const leaf_type> page, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock lock) -> void {
        for (const auto& elem_ptr : page->elements()) {
          out << indent << "- "sv << elem_ptr->index << ": "sv << elem_ptr->type() << ", "sv
              << byte_span_printer(elem_ptr->key_span()) << " => "sv << byte_span_printer(elem_ptr->value_span()) << "\n"sv;
          if (elem_ptr->owner != page)
            out << indent << "  error: wrong owner "sv << elem_ptr->owner << " (expected "sv << page << ")\n"sv;
        }

        lock.reset();
        complete();
      }

      auto complete() -> void {
        asio::post(raw_db->get_executor(), std::move(handler));
      }

      auto print_page_header(std::string_view type, const page_type& page) -> void {
        out << first_indent() << type << " "sv << page.address << "\n"sv;
        if (page.parent_page_address() != parent_addr)
          out << indent << "parent address: "sv << page.parent_page_address() << " (expected: "sv << parent_addr << ")\n"sv;
      }

      auto first_indent() const -> std::string {
        std::string s = indent;
        if (s.size() >= 2) s[s.size() - 2u] = '-';
        return s;
      }

      std::ostream& out;
      std::vector<typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock> elem_locks;
      const cycle_ptr::cycle_gptr<raw_db_type> raw_db;
      const std::shared_ptr<const bplus_tree_spec> spec;
      const std::string indent;
      const db_address parent_addr;
      [[maybe_unused]] const cycle_ptr::cycle_gptr<const void> from_ref;
      [[maybe_unused]] const std::remove_cvref_t<LockType> from_lock;
      move_only_function<void()> handler;
    };

    std::invoke(*std::make_shared<op>(out, std::move(indent), std::move(parent_addr), std::move(from_ref), std::move(from_lock), std::move(raw_db), std::move(spec), std::move(handler)), std::move(page_addr));
  }

  public:
  const db_address address;

  protected:
  const cycle_ptr::cycle_weak_ptr<raw_db_type> raw_db;
  const std::shared_ptr<const bplus_tree_spec> spec;
  mutable monitor<executor_type, typename raw_db_type::allocator_type> page_lock;

  private:
  executor_type ex_;
  allocator_type alloc_;
  bplus_tree_header hdr_;
  DBAllocator db_alloc_;
  bool erased_ = false;
};


} /* namespace earnest::detail */
