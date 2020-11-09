#ifndef EARNEST_DETAIL_TREE_LOADER_IMPL_H
#define EARNEST_DETAIL_TREE_LOADER_IMPL_H

#include <earnest/detail/export_.h>
#include <earnest/detail/tree/loader.h>
#include <earnest/detail/tree/tx_aware_value_type.h>
#include <earnest/detail/tree/key_type.h>
#include <earnest/detail/tree/augmented_page_ref.h>
#include <cycle_ptr/cycle_ptr.h>
#include <memory>
#include <tuple>
#include <type_traits>

namespace earnest::detail::tree {


template<typename ValueType>
class tx_aware_value_type_impl final
: public tx_aware_value_type
{
  public:
  using tx_aware_value_type::tx_aware_value_type;
  ~tx_aware_value_type_impl() noexcept override = default;

  explicit tx_aware_value_type_impl(ValueType value)
  : tx_aware_value_type(allocator_type()),
    value(std::move(value))
  {}

  tx_aware_value_type_impl(ValueType value, allocator_type alloc)
  : tx_aware_value_type(alloc),
    value(std::move(value))
  {}

  private:
  void encode_value(boost::asio::mutable_buffer buf) const override {
    value.encode(buf);
  }

  void decode_value(boost::asio::const_buffer buf) override {
    value.decode(buf);
  }

  public:
  ValueType value;
};


template<typename KeyType>
class key_type_impl final
: public key_type
{
  public:
  using key_type::key_type;
  ~key_type_impl() noexcept override = default;

  key_type_impl(KeyType key, allocator_type alloc)
  : key_type(alloc),
    key(std::move(key))
  {}

  explicit key_type_impl(KeyType key)
  : key_type_impl(std::move(key), allocator_type())
  {}

  void encode(boost::asio::mutable_buffer buf) const override {
    key.encode(buf);
  }

  void decode(boost::asio::const_buffer buf) override {
    key.decode(buf);
  }

  auto before(const key_type& y) const -> std::optional<bool> override {
    const key_type_impl*const y_impl = dynamic_cast<const key_type_impl*>(&y);
    if (y_impl == nullptr) return std::nullopt;
    return key < y_impl->key;
  }

  KeyType key;
};


template<typename... Augments>
class augment_impl final
: public augmented_page_ref
{
  public:
  template<typename Alloc>
  augment_impl(std::allocator_arg_t aa, Alloc&& alloc, Augments&&... augments, offset_type offset = 0)
  : augmented_page_ref(offset),
    augments(aa, std::forward<Alloc>(alloc), std::move(augments)...)
  {}

  template<typename Alloc>
  augment_impl(std::allocator_arg_t aa, Alloc&& alloc, const Augments&... augments, offset_type offset = 0)
  : augmented_page_ref(offset),
    augments(aa, std::forward<Alloc>(alloc), augments...)
  {}

  template<typename Alloc>
  augment_impl(std::allocator_arg_t aa, Alloc&& alloc, offset_type offset = 0)
  : augmented_page_ref(offset),
    augments(aa, std::forward<Alloc>(alloc))
  {}

  ~augment_impl() noexcept override = default;

  std::tuple<Augments...> augments;
};


template<>
class earnest_export_ augment_impl<> final
: public augmented_page_ref
{
  public:
  template<typename Alloc>
  augment_impl([[maybe_unused]] std::allocator_arg_t aa, [[maybe_unused]] Alloc alloc, offset_type offset = 0)
  : augmented_page_ref(offset)
  {}

  ~augment_impl() noexcept override;

  private:
  void do_encode(boost::asio::mutable_buffer buf) const override;
  void do_decode(boost::asio::const_buffer buf) override;
};


template<typename KeyType, typename ValueType, typename... Augments>
class tx_aware_loader
: public loader
{
  public:
  using key_type = key_type_impl<KeyType>;
  using value_type = tx_aware_value_type_impl<ValueType>;
  using augment_type = augment_impl<Augments...>;

  ~tx_aware_loader() noexcept = default;

  auto allocate_elem(db_cache::allocator_type alloc) const -> cycle_ptr::cycle_gptr<earnest::detail::tree::value_type> override;
  auto allocate_key(db_cache::allocator_type alloc) const -> cycle_ptr::cycle_gptr<earnest::detail::tree::key_type> override;
  auto allocate_key(const earnest::detail::tree::value_type& value, db_cache::allocator_type alloc) const -> cycle_ptr::cycle_gptr<earnest::detail::tree::key_type> override;
  auto allocate_augmented_page_ref(db_cache::allocator_type alloc) const -> cycle_ptr::cycle_gptr<earnest::detail::tree::augmented_page_ref> override;
  auto allocate_augmented_page_ref(const leaf& leaf, db_cache::allocator_type alloc) const -> cycle_ptr::cycle_gptr<earnest::detail::tree::augmented_page_ref> override;
  auto allocate_augmented_page_ref(const branch& leaf, db_cache::allocator_type alloc) const -> cycle_ptr::cycle_gptr<earnest::detail::tree::augmented_page_ref> override;

  private:
  static auto reduce_(std::tuple<Augments...>&& x, const std::tuple<Augments...>& y)
  -> std::tuple<Augments...>;

  static auto reduce_(std::tuple<Augments...>&& x, std::tuple<Augments...>&& y)
  -> std::tuple<Augments...>;

  template<std::size_t... Idx>
  static auto reduce_(std::tuple<Augments...>&& x, const std::tuple<Augments...>& y, std::index_sequence<Idx...> idx_seq)
  -> std::tuple<Augments...>;

  template<std::size_t... Idx>
  static auto reduce_(std::tuple<Augments...>&& x, std::tuple<Augments...>&& y, std::index_sequence<Idx...> idx_seq)
  -> std::tuple<Augments...>;
};


} /* namespace earnest::detail::tree */

namespace std {

template<class... Augments, class Alloc>
struct uses_allocator<earnest::detail::tree::augment_impl<Augments...>, Alloc>
: std::true_type
{};

} /* namespace std */

#include "loader_impl-inl.h"

#endif /* EARNEST_DETAIL_TREE_LOADER_IMPL_H */
