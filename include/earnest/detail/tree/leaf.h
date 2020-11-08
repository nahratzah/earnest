#ifndef EARNEST_DETAIL_TREE_LEAF_H
#define EARNEST_DETAIL_TREE_LEAF_H

#include <cstddef>
#include <cstdint>
#include <shared_mutex>
#include <system_error>
#include <tuple>
#include <memory>

#include <boost/asio/buffer.hpp>
#include <boost/system/error_code.hpp>
#include <cycle_ptr/cycle_ptr.h>

#include <earnest/txfile.h>

#include <earnest/detail/export_.h>
#include <earnest/detail/locked_ptr.h>
#include <earnest/detail/layout_domain.h>

#include <earnest/detail/tree/fwd.h>
#include <earnest/detail/tree/page.h>

namespace earnest::detail::tree {


class earnest_export_ leaf final
: public abstract_page,
  public layout_obj
{
  friend tx_aware_value_type;
  friend leaf_iterator;
  friend ops;
  template<typename KeyType, typename ValueType, typename... Augments> friend class tx_aware_loader;

  public:
  using shared_lock_ptr = earnest::detail::shared_lock_ptr<cycle_ptr::cycle_gptr<const leaf>>;
  using unique_lock_ptr = earnest::detail::unique_lock_ptr<cycle_ptr::cycle_gptr<leaf>>;

  ///\brief Magic value of the header.
  static constexpr std::uint32_t magic = 0x2901'c28fU;

  struct header;
  using size_type = index_type;

  protected:
  void init() override;

  public:
  explicit leaf(std::shared_ptr<const struct cfg> tree_config, allocator_type alloc = allocator_type());
  ~leaf() noexcept override;

  /**
   * \brief Merge two pages together.
   * \details Moves all elements from \p back into \p front.
   * \param[in,out] front The first page that is being merged.
   * \param[in,out] back The second page that is being merged.
   * \param[out] tx Transaction.
   */
  static void merge(const cycle_ptr::cycle_gptr<basic_tree>& tree, const unique_lock_ptr& front, const unique_lock_ptr& back, txfile::transaction& tx);
  /**
   * \brief Split into two pages.
   * \details Moves some elements from \p front into \p back.
   * \param loader Loader for loading additional data from file.
   * \param[in,out] front The page being split.
   * \param[out] back Empty destination page.
   * \param[out] tx Transaction.
   * \return The key of the back page.
   */
  static auto split(const cycle_ptr::cycle_gptr<basic_tree>& tree, const unique_lock_ptr& front, const unique_lock_ptr& back, txfile::transaction& tx)
  -> cycle_ptr::cycle_gptr<const key_type>;

  /**
   * \brief Remove element from this page.
   * \details The on-disk storage of this element will be zeroed out.
   * \param[in,out] self The page from which the element is to be removed.
   * \param[in,out] elem The element which is to be removed.
   * \param[out] tx Transaction.
   */
  static void unlink(const unique_lock_ptr& self, cycle_ptr::cycle_gptr<value_type> elem, txfile::transaction& tx);
  /**
   * \brief Insert element into this page.
   * \details Insert \p elem before \p pos.
   * \param[in,out] self The page from which the element is to be removed.
   * \param[in,out] elem The element that is to be inserted.
   * \param[in,out] pos The element before which \p elem is to be inserted.
   *   Use `nullptr` if you wish the element to be inserted at the last position.
   * \param[out] tx Transaction.
   */
  static void link(const unique_lock_ptr& self, cycle_ptr::cycle_gptr<value_type> elem, cycle_ptr::cycle_gptr<value_type> pos, txfile::transaction& tx);

  static auto valid(const shared_lock_ptr& page) -> bool { return page->valid_; }
  static auto valid(const unique_lock_ptr& page) -> bool { return page->valid_; }

  ///\brief Retrieve the size of this page.
  static auto size(const shared_lock_ptr& self) -> size_type { return self->size_; }
  ///\brief Retrieve the size of this page.
  static auto size(const unique_lock_ptr& self) -> size_type { return self->size_; }
  ///\brief Retrieve the max size of this page.
  auto max_size() const -> size_type;

  ///\brief Get the key of this page.
  ///\details Key may be nil.
  ///\note No lock required: key is only modified during decoding phase.
  auto key() const -> cycle_ptr::cycle_gptr<const key_type>;

  static auto before_begin(cycle_ptr::cycle_gptr<const basic_tree> tree, const shared_lock_ptr& self) -> leaf_iterator;
  static auto before_begin(cycle_ptr::cycle_gptr<const basic_tree> tree, const unique_lock_ptr& self) -> leaf_iterator;
  static auto begin(cycle_ptr::cycle_gptr<const basic_tree> tree, const shared_lock_ptr& self) -> leaf_iterator;
  static auto begin(cycle_ptr::cycle_gptr<const basic_tree> tree, const unique_lock_ptr& self) -> leaf_iterator;
  static auto end(cycle_ptr::cycle_gptr<const basic_tree> tree, const shared_lock_ptr& self) -> leaf_iterator;
  static auto end(cycle_ptr::cycle_gptr<const basic_tree> tree, const unique_lock_ptr& self) -> leaf_iterator;

  private:
  earnest_local_ static auto lock_elem_with_siblings_(const unique_lock_ptr& self, cycle_ptr::cycle_gptr<value_type> elem)
  -> std::tuple<
      earnest::detail::unique_lock_ptr<cycle_ptr::cycle_gptr<value_type>>,
      earnest::detail::unique_lock_ptr<cycle_ptr::cycle_gptr<value_type>>,
      earnest::detail::unique_lock_ptr<cycle_ptr::cycle_gptr<value_type>>>;
  ///\brief Size in bytes of the key type.
  auto bytes_per_key_() const noexcept -> std::size_t;
  ///\brief Size in bytes of the value type.
  auto bytes_per_val_() const noexcept -> std::size_t;
  ///\brief Size in bytes of the page.
  auto bytes_per_page_() const noexcept -> std::size_t;
  ///\brief Compute offset from slot index.
  auto offset_for_idx_(index_type idx) const noexcept -> offset_type;

  void decode_(const loader& loader, const txfile::transaction& tx, offset_type off, boost::system::error_code& ec) override;

  auto get_layout_domain() const noexcept -> const layout_domain& override;
  void lock_layout() const override final;
  bool try_lock_layout() const override final;
  void unlock_layout() const override final;

  cycle_ptr::cycle_member_ptr<value_type> head_sentinel_, tail_sentinel_;
  cycle_ptr::cycle_member_ptr<const key_type> key_;
  size_type size_ = 0;
  offset_type predecessor_off_ = 0, successor_off_ = 0;
};


struct earnest_export_ leaf::header {
  static const std::size_t OFFSET_PARENT_PTR;
  static const std::size_t OFFSET_NEXT_SIBLING_PTR;
  static const std::size_t OFFSET_PREV_SIBLING_PTR;
  static constexpr std::size_t SIZE = 32;
  static constexpr std::uint32_t flag_has_key = 0x0000'0001;

  std::uint32_t magic;
  std::uint32_t flags;
  std::uint64_t parent_off;
  std::uint64_t next_sibling_off;
  std::uint64_t prev_sibling_off;

  void native_to_big_endian() noexcept;
  void big_to_native_endian() noexcept;
  earnest_export_ void encode(boost::asio::mutable_buffer buf) const;

  template<typename SyncReadStream>
  void decode(SyncReadStream& stream, boost::system::error_code&);
};


} /* namespace earnest::detail::tree */

#include "leaf-inl.h"

#endif /* EARNEST_DETAIL_TREE_LEAF_H */
