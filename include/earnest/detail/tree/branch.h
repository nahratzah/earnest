#ifndef EARNEST_DETAIL_TREE_BRANCH_H
#define EARNEST_DETAIL_TREE_BRANCH_H

#include <earnest/detail/export_.h>

#include <earnest/detail/tree/fwd.h>
#include <earnest/detail/tree/page.h>
#include <earnest/detail/tree/augmented_page_ref.h>

#include <earnest/detail/locked_ptr.h>

#include <cycle_ptr/allocator.h>
#include <cycle_ptr/cycle_ptr.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace earnest::detail::tree {


class earnest_export_ branch final
: public abstract_page
{
  friend ops;
  template<typename KeyType, typename ValueType, typename... Augments> friend class tx_aware_loader;

  public:
  using shared_lock_ptr = earnest::detail::shared_lock_ptr<cycle_ptr::cycle_gptr<const branch>>;
  using unique_lock_ptr = earnest::detail::unique_lock_ptr<cycle_ptr::cycle_gptr<branch>>;

  ///\brief Magic value of the header.
  static constexpr std::uint32_t magic = 0x5825'b1f0U;

  struct header;

  private:
  template<typename T>
  using vector_type = std::vector<
      cycle_ptr::cycle_member_ptr<T>,
      cycle_ptr::cycle_allocator<
          std::allocator_traits<allocator_type>::template rebind_alloc<cycle_ptr::cycle_member_ptr<T>>>>;
  using key_vector = vector_type<const key_type>;
  using page_ref_vector = vector_type<augmented_page_ref>;

  public:
  using size_type = page_ref_vector::size_type;

  protected:
  void init() override;

  public:
  explicit branch(std::shared_ptr<const struct cfg> tree_config, allocator_type alloc = allocator_type());
  ~branch() noexcept override;

  /**
   * \brief Merge two pages together.
   * \details Moves all elements from \p back into \p front.
   * \param[in,out] front The first page that is being merged.
   * \param[in,out] back The second page that is being merged.
   * \param[out] tx Transaction.
   */
  static void merge(const loader& loader, const unique_lock_ptr& front, const unique_lock_ptr& back, txfile::transaction& tx, cycle_ptr::cycle_gptr<const key_type> separator_key);
  /**
   * \brief Split into two pages.
   * \details Moves some elements from \p front into \p back.
   * \param loader Loader for loading additional data from file.
   * \param[in,out] front The page being split.
   * \param[out] back Empty destination page.
   * \param[out] tx Transaction.
   * \return The key of the back page.
   */
  static auto split(const loader& loader, const unique_lock_ptr& front, const unique_lock_ptr& back, txfile::transaction& tx)
  -> cycle_ptr::cycle_gptr<const key_type>;

  /**
   * \brief Find the index at which this page is present.
   * \param self A branch page.
   * \param page The offset of the page that is to be found.
   * \return Index of \p page in \p self.
   */
  static auto find_slot(const shared_lock_ptr& self, offset_type page) -> std::optional<size_type>;
  /**
   * \brief Find the index at which this page is present.
   * \param self A branch page.
   * \param page The offset of the page that is to be found.
   * \return Index of \p page in \p self.
   */
  static auto find_slot(const unique_lock_ptr& self, offset_type page) -> std::optional<size_type>;

  static auto valid(const shared_lock_ptr& page) -> bool { return page->valid_; }
  static auto valid(const unique_lock_ptr& page) -> bool { return page->valid_; }

  ///\brief Retrieve the size of this page.
  static auto size(const shared_lock_ptr& self) -> size_type { return self->pages_.size(); }
  ///\brief Retrieve the size of this page.
  static auto size(const unique_lock_ptr& self) -> size_type { return self->pages_.size(); }
  ///\brief Retrieve the max size of this page.
  auto max_size() const -> size_type;

  private:
  ///\brief Size in bytes of the key type.
  auto bytes_per_key_() const noexcept -> std::size_t;
  ///\brief Size in bytes of the augmented page reference.
  auto bytes_per_augmented_page_ref_() const noexcept -> std::size_t;
  ///\brief Size in bytes of the page.
  auto bytes_per_page_() const noexcept -> std::size_t;
  ///\brief Implementation of find_slot.
  auto find_slot_(offset_type page) const noexcept -> std::optional<size_type>;

  void decode_(const loader& loader, const txfile::transaction& tx, offset_type off, boost::system::error_code& ec) override;

  key_vector keys_;
  page_ref_vector pages_;
};

struct earnest_export_ branch::header {
  static const std::size_t OFFSET_SIZE;
  static const std::size_t OFFSET_PARENT_PTR;
  static constexpr std::size_t SIZE = 16;

  std::uint32_t magic;
  std::uint32_t size;
  std::uint64_t parent_off;

  void native_to_big_endian() noexcept;
  void big_to_native_endian() noexcept;
  earnest_export_ void encode(boost::asio::mutable_buffer buf) const;

  template<typename SyncReadStream>
  void decode(SyncReadStream& stream, boost::system::error_code&);
};


} /* namespace earnest::detail::tree */

#include "branch-inl.h"

#endif /* EARNEST_DETAIL_TREE_BRANCH_H */
