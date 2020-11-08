#ifndef EARNEST_DETAIL_TREE_PAGE_H
#define EARNEST_DETAIL_TREE_PAGE_H

#include <memory>
#include <type_traits>
#include <cycle_ptr/cycle_ptr.h>
#include <boost/system/error_code.hpp>

#include <earnest/txfile.h>

#include <earnest/detail/export_.h>
#include <earnest/detail/db_cache.h>

#include <earnest/detail/tree/fwd.h>

namespace earnest::detail::tree {


class earnest_export_ abstract_page
: protected virtual cycle_ptr::cycle_base,
  public db_cache::cache_obj
{
  friend branch;
  friend ops;

  public:
  using offset_type = std::uint64_t;
  using allocator_type = db_cache::allocator_type;

  explicit abstract_page(std::shared_ptr<const struct cfg> tree_config, allocator_type alloc = allocator_type());
  virtual ~abstract_page() noexcept = 0;

  auto get_allocator() const -> allocator_type { return alloc; }

  void lock() { mtx_.lock(); }
  auto try_lock() -> bool { return mtx_.try_lock(); }
  void unlock() { mtx_.unlock(); }
  void lock_shared() const { mtx_.lock_shared(); }
  auto try_lock_shared() const -> bool { return mtx_.try_lock_shared(); }
  void unlock_shared() const { mtx_.unlock_shared(); }

  template<typename Page>
  static auto allocate_page(std::shared_ptr<const cfg> tree_config, allocator_type alloc = allocator_type())
  -> std::enable_if_t<std::is_base_of_v<abstract_page, Page>, cycle_ptr::cycle_gptr<Page>>;

  /**
   * \brief Decode a page from disk.
   * \param tree Tree that owns this page.
   * \param[in] tx Read-transaction in which the tree is located.
   * \param off Offset of the page.
   * \param alloc Allocator to use for the page.
   */
  static auto decode(const loader& loader, std::shared_ptr<const struct cfg> tree_config, const txfile::transaction& tx, offset_type off, allocator_type alloc, boost::system::error_code& ec)
  -> cycle_ptr::cycle_gptr<abstract_page>;

  protected:
  virtual void init();

  private:
  /**
   * \brief Implementation of the decode operation.
   * \param[in] tx Read-transaction in which the tree is located.
   * \param off Offset of the page.
   */
  virtual void decode_(const loader& loader, const txfile::transaction& tx, offset_type off, boost::system::error_code& ec) = 0;

  mutable std::shared_mutex mtx_;

  protected:
  offset_type offset = 0, parent = 0;
  allocator_type alloc;
  bool valid_ = true;

  public:
  const std::shared_ptr<const cfg> cfg;
};


} /* namespace earnest::detail::tree */

#include "page-inl.h"

#endif /* EARNEST_DETAIL_TREE_PAGE_H */
