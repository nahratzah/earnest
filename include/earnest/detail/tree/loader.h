#ifndef EARNEST_DETAIL_TREE_LOADER_H
#define EARNEST_DETAIL_TREE_LOADER_H

#include <earnest/detail/tree/fwd.h>
#include <earnest/detail/export_.h>
#include <earnest/txfile.h>
#include <earnest/detail/db_cache.h>
#include <earnest/detail/cheap_fn_ref.h>
#include <cycle_ptr/cycle_ptr.h>
#include <boost/polymorphic_pointer_cast.hpp>

namespace earnest::detail::tree {


class earnest_export_ loader {
  public:
  using offset_type = txfile::transaction::offset_type;

  virtual ~loader() noexcept;

  virtual auto allocate_elem(db_cache::allocator_type alloc) const -> cycle_ptr::cycle_gptr<value_type> = 0;
  virtual auto allocate_key(db_cache::allocator_type alloc) const -> cycle_ptr::cycle_gptr<key_type> = 0;
  virtual auto allocate_key(const value_type& value, db_cache::allocator_type alloc) const -> cycle_ptr::cycle_gptr<key_type> = 0;
  virtual auto allocate_augmented_page_ref(db_cache::allocator_type alloc) const -> cycle_ptr::cycle_gptr<augmented_page_ref> = 0;
  virtual auto allocate_augmented_page_ref(const leaf& leaf, db_cache::allocator_type alloc) const -> cycle_ptr::cycle_gptr<augmented_page_ref> = 0;
  virtual auto allocate_augmented_page_ref(const branch& branch, db_cache::allocator_type alloc) const -> cycle_ptr::cycle_gptr<augmented_page_ref> = 0;

  template<typename T>
  auto load_from_disk(offset_type off, cheap_fn_ref<cycle_ptr::cycle_gptr<db_cache::cache_obj>(db_cache::allocator_type, std::shared_ptr<const cfg>, const txfile::transaction&, offset_type)> load) const
  -> std::enable_if_t<std::is_base_of_v<db_cache::cache_obj, std::remove_const_t<T>>, cycle_ptr::cycle_gptr<T>>;

  /**
   * \brief Allocate space in a txfile.
   * \param[in,out] tx Transaction in which the allocation happens.
   * \param bytes Number of bytes to allocate.
   * \return Offset of allocated space.
   */
  virtual auto allocate_disk_space(txfile::transaction& tx, std::size_t bytes) const -> offset_type = 0;

  private:
  virtual auto do_load_from_disk(offset_type off, cheap_fn_ref<cycle_ptr::cycle_gptr<db_cache::cache_obj>(db_cache::allocator_type, std::shared_ptr<const cfg>, const txfile::transaction&, offset_type)> load) const -> cycle_ptr::cycle_gptr<db_cache::cache_obj> = 0;
};

template<typename T>
auto loader::load_from_disk(offset_type off, cheap_fn_ref<cycle_ptr::cycle_gptr<db_cache::cache_obj>(db_cache::allocator_type, std::shared_ptr<const cfg>, const txfile::transaction&, offset_type)> load) const
-> std::enable_if_t<std::is_base_of_v<db_cache::cache_obj, std::remove_const_t<T>>, cycle_ptr::cycle_gptr<T>> {
  return boost::polymorphic_pointer_downcast<T>(do_load_from_disk(off, std::move(load)));
}


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_LOADER_H */
