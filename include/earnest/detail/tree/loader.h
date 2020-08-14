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

  template<typename T>
  auto load_from_disk(offset_type off, cheap_fn_ref<cycle_ptr::cycle_gptr<db_cache::cache_obj>(db_cache::allocator_type, cycle_ptr::cycle_gptr<abstract_tree>, const txfile::transaction&, offset_type)> load) const
  -> std::enable_if_t<std::is_base_of_v<db_cache::cache_obj, std::remove_const_t<T>>, cycle_ptr::cycle_gptr<T>>;

  private:
  virtual auto do_load_from_disk(offset_type off, cheap_fn_ref<cycle_ptr::cycle_gptr<db_cache::cache_obj>(db_cache::allocator_type, cycle_ptr::cycle_gptr<abstract_tree>, const txfile::transaction&, offset_type)> load) const -> cycle_ptr::cycle_gptr<db_cache::cache_obj> = 0;
};

template<typename T>
auto loader::load_from_disk(offset_type off, cheap_fn_ref<cycle_ptr::cycle_gptr<db_cache::cache_obj>(db_cache::allocator_type, cycle_ptr::cycle_gptr<abstract_tree>, const txfile::transaction&, offset_type)> load) const
-> std::enable_if_t<std::is_base_of_v<db_cache::cache_obj, std::remove_const_t<T>>, cycle_ptr::cycle_gptr<T>> {
  return boost::polymorphic_pointer_downcast<T>(do_load_from_disk(off, std::move(load)));
}


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_LOADER_H */
