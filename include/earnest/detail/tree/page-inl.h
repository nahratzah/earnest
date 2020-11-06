#ifndef EARNEST_DETAIL_TREE_PAGE_INL_H
#define EARNEST_DETAIL_TREE_PAGE_INL_H

namespace earnest::detail::tree {


template<typename Page>
auto abstract_page::allocate_page(std::shared_ptr<const struct cfg> tree_config, allocator_type alloc)
-> std::enable_if_t<std::is_base_of_v<abstract_page, Page>, cycle_ptr::cycle_gptr<Page>> {
  auto page_ptr = cycle_ptr::allocate_cycle<Page>(alloc, std::move(tree_config), alloc);
  abstract_page* apg = page_ptr.get();
  apg->init();
  return page_ptr;
}


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_PAGE_INL_H */
