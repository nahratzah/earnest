#ifndef EARNEST_DETAIL_TREE_OPS_H
#define EARNEST_DETAIL_TREE_OPS_H

#include <earnest/detail/export_.h>
#include <earnest/detail/tree/fwd.h>
#include <earnest/detail/db_cache.h>
#include <earnest/detail/locked_ptr.h>
#include <earnest/txfile.h>
#include <cycle_ptr/cycle_ptr.h>
#include <type_traits>

namespace earnest::detail::tree {


struct earnest_local_ ops {
  private:
  template<typename Child>
  static auto split_child_page_(
      const cycle_ptr::cycle_gptr<basic_tree>& f,
      const unique_lock_ptr<cycle_ptr::cycle_gptr<branch>>& self,
      const unique_lock_ptr<cycle_ptr::cycle_gptr<Child>>& child)
  -> unique_lock_ptr<cycle_ptr::cycle_gptr<Child>>;

  static auto load_page_(
      const cycle_ptr::cycle_gptr<const basic_tree>& f,
      std::uint64_t offset)
  -> cycle_ptr::cycle_gptr<abstract_page>;

  template<typename Tree, typename BranchPageSel>
  static auto begin_end_leaf_(
      const shared_lock_ptr<cycle_ptr::cycle_gptr<const Tree>>& f,
      BranchPageSel&& branch_page_sel)
  -> std::enable_if_t<std::is_base_of_v<basic_tree, Tree>, shared_lock_ptr<cycle_ptr::cycle_gptr<const leaf>>>;

  public:
  template<typename Page = abstract_page>
  static auto load_page(
      const cycle_ptr::cycle_gptr<const basic_tree>& f,
      std::uint64_t offset)
  -> std::enable_if_t<std::is_base_of_v<abstract_page, std::remove_const_t<Page>>, cycle_ptr::cycle_gptr<Page>>;

  static auto split_child_page(
      const cycle_ptr::cycle_gptr<basic_tree>& f,
      const unique_lock_ptr<cycle_ptr::cycle_gptr<branch>>& self,
      const unique_lock_ptr<cycle_ptr::cycle_gptr<branch>>& child)
  -> unique_lock_ptr<cycle_ptr::cycle_gptr<branch>>;

  static auto split_child_page(
      const cycle_ptr::cycle_gptr<basic_tree>& f,
      const unique_lock_ptr<cycle_ptr::cycle_gptr<branch>>& self,
      const unique_lock_ptr<cycle_ptr::cycle_gptr<leaf>>& child)
  -> unique_lock_ptr<cycle_ptr::cycle_gptr<leaf>>;

  template<typename Tree>
  static auto begin(
      const shared_lock_ptr<cycle_ptr::cycle_gptr<const Tree>>& f)
  -> std::enable_if_t<std::is_base_of_v<basic_tree, Tree>, leaf_iterator>;

  template<typename Tree>
  static auto end(
      const shared_lock_ptr<cycle_ptr::cycle_gptr<const Tree>>& f)
  -> std::enable_if_t<std::is_base_of_v<basic_tree, Tree>, leaf_iterator>;

  template<typename Tree>
  static auto rbegin(
      const shared_lock_ptr<cycle_ptr::cycle_gptr<const Tree>>& f)
  -> std::enable_if_t<std::is_base_of_v<basic_tree, Tree>, reverse_leaf_iterator>;

  template<typename Tree>
  static auto rend(
      const shared_lock_ptr<cycle_ptr::cycle_gptr<const Tree>>& f)
  -> std::enable_if_t<std::is_base_of_v<basic_tree, Tree>, reverse_leaf_iterator>;
};


} /* namespace earnest::detail::tree */

#include "ops-inl.h"

#endif /* EARNEST_DETAIL_TREE_OPS_H */
