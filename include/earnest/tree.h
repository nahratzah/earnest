#ifndef EARNEST_TREE_H
#define EARNEST_TREE_H

#include <earnest/db.h>
#include <earnest/fd.h>
#include <earnest/detail/tree_spec.h>
#include <earnest/detail/tree_page.h>
#include <earnest/detail/commit_manager.h>
#include <functional>
#include <unordered_set>
#include <cycle_ptr/allocator.h>
#include <cycle_ptr/cycle_ptr.h>

namespace earnest {


class abstract_tree {
  public:
  class abstract_tx_object;
};


template<
    typename Key, typename Val, typename Less = std::less<Key>,
    typename... Augments>
class tree
: protected abstract_tree
{
  static_assert(earnest::detail::tree_key_spec<Key>::value);
  static_assert(earnest::detail::tree_val_spec<Val>::value);
  static_assert(std::is_invocable_r_v<bool, const Less&, const Key&, const Key&>,
      "less operation must be invocable on the key type");
  static_assert(std::conjunction_v<earnest::detail::tree_augment_spec<Key, Val, Augments>...>);

  public:
  class tx_object;

  ///\brief Number of bytes taken up by the tree.
  static constexpr std::size_t SIZE = 0;

  tree() = default;

  /**
   * \brief Open an existing tree.
   * \details
   * Reads \p SIZE bytes at offset \p off to figure out the configuration parameters for the tree.
   *
   * \param[in] tx A readable transaction in the file.
   * \param[in] off The offset of the tree.
   */
  tree(const txfile::transaction& t, fd::offset_type off);

  /**
   * \brief Initialize a new tree.
   * \details
   * Overwrites \p SIZE bytes at offset \p off initializing an empty tree.
   *
   * \note The tree is unusable until the transaction has been committed.
   * \param[in,out] tx A writeable transaction in the file.
   * \param[in] off The offset at which the tree will exist.
   */
  static auto init(
      txfile::transaction& t, fd::offset_type off,
      bool compressed_pages = false,
      std::size_t leaf_elems = earnest::detail::autoconf_tree_leaf_elems(Key::SIZE, Val::SIZE),
      std::size_t node_elems = earnest::detail::autoconf_tree_leaf_elems(Key::SIZE, sizeof(std::uint64_t), Augments::SIZE...));
};


class abstract_tree::abstract_tx_object
: public db::transaction_obj
{
  private:
  template<typename T>
  using unordered_cycle_set_type = std::unordered_set<
      cycle_ptr::cycle_member_ptr<T>,
      std::hash<cycle_ptr::cycle_member_ptr<T>>,
      std::equal_to<cycle_ptr::cycle_member_ptr<T>>,
      cycle_ptr::cycle_allocator<std::allocator<cycle_ptr::cycle_member_ptr<T>>>>;

  abstract_tx_object() = delete;
  abstract_tx_object(const abstract_tx_object&) = delete;

  protected:
  abstract_tx_object(db::transaction& tx, cycle_ptr::cycle_gptr<abstract_tree> self);
  ~abstract_tx_object() noexcept override;

  auto self() const -> cycle_ptr::cycle_gptr<abstract_tree>;

  private:
  void do_commit_phase1(detail::commit_manager::write_id& tx) override final;
  void do_rollback() noexcept override final;

  protected:
  db::transaction& tx_;

  private:
  cycle_ptr::cycle_member_ptr<abstract_tree> self_;
  unordered_cycle_set_type<earnest::detail::abstract_tree_elem> pending_create_, pending_delete_, must_exist_during_commit_;
};


/**
 * \brief Transaction logic of the tree.
 * \details
 * This objects acts as a tree inside a transaction.
 */
template<typename Key, typename Val, typename Less, typename... Augments>
class tree<Key, Val, Less, Augments...>::tx_object
: public abstract_tree::abstract_tx_object
{
  public:
  tx_object(db::transaction& tx, cycle_ptr::cycle_gptr<tree> self);

  protected:
  auto self() const noexcept -> cycle_ptr::cycle_gptr<tree>;
};


} /* namespace earnest */

#include "tree-inl.h"

#endif /* EARNEST_TREE_H */
