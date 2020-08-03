#ifndef EARNEST_DETAIL_TREE_PAGE_LEAF_ELEMS_H
#define EARNEST_DETAIL_TREE_PAGE_LEAF_ELEMS_H

#include <earnest/detail/export_.h>
#include <earnest/detail/tx_op.h>
#include <cstddef>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>
#include <cycle_ptr/cycle_ptr.h>
#include <cycle_ptr/allocator.h>

namespace earnest::detail {


/**
 * Helper class for tree_page_leaf_elems, that holds the predecessor and successor pointers.
 */
class tree_page_leaf_elems_hook {
  template<typename Elem, typename Alloc> friend class tree_page_leaf_elems;
  friend struct tree_page_leaf_elems_idx_compare_;

  tree_page_leaf_elems_hook(const tree_page_leaf_elems_hook&) = delete;
  tree_page_leaf_elems_hook(tree_page_leaf_elems_hook&&) = delete;
  tree_page_leaf_elems_hook& operator=(const tree_page_leaf_elems_hook&) = delete;
  tree_page_leaf_elems_hook& operator=(tree_page_leaf_elems_hook&&) = delete;

  public:
  using index_type = std::size_t;

  static constexpr index_type no_index = std::numeric_limits<index_type>::max();

  protected:
  tree_page_leaf_elems_hook(cycle_ptr::cycle_base& owner) noexcept;
  earnest_export_ virtual ~tree_page_leaf_elems_hook() noexcept;

  public:
  auto index() const -> index_type;

  protected:
  auto index(const std::shared_lock<std::shared_mutex>& lck) const -> index_type;
  auto index(const std::unique_lock<std::shared_mutex>& lck) const -> index_type;

  auto succ(const std::shared_lock<std::shared_mutex>& lck) const noexcept -> cycle_ptr::cycle_gptr<tree_page_leaf_elems_hook>;
  auto pred(const std::shared_lock<std::shared_mutex>& lck) const noexcept -> cycle_ptr::cycle_gptr<tree_page_leaf_elems_hook>;
  auto succ(const std::unique_lock<std::shared_mutex>& lck) const noexcept -> cycle_ptr::cycle_gptr<tree_page_leaf_elems_hook>;
  auto pred(const std::unique_lock<std::shared_mutex>& lck) const noexcept -> cycle_ptr::cycle_gptr<tree_page_leaf_elems_hook>;

  private:
  ///\brief Helper function, retrieves the mtx.
  virtual auto mtx_ref_() const noexcept -> std::shared_mutex& = 0;

  cycle_ptr::cycle_member_ptr<tree_page_leaf_elems_hook> succ_, pred_;
  index_type idx_ = no_index;
  index_type parent_idx_ = no_index;
};


template<typename ElemPtrIter>
class tree_page_leaf_elems_iterator {
  template<typename Elem, typename Alloc> friend class tree_page_leaf_elems;
  template<typename OtherIter> friend class tree_page_leaf_elems_iterator;

  private:
  ///\brief The raw pointer type of the iterator, with constness preserved.
  using raw_pointer_type = std::remove_reference_t<typename std::iterator_traits<ElemPtrIter>::reference>;
  ///\brief Raw element type of the pointer.
  using raw_element_type = typename std::pointer_traits<raw_pointer_type>::element_type;

  public:
  using iterator_category = typename std::iterator_traits<ElemPtrIter>::iterator_category;
  using difference_type = typename std::iterator_traits<ElemPtrIter>::difference_type;
  // We use const element if the iterator is a const iterator.
  using value_type = std::conditional_t<
      std::is_const_v<raw_pointer_type>,
      std::add_const_t<raw_element_type>,
      raw_element_type>;
  using reference = value_type&;
  using pointer = value_type*;

  tree_page_leaf_elems_iterator() noexcept(std::is_nothrow_default_constructible_v<ElemPtrIter>) = default;

  template<typename OtherIter, typename = std::enable_if_t<std::is_constructible_v<ElemPtrIter, const OtherIter&>>>
  tree_page_leaf_elems_iterator(const tree_page_leaf_elems_iterator<OtherIter>& other);

  private:
  tree_page_leaf_elems_iterator(const ElemPtrIter& iter)
      noexcept(std::is_nothrow_copy_constructible_v<ElemPtrIter>);
  tree_page_leaf_elems_iterator(ElemPtrIter&& iter)
      noexcept(std::is_nothrow_move_constructible_v<ElemPtrIter>);

  public:
  auto operator==(const tree_page_leaf_elems_iterator& y) const noexcept -> bool;
  auto operator!=(const tree_page_leaf_elems_iterator& y) const noexcept -> bool;
  auto operator<(const tree_page_leaf_elems_iterator& y) const noexcept -> bool;
  auto operator>(const tree_page_leaf_elems_iterator& y) const noexcept -> bool;
  auto operator<=(const tree_page_leaf_elems_iterator& y) const noexcept -> bool;
  auto operator>=(const tree_page_leaf_elems_iterator& y) const noexcept -> bool;
  auto operator-(const tree_page_leaf_elems_iterator& y) const noexcept -> difference_type;

  auto operator++() noexcept(noexcept(++std::declval<ElemPtrIter>())) -> tree_page_leaf_elems_iterator&;
  auto operator--() noexcept(noexcept(--std::declval<ElemPtrIter>())) -> tree_page_leaf_elems_iterator&;
  auto operator++(int) noexcept(noexcept(std::declval<ElemPtrIter>()++)) -> tree_page_leaf_elems_iterator;
  auto operator--(int) noexcept(noexcept(std::declval<ElemPtrIter>()--)) -> tree_page_leaf_elems_iterator;

  auto operator+=(difference_type n) noexcept(noexcept(std::declval<ElemPtrIter>() += std::declval<difference_type>())) -> tree_page_leaf_elems_iterator&;
  auto operator-=(difference_type n) noexcept(noexcept(std::declval<ElemPtrIter>() -= std::declval<difference_type>())) -> tree_page_leaf_elems_iterator&;
  auto operator+(difference_type n) const noexcept(noexcept(std::declval<const ElemPtrIter>() + std::declval<difference_type>())) -> tree_page_leaf_elems_iterator;
  auto operator-(difference_type n) const noexcept(noexcept(std::declval<const ElemPtrIter>() - std::declval<difference_type>())) -> tree_page_leaf_elems_iterator;

  auto operator*() const noexcept(noexcept(std::declval<const ElemPtrIter>()->operator*())) -> reference;
  auto operator->() const noexcept(noexcept(std::declval<const ElemPtrIter>()->operator->())) -> pointer;

  auto operator[](difference_type n) const noexcept(noexcept(std::declval<const ElemPtrIter>()[std::declval<difference_type>()].operator*())) -> reference;

  ///\brief Gain access to the underlying pointer.
  auto get() const noexcept(noexcept(*std::declval<const ElemPtrIter>())) -> cycle_ptr::cycle_gptr<value_type>;

  friend auto operator+(difference_type n, const tree_page_leaf_elems_iterator& iter) noexcept(noexcept(std::declval<const ElemPtrIter>() + std::declval<difference_type>())) -> tree_page_leaf_elems_iterator { return iter + n; }

  private:
  ElemPtrIter iter_;
};


template<typename Elem, typename Alloc = std::allocator<void>>
class tree_page_leaf_elems {
  tree_page_leaf_elems(const tree_page_leaf_elems&) = delete;
  tree_page_leaf_elems(tree_page_leaf_elems&&) = delete;
  tree_page_leaf_elems& operator=(const tree_page_leaf_elems&) = delete;
  tree_page_leaf_elems& operator=(tree_page_leaf_elems&&) = delete;

  public:
  using allocator_type = Alloc;

  private:
  using hook_type = tree_page_leaf_elems_hook;

  public:
  using index_type = hook_type::index_type;

  private:
  using elems_vector_entry = cycle_ptr::cycle_member_ptr<Elem>;
  using elems_vector = std::vector<
      elems_vector_entry,
      typename std::allocator_traits<cycle_ptr::cycle_allocator<allocator_type>>::template rebind_alloc<elems_vector_entry>>;

  public:
  using iterator = tree_page_leaf_elems_iterator<typename elems_vector::iterator>;
  using const_iterator = tree_page_leaf_elems_iterator<typename elems_vector::const_iterator>;
  using size_type = typename elems_vector::size_type;

  tree_page_leaf_elems(cycle_ptr::cycle_base& owner, std::size_t sz, allocator_type allocator = allocator_type());
  ~tree_page_leaf_elems() noexcept = default;

  auto begin() -> iterator;
  auto end() -> iterator;
  auto begin() const -> const_iterator;
  auto end() const -> const_iterator;
  auto cbegin() const -> const_iterator;
  auto cend() const -> const_iterator;

  void clear() noexcept;

  ///\brief Test if the collection is empty.
  auto empty() const noexcept -> bool;
  ///\brief Test if the collection has space for at least one more element.
  auto space_avail() const noexcept -> bool;
  ///\brief Count how many elements are present.
  auto size() const noexcept -> size_type;
  ///\brief Count the max number of elements this collection can hold.
  auto capacity() const noexcept -> size_type;

  /**
   * \brief Insert an element at the specified position.
   * \details
   * Inserts \p elem at (on-disk) index \p idx.
   * If there is already an element at \p idx, the insertion will fail.
   *
   * \param idx The index at which \p elem is to exist.
   * \param elem The element being inserted.
   * \return Pair with the iterator of that position, and an indication if the insert succeeded.
   */
  auto insert(index_type idx, cycle_ptr::cycle_gptr<Elem> elem) -> std::pair<iterator, bool>;
  /**
   * \brief Erase an element.
   * \details
   * Erases the element at \p pos from the collection.
   *
   * \param pos Iterator to the element that is to be removed from this collection.
   * \return Iterator to the element that used to be the successor of \p pos.
   */
  auto erase(const_iterator pos) -> iterator;
  /**
   * \brief Insert and element.
   * \details
   * This method will shift other elements around if needed.
   *
   * The collection may shift elements around to ensure there is room for the
   * new element.
   * All elements which have been moved, as well as the inserted element,
   * will be reported in the return value of the method (the last two tuple elements).
   *
   * If the insertion fails, no elements will change position and the returned
   * tuple will hold an empty range.
   *
   * \param[out] ops Transaction operations, for storing the undo operation.
   * \param pos Iterator the the element before which the insertion is to take place.
   * \param elem The element that will be inserted.
   *
   * \return A tuple.
   * The first element of the tuple points at the inserted element.
   * The second element indicates if the insertion is successful.
   * The third and fourth elements denote the range of the moved elements (this range always includes the inserted element).
   */
  auto insert_and_shift_before(tx_op_collection& ops, const_iterator pos, cycle_ptr::cycle_gptr<Elem> elem)
  -> std::tuple<iterator, bool, const_iterator, const_iterator>;

  ///\brief Find the iterator that points at \p e.
  auto iterator_to(const Elem& e) -> iterator;
  ///\brief Find the iterator that points at \p e.
  auto iterator_to(const Elem& e) const -> const_iterator;

  auto operator[](index_type idx) -> cycle_ptr::cycle_gptr<Elem>;
  auto operator[](index_type idx) const -> cycle_ptr::cycle_gptr<const Elem>;

  auto front() -> cycle_ptr::cycle_gptr<Elem>;
  auto front() const -> cycle_ptr::cycle_gptr<const Elem>;
  auto back() -> cycle_ptr::cycle_gptr<Elem>;
  auto back() const -> cycle_ptr::cycle_gptr<const Elem>;

  ///\brief Splice all elements from list.
  ///\note This list must be empty.
  void splice(tx_op_collection& ops, tree_page_leaf_elems& list);
  ///\brief Splice all elements from list.
  ///\note This list must be empty.
  void splice(tx_op_collection& ops, tree_page_leaf_elems&& list);
  ///\brief Splice one element from list.
  ///\note This list must be empty.
  void splice(tx_op_collection& ops, tree_page_leaf_elems& list, const_iterator it);
  ///\brief Splice one element from list.
  ///\note This list must be empty.
  void splice(tx_op_collection& ops, tree_page_leaf_elems&& list, const_iterator it);
  ///\brief Splice some elements from list.
  ///\note This list must be empty.
  void splice(tx_op_collection& ops, tree_page_leaf_elems& list, const_iterator first, const_iterator last);
  ///\brief Splice some elements from list.
  ///\note This list must be empty.
  void splice(tx_op_collection& ops, tree_page_leaf_elems&& list, const_iterator first, const_iterator last);

  private:
  auto verify_invariant(size_type expected_capacity) const noexcept;

  class invariant_check {
    explicit invariant_check(const tree_page_leaf_elems& self) noexcept
    : self(self),
      expected_capacity(self->vector_.capacity())
    {
      this->self.verify_invariant(expected_capacity);
    }

    ~invariant_check() noexcept {
      this->self.verify_invariant(expected_capacity);
    }

    private:
    const tree_page_leaf_elems& self;
    size_type expected_capacity;
  };

  elems_vector vector_;
};


struct tree_page_leaf_elems_idx_compare_ {
  using index_type = tree_page_leaf_elems_hook::index_type;

  template<typename X, typename Y>
  constexpr auto operator()(const X& x, const Y& y) const noexcept -> bool;

  private:
  static constexpr auto get_idx_(index_type idx) noexcept -> index_type;
  static auto get_idx_(const tree_page_leaf_elems_hook& hook) noexcept -> index_type;
  static auto get_idx_(const cycle_ptr::cycle_member_ptr<tree_page_leaf_elems_hook>& hook) noexcept -> index_type;
  static auto get_idx_(const cycle_ptr::cycle_member_ptr<const tree_page_leaf_elems_hook>& hook) noexcept -> index_type;
  static auto get_idx_(const cycle_ptr::cycle_gptr<tree_page_leaf_elems_hook>& hook) noexcept -> index_type;
  static auto get_idx_(const cycle_ptr::cycle_gptr<const tree_page_leaf_elems_hook>& hook) noexcept -> index_type;
};


} /* namespace earnest::detail */

#include "tree_page_leaf_elems-inl.h"

#endif /* EARNEST_DETAIL_TREE_PAGE_LEAF_ELEMS_H */
