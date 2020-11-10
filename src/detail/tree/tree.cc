#include <earnest/detail/tree/tree.h>
#include <earnest/detail/tree/leaf_iterator.h>
#include <earnest/detail/tree/ops.h>
#include <earnest/detail/locked_ptr.h>
#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/read_at.hpp>
#include <boost/endian/conversion.hpp>
#include <array>
#include <shared_mutex>

namespace earnest::detail::tree {
namespace {


auto decode_cfg(const txfile::transaction& tx, txfile::transaction::offset_type offset, basic_tree::allocator_type alloc) -> std::shared_ptr<cfg> {
  std::uint32_t magic;
  std::array<std::uint8_t, cfg::SIZE> buf;

  boost::asio::read_at(tx, offset,
      std::array<boost::asio::mutable_buffer, 2>{
          boost::asio::buffer(&magic, sizeof(magic)),
          boost::asio::buffer(buf)
      },
      boost::asio::transfer_all());

  // Validate magic.
  boost::endian::big_to_native_inplace(magic);
  if (magic != basic_tree::magic)
    throw std::runtime_error("invalid magic"); // XXX

  auto c = std::allocate_shared<cfg>(alloc);
  c->decode(boost::asio::buffer(buf));
  return c;
}


}


basic_tree::basic_tree(std::shared_ptr<class db> db, txfile::transaction::offset_type offset, std::shared_ptr<const class loader> loader, allocator_type alloc)
: db::db_obj(db),
  cfg(decode_cfg(txfile_begin(db), offset, alloc)), // Validates magic.
  loader(std::move(loader)),
  alloc_(alloc)
{
  auto tx = txfile_begin(db);
  boost::asio::read_at(
      tx,
      offset + ROOT_PAGE_OFFSET,
      boost::asio::buffer(&root_page_, sizeof(root_page_)),
      boost::asio::transfer_all());
  boost::endian::big_to_native_inplace(root_page_);
  tx.commit();

  // Validate cfg against loader.
  if (cfg->key_bytes != loader->key_bytes())
    throw std::runtime_error("key bytes mismatch");
  if (cfg->val_bytes != loader->val_bytes())
    throw std::runtime_error("value bytes mismatch");
  if (cfg->augment_bytes != loader->augment_bytes())
    throw std::runtime_error("augment bytes mismatch");
}

basic_tree::~basic_tree() noexcept = default;

void basic_tree::create_(std::shared_ptr<class db> db, txfile::transaction::offset_type offset, const class loader& loader, std::size_t items_per_leaf, std::size_t items_per_branch) {
  using cfg = earnest::detail::tree::cfg;

  if (items_per_leaf > 0xffff'ffffU) throw std::range_error("too many items per leaf");
  if (items_per_branch > 0xffff'ffffU) throw std::range_error("too many items per branch");
  if (loader.key_bytes() > 0xffff'ffffU) throw std::invalid_argument("too many bytes in key type");
  if (loader.val_bytes() > 0xffff'ffffU) throw std::invalid_argument("too many bytes in value type");
  if (loader.augment_bytes() > 0xffff'ffffU) throw std::invalid_argument("too many bytes in augmentations");

  std::uint32_t magic = basic_tree::magic;
  std::array<std::uint8_t, cfg::SIZE> cfg_buf;
  std::uint64_t root_page = 0;

  boost::endian::native_to_big_inplace(magic);
  boost::endian::native_to_big_inplace(root_page);
  cfg{
    static_cast<std::uint32_t>(items_per_leaf),
    static_cast<std::uint32_t>(items_per_branch),
    static_cast<std::uint32_t>(loader.key_bytes()),
    static_cast<std::uint32_t>(loader.val_bytes()),
    static_cast<std::uint32_t>(loader.augment_bytes())
  }.encode(boost::asio::buffer(cfg_buf));

  auto tx = txfile_begin(db, false);
  boost::asio::write_at(
      tx,
      offset,
      std::array<boost::asio::const_buffer, 3>{
          boost::asio::buffer(&magic, sizeof(magic)),
          boost::asio::buffer(cfg_buf),
          boost::asio::buffer(&root_page, sizeof(root_page))
      });
  tx.commit();
}

void basic_tree::ensure_root_page_exists() {
  ops::ensure_root_page(*this);
}

auto basic_tree::has_pages() const -> bool {
  return root_page_ != 0u;
}


basic_tx_aware_tree::~basic_tx_aware_tree() noexcept = default;


basic_tx_aware_tree::tx_object::~tx_object() noexcept = default;

auto basic_tx_aware_tree::tx_object::empty() const -> bool {
  basic_tx_aware_tree::shared_lock_ptr locked_tree(tree_);
  return !locked_tree->has_pages()
      || begin_(locked_tree).is_sentinel();
}

auto basic_tx_aware_tree::tx_object::begin() const -> iterator {
  return begin_(basic_tx_aware_tree::shared_lock_ptr(tree_));
}

auto basic_tx_aware_tree::tx_object::end() const -> iterator {
  return end_(basic_tx_aware_tree::shared_lock_ptr(tree_));
}

auto basic_tx_aware_tree::tx_object::rbegin() const -> reverse_iterator {
  return rbegin_(basic_tx_aware_tree::shared_lock_ptr(tree_));
}

auto basic_tx_aware_tree::tx_object::rend() const -> reverse_iterator {
  return rend_(basic_tx_aware_tree::shared_lock_ptr(tree_));
}

auto basic_tx_aware_tree::tx_object::begin_(const basic_tx_aware_tree::shared_lock_ptr& tree) const -> iterator {
  assert(tree.owns_lock() && tree.mutex() == tree_);
  return iterator(this->shared_from_this(this), ops::begin(tree));
}

auto basic_tx_aware_tree::tx_object::end_(const basic_tx_aware_tree::shared_lock_ptr& tree) const -> iterator {
  assert(tree.owns_lock() && tree.mutex() == tree_);
  return iterator(this->shared_from_this(this), ops::end(tree), true);
}

auto basic_tx_aware_tree::tx_object::rbegin_(const basic_tx_aware_tree::shared_lock_ptr& tree) const -> reverse_iterator {
  assert(tree.owns_lock() && tree.mutex() == tree_);
  return reverse_iterator(this->shared_from_this(this), ops::rbegin(tree));
}

auto basic_tx_aware_tree::tx_object::rend_(const basic_tx_aware_tree::shared_lock_ptr& tree) const -> reverse_iterator {
  assert(tree.owns_lock() && tree.mutex() == tree_);
  return reverse_iterator(this->shared_from_this(this), ops::rend(tree), true);
}


} /* namespace earnest::detail::tree */
