#include "UnitTest++/UnitTest++.h"
#include "print.h"
#include <earnest/fd.h>
#include <earnest/db.h>
#include <earnest/detail/tree/tree.h>
#include <earnest/detail/tree/loader_impl.h>
#include <earnest/detail/tree/leaf_iterator.h>
#include <boost/asio/buffer.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/write_at.hpp>
#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>
#include <boost/endian/conversion.hpp>
#include <cycle_ptr/cycle_ptr.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <memory>
#include <vector>
#include <algorithm>

namespace tree = ::earnest::detail::tree;


struct string_value_type {
  static const std::size_t SIZE = 4;

  string_value_type() = default;
  string_value_type(std::string value) : value(std::move(value)) {}

  void encode(boost::asio::mutable_buffer buf) const {
    assert(buf.size() >= SIZE);
    std::memset(buf.data(), 0, buf.size());
    boost::asio::buffer_copy(boost::asio::buffer(buf, SIZE), boost::asio::buffer(value));
  }

  void decode(boost::asio::const_buffer buf) {
    std::string_view str_buf(reinterpret_cast<const char*>(buf.data()), buf.size());
    value.assign(
        str_buf.begin(),
        std::find(str_buf.begin(), str_buf.end(), '\0'));
  }

  auto operator<(const string_value_type& y) const -> bool {
    return value < y.value;
  }

  std::string value;
};


class db_fixture {
  public:
  db_fixture()
  : io_context(),
    db(earnest::db::create("test", tmpfile_fd_(this->io_context.get_executor(), "tree-test")))
  {}

  private:
  static auto tmpfile_fd_(earnest::fd::executor_type x, std::string file) -> earnest::fd {
    earnest::fd f(x);
    f.tmpfile(file);
    return f;
  }

  protected:
  auto txfile_begin(bool read_only = true) -> earnest::txfile::transaction {
    class db_obj_exposition
    : public earnest::db::db_obj
    {
      public:
      explicit db_obj_exposition(std::shared_ptr<earnest::db> db)
      : earnest::db::db_obj(std::move(db))
      {}

      using earnest::db::db_obj::txfile_begin;
    };

    return db_obj_exposition(db).txfile_begin(read_only);
  }

  auto txfile_begin() const -> earnest::txfile::transaction {
    class db_obj_exposition
    : public earnest::db::db_obj
    {
      public:
      explicit db_obj_exposition(std::shared_ptr<earnest::db> db)
      : earnest::db::db_obj(std::move(db))
      {}

      using earnest::db::db_obj::txfile_begin;
    };

    return db_obj_exposition(db).txfile_begin();
  }

  auto offset_advance(std::uint64_t bytes) -> std::uint64_t {
    std::lock_guard<std::mutex> lck(offset_mtx_);

    auto tx = txfile_begin(false);
    if (tx.size() < offset_ + bytes) tx.resize(offset_ + bytes);
    tx.commit();

    return std::exchange(offset_, offset_ + bytes);
  }

  boost::asio::io_context io_context;
  const std::shared_ptr<earnest::db> db;

  private:
  std::uint64_t offset_ = earnest::db::DB_HEADER_SIZE;
  std::mutex offset_mtx_;
};


class tree_fixture
: public db_fixture
{
  public:
  tree_fixture()
  : db_fixture(),
    tree(tree::basic_tree::create(this->db, this->offset_advance(tree::basic_tree::SIZE), mk_loader_(), 4, 4))
  {}

  private:
  class mock_loader
  : public tree::tx_aware_loader<string_value_type, string_value_type>
  {
    public:
    auto allocate_disk_space(earnest::txfile::transaction& tx, std::size_t bytes) const -> offset_type override {
      REQUIRE CHECK(false);
      return 0;
    }
  };

  static auto mk_loader_() -> std::shared_ptr<tree::loader> {
    return std::make_shared<mock_loader>();
  }

  protected:
  using loader_value_type = mock_loader::value_type;
  using loader_key_type = mock_loader::key_type;

  auto leaf_bytes() const -> std::size_t {
    return tree::leaf::header::SIZE + tree->cfg->key_bytes + tree->cfg->items_per_leaf_page * tree->cfg->val_bytes;
  }

  auto branch_bytes() const -> std::size_t {
    return tree::branch::header::SIZE
        + tree->cfg->items_per_node_page * (sizeof(std::uint64_t) + tree->cfg->augment_bytes)
        + (tree->cfg->items_per_node_page - 1u) * tree->cfg->key_bytes;
  }

  auto read_all_from_leaf(const tree::leaf::shared_lock_ptr& leaf) const -> std::vector<std::string> {
    std::vector<std::string> result;
    std::transform(
        tree::leaf::begin(tree, leaf), tree::leaf::end(tree, leaf),
        std::back_inserter(result),
        [](const earnest::detail::tree::value_type& value_ref) -> const std::string& {
          auto ptr = boost::polymorphic_downcast<const mock_loader::value_type*>(&value_ref);
          REQUIRE CHECK(ptr != nullptr);
          return ptr->value.value;
        });
    return result;
  }

  auto read_all_from_leaf(const tree::leaf::unique_lock_ptr& leaf) const -> std::vector<std::string> {
    std::vector<std::string> result;
    std::transform(
        tree::leaf::begin(tree, leaf), tree::leaf::end(tree, leaf),
        std::back_inserter(result),
        [](const earnest::detail::tree::value_type& value_ref) -> const std::string& {
          auto ptr = boost::polymorphic_downcast<const mock_loader::value_type*>(&value_ref);
          REQUIRE CHECK(ptr != nullptr);
          return ptr->value.value;
        });
    return result;
  }

  auto branch_keys(const tree::branch::shared_lock_ptr& branch) const -> std::vector<std::string> {
    std::vector<std::string> result;
    std::transform(
        branch->keys().begin(), branch->keys().end(),
        std::back_inserter(result),
        [](const auto& key_ptr) -> const std::string& {
          return boost::polymorphic_downcast<const loader_key_type*>(key_ptr.get())->key.value;
        });
    return result;
  }

  auto branch_keys(const tree::branch::unique_lock_ptr& branch) const -> std::vector<std::string> {
    std::vector<std::string> result;
    std::transform(
        branch->keys().begin(), branch->keys().end(),
        std::back_inserter(result),
        [](const auto& key_ptr) -> const std::string& {
          return boost::polymorphic_downcast<const loader_key_type*>(key_ptr.get())->key.value;
        });
    return result;
  }

  auto branch_offsets(const tree::branch::shared_lock_ptr& branch) const -> std::vector<std::uint64_t> {
    std::vector<std::uint64_t> result;
    std::transform(
        branch->pages().begin(), branch->pages().end(),
        std::back_inserter(result),
        [](const auto& augmented_page_ref_ptr) -> std::uint64_t {
          return augmented_page_ref_ptr->offset();
        });
    return result;
  }

  auto branch_offsets(const tree::branch::unique_lock_ptr& branch) const -> std::vector<std::uint64_t> {
    std::vector<std::uint64_t> result;
    std::transform(
        branch->pages().begin(), branch->pages().end(),
        std::back_inserter(result),
        [](const auto& augmented_page_ref_ptr) -> std::uint64_t {
          return augmented_page_ref_ptr->offset();
        });
    return result;
  }

  auto write_empty_leaf(cycle_ptr::cycle_gptr<const loader_key_type> key = nullptr) -> std::uint64_t {
    const auto offset = offset_advance(leaf_bytes());

    auto tx = txfile_begin(false);
    assert(tx.size() >= offset + leaf_bytes());
    std::vector<std::uint8_t> buf(leaf_bytes(), std::uint8_t(0));
    tree::leaf::header{ tree::leaf::magic, (key ? tree::leaf::header::flag_has_key : 0u), 0u, 0u, 0u }.encode(boost::asio::buffer(buf));
    if (key) key->encode(boost::asio::buffer(buf) + tree::leaf::header::SIZE);
    boost::asio::write_at(tx, offset, boost::asio::buffer(buf));
    tx.commit();

    return offset;
  }

  auto write_empty_branch() -> std::uint64_t {
    const auto offset = offset_advance(branch_bytes());

    auto tx = txfile_begin(false);
    assert(tx.size() >= offset + leaf_bytes());
    std::vector<std::uint8_t> buf(branch_bytes(), std::uint8_t(0));
    tree::branch::header{ tree::branch::magic, 0u, 0u }.encode(boost::asio::buffer(buf));
    boost::asio::write_at(tx, offset, boost::asio::buffer(buf));
    tx.commit();

    return offset;
  }

  auto write_leaf(std::initializer_list<cycle_ptr::cycle_gptr<loader_value_type>> elems, cycle_ptr::cycle_gptr<const loader_key_type> key = nullptr) -> std::uint64_t {
    const auto offset = write_empty_leaf(key);

    tree::leaf::unique_lock_ptr leaf(load_page_from_disk<tree::leaf>(offset));
    std::for_each(
        elems.begin(), elems.end(),
        [&leaf, this](cycle_ptr::cycle_gptr<loader_value_type> elem) {
          auto tx = txfile_begin(false);
          tree::leaf::link(leaf, elem, nullptr, tx);
          tx.commit();
        });

    return offset;
  }

  auto write_branch(std::initializer_list<cycle_ptr::cycle_gptr<const loader_key_type>> keys) -> std::uint64_t {
    const auto offset = offset_advance(branch_bytes());

    auto tx = txfile_begin(false);
    assert(tx.size() >= offset + leaf_bytes());
    std::vector<std::uint8_t> buf(branch_bytes(), std::uint8_t(0));
    assert(keys.size() < 0xffff'ffffu);
    tree::branch::header{ tree::branch::magic, static_cast<std::uint32_t>(keys.size() + 1u), 0u }.encode(boost::asio::buffer(buf));

    auto wbuf = boost::asio::buffer(buf) + tree::branch::header::SIZE;
    {
      std::uint64_t child_offset = write_empty_leaf();
      boost::endian::native_to_big_inplace(child_offset);
      boost::asio::buffer_copy(
          wbuf,
          boost::asio::buffer(&child_offset, sizeof(child_offset)));
      wbuf += sizeof(child_offset);
    }
    std::for_each(
        keys.begin(), keys.end(),
        [&wbuf, this](cycle_ptr::cycle_gptr<const loader_key_type> key) {
          key->encode(wbuf);
          wbuf += tree->cfg->key_bytes;

          std::uint64_t child_offset = write_empty_leaf(key);
          boost::endian::native_to_big_inplace(child_offset);
          boost::asio::buffer_copy(
              wbuf,
              boost::asio::buffer(&child_offset, sizeof(child_offset)));
          wbuf += sizeof(child_offset);
        });

    boost::asio::write_at(tx, offset, boost::asio::buffer(buf));
    tx.commit();

    return offset;
  }

  private:
  auto load_page_from_disk_(std::uint64_t offset) const -> cycle_ptr::cycle_gptr<tree::abstract_page> {
    auto tx = txfile_begin();
    boost::system::error_code ec;
    auto page_ptr = tree::abstract_page::decode(*tree->loader, tree->cfg, tx, offset, {}, ec);
    if (ec) throw boost::system::error_code(ec);
    tx.commit();
    return page_ptr;
  }

  protected:
  template<typename Page>
  auto load_page_from_disk(std::uint64_t offset) const
  -> std::enable_if_t<std::is_base_of_v<tree::abstract_page, std::remove_const_t<Page>>, cycle_ptr::cycle_gptr<Page>> {
    if constexpr(std::is_same_v<tree::abstract_page, std::remove_const_t<Page>>)
      return load_page_from_disk_(offset);
    else
      return boost::polymorphic_pointer_downcast<Page>(load_page_from_disk_(offset));
  }

  const cycle_ptr::cycle_gptr<tree::basic_tree> tree;
};


SUITE(leaf) {

  TEST_FIXTURE(tree_fixture, empty_page) {
    const auto offset = write_empty_leaf();
    tree::leaf::shared_lock_ptr leaf(load_page_from_disk<tree::leaf>(offset));

    CHECK(tree::leaf::valid(leaf));
    CHECK_EQUAL(tree->cfg->items_per_leaf_page, leaf->max_size());
    CHECK_EQUAL(0, tree::leaf::size(leaf));
    CHECK_EQUAL(std::vector<std::string>(), read_all_from_leaf(leaf));
    CHECK_EQUAL(cycle_ptr::cycle_gptr<const earnest::detail::tree::key_type>(nullptr), leaf->key());
  }

  TEST_FIXTURE(tree_fixture, append_first_element) {
    const auto offset = write_empty_leaf();
    tree::leaf::unique_lock_ptr leaf(load_page_from_disk<tree::leaf>(offset));
    const auto new_elem = cycle_ptr::allocate_cycle<loader_value_type>(leaf->get_allocator(), string_value_type("val"));

    auto tx = txfile_begin(false);
    tree::leaf::link(leaf, new_elem, nullptr, tx);

    // Element doesn't appear until commit is called.
    CHECK_EQUAL(0, tree::leaf::size(leaf));
    CHECK_EQUAL(std::vector<std::string>(), read_all_from_leaf(leaf));

    // Nothing is written to disk yet.
    {
      tree::leaf::shared_lock_ptr uncached_leaf(load_page_from_disk<tree::leaf>(offset));
      CHECK_EQUAL(0, tree::leaf::size(uncached_leaf));
      CHECK_EQUAL(std::vector<std::string>(), read_all_from_leaf(uncached_leaf));
    }

    tx.commit();
    CHECK_EQUAL(1, tree::leaf::size(leaf));
    CHECK_EQUAL(std::vector<std::string>{ "val" }, read_all_from_leaf(leaf));

    // Key of the page is unchanged.
    CHECK_EQUAL(cycle_ptr::cycle_gptr<const earnest::detail::tree::key_type>(nullptr), leaf->key());

    // Changes have been committed to disk.
    {
      tree::leaf::shared_lock_ptr uncached_leaf(load_page_from_disk<tree::leaf>(offset));
      CHECK_EQUAL(1, tree::leaf::size(uncached_leaf));
      CHECK_EQUAL(std::vector<std::string>{ "val" }, read_all_from_leaf(uncached_leaf));
    }
  }

  TEST_FIXTURE(tree_fixture, append_many_elements) {
    const std::vector<std::string> elems{ "1", "2", "3", "4" };
    const auto offset = write_empty_leaf();
    tree::leaf::unique_lock_ptr leaf(load_page_from_disk<tree::leaf>(offset));

    for (const auto& new_elem : elems) {
      auto tx = txfile_begin(false);
      tree::leaf::link(leaf, cycle_ptr::allocate_cycle<loader_value_type>(leaf->get_allocator(), string_value_type(new_elem)), nullptr, tx);
      tx.commit();
    }

    CHECK_EQUAL(elems.size(), tree::leaf::size(leaf));
    CHECK_EQUAL(elems, read_all_from_leaf(leaf));

    // Changes have been committed to disk.
    {
      tree::leaf::shared_lock_ptr uncached_leaf(load_page_from_disk<tree::leaf>(offset));
      CHECK_EQUAL(elems.size(), tree::leaf::size(uncached_leaf));
      CHECK_EQUAL(elems, read_all_from_leaf(uncached_leaf));
    }
  }

  TEST_FIXTURE(tree_fixture, erase_front) {
    auto key1 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("1"));
    auto key2 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("2"));
    auto key3 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("3"));
    auto key4 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("4"));
    const auto offset = write_leaf({ key1, key2, key3, key4 });

    tree::leaf::unique_lock_ptr leaf(load_page_from_disk<tree::leaf>(offset));
    auto tx = txfile_begin(false);
    tree::leaf::unlink(leaf, key1, tx);

    // Unlink operation won't take effect until commited.
    CHECK_EQUAL(4, tree::leaf::size(leaf));
    CHECK_EQUAL((std::vector<std::string>{ "1", "2", "3", "4" }), read_all_from_leaf(leaf));

    // Nothing is written to disk yet.
    {
      tree::leaf::shared_lock_ptr uncached_leaf(load_page_from_disk<tree::leaf>(offset));
      CHECK_EQUAL(4, tree::leaf::size(uncached_leaf));
      CHECK_EQUAL((std::vector<std::string>{ "1", "2", "3", "4" }), read_all_from_leaf(uncached_leaf));
    }

    tx.commit();

    // key1 was deleted.
    CHECK_EQUAL(3, tree::leaf::size(leaf));
    CHECK_EQUAL((std::vector<std::string>{ "2", "3", "4" }), read_all_from_leaf(leaf));

    // Changes have been committed to disk.
    {
      tree::leaf::shared_lock_ptr uncached_leaf(load_page_from_disk<tree::leaf>(offset));
      CHECK_EQUAL(3, tree::leaf::size(uncached_leaf));
      CHECK_EQUAL((std::vector<std::string>{ "2", "3", "4" }), read_all_from_leaf(uncached_leaf));
    }
  }

  TEST_FIXTURE(tree_fixture, erase_middle) {
    auto key1 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("1"));
    auto key2 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("2"));
    auto key3 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("3"));
    auto key4 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("4"));
    const auto offset = write_leaf({ key1, key2, key3, key4 });

    tree::leaf::unique_lock_ptr leaf(load_page_from_disk<tree::leaf>(offset));
    auto tx = txfile_begin(false);
    tree::leaf::unlink(leaf, key2, tx);

    // Unlink operation won't take effect until commited.
    CHECK_EQUAL(4, tree::leaf::size(leaf));
    CHECK_EQUAL((std::vector<std::string>{ "1", "2", "3", "4" }), read_all_from_leaf(leaf));

    // Nothing is written to disk yet.
    {
      tree::leaf::shared_lock_ptr uncached_leaf(load_page_from_disk<tree::leaf>(offset));
      CHECK_EQUAL(4, tree::leaf::size(uncached_leaf));
      CHECK_EQUAL((std::vector<std::string>{ "1", "2", "3", "4" }), read_all_from_leaf(uncached_leaf));
    }

    tx.commit();

    // key2 was deleted.
    CHECK_EQUAL(3, tree::leaf::size(leaf));
    CHECK_EQUAL((std::vector<std::string>{ "1", "3", "4" }), read_all_from_leaf(leaf));

    // Changes have been committed to disk.
    {
      tree::leaf::shared_lock_ptr uncached_leaf(load_page_from_disk<tree::leaf>(offset));
      CHECK_EQUAL(3, tree::leaf::size(uncached_leaf));
      CHECK_EQUAL((std::vector<std::string>{ "1", "3", "4" }), read_all_from_leaf(uncached_leaf));
    }
  }

  TEST_FIXTURE(tree_fixture, erase_back) {
    auto key1 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("1"));
    auto key2 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("2"));
    auto key3 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("3"));
    auto key4 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("4"));
    const auto offset = write_leaf({ key1, key2, key3, key4 });

    tree::leaf::unique_lock_ptr leaf(load_page_from_disk<tree::leaf>(offset));
    auto tx = txfile_begin(false);
    tree::leaf::unlink(leaf, key4, tx);

    // Unlink operation won't take effect until commited.
    CHECK_EQUAL(4, tree::leaf::size(leaf));
    CHECK_EQUAL((std::vector<std::string>{ "1", "2", "3", "4" }), read_all_from_leaf(leaf));

    // Nothing is written to disk yet.
    {
      tree::leaf::shared_lock_ptr uncached_leaf(load_page_from_disk<tree::leaf>(offset));
      CHECK_EQUAL(4, tree::leaf::size(uncached_leaf));
      CHECK_EQUAL((std::vector<std::string>{ "1", "2", "3", "4" }), read_all_from_leaf(uncached_leaf));
    }

    tx.commit();

    // key4 was deleted.
    CHECK_EQUAL(3, tree::leaf::size(leaf));
    CHECK_EQUAL((std::vector<std::string>{ "1", "2", "3" }), read_all_from_leaf(leaf));

    // Changes have been committed to disk.
    {
      tree::leaf::shared_lock_ptr uncached_leaf(load_page_from_disk<tree::leaf>(offset));
      CHECK_EQUAL(3, tree::leaf::size(uncached_leaf));
      CHECK_EQUAL((std::vector<std::string>{ "1", "2", "3" }), read_all_from_leaf(uncached_leaf));
    }
  }

  TEST_FIXTURE(tree_fixture, merge) {
    auto key1 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("1"));
    auto key2 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("2"));
    auto key3 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("3"));
    auto key4 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("4"));
    const auto first_offset = write_leaf({ key1, key2 });
    const auto second_offset = write_leaf({ key3, key4 }, cycle_ptr::make_cycle<loader_key_type>(string_value_type("3")));

    tree::leaf::unique_lock_ptr first(load_page_from_disk<tree::leaf>(first_offset));
    tree::leaf::unique_lock_ptr second(load_page_from_disk<tree::leaf>(second_offset));
    auto tx = txfile_begin(false);
    tree::leaf::merge(tree, first, second, tx);

    // No change is seen until transaction commit.
    CHECK_EQUAL(2, tree::leaf::size(first));
    CHECK_EQUAL(2, tree::leaf::size(second));
    CHECK_EQUAL((std::vector<std::string>{ "1", "2" }), read_all_from_leaf(first));
    CHECK_EQUAL((std::vector<std::string>{ "3", "4" }), read_all_from_leaf(second));

    // Nothing is written to disk yet.
    {
      tree::leaf::shared_lock_ptr uncached_first(load_page_from_disk<tree::leaf>(first_offset));
      tree::leaf::shared_lock_ptr uncached_second(load_page_from_disk<tree::leaf>(second_offset));
      CHECK_EQUAL(2, tree::leaf::size(uncached_first));
      CHECK_EQUAL(2, tree::leaf::size(uncached_second));
      CHECK_EQUAL((std::vector<std::string>{ "1", "2" }), read_all_from_leaf(uncached_first));
      CHECK_EQUAL((std::vector<std::string>{ "3", "4" }), read_all_from_leaf(uncached_second));
    }

    tx.commit();

    // Everything was merged into the first page.
    CHECK_EQUAL(4, tree::leaf::size(first));
    CHECK_EQUAL(0, tree::leaf::size(second));
    CHECK_EQUAL((std::vector<std::string>{ "1", "2", "3", "4" }), read_all_from_leaf(first));
    CHECK_EQUAL(std::vector<std::string>(), read_all_from_leaf(second));

    CHECK(tree::leaf::valid(first)); // First page remains valid.
    CHECK(!tree::leaf::valid(second)); // Second page is no longer valid.

    // Changes have been committed to disk.
    {
      tree::leaf::shared_lock_ptr uncached_first(load_page_from_disk<tree::leaf>(first_offset));
      tree::leaf::shared_lock_ptr uncached_second(load_page_from_disk<tree::leaf>(second_offset));
      CHECK_EQUAL(4, tree::leaf::size(uncached_first));
      CHECK_EQUAL(0, tree::leaf::size(uncached_second));
      CHECK_EQUAL((std::vector<std::string>{ "1", "2", "3", "4" }), read_all_from_leaf(uncached_first));
      CHECK_EQUAL(std::vector<std::string>(), read_all_from_leaf(uncached_second));
    }
  }

  TEST_FIXTURE(tree_fixture, split) {
    auto key1 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("1"));
    auto key2 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("2"));
    auto key3 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("3"));
    auto key4 = cycle_ptr::make_cycle<loader_value_type>(string_value_type("4"));
    const auto first_offset = write_leaf({ key1, key2, key3, key4 });
    const auto second_offset = write_empty_leaf();

    tree::leaf::unique_lock_ptr first(load_page_from_disk<tree::leaf>(first_offset));
    tree::leaf::unique_lock_ptr second(load_page_from_disk<tree::leaf>(second_offset));
    auto tx = txfile_begin(false);
    tree::leaf::split(tree, first, second, tx);

    // No change is seen until transaction commit.
    CHECK_EQUAL(4, tree::leaf::size(first));
    CHECK_EQUAL(0, tree::leaf::size(second));
    CHECK_EQUAL((std::vector<std::string>{ "1", "2", "3", "4" }), read_all_from_leaf(first));
    CHECK_EQUAL(std::vector<std::string>(), read_all_from_leaf(second));

    // Nothing is written to disk yet.
    {
      tree::leaf::shared_lock_ptr uncached_first(load_page_from_disk<tree::leaf>(first_offset));
      tree::leaf::shared_lock_ptr uncached_second(load_page_from_disk<tree::leaf>(second_offset));
      CHECK_EQUAL(4, tree::leaf::size(uncached_first));
      CHECK_EQUAL(0, tree::leaf::size(uncached_second));
      CHECK_EQUAL((std::vector<std::string>{ "1", "2", "3", "4" }), read_all_from_leaf(uncached_first));
      CHECK_EQUAL(std::vector<std::string>(), read_all_from_leaf(uncached_second));
    }

    tx.commit();

    // Some elements from the first page are now in the second page.
    CHECK_EQUAL(2, tree::leaf::size(first));
    CHECK_EQUAL(2, tree::leaf::size(second));
    CHECK_EQUAL((std::vector<std::string>{ "1", "2", "3", "4" }), read_all_from_leaf(first));
    CHECK_EQUAL(std::vector<std::string>(), read_all_from_leaf(second));

    // Key has been set.
    CHECK_EQUAL("3", boost::polymorphic_pointer_downcast<const loader_key_type>(second->key())->key.value);

    // Changes have been committed to disk.
    {
      tree::leaf::shared_lock_ptr uncached_first(load_page_from_disk<tree::leaf>(first_offset));
      tree::leaf::shared_lock_ptr uncached_second(load_page_from_disk<tree::leaf>(second_offset));
      CHECK_EQUAL(2, tree::leaf::size(uncached_first));
      CHECK_EQUAL(2, tree::leaf::size(uncached_second));
      CHECK_EQUAL((std::vector<std::string>{ "1", "2" }), read_all_from_leaf(uncached_first));
      CHECK_EQUAL((std::vector<std::string>{ "3", "4" }), read_all_from_leaf(uncached_second));

      // Key has been set.
      CHECK_EQUAL("3", boost::polymorphic_pointer_downcast<const loader_key_type>(uncached_second->key())->key.value);
    }
  }

} /* SUITE(leaf) */


SUITE(branch) {

  TEST_FIXTURE(tree_fixture, empty_page) {
    const auto offset = write_empty_branch();
    tree::branch::shared_lock_ptr branch(load_page_from_disk<tree::branch>(offset));

    CHECK(tree::branch::valid(branch));
    CHECK_EQUAL(0, tree::branch::size(branch));
    CHECK_EQUAL(4, branch->max_size());
  }

  TEST_FIXTURE(tree_fixture, single_element) {
    const auto offset = write_branch({});
    tree::branch::shared_lock_ptr branch(load_page_from_disk<tree::branch>(offset));

    CHECK(tree::branch::valid(branch));
    CHECK_EQUAL(1, tree::branch::size(branch));
    CHECK_EQUAL(std::vector<std::string>(), branch_keys(branch));
  }

  TEST_FIXTURE(tree_fixture, merge) {
    const auto first_offset = write_branch({ cycle_ptr::make_cycle<loader_key_type>(string_value_type("k1")) });
    const auto second_offset = write_branch({ cycle_ptr::make_cycle<loader_key_type>(string_value_type("k3")) });
    const auto separator_key = cycle_ptr::make_cycle<loader_key_type>(string_value_type("k2"));
    tree::branch::unique_lock_ptr first(load_page_from_disk<tree::branch>(first_offset));
    tree::branch::unique_lock_ptr second(load_page_from_disk<tree::branch>(second_offset));

    REQUIRE CHECK_EQUAL(2, tree::branch::size(first));
    REQUIRE CHECK_EQUAL(2, tree::branch::size(second));

    std::vector<std::uint64_t> child_offsets;
    for (const auto& offset : branch_offsets(first)) child_offsets.push_back(offset);
    for (const auto& offset : branch_offsets(second)) child_offsets.push_back(offset);

    auto tx = txfile_begin(false);
    tree::branch::merge(tree, first, second, tx, separator_key);

    // No change until transaction commit.
    CHECK_EQUAL(2, tree::branch::size(first));
    CHECK_EQUAL(2, tree::branch::size(second));
    CHECK_EQUAL((std::vector<std::string>{ "k1" }), branch_keys(first));
    CHECK_EQUAL((std::vector<std::string>{ "k3" }), branch_keys(second));
    CHECK_EQUAL(
        std::vector<std::uint64_t>(child_offsets.begin(), child_offsets.begin() + 2),
        branch_offsets(first));
    CHECK_EQUAL(
        std::vector<std::uint64_t>(child_offsets.end() - 2, child_offsets.end()),
        branch_offsets(second));

    // Nothing is written to disk yet.
    {
      tree::branch::shared_lock_ptr uncached_first(load_page_from_disk<tree::branch>(first_offset));
      CHECK_EQUAL(2, tree::branch::size(uncached_first));
      CHECK_EQUAL((std::vector<std::string>{ "k1" }), branch_keys(uncached_first));
      CHECK_EQUAL(
          std::vector<std::uint64_t>(child_offsets.begin(), child_offsets.begin() + 2),
          branch_offsets(uncached_first));

      tree::branch::shared_lock_ptr uncached_second(load_page_from_disk<tree::branch>(second_offset));
      CHECK_EQUAL(2, tree::branch::size(uncached_second));
      CHECK_EQUAL((std::vector<std::string>{ "k3" }), branch_keys(uncached_second));
      CHECK_EQUAL(
          std::vector<std::uint64_t>(child_offsets.end() - 2, child_offsets.end()),
          branch_offsets(uncached_second));
    }

    tx.commit();

    // All elements have moved to the first page.
    CHECK_EQUAL((std::vector<std::string>{ "k1", "k2", "k3" }), branch_keys(first));
    CHECK_EQUAL(std::vector<std::string>(), branch_keys(second));
    CHECK_EQUAL(child_offsets, branch_offsets(first));
    CHECK_EQUAL(std::vector<std::uint64_t>(), branch_offsets(second));

    CHECK(tree::branch::valid(first)); // First page remains valid.
    CHECK(!tree::branch::valid(second)); // Second page is no longer valid.

    // Changes have been committed to disk.
    {
      tree::branch::shared_lock_ptr uncached_first(load_page_from_disk<tree::branch>(first_offset));
      CHECK_EQUAL(4, tree::branch::size(uncached_first));
      CHECK_EQUAL((std::vector<std::string>{ "k1", "k2", "k3" }), branch_keys(uncached_first));
      CHECK_EQUAL(child_offsets, branch_offsets(uncached_first));

      tree::branch::shared_lock_ptr uncached_second(load_page_from_disk<tree::branch>(second_offset));
      CHECK_EQUAL(0, tree::branch::size(uncached_second));
      CHECK_EQUAL(std::vector<std::string>(), branch_keys(uncached_second));
      CHECK_EQUAL(std::vector<std::uint64_t>(), branch_offsets(uncached_second));
    }
  }

  TEST_FIXTURE(tree_fixture, split) {
    const auto second_offset = write_empty_branch();
    const auto first_offset = write_branch({ cycle_ptr::make_cycle<loader_key_type>(string_value_type("k1")), cycle_ptr::make_cycle<loader_key_type>(string_value_type("k2")) });
    tree::branch::unique_lock_ptr first(load_page_from_disk<tree::branch>(first_offset));
    tree::branch::unique_lock_ptr second(load_page_from_disk<tree::branch>(second_offset));

    const auto child_offsets = branch_offsets(first);
    REQUIRE CHECK_EQUAL(4, child_offsets.size());
    REQUIRE CHECK_EQUAL(4, tree::branch::size(first));
    REQUIRE CHECK_EQUAL(0, tree::branch::size(second));
    REQUIRE CHECK_EQUAL(std::vector<std::uint64_t>(), branch_offsets(second));

    auto tx = txfile_begin(false);
    const auto split_key = tree::branch::split(tree, first, second, tx);

    CHECK_EQUAL("k2", boost::polymorphic_pointer_downcast<const loader_key_type>(split_key)->key.value);

    // No change until transaction commit.
    CHECK_EQUAL((std::vector<std::string>{ "k1", "k2", "k3" }), branch_keys(first));
    CHECK_EQUAL(std::vector<std::string>(), branch_keys(second));

    // Nothing is written to disk yet.
    {
      tree::branch::shared_lock_ptr uncached_first(load_page_from_disk<tree::branch>(first_offset));
      CHECK_EQUAL(4, tree::branch::size(uncached_first));
      CHECK_EQUAL((std::vector<std::string>{ "k1", "k2", "k3" }), branch_keys(uncached_first));
      CHECK_EQUAL(child_offsets, branch_offsets(uncached_first));

      tree::branch::shared_lock_ptr uncached_second(load_page_from_disk<tree::branch>(second_offset));
      CHECK_EQUAL(0, tree::branch::size(uncached_second));
      CHECK_EQUAL(std::vector<std::string>(), branch_keys(uncached_second));
      CHECK_EQUAL(std::vector<std::uint64_t>(), branch_offsets(uncached_second));
    }

    tx.commit();

    // Some elements from the first page are now in the second page.
    CHECK_EQUAL(2, tree::branch::size(first));
    CHECK_EQUAL(2, tree::branch::size(second));
    CHECK_EQUAL((std::vector<std::string>{ "k1" }), branch_keys(first));
    CHECK_EQUAL((std::vector<std::string>{ "k3" }), branch_keys(second));
    CHECK_EQUAL(
        std::vector<std::uint64_t>(child_offsets.begin(), child_offsets.begin() + 2),
        branch_offsets(first));
    CHECK_EQUAL(
        std::vector<std::uint64_t>(child_offsets.end() - 2, child_offsets.end()),
        branch_offsets(second));

    // Changes have been committed to disk.
    {
      tree::branch::shared_lock_ptr uncached_first(load_page_from_disk<tree::branch>(first_offset));
      CHECK_EQUAL(2, tree::branch::size(uncached_first));
      CHECK_EQUAL((std::vector<std::string>{ "k1" }), branch_keys(uncached_first));
      CHECK_EQUAL(
          std::vector<std::uint64_t>(child_offsets.begin(), child_offsets.begin() + 2),
          branch_offsets(uncached_first));

      tree::branch::shared_lock_ptr uncached_second(load_page_from_disk<tree::branch>(second_offset));
      CHECK_EQUAL(2, tree::branch::size(uncached_second));
      CHECK_EQUAL((std::vector<std::string>{ "k3" }), branch_keys(uncached_second));
      CHECK_EQUAL(
          std::vector<std::uint64_t>(child_offsets.end() - 2, child_offsets.end()),
          branch_offsets(uncached_second));
    }
  }

} /* SUITE(branch) */


int main() {
  return UnitTest::RunAllTests();
}
