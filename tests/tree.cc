#include "UnitTest++/UnitTest++.h"
#include "print.h"
#include <earnest/detail/tree/leaf.h>
#include <earnest/detail/tree/tree.h>
#include <earnest/detail/tree/value_type.h>
#include <earnest/detail/tree/key_type.h>
#include <earnest/detail/tree/augmented_page_ref.h>
#include <earnest/detail/tree/loader_impl.h>
#include <earnest/detail/tree/leaf_iterator.h>
#include <earnest/fd.h>
#include <earnest/txfile.h>
#include <boost/polymorphic_cast.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/write_at.hpp>
#include <array>
#include <cstring>
#include <initializer_list>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <vector>

using earnest::detail::tree::leaf;

struct fixture {
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

  static constexpr std::size_t items_per_leaf_page = 4;

  fixture()
  : io_context(),
    file(earnest::txfile::create("tree_leaf", tmpfile_fd_(this->io_context.get_executor(), "tree_leaf"), 0u, 1u << 20)),
    cfg(mk_cfg_())
  {}

  auto leaf_bytes() const -> std::size_t {
    return leaf::header::SIZE + cfg->key_bytes + cfg->items_per_leaf_page * cfg->val_bytes;
  }

  auto load_leaf(earnest::fd::offset_type offset) -> cycle_ptr::cycle_gptr<leaf> {
    auto tx = file.begin();

    boost::system::error_code ec;
    auto page_ptr = earnest::detail::tree::abstract_page::decode(*loader, cfg, tx, offset, earnest::shared_resource_allocator<void>(), ec);
    REQUIRE CHECK(!ec);
    REQUIRE CHECK(page_ptr != nullptr);

    REQUIRE CHECK(std::dynamic_pointer_cast<leaf>(page_ptr) != nullptr);
    return std::dynamic_pointer_cast<leaf>(page_ptr);
  }

  auto write_empty_leaf(earnest::fd::offset_type offset) -> cycle_ptr::cycle_gptr<leaf> {
    auto tx = file.begin(false);
    if (tx.size() < offset + leaf_bytes()) tx.resize(offset + leaf_bytes());
    auto magic = leaf::magic;
    boost::endian::native_to_big_inplace(magic);
    boost::asio::write_at(tx, offset, boost::asio::buffer(&magic, sizeof(magic)));
    tx.commit();

    return load_leaf(offset);
  }

  template<typename Vector>
  auto write_leaf(earnest::fd::offset_type offset, const Vector& elems) -> cycle_ptr::cycle_gptr<leaf> {
    struct mk_elem_t {
      auto operator()(const std::string& s) const
      -> cycle_ptr::cycle_gptr<mock_loader::value_type> {
        return cycle_ptr::make_cycle<mock_loader::value_type>(s);
      }

      auto operator()(cycle_ptr::cycle_gptr<mock_loader::value_type> elem_ptr) const
      -> cycle_ptr::cycle_gptr<mock_loader::value_type> {
        return elem_ptr;
      }
    };
    mk_elem_t mk_elem;

    leaf::unique_lock_ptr leaf(write_empty_leaf(offset));

    for (const auto& new_elem : elems) {
      auto tx = file.begin(false);
      leaf::link(leaf, mk_elem(new_elem), nullptr, tx);
      tx.commit();
    }

    return leaf.mutex();
  }

  auto read_all_from_leaf(const leaf::shared_lock_ptr& leaf) -> std::vector<std::string> {
    std::vector<std::string> result;
    std::transform(leaf::begin(loader, leaf), leaf::end(loader, leaf), std::back_inserter(result),
        [](const earnest::detail::tree::value_type& value_ref) -> const std::string& {
          auto ptr = boost::polymorphic_downcast<const mock_loader::value_type*>(&value_ref);
          REQUIRE CHECK(ptr != nullptr);
          return ptr->value.value;
        });
    return result;
  }

  auto read_all_from_leaf(const leaf::unique_lock_ptr& leaf) -> std::vector<std::string> {
    std::vector<std::string> result;
    std::transform(leaf::begin(loader, leaf), leaf::end(loader, leaf), std::back_inserter(result),
        [](const earnest::detail::tree::value_type& value_ref) -> const std::string& {
          auto ptr = boost::polymorphic_downcast<const mock_loader::value_type*>(&value_ref);
          REQUIRE CHECK(ptr != nullptr);
          return ptr->value.value;
        });
    return result;
  }

  private:
  static auto tmpfile_fd_(earnest::fd::executor_type x, std::string file) -> earnest::fd {
    earnest::fd f(x);
    f.tmpfile(file);
    return f;
  }

  static auto mk_cfg_() -> std::shared_ptr<earnest::detail::tree::cfg> {
    return std::make_shared<earnest::detail::tree::cfg>(
        earnest::detail::tree::cfg{
          items_per_leaf_page, // items_per_leaf_page
          2, // items_per_node_page
          string_value_type::SIZE, // key_bytes
          string_value_type::SIZE, // val_bytes
          0 // augment_bytes
        });
  }

  protected:
  class mock_loader
  : public earnest::detail::tree::tx_aware_loader<string_value_type, string_value_type>
  {
    public:
    explicit mock_loader(fixture& self) noexcept : self_(self) {}

    auto allocate_disk_space(earnest::txfile::transaction& tx, std::size_t bytes) const -> offset_type override {
      REQUIRE CHECK(false);
      return 0;
    }

    private:
    auto do_load_from_disk(offset_type off, earnest::detail::cheap_fn_ref<cycle_ptr::cycle_gptr<earnest::detail::db_cache::cache_obj>(earnest::detail::db_cache::allocator_type, std::shared_ptr<const earnest::detail::tree::cfg>, const earnest::txfile::transaction&, offset_type)> load) const
    -> cycle_ptr::cycle_gptr<earnest::detail::db_cache::cache_obj> override {
      return load(alloc, self_.cfg, self_.file.begin(), off);
    }

    fixture& self_;
    earnest::detail::db_cache::allocator_type alloc;
  };

  static auto key_type_cast(cycle_ptr::cycle_gptr<const earnest::detail::tree::key_type> key) -> const std::string& {
    return boost::polymorphic_downcast<const mock_loader::key_type*>(key.get())->key.value;
  }

  public:
  boost::asio::io_context io_context;
  earnest::txfile file;
  std::shared_ptr<earnest::detail::tree::cfg> cfg;
  cycle_ptr::cycle_gptr<mock_loader> loader = cycle_ptr::make_cycle<mock_loader>(*this);
};

SUITE(leaf) {

TEST_FIXTURE(fixture, empty_page) {
  auto leaf = leaf::shared_lock_ptr(write_empty_leaf(0));
  CHECK_EQUAL(cfg->items_per_leaf_page, leaf->max_size());
  CHECK_EQUAL(0, leaf::size(leaf));

  CHECK_EQUAL(std::vector<std::string>(), fixture::read_all_from_leaf(leaf));
  CHECK_EQUAL(cycle_ptr::cycle_gptr<const earnest::detail::tree::key_type>(nullptr), leaf->key());
}

TEST_FIXTURE(fixture, append_first_element) {
  const auto new_elem = cycle_ptr::make_cycle<mock_loader::value_type>(string_value_type("val"));

  auto leaf = leaf::unique_lock_ptr(write_empty_leaf(0));

  auto tx = file.begin(false);
  leaf::link(leaf, new_elem, nullptr, tx);

  // Element don't appear until commit is called.
  CHECK_EQUAL(std::vector<std::string>(), fixture::read_all_from_leaf(leaf));
  tx.commit();

  CHECK_EQUAL(
      std::vector<std::string>{ "val" },
      fixture::read_all_from_leaf(leaf));
}

TEST_FIXTURE(fixture, append_many_elements) {
  const std::vector<std::string> elems{ "1", "2", "3", "4" };

  auto leaf = leaf::unique_lock_ptr(write_empty_leaf(0));
  for (const auto& new_elem : elems) {
    auto tx = file.begin(false);
    leaf::link(leaf, cycle_ptr::make_cycle<mock_loader::value_type>(string_value_type(new_elem)), nullptr, tx);
    tx.commit();
  }

  CHECK_EQUAL(elems, fixture::read_all_from_leaf(leaf));
}

TEST_FIXTURE(fixture, read_back_empty) {
  write_empty_leaf(0);

  CHECK_EQUAL(std::vector<std::string>(), fixture::read_all_from_leaf(leaf::shared_lock_ptr(load_leaf(0))));
}

TEST_FIXTURE(fixture, read_back) {
  const std::vector<std::string> elems{ "1", "2", "3", "4" };
  write_leaf(0, elems);

  CHECK_EQUAL(elems, fixture::read_all_from_leaf(leaf::shared_lock_ptr(load_leaf(0))));
}

TEST_FIXTURE(fixture, erase) {
  auto key1 = cycle_ptr::make_cycle<mock_loader::value_type>(string_value_type("1"));
  auto key2 = cycle_ptr::make_cycle<mock_loader::value_type>(string_value_type("2"));
  auto key3 = cycle_ptr::make_cycle<mock_loader::value_type>(string_value_type("3"));
  auto key4 = cycle_ptr::make_cycle<mock_loader::value_type>(string_value_type("4"));

  leaf::unique_lock_ptr leaf(
      write_leaf(0, std::vector<cycle_ptr::cycle_gptr<mock_loader::value_type>>{ key1, key2, key3, key4 }));
  auto tx = file.begin(false);
  leaf::unlink(leaf, key2, tx);

  // Unlink operation won't take effect until commited.
  CHECK_EQUAL((std::vector<std::string>{ "1", "2", "3", "4" }), fixture::read_all_from_leaf(leaf));
  tx.commit();

  // key2 was deleted.
  CHECK_EQUAL((std::vector<std::string>{ "1", "3", "4" }), fixture::read_all_from_leaf(leaf));
}

TEST_FIXTURE(fixture, read_back_erase) {
  {
    auto key1 = cycle_ptr::make_cycle<mock_loader::value_type>(string_value_type("1"));
    auto key2 = cycle_ptr::make_cycle<mock_loader::value_type>(string_value_type("2"));
    auto key3 = cycle_ptr::make_cycle<mock_loader::value_type>(string_value_type("3"));
    auto key4 = cycle_ptr::make_cycle<mock_loader::value_type>(string_value_type("4"));

    leaf::unique_lock_ptr leaf(
        write_leaf(0, std::vector<cycle_ptr::cycle_gptr<mock_loader::value_type>>{ key1, key2, key3, key4 }));
    auto tx = file.begin(false);
    leaf::unlink(leaf, key2, tx);

    // Unlink operation won't take effect until commited.
    CHECK_EQUAL((std::vector<std::string>{ "1", "2", "3", "4" }), fixture::read_all_from_leaf(leaf));
    tx.commit();
  }

  CHECK_EQUAL(
      (std::vector<std::string>{ "1", "3", "4" }),
      fixture::read_all_from_leaf(leaf::shared_lock_ptr(load_leaf(0))));
}

TEST_FIXTURE(fixture, merge) {
  auto key1 = cycle_ptr::make_cycle<mock_loader::value_type>(string_value_type("1"));
  auto key2 = cycle_ptr::make_cycle<mock_loader::value_type>(string_value_type("2"));
  auto key3 = cycle_ptr::make_cycle<mock_loader::value_type>(string_value_type("3"));
  auto key4 = cycle_ptr::make_cycle<mock_loader::value_type>(string_value_type("4"));

  leaf::unique_lock_ptr first(
      write_leaf(0, std::vector<cycle_ptr::cycle_gptr<mock_loader::value_type>>{ key1, key2 }));
  leaf::unique_lock_ptr second(
      write_leaf(leaf_bytes(), std::vector<cycle_ptr::cycle_gptr<mock_loader::value_type>>{ key3, key4 }));

  auto tx = file.begin(false);
  leaf::merge(*loader, first, second, tx);

  // No change is seen until transaction commit.
  CHECK_EQUAL(
      (std::vector<std::string>{ "1", "2" }),
      fixture::read_all_from_leaf(first));
  CHECK_EQUAL(
      (std::vector<std::string>{ "3", "4" }),
      fixture::read_all_from_leaf(second));

  tx.commit();

  CHECK_EQUAL(
      (std::vector<std::string>{ "1", "2", "3", "4" }),
      fixture::read_all_from_leaf(first));
  CHECK_EQUAL(
      std::vector<std::string>(),
      fixture::read_all_from_leaf(second));

  // Read-back test.
  CHECK_EQUAL(
      (std::vector<std::string>{ "1", "2", "3", "4" }),
      fixture::read_all_from_leaf(leaf::shared_lock_ptr(load_leaf(0))));
  CHECK_EQUAL(
      cycle_ptr::cycle_gptr<const earnest::detail::tree::key_type>(nullptr),
      load_leaf(0)->key());

  CHECK_EQUAL(
      std::vector<std::string>(),
      fixture::read_all_from_leaf(leaf::shared_lock_ptr(load_leaf(leaf_bytes()))));
}

TEST_FIXTURE(fixture, split) {
  const auto key1 = cycle_ptr::make_cycle<mock_loader::value_type>(string_value_type("1"));
  const auto key2 = cycle_ptr::make_cycle<mock_loader::value_type>(string_value_type("2"));
  const auto key3 = cycle_ptr::make_cycle<mock_loader::value_type>(string_value_type("3"));
  const auto key4 = cycle_ptr::make_cycle<mock_loader::value_type>(string_value_type("4"));

  leaf::unique_lock_ptr first(
      write_leaf(0, std::vector<cycle_ptr::cycle_gptr<mock_loader::value_type>>{ key1, key2, key3, key4 }));
  leaf::unique_lock_ptr second(
      write_empty_leaf(leaf_bytes()));

  auto tx = file.begin(false);
  leaf::split(*loader, first, second, tx);

  // No change is seen until transaction commit.
  CHECK_EQUAL(
      (std::vector<std::string>{ "1", "2", "3", "4" }),
      fixture::read_all_from_leaf(first));
  CHECK_EQUAL(
      std::vector<std::string>(),
      fixture::read_all_from_leaf(second));

  tx.commit();

  CHECK_EQUAL(
      (std::vector<std::string>{ "1", "2" }),
      fixture::read_all_from_leaf(first));
  CHECK_EQUAL(
      cycle_ptr::cycle_gptr<const earnest::detail::tree::key_type>(nullptr),
      first->key());
  CHECK_EQUAL(
      (std::vector<std::string>{ "3", "4" }),
      fixture::read_all_from_leaf(second));
  CHECK_EQUAL(
      "3",
      key_type_cast(second->key()));

  // Read-back test.
  CHECK_EQUAL(
      (std::vector<std::string>{ "1", "2" }),
      fixture::read_all_from_leaf(leaf::shared_lock_ptr(load_leaf(0))));
  CHECK_EQUAL(
      cycle_ptr::cycle_gptr<const earnest::detail::tree::key_type>(nullptr),
      load_leaf(0)->key());
  CHECK_EQUAL(
      (std::vector<std::string>{ "3", "4" }),
      fixture::read_all_from_leaf(leaf::shared_lock_ptr(load_leaf(leaf_bytes()))));
  CHECK_EQUAL(
      "3",
      key_type_cast(load_leaf(leaf_bytes())->key()));
}

} /* SUITE(leaf) */

int main() {
  return UnitTest::RunAllTests();
}
