#include "UnitTest++/UnitTest++.h"
#include <earnest/detail/tree/leaf.h>
#include <earnest/detail/tree/tree.h>
#include <earnest/detail/tree/value_type.h>
#include <earnest/detail/tree/key_type.h>
#include <earnest/detail/tree/loader.h>
#include <earnest/fd.h>
#include <earnest/txfile.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/write_at.hpp>
#include <array>
#include <initializer_list>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <vector>

using earnest::detail::tree::abstract_leaf;

template<typename T>
class pointer_vector {
  public:
  pointer_vector() = default;

  pointer_vector(std::vector<cycle_ptr::cycle_gptr<T>> vector)
  : vector_(std::move(vector))
  {}

  pointer_vector(std::initializer_list<cycle_ptr::cycle_gptr<T>> vector)
  : vector_(vector)
  {}

  friend inline auto operator<<(std::ostream& out, const pointer_vector& v) -> std::ostream& {
    if (v.vector_.empty()) return out << "[]";

    bool first = true;
    out << "[ ";
    for (const auto& ptr : v.vector_) {
      if (!std::exchange(first, false)) out << ", ";

      if (ptr == nullptr)
        out << "nil";
      else
        out << *ptr;
    }

    return out << " ]";
  }

  auto operator==(const pointer_vector& y) const -> bool {
    return std::equal(
        vector_.begin(), vector_.end(),
        y.vector_.begin(), y.vector_.end(),
        [](const auto& x_ptr, const auto& y_ptr) {
          if (x_ptr == nullptr || y_ptr == nullptr)
            return x_ptr == y_ptr;
          return *x_ptr == *y_ptr;
        });
  }

  auto operator!=(const pointer_vector& y) const -> bool {
    return !(*this == y);
  }

  auto begin() { return vector_.begin(); }
  auto end() { return vector_.end(); }
  auto begin() const { return vector_.begin(); }
  auto end() const { return vector_.end(); }
  auto cbegin() const { return vector_.cbegin(); }
  auto cend() const { return vector_.cend(); }

  auto empty() const -> bool { return vector_.empty(); }
  auto size() const { return vector_.size(); }

  private:
  std::vector<cycle_ptr::cycle_gptr<T>> vector_;
};

struct fixture {
  static constexpr std::size_t key_bytes = 4;
  static constexpr std::size_t val_bytes = 4;
  static constexpr std::size_t items_per_leaf_page = 4;

  fixture()
  : io_context(),
    file(earnest::txfile::create("tree_leaf", tmpfile_fd_(this->io_context.get_executor(), "tree_leaf"), 0u, 1u << 20)),
    cfg(mk_cfg_()),
    tree(mk_tree_(this->cfg))
  {}

  auto leaf_bytes() const -> std::size_t {
    return abstract_leaf::header::SIZE + cfg->key_bytes + cfg->items_per_leaf_page * (cfg->key_bytes + cfg->val_bytes);
  }

  auto load_leaf(earnest::fd::offset_type offset) -> cycle_ptr::cycle_gptr<abstract_leaf> {
    auto tx = file.begin();

    boost::system::error_code ec;
    auto page_ptr = earnest::detail::tree::abstract_page::decode(tree, tx, offset, earnest::shared_resource_allocator<void>(), ec);
    REQUIRE CHECK(!ec);
    REQUIRE CHECK(page_ptr != nullptr);

    REQUIRE CHECK(std::dynamic_pointer_cast<abstract_leaf>(page_ptr) != nullptr);
    return std::dynamic_pointer_cast<abstract_leaf>(page_ptr);
  }

  auto write_empty_leaf(earnest::fd::offset_type offset) -> cycle_ptr::cycle_gptr<abstract_leaf> {
    auto tx = file.begin(false);
    if (tx.size() < offset + leaf_bytes()) tx.resize(offset + leaf_bytes());
    auto magic = abstract_leaf::magic;
    boost::endian::native_to_big_inplace(magic);
    boost::asio::write_at(tx, offset, boost::asio::buffer(&magic, sizeof(magic)));
    tx.commit();

    return load_leaf(offset);
  }

  template<typename Vector>
  auto write_leaf(earnest::fd::offset_type offset, const Vector& elems) -> cycle_ptr::cycle_gptr<abstract_leaf> {
    abstract_leaf::unique_lock_ptr leaf(write_empty_leaf(offset));

    for (const auto& new_elem : elems) {
      auto tx = file.begin(false);
      abstract_leaf::link(leaf, new_elem, nullptr, tx);
      tx.commit();
    }

    return leaf.mutex();
  }

  class key_type
  : public earnest::detail::tree::key_type
  {
    public:
    static auto cast(const earnest::detail::tree::key_type& k) -> const key_type& {
      return dynamic_cast<const key_type&>(k);
    }

    static auto cast(const cycle_ptr::cycle_gptr<const earnest::detail::tree::key_type>& k) -> const key_type& {
      REQUIRE CHECK(k != nullptr);
      return cast(*k);
    }

    explicit key_type(allocator_type alloc = allocator_type())
    : earnest::detail::tree::key_type(alloc)
    {}

    explicit key_type(std::string key, allocator_type alloc = allocator_type())
    : earnest::detail::tree::key_type(alloc),
      key(std::move(key))
    {}

    void encode(boost::asio::mutable_buffer buf) const override {
      std::array<char, key_bytes> key_filler;
      std::fill(key_filler.begin(), key_filler.end(), 0);

      boost::asio::buffer_copy(
          buf,
          std::array<boost::asio::const_buffer, 2>{
              boost::asio::buffer(key),
              boost::asio::buffer(key_filler, key_bytes - key.size())
          });
    }

    void decode(boost::asio::const_buffer buf) override {
      std::array<char, key_bytes> key;

      boost::asio::buffer_copy(
          boost::asio::buffer(key),
          buf);

      const auto key_end = std::find(key.begin(), key.end(), '\0');
      this->key.assign(key.begin(), key_end);
    }

    auto operator==(const key_type& y) const -> bool {
      return key == y.key;
    }

    auto operator!=(const key_type& y) const -> bool {
      return !(*this == y);
    }

    std::string key;
  };

  class element_type
  : public earnest::detail::tree::value_type
  {
    public:
    using earnest::detail::tree::value_type::value_type;

    element_type(std::string key, std::string value, allocator_type alloc = allocator_type())
    : value_type(std::move(alloc)),
      key(std::move(key)),
      value(std::move(value))
    {}

    void lock() override final { mtx_.lock(); }
    auto try_lock() -> bool override final { return mtx_.try_lock(); }
    void unlock() override final { mtx_.unlock(); }
    void lock_shared() const override final { mtx_.lock_shared(); }
    auto try_lock_shared() const -> bool override final { return mtx_.try_lock_shared(); }
    void unlock_shared() const override final { mtx_.unlock_shared(); }

    auto is_never_visible() const noexcept -> bool override final { return key.empty(); }

    auto get_key(allocator_type alloc) const -> cycle_ptr::cycle_gptr<const earnest::detail::tree::key_type> override final {
      return cycle_ptr::allocate_cycle<key_type>(alloc, key, alloc);
    }

    auto operator==(const element_type& y) const -> bool {
      return key == y.key && value == y.value;
    }

    auto operator!=(const element_type& y) const -> bool {
      return !(*this == y);
    }

    private:
    void encode(boost::asio::mutable_buffer buf) const override {
      std::array<char, key_bytes> key_filler;
      std::fill(key_filler.begin(), key_filler.end(), 0);
      std::array<char, val_bytes> val_filler;
      std::fill(val_filler.begin(), val_filler.end(), 0);

      boost::asio::buffer_copy(
          buf,
          std::array<boost::asio::const_buffer, 4>{
              boost::asio::buffer(key),
              boost::asio::buffer(key_filler, key_bytes - key.size()),
              boost::asio::buffer(value),
              boost::asio::buffer(val_filler, val_bytes - value.size())
          });
    }

    void decode(boost::asio::const_buffer buf) override {
      std::array<char, key_bytes> key;
      std::array<char, val_bytes> val;

      boost::asio::buffer_copy(
          std::array<boost::asio::mutable_buffer, 2>{
              boost::asio::buffer(key),
              boost::asio::buffer(val)
          },
          buf);

      const auto key_end = std::find(key.begin(), key.end(), '\0');
      this->key.assign(key.begin(), key_end);
      const auto val_end = std::find(val.begin(), val.end(), '\0');
      this->value.assign(val.begin(), val_end);
    }

    public:
    std::string key;
    std::string value;

    private:
    mutable std::shared_mutex mtx_;
  };

  static auto read_all_from_leaf(const abstract_leaf::shared_lock_ptr& leaf) -> pointer_vector<element_type> {
    auto raw_value_types = abstract_leaf::get_elements(leaf);

    std::vector<cycle_ptr::cycle_gptr<element_type>> result;
    std::transform(raw_value_types.begin(), raw_value_types.end(), std::back_inserter(result),
        [](const cycle_ptr::cycle_gptr<earnest::detail::tree::value_type>& value_ptr) -> cycle_ptr::cycle_gptr<element_type> {
          auto ptr = std::dynamic_pointer_cast<element_type>(value_ptr);
          REQUIRE CHECK(ptr != nullptr);
          return ptr;
        });
    return result;
  }

  static auto read_all_from_leaf(const abstract_leaf::unique_lock_ptr& leaf) -> pointer_vector<element_type> {
    auto raw_value_types = abstract_leaf::get_elements(leaf);

    std::vector<cycle_ptr::cycle_gptr<element_type>> result;
    std::transform(raw_value_types.begin(), raw_value_types.end(), std::back_inserter(result),
        [](const cycle_ptr::cycle_gptr<earnest::detail::tree::value_type>& value_ptr) -> cycle_ptr::cycle_gptr<element_type> {
          auto ptr = std::dynamic_pointer_cast<element_type>(value_ptr);
          REQUIRE CHECK(ptr != nullptr);
          return ptr;
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
    class cfg_impl
    : public earnest::detail::tree::cfg
    {
      public:
      using earnest::detail::tree::cfg::cfg;

      auto allocate_elem(earnest::detail::db_cache::allocator_type alloc) const -> cycle_ptr::cycle_gptr<earnest::detail::tree::value_type> override {
        return cycle_ptr::allocate_cycle<element_type>(alloc, alloc);
      }

      auto allocate_key(earnest::detail::db_cache::allocator_type alloc) const -> cycle_ptr::cycle_gptr<earnest::detail::tree::key_type> override {
        return cycle_ptr::allocate_cycle<key_type>(alloc, alloc);
      }
    };

    auto cfg = std::make_shared<cfg_impl>();
    cfg->items_per_leaf_page = items_per_leaf_page;
    cfg->items_per_node_page = 2;
    cfg->key_bytes = key_bytes;
    cfg->val_bytes = val_bytes;
    cfg->augment_bytes = 1;
    return cfg;
  }

  static auto mk_tree_(std::shared_ptr<earnest::detail::tree::cfg> cfg) -> cycle_ptr::cycle_gptr<earnest::detail::tree::abstract_tree> {
    class test_tree
    : public earnest::detail::tree::abstract_tree
    {
      public:
      using earnest::detail::tree::abstract_tree::abstract_tree;
    };

    return cycle_ptr::make_cycle<test_tree>(cfg);
  }

  class mock_loader
  : public earnest::detail::tree::loader
  {
    public:
    explicit mock_loader(fixture& self) noexcept : self_(self) {}

    private:
    auto do_load_from_disk(offset_type off, earnest::detail::cheap_fn_ref<cycle_ptr::cycle_gptr<earnest::detail::db_cache::cache_obj>(earnest::detail::db_cache::allocator_type, cycle_ptr::cycle_gptr<earnest::detail::tree::abstract_tree>, const earnest::txfile::transaction&, offset_type)> load) const
    -> cycle_ptr::cycle_gptr<earnest::detail::db_cache::cache_obj> override {
      return load(alloc, self_.tree, self_.file.begin(), off);
    }

    fixture& self_;
    earnest::detail::db_cache::allocator_type alloc;
  };

  public:
  boost::asio::io_context io_context;
  earnest::txfile file;
  std::shared_ptr<earnest::detail::tree::cfg> cfg;
  cycle_ptr::cycle_gptr<earnest::detail::tree::abstract_tree> tree;
  mock_loader loader{*this};
};

inline auto operator<<(std::ostream& out, const fixture::element_type& e) -> std::ostream& {
  return out << "{" << e.key << "=" << e.value << "}";
}

inline auto operator<<(std::ostream& out, const fixture::key_type& e) -> std::ostream& {
  return out << "{" << e.key << "}";
}

SUITE(leaf) {

TEST_FIXTURE(fixture, empty_page) {
  auto leaf = abstract_leaf::shared_lock_ptr(write_empty_leaf(0));
  CHECK_EQUAL(cfg->items_per_leaf_page, leaf->max_size());
  CHECK_EQUAL(0, abstract_leaf::size(leaf));

  CHECK_EQUAL(pointer_vector<element_type>(), fixture::read_all_from_leaf(leaf));
  CHECK_EQUAL(cycle_ptr::cycle_gptr<const earnest::detail::tree::key_type>(nullptr), leaf->key());
}

TEST_FIXTURE(fixture, append_first_element) {
  const auto new_elem = cycle_ptr::make_cycle<element_type>("key", "val");

  auto leaf = abstract_leaf::unique_lock_ptr(write_empty_leaf(0));

  auto tx = file.begin(false);
  abstract_leaf::link(leaf, new_elem, nullptr, tx);

  // Element don't appear until commit is called.
  CHECK_EQUAL(pointer_vector<element_type>(), fixture::read_all_from_leaf(leaf));
  tx.commit();

  CHECK_EQUAL(
      pointer_vector<element_type>{ new_elem },
      fixture::read_all_from_leaf(leaf));
}

TEST_FIXTURE(fixture, append_many_elements) {
  const pointer_vector<element_type> elems{
      cycle_ptr::make_cycle<element_type>("key1", "1"),
      cycle_ptr::make_cycle<element_type>("key2", "2"),
      cycle_ptr::make_cycle<element_type>("key3", "3"),
      cycle_ptr::make_cycle<element_type>("key4", "4"),
  };

  auto leaf = abstract_leaf::unique_lock_ptr(write_empty_leaf(0));
  for (const auto& new_elem : elems) {
    auto tx = file.begin(false);
    abstract_leaf::link(leaf, new_elem, nullptr, tx);
    tx.commit();
  }

  CHECK_EQUAL(elems, fixture::read_all_from_leaf(leaf));
}

TEST_FIXTURE(fixture, read_back_empty) {
  write_empty_leaf(0);

  CHECK_EQUAL(pointer_vector<element_type>(), fixture::read_all_from_leaf(abstract_leaf::shared_lock_ptr(load_leaf(0))));
}

TEST_FIXTURE(fixture, read_back) {
  const pointer_vector<element_type> elems{
      cycle_ptr::make_cycle<element_type>("key1", "1"),
      cycle_ptr::make_cycle<element_type>("key2", "2"),
      cycle_ptr::make_cycle<element_type>("key3", "3"),
      cycle_ptr::make_cycle<element_type>("key4", "4")
  };
  write_leaf(0, elems);

  CHECK_EQUAL(elems, fixture::read_all_from_leaf(abstract_leaf::shared_lock_ptr(load_leaf(0))));
}

TEST_FIXTURE(fixture, erase) {
  const auto key1 = cycle_ptr::make_cycle<element_type>("key1", "1");
  const auto key2 = cycle_ptr::make_cycle<element_type>("key2", "2");
  const auto key3 = cycle_ptr::make_cycle<element_type>("key3", "3");
  const auto key4 = cycle_ptr::make_cycle<element_type>("key4", "4");

  abstract_leaf::unique_lock_ptr leaf(
      write_leaf(0, pointer_vector<element_type>{ key1, key2, key3, key4 }));
  auto tx = file.begin(false);
  abstract_leaf::unlink(leaf, key2, tx);

  // Unlink operation won't take effect until commited.
  CHECK_EQUAL((pointer_vector<element_type>{ key1, key2, key3, key4 }), fixture::read_all_from_leaf(leaf));
  tx.commit();

  // key2 was deleted.
  CHECK_EQUAL((pointer_vector<element_type>{ key1, key3, key4 }), fixture::read_all_from_leaf(leaf));
}

TEST_FIXTURE(fixture, read_back_erase) {
  const auto key1 = cycle_ptr::make_cycle<element_type>("key1", "1");
  const auto key2 = cycle_ptr::make_cycle<element_type>("key2", "2");
  const auto key3 = cycle_ptr::make_cycle<element_type>("key3", "3");
  const auto key4 = cycle_ptr::make_cycle<element_type>("key4", "4");

  {
    abstract_leaf::unique_lock_ptr leaf(
        write_leaf(0, pointer_vector<element_type>{ key1, key2, key3, key4 }));
    auto tx = file.begin(false);
    abstract_leaf::unlink(leaf, key2, tx);

    // Unlink operation won't take effect until commited.
    CHECK_EQUAL((pointer_vector<element_type>{ key1, key2, key3, key4 }), fixture::read_all_from_leaf(leaf));
    tx.commit();
  }

  CHECK_EQUAL(
      (pointer_vector<element_type>{ key1, key3, key4 }),
      fixture::read_all_from_leaf(abstract_leaf::shared_lock_ptr(load_leaf(0))));
}

TEST_FIXTURE(fixture, merge) {
  const auto key1 = cycle_ptr::make_cycle<element_type>("key1", "1");
  const auto key2 = cycle_ptr::make_cycle<element_type>("key2", "2");
  const auto key3 = cycle_ptr::make_cycle<element_type>("key3", "3");
  const auto key4 = cycle_ptr::make_cycle<element_type>("key4", "4");

  abstract_leaf::unique_lock_ptr first(
      write_leaf(0, pointer_vector<element_type>{ key1, key2 }));
  abstract_leaf::unique_lock_ptr second(
      write_leaf(leaf_bytes(), pointer_vector<element_type>{ key3, key4 }));

  auto tx = file.begin(false);
  abstract_leaf::merge(loader, first, second, tx);

  // No change is seen until transaction commit.
  CHECK_EQUAL(
      (pointer_vector<element_type>{ key1, key2 }),
      fixture::read_all_from_leaf(first));
  CHECK_EQUAL(
      (pointer_vector<element_type>{ key3, key4 }),
      fixture::read_all_from_leaf(second));

  tx.commit();

  CHECK_EQUAL(
      (pointer_vector<element_type>{ key1, key2, key3, key4 }),
      fixture::read_all_from_leaf(first));
  CHECK_EQUAL(
      pointer_vector<element_type>(),
      fixture::read_all_from_leaf(second));

  // Read-back test.
  CHECK_EQUAL(
      (pointer_vector<element_type>{ key1, key2, key3, key4 }),
      fixture::read_all_from_leaf(abstract_leaf::shared_lock_ptr(load_leaf(0))));
  CHECK_EQUAL(
      cycle_ptr::cycle_gptr<const earnest::detail::tree::key_type>(nullptr),
      load_leaf(0)->key());

  CHECK_EQUAL(
      pointer_vector<element_type>(),
      fixture::read_all_from_leaf(abstract_leaf::shared_lock_ptr(load_leaf(leaf_bytes()))));
}

TEST_FIXTURE(fixture, split) {
  const auto key1 = cycle_ptr::make_cycle<element_type>("key1", "1");
  const auto key2 = cycle_ptr::make_cycle<element_type>("key2", "2");
  const auto key3 = cycle_ptr::make_cycle<element_type>("key3", "3");
  const auto key4 = cycle_ptr::make_cycle<element_type>("key4", "4");

  abstract_leaf::unique_lock_ptr first(
      write_leaf(0, pointer_vector<element_type>{ key1, key2, key3, key4 }));
  abstract_leaf::unique_lock_ptr second(
      write_empty_leaf(leaf_bytes()));

  auto tx = file.begin(false);
  abstract_leaf::split(loader, first, second, tx);

  // No change is seen until transaction commit.
  CHECK_EQUAL(
      (pointer_vector<element_type>{ key1, key2, key3, key4 }),
      fixture::read_all_from_leaf(first));
  CHECK_EQUAL(
      pointer_vector<element_type>(),
      fixture::read_all_from_leaf(second));

  tx.commit();

  CHECK_EQUAL(
      (pointer_vector<element_type>{ key1, key2 }),
      fixture::read_all_from_leaf(first));
  CHECK_EQUAL(
      cycle_ptr::cycle_gptr<const earnest::detail::tree::key_type>(nullptr),
      first->key());
  CHECK_EQUAL(
      (pointer_vector<element_type>{ key3, key4 }),
      fixture::read_all_from_leaf(second));
  CHECK_EQUAL(
      key_type("key3"),
      key_type::cast(second->key()));

  // Read-back test.
  CHECK_EQUAL(
      (pointer_vector<element_type>{ key1, key2 }),
      fixture::read_all_from_leaf(abstract_leaf::shared_lock_ptr(load_leaf(0))));
  CHECK_EQUAL(
      cycle_ptr::cycle_gptr<const earnest::detail::tree::key_type>(nullptr),
      load_leaf(0)->key());
  CHECK_EQUAL(
      (pointer_vector<element_type>{ key3, key4 }),
      fixture::read_all_from_leaf(abstract_leaf::shared_lock_ptr(load_leaf(leaf_bytes()))));
  CHECK_EQUAL(
      key_type("key3"),
      key_type::cast(load_leaf(leaf_bytes())->key()));
}

} /* SUITE(leaf) */

int main() {
  return UnitTest::RunAllTests();
}
