#include <earnest/detail/bplus_tree_page.h>

#include <UnitTest++/UnitTest++.h>

#include <asio/deferred.hpp>
#include <asio/io_context.hpp>
#include <earnest/detail/file_grow_allocator.h>
#include <earnest/dir.h>
#include <earnest/isolation.h>
#include <iostream>
#include <string_view>
#include <system_error>

earnest::dir write_dir;

auto ensure_dir_exists_and_is_empty(std::filesystem::path name) -> earnest::dir;

#define NEW_TEST(Name)                                                 \
class Name##_fixture                                                   \
: public fixture                                                       \
{                                                                      \
  public:                                                              \
  Name##_fixture()                                                     \
  : fixture(#Name)                                                     \
  {}                                                                   \
};                                                                     \
                                                                       \
TEST_FIXTURE(Name##_fixture, test)

class fixture {
  public:
  static constexpr std::size_t key_bytes = 4;
  static constexpr std::size_t value_bytes = 4;
  static constexpr std::string_view tree_filename = "test.tree";
  static const earnest::db_address tree_address;

  using raw_db_type = earnest::raw_db<asio::io_context::executor_type>;
  using db_allocator = earnest::detail::file_grow_allocator<raw_db_type>;
  using bplus_tree = earnest::detail::bplus_tree<raw_db_type, db_allocator>;

  protected:
  explicit fixture(std::string_view name);
  ~fixture();

  asio::io_context ioctx;
  const std::shared_ptr<earnest::detail::bplus_tree_spec> spec;
  const cycle_ptr::cycle_gptr<raw_db_type> raw_db;
  cycle_ptr::cycle_gptr<bplus_tree> tree;
};


NEW_TEST(constructor) {
}

NEW_TEST(temp) {
  tree->TEST_FN();
  ioctx.run();
}

NEW_TEST(insert) {
  std::array<std::byte, key_bytes> key{ std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4} };
  std::array<std::byte, value_bytes> value{ std::byte{5}, std::byte{6}, std::byte{7}, std::byte{8} };

  bool callback_called = false;
  tree->insert(key, value, std::allocator<std::byte>(),
      [&callback_called](std::error_code ec, [[maybe_unused]] auto elem_ref) {
        CHECK_EQUAL(std::error_code(), ec);
        callback_called = true;
      });
  ioctx.run();

  CHECK(callback_called);
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: " << (argc > 0 ? argv[0] : "bplus_tree_page_test") << "writeable_dir\n"
        << "writeable_dir: points at a directory where we can write files\n";
    return 1;
  }
  try {
    write_dir = earnest::dir(argv[1]);
  } catch (const std::exception& e) {
    std::cerr << "error opening dirs: " << e.what() << std::endl;
    return 1;
  }

  return UnitTest::RunAllTests();
}

auto ensure_dir_exists_and_is_empty(std::filesystem::path name) -> earnest::dir {
  using namespace std::literals;

  std::cerr << "ensure_dir_exists_and_is_empty(" << name << ")\n";
  if (name.has_parent_path())
    throw std::runtime_error("directory '"s + name.string() + "' is not a relative name"s);

  bool exists = false;
  for (auto d : write_dir) {
    if (d.path() == name) exists = true;
  }

  earnest::dir new_dir;
  if (!exists)
    new_dir.create(write_dir, name);
  else
    new_dir.open(write_dir, name);

  for (auto file : new_dir) {
    if (file.path() == "." || file.path() == "..") continue;

    try {
      new_dir.erase(file);
    } catch (const std::exception& e) {
      std::cerr << "error cleaning dir'" << name << "', failed to erase '" << file.path() << "': " << e.what() << std::endl;
      throw;
    }
  }
  return new_dir;
}

fixture::fixture(std::string_view name)
: ioctx(),
  spec(std::make_shared<earnest::detail::bplus_tree_spec>(
          earnest::detail::bplus_tree_spec{
            .element{
              .key{
                .byte_equality_enabled=true,
                .byte_order_enabled=true,
                .bytes=key_bytes,
              },
              .value{
                .byte_equality_enabled=true,
                .byte_order_enabled=true,
                .bytes=value_bytes,
              },
            },
            .elements_per_leaf=1000,
            .child_pages_per_intr=1000,
          })),
  raw_db(cycle_ptr::make_cycle<raw_db_type>(ioctx.get_executor()))
{
  auto ensure_no_error = asio::deferred(
      [](std::error_code ec, [[maybe_unused]] const auto&... args) {
        REQUIRE CHECK_EQUAL(std::error_code(), ec);
        return asio::deferred.values();
      });

  raw_db->async_create(ensure_dir_exists_and_is_empty(name), asio::deferred)
  | ensure_no_error
  | asio::deferred(
      [this, ensure_no_error]() {
        auto tx = this->raw_db->fdb_tx_begin(earnest::isolation::read_commited);
        auto f = tx[tree_address.file];
        auto creation_bytes = bplus_tree::creation_bytes();

        using tx_type = decltype(tx);
        using f_type = decltype(f);
        using creation_bytes_type = decltype(creation_bytes);
        struct state_t {
          tx_type tx;
          f_type f;
          creation_bytes_type creation_bytes;
        };
        auto state = std::make_shared<state_t>(state_t{
              .tx=std::move(tx),
              .f=std::move(f),
              .creation_bytes=std::move(creation_bytes)
            });

        return state->f.async_create(asio::deferred)
        | ensure_no_error
        | asio::deferred(
            [state, offset=tree_address.offset]() {
              return state->f.async_truncate(offset + state->creation_bytes.size(), asio::deferred);
            })
        | ensure_no_error
        | asio::deferred(
            [state, offset=tree_address.offset]() {
              return asio::async_write_at(state->f, offset, asio::buffer(state->creation_bytes), asio::deferred);
            })
        | ensure_no_error
        | asio::deferred(
            [state]() {
              return state->tx.async_commit(asio::deferred);
            });
      })
  | ensure_no_error
  | asio::deferred(
      [spec=this->spec, raw_db=this->raw_db]() {
        return bplus_tree::async_load_op(spec, raw_db, tree_address, db_allocator(raw_db, tree_address.file),
            []() {
              return asio::deferred.values(std::error_code());
            });
      })
  | asio::deferred(
      [this](std::error_code ec, cycle_ptr::cycle_gptr<bplus_tree> tree) {
        this->tree = std::move(tree);
        return asio::deferred.values(ec);
      })
  | ensure_no_error
  | []() -> void { /* Empty callback, just to ensure this starts running. */ };

  ioctx.run();
  ioctx.restart();
}

fixture::~fixture() = default;

const earnest::db_address fixture::tree_address = earnest::db_address(earnest::file_id("", std::string(tree_filename)), 0);
