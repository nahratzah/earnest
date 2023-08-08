#include <earnest/detail/bplus_tree.h>

#include <UnitTest++/UnitTest++.h>

#include <algorithm>
#include <asio/deferred.hpp>
#include <asio/io_context.hpp>
#include <asio/thread_pool.hpp>
#include <cycle_ptr.h>
#include <earnest/detail/asio_cycle_ptr.h>
#include <earnest/detail/file_grow_allocator.h>
#include <earnest/dir.h>
#include <earnest/isolation.h>
#include <iostream>
#include <random>
#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

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
  static constexpr std::size_t elements_per_leaf = 3;
  static constexpr std::size_t child_pages_per_intr = 3;
  static constexpr std::size_t total_positions_to_fill_tree_3_levels = elements_per_leaf * child_pages_per_intr * child_pages_per_intr;
  static constexpr std::string_view tree_filename = "test.tree";
  static const earnest::db_address tree_address;
  using read_kv_type = std::pair<std::string, std::string>;
  using read_kv_vector = std::vector<read_kv_type>;

  using raw_db_type = earnest::raw_db<asio::io_context::executor_type>;
  using db_allocator = earnest::detail::file_grow_allocator<raw_db_type>;
  using bplus_tree = earnest::detail::bplus_tree<raw_db_type, db_allocator>;

  protected:
  explicit fixture(std::string_view name);
  ~fixture();

  auto insert(std::string_view key, std::string_view value, bool sync) -> void;
  auto read_all() -> std::vector<read_kv_type>;
  auto ioctx_run(std::size_t threads) -> void;
  auto print_tree() -> void;

  template<typename Collection>
  static auto random_shuffled_collection(Collection c) -> Collection {
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(c.begin(), c.end(), g);
    return c;
  }

  asio::io_context ioctx;
  const std::shared_ptr<earnest::detail::bplus_tree_spec> spec;
  const cycle_ptr::cycle_gptr<raw_db_type> raw_db;
  cycle_ptr::cycle_gptr<bplus_tree> tree;
};


namespace std {

template<typename Alloc>
inline auto operator<<(std::ostream& out, const std::vector<::fixture::read_kv_type, Alloc>& v) -> std::ostream& {
  bool first = true;
  out << "{";
  for (const auto& kv : v) {
    out << (first ? " " : ", ");
    first = false;
    out << "{" << kv.first << " => " << kv.second << "}";
  }
  return out << (first ? "" : " ") << "}";
}

}


NEW_TEST(insert) {
  std::array<std::byte, key_bytes> key{ std::byte{'a'}, std::byte{'b'}, std::byte{'b'}, std::byte{'a'} };
  std::array<std::byte, value_bytes> value{ std::byte{'c'}, std::byte{'d'}, std::byte{'e'}, std::byte{'f'} };

  bool callback_called = false;
  tree->insert(key, value, std::allocator<std::byte>(),
      [&callback_called](std::error_code ec, [[maybe_unused]] auto elem_ref) {
        CHECK_EQUAL(std::error_code(), ec);
        callback_called = true;
      });
  ioctx.run();
  ioctx.restart();

  CHECK(callback_called);
  CHECK_EQUAL(
      read_kv_vector({
        { "abba", "cdef" },
      }),
      read_all());
}

NEW_TEST(insert_many_sequentially) {
  read_kv_vector expect;
  for (std::size_t seq = 0; seq < total_positions_to_fill_tree_3_levels; ++seq) {
    auto key = std::string(
        {
          static_cast<char>('a' + (seq / 26 / 26 / 26 % 26)),
          static_cast<char>('a' + (seq / 26 / 26 % 26)),
          static_cast<char>('a' + (seq / 26 % 26)),
          static_cast<char>('a' + (seq % 26)),
        });
    auto value = std::to_string(seq % 10'000);

    expect.emplace_back(std::move(key), std::move(value));
  }

  for (const auto& e : expect)
    insert(e.first, e.second, true);
  CHECK_EQUAL(expect, read_all());

  print_tree();
}

NEW_TEST(insert_many_concurrently) {
  read_kv_vector expect;
  for (std::size_t seq = 0; seq < total_positions_to_fill_tree_3_levels; ++seq) {
    auto key = std::string(
        {
          static_cast<char>('a' + (seq / 26 / 26 / 26 % 26)),
          static_cast<char>('a' + (seq / 26 / 26 % 26)),
          static_cast<char>('a' + (seq / 26 % 26)),
          static_cast<char>('a' + (seq % 26)),
        });
    auto value = std::to_string(seq % 10'000);

    expect.emplace_back(std::move(key), std::move(value));
  }

  for (const auto& e : expect)
    insert(e.first, e.second, false);
  ioctx.run();
  ioctx.restart();

  CHECK_EQUAL(expect, read_all());

  print_tree();
}

NEW_TEST(insert_many_concurrently_with_random_order_multithreaded) {
  read_kv_vector expect;
  for (std::size_t seq = 0; seq < total_positions_to_fill_tree_3_levels; ++seq) {
    auto key = std::string(
        {
          static_cast<char>('a' + (seq / 26 / 26 / 26 % 26)),
          static_cast<char>('a' + (seq / 26 / 26 % 26)),
          static_cast<char>('a' + (seq / 26 % 26)),
          static_cast<char>('a' + (seq % 26)),
        });
    auto value = std::to_string(seq % 10'000);

    expect.emplace_back(std::move(key), std::move(value));
  }

  for (const auto& e : random_shuffled_collection(expect))
    insert(e.first, e.second, false);
  ioctx_run(8);
  ioctx.restart();

  CHECK_EQUAL(expect, read_all());

  print_tree();
}

int main(int argc, char** argv) {
  asio::thread_pool gc_pool(1);
  earnest::detail::asio_cycle_ptr gc(gc_pool.get_executor());

  {
    auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(std::clog);
    spdlog::register_logger(std::make_shared<spdlog::logger>("earnest.db", sink));
    spdlog::register_logger(std::make_shared<spdlog::logger>("earnest.bplustree", sink));
    spdlog::register_logger(std::make_shared<spdlog::logger>("earnest.file_grow_allocator", sink));
    spdlog::register_logger(std::make_shared<spdlog::logger>("earnest.monitor", sink));

    auto dfl_logger = std::make_shared<spdlog::logger>("log", sink);
    spdlog::register_logger(dfl_logger);
    spdlog::set_default_logger(dfl_logger);
  }

  if (argc < 2) {
    std::cerr << "Usage: " << (argc > 0 ? argv[0] : "bplus_tree_test") << "writeable_dir\n"
        << "writeable_dir: points at a directory where we can write files\n";
    return 1;
  }
  try {
    write_dir = earnest::dir(argv[1]);
  } catch (const std::exception& e) {
    spdlog::error("error opening dirs: {}", e.what());
    return 1;
  }

  return UnitTest::RunAllTests();
}

auto ensure_dir_exists_and_is_empty(std::filesystem::path name) -> earnest::dir {
  using namespace std::literals;

  spdlog::trace("ensure_dir_exists_and_is_empty({})", name.string());
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
      spdlog::error("error cleaning dir'{}', failed to erase '{}': {}", name.string(), file.path().string(), e.what());
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
            .elements_per_leaf=elements_per_leaf,
            .child_pages_per_intr=child_pages_per_intr,
          })),
  raw_db(cycle_ptr::make_cycle<raw_db_type>(ioctx.get_executor(), "test-db"))
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

auto fixture::insert(std::string_view key, std::string_view value, bool sync) -> void {
  if (key.size() > key_bytes || value.size() > value_bytes) throw std::invalid_argument("key/value too long");

  struct state {
    state(std::string_view key, std::string_view value)
    : key(key.begin(), key.end()),
      value(value.begin(), value.end())
    {
      this->key.resize(key_bytes);
      this->value.resize(value_bytes);
    }

    auto printable_key() const -> std::string_view {
      std::string_view s = key;
      while (!s.empty() && s.back() == '\0') s = s.substr(0, s.size() - 1u);
      return s;
    }

    auto printable_value() const -> std::string_view {
      std::string_view s = value;
      while (!s.empty() && s.back() == '\0') s = s.substr(0, s.size() - 1u);
      return s;
    }

    std::string key, value;
    bool callback_called = false;
  };
  auto state_ptr = std::make_shared<state>(key, value);

  tree->insert(
      std::as_bytes(std::span<const char>(state_ptr->key.data(), state_ptr->key.size())),
      std::as_bytes(std::span<const char>(state_ptr->value.data(), state_ptr->value.size())),
      std::allocator<std::byte>(),
      [tree=this->tree, state_ptr](std::error_code ec, [[maybe_unused]] auto elem_ref) {
        CHECK_EQUAL(std::error_code(), ec);
        state_ptr->callback_called = true;
        spdlog::trace("inserted {} => {}: {}", state_ptr->printable_key(), state_ptr->printable_value(), ec.message());

        if (ec)
          tree->async_log_dump(std::clog, []() { /* skip */ });
      });

  if (sync) {
    ioctx.run();
    ioctx.restart();
    REQUIRE CHECK(state_ptr->callback_called);
  }
}

auto fixture::read_all() -> std::vector<read_kv_type> {
  std::vector<read_kv_type> elements;
  bool read_all_callback_called = false;

  struct op {
    explicit op(std::vector<read_kv_type>& output)
    : output(output)
    {}

    auto operator()(
        std::span<const earnest::detail::bplus_element_reference<raw_db_type, true>> elements,
        earnest::detail::move_only_function<void(std::error_code)> callback) -> void {
      while (!elements.empty()) {
        auto opt_shlock = elements.front()->element_lock.try_shared(__FILE__, __LINE__);
        if (opt_shlock) {
          output.emplace_back(byte_array_as_string(elements.front()->key_span()), byte_array_as_string(elements.front()->value_span()));
          elements = elements.subspan(1);
        } else {
          elements.front()->element_lock.async_shared(
              [ op=*this, // copy
                elements,
                callback=std::move(callback)
              ](auto lock) mutable -> void {
                op.output.emplace_back(byte_array_as_string(elements.front()->key_span()), byte_array_as_string(elements.front()->value_span()));
                lock.reset();
                std::invoke(op, elements.subspan(1), std::move(callback));
              },
              __FILE__, __LINE__);
          return; // Callback will resume the loop.
        }
      }

      if (output.empty())
        spdlog::trace("read elements, but still empty :P");
      else
        spdlog::trace("read elements, now up to \"{}\"", output.back().first);
      callback(std::error_code{});
      return;
    }

    private:
    static auto byte_array_as_string(std::span<const std::byte> bytes) -> std::string {
      auto s = std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
      while (!s.empty() && s.back() == '\0') s.pop_back();
      return s;
    }

    std::vector<read_kv_type>& output;
  };

  tree->async_visit_all(
      std::allocator<std::byte>(),
      op(elements),
      [&read_all_callback_called](std::error_code ec) {
        read_all_callback_called = true;
        CHECK_EQUAL(std::error_code(), ec);
      });

  ioctx.run();
  ioctx.restart();
  REQUIRE CHECK(read_all_callback_called);
  return elements;
}

auto fixture::ioctx_run(std::size_t threads) -> void {
  std::vector<std::thread> thread_vec;
  for (std::size_t i = 0; i < threads; ++i) {
    thread_vec.emplace_back(
        [this]() {
          this->ioctx.run();
        });
  }

  for (auto& t : thread_vec) t.join();
}

auto fixture::print_tree() -> void {
  bool callback_called = false;
  tree->async_log_dump(std::clog,
      [&callback_called]() {
        callback_called = true;
      });

  ioctx.run();
  ioctx.restart();
  REQUIRE CHECK(callback_called);
}

const earnest::db_address fixture::tree_address = earnest::db_address(earnest::file_id("", std::string(tree_filename)), 0);
