#include <earnest/detail/wal_file_entry.h>

#include <UnitTest++/UnitTest++.h>

#include <iomanip>
#include <iostream>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>

earnest::dir source_files, write_dir;

auto hex_string(std::string_view sv) -> std::string {
  using namespace std::literals;

  std::ostringstream out;
  out << std::setfill('0');
  bool first = true;
  for (char c : sv) {
    if (!std::exchange(first, false)) out << " "sv;
    if (c < 32 || c >= 127) {
      out << "0x"sv << std::setw(2) << std::hex << static_cast<unsigned int>(static_cast<unsigned char>(c));
    } else {
      out << " '"sv << std::string_view(&c, 1) << "'"sv;
    }
  }

  return std::move(out).str();
}

void ensure_file_is_gone(std::string filename) {
  std::error_code ec;
  write_dir.erase(filename, ec);
  REQUIRE CHECK(ec == std::error_code() || ec == make_error_code(std::errc::no_such_file_or_directory));
}

TEST(read_empty_wal_file_entry) {
  auto test_function = [&]() -> std::error_code {
    try {
      earnest::execution::sync_wait(
          earnest::detail::wal_file_entry::open(source_files, "empty")).value();
    } catch (const std::error_code& ec) {
      return ec;
    }
    return {};
  };

  CHECK(test_function() != std::error_code{});
}

TEST(read_non_existant_wal_file_entry) {
  auto test_function = [&]() -> std::error_code {
    try {
      earnest::execution::sync_wait(
          earnest::detail::wal_file_entry::open(source_files, "non_existant_file")).value();
    } catch (const std::error_code& ec) {
      return ec;
    }
    return {};
  };

  CHECK(test_function() != std::error_code{});
}

TEST(read_wal_file_entry) {
  auto [f] = earnest::execution::sync_wait(
      earnest::detail::wal_file_entry::open(source_files, "version_0")).value();

  CHECK_EQUAL(0u, f->version);
  CHECK_EQUAL(17u, f->sequence);
  CHECK_EQUAL(32u, f->end_offset());
  CHECK_EQUAL(28u, f->link_offset());

  REQUIRE CHECK_EQUAL(earnest::detail::wal_file_entry_state::ready, f->state());

  auto [records] = earnest::execution::sync_wait(f->records()).value();
  CHECK(records.empty());
}

TEST(read_sealed_wal_file_entry) {
  auto [f] = earnest::execution::sync_wait(
      earnest::detail::wal_file_entry::open(source_files, "sealed_0")).value();

  CHECK_EQUAL(0u, f->version);
  CHECK_EQUAL(17u, f->sequence);
  CHECK_EQUAL(36u, f->end_offset());
  CHECK_EQUAL(32u, f->link_offset());

  REQUIRE CHECK_EQUAL(earnest::detail::wal_file_entry_state::sealed, f->state());

  auto [records] = earnest::execution::sync_wait(f->records()).value();
  CHECK_EQUAL(1u, records.size());
  if (!records.empty())
    CHECK(std::holds_alternative<::earnest::detail::wal_record_seal>(records.back()));
}

TEST(write_wal_file_entry) {
  using namespace std::string_literals;

  // We have to make sure the file doesn't exist, or the test will fail.
  ensure_file_is_gone("wal_19");

  auto [f, acceptor] = earnest::execution::sync_wait(
      earnest::detail::wal_file_entry::create(write_dir, "wal_19", 19)).value();
  acceptor.assign_values();

  CHECK_EQUAL(
      (earnest::detail::wal_file_entry::max_version),
      f->version);
  CHECK_EQUAL(19u, f->sequence);
  CHECK_EQUAL(32u, f->end_offset());
  CHECK_EQUAL(28u, f->link_offset());

  {
    earnest::fd raw_file;
    raw_file.open(write_dir, "wal_19", earnest::open_mode::READ_ONLY);
    CHECK_EQUAL(
        hex_string("\013\013earnest.wal\000\000\000\000\000\000\000\000\000\000\000\000\000\000\023\000\000\000\000"s),
        hex_string(raw_file.contents<std::string>()));
  }

  REQUIRE CHECK_EQUAL(earnest::detail::wal_file_entry_state::ready, f->state());

  auto [records] = earnest::execution::sync_wait(f->records()).value();
  CHECK(records.empty());
}

TEST(durable_append_wal_file_entry) {
  using namespace std::string_literals;
  using wal_file_entry_t = earnest::detail::wal_file_entry;
  using ::earnest::detail::wal_record_noop;
  using ::earnest::detail::wal_record_skip32;

  // We have to make sure the file doesn't exist, or the test will fail.
  ensure_file_is_gone("durable_append_log");

  auto [f, acceptor] = earnest::execution::sync_wait(
      earnest::detail::wal_file_entry::create(write_dir, "durable_append_log", 17)).value();
  acceptor.assign_values();

  earnest::execution::sync_wait(
      f->append(
          wal_file_entry_t::write_records_buffer_t(
              std::initializer_list<wal_file_entry_t::write_variant_type>{
                wal_record_noop{}, wal_record_skip32{ .bytes = 8 }, wal_record_skip32{ .bytes = 0 }
              },
              f->get_allocator())));

  CHECK_EQUAL(60u, f->end_offset());
  CHECK_EQUAL(56u, f->link_offset());

  {
    earnest::fd raw_file;
    raw_file.open(write_dir, "durable_append_log", earnest::open_mode::READ_ONLY);
    CHECK_EQUAL(
        hex_string("\013\013earnest.wal\000\000\000\000\000\000\000\000\000\000\000\000\000\000\021"s
            + "\000\000\000\001"s // wal_record_noop
            + "\000\000\000\002\000\000\000\010\000\000\000\000\000\000\000\000"s // wal_record_skip32(8)
            + "\000\000\000\002\000\000\000\000"s // wal_record_skip32(0)
            + "\000\000\000\000"s // sentinel (wal_record_end_of_records)
            ),
        hex_string(raw_file.contents<std::string>()));
  }

  auto [records] = earnest::execution::sync_wait(f->records()).value();
  CHECK_EQUAL(3u, records.size());
  if (records.size() == 3) {
    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_noop>(records.at(0)));
    CHECK(wal_record_noop{} == std::get<wal_record_noop>(records.at(0)));

    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(1)));
    CHECK(wal_record_skip32{8} == std::get<wal_record_skip32>(records.at(1)));

    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(2)));
    CHECK(wal_record_skip32{0} == std::get<wal_record_skip32>(records.at(2)));
  }
}

TEST(durable_append_wal_file_entry_with_succeeding_txvalidation) {
  using namespace std::string_literals;
  using wal_file_entry_t = earnest::detail::wal_file_entry;
  using ::earnest::detail::wal_record_noop;
  using ::earnest::detail::wal_record_skip32;

  // We have to make sure the file doesn't exist, or the test will fail.
  ensure_file_is_gone("durable_txvalidating_append_log");

  auto [f, acceptor] = earnest::execution::sync_wait(
      earnest::detail::wal_file_entry::create(write_dir, "durable_txvalidating_append_log", 17)).value();
  acceptor.assign_values();

  bool tx_validator_called = false;
  earnest::execution::sync_wait(
      f->append(
          wal_file_entry_t::write_records_buffer_t(
              std::initializer_list<wal_file_entry_t::write_variant_type>{
                wal_record_noop{}, wal_record_skip32{ .bytes = 8 }
              },
              f->get_allocator()),
          [&tx_validator_called]() {
            CHECK(!tx_validator_called);
            tx_validator_called = true;
            return earnest::execution::just();
          }));
  // Confirm we can do another write after.
  earnest::execution::sync_wait(
      f->append(
          wal_file_entry_t::write_records_buffer_t(
              std::initializer_list<wal_file_entry_t::write_variant_type>{
                wal_record_skip32{ .bytes = 0 }
              },
              f->get_allocator())));

  CHECK(tx_validator_called);
  CHECK_EQUAL(60u, f->end_offset());
  CHECK_EQUAL(56u, f->link_offset());

  {
    earnest::fd raw_file;
    raw_file.open(write_dir, "durable_txvalidating_append_log", earnest::open_mode::READ_ONLY);
    CHECK_EQUAL(
        hex_string("\013\013earnest.wal\000\000\000\000\000\000\000\000\000\000\000\000\000\000\021"s
            + "\000\000\000\001"s // wal_record_noop
            + "\000\000\000\002\000\000\000\010\000\000\000\000\000\000\000\000"s // wal_record_skip32(8)
            + "\000\000\000\002\000\000\000\000"s // wal_record_skip32(0)
            + "\000\000\000\000"s // sentinel (wal_record_end_of_records)
            ),
        hex_string(raw_file.contents<std::string>()));
  }

  auto [records] = earnest::execution::sync_wait(f->records()).value();
  CHECK_EQUAL(3u, records.size());
  if (records.size() == 3) {
    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_noop>(records.at(0)));
    CHECK(wal_record_noop{} == std::get<wal_record_noop>(records.at(0)));

    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(1)));
    CHECK(wal_record_skip32{8} == std::get<wal_record_skip32>(records.at(1)));

    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(2)));
    CHECK(wal_record_skip32{0} == std::get<wal_record_skip32>(records.at(2)));
  }
}

TEST(durable_append_wal_file_entry_with_failing_txvalidation) {
  struct error {};
  using namespace std::string_literals;
  using wal_file_entry_t = earnest::detail::wal_file_entry;
  using ::earnest::detail::wal_record_noop;
  using ::earnest::detail::wal_record_skip32;

  // We have to make sure the file doesn't exist, or the test will fail.
  ensure_file_is_gone("durable_failing_txvalidating_append_log");

  auto [f, acceptor] = earnest::execution::sync_wait(
      earnest::detail::wal_file_entry::create(write_dir, "durable_failing_txvalidating_append_log", 17)).value();
  acceptor.assign_values();

  bool tx_validator_called = false;
  CHECK_THROW(
      earnest::execution::sync_wait(
          f->append(
              wal_file_entry_t::write_records_buffer_t(
                  std::initializer_list<wal_file_entry_t::write_variant_type>{
                    wal_record_noop{}, wal_record_skip32{ .bytes = 8 }
                  },
                  f->get_allocator()),
              [&tx_validator_called]() -> decltype(earnest::execution::just()) {
                CHECK(!tx_validator_called);
                tx_validator_called = true;
                throw error{};
              })),
      error);
  // Confirm we can do another write after.
  earnest::execution::sync_wait(
      f->append(
          wal_file_entry_t::write_records_buffer_t(
              std::initializer_list<wal_file_entry_t::write_variant_type>{
                wal_record_skip32{ .bytes = 0 }
              },
              f->get_allocator())));

  CHECK(tx_validator_called);
  CHECK_EQUAL(60u, f->end_offset());
  CHECK_EQUAL(56u, f->link_offset());

  auto [records] = earnest::execution::sync_wait(f->records()).value();
  CHECK_EQUAL(2u, records.size());
  if (records.size() == 2) {
    // Record from the tx-validation-failure is a skip record.
    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(0)));
    CHECK(wal_record_skip32{12} == std::get<wal_record_skip32>(records.at(0)));

    // This record follows, so it must be written.
    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(1)));
    CHECK(wal_record_skip32{0} == std::get<wal_record_skip32>(records.at(1)));
  }
}

TEST(durable_append_wal_file_entry_callback) {
  using namespace std::string_literals;
  using wal_file_entry_t = earnest::detail::wal_file_entry;
  using ::earnest::detail::wal_record_noop;
  using ::earnest::detail::wal_record_skip32;

  // We have to make sure the file doesn't exist, or the test will fail.
  ensure_file_is_gone("durable_append_log");

  auto [f, acceptor] = earnest::execution::sync_wait(
      earnest::detail::wal_file_entry::create(write_dir, "durable_append_log", 17)).value();
  acceptor.assign_values();

  bool callback_called = false;
  earnest::execution::sync_wait(
      f->append(
          wal_file_entry_t::write_records_buffer_t(
              std::initializer_list<wal_file_entry_t::write_variant_type>{
                wal_record_noop{}, wal_record_skip32{ .bytes = 8 }, wal_record_skip32{ .bytes = 0 }
              },
              f->get_allocator()),
          nullptr,
          [&callback_called](const auto& records) {
            CHECK_EQUAL(3u, records.size());
            CHECK(wal_record_noop{} == std::get<wal_record_noop>(records.at(0)));
            CHECK(wal_record_skip32{8} == std::get<wal_record_skip32>(records.at(1)));
            CHECK(wal_record_skip32{0} == std::get<wal_record_skip32>(records.at(2)));
            callback_called = true;
            return earnest::execution::just();
          }));

  CHECK(callback_called);
  CHECK_EQUAL(60u, f->end_offset());
  CHECK_EQUAL(56u, f->link_offset());

  {
    earnest::fd raw_file;
    raw_file.open(write_dir, "durable_append_log", earnest::open_mode::READ_ONLY);
    CHECK_EQUAL(
        hex_string("\013\013earnest.wal\000\000\000\000\000\000\000\000\000\000\000\000\000\000\021"s
            + "\000\000\000\001"s // wal_record_noop
            + "\000\000\000\002\000\000\000\010\000\000\000\000\000\000\000\000"s // wal_record_skip32(8)
            + "\000\000\000\002\000\000\000\000"s // wal_record_skip32(0)
            + "\000\000\000\000"s // sentinel (wal_record_end_of_records)
            ),
        hex_string(raw_file.contents<std::string>()));
  }

  auto [records] = earnest::execution::sync_wait(f->records()).value();
  CHECK_EQUAL(3u, records.size());
  if (records.size() == 3) {
    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_noop>(records.at(0)));
    CHECK(wal_record_noop{} == std::get<wal_record_noop>(records.at(0)));

    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(1)));
    CHECK(wal_record_skip32{8} == std::get<wal_record_skip32>(records.at(1)));

    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(2)));
    CHECK(wal_record_skip32{0} == std::get<wal_record_skip32>(records.at(2)));
  }
}

TEST(durable_append_wal_file_entry_with_succeeding_txvalidation_callback) {
  using namespace std::string_literals;
  using wal_file_entry_t = earnest::detail::wal_file_entry;
  using ::earnest::detail::wal_record_noop;
  using ::earnest::detail::wal_record_skip32;

  // We have to make sure the file doesn't exist, or the test will fail.
  ensure_file_is_gone("durable_txvalidating_append_log");

  auto [f, acceptor] = earnest::execution::sync_wait(
      earnest::detail::wal_file_entry::create(write_dir, "durable_txvalidating_append_log", 17)).value();
  acceptor.assign_values();

  bool tx_validator_called = false;
  bool callback_called = false;
  earnest::execution::sync_wait(
      f->append(
          wal_file_entry_t::write_records_buffer_t(
              std::initializer_list<wal_file_entry_t::write_variant_type>{
                wal_record_noop{}, wal_record_skip32{ .bytes = 8 }
              },
              f->get_allocator()),
          [&tx_validator_called]() {
            CHECK(!tx_validator_called);
            tx_validator_called = true;
            return earnest::execution::just();
          },
          [&callback_called](const auto& records) {
            CHECK_EQUAL(2u, records.size());
            CHECK(wal_record_noop{} == std::get<wal_record_noop>(records.at(0)));
            CHECK(wal_record_skip32{8} == std::get<wal_record_skip32>(records.at(1)));
            callback_called = true;
            return earnest::execution::just();
          }));
  // Confirm we can do another write after.
  earnest::execution::sync_wait(
      f->append(
          wal_file_entry_t::write_records_buffer_t(
              std::initializer_list<wal_file_entry_t::write_variant_type>{
                wal_record_skip32{ .bytes = 0 }
              },
              f->get_allocator())));

  CHECK(tx_validator_called);
  CHECK(callback_called);
  CHECK_EQUAL(60u, f->end_offset());
  CHECK_EQUAL(56u, f->link_offset());

  {
    earnest::fd raw_file;
    raw_file.open(write_dir, "durable_txvalidating_append_log", earnest::open_mode::READ_ONLY);
    CHECK_EQUAL(
        hex_string("\013\013earnest.wal\000\000\000\000\000\000\000\000\000\000\000\000\000\000\021"s
            + "\000\000\000\001"s // wal_record_noop
            + "\000\000\000\002\000\000\000\010\000\000\000\000\000\000\000\000"s // wal_record_skip32(8)
            + "\000\000\000\002\000\000\000\000"s // wal_record_skip32(0)
            + "\000\000\000\000"s // sentinel (wal_record_end_of_records)
            ),
        hex_string(raw_file.contents<std::string>()));
  }

  auto [records] = earnest::execution::sync_wait(f->records()).value();
  CHECK_EQUAL(3u, records.size());
  if (records.size() == 3) {
    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_noop>(records.at(0)));
    CHECK(wal_record_noop{} == std::get<wal_record_noop>(records.at(0)));

    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(1)));
    CHECK(wal_record_skip32{8} == std::get<wal_record_skip32>(records.at(1)));

    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(2)));
    CHECK(wal_record_skip32{0} == std::get<wal_record_skip32>(records.at(2)));
  }
}

TEST(durable_append_wal_file_entry_with_failing_txvalidation_callback) {
  struct error {};
  using namespace std::string_literals;
  using wal_file_entry_t = earnest::detail::wal_file_entry;
  using ::earnest::detail::wal_record_noop;
  using ::earnest::detail::wal_record_skip32;

  // We have to make sure the file doesn't exist, or the test will fail.
  ensure_file_is_gone("durable_failing_txvalidating_append_log");

  auto [f, acceptor] = earnest::execution::sync_wait(
      earnest::detail::wal_file_entry::create(write_dir, "durable_failing_txvalidating_append_log", 17)).value();
  acceptor.assign_values();

  bool tx_validator_called = false;
  bool callback_called = false;
  CHECK_THROW(
      earnest::execution::sync_wait(
          f->append(
              wal_file_entry_t::write_records_buffer_t(
                  std::initializer_list<wal_file_entry_t::write_variant_type>{
                    wal_record_noop{}, wal_record_skip32{ .bytes = 8 }
                  },
                  f->get_allocator()),
              [&tx_validator_called]() -> decltype(earnest::execution::just()) {
                CHECK(!tx_validator_called);
                tx_validator_called = true;
                throw error{};
              },
              [&callback_called]([[maybe_unused]] const auto& records) {
                callback_called = true;
                return earnest::execution::just();
              })),
      error);
  // Confirm we can do another write after.
  earnest::execution::sync_wait(
      f->append(
          wal_file_entry_t::write_records_buffer_t(
              std::initializer_list<wal_file_entry_t::write_variant_type>{
                wal_record_skip32{ .bytes = 0 }
              },
              f->get_allocator())));

  CHECK(tx_validator_called);
  CHECK(!callback_called);
  CHECK_EQUAL(60u, f->end_offset());
  CHECK_EQUAL(56u, f->link_offset());

  auto [records] = earnest::execution::sync_wait(f->records()).value();
  CHECK_EQUAL(2u, records.size());
  if (records.size() == 2) {
    // Record from the tx-validation-failure is a skip record.
    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(0)));
    CHECK(wal_record_skip32{12} == std::get<wal_record_skip32>(records.at(0)));

    // This record follows, so it must be written.
    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(1)));
    CHECK(wal_record_skip32{0} == std::get<wal_record_skip32>(records.at(1)));
  }
}

TEST(non_durable_append_wal_file_entry) {
  using namespace std::string_literals;
  using wal_file_entry_t = earnest::detail::wal_file_entry;
  using ::earnest::detail::wal_record_noop;
  using ::earnest::detail::wal_record_skip32;

  // We have to make sure the file doesn't exist, or the test will fail.
  ensure_file_is_gone("durable_append_log");

  auto [f, acceptor] = earnest::execution::sync_wait(
      earnest::detail::wal_file_entry::create(write_dir, "durable_append_log", 17)).value();
  acceptor.assign_values();

  earnest::execution::sync_wait(
      f->append(
          wal_file_entry_t::write_records_buffer_t(
              std::initializer_list<wal_file_entry_t::write_variant_type>{
                wal_record_noop{}, wal_record_skip32{ .bytes = 8 }, wal_record_skip32{ .bytes = 0 }
              },
              f->get_allocator()),
          nullptr,
          nullptr,
          true)
      | earnest::execution::let_value([f]() { return f->records(); })
      | earnest::execution::observe_value(
          [](const auto& records) {
            CHECK_EQUAL(3u, records.size());
            if (records.size() == 3) {
              REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_noop>(records.at(0)));
              CHECK(wal_record_noop{} == std::get<wal_record_noop>(records.at(0)));

              REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(1)));
              CHECK(wal_record_skip32{8} == std::get<wal_record_skip32>(records.at(1)));

              REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(2)));
              CHECK(wal_record_skip32{0} == std::get<wal_record_skip32>(records.at(2)));
            }
          }));
}

TEST(non_durable_append_wal_file_entry_with_succeeding_txvalidation) {
  using namespace std::string_literals;
  using wal_file_entry_t = earnest::detail::wal_file_entry;
  using ::earnest::detail::wal_record_noop;
  using ::earnest::detail::wal_record_skip32;

  // We have to make sure the file doesn't exist, or the test will fail.
  ensure_file_is_gone("durable_txvalidating_append_log");

  auto [f, acceptor] = earnest::execution::sync_wait(
      earnest::detail::wal_file_entry::create(write_dir, "durable_txvalidating_append_log", 17)).value();
  acceptor.assign_values();

  bool tx_validator_called = false;
  earnest::execution::sync_wait(
      f->append(
          wal_file_entry_t::write_records_buffer_t(
              std::initializer_list<wal_file_entry_t::write_variant_type>{
                wal_record_noop{}, wal_record_skip32{ .bytes = 8 }
              },
              f->get_allocator()),
          [&tx_validator_called]() {
            CHECK(!tx_validator_called);
            tx_validator_called = true;
            return earnest::execution::just();
          },
          nullptr,
          true)
      | earnest::execution::let_value([f]() { return f->records(); })
      | earnest::execution::observe_value(
          [](const auto& records) {
            CHECK_EQUAL(2u, records.size());
            if (records.size() == 2) {
              REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_noop>(records.at(0)));
              CHECK(wal_record_noop{} == std::get<wal_record_noop>(records.at(0)));

              REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(1)));
              CHECK(wal_record_skip32{8} == std::get<wal_record_skip32>(records.at(1)));
            }
          }));
  // Confirm we can do another write after.
  earnest::execution::sync_wait(
      f->append(
          wal_file_entry_t::write_records_buffer_t(
              std::initializer_list<wal_file_entry_t::write_variant_type>{
                wal_record_skip32{ .bytes = 0 }
              },
              f->get_allocator())));

  CHECK(tx_validator_called);

  auto [records] = earnest::execution::sync_wait(f->records()).value();
  CHECK_EQUAL(3u, records.size());
  if (records.size() == 3) {
    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_noop>(records.at(0)));
    CHECK(wal_record_noop{} == std::get<wal_record_noop>(records.at(0)));

    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(1)));
    CHECK(wal_record_skip32{8} == std::get<wal_record_skip32>(records.at(1)));

    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(2)));
    CHECK(wal_record_skip32{0} == std::get<wal_record_skip32>(records.at(2)));
  }
}

TEST(non_durable_append_wal_file_entry_with_failing_txvalidation) {
  struct error {};
  using namespace std::string_literals;
  using wal_file_entry_t = earnest::detail::wal_file_entry;
  using ::earnest::detail::wal_record_noop;
  using ::earnest::detail::wal_record_skip32;

  // We have to make sure the file doesn't exist, or the test will fail.
  ensure_file_is_gone("durable_failing_txvalidating_append_log");

  auto [f, acceptor] = earnest::execution::sync_wait(
      earnest::detail::wal_file_entry::create(write_dir, "durable_failing_txvalidating_append_log", 17)).value();
  acceptor.assign_values();

  bool tx_validator_called = false;
  CHECK_THROW(
      earnest::execution::sync_wait(
          f->append(
              wal_file_entry_t::write_records_buffer_t(
                  std::initializer_list<wal_file_entry_t::write_variant_type>{
                    wal_record_noop{}, wal_record_skip32{ .bytes = 8 }
                  },
                  f->get_allocator()),
              [&tx_validator_called]() -> decltype(earnest::execution::just()) {
                CHECK(!tx_validator_called);
                tx_validator_called = true;
                throw error{};
              },
              nullptr,
              true)),
      error);
  // Confirm we can do another write after.
  earnest::execution::sync_wait(
      f->append(
          wal_file_entry_t::write_records_buffer_t(
              std::initializer_list<wal_file_entry_t::write_variant_type>{
                wal_record_skip32{ .bytes = 0 }
              },
              f->get_allocator())));

  CHECK(tx_validator_called);

  auto [records] = earnest::execution::sync_wait(f->records()).value();
  CHECK_EQUAL(2u, records.size());
  if (records.size() == 2) {
    // Record from the tx-validation-failure is a skip record.
    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(0)));
    CHECK(wal_record_skip32{12} == std::get<wal_record_skip32>(records.at(0)));

    // This record follows, so it must be written.
    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(1)));
    CHECK(wal_record_skip32{0} == std::get<wal_record_skip32>(records.at(1)));
  }
}

TEST(non_durable_append_wal_file_entry_callback) {
  using namespace std::string_literals;
  using wal_file_entry_t = earnest::detail::wal_file_entry;
  using ::earnest::detail::wal_record_noop;
  using ::earnest::detail::wal_record_skip32;

  // We have to make sure the file doesn't exist, or the test will fail.
  ensure_file_is_gone("durable_append_log");

  auto [f, acceptor] = earnest::execution::sync_wait(
      earnest::detail::wal_file_entry::create(write_dir, "durable_append_log", 17)).value();
  acceptor.assign_values();

  bool callback_called = false;
  earnest::execution::sync_wait(
      f->append(
          wal_file_entry_t::write_records_buffer_t(
              std::initializer_list<wal_file_entry_t::write_variant_type>{
                wal_record_noop{}, wal_record_skip32{ .bytes = 8 }, wal_record_skip32{ .bytes = 0 }
              },
              f->get_allocator()),
          nullptr,
          [&callback_called](const auto& records) {
            CHECK_EQUAL(3u, records.size());
            CHECK(wal_record_noop{} == std::get<wal_record_noop>(records.at(0)));
            CHECK(wal_record_skip32{8} == std::get<wal_record_skip32>(records.at(1)));
            CHECK(wal_record_skip32{0} == std::get<wal_record_skip32>(records.at(2)));
            callback_called = true;
            return earnest::execution::just();
          },
          true)
      | earnest::execution::let_value([f]() { return f->records(); })
      | earnest::execution::observe_value(
          [](const auto& records) {
            CHECK_EQUAL(3u, records.size());
            if (records.size() == 3) {
              REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_noop>(records.at(0)));
              CHECK(wal_record_noop{} == std::get<wal_record_noop>(records.at(0)));

              REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(1)));
              CHECK(wal_record_skip32{8} == std::get<wal_record_skip32>(records.at(1)));

              REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(2)));
              CHECK(wal_record_skip32{0} == std::get<wal_record_skip32>(records.at(2)));
            }
          }));

  CHECK(callback_called);

  auto [records] = earnest::execution::sync_wait(f->records()).value();
  CHECK_EQUAL(3u, records.size());
  if (records.size() == 3) {
    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_noop>(records.at(0)));
    CHECK(wal_record_noop{} == std::get<wal_record_noop>(records.at(0)));

    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(1)));
    CHECK(wal_record_skip32{8} == std::get<wal_record_skip32>(records.at(1)));

    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(2)));
    CHECK(wal_record_skip32{0} == std::get<wal_record_skip32>(records.at(2)));
  }
}

TEST(non_durable_append_wal_file_entry_with_succeeding_txvalidation_callback) {
  using namespace std::string_literals;
  using wal_file_entry_t = earnest::detail::wal_file_entry;
  using ::earnest::detail::wal_record_noop;
  using ::earnest::detail::wal_record_skip32;

  // We have to make sure the file doesn't exist, or the test will fail.
  ensure_file_is_gone("durable_txvalidating_append_log");

  auto [f, acceptor] = earnest::execution::sync_wait(
      earnest::detail::wal_file_entry::create(write_dir, "durable_txvalidating_append_log", 17)).value();
  acceptor.assign_values();

  bool tx_validator_called = false;
  bool callback_called = false;
  earnest::execution::sync_wait(
      f->append(
          wal_file_entry_t::write_records_buffer_t(
              std::initializer_list<wal_file_entry_t::write_variant_type>{
                wal_record_noop{}, wal_record_skip32{ .bytes = 8 }
              },
              f->get_allocator()),
          [&tx_validator_called]() {
            CHECK(!tx_validator_called);
            tx_validator_called = true;
            return earnest::execution::just();
          },
          [&callback_called](const auto& records) {
            CHECK_EQUAL(2u, records.size());
            CHECK(wal_record_noop{} == std::get<wal_record_noop>(records.at(0)));
            CHECK(wal_record_skip32{8} == std::get<wal_record_skip32>(records.at(1)));
            callback_called = true;
            return earnest::execution::just();
          },
          true)
      | earnest::execution::let_value([f]() { return f->records(); })
      | earnest::execution::observe_value(
          [](const auto& records) {
            CHECK_EQUAL(2u, records.size());
            if (records.size() == 2) {
              REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_noop>(records.at(0)));
              CHECK(wal_record_noop{} == std::get<wal_record_noop>(records.at(0)));

              REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(1)));
              CHECK(wal_record_skip32{8} == std::get<wal_record_skip32>(records.at(1)));
            }
          }));
  // Confirm we can do another write after.
  earnest::execution::sync_wait(
      f->append(
          wal_file_entry_t::write_records_buffer_t(
              std::initializer_list<wal_file_entry_t::write_variant_type>{
                wal_record_skip32{ .bytes = 0 }
              },
              f->get_allocator())));

  CHECK(tx_validator_called);
  CHECK(callback_called);
  CHECK_EQUAL(60u, f->end_offset());
  CHECK_EQUAL(56u, f->link_offset());

  {
    earnest::fd raw_file;
    raw_file.open(write_dir, "durable_txvalidating_append_log", earnest::open_mode::READ_ONLY);
    CHECK_EQUAL(
        hex_string("\013\013earnest.wal\000\000\000\000\000\000\000\000\000\000\000\000\000\000\021"s
            + "\000\000\000\001"s // wal_record_noop
            + "\000\000\000\002\000\000\000\010\000\000\000\000\000\000\000\000"s // wal_record_skip32(8)
            + "\000\000\000\002\000\000\000\000"s // wal_record_skip32(0)
            + "\000\000\000\000"s // sentinel (wal_record_end_of_records)
            ),
        hex_string(raw_file.contents<std::string>()));
  }

  auto [records] = earnest::execution::sync_wait(f->records()).value();
  CHECK_EQUAL(3u, records.size());
  if (records.size() == 3) {
    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_noop>(records.at(0)));
    CHECK(wal_record_noop{} == std::get<wal_record_noop>(records.at(0)));

    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(1)));
    CHECK(wal_record_skip32{8} == std::get<wal_record_skip32>(records.at(1)));

    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(2)));
    CHECK(wal_record_skip32{0} == std::get<wal_record_skip32>(records.at(2)));
  }
}

TEST(non_durable_append_wal_file_entry_with_failing_txvalidation_callback) {
  struct error {};
  using namespace std::string_literals;
  using wal_file_entry_t = earnest::detail::wal_file_entry;
  using ::earnest::detail::wal_record_noop;
  using ::earnest::detail::wal_record_skip32;

  // We have to make sure the file doesn't exist, or the test will fail.
  ensure_file_is_gone("durable_failing_txvalidating_append_log");

  auto [f, acceptor] = earnest::execution::sync_wait(
      earnest::detail::wal_file_entry::create(write_dir, "durable_failing_txvalidating_append_log", 17)).value();
  acceptor.assign_values();

  bool tx_validator_called = false;
  bool callback_called = false;
  CHECK_THROW(
      earnest::execution::sync_wait(
          f->append(
              wal_file_entry_t::write_records_buffer_t(
                  std::initializer_list<wal_file_entry_t::write_variant_type>{
                    wal_record_noop{}, wal_record_skip32{ .bytes = 8 }
                  },
                  f->get_allocator()),
              [&tx_validator_called]() -> decltype(earnest::execution::just()) {
                CHECK(!tx_validator_called);
                tx_validator_called = true;
                throw error{};
              },
              [&callback_called]([[maybe_unused]] const auto& records) {
                callback_called = true;
                return earnest::execution::just();
              },
              true)),
      error);
  // Confirm we can do another write after.
  earnest::execution::sync_wait(
      f->append(
          wal_file_entry_t::write_records_buffer_t(
              std::initializer_list<wal_file_entry_t::write_variant_type>{
                wal_record_skip32{ .bytes = 0 }
              },
              f->get_allocator())));

  CHECK(tx_validator_called);
  CHECK(!callback_called);

  auto [records] = earnest::execution::sync_wait(f->records()).value();
  CHECK_EQUAL(2u, records.size());
  if (records.size() == 2) {
    // Record from the tx-validation-failure is a skip record.
    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(0)));
    CHECK(wal_record_skip32{12} == std::get<wal_record_skip32>(records.at(0)));

    // This record follows, so it must be written.
    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(1)));
    CHECK(wal_record_skip32{0} == std::get<wal_record_skip32>(records.at(1)));
  }
}

TEST(seal_wal_file_entry) {
  using namespace std::string_literals;
  using wal_file_entry_t = earnest::detail::wal_file_entry;
  using ::earnest::detail::wal_record_noop;
  using ::earnest::detail::wal_record_skip32;
  using ::earnest::detail::wal_record_seal;

  // We have to make sure the file doesn't exist, or the test will fail.
  ensure_file_is_gone("seal_log");

  auto [f, acceptor] = earnest::execution::sync_wait(
      earnest::detail::wal_file_entry::create(write_dir, "seal_log", 17)).value();
  acceptor.assign_values();

  earnest::execution::sync_wait(
      f->append(
          wal_file_entry_t::write_records_buffer_t(
              std::initializer_list<wal_file_entry_t::write_variant_type>{
                wal_record_noop{}, wal_record_skip32{ .bytes = 8 }, wal_record_skip32{ .bytes = 0 }
              },
              f->get_allocator())));

  earnest::execution::sync_wait(f->seal());

  CHECK_EQUAL(64u, f->end_offset());
  CHECK_EQUAL(60u, f->link_offset());
  CHECK_EQUAL(::earnest::detail::wal_file_entry_state::sealed, f->state());

  {
    earnest::fd raw_file;
    raw_file.open(write_dir, "seal_log", earnest::open_mode::READ_ONLY);
    CHECK_EQUAL(
        hex_string("\013\013earnest.wal\000\000\000\000\000\000\000\000\000\000\000\000\000\000\021"s
            + "\000\000\000\001"s // wal_record_noop
            + "\000\000\000\002\000\000\000\010\000\000\000\000\000\000\000\000"s // wal_record_skip32(8)
            + "\000\000\000\002\000\000\000\000"s // wal_record_skip32(0)
            + "\000\000\000\004"s // wal_record_seal
            + "\000\000\000\000"s // sentinel (wal_record_end_of_records)
            ),
        hex_string(raw_file.contents<std::string>()));
  }

  auto [records] = earnest::execution::sync_wait(f->records()).value();
  CHECK_EQUAL(4u, records.size());
  if (records.size() == 4) {
    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_noop>(records.at(0)));
    CHECK(wal_record_noop{} == std::get<wal_record_noop>(records.at(0)));

    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(1)));
    CHECK(wal_record_skip32{8} == std::get<wal_record_skip32>(records.at(1)));

    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_skip32>(records.at(2)));
    CHECK(wal_record_skip32{0} == std::get<wal_record_skip32>(records.at(2)));

    REQUIRE CHECK(std::holds_alternative<::earnest::detail::wal_record_seal>(records.at(3)));
    CHECK(wal_record_seal{} == std::get<wal_record_seal>(records.at(3)));
  }
}

#if 0
TEST(discard_all) {
  using namespace std::string_literals;
  using wal_file_entry_t = earnest::detail::wal_file_entry;
  using ::earnest::detail::wal_record_noop;
  using ::earnest::detail::wal_record_skip32;

  // We have to make sure the file doesn't exist, or the test will fail.
  ensure_file_is_gone("discard_all");

  asio::io_context ioctx;
  auto f = std::make_shared<wal_file_entry_t>(std::allocator<std::byte>());
  // Preparation: create a file with data.
  f->async_create(write_dir, "discard_all", 17,
      [&f](std::error_code ec, auto link_done_event) {
        REQUIRE CHECK_EQUAL(std::error_code(), ec);
        std::invoke(link_done_event, std::error_code());

        f->async_append(std::initializer_list<wal_file_entry_t::write_variant_type>{
              wal_record_noop{}, wal_record_skip32{ .bytes = 8 }, wal_record_skip32{ .bytes = 0 }
            },
            [&](std::error_code ec) {
              REQUIRE CHECK_EQUAL(std::error_code(), ec);
            });
      });
  ioctx.run();
  ioctx.restart();

  bool completion_all_was_called = false;
  f->async_discard_all(
      [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
        completion_all_was_called = true;
      });
  ioctx.run();
  CHECK(completion_all_was_called);

  CHECK_EQUAL(36u, f->write_offset());
  CHECK_EQUAL(32u, f->link_offset());
  CHECK_EQUAL(::earnest::detail::wal_file_entry_state::sealed, f->state());


  CHECK_EQUAL(
      hex_string("\013\013earnest.wal\000\000\000\000\000\000\000\000\000\000\000\000\000\000\021"s
          + "\000\000\000\004"s // wal_record_seal
          + "\000\000\000\000"s // sentinel (std::monostate)
          ),
      hex_string(f->file.contents<std::string>().substr(0, 36)));
}
#endif

int main(int argc, char** argv) {
  if (argc < 3) {
    std::cerr << "Usage:  " << (argc > 0 ? argv[0] : "wal_file_entry_test") << " wal_source_dir writeable_dir\n"
        << "  wal_source_dir:  points at the directory containing test files\n"
        << "  writeable_dir:   points at a directory where we can write files\n";
    return 1;
  }
  try {
    source_files = earnest::dir(argv[1]);
    write_dir = earnest::dir(argv[2]);
  } catch (const std::exception& e) {
    std::cerr << "error opening dirs: " << e.what() << std::endl;
    return 1;
  }

  return UnitTest::RunAllTests();
}
