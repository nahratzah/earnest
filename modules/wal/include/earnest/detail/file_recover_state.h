#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <optional>
#include <string_view>
#include <system_error>
#include <utility>

#include <earnest/detail/replacement_map.h>
#include <earnest/detail/wal_records.h>
#include <earnest/file_db_error.h>

namespace earnest::detail {


template<typename FD, typename Allocator>
struct file_recover_state {
  using fd_type = FD;
  using allocator_type = Allocator;

  explicit file_recover_state(allocator_type alloc = allocator_type())
  : replacements(std::move(alloc))
  {}

  auto apply(const wal_record_create_file& record) -> std::error_code {
    if (exists.value_or(false) != false) [[unlikely]] {
      log(record.file) << "file was double-created\n";
      return make_error_code(file_db_errc::unrecoverable);
    }

    exists = true;
    file_size = 0;
    replacements.clear();
    return {};
  }

  auto apply(const wal_record_erase_file& record) -> std::error_code {
    if (exists.value_or(true) != true) [[unlikely]] {
      log(record.file) << "file was double-erased\n";
      return make_error_code(file_db_errc::unrecoverable);
    }

    exists = false;
    file_size = 0;
    replacements.clear();
    return {};
  }

  auto apply(const wal_record_truncate_file& record) -> std::error_code {
    if (!exists.has_value()) exists = true;
    if (!exists.value()) [[unlikely]] {
      log(record.file) << "file doesn't exist, but has logs of file truncation\n";
      return make_error_code(file_db_errc::unrecoverable);
    }

    file_size = record.new_size;
    replacements.truncate(record.new_size);
    return {};
  }

  template<typename E, typename R>
  auto apply(const wal_record_modify_file32<E, R>& record) -> std::error_code {
    if (!exists.has_value()) exists = true;
    if (!exists.value()) [[unlikely]] {
      log(record.file) << "file doesn't exist, but has logs of written data\n";
      return make_error_code(file_db_errc::unrecoverable);
    }

    if (record.file_offset + record.wal_len < record.file_offset) [[unlikely]] { // overflow
      log(record.file) << "overflow in WAL write record\n";
      return make_error_code(file_db_errc::unrecoverable);
    } else if (file_size.has_value() && record.file_offset + record.wal_len > file_size.value()) [[unlikely]] { // out-of-bound write
      log(record.file) << "write record extends past end of file\n";
      return make_error_code(file_db_errc::unrecoverable);
    }

    replacements.insert(record);
    return {};
  }

  private:
  auto log(const file_id& id) const -> std::ostream& {
    using namespace std::string_view_literals;

    return std::clog << "File-DB "sv << id.ns << "/"sv << id.filename << ": "sv;
  }

  public:
  replacement_map<fd_type, allocator_type> replacements;
  std::optional<bool> exists = std::nullopt;
  std::optional<std::uint64_t> file_size = 0;
};


} /* namespace earnest::detail */
