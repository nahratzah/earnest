#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string_view>
#include <system_error>
#include <utility>

#include <spdlog/spdlog.h>

#include <earnest/detail/fdb_logger.h>
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
      logger->error("{}: file was double-created", record.file);
      return make_error_code(file_db_errc::unrecoverable);
    }

    exists = true;
    file_size = 0;
    replacements.clear();
    return {};
  }

  auto apply(const wal_record_erase_file& record) -> std::error_code {
    if (exists.value_or(true) != true) [[unlikely]] {
      logger->error("{}: file was double-erased", record.file);
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
      logger->error("{}: file doesn't exist, but has logs of file truncation", record.file);
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
      logger->error("{}: file doesn't exist, but has logs of written data", record.file);
      return make_error_code(file_db_errc::unrecoverable);
    }

    if (record.file_offset + record.wal_len < record.file_offset) [[unlikely]] { // overflow
      logger->error("{}: overflow in WAL write record", record.file);
      return make_error_code(file_db_errc::unrecoverable);
    } else if (file_size.has_value() && record.file_offset + record.wal_len > file_size.value()) [[unlikely]] { // out-of-bound write
      logger->error("{}: write record extends past end of file", record.file);
      return make_error_code(file_db_errc::unrecoverable);
    }

    if (!omit_reads) replacements.insert(record);
    return {};
  }

  replacement_map<fd_type, allocator_type> replacements;
  std::optional<bool> exists = std::nullopt;
  std::optional<std::uint64_t> file_size = std::nullopt;
  bool omit_reads = false;

  private:
  const std::shared_ptr<spdlog::logger> logger = get_file_recover_state_logger();
};


} /* namespace earnest::detail */
