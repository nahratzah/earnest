#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string_view>
#include <system_error>
#include <utility>

#include <earnest/detail/logger.h>
#include <earnest/detail/replacement_map.h>
#include <earnest/detail/wal_records.h>
#include <earnest/file_db_error.h>

namespace earnest::detail {


template<typename Allocator = std::allocator<std::byte>>
struct file_recover_state
: private with_logger
{
  using allocator_type = Allocator;

  explicit file_recover_state(allocator_type alloc = allocator_type())
  : with_logger(get_file_recover_state_logger()),
    replacements(alloc)
  {}

  file_recover_state(const file_recover_state& y, allocator_type alloc)
  : with_logger(y),
    replacements(y.replacements, alloc),
    exists(y.exists),
    file_size(y.file_size),
    omit_reads(y.omit_reads)
  {}

  file_recover_state(file_recover_state&& y, allocator_type alloc)
  : with_logger(y.logger),
    replacements(std::move(y.replacements), alloc),
    exists(std::move(y.exists)),
    file_size(std::move(y.file_size)),
    omit_reads(std::move(y.omit_reads))
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

  auto apply(const wal_record_modify_file32& record) -> std::error_code {
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

  replacement_map<allocator_type> replacements;
  std::optional<bool> exists = std::nullopt;
  std::optional<std::uint64_t> file_size = std::nullopt;
  bool omit_reads = false;
};


} /* namespace earnest::detail */
