#pragma once

#include <array>
#include <memory>
#include <string>
#include <string_view>

#include <gsl/gsl>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/spdlog.h>

namespace earnest::detail {


struct log_config {
  constexpr log_config(std::string_view name) noexcept
  : name(name)
  {}

  auto operator()() const -> gsl::not_null<std::shared_ptr<spdlog::logger>> {
    std::shared_ptr<spdlog::logger> logger = spdlog::get(std::string(name));
    if (!logger) logger = std::make_shared<spdlog::logger>(std::string(name), std::make_shared<spdlog::sinks::null_sink_mt>());
    return logger;
  }

  std::string_view name;
};


inline constexpr log_config get_fdb_logger("earnest.file_db");
inline constexpr log_config get_fdb_transaction_logger("earnest.file_db.transaction");
inline constexpr log_config get_file_recover_state_logger("earnest.file_db.file_recover_state");
inline constexpr log_config get_wal_logger("earnest.wal");
inline constexpr log_config get_wal_entry_logger("earnest.wal.entry");

inline constexpr std::array<log_config, 5> all_loggers = {
  get_fdb_logger,
  get_fdb_transaction_logger,
  get_file_recover_state_logger,
  get_wal_logger,
  get_wal_entry_logger,
};


class with_logger {
  protected:
  explicit with_logger(gsl::not_null<std::shared_ptr<spdlog::logger>> logger) noexcept
  : logger(logger)
  {}

  explicit with_logger(log_config cfg)
  : with_logger(cfg())
  {}

  with_logger(const with_logger& y) noexcept
  : with_logger(y.logger)
  {}

  ~with_logger() = default;

  auto operator=(const with_logger& y) noexcept -> with_logger& {
    logger = y.logger;
    return *this;
  }

  auto operator==(const with_logger& y) const noexcept -> bool {
    return true;
  }

  auto operator!=(const with_logger& y) const noexcept -> bool {
    return false;
  }

  gsl::not_null<std::shared_ptr<spdlog::logger>> logger;
};


} /* namespace earnest::detail */
