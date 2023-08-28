#pragma once

#include <memory>

#include <spdlog/sinks/null_sink.h>
#include <spdlog/spdlog.h>

namespace earnest::detail {


inline auto get_fdb_logger() -> std::shared_ptr<spdlog::logger> {
  std::shared_ptr<spdlog::logger> logger = spdlog::get("earnest.file_db");
  if (!logger) logger = std::make_shared<spdlog::logger>("earnest.file_db", std::make_shared<spdlog::sinks::null_sink_mt>());
  return logger;
}

inline auto get_fdb_transaction_logger() -> std::shared_ptr<spdlog::logger> {
  std::shared_ptr<spdlog::logger> logger = spdlog::get("earnest.file_db.transaction");
  if (!logger) logger = std::make_shared<spdlog::logger>("earnest.file_db.transaction", std::make_shared<spdlog::sinks::null_sink_mt>());
  return logger;
}

inline auto get_file_recover_state_logger() -> std::shared_ptr<spdlog::logger> {
  std::shared_ptr<spdlog::logger> logger = spdlog::get("earnest.file_db.file_recover_state");
  if (!logger) logger = std::make_shared<spdlog::logger>("earnest.file_db.file_recover_state", std::make_shared<spdlog::sinks::null_sink_mt>());
  return logger;
}


} /* namespace earnest::detail */
