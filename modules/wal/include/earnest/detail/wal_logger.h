#pragma once

#include <memory>

#include <spdlog/sinks/null_sink.h>
#include <spdlog/spdlog.h>

namespace earnest::detail {


inline auto get_wal_logger() -> std::shared_ptr<spdlog::logger> {
  std::shared_ptr<spdlog::logger> logger = spdlog::get("earnest.wal");
  if (!logger) logger = std::make_shared<spdlog::logger>("earnest.wal", std::make_shared<spdlog::sinks::null_sink_mt>());
  return logger;
}


} /* namespace earnest::detail */
