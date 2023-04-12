#pragma once

#include <cstddef>
#include <mutex>
#include <queue>
#include <utility>

#include <asio/defer.hpp>
#include <cycle_ptr.h>

namespace earnest::detail {


class asio_cycle_ptr {
  public:
  template<typename Executor>
  explicit asio_cycle_ptr(Executor ex) {
    cycle_ptr::set_delay_gc(
        [ex](cycle_ptr::gc_operation op) {
          asio::defer(ex, std::move(op));
        });
  }

  asio_cycle_ptr(const asio_cycle_ptr&) = delete;
  asio_cycle_ptr(asio_cycle_ptr&&) = delete;
  asio_cycle_ptr& operator=(const asio_cycle_ptr&) = delete;
  asio_cycle_ptr& operator=(asio_cycle_ptr&&) = delete;

  ~asio_cycle_ptr() {
    cycle_ptr::set_delay_gc(nullptr);
  }
};


} /* namespace earnest::detail */
