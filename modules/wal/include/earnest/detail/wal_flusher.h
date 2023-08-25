#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <system_error>
#include <type_traits>
#include <utility>

#include <asio/error.hpp>
#include <asio/steady_timer.hpp>

#include <earnest/detail/fanout.h>
#include <earnest/detail/move_only_function.h>

namespace earnest::detail {


/* Controls the async_flush operation on a flushable (fd).
 * Ensure at most 1 flush runs in parallel.
 * Note: flush only guarantees that completed writes will be flushed;
 * aio-writes that are in flight may or may not get flushed.
 */
template<typename AsyncFlushable, typename Allocator = typename std::remove_cvref_t<AsyncFlushable>::allocator_type>
class wal_flusher {
  private:
  static inline constexpr std::uint32_t state_needed = 0x01;
  static inline constexpr std::uint32_t state_running = 0x02;
  // Time we delay a flush, if delayed was set.
  static inline constexpr auto delay_interval = std::chrono::seconds(15u);

  public:
  using executor_type = typename std::remove_cvref_t<AsyncFlushable>::executor_type;
  using allocator_type = Allocator;

  private:
  using fanout_type = fanout<executor_type, void(std::error_code)>;
  template<typename T> using allocator_type_for = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;

  public:
  explicit wal_flusher(AsyncFlushable flushable, allocator_type alloc)
  : flushable_(std::forward<AsyncFlushable>(flushable)),
    fanout_(this->flushable_.get_executor(), alloc),
    timer_(this->flushable_.get_executor().context())
  {}

  /* Start a new flush operation.
   *
   * - data_only: if true, an fdatasync will be executed, otherwise an fsync will be executed.
   * - token: asio completion token, invoked after the flush completes.
   * - delay_start: don't start the flush.
   *
   * If delay_start is set, the flush operation won't start immediately, but instead wait until
   * another operation requests a flush.
   */
  template<typename CompletionToken>
  auto async_flush(bool data_only, CompletionToken&& token, bool delay_start = false) {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [this](auto handler, bool data_only, bool delay_start) -> void {
          std::scoped_lock lck{this->mtx_};

          this->fanout_.async_on_ready(std::move(handler));

          (data_only ? this->data_only_needed_ : this->all_needed_) = true;
          if (delay_start) {
            start_timer_();
          } else {
            this->delay_ = false;
            if (!this->running_) this->start_();
          }
        },
        token, data_only, delay_start);
  }

  private:
  void start_() {
    using std::swap;

    assert(!running_);
    timer_.cancel();
    timer_started_ = false;

    auto f = fanout_type(fanout_.get_executor(), fanout_.get_allocator());
    swap(fanout_, f);
    f.async_on_ready(
        [this]([[maybe_unused]] std::error_code ec) {
          // We ignore ec: just because the last flush failed,
          // doesn't mean we should never run another again. :)
          std::scoped_lock lck{mtx_};
          running_ = false;
          if (!delay_ && (data_only_needed_ || all_needed_)) start_();
        });

    flushable_.async_flush(!all_needed_, std::move(f));
    data_only_needed_ = all_needed_ = false;
    running_ = true;
    delay_ = true;
  }

  void start_timer_() {
    if (timer_started_) return;

    timer_.expires_after(delay_interval);
    timer_.async_wait(
        [this](std::error_code ec) {
          if (ec == asio::error::operation_aborted) return;
          assert(!ec);

          std::scoped_lock lck{this->mtx_};
          if (!this->running_) start_();
        });
    timer_started_ = true;
  }

  AsyncFlushable flushable_;
  fanout_type fanout_;
  bool data_only_needed_ = false, all_needed_ = false, running_ = false, delay_ = true, timer_started_ = false;
  std::mutex mtx_;
  asio::steady_timer timer_;
};


} /* namespace earnest::detail */
