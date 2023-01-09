#pragma once

#include <cstdint>
#include <mutex>
#include <system_error>
#include <type_traits>
#include <utility>

#include <earnest/detail/fanout.h>

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

  public:
  using executor_type = typename std::remove_cvref_t<AsyncFlushable>::executor_type;
  using allocator_type = Allocator;

  explicit wal_flusher(AsyncFlushable flushable, allocator_type alloc)
  : flushable_(std::forward<AsyncFlushable>(flushable)),
    fanout_(this->flushable_.get_executor(), std::move(alloc))
  {}

  template<typename CompletionToken>
  auto async_flush(bool data_only, CompletionToken&& token) {
    std::scoped_lock lck{mtx_};

    if constexpr(std::is_void_v<std::remove_cvref_t<decltype(fanout_.async_on_ready(std::forward<CompletionToken>(token)))>>) {
      fanout_.async_on_ready(std::forward<CompletionToken>(token));
      (data_only ? data_only_needed_ : all_needed_) = true;
      if (!running_) start_();
    } else {
      auto result = fanout_.async_on_ready(std::forward<CompletionToken>(token));
      (data_only ? data_only_needed_ : all_needed_) = true;
      if (!running_) start_();
      return result;
    }
  }

  private:
  void start_() {
    using std::swap;

    assert(!running_);

    auto f = fanout<executor_type, void(std::error_code)>(fanout_.get_executor(), fanout_.get_allocator());
    swap(fanout_, f);
    f.async_on_ready(
        [this]([[maybe_unused]] std::error_code ec) {
          // We ignore ec: just because the last flush failed,
          // doesn't mean we should never run another again. :)
          std::scoped_lock lck{mtx_};
          running_ = false;
          if (data_only_needed_ || all_needed_) start_();
        });

    flushable_.async_flush(!all_needed_, std::move(f));
    data_only_needed_ = all_needed_ = false;
    running_ = true;
  }

  AsyncFlushable flushable_;
  fanout<executor_type, void(std::error_code)> fanout_;
  bool data_only_needed_ = false, all_needed_ = false, running_ = false;
  std::mutex mtx_;
};


} /* namespace earnest::detail */
