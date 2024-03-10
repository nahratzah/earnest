#pragma once

#include <exception>
#include <memory>
#include <mutex>
#include <system_error>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <asio.hpp>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/spdlog.h>

#include <earnest/detail/completion_handler_fun.h>
#include <earnest/execution.h>
#include <earnest/execution_io.h>
#include <earnest/execution_util.h>

namespace earnest::detail {


/* Controls the async_flush operation on a flushable (fd).
 * Ensure at most 1 flush runs in parallel.
 * Note: flush only guarantees that completed writes will be flushed;
 * aio-writes that are in flight may or may not get flushed.
 */
template<typename AsyncFlushable, typename Allocator = typename std::remove_cvref_t<AsyncFlushable>::allocator_type>
class wal_flusher {
  private:
  class sender_impl;

  public:
  using allocator_type = Allocator;

  private:
  template<typename T> using allocator_type_for = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;

  public:
  explicit wal_flusher(AsyncFlushable flushable, allocator_type alloc)
  : flushable_(std::forward<AsyncFlushable>(flushable)),
    pending_(alloc)
  {}

  public:
  /* Start a new flush operation.
   *
   * - data_only: if true, an fdatasync will be executed, otherwise an fsync will be executed.
   * - delay_start: don't start the flush.
   *
   * If delay_start is set, the flush operation won't start immediately, but instead wait until
   * another operation requests a flush.
   */
  auto lazy_flush(bool data_only, bool delay_start = false) -> sender_impl {
    return sender_impl(*this, data_only, delay_start);
  }

  private:
  static inline auto get_logger() -> std::shared_ptr<spdlog::logger> {
    std::shared_ptr<spdlog::logger> logger = spdlog::get("earnest.wal.flusher");
    if (!logger) logger = std::make_shared<spdlog::logger>("earnest.wal.flusher", std::make_shared<spdlog::sinks::null_sink_mt>());
    return logger;
  }

  class opstate_intf {
    friend class opstate_intf_receiver;

    protected:
    opstate_intf() = default;

    public:
    virtual ~opstate_intf() = default;

    virtual void set_error(std::exception_ptr ex) noexcept = 0;
    virtual void set_error(std::error_code ec) noexcept = 0;
    virtual void set_value() noexcept = 0;
    virtual void set_done() noexcept = 0;
  };

  class opstate_intf_receiver {
    public:
    explicit opstate_intf_receiver(opstate_intf* o) noexcept
    : o(o)
    {}

    opstate_intf_receiver(const opstate_intf_receiver&) = delete;

    opstate_intf_receiver(opstate_intf_receiver&& other) noexcept
    : o(std::exchange(other.o, nullptr))
    {}

    ~opstate_intf_receiver() {
      // There is a requirement we always invoke the receiver.
      // So if the destructor runs prior to the receiver having been given a value,
      // we report an exception.
      // We expect this only happens during stack-unwinding, thus `std::current_exception()'
      // being correct.
      if (o != nullptr) {
        std::exception_ptr exptr = (std::uncaught_exceptions() ?
            std::current_exception() :
            std::make_exception_ptr(std::runtime_error("wal-flusher: receiver destroyed without being invoked")));
        o->set_error(std::move(exptr));
        o = nullptr;
      }
    }

    auto set_value() noexcept -> void {
      o->set_value();
      o = nullptr;
    }

    auto set_error(std::exception_ptr ex) noexcept -> void {
      o->set_error(std::move(ex));
      o = nullptr;
    }

    auto set_error(std::error_code ec) noexcept -> void {
      o->set_error(std::move(ec));
      o = nullptr;
    }

    auto set_done() noexcept -> void {
      o->set_done();
      o = nullptr;
    }

    private:
    opstate_intf* o;
  };

  using pending_vector = std::vector<opstate_intf_receiver, allocator_type_for<opstate_intf_receiver>>;

  template<execution::receiver_of<> Receiver>
  class opstate
  : public opstate_intf,
    public execution::operation_state_base_
  {
    public:
    explicit opstate(wal_flusher& flusher, Receiver&& r, bool data_only, bool delay_start)
    : flusher(flusher),
      r(std::move(r)),
      data_only(data_only),
      delay_start(delay_start)
    {}

    ~opstate() override = default;

    friend auto tag_invoke([[maybe_unused]] execution::start_t tag, opstate& self) noexcept -> void {
      try {
        self.flusher.add(&self, self.data_only, self.delay_start);
      } catch (...) {
        execution::set_error(std::move(self.r), std::current_exception());
      }
    }

    void set_error(std::exception_ptr ex) noexcept override {
      execution::set_error(std::move(r), std::move(ex));
    }

    void set_error(std::error_code ec) noexcept override {
      execution::set_error(std::move(r), std::move(ec));
    }

    void set_value() noexcept override {
      try {
        execution::set_value(std::move(r));
      } catch (...) {
        execution::set_error(std::move(r), std::current_exception());
      }
    }

    void set_done() noexcept override {
      assert(false); // We don't advertise the sending of `done'.
      execution::set_done(std::move(r));
    }

    private:
    wal_flusher& flusher;
    Receiver r;
    bool data_only, delay_start;
  };

  class sender_impl {
    public:
    // We only have the no-argument value-type.
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<Tuple<>>;

    // We produce either exceptions, or error-codes.
    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr, std::error_code>;

    // We don't send the done signal.
    static inline constexpr bool sends_done = false;

    explicit sender_impl(wal_flusher& flusher, bool data_only, bool delay_start) noexcept
    : flusher(flusher),
      data_only(data_only),
      delay_start(delay_start)
    {}

    template<execution::receiver_of<> Receiver>
    requires execution::receiver<Receiver, std::error_code>
    friend auto tag_invoke([[maybe_unused]] execution::connect_t tag, sender_impl&& self, Receiver&& r) -> opstate<std::remove_cvref_t<Receiver>> {
      return opstate<std::remove_cvref_t<Receiver>>(self.flusher, std::forward<Receiver>(r), self.data_only, self.delay_start);
    }

    private:
    wal_flusher& flusher;
    bool data_only, delay_start;
  };

  auto add(opstate_intf* o, bool data_only, bool delay_start) -> void {
    std::unique_lock lck{mtx_};
    pending_.emplace_back(o);
    (data_only ? this->data_only_needed_ : this->all_needed_) = true;

    if (delay_start) {
      start_timer_(lck); // When a delay is permitted, we start a timer.
                         // This allows more flush operations to join this operation.
    } else {
      delay_ = false;
      start_(lck); // never throws
    }
  }

  auto start_timer_(std::unique_lock<std::mutex>& lck) noexcept -> void {
    // XXX Since timer logic isn't implemented at this point, just start immediately.
    if (pending_.size() > 128) {
      delay_ = false;
      start_(lck);
    }
  }

  auto start_(std::unique_lock<std::mutex>& lck) noexcept -> void {
    assert(lck.owns_lock());
    if (running_) return;

    running_ = true;
    start_op_(lck);
  }

  template<execution::receiver Receiver>
  class complete_pending_receiver
  : public execution::_generic_receiver_wrapper<Receiver, execution::set_value_t, execution::set_error_t, execution::set_done_t>
  {
    public:
    explicit complete_pending_receiver(Receiver&& r, pending_vector&& pending)
    : execution::_generic_receiver_wrapper<Receiver, execution::set_value_t, execution::set_error_t, execution::set_done_t>(std::move(r)),
      pending(std::move(pending))
    {}

    complete_pending_receiver(const complete_pending_receiver&) = delete;
    complete_pending_receiver(complete_pending_receiver&&) noexcept = default;

    friend auto tag_invoke([[maybe_unused]] execution::set_value_t tag, complete_pending_receiver&& self) noexcept -> void {
      for (auto& r : self.pending) r.set_value();
      execution::set_value(std::move(self.r));
    }

    friend auto tag_invoke([[maybe_unused]] execution::set_error_t tag, complete_pending_receiver&& self, std::exception_ptr ex) noexcept -> void {
      for (auto& r : self.pending) r.set_error(ex);
      execution::set_value(std::move(self.r));
    }

    friend auto tag_invoke([[maybe_unused]] execution::set_error_t tag, complete_pending_receiver&& self, std::error_code ec) noexcept -> void {
      for (auto& r : self.pending) r.set_error(ec);
      execution::set_value(std::move(self.r));
    }

    friend auto tag_invoke([[maybe_unused]] execution::set_done_t tag, complete_pending_receiver&& self) noexcept -> void {
      for (auto& r : self.pending) r.set_done();
      execution::set_value(std::move(self.r));
    }

    private:
    pending_vector pending;
  };

  template<execution::sender Sender>
  class complete_pending_sender
  : public execution::_generic_sender_wrapper<complete_pending_sender<Sender>, Sender, execution::connect_t>
  {
    public:
    template<template<typename...> class Tuple, template<typename...> class Variant>
    using value_types = Variant<Tuple<>>;

    template<template<typename...> class Variant>
    using error_types = Variant<std::exception_ptr>;

    static inline constexpr bool sends_done = false;

    explicit complete_pending_sender(Sender&& s, pending_vector&& pending)
        noexcept(std::is_nothrow_move_constructible_v<Sender>)
    : execution::_generic_sender_wrapper<complete_pending_sender<Sender>, Sender, execution::connect_t>(std::move(s)),
      pending(std::move(pending))
    {}

    template<execution::receiver Receiver>
    friend auto tag_invoke([[maybe_unused]] execution::connect_t tag, complete_pending_sender&& self, Receiver&& r) {
      return execution::connect(
          std::move(self.s),
          complete_pending_receiver<std::remove_cvref_t<Receiver>>(std::forward<Receiver>(r), std::move(self.pending)));
    }

    private:
    pending_vector pending;
  };

  struct make_complete_pending_sender_t {
    template<execution::sender S>
    constexpr auto operator()(S&& s, pending_vector&& pending) const -> complete_pending_sender<std::remove_cvref_t<S>> {
      return complete_pending_sender<std::remove_cvref_t<S>>(std::forward<S>(s), std::move(pending));
    }

    auto operator()(pending_vector&& pending) const {
      return execution::_generic_adapter(*this, std::move(pending));
    }
  };
  static inline constexpr make_complete_pending_sender_t make_complete_pending_sender{};

  auto start_op_(std::unique_lock<std::mutex>& lck) -> void {
    using namespace execution;

    auto sync_operation = [](wal_flusher* self) -> sender_of</*void()*/> auto {
      return io::sync_ec(self->flushable_);
    };
    auto datasync_operation = [](wal_flusher* self) -> sender_of</*void()*/> auto {
      return io::datasync_ec(self->flushable_);
    };

    using variant_type = std::variant<
        decltype(sync_operation(std::declval<wal_flusher*>())),
        decltype(datasync_operation(std::declval<wal_flusher*>()))>;

    const bool data_only_needed = data_only_needed_ && !all_needed_;
    pending_vector pending(this->pending_.get_allocator());
    swap(pending, this->pending_);

    // Reset.
    data_only_needed_ = all_needed_ = false;
    delay_ = true;
    // And unlock.
    lck.unlock();

    start_detached(
        just(this, data_only_needed) // XXX use on(), so that the flush can be an async operation;
                                     // when adding on(), also ensure the scheduler is advertised.
        | let_variant(
            [sync_operation, datasync_operation](wal_flusher* self, bool data_only_needed) -> variant_type {
              std::lock_guard lck{self->mtx_};

              if (data_only_needed) {
                self->logger->trace("starting datasync");
                return variant_type(std::in_place_index<1>, datasync_operation(self));
              } else {
                self->logger->trace("starting sync");
                return variant_type(std::in_place_index<0>, sync_operation(self));
              }
            })
        | make_complete_pending_sender(std::move(pending)) // any errors are handled and consumed here
        | lazy_then(
            [this]() noexcept -> void {
              // Once we're done with this flush,
              // we can either start the next flush,
              // or stop if there's nothing pending.
              std::unique_lock lck{this->mtx_};
              if (!this->delay_)
                this->start_op_(lck);
              else
                this->running_ = false;
            }));
  }

  AsyncFlushable flushable_;
  bool data_only_needed_ = false, all_needed_ = false, running_ = false, delay_ = true;
  std::mutex mtx_;
  pending_vector pending_;
  const std::shared_ptr<spdlog::logger> logger = get_logger();
};


} /* namespace earnest::detail */
