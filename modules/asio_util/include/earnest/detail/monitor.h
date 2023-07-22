#pragma once

#include <algorithm>
#include <cstddef>
#include <deque>
#include <functional>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <earnest/detail/completion_handler_fun.h>
#include <earnest/detail/completion_wrapper.h>
#include <earnest/detail/move_only_function.h>
#include <earnest/detail/on_scope_exit.h>
#include <earnest/detail/overload.h>

#include <asio/async_result.hpp>
#include <boost/intrusive/link_mode.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>
#include <boost/intrusive/options.hpp>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/spdlog.h>

namespace earnest::detail {


class monitor_shared_lock;
template<typename Executor> class monitor_upgrade_lock;
template<typename Executor> class monitor_exclusive_lock;

class monitor_state
: public std::enable_shared_from_this<monitor_state>,
  public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::safe_link>>
{
  public:
  class monitor_debug_impl {
    public:
    void register_new_state(monitor_state* ms) noexcept {
      std::lock_guard lck{mtx};
      all_states.push_back(*ms);
    }

    void deregister_state(monitor_state* ms) noexcept {
      std::lock_guard lck{mtx};
      all_states.erase(all_states.iterator_to(*ms));
    }

    void dump(std::ostream& out, bool only_blocking_locks) {
      std::lock_guard lck{mtx};
      for (const monitor_state& s : all_states)
        s.dump(out, only_blocking_locks);
    }

    private:
    std::mutex mtx;
    boost::intrusive::list<monitor_state, boost::intrusive::constant_time_size<false>> all_states;
  };

  static inline monitor_debug_impl monitor_debug; // singleton

  struct location {
    location(std::string_view file, int line) noexcept
    : file(file),
      line(line)
    {}

    auto operator<=>(const location& y) const noexcept = default;

    std::string_view file;
    int line;
  };

  private:
  struct location_hash {
    auto operator()(const location& loc) const noexcept -> std::size_t {
      std::hash<std::string_view> sv_hash;
      std::hash<int> i_hash;
      return 65537u * sv_hash(loc.file) + i_hash(loc.line);
    }
  };

  protected:
  class counter {
    public:
    using number_type = std::uintptr_t;

    auto operator==(const std::uintptr_t& y) const noexcept -> bool { return n_ == y; }
    auto operator!=(const std::uintptr_t& y) const noexcept -> bool { return n_ != y; }
    auto operator>(const std::uintptr_t& y) const noexcept -> bool { return n_ > y; }
    auto operator<(const std::uintptr_t& y) const noexcept -> bool { return n_ < y; }
    auto operator>=(const std::uintptr_t& y) const noexcept -> bool { return n_ >= y; }
    auto operator<=(const std::uintptr_t& y) const noexcept -> bool { return n_ <= y; }

    auto inc(location source_loc) {
      assert(n_ < std::numeric_limits<number_type>::max());
      ++n_;
      ++by_location_[source_loc];
    }

    auto dec(location source_loc) {
      assert(n_ > 0);
      --n_;
      if (--by_location_[source_loc] == 0) by_location_.erase(source_loc);
    }

    auto get() const noexcept -> number_type {
      return n_;
    }

    void dump(std::ostream& out, std::string_view locktype) const {
      if (by_location_.empty()) return;

      out << "  " << locktype << " locks:\n";
      for (const auto& x : by_location_)
        out << "  - " << x.first.file << ":" << x.first.line << " = " << x.second << "\n";
    }

    private:
    number_type n_ = 0;
    std::unordered_map<location, number_type, location_hash> by_location_;
  };

  public:
  struct exclusive_function
  : location
  {
    exclusive_function(move_only_function<void(std::shared_ptr<monitor_state>, bool, location)> fn, location source_loc) noexcept
    : location(source_loc),
      fn(std::move(fn))
    {}

    void operator()(std::shared_ptr<monitor_state> state, bool immediate) const {
      std::invoke(fn, std::move(state), immediate, get_source_loc_());
    }

    private:
    auto get_source_loc_() const noexcept -> const location& { return *this; }

    move_only_function<void(std::shared_ptr<monitor_state>, bool, location)> fn;
  };

  struct upgrade_function
  : location
  {
    upgrade_function(move_only_function<void(std::shared_ptr<monitor_state>, bool, location)> fn, location source_loc) noexcept
    : location(source_loc),
      fn(std::move(fn))
    {}

    void operator()(std::shared_ptr<monitor_state> state, bool immediate) const {
      std::invoke(fn, std::move(state), immediate, get_source_loc_());
    }

    private:
    auto get_source_loc_() const noexcept -> const location& { return *this; }

    move_only_function<void(std::shared_ptr<monitor_state>, bool, location)> fn;
  };

  struct shared_function
  : location
  {
    shared_function(move_only_function<void(std::shared_ptr<monitor_state>, bool, location)> fn, location source_loc) noexcept
    : location(source_loc),
      fn(std::move(fn))
    {}

    void operator()(std::shared_ptr<monitor_state> state, bool immediate) const {
      std::invoke(fn, std::move(state), immediate, get_source_loc_());
    }

    private:
    auto get_source_loc_() const noexcept -> const location& { return *this; }

    move_only_function<void(std::shared_ptr<monitor_state>, bool, location)> fn;
  };

  protected:
  monitor_state(std::string name) noexcept
  : name_(std::move(name))
  {
    assert(name_ != R"--(earnest::detail::bplus_tree{0@file_id{ns="", filename=""}})--");
  }

  public:
  monitor_state() = delete;
  monitor_state(const monitor_state&) = delete;
  monitor_state(monitor_state&&) = delete;
  monitor_state& operator=(const monitor_state&) = delete;
  monitor_state& operator=(monitor_state&&) = delete;

  virtual ~monitor_state() noexcept {
#ifndef NDEBUG
    std::lock_guard lck{mtx_};
    assert(shlocks == 0);
    assert(exlocks == 0);
    assert(uplocks == 0);
#endif // NDEBUG
  }

  auto name() const noexcept -> const std::string& { return name_; }

  auto shared_inc(const location& source_loc) noexcept -> void {
    std::lock_guard lck{mtx_};
    assert(shlocks > 0);
    shlocks.inc(source_loc);
  }

  auto upgrade_inc(const location& source_loc) noexcept -> void {
    std::lock_guard lck{mtx_};
    assert(uplocks > 0);
    uplocks.inc(source_loc);
  }

  auto exclusive_inc(const location& source_loc) noexcept -> void {
    std::lock_guard lck{mtx_};
    assert(exlocks > 0);
    exlocks.inc(source_loc);
  }

  virtual auto unlock_shared(location source_loc) -> void = 0;
  virtual auto unlock_upgrade(location source_loc) -> void = 0;
  virtual auto unlock_exclusive(location source_loc) -> void = 0;

  virtual auto add(shared_function&& fn) -> void = 0;
  virtual auto add(upgrade_function&& fn) -> void = 0;
  virtual auto add(exclusive_function&& fn) -> void = 0;
  virtual auto add_upgrade(exclusive_function&& fn, location upgrade_source_loc) -> void = 0;

  [[nodiscard]] auto try_shared(std::string_view file, int line) noexcept -> std::optional<std::shared_ptr<monitor_state>> {
    std::lock_guard lck{mtx_};
    return maybe_lock_shared(file, line);
  }

  [[nodiscard]] auto try_upgrade(std::string_view file, int line) noexcept -> std::optional<std::shared_ptr<monitor_state>> {
    std::lock_guard lck{mtx_};
    return maybe_lock_upgrade(file, line);
  }

  [[nodiscard]] auto try_exclusive(std::string_view file, int line) noexcept -> std::optional<std::shared_ptr<monitor_state>> {
    std::lock_guard lck{mtx_};
    return maybe_lock_exclusive(file, line);
  }

  // Note: must grant the upgrade lock, if the attempt is successful.
  auto add_upgrade_try(std::string_view file, int line, [[maybe_unused]] location upgrade_source_loc) noexcept -> std::optional<std::shared_ptr<monitor_state>> {
    std::unique_lock lck{mtx_};
    assert(uplocks > 0);
    if (shlocks == 0) {
      dlog("up->ex", file, line);
      uplocks.dec(upgrade_source_loc);
      exlocks.inc(location(file, line));
      lck.unlock();

      return this->shared_from_this();
    }

    return std::nullopt;
  }

  auto make_upgrade_lock_from_lock(std::string_view file, int line) noexcept -> std::shared_ptr<monitor_state> {
    std::lock_guard lck{mtx_};
    assert(exlocks > 0);
    dlog("ex->up", file, line);
    uplocks.inc(location(file, line));
    return this->shared_from_this();
  }

  private:
  static auto get_logger() -> std::shared_ptr<spdlog::logger> {
    std::shared_ptr<spdlog::logger> logger = spdlog::get("earnest.monitor");
    if (!logger) logger = std::make_shared<spdlog::logger>("earnest.monitor", std::make_shared<spdlog::sinks::null_sink_mt>());
    return logger;
  }

  virtual auto wq_size() const noexcept -> std::size_t = 0;
  virtual auto pq_size() const noexcept -> std::size_t = 0;
  virtual auto shq_size() const noexcept -> std::size_t = 0;

  virtual auto wq_empty() const noexcept -> bool = 0;
  virtual auto pq_empty() const noexcept -> bool = 0;
  virtual auto shq_empty() const noexcept -> bool = 0;

  virtual auto next_wq_is_exclusive() const noexcept -> bool = 0;

  virtual void pq_walk(std::function<void(location, location)>) const = 0;
  virtual void shq_walk(std::function<void(location)>) const = 0;
  virtual void wq_walk(std::function<void(location, bool)>) const = 0;

  void dump(std::ostream& out, bool only_blocking_locks) const {
    std::lock_guard lck{mtx_};
    if (shlocks == 0 && exlocks == 0 && uplocks == 0) return; // Skip locks that are idle.
    if (only_blocking_locks && shq_empty() && pq_empty() && wq_empty()) return; // Skip locks that aren't blocking program execution.

    out << "Lock: " << name() << "\n";
    out << "  Summary: "
        << "exlocks=" << exlocks.get() << ", "
        << "uplocks=" << uplocks.get() << ", "
        << "shlocks=" << shlocks.get() << ", "
        << "wq=" << wq_size() << ", "
        << "pq=" << pq_size() << ", "
        << "shq=" << shq_size() << ", "
        << "name=" << name() << "\n";

    shlocks.dump(out, "shared");
    uplocks.dump(out, "upgrade");
    exlocks.dump(out, "exclusive");

    if (!pq_empty()) {
      out << "  Promotion queue:\n";
      pq_walk(
          [&out](location loc, location upgrade_loc) {
            out << "  - " << loc.file << ":" << loc.line << " (from " << upgrade_loc.file << ":" << upgrade_loc.line << ")\n";
          });
    }

    if (!wq_empty()) {
      out << "  Write queue:\n";
      wq_walk(
          [&out](location loc, bool is_exclusive) {
            out << "  - " << loc.file << ":" << loc.line << (is_exclusive ? " (exclusive)" : " (upgrade)") << "\n";
          });
    }

    if (!shq_empty()) {
      out << "  Shared queue:\n";
      shq_walk(
          [&out](location loc) {
            out << "  - " << loc.file << ":" << loc.line << "\n";
          });
    }
  }

  protected:
  void dlog(std::string_view op, std::string_view file, int line);

  [[nodiscard]] auto maybe_lock_exclusive(std::string_view file, int line) noexcept -> std::optional<std::shared_ptr<monitor_state>> {
    if (exlocks != 0) return std::nullopt;
    if (uplocks != 0) return std::nullopt;
    if (shlocks != 0) return std::nullopt;

    dlog("exclusive", file, line);
    exlocks.inc(location(file, line));
    return this->shared_from_this();
  }

  [[nodiscard]] auto maybe_lock_upgrade(std::string_view file, int line) noexcept -> std::optional<std::shared_ptr<monitor_state>> {
    if (exlocks != 0) return std::nullopt;
    if (uplocks != 0) return std::nullopt;

    dlog("upgrade", file, line);
    uplocks.inc(location(file, line));
    return this->shared_from_this();
  }

  [[nodiscard]] auto maybe_lock_shared(std::string_view file, int line) noexcept -> std::optional<std::shared_ptr<monitor_state>> {
    if (exlocks != 0) return std::nullopt;
    if (!pq_empty()) return std::nullopt;
    if (uplocks == 0 && next_wq_is_exclusive()) return std::nullopt;

    dlog("shared", file, line);
    shlocks.inc(location(file, line));
    return this->shared_from_this();
  }

  mutable std::mutex mtx_;
  counter shlocks, exlocks, uplocks;

  private:
  const std::string name_;

  protected:
  const std::shared_ptr<spdlog::logger> logger = get_logger();
};

inline void monitor_debug_dump(std::ostream& out = std::cerr, bool only_blocking_locks = true) {
  monitor_state::monitor_debug.dump(out, only_blocking_locks);
}

template<typename Allocator> class monitor_state_impl;


} /* namespace earnest::detail */


namespace fmt {


template<>
struct formatter<earnest::detail::monitor_state::location>
: formatter<std::string>
{
  auto format(const earnest::detail::monitor_state::location& loc, format_context& ctx) -> decltype(ctx.out()) {
    return format_to(ctx.out(), "{}:{}", loc.file, loc.line);
  }
};


} /* namespace fmt */


namespace earnest::detail {


template<typename Executor, typename Allocator = std::allocator<std::byte>>
class monitor {
  friend class monitor_shared_lock;
  template<typename> friend class monitor_upgrade_lock;
  template<typename> friend class monitor_exclusive_lock;

  public:
  using executor_type = Executor;
  using allocator_type = Allocator;

  private:
  using state = monitor_state_impl<allocator_type>;
  using location = monitor_state::location;

  public:
  using exclusive_lock = monitor_exclusive_lock<executor_type>;
  using upgrade_lock = monitor_upgrade_lock<executor_type>;
  using shared_lock = monitor_shared_lock;

  private:
  using exclusive_function = monitor_state::exclusive_function;
  using upgrade_function = monitor_state::upgrade_function;
  using shared_function = monitor_state::shared_function;

  public:
  explicit monitor(executor_type ex, std::string name, allocator_type alloc = allocator_type())
  : state_(std::allocate_shared<state>(alloc, std::move(name), alloc)),
    ex_(std::move(ex))
  {}

  [[nodiscard]] auto try_shared(std::string_view file, int line) noexcept -> std::optional<shared_lock>;
  [[nodiscard]] auto try_upgrade(std::string_view file, int line) noexcept -> std::optional<upgrade_lock>;
  [[nodiscard]] auto try_exclusive(std::string_view file, int line) noexcept -> std::optional<exclusive_lock>;
  template<typename CompletionToken> auto async_shared(CompletionToken&& token, std::string_view file, int line);
  template<typename CompletionToken> auto async_upgrade(CompletionToken&& token, std::string_view file, int line);
  template<typename CompletionToken> auto async_exclusive(CompletionToken&& token, std::string_view file, int line);
  template<typename CompletionToken> auto dispatch_shared(CompletionToken&& token, std::string_view file, int line);
  template<typename CompletionToken> auto dispatch_upgrade(CompletionToken&& token, std::string_view file, int line);
  template<typename CompletionToken> auto dispatch_exclusive(CompletionToken&& token, std::string_view file, int line);

  auto get_executor() const -> executor_type {
    return ex_;
  }

  auto get_allocator() const -> allocator_type {
    return state_->get_allocator();
  }

  auto name() const noexcept -> const std::string& {
    return state_->name();
  }

  private:
  std::shared_ptr<state> state_;
  Executor ex_;
};


template<typename Allocator>
class monitor_state_impl final
: public monitor_state
{
  public:
  using allocator_type = Allocator;

  private:
  struct pending_function
  : exclusive_function
  {
    pending_function(exclusive_function&& fn, const location& upgrade_source_loc)
    : exclusive_function(std::move(fn)),
      upgrade_source_loc(upgrade_source_loc)
    {}

    location upgrade_source_loc;
  };

  using shared_fn_list = std::vector<shared_function, typename std::allocator_traits<allocator_type>::template rebind_alloc<shared_function>>;
  using queue_element = std::variant<exclusive_function, upgrade_function>;
  using queue_type = std::deque<queue_element, typename std::allocator_traits<allocator_type>::template rebind_alloc<queue_element>>;
  using pending_upgrades_list = std::vector<pending_function, typename std::allocator_traits<allocator_type>::template rebind_alloc<pending_function>>;

  public:
  monitor_state_impl(std::string name, allocator_type alloc)
  : monitor_state(std::move(name)),
    alloc_(alloc),
    shq_(alloc),
    wq_(alloc),
    pq_(alloc)
  {
    monitor_debug.register_new_state(this);
  }

  ~monitor_state_impl() {
    monitor_debug.deregister_state(this);

#ifndef NDEBUG
    std::lock_guard lck{mtx_};
    assert(shq_.empty());
    assert(wq_.empty());
    assert(pq_.empty());
#endif // NDEBUG
  }

  auto get_allocator() const -> allocator_type { return alloc_; }

  auto add(shared_function&& fn) -> void override {
    std::unique_lock lck{mtx_};
    auto shlock = maybe_lock_shared(fn.file, fn.line);
    if (shlock.has_value()) {
      lck.unlock();
      std::invoke(fn, std::move(shlock).value(), true);
      return;
    }

    dlog("enq sh", fn.file, fn.line);
    shq_.emplace_back(std::move(fn));
  }

  auto add(upgrade_function&& fn) -> void override {
    std::unique_lock lck{mtx_};
    auto uplock = maybe_lock_upgrade(fn.file, fn.line);
    if (uplock.has_value()) {
      lck.unlock();
      std::invoke(fn, std::move(uplock).value(), true);
      return;
    }

    dlog("enq up", fn.file, fn.line);
    wq_.emplace_back(std::move(fn));
  }

  auto add(exclusive_function&& fn) -> void override {
    std::unique_lock lck{mtx_};
    auto exlock = maybe_lock_exclusive(fn.file, fn.line);
    if (exlock.has_value()) {
      lck.unlock();
      std::invoke(fn, std::move(exlock).value(), true);
      return;
    }

    dlog("enq ex", fn.file, fn.line);
    wq_.emplace_back(std::move(fn));
  }

  auto unlock_shared(location source_loc) -> void override {
    std::lock_guard lck{mtx_};
    on_scope_exit log_release(
        [this]() {
          this->dlog("unlock shared", __FILE__, __LINE__);
        });

    shlocks.dec(source_loc);
    if (shlocks > 0) return;

    if (!pq_.empty()) {
      assert(uplocks >= pq_.size());
      if (exlocks == 0) {
        std::for_each(
            pq_.begin(), pq_.end(),
            [this](pending_function& f) {
              dlog("up->ex", f.file, f.line);
              exlocks.inc(f);
              uplocks.dec(f.upgrade_source_loc);
              std::invoke(f, this->shared_from_this(), false);
            });
        pq_.clear();
      }
      return;
    }

    if (!wq_.empty() && std::holds_alternative<exclusive_function>(wq_.front())) {
      auto exlock = maybe_lock_exclusive(std::get<exclusive_function>(wq_.front()).file, std::get<exclusive_function>(wq_.front()).line);
      if (exlock.has_value()) {
        exclusive_function f = std::get<exclusive_function>(std::move(wq_.front()));
        wq_.pop_front();
        std::invoke(f, std::move(exlock).value(), false);
      }
      return;
    }

    assert(shq_.empty());
  }

  auto unlock_upgrade(location source_loc) -> void override {
    std::lock_guard lck{mtx_};
    on_scope_exit log_release(
        [this]() {
          this->dlog("unlock upgrade", __FILE__, __LINE__);
        });

    uplocks.dec(source_loc);

    if (exlocks != 0) return;
    if (!pq_.empty()) return;

    if (!wq_.empty()) {
      if (std::holds_alternative<exclusive_function>(wq_.front())) {
        auto exlock = maybe_lock_exclusive(std::get<exclusive_function>(wq_.front()).file, std::get<exclusive_function>(wq_.front()).line);
        if (exlock.has_value()) {
          exclusive_function f = std::get<exclusive_function>(std::move(wq_.front()));
          wq_.pop_front();
          std::invoke(f, std::move(exlock).value(), false);
        }
        return;
      } else {
        auto uplock = maybe_lock_upgrade(std::get<upgrade_function>(wq_.front()).file, std::get<upgrade_function>(wq_.front()).line);
        if (uplock.has_value()) {
          upgrade_function f = std::get<upgrade_function>(std::move(wq_.front()));
          wq_.pop_front();
          std::invoke(f, std::move(uplock).value(), false);
        }
      }
    }

    assert(exlocks == 0); // We know this to be true, because of the preceding code.
    assert(shq_.empty()); // We know that, seeing as this was an upgrade lock, and there are no exclusive locks (neither active nor pending),
                          // that shared locks can happen uncontested.
  }

  auto unlock_exclusive(location source_loc) -> void override {
    std::lock_guard lck{mtx_};
    on_scope_exit log_release(
        [this]() {
          this->dlog("unlock exclusive", __FILE__, __LINE__);
        });

    exlocks.dec(source_loc);

    // While an exclusive lock is held, all upgrades succeed.
    assert(pq_.empty());

    // If there are still exclusive locks held, nothing can be locked.
    if (exlocks != 0) return;

    if (!wq_.empty()) {
      if (std::holds_alternative<exclusive_function>(wq_.front())) {
        auto exlock = maybe_lock_exclusive(std::get<exclusive_function>(wq_.front()).file, std::get<exclusive_function>(wq_.front()).line);
        if (exlock.has_value()) {
          exclusive_function f = std::get<exclusive_function>(std::move(wq_.front()));
          wq_.pop_front();
          std::invoke(f, std::move(exlock).value(), false);
        }
        return;
      } else {
        auto uplock = maybe_lock_upgrade(std::get<upgrade_function>(wq_.front()).file, std::get<upgrade_function>(wq_.front()).line);
        if (uplock.has_value()) {
          upgrade_function f = std::get<upgrade_function>(std::move(wq_.front()));
          wq_.pop_front();
          std::invoke(f, std::move(uplock).value(), false);
        }
      }
    }

    std::for_each(
        shq_.begin(), shq_.end(),
        [this](shared_function& f) {
          std::invoke(f, this->maybe_lock_shared(f.file, f.line).value(), false);
        });
    shq_.clear();
  }

  // Note: must grant the upgrade lock.
  auto add_upgrade(exclusive_function&& fn, location upgrade_source_loc) -> void override {
    std::unique_lock lck{mtx_};
    assert(uplocks > 0);
    if (shlocks == 0) {
      dlog("up->ex", fn.file, fn.line);
      uplocks.dec(upgrade_source_loc);
      exlocks.inc(fn);
      lck.unlock();

      std::invoke(fn, this->shared_from_this(), true);
      return;
    }

    dlog("enq up->ex", fn.file, fn.line);
    pq_.emplace_back(std::move(fn), upgrade_source_loc);
  }

  private:
  auto wq_size() const noexcept -> std::size_t override { return wq_.size(); }
  auto pq_size() const noexcept -> std::size_t override { return pq_.size(); }
  auto shq_size() const noexcept -> std::size_t override { return shq_.size(); }
  auto wq_empty() const noexcept -> bool override { return wq_.empty(); }
  auto pq_empty() const noexcept -> bool override { return pq_.empty(); }
  auto shq_empty() const noexcept -> bool override { return shq_.empty(); }

  auto next_wq_is_exclusive() const noexcept -> bool override {
    return !wq_empty() && std::holds_alternative<exclusive_function>(wq_.front());
  }

  void pq_walk(std::function<void(location, location)> cb) const override {
    for (const auto& task : pq_) cb(task, task.upgrade_source_loc);
  }

  void shq_walk(std::function<void(location)> cb) const override {
    for (const auto& task : shq_) cb(task);
  }

  void wq_walk(std::function<void(location, bool)> cb) const override {
    for (const auto& task : wq_) {
      std::visit(
          overload(
              [&cb](const exclusive_function& ex_task) {
                cb(ex_task, true);
              },
              [&cb](const upgrade_function& ex_task) {
                cb(ex_task, false);
              }),
          task);
    }
  }

  allocator_type alloc_;
  shared_fn_list shq_;
  queue_type wq_;
  pending_upgrades_list pq_;
};


class monitor_shared_lock {
  template<typename, typename> friend class monitor;

  private:
  using location = monitor_state::location;

  explicit monitor_shared_lock(std::shared_ptr<monitor_state> s, const location& source_loc) noexcept
  : state_(std::move(s)),
    source_loc_(source_loc)
  {
    assert(state_ != nullptr);
  }

  public:
  monitor_shared_lock() noexcept = default;

  monitor_shared_lock(const monitor_shared_lock& y) noexcept
  : state_(y.state_),
    source_loc_(y.source_loc_)
  {
    if (state_ != nullptr) state_->shared_inc(source_loc_);
  }

  monitor_shared_lock(monitor_shared_lock&& y) noexcept
  : state_(std::move(y.state_)),
    source_loc_(std::move(y.source_loc_))
  {}

  auto operator=(const monitor_shared_lock& y) noexcept -> monitor_shared_lock& {
    using std::swap;
    monitor_shared_lock copy = y;
    swap(state_, copy.state_);
    swap(source_loc_, copy.source_loc_);
    return *this;
  }

  auto operator=(monitor_shared_lock&& y) noexcept -> monitor_shared_lock& {
    using std::swap;
    monitor_shared_lock copy = std::move(y);
    swap(state_, copy.state_);
    swap(source_loc_, copy.source_loc_);
    return *this;
  }

  ~monitor_shared_lock() {
    if (state_ != nullptr) state_->unlock_shared(source_loc_);
  }

  auto reset() -> void {
    if (state_ != nullptr) {
      state_->unlock_shared(source_loc_);
      state_.reset();
    }
  }

  auto is_locked() const noexcept -> bool { return state_ != nullptr; }
  explicit operator bool() const noexcept { return is_locked(); }
  auto operator!() const noexcept -> bool { return !is_locked(); }

  template<typename Executor, typename Allocator>
  auto holds_monitor(const monitor<Executor, Allocator>& m) const noexcept -> bool { return m.state_ == state_; }

  auto monitor_name() const -> const std::string& { return state_->name(); }

  private:
  std::shared_ptr<monitor_state> state_;
  location source_loc_ = location(std::string_view(), 0);
};


template<typename Executor>
class monitor_upgrade_lock {
  template<typename, typename> friend class monitor;
  template<typename> friend class monitor_exclusive_lock;

  private:
  using exclusive_lock = monitor_exclusive_lock<Executor>;
  using exclusive_function = monitor_state::exclusive_function;
  using location = monitor_state::location;

  explicit monitor_upgrade_lock(Executor ex, std::shared_ptr<monitor_state> state, const location& source_loc) noexcept
  : state_(std::move(state)),
    ex(std::move(ex)),
    source_loc_(source_loc)
  {
    assert(state_ != nullptr);
  }

  public:
  monitor_upgrade_lock() noexcept = default;

  monitor_upgrade_lock(const monitor_upgrade_lock& y) noexcept
  : state_(y.state_),
    ex(y.ex),
    source_loc_(y.source_loc_)
  {
    if (state_ != nullptr) state_->upgrade_inc(source_loc_);
  }

  monitor_upgrade_lock(monitor_upgrade_lock&& y) noexcept
  : state_(std::move(y.state_)),
    ex(std::move(y.ex)),
    source_loc_(std::move(y.source_loc_))
  {}

  auto operator=(const monitor_upgrade_lock& y) noexcept -> monitor_upgrade_lock& {
    using std::swap;
    monitor_upgrade_lock copy = y;
    swap(ex, copy.ex);
    swap(state_, copy.state_);
    swap(source_loc_, copy.source_loc_);
    return *this;
  }

  auto operator=(monitor_upgrade_lock&& y) noexcept -> monitor_upgrade_lock& {
    using std::swap;
    monitor_upgrade_lock copy = std::move(y);
    swap(ex, copy.ex);
    swap(state_, copy.state_);
    swap(source_loc_, copy.source_loc_);
    return *this;
  }

  ~monitor_upgrade_lock() {
    if (state_ != nullptr) state_->unlock_upgrade(source_loc_);
  }

  auto reset() -> void {
    if (state_ != nullptr) {
      state_->unlock_upgrade(source_loc_);
      state_.reset();
      ex.reset();
    }
  }

  auto is_locked() const noexcept -> bool { return state_ != nullptr; }
  explicit operator bool() const noexcept { return is_locked(); }
  auto operator!() const noexcept -> bool { return !is_locked(); }

  template<typename E, typename A>
  auto holds_monitor(const monitor<E, A>& m) const noexcept -> bool { return m.state_ == state_; }

  auto monitor_name() const -> const std::string& { return state_->name(); }

  [[nodiscard]] auto try_exclusive(std::string_view file, int line) const & noexcept -> std::optional<exclusive_lock>;
  [[nodiscard]] auto try_exclusive(std::string_view file, int line) && noexcept -> std::optional<exclusive_lock>;
  template<typename CompletionToken> auto async_exclusive(CompletionToken&& token, std::string_view file, int line) const &;
  template<typename CompletionToken> auto async_exclusive(CompletionToken&& token, std::string_view file, int line) &&;
  template<typename CompletionToken> auto dispatch_exclusive(CompletionToken&& token, std::string_view file, int line) const &;
  template<typename CompletionToken> auto dispatch_exclusive(CompletionToken&& token, std::string_view file, int line) &&;

  private:
  std::shared_ptr<monitor_state> state_;
  std::optional<Executor> ex;
  location source_loc_ = location(std::string_view(), 0);
};


template<typename Executor>
class monitor_exclusive_lock {
  template<typename, typename> friend class monitor;
  template<typename> friend class monitor_upgrade_lock;

  private:
  using upgrade_lock = monitor_upgrade_lock<Executor>;
  using upgrade_function = monitor_state::upgrade_function;
  using location = monitor_state::location;

  explicit monitor_exclusive_lock(Executor ex, std::shared_ptr<monitor_state> state, const location& source_loc) noexcept
  : state_(std::move(state)),
    ex(std::move(ex)),
    source_loc_(source_loc)
  {
    assert(state_ != nullptr);
  }

  public:
  monitor_exclusive_lock() noexcept = default;

  monitor_exclusive_lock(const monitor_exclusive_lock& y) noexcept
  : state_(y.state_),
    ex(y.ex),
    source_loc_(y.source_loc_)
  {
    if (state_ != nullptr) state_->exclusive_inc(source_loc_);
  }

  monitor_exclusive_lock(monitor_exclusive_lock&& y) noexcept
  : state_(std::move(y.state_)),
    ex(std::move(y.ex)),
    source_loc_(std::move(y.source_loc_))
  {}

  auto operator=(const monitor_exclusive_lock& y) noexcept -> monitor_exclusive_lock& {
    using std::swap;
    monitor_exclusive_lock copy = y;
    swap(ex, copy.ex);
    swap(state_, copy.state_);
    swap(source_loc_, copy.source_loc_);
    return *this;
  }

  auto operator=(monitor_exclusive_lock&& y) noexcept -> monitor_exclusive_lock& {
    using std::swap;
    monitor_exclusive_lock copy = std::move(y);
    swap(ex, copy.ex);
    swap(state_, copy.state_);
    swap(source_loc_, copy.source_loc_);
    return *this;
  }

  ~monitor_exclusive_lock() {
    if (state_ != nullptr) state_->unlock_exclusive(source_loc_);
  }

  auto reset() -> void {
    if (state_ != nullptr) {
      state_->unlock_exclusive(source_loc_);
      state_.reset();
      ex.reset();
    }
  }

  auto is_locked() const noexcept -> bool { return state_ != nullptr; }
  explicit operator bool() const noexcept { return is_locked(); }
  auto operator!() const noexcept -> bool { return !is_locked(); }

  template<typename E, typename A>
  auto holds_monitor(const monitor<E, A>& m) const noexcept -> bool { return m.state_ == state_; }

  auto monitor_name() const -> const std::string& { return state_->name(); }

  auto as_upgrade_lock(std::string_view file, int line) const & -> upgrade_lock {
    if (state_ == nullptr) throw std::logic_error("lock not held");
    return upgrade_lock(ex.value(), state_->make_upgrade_lock_from_lock(file, line), location(file, line));
  }

  auto as_upgrade_lock(std::string_view file, int line) && -> upgrade_lock {
    if (state_ == nullptr) throw std::logic_error("lock not held");
    upgrade_lock uplock = upgrade_lock(ex.value(), state_->make_upgrade_lock_from_lock(file, line), location(file, line));
    reset();
    return uplock;
  }

  private:
  std::shared_ptr<monitor_state> state_;
  std::optional<Executor> ex;
  location source_loc_ = location(std::string_view(), 0);
};


template<typename Executor, typename Allocator>
inline auto monitor<Executor, Allocator>::try_shared(std::string_view file, int line) noexcept -> std::optional<shared_lock> {
  assert(state_ != nullptr);
  auto ml = state_->try_shared(file, line);
  if (ml.has_value())
    return shared_lock(std::move(ml).value(), location(file, line));
  else
    return std::nullopt;
}

template<typename Executor, typename Allocator>
inline auto monitor<Executor, Allocator>::try_upgrade(std::string_view file, int line) noexcept -> std::optional<upgrade_lock> {
  assert(state_ != nullptr);
  auto ml = state_->try_upgrade(file, line);
  if (ml.has_value())
    return upgrade_lock(get_executor(), std::move(ml).value(), location(file, line));
  else
    return std::nullopt;
}

template<typename Executor, typename Allocator>
inline auto monitor<Executor, Allocator>::try_exclusive(std::string_view file, int line) noexcept -> std::optional<exclusive_lock> {
  assert(state_ != nullptr);
  auto ml = state_->try_exclusive(file, line);
  if (ml.has_value())
    return exclusive_lock(get_executor(), std::move(ml).value(), location(file, line));
  else
    return std::nullopt;
}

template<typename Executor, typename Allocator>
template<typename CompletionToken>
inline auto monitor<Executor, Allocator>::async_shared(CompletionToken&& token, std::string_view file, int line) {
  return asio::async_initiate<CompletionToken, void(shared_lock)>(
      [](auto handler, std::shared_ptr<state> state_, std::string_view file, int line, Executor ex) {
        state_->add(
            shared_function(
                completion_wrapper<void(std::shared_ptr<monitor_state>, bool, location)>(
                    completion_handler_fun(std::move(handler), ex),
                    [](auto handler, std::shared_ptr<monitor_state> lock, [[maybe_unused]] bool immediate, location source_loc) {
                      std::invoke(handler, shared_lock(std::move(lock), source_loc));
                    }),
                monitor_state::location(file, line)));
      },
      token, state_, file, line, get_executor());
}

template<typename Executor, typename Allocator>
template<typename CompletionToken>
inline auto monitor<Executor, Allocator>::async_upgrade(CompletionToken&& token, std::string_view file, int line) {
  return asio::async_initiate<CompletionToken, void(upgrade_lock)>(
      [](auto handler, std::shared_ptr<monitor_state> state_, std::string_view file, int line, Executor ex) {
        state_->add(
            upgrade_function(
                completion_wrapper<void(std::shared_ptr<monitor_state>, bool, location)>(
                    completion_handler_fun(std::move(handler), ex),
                    [ex](auto handler, std::shared_ptr<monitor_state> lock, [[maybe_unused]] bool immediate, location source_loc) {
                      std::invoke(handler, upgrade_lock(ex, std::move(lock), source_loc));
                    }),
                monitor_state::location(file, line)));
      },
      token, state_, file, line, get_executor());
}

template<typename Executor, typename Allocator>
template<typename CompletionToken>
inline auto monitor<Executor, Allocator>::async_exclusive(CompletionToken&& token, std::string_view file, int line) {
  return asio::async_initiate<CompletionToken, void(exclusive_lock)>(
      [](auto handler, std::shared_ptr<monitor_state> state_, std::string_view file, int line, Executor ex) {
        state_->add(
            exclusive_function(
                completion_wrapper<void(std::shared_ptr<monitor_state>, bool, location)>(
                    completion_handler_fun(std::move(handler), ex),
                    [ex](auto handler, std::shared_ptr<monitor_state> lock, [[maybe_unused]] bool immediate, location source_loc) {
                      std::invoke(handler, exclusive_lock(ex, std::move(lock), source_loc));
                    }),
                monitor_state::location(file, line)));
      },
      token, state_, file, line, get_executor());
}

template<typename Executor, typename Allocator>
template<typename CompletionToken>
inline auto monitor<Executor, Allocator>::dispatch_shared(CompletionToken&& token, std::string_view file, int line) {
  return asio::async_initiate<CompletionToken, void(shared_lock)>(
      [](auto handler, std::shared_ptr<monitor_state> state_, std::string_view file, int line, Executor ex) {
        state_->add(
            shared_function(
                completion_wrapper<void(std::shared_ptr<monitor_state>, bool, location)>(
                    completion_handler_fun(std::move(handler), ex),
                    [](auto handler, std::shared_ptr<monitor_state> lock, bool immediate, location source_loc) {
                      if (immediate)
                        handler.dispatch(shared_lock(std::move(lock), source_loc));
                      else
                        handler.post(shared_lock(std::move(lock), source_loc));
                    }),
                monitor_state::location(file, line)));
      },
      token, state_, file, line, get_executor());
}

template<typename Executor, typename Allocator>
template<typename CompletionToken>
inline auto monitor<Executor, Allocator>::dispatch_upgrade(CompletionToken&& token, std::string_view file, int line) {
  return asio::async_initiate<CompletionToken, void(upgrade_lock)>(
      [](auto handler, std::shared_ptr<monitor_state> state_, std::string_view file, int line, Executor ex) {
        state_->add(
            upgrade_function(
                completion_wrapper<void(std::shared_ptr<monitor_state>, bool, location)>(
                    completion_handler_fun(std::move(handler), ex),
                    [ex](auto handler, std::shared_ptr<monitor_state> lock, bool immediate, location source_loc) {
                      if (immediate)
                        handler.dispatch(upgrade_lock(ex, std::move(lock), source_loc));
                      else
                        handler.post(upgrade_lock(ex, std::move(lock), source_loc));
                    }),
                monitor_state::location(file, line)));
      },
      token, state_, file, line, get_executor());
}

template<typename Executor, typename Allocator>
template<typename CompletionToken>
inline auto monitor<Executor, Allocator>::dispatch_exclusive(CompletionToken&& token, std::string_view file, int line) {
  return asio::async_initiate<CompletionToken, void(exclusive_lock)>(
      [](auto handler, std::shared_ptr<monitor_state> state_, std::string_view file, int line, Executor ex) {
        state_->add(
            exclusive_function(
                completion_wrapper<void(std::shared_ptr<monitor_state>, bool, location)>(
                    completion_handler_fun(std::move(handler), ex),
                    [ex](auto handler, std::shared_ptr<monitor_state> lock, bool immediate, location source_loc) {
                      if (immediate)
                        handler.dispatch(exclusive_lock(ex, std::move(lock), source_loc));
                      else
                        handler.post(exclusive_lock(ex, std::move(lock), source_loc));
                    }),
                monitor_state::location(file, line)));
      },
      token, state_, file, line, get_executor());
}

inline void monitor_state::dlog(std::string_view op, std::string_view file, int line) {
  this->logger->debug(
      "monitor {}, exlocks={}, uplocks={}, shlocks={}, wq={}, pq={}, shq={}, name={} (at {})",
      op, exlocks.get(), uplocks.get(), shlocks.get(), wq_size(), pq_size(), shq_size(), name(), location(file, line));
}


template<typename Executor>
[[nodiscard]] inline auto monitor_upgrade_lock<Executor>::try_exclusive(std::string_view file, int line) const & noexcept -> std::optional<exclusive_lock> {
  return monitor_upgrade_lock(*this).try_exclusive(file, line); // Invoke the move-operation.
}

template<typename Executor>
[[nodiscard]] inline auto monitor_upgrade_lock<Executor>::try_exclusive(std::string_view file, int line) && noexcept -> std::optional<exclusive_lock> {
  std::optional<std::shared_ptr<monitor_state>> lck = state_->add_upgrade_try(file, line, source_loc_);
  if (lck.has_value()) state_.reset(); // Grant uplock to the 'add_upgrade' function.

  if (lck.has_value())
    return exclusive_lock(ex.value(), std::move(lck).value(), location(file, line));
  else
    return std::nullopt;
}

template<typename Executor>
template<typename CompletionToken>
inline auto monitor_upgrade_lock<Executor>::async_exclusive(CompletionToken&& token, std::string_view file, int line) const & {
  return monitor_upgrade_lock(*this).async_exclusive(std::forward<CompletionToken>(token), file, line); // Invokes the move function.
}

template<typename Executor>
template<typename CompletionToken>
inline auto monitor_upgrade_lock<Executor>::async_exclusive(CompletionToken&& token, std::string_view file, int line) && {
  return asio::async_initiate<CompletionToken, void(exclusive_lock)>(
      [](auto handler, monitor_upgrade_lock self, std::string_view file, int line) {
        if (self.state_ == nullptr) throw std::logic_error("lock not held");

        self.state_->add_upgrade(
            exclusive_function(
                completion_wrapper<void(std::shared_ptr<monitor_state>, bool, location)>(
                    completion_handler_fun(std::move(handler), self.ex.value()),
                    [ex=self.ex.value()](auto handler, std::shared_ptr<monitor_state> lock, [[maybe_unused]] bool immediate, location source_loc) {
                      std::invoke(handler, exclusive_lock(ex, std::move(lock), source_loc));
                    }),
                monitor_state::location(file, line)),
            self.source_loc_);
        self.state_.reset(); // Grant uplock to the 'add_upgrade' function.
      },
      token, std::move(*this), file, line);
}

template<typename Executor>
template<typename CompletionToken>
inline auto monitor_upgrade_lock<Executor>::dispatch_exclusive(CompletionToken&& token, std::string_view file, int line) const & {
  return monitor_upgrade_lock(*this).dispatch_exclusive(std::forward<CompletionToken>(token), file, line); // Invokes the move function.
}

template<typename Executor>
template<typename CompletionToken>
inline auto monitor_upgrade_lock<Executor>::dispatch_exclusive(CompletionToken&& token, std::string_view file, int line) && {
  return asio::async_initiate<CompletionToken, void(exclusive_lock)>(
      [](auto handler, monitor_upgrade_lock self, std::string_view file, int line) {
        if (self.state_ == nullptr) throw std::logic_error("lock not held");

        self.state_->add_upgrade(
            exclusive_function(
                completion_wrapper<void(std::shared_ptr<monitor_state>, bool, location)>(
                    completion_handler_fun(std::move(handler), self.ex.value()),
                    [ex=self.ex.value()](auto handler, std::shared_ptr<monitor_state> lock, bool immediate, location source_loc) {
                      if (immediate)
                        handler.dispatch(exclusive_lock(ex, std::move(lock), source_loc));
                      else
                        handler.post(exclusive_lock(ex, std::move(lock), source_loc));
                    }),
                monitor_state::location(file, line)),
            self.source_loc_);
        self.state_.reset(); // Grant uplock to the 'add_upgrade' function.
      },
      token, std::move(*this), file, line);
}


} /* namespace earnest::detail */
