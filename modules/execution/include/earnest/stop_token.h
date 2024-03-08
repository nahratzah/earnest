/*
 * This is an implementation of p2300r1.
 * https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2021/p2300r1.html
 *
 * You probably want to read that, to get the what/how/why of this file.
 *
 * Notes:
 * - stoppable_token: the spec requires we confirm if stop_requested() and stop_possible() methods
 *   return a boolean-testable object. We chose to require it to be a bool instead.
 * - never_stop_token: the spec doesn't mandate a specific implementation, just that it exists.
 *   It's even not saying it must be a stop-token, but from context I think it's pretty clear it must
 *   match the unstoppable-token concept.
 * - in_place_stop_token: the spec doesn't say anything about this.
 *   My guess is that the types match behaviour in the <stop_token>-header counterparts:
 *   - in_place_stop_token observes an in_place_stop_source
 *   - its callback-type is in_place_stop_callback
 * - in_place_stop_callback: the spec doesn't say if it should or shouldn't be copy or move constructible.
 *   Not supporting either makes it cheaper to implement, so I opted for not supporting it.
 *   This matches the std::stop_callback implementation, so should be fine (and probably intended).
 * - in_place_stop_source: is move/copy constructible/assignable just like std::stop_source.
 */

#pragma once

#include <concepts>
#include <memory>
#include <mutex>
#include <type_traits>
#include <utility>

#if __has_include(<stop_token>)
# include <stop_token>
#endif

namespace earnest::execution {


// For stoppable-token concept, we must confirm the callback_type template exists.
// Since we cannot ask if a template exists (we sorta can, but not reliably)
// we'll opt to instead confirm it's usable.
struct __stoppable_token__no_arg_invocable {
  void operator()() const;
};


// Stoppable token concept.
// A stoppable token is something that'll tell us that a "stop" has been requested.
// (I.e. an operation is to be canceled.)
//
// It'll also tell us if it's possible there would be such a request.
// And allows the installation of callbacks, to be executed upon a "stop" request.
template<typename T>
concept stoppable_token =
    std::copy_constructible<T> &&
    std::move_constructible<T> &&
    std::is_nothrow_copy_constructible_v<T> &&
    std::is_nothrow_move_constructible_v<T> &&
    std::equality_comparable<T> &&
    requires (const T& token) {
      { token.stop_requested() } noexcept -> std::same_as<bool>;
      { token.stop_possible() } noexcept -> std::same_as<bool>;
      typename T::template callback_type<__stoppable_token__no_arg_invocable>;
    };


// Checks if a stoppable-token can accept the given callback type.
template<typename T, typename CB, typename Initializer = CB>
concept stoppable_token_for =
    stoppable_token<T> &&
    std::invocable<CB> &&
    requires {
      typename T::template callback_type<CB>;
    } &&
    std::constructible_from<CB, Initializer> &&
    std::constructible_from<typename T::template callback_type<CB>, T, Initializer> &&
    std::constructible_from<typename T::template callback_type<CB>, T&, Initializer> &&
    std::constructible_from<typename T::template callback_type<CB>, const T, Initializer> &&
    std::constructible_from<typename T::template callback_type<CB>, const T&, Initializer>;


// An unstoppable-token is a token that will never request a stop.
template<typename T>
concept unstoppable_token =
    stoppable_token<T> &&
    requires {
      { T::stop_possible() } -> std::same_as<bool>;
    } &&
    (!T::stop_possible());


// If the c++ library has the <stop_source> header, it'll #define the __cpp_lib_jthread constant.
// We alias the nostopstate from the STL in this case.
// But if the library doesn't have this header, or doesn't match the feature-flag value, we'll define our own.
#if __cpp_lib_jthread >= 201911L
using std::nostopstate_t;
using std::nostopstate;
#else
struct nostopstate_t {};
inline constexpr nostopstate_t nostopstate;
#endif


// The never-stop-token is a stop-token, that never says it's stopped.
class never_stop_token {
  private:
  // The callback-type for a never_stop_token... is never invoked.
  // So we might as well ignore its existence entirely.
  // This callback-type will always ignore any arguments given.
  struct callback_type_impl {
    template<typename Initializer>
    constexpr callback_type_impl([[maybe_unused]] const never_stop_token&, [[maybe_unused]] Initializer&&) noexcept {}
  };

  public:
  template<std::invocable CB>
  using callback_type = callback_type_impl;

  static constexpr auto stop_requested() noexcept -> bool { return false; }
  static constexpr auto stop_possible() noexcept -> bool { return false; }
};

constexpr auto operator==([[maybe_unused]] const never_stop_token&, [[maybe_unused]] const never_stop_token&) noexcept -> bool {
  return true;
}

constexpr auto operator!=([[maybe_unused]] const never_stop_token&, [[maybe_unused]] const never_stop_token&) noexcept -> bool {
  return false;
}

static_assert(unstoppable_token<never_stop_token>);


class in_place_stop_token;

class in_place_stop_source {
  friend class in_place_stop_token;
  template<std::invocable<>> friend class in_place_stop_callback;

  private:
  class callback_registry;

  class callback_interface {
    friend callback_registry;
    friend in_place_stop_source;
    friend in_place_stop_token;

    protected:
    callback_interface() noexcept = default;

    // We explicitly delete the move/copy constructor/assignment.
    // This ensures that even derived types won't have these.
    callback_interface(const callback_interface&) = delete;
    callback_interface(callback_interface&&) = delete;
    callback_interface& operator=(const callback_interface&) = delete;
    callback_interface& operator=(callback_interface&&) = delete;

    ~callback_interface() {
      assert(!registered()); // We are not registered.
      assert(pred == nullptr && succ == nullptr); // We are not linked in a list.
    }

    auto deregister() noexcept -> void;

    private:
    auto registered() const noexcept -> bool {
      return cb_registry != nullptr;
    }

    virtual auto invoke() -> void = 0;

    // pred and succ pointers are managed by the callback_registry.
    // We don't touch those.
    // (We don't even look.)
    callback_interface* pred = nullptr;
    callback_interface* succ = nullptr;

    // Registry on which this callback is registered.
    std::shared_ptr<callback_registry> cb_registry;
  };

  class callback_registry {
    public:
    auto lock() noexcept -> void {
      mtx.lock();
    }

    auto unlock() noexcept -> void {
      mtx.unlock();
    }

    auto link(callback_interface* cb) noexcept -> void {
      // We require that the callback isn't registered.
      assert(cb->pred == nullptr && cb->succ == nullptr);

      cb->succ = elements;
      if (elements != nullptr) elements->pred = cb;
      elements = cb;
    }

    auto unlink(callback_interface* cb) noexcept -> void {
      // We require that the callback is registered.
      assert(elements == cb || cb->pred != nullptr);

      if (elements == cb) {
        assert(cb->pred == nullptr);
        elements = cb->succ;
        elements->pred = nullptr;
      } else {
        assert(cb->pred != nullptr);
      }

      if (elements->succ != nullptr) elements->succ->pred = elements->pred;
      if (elements->pred != nullptr) elements->pred->succ = elements->succ;
      elements->pred = elements->succ = nullptr;
    }

    template<std::invocable<callback_interface&> Fn>
    auto for_each(Fn&& fn)
    noexcept(std::is_nothrow_invocable_v<Fn, callback_interface&>)
    -> Fn&& {
      for (callback_interface* iter = elements; iter != nullptr; iter = iter->succ) {
        // Run the callback.
        // We don't release the lock, because if we do,
        // we could have a race between one thread cleaning up this object,
        // while we invoke its callback.
        std::invoke(fn, *iter); // May throw.
      }
      return std::forward<Fn>(fn);
    }

    auto stop_requested() const noexcept -> bool {
      return stop_requested_;
    }

    auto request_stop() noexcept -> bool {
      // If we're already stopped, don't stop again.
      if (stop_requested_) return false;

      stop_requested_ = true;
      for_each(
          [](callback_interface& cb) {
            cb.invoke();
          });

      return true;
    }

    private:
    std::mutex mtx;
    callback_interface* elements = nullptr;
    bool stop_requested_ = false;
  };

  public:
  in_place_stop_source()
  : cb_registry(std::make_shared<callback_registry>())
  {}

  in_place_stop_source(nostopstate_t nss) noexcept
  : cb_registry(nullptr)
  {}

  auto get_token() const noexcept -> in_place_stop_token;

  auto stop_requested() const noexcept -> bool {
    std::lock_guard lck{*cb_registry};
    return cb_registry->stop_requested();
  }

  static constexpr auto stop_possible() noexcept -> bool {
    return true;
  }

  auto request_stop() noexcept -> bool {
    std::lock_guard lck{*cb_registry};
    return cb_registry->request_stop();
  }

  private:
  std::shared_ptr<callback_registry> cb_registry;
};

inline auto in_place_stop_source::callback_interface::deregister() noexcept -> void {
  if (!registered()) return;

  std::lock_guard lck{*cb_registry};
  cb_registry->unlink(this);
}


template<std::invocable<> CB> class in_place_stop_callback;

// Stop token associated with an in_place_stop_source.
class in_place_stop_token {
  friend class in_place_stop_source;
  template<std::invocable<> CB> friend class in_place_stop_callback;

  public:
  template<std::invocable CB>
  using callback_type = in_place_stop_callback<CB>;

  constexpr in_place_stop_token() noexcept = default;

  private:
  explicit in_place_stop_token(const std::shared_ptr<in_place_stop_source::callback_registry>& cb_registry) noexcept
  : cb_registry(cb_registry)
  {}

  public:
  auto operator==(const in_place_stop_token& other) const noexcept -> bool {
    return cb_registry == other.cb_registry;
  }

  auto operator!=(const in_place_stop_token& other) const noexcept -> bool {
    return !(*this == other);
  }

  auto stop_requested() const noexcept -> bool {
    if (cb_registry == nullptr) return false;

    std::lock_guard lck{*cb_registry};
    return cb_registry->stop_requested();
  }

  auto stop_possible() const noexcept -> bool {
    return cb_registry != nullptr;
  }

  private:
  auto register_callback(in_place_stop_source::callback_interface* cb) const noexcept -> void {
    if (cb_registry != nullptr) {
      std::unique_lock lck{*cb_registry};

      if (cb_registry->stop_requested()) {
        lck.unlock(); // Run the callback without lock held.
        cb->invoke();
      } else {
        cb_registry->link(cb);
      }
    }
  }

  std::shared_ptr<in_place_stop_source::callback_registry> cb_registry;
};


// Callback for in_place_stop_source.
//
// The callback ensures that:
// - it is registered
// - it is de-registered when destroyed
// - the contained callback is invoked when the stop-source becomes stopped
template<std::invocable<> CB>
class in_place_stop_callback
: private in_place_stop_source::callback_interface
{
  public:
  template<typename Initializer>
  in_place_stop_callback(const in_place_stop_token& src, Initializer&& initializer)
  noexcept(std::is_nothrow_constructible_v<CB, Initializer>)
  : cb(std::forward<Initializer>(initializer)) // may throw
  {
    src.register_callback(this); // never throws
  }

  ~in_place_stop_callback() {
    this->deregister();
  }

  private:
  auto invoke() noexcept -> void override {
    std::invoke(cb);
  }

  CB cb;
};


inline auto in_place_stop_source::get_token() const noexcept -> in_place_stop_token {
  return in_place_stop_token(cb_registry);
}

static_assert(stoppable_token<never_stop_token>);


} /* namespace earnest::execution */
