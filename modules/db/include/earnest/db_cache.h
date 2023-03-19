#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <typeindex>
#include <typeinfo>
#include <utility>

#include <cycle_ptr.h>
#include <libhoard/cache.h>
#include <libhoard/policies.h>
#include <libhoard/asio/resolver_policy.h>
#include <prometheus/registry.h>
#include <prometheus/counter.h>

#include <earnest/db_error.h>
#include <earnest/detail/completion_wrapper.h>
#include <earnest/detail/hash_combine.h>
#include <earnest/detail/prom_allocator.h>
#include <earnest/detail/tracking_allocator.h>
#include <earnest/file_id.h>

namespace earnest {
namespace detail {


// Cache-policy that uses the tracker from tracked_allocator, to aim for a specific max-memory.
class max_mem_policy {
  public:
  using dependencies = libhoard::detail::type_list<::libhoard::detail::queue_policy>;

  template<typename HashTable, typename ValueType, typename Allocator>
  class table_base {
    public:
    explicit table_base(const max_mem_policy& p, const Allocator& alloc) noexcept
    : max_mem_(p.max_mem_),
      inverse_max_mem_(1.0 / p.max_mem_),
      t_(alloc.get_tracker())
    {}

    auto policy_removal_check_() const noexcept -> std::size_t {
      const auto curmem = t_->get();
      if (t_ == nullptr || curmem <= max_mem_) return 0;

      auto& self = static_cast<const HashTable&>(*this);
      const float reduce_factor = 1.0 - float(max_mem_) / float(curmem);
      std::size_t reduce = static_cast<std::size_t>(self.size() * reduce_factor);
      return std::max(reduce, std::size_t(1));
    }

    auto max_mem() const noexcept -> std::size_t {
      return max_mem_;
    }

    auto max_mem(std::size_t new_max_mem) noexcept -> std::size_t {
      return std::exchange(max_mem_, new_max_mem);
    }

    private:
    std::size_t max_mem_;
    float inverse_max_mem_;
    std::shared_ptr<alloc_tracker> t_;
  };

  template<typename Impl, typename HashTableType>
  class add_cache_base {
    protected:
    add_cache_base() noexcept = default;
    add_cache_base(const add_cache_base&) noexcept = default;
    add_cache_base(add_cache_base&&) noexcept = default;
    ~add_cache_base() noexcept = default;
    auto operator=(const add_cache_base&) noexcept -> add_cache_base& = default;
    auto operator=(add_cache_base&&) noexcept -> add_cache_base& = default;

    public:
    auto max_mem() const noexcept -> std::size_t {
      const Impl*const self = static_cast<const Impl*>(this);
      std::lock_guard<HashTableType> lck{ *self->impl_ };
      return self->impl_->max_mem();
    }

    auto max_mem(std::size_t new_max_mem) noexcept -> std::size_t {
      const Impl*const self = static_cast<const Impl*>(this);
      std::lock_guard<HashTableType> lck{ *self->impl_ };
      return self->impl_->max_mem(new_max_mem);
    }
  };

  constexpr max_mem_policy() noexcept = default;

  explicit constexpr max_mem_policy(std::size_t max_mem) noexcept
  : max_mem_(max_mem)
  {}

  private:
  std::size_t max_mem_ = std::numeric_limits<std::size_t>::max();
};


class prom_metrics_policy {
  public:
  prom_metrics_policy() = default;

  prom_metrics_policy(std::shared_ptr<prometheus::Registry> prom_registry, std::string_view prom_db_name)
  : prom_registry(std::move(prom_registry)),
    prom_db_name(std::move(prom_db_name))
  {}

  private:
  class table_base_impl {
    public:
    template<typename Allocator>
    explicit table_base_impl(const prom_metrics_policy& p, const Allocator& alloc) noexcept
    : cache_hit_(build_cache_hit_miss_counter_(p, "hit", alloc)),
      cache_miss_(build_cache_hit_miss_counter_(p, "miss", alloc))
    {}

    auto on_hit_([[maybe_unused]] void* vptr) noexcept -> void {
      cache_hit_->Increment();
    }

    auto on_miss_([[maybe_unused]] void* vptr) noexcept -> void {
      cache_miss_->Increment();
    }

    private:
    template<typename Alloc>
    static auto build_cache_hit_miss_counter_(const prom_metrics_policy& p, std::string_view hit_or_miss, Alloc alloc) -> std::shared_ptr<prometheus::Counter> {
      if (p.prom_registry == nullptr) return std::allocate_shared<prometheus::Counter>(alloc);

      return std::shared_ptr<prometheus::Counter>(
          p.prom_registry,
          &prometheus::BuildCounter()
              .Name("earnest_db_cache_lookup")
              .Help("Number of cache lookups")
              .Register(*p.prom_registry)
              .Add({
                    {"db_name", std::string(p.prom_db_name)},
                    {"hit_miss", std::string(hit_or_miss)},
                  }));
    }

    std::shared_ptr<prometheus::Counter> cache_hit_;
    std::shared_ptr<prometheus::Counter> cache_miss_;
  };

  public:
  template<typename HashTable, typename ValueType, typename Allocator>
  using table_base = table_base_impl;

  private:
  std::shared_ptr<prometheus::Registry> prom_registry = nullptr;
  std::string prom_db_name;
};


class cache_value_on_load_invocation_policy {
  public:
  template<typename HashTable, typename ValueType, typename Allocator>
  class table_base {
    public:
    explicit table_base([[maybe_unused]] const cache_value_on_load_invocation_policy& p, [[maybe_unused]] const Allocator& alloc) noexcept
    {}

    auto on_assign_(ValueType* vptr, bool value, [[maybe_unused]] bool assigned_via_callback) noexcept -> void {
      if (value) {
        std::visit(
            [](const auto& mapped_value) {
              if constexpr(std::is_same_v<typename ValueType::mapped_type, std::remove_cvref_t<decltype(mapped_value)>>)
                invoke_on_load_(*mapped_value);
            },
            vptr->get(std::false_type()));
      }
    }
  };

  private:
  template<typename T>
  static auto invoke_on_load_(T& v) noexcept -> void {
    v.on_load();
  }
};


} /* namespace earnest::detail */


class db_cache_value {
  friend class detail::cache_value_on_load_invocation_policy;

  public:
  virtual ~db_cache_value() = default;

  protected:
  virtual void on_load() noexcept {}
};


template<typename Executor, typename Allocator = std::allocator<std::byte>>
class db_cache {
  public:
  static inline constexpr std::size_t default_max_mem = 1024ull * 1024ull * 1024ull;
  using executor_type = Executor;
  using allocator_type = detail::prom_allocator<detail::tracking_allocator<Allocator>>;
  using offset_type = std::uint64_t;

  private:
  struct underlying_key_type {
    underlying_key_type() = default;

    underlying_key_type(std::type_index type, file_id file, offset_type offset) noexcept
    : type(std::move(type)),
      file(std::move(file)),
      offset(std::move(offset))
    {}

    template<typename K, typename... Args, std::enable_if_t<sizeof...(Args) != 0, int> = 0>
    explicit underlying_key_type(K&& k, [[maybe_unused]] Args&&... args)
    : underlying_key_type(std::forward<K>(k))
    {}

    auto type_index() const noexcept -> const std::type_index& {
      return type;
    }

    std::type_index type;
    file_id file;
    offset_type offset;
  };

  public:
  template<typename T>
  struct key_type {
    file_id file;
    offset_type offset;

    auto type_index() const noexcept -> std::type_index {
      return std::type_index(typeid(T));
    }

    operator underlying_key_type() const & {
      return underlying_key_type(type_index(), file, offset);
    }

    operator underlying_key_type() && {
      return underlying_key_type(type_index(), std::move(file), std::move(offset));
    }
  };

  private:
  struct hash_fn {
    template<typename... Args>
    auto operator()(const underlying_key_type& k, [[maybe_unused]] Args&&... args) const noexcept -> std::size_t {
      return detail::hash_combine(
          type_hasher(k.type_index()),
          file_hasher(k.file),
          offset_hasher(k.offset));
    }

    template<typename T, typename... Args>
    auto operator()(const key_type<T>& k, [[maybe_unused]] Args&&... args) const noexcept -> std::size_t {
      return detail::hash_combine(
          type_hasher(k.type_index()),
          file_hasher(k.file),
          offset_hasher(k.offset));
    }

    private:
    std::hash<std::type_index> type_hasher;
    std::hash<file_id> file_hasher;
    std::hash<offset_type> offset_hasher;
  };

  struct equal_fn {
    template<typename... Args>
    auto operator()(const underlying_key_type& x, const underlying_key_type& y, [[maybe_unused]] Args&&... args) const noexcept -> std::size_t {
      return type_equal(x.type_index(), y.type_index())
          && file_equal(x.file, y.file)
          && offset_equal(x.offset, y.offset);
    }

    template<typename T, typename... Args>
    auto operator()(const underlying_key_type& x, const key_type<T>& y, [[maybe_unused]] Args&&... args) const noexcept -> std::size_t {
      return type_equal(x.type_index(), y.type_index())
          && file_equal(x.file, y.file)
          && offset_equal(x.offset, y.offset);
    }

    private:
    std::equal_to<std::type_index> type_equal;
    std::equal_to<file_id> file_equal;
    std::equal_to<offset_type> offset_equal;
  };

  struct mk_member_pointer_ {
    mk_member_pointer_(cycle_ptr::cycle_base& self) noexcept
    : self(&self)
    {}

    auto operator()(cycle_ptr::cycle_gptr<db_cache_value> ptr) const -> std::tuple<cycle_ptr::cycle_base&, cycle_ptr::cycle_gptr<db_cache_value>> {
      return {*self, ptr};
    }

    private:
    cycle_ptr::cycle_base*const self;
  };

  struct resolver_functor {
    auto operator()(const underlying_key_type& k) const -> cycle_ptr::cycle_gptr<db_cache_value> = delete;

    template<typename CallbackPtr, typename T, typename Fn, typename... Args>
    auto operator()(CallbackPtr callback_ptr, [[maybe_unused]] const key_type<T>& k, Fn&& fn, Args&&... args) const -> void {
      std::invoke(fn, std::forward<Args>(args)...)
      | [callback_ptr](std::error_code ec, cycle_ptr::cycle_gptr<T> ptr) -> void {
          if (ec)
            callback_ptr->assign_error(ec);
          else
            callback_ptr->assign(std::move(ptr));
        };
    }
  };

  public:
  explicit db_cache(cycle_ptr::cycle_base& owner, std::shared_ptr<prometheus::Registry> prom_registry, std::string_view prom_db_name, executor_type ex, Allocator alloc = Allocator())
  : tracker_(std::allocate_shared<detail::alloc_tracker>(alloc)),
    alloc_(prom_registry, "earnest_db_cache", prometheus::Labels{{"db_name", std::string(prom_db_name)}}, this->tracker_, alloc),
    impl_(
      std::allocator_arg, this->alloc_,
      libhoard::asio_resolver_policy<resolver_functor, executor_type>(resolver_functor(), ex),
      libhoard::pointer_policy<cycle_ptr::cycle_weak_ptr<db_cache_value>, cycle_ptr::cycle_member_ptr<db_cache_value>, mk_member_pointer_>(mk_member_pointer_(owner)),
      detail::max_mem_policy(default_max_mem),
      detail::prom_metrics_policy(prom_registry, prom_db_name))
  {}

  explicit db_cache(cycle_ptr::cycle_base& owner, executor_type ex, Allocator alloc = Allocator())
  : db_cache(owner, nullptr, std::string_view(), std::move(ex), std::move(alloc))
  {}

  template<typename T>
  auto get_if_exists(key_type<T> k) -> cycle_ptr::cycle_gptr<T> {
    return std::dynamic_pointer_cast<T>(impl_.get_if_exists(std::move(k)).value_or(nullptr));
  }

  template<typename T, typename CompletionToken, typename Fn, typename... Args>
  auto async_get(key_type<T> k, CompletionToken&& token, Fn&& fn, Args&&... args) {
    return asio::async_initiate<CompletionToken, void(std::error_code, cycle_ptr::cycle_gptr<T>)>(
        [](auto handler, auto impl, key_type<T> k, auto fn, auto... args) {
          impl.async_get(
              detail::completion_wrapper<void(cycle_ptr::cycle_gptr<db_cache_value>, std::error_code)>(
                  std::move(handler),
                  [](auto handler, cycle_ptr::cycle_gptr<db_cache_value> raw_ptr, std::error_code ec) {
                    cycle_ptr::cycle_gptr<T> ptr;
                    if (!ec) {
                      ptr = std::dynamic_pointer_cast<T>(raw_ptr);
                      if (ptr == nullptr) ec = make_error_code(db_errc::cache_collision);
                    }
                    std::invoke(handler, std::move(ec), std::move(ptr));
                  }),
              std::move(k), std::move(fn), std::move(args)...);
        },
        token, this->impl_, std::move(k), std::forward<Fn>(fn), std::forward<Args>(args)...);
  }

  template<typename T>
  auto erase(const key_type<T>& k) noexcept -> void {
    return impl_.erase(k);
  }

  template<typename T>
  auto emplace(key_type<T> k, cycle_ptr::cycle_gptr<T> v) -> void {
    impl_.emplace(std::move(k), std::move(v));
  }

  auto get_executor() const -> executor_type {
    return impl_.get_executor();
  }

  auto get_allocator() const -> allocator_type {
    return alloc_;
  }

  auto max_mem() const noexcept -> std::size_t {
    return impl_.max_mem();
  }

  auto max_mem(std::size_t new_max_mem) noexcept -> std::size_t {
    return impl_.max_mem(new_max_mem);
  }

  private:
  using cache_type = libhoard::cache<
      underlying_key_type, cycle_ptr::cycle_gptr<db_cache_value>,
      libhoard::hash<hash_fn>,
      libhoard::equal<equal_fn>,
      libhoard::allocator<allocator_type>,
      libhoard::thread_safe_policy,
      libhoard::asio_resolver_policy<resolver_functor, executor_type>,
      libhoard::weaken_policy,
      libhoard::error_policy<std::error_code>,
      detail::max_mem_policy,
      detail::prom_metrics_policy,
      detail::cache_value_on_load_invocation_policy,
      libhoard::pointer_policy<cycle_ptr::cycle_weak_ptr<db_cache_value>, cycle_ptr::cycle_member_ptr<db_cache_value>, mk_member_pointer_>>;

  std::shared_ptr<detail::alloc_tracker> tracker_;
  allocator_type alloc_;
  cache_type impl_;
};


} /* namespace earnest */
