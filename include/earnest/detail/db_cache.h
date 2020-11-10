#ifndef EARNEST_DETAIL_DB_CACHE_H
#define EARNEST_DETAIL_DB_CACHE_H

#include <earnest/detail/export_.h>
#include <earnest/detail/tx_op.h>
#include <earnest/txfile.h>
#include <monsoon/cache/cache.h>
#include <monsoon/cache/allocator.h>
#include <earnest/shared_resource_allocator.h>
#include <earnest/detail/cheap_fn_ref.h>
#include <cycle_ptr/cycle_ptr.h>
#include <functional>

namespace earnest::detail {


class earnest_export_ db_cache
: public cycle_ptr::cycle_base
{
  public:
  static constexpr std::uintptr_t default_max_memory = 1024 * 1024 * 1024;

  class domain;
  class cache_obj;

  ///\brief Allocator used by the cache.
  using allocator_type = monsoon::cache::cache_allocator<shared_resource_allocator<std::byte>>;
  ///\brief Allocator traits.
  using traits_type = std::allocator_traits<allocator_type>;

  private:
  class key {
    public:
    key(txfile::transaction::offset_type off, cycle_ptr::cycle_gptr<const class domain> domain) noexcept
    : off(off),
      domain(domain),
      raw_domain_ptr(domain.get())
    {}

    auto operator==(const key& y) const noexcept -> bool {
      return off == y.off
          && raw_domain_ptr == y.raw_domain_ptr
          && domain.lock() != nullptr
          && y.domain.lock() != nullptr;
    }

    auto operator!=(const key& y) const noexcept -> bool {
      return !(*this == y);
    }

    txfile::transaction::offset_type off;
    cycle_ptr::cycle_weak_ptr<const class domain> domain;
    const class domain* raw_domain_ptr;
  };

  struct search {
    key k;
    cheap_fn_ref<cycle_ptr::cycle_gptr<cache_obj>(allocator_type, txfile::transaction::offset_type)> load;

    operator key() const noexcept {
      return k;
    }
  };

  struct key_hash {
    auto operator()(const key& k) const noexcept -> std::size_t {
      std::hash<txfile::transaction::offset_type> off_hash;
      std::hash<const void*> ptr_hash;
      return 27u * off_hash(k.off) + ptr_hash(k.raw_domain_ptr);
    }

    auto operator()(const search& s) const noexcept -> std::size_t {
      return (*this)(s.k);
    }
  };

  struct create_cb {
    auto operator()(allocator_type alloc, const key& k) const -> cycle_ptr::cycle_gptr<cache_obj> {
      throw std::logic_error("this key is not directly constructible");
    }

    auto operator()(allocator_type alloc, const search& s) const -> cycle_ptr::cycle_gptr<cache_obj> {
      return s.load(std::move(alloc), s.k.off);
    }
  };

  using impl_type = monsoon::cache::extended_cache<key, cache_obj, key_hash, std::equal_to<key>, allocator_type, create_cb, cycle_ptr::cycle_gptr<cache_obj>>;

  public:
  explicit db_cache(std::string name, std::uintptr_t max_memory = default_max_memory, shared_resource_allocator<std::byte> allocator = shared_resource_allocator<std::byte>());
  ~db_cache() noexcept;

  auto get_if_present(
      txfile::transaction::offset_type off,
      cycle_ptr::cycle_gptr<const domain> dom) noexcept
    -> cycle_ptr::cycle_gptr<cache_obj>;
  auto get(
      txfile::transaction::offset_type off,
      cycle_ptr::cycle_gptr<const domain> dom,
      cheap_fn_ref<cycle_ptr::cycle_gptr<cache_obj>(allocator_type, txfile::transaction::offset_type)> load)
    -> cycle_ptr::cycle_gptr<cache_obj>;
  auto create(
      txfile::transaction::offset_type off,
      cycle_ptr::cycle_gptr<const domain> dom,
      cheap_fn_ref<cycle_ptr::cycle_gptr<cache_obj>(allocator_type, txfile::transaction::offset_type)> load,
      tx_op_collection& ops)
    -> cycle_ptr::cycle_gptr<cache_obj>;
  void invalidate(
      txfile::transaction::offset_type off,
      cycle_ptr::cycle_gptr<const domain> dom) noexcept;
  auto invalidate_on_rollback(
      txfile::transaction::offset_type off,
      cycle_ptr::cycle_gptr<const domain> dom,
      shared_resource_allocator<std::byte> alloc = shared_resource_allocator<std::byte>())
    -> std::shared_ptr<tx_op>;
  void invalidate_on_rollback(
      txfile::transaction::offset_type off,
      cycle_ptr::cycle_gptr<const domain> dom,
      tx_op_collection& ops);

  private:
  impl_type impl_;
};

class earnest_export_ db_cache::domain {
  protected:
  ~domain() noexcept = default;
};

class earnest_export_ db_cache::cache_obj {
  public:
  virtual ~cache_obj() noexcept = 0;
};


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_DB_CACHE_H */
