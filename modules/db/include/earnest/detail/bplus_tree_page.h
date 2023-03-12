#pragma once

#include <algorithm>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iterator>
#include <memory>
#include <numeric>
#include <span>
#include <system_error>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <asio/append.hpp>
#include <asio/async_result.hpp>
#include <asio/buffer.hpp>
#include <asio/deferred.hpp>
#include <asio/io_context.hpp>
#include <boost/endian.hpp>
#include <boost/polymorphic_pointer_cast.hpp>

#include <earnest/db_address.h>
#include <earnest/db_cache.h>
#include <earnest/detail/completion_handler_fun.h>
#include <earnest/detail/completion_wrapper.h>
#include <earnest/detail/constexpr_rounding.h>
#include <earnest/detail/overload.h>
#include <earnest/raw_db.h>
#include <earnest/xdr.h>

namespace earnest::detail {


// Error codes used internally for the b+tree.
// Mostly used for signaling.
enum class bplus_tree_errc {
  restart=1,
  allocator_file_mismatch,
  root_page_present,
  bad_tree,
};

inline auto bplus_tree_category() -> const std::error_category& {
  class category_impl
  : public std::error_category
  {
    public:
    constexpr category_impl() noexcept = default;

    auto name() const noexcept -> const char* override {
      return "earnest::detail::bplus_tree";
    }

    auto message(int condition) const -> std::string override {
      using namespace std::string_literals;

      switch (static_cast<bplus_tree_errc>(condition)) {
        default:
          return "unrecognized condition"s;
        case bplus_tree_errc::restart:
          return "B+tree restart operation"s;
        case bplus_tree_errc::allocator_file_mismatch:
          return "B+tree allocator allocated in a different file"s;
        case bplus_tree_errc::root_page_present:
          return "B+tree already has a root page"s;
        case bplus_tree_errc::bad_tree:
          return "B+tree is incorrect"s;
      }
    }
  };

  static category_impl impl;
  return impl;
}

inline auto make_error_code(bplus_tree_errc e) noexcept -> std::error_code {
  return std::error_code(static_cast<int>(e), bplus_tree_category());
}


} /* namespace earnest::detail */

namespace std {


template<>
struct is_error_code_enum<::earnest::detail::bplus_tree_errc>
: std::true_type
{};


} /* namespace std */

namespace earnest::detail {


struct bplus_tree_header {
  static inline constexpr std::uint32_t magic = 0x72bd'8d1aU;
  std::uint64_t root;

  template<typename X>
  friend auto operator&(::earnest::xdr<X>&& x, typename ::earnest::xdr<X>::template typed_function_arg<bplus_tree_header> y) {
    return std::move(x)
        & xdr_constant(magic).as(xdr_uint32)
        & xdr_constant(0).as(xdr_uint32) // reserved
        & xdr_uint64(y.root);
  }
};

struct bplus_tree_page_header {
  std::uint32_t magic;
  std::uint64_t parent;

  template<typename X>
  friend auto operator&(::earnest::xdr<X>&& x, typename ::earnest::xdr<X>::template typed_function_arg<bplus_tree_page_header> y) {
    return std::move(x)
        & xdr_uint32(y.magic)
        & xdr_constant(0).as(xdr_uint32) // reserved
        & xdr_uint64(y.parent);
  }
};

struct bplus_tree_intr_header {
  std::uint32_t size = 0;

  template<typename X>
  friend auto operator&(::earnest::xdr<X>&& x, [[maybe_unused]] typename ::earnest::xdr<X>::template typed_function_arg<bplus_tree_intr_header> y) {
    return std::move(x) & xdr_uint32(y.size);
  }
};

struct bplus_tree_leaf_header {
  std::uint64_t successor_page, predecessor_page;

  template<typename X>
  friend auto operator&(::earnest::xdr<X>&& x, typename ::earnest::xdr<X>::template typed_function_arg<bplus_tree_leaf_header> y) {
    return std::move(x)
        & xdr_uint64(y.predecessor_page)
        & xdr_uint64(y.successor_page);
  }
};


// Different states for leaf elements.
enum class bplus_tree_leaf_use_element : std::uint8_t {
  unused = 0,
  used = 1,
  before_first = 0x80,
  after_last = 0x81,
};


struct bplus_tree_versions {
  using type = std::uint64_t;

  type page_split = 0, page_merge = 0, augment = 0;
};


struct bplus_tree_value_spec {
  bool byte_equality_enabled, byte_order_enabled;
  std::size_t bytes;
  std::function<std::strong_ordering(std::span<const std::byte>, std::span<const std::byte>)> compare;

  auto padded_bytes() const noexcept -> std::size_t {
    return round_up(bytes, 4u);
  }

  auto padding_bytes() const noexcept -> std::size_t {
    return padded_bytes() - bytes;
  }
};


struct bplus_tree_augment_spec
: public bplus_tree_value_spec
{
  std::function<void(std::span<std::byte>, std::vector<std::span<const std::byte>>)> augment_merge;
  std::function<void(std::span<std::byte>, std::span<const std::byte>, std::span<const std::byte>)> augment_value;
};


struct bplus_tree_element_spec {
  bplus_tree_value_spec key;
  bplus_tree_value_spec value;
  std::vector<bplus_tree_augment_spec> augments;

  auto padded_augment_bytes() const noexcept -> std::size_t {
    return std::accumulate(
        augments.begin(), augments.end(),
        std::size_t(0),
        [](std::size_t x, const bplus_tree_augment_spec& y) noexcept -> std::size_t {
          return x + y.padded_bytes();
        });
  }
};


struct bplus_tree_spec {
  bplus_tree_element_spec element;
  std::size_t elements_per_leaf;
  std::size_t child_pages_per_intr;

  auto payload_bytes_per_leaf() const -> std::size_t {
    const auto use_list_bytes = round_up(elements_per_leaf, 4u);
    const auto bytes_per_value = element.key.padded_bytes() + element.value.padded_bytes();
    const auto payload_bytes = elements_per_leaf * bytes_per_value;
    return use_list_bytes + payload_bytes;
  }

  auto payload_bytes_per_intr() const -> std::size_t {
    const auto num_keys = child_pages_per_intr - 1u;
    const auto bytes_per_pointer = sizeof(std::uint64_t) + element.padded_augment_bytes();
    const auto payload_bytes = num_keys * element.key.padded_bytes() + child_pages_per_intr * bytes_per_pointer;
    return payload_bytes;
  }

  auto bytes_per_leaf() const -> std::size_t {
    using xdr_page_hdr = std::remove_cvref_t<decltype(::earnest::xdr_reader<>() & std::declval<bplus_tree_page_header&>())>;
    using xdr_leaf_hdr = std::remove_cvref_t<decltype(::earnest::xdr_reader<>() & std::declval<bplus_tree_leaf_header&>())>;
    constexpr std::size_t header_bytes = xdr_page_hdr::bytes.value() + xdr_leaf_hdr::bytes.value();

    return header_bytes + payload_bytes_per_leaf();
  }

  auto bytes_per_intr() const -> std::size_t {
    using xdr_page_hdr = std::remove_cvref_t<decltype(::earnest::xdr_reader<>() & std::declval<bplus_tree_page_header&>())>;
    using xdr_intr_hdr = std::remove_cvref_t<decltype(::earnest::xdr_reader<>() & std::declval<bplus_tree_intr_header&>())>;
    constexpr std::size_t header_bytes = xdr_page_hdr::bytes.value() + xdr_intr_hdr::bytes.value();

    return header_bytes + payload_bytes_per_intr();
  }
};


template<typename Executor, typename Allocator> class bplus_tree_page;
template<typename Executor, typename Allocator> class bplus_tree_intr;
template<typename Executor, typename Allocator> class bplus_tree_leaf;
template<typename Executor, typename DBAllocator, typename Allocator> class bplus_tree;


template<typename PageType>
struct bplus_tree_siblings {
  using pointer_type = cycle_ptr::cycle_gptr<PageType>;

  bplus_tree_siblings(pointer_type predecessor, pointer_type self, pointer_type successor) noexcept
  : predecessor(std::move(predecessor)),
    self(std::move(self)),
    successor(std::move(successor))
  {}

  template<bool ForWrite>
  auto lock_op([[maybe_unused]] std::bool_constant<ForWrite> for_write) const {
    using lock_type = std::conditional_t<ForWrite,
          typename monitor<typename PageType::executor_type, typename PageType::allocator_type>::exclusive_lock,
          typename monitor<typename PageType::executor_type, typename PageType::allocator_type>::shared_lock>;

    auto lock_op = [](pointer_type ptr, auto... locks) {
      return asio::deferred.when(ptr == nullptr)
          .then(asio::deferred.values(lock_type{}, locks...))
          .otherwise(
              [ ptr,
                deferred=asio::append(asio::deferred, locks...)
              ]() mutable {
                if constexpr(ForWrite)
                  return ptr->page_lock.dispatch_exclusive(std::move(deferred));
                else
                  return ptr->page_lock.dispatch_shared(std::move(deferred));
              });
    };

    return lock_op(predecessor)
    | asio::deferred(
        [self=this->self, lock_op](auto predecessor_lock) {
          return lock_op(self, std::move(predecessor_lock));
        })
    | asio::deferred(
        [successor=this->successor, lock_op](auto self_lock, auto predecessor_lock) {
          return lock_op(successor, std::move(self_lock), std::move(predecessor_lock));
        })
    | asio::deferred(
        [](auto successor_lock, auto self_lock, auto predecessor_lock) {
          return asio::deferred.values(
              std::array<lock_type, 3>{
                std::move(predecessor_lock),
                std::move(self_lock),
                std::move(successor_lock),
              });
        });
  }

  pointer_type predecessor, self, successor;
};


template<typename Executor, typename Allocator>
class bplus_tree_page
: public db_cache_value
{
  template<typename> friend struct bplus_tree_siblings;
  template<typename, typename, typename> friend class bplus_tree;

  public:
  static inline constexpr std::uint64_t nil_page = -1;
  using raw_db_type = raw_db<Executor, Allocator>;
  using executor_type = typename raw_db_type::executor_type;
  using allocator_type = typename raw_db_type::cache_allocator_type;

  private:
  using intr_type = bplus_tree_intr<Executor, Allocator>;
  using leaf_type = bplus_tree_leaf<Executor, Allocator>;

  public:
  explicit bplus_tree_page(std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address, std::uint64_t parent_offset = nil_page) noexcept(std::is_nothrow_move_constructible_v<db_address>)
  : address(std::move(address)),
    raw_db(raw_db),
    spec(std::move(spec)),
    page_lock(raw_db->get_executor(), raw_db->get_allocator()),
    ex_(raw_db->get_executor()),
    alloc_(raw_db->get_cache_allocator()),
    parent_offset(std::move(parent_offset))
  {}

  ~bplus_tree_page() override = default;

  auto get_executor() const -> executor_type { return ex_; }
  auto get_allocator() const -> allocator_type { return alloc_; }

  template<typename AcceptanceFn>
  static auto async_load_op(std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address, AcceptanceFn&& acceptance_fn) {
    return raw_db->async_get(
        typename raw_db_type::template key_type<bplus_tree_page>{.file=address.file, .offset=address.offset},
        asio::deferred,
        [](auto stream, std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address) {
          struct state {
            explicit state(decltype(stream)&& s)
            : s(std::move(s))
            {}

            decltype(stream) s;
          };

          auto state_ptr = std::make_shared<state>(std::move(stream));
          return raw_load_op(state_ptr->s, std::move(spec), std::move(raw_db), std::move(address))
          | asio::deferred(
              [state_ptr](std::error_code ec, cycle_ptr::cycle_gptr<bplus_tree_page> page) mutable {
                state_ptr.reset(); // Using `state_ptr` like this silences compiler warnings.
                return asio::deferred.values(std::move(ec), std::move(page));
              });
        },
        std::forward<AcceptanceFn>(acceptance_fn),
        spec, raw_db, address)
    | asio::deferred(
        [](std::error_code ec, cycle_ptr::cycle_gptr<bplus_tree_page> page) {
          using variant_type = std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>>;

          variant_type casted_page;
          if (!ec) {
            if (auto leaf_ptr = std::dynamic_pointer_cast<leaf_type>(page)) {
              casted_page.template emplace<cycle_ptr::cycle_gptr<leaf_type>>(std::move(leaf_ptr));
            } else if (auto intr_ptr = std::dynamic_pointer_cast<intr_type>(page)) {
              casted_page.template emplace<cycle_ptr::cycle_gptr<intr_type>>(std::move(intr_ptr));
            } else [[unlikely]] {
              // Can only happen if there is a third type.
              ec = make_error_code(db_errc::cache_collision);
            }
          }

          return asio::deferred.values(ec, std::move(casted_page));
        });
  }

  private:
  template<typename Stream>
  static auto raw_load_op(Stream& stream, std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address) {
    auto result = std::make_shared<cycle_ptr::cycle_gptr<bplus_tree_page>>();
    return async_read(
        stream,
        ::earnest::xdr_reader<>() & xdr_manual(
            [spec, raw_db, address, result](bplus_tree_page_header h, auto& stream, auto callback) {
              switch (h.magic) {
                default:
                  std::invoke(callback, make_error_code(xdr_errc::decoding_error));
                  break;
                case intr_type::magic:
                  {
                    const auto ptr = cycle_ptr::allocate_cycle<intr_type>(raw_db->get_cache_allocator(), std::move(spec), raw_db, std::move(address), h.parent);
                    *result = ptr;
                    ptr->continue_load_(stream, raw_db, std::move(callback));
                  }
                  break;
                case leaf_type::magic:
                  {
                    const auto ptr = cycle_ptr::allocate_cycle<leaf_type>(raw_db->get_cache_allocator(), std::move(spec), raw_db, std::move(address), h.parent);
                    *result = ptr;
                    ptr->continue_load_(stream, raw_db, std::move(callback));
                  }
                  break;
              }
            }).template with_lead<bplus_tree_page_header>(),
        asio::deferred)
    | asio::deferred(
        [result](std::error_code ec) {
          return asio::deferred.values(ec, *result);
        });
  }

  public:
  auto parent_page_offset() const -> std::uint64_t {
    return this->parent_offset;
  }

  auto parent_page_address() const -> db_address {
    return db_address(this->address.file, parent_page_offset());
  }

  template<bool ForWrite>
  auto parent_page_op(std::bool_constant<ForWrite> for_write) const {
    return intr_page_op(
        std::move(for_write), &bplus_tree_page::parent_page_address,
        [](const auto& target_ptr, const auto& self_ptr) -> bool {
          return target_ptr->contains(self_ptr->address);
        });
  }

  template<bool ParentForWrite, typename SiblingPageType = bplus_tree_page>
  auto sibling_page_op(std::bool_constant<ParentForWrite> parent_for_write) const {
    using siblings_type = bplus_tree_siblings<SiblingPageType>;
    using page_ptr = cycle_ptr::cycle_gptr<bplus_tree_page>;

    return parent_page_op(std::move(parent_for_write))
    | asio::deferred(
        [self=this->shared_from_this(this)](std::error_code ec, cycle_ptr::cycle_gptr<intr_type> parent, auto parent_lock) {
          return asio::deferred.when(!ec && parent != nullptr)
              .then(
                  [self, parent, parent_lock]() {
                    assert(parent->contains(self->address));

                    const std::optional<std::size_t> self_idx = parent->find_index(self->address);
                    assert(self_idx.has_value());

                    std::optional<db_address> predecessor_address, successor_address;
                    if (self_idx.value() > 0u)
                      predecessor_address = parent->get_index(self_idx.value() - 1u);
                    successor_address = parent->get_index(self_idx.value() + 1u);

                    return asio::deferred.values(std::error_code(), parent, parent_lock, self, std::move(predecessor_address), std::move(successor_address));
                  })
              .otherwise(asio::deferred.values(ec, parent, parent_lock, self, std::optional<db_address>(), std::optional<db_address>()));
        })
    | asio::deferred(
        [](std::error_code ec, cycle_ptr::cycle_gptr<intr_type> parent, auto parent_lock, page_ptr self, std::optional<db_address> predecessor_address, std::optional<db_address> successor_address) {
          const bool need_to_load_things = (predecessor_address.has_value() || successor_address.has_value());

          cycle_ptr::cycle_gptr<raw_db_type> raw_db;
          if (!ec) {
            raw_db = self->raw_db.lock();
            if (raw_db == nullptr) [[unlikely]] ec = make_error_code(db_errc::data_expired);
          }

          const std::shared_ptr<std::array<page_ptr, 3>> state;
          if (!ec && need_to_load_things) {
            state = std::make_shared<std::array<page_ptr, 3>>();
            (*state)[1] = self;
          }

          return asio::deferred.when(!ec && need_to_load_things)
              .then(
                  asio::async_initiate<decltype(asio::deferred), void()>(
                      [state](auto handler, std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, std::optional<db_address> predecessor_address, std::optional<db_address> successor_address) -> void {
                        // We load the predecessor and successor in parallel.
                        auto barrier = make_completion_barrier(std::move(handler));

                        if (predecessor_address.has_value()) {
                          async_load_op(spec, raw_db, predecessor_address.value())
                          | completion_wrapper<void(std::error_code, std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>>)>(
                              ++barrier,
                              [state](auto handler, std::error_code ec, std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>> variant) {
                                if (!ec) {
                                  std::visit(
                                      [&state](auto ptr) {
                                        (*state)[0] = std::move(ptr);
                                      },
                                      std::move(variant));
                                }
                                std::invoke(handler, ec);
                              });
                        }
                        if (successor_address.has_value()) {
                          async_load_op(spec, raw_db, successor_address.value())
                          | completion_wrapper<void(std::error_code, std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>>)>(
                              ++barrier,
                              [state](auto handler, std::error_code ec, std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>> variant) {
                                if (!ec) {
                                  std::visit(
                                      [&state](auto ptr) {
                                        (*state)[2] = std::move(ptr);
                                      },
                                      std::move(variant));
                                }
                                std::invoke(handler, ec);
                              });
                        }
                        std::invoke(barrier, std::error_code());
                      },
                      asio::deferred,
                      self->spec, std::move(raw_db), predecessor_address, successor_address)
                  | asio::deferred(
                      [state, parent, parent_lock, self](std::error_code ec) mutable {
                        return asio::deferred.values(ec, std::move(parent), std::move(parent_lock), (*state)[0], (*state)[1], (*state)[2]);
                      }))
              .otherwise(asio::deferred.values(ec, parent, parent_lock, page_ptr(nullptr), self, page_ptr(nullptr)));
        })
    | asio::deferred(
        [](std::error_code ec, cycle_ptr::cycle_gptr<intr_type> parent, auto parent_lock, page_ptr predecessor, page_ptr self, page_ptr successor) {
          auto cast = [](std::error_code& ec, page_ptr ptr) {
            if constexpr(std::is_same_v<std::remove_cv_t<SiblingPageType>, bplus_tree_page>) {
              return ptr;
            } else {
              auto casted_ptr = std::dynamic_pointer_cast<SiblingPageType>(ptr);
              if (!ec && casted_ptr == nullptr && ptr != nullptr)
                ec = make_error_code(db_errc::cache_collision);
              return casted_ptr;
            }
          };

          auto siblings = siblings_type(cast(ec, predecessor), cast(ec, self), cast(ec, successor));
          assert(siblings.self != nullptr);

          return asio::deferred.values(std::move(ec), std::move(parent), std::move(parent_lock), std::move(siblings));
        });
  }

  // Must lock:
  // - this read-locked
  // - parent read-locked
  template<typename CompletionToken>
  auto async_compute_page_augments(CompletionToken&& token) const {
    return asio::async_initiate<CompletionToken, void(std::vector<std::byte>)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree_page> self) -> void {
          return self->async_compute_page_augments_impl_(std::move(handler));
        },
        token, this->shared_from_this(this));
  }

  private:
  virtual auto async_compute_page_augments_impl_(std::function<void(std::vector<std::byte>)> callback) const -> void = 0;

  protected:
  template<bool ForWrite, typename GetTargetAddrFn, typename AcceptancePredicateFn>
  auto intr_page_op(std::bool_constant<ForWrite> for_write, GetTargetAddrFn get_target_addr_fn, AcceptancePredicateFn acceptance_predicate_fn) const {
    return this->page_lock.async_shared(asio::deferred)
    | asio::deferred(
        [ self=this->shared_from_this(this),
          for_write=std::move(for_write),
          get_target_addr_fn=std::move(get_target_addr_fn),
          acceptance_predicate_fn=std::move(acceptance_predicate_fn)
        ](auto lock) mutable {
          return self->page_variant_op(
              for_write, std::move(lock), get_target_addr_fn, acceptance_predicate_fn,
              [self, for_write, get_target_addr_fn, acceptance_predicate_fn]() mutable {
                return self->page_op_(for_write, get_target_addr_fn, acceptance_predicate_fn);
              });
        })
    | variant_to_pointer_op<intr_type>();
  }

  template<typename PageType>
  static auto variant_to_pointer_op() {
    return asio::deferred(
        [](std::error_code ec, auto ptr_variant, auto... args) {
          cycle_ptr::cycle_gptr<PageType> ptr;
          if (!ec) {
            if (std::holds_alternative<cycle_ptr::cycle_gptr<PageType>>(ptr_variant))
              ptr = std::get<cycle_ptr::cycle_gptr<PageType>>(ptr_variant);
            else
              ec = make_error_code(xdr_errc::decoding_error);
          }
          return asio::deferred.values(std::move(ec), std::move(ptr), std::move(args)...);
        });
  }

  template<bool ForWrite, typename Lock, typename GetTargetAddrFn, typename AcceptancePredicateFn, typename MakeRestartOp>
  auto page_variant_op([[maybe_unused]] std::bool_constant<ForWrite> for_write, Lock lock, GetTargetAddrFn get_target_addr_fn, AcceptancePredicateFn acceptance_predicate_fn, MakeRestartOp make_restart_op) const {
    using bplus_tree_intr_ptr = cycle_ptr::cycle_gptr<intr_type>;
    using bplus_tree_leaf_ptr = cycle_ptr::cycle_gptr<leaf_type>;
    using ptr_type = std::variant<bplus_tree_intr_ptr, bplus_tree_leaf_ptr>;
    using target_lock_type = std::conditional_t<
        ForWrite,
        typename monitor<Executor, Allocator>::exclusive_lock,
        typename monitor<Executor, Allocator>::shared_lock>;

    cycle_ptr::cycle_gptr<raw_db_type> raw_db = this->raw_db.lock();
    std::error_code ec;
    if (raw_db == nullptr || this->erased_) [[unlikely]] ec = make_error_code(db_errc::data_expired);
    auto addr = std::invoke(get_target_addr_fn, this);

    return asio::deferred.when(addr.offset == nil_page || ec)
        .then(asio::deferred(ec, ptr_type()))
        .otherwise(
            async_load_op(this->spec, raw_db, addr)
            | asio::deferred(
                [lock](std::error_code ec, ptr_type ptr) mutable {
                  lock.reset(); // No longer need the old lock (but we did need it up-to-now).
                  return asio::deferred.values(std::move(ec), std::move(ptr));
                }))
    | asio::deferred(
        [](std::error_code ec, ptr_type ptr) {
          return asio::deferred.when(!ec && std::visit([](const auto& ptr) { return ptr != nullptr; }, ptr))
              .then(
                  asio::deferred(
                      [ptr]() {
                        std::visit(
                            [ptr](const auto& actual_ptr) {
                              if constexpr(ForWrite) {
                                return actual_ptr->page_lock.async_exclusive(asio::append(asio::deferred, std::error_code(), ptr));
                              } else {
                                return actual_ptr->page_lock.async_shared(asio::append(asio::deferred, std::error_code(), ptr));
                              }
                            },
                            ptr);
                      }))
              .otherwise(asio::deferred.values(target_lock_type(), ec, ptr_type()));
        })
    | asio::deferred(
        [ self=this->shared_from_this(this),
          make_restart_op=std::move(make_restart_op),
          acceptance_predicate_fn=std::move(acceptance_predicate_fn)
        ](target_lock_type lock, std::error_code ec, ptr_type ptr) mutable {
          const bool need_restart = std::visit(
              [ec](const auto& ptr) -> bool {
                if (!ec && ptr != nullptr) {
                  if (ptr->erased_)
                    return true;
                  else if (!std::invoke(acceptance_predicate_fn, ptr, self))
                    return true;
                }
                return false;
              },
              ptr);

          if (need_restart) [[unlikely]] lock.reset();
          return asio::deferred.when(need_restart)
              .then(std::invoke(make_restart_op))
              .otherwise(asio::deferred.values(std::move(ec), std::move(ptr), std::move(lock)));
        });
  }

  auto combine_augments(std::vector<std::span<const std::byte>> augments) const -> std::vector<std::byte> {
    auto combined_augments = std::vector<std::byte>(this->spec->element.padded_augment_bytes(), std::byte(0));
    std::span<std::byte> data_span = std::span(combined_augments);

    for (const bplus_tree_augment_spec& spec : this->spec->element.augments) {
      auto fn_arg = std::vector<std::span<const std::byte>>(augments.size());
      std::transform(
          augments.begin(), augments.end(),
          fn_arg.begin(),
          [&spec](std::span<const std::byte>& input) {
            assert(input.size() >= spec.padded_bytes());
            auto this_augment = input.subspan(0, spec.bytes);
            input = input.subspan(spec.padded_bytes());
            return this_augment;
          });
      spec.augment_merge(data_span.subspan(spec.bytes), std::move(fn_arg));
      data_span = data_span.subspan(spec.padded_bytes());
    }
    assert(data_span.empty());
    assert(std::all_of(augments.begin(), augments.end(), [](const std::span<const std::byte>& s) { return s.empty(); }));

    return combined_augments;
  }

  public:
  const db_address address;
  bplus_tree_versions versions;

  protected:
  const cycle_ptr::cycle_weak_ptr<raw_db_type> raw_db;
  const std::shared_ptr<const bplus_tree_spec> spec;
  monitor<executor_type, typename raw_db_type::allocator_type> page_lock;
  bool erased_ = false;

  private:
  executor_type ex_;
  allocator_type alloc_;
  std::uint64_t parent_offset = nil_page;
};


/* Interior page.
 *
 * Points at more pages.
 * Has a size >= 1.
 * For each elements:
 * - has a child-page offset.
 * - has a augmentation for the pointed-to page.
 *
 * Has size-1 keys.
 * For key[N]:
 * - elements in child[0..N] (inclusive) have keys <= key[N]
 * - elements in child[N..size-1] have keys >= key[N]
 */
template<typename Executor, typename Allocator>
class bplus_tree_intr
: public bplus_tree_page<Executor, Allocator>,
  private cycle_ptr::cycle_base
{
  template<typename, typename> friend class bplus_tree_page;

  public:
  static inline constexpr std::uint32_t magic = 0x237d'bf9aU;
  using raw_db_type = typename bplus_tree_page<Executor, Allocator>::raw_db_type;
  using executor_type = typename bplus_tree_page<Executor, Allocator>::executor_type;
  using allocator_type = typename bplus_tree_page<Executor, Allocator>::allocator_type;

  private:
  template<typename T> using rebind_alloc = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;

  public:
  bplus_tree_intr(std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address, std::uint64_t parent_offset = bplus_tree_page<Executor, Allocator>::nil_page)
  : bplus_tree_page<Executor, Allocator>(spec, std::move(raw_db), std::move(address), parent_offset),
    bytes_(spec->payload_bytes_per_intr(), this->get_allocator())
  {
    assert(spec->payload_bytes_per_intr() == key_offset(spec->child_pages_per_intr - 1u));
  }

  ~bplus_tree_intr() override = default;

  template<typename TxAlloc, typename DbAlloc, typename CompletionToken>
  static auto async_new_page(
      typename raw_db_type::template fdb_transaction<TxAlloc> tx,
      DbAlloc db_alloc,
      std::shared_ptr<const bplus_tree_spec> spec,
      cycle_ptr::cycle_gptr<raw_db_type> raw_db,
      std::uint64_t parent_offset,
      CompletionToken&& token) {
    using tx_file_type = typename raw_db_type::template fdb_transaction<TxAlloc>::file;
    using stream_type = positional_stream_adapter<tx_file_type>;
    using page_type = bplus_tree_page<Executor, Allocator>;

    return asio::async_initiate<CompletionToken, void(std::error_code, cycle_ptr::cycle_gptr<bplus_tree_intr>)>(
        [](auto handler, typename raw_db_type::template fdb_transaction<TxAlloc> tx, DbAlloc db_alloc, std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, std::uint64_t parent_offset) -> void {
          db_alloc.async_allocate(tx, spec->bytes_per_intr(), asio::deferred)
          | asio::deferred(
              [tx, raw_db, spec, parent_offset](std::error_code ec, db_address addr) {
                cycle_ptr::cycle_gptr<bplus_tree_intr> new_page;
                if (!ec) {
                  new_page = cycle_ptr::allocate_cycle<bplus_tree_intr>(raw_db->get_cache_allocator(), spec, raw_db, std::move(addr), parent_offset);
                  tx.on_commit(
                      [raw_db, new_page]() {
                        raw_db->template emplace<page_type>(
                            typename raw_db_type::template key_type<page_type>{
                              .file=new_page->address.file,
                              .offset=new_page->address.offset
                            },
                            new_page);
                      });
                }

                return asio::deferred.when(!ec)
                    .then(
                        asio::deferred(
                            [tx, new_page]() {
                              auto stream = std::allocate_shared<stream_type>(tx.get_allocator(), tx[new_page->address.file], new_page->address.offset);
                              return async_write(
                                  *stream,
                                  (::earnest::xdr_writer<>() & xdr_constant(bplus_tree_page_header{.magic=magic, .parent=new_page->parent}))
                                  + bplus_tree_intr::continue_xdr_(::earnest::xdr_writer<>(), *new_page),
                                  asio::append(asio::deferred, new_page, stream))
                              | asio::deferred(
                                  [](std::error_code ec, cycle_ptr::cycle_gptr<bplus_tree_intr> new_page, [[maybe_unused]] auto stream) {
                                    return asio::deferred.values(std::move(ec), std::move(new_page));
                                  });
                            }))
                    .otherwise(asio::deferred.values(ec, new_page));
              })
          | std::move(handler);
        },
        token, std::move(tx), std::move(db_alloc), std::move(spec), std::move(raw_db), std::move(parent_offset));
  }

  // Must hold page_lock.
  auto contains(const db_address& child) const noexcept -> bool {
    for (std::size_t i = 0; i < hdr_.size; ++i) {
      if (element(i) == child.offset)
        return child.file == this->address.file;
    }
    return false;
  }

  // Must hold page_lock.
  auto find_index(const db_address& child) const noexcept -> std::optional<std::size_t> {
    if (child.file != this->address.file) [[unlikely]] return std::nullopt;
    for (std::size_t i = 0; i < hdr_.size; ++i) {
      if (element(i) == child.offset)
        return i;
    }
    return std::nullopt;
  }

  auto get_index(std::size_t idx) const -> std::optional<db_address> {
    if (idx >= hdr_.size) return std::nullopt;
    return db_address(this->address.file, element(idx));
  }

  auto key_find_index(std::span<const std::byte> key) const -> std::optional<std::size_t> {
    const bplus_tree_value_spec& key_spec = this->spec->element.key;
    if (hdr_.size == 0) [[unlikely]] return std::nullopt;

    for (std::size_t i = 0; i < hdr_.size - 1u; ++i) {
      const std::span<const std::byte> key_i = key_span(i);
      const bool less = (key_spec.byte_order_enabled
          ? std::lexicographical_compare(key.begin(), key.end(), key_i.begin(), key_i.end())
          : key_spec.compare(key, key_i) == std::strong_ordering::less);
      if (less) return i;
    }
    return hdr_.size - 1u;
  }

  auto key_find(std::span<const std::byte> key) const -> std::optional<db_address> {
    const std::optional<std::size_t> index = key_find_index(key);
    if (index.has_value())
      return get_index(*index);
    else
      return std::nullopt;
  }

  auto index_size() const noexcept -> std::size_t {
    return hdr_.size;
  }

  private:
  template<typename Stream, typename CompletionToken>
  auto continue_load_(Stream& stream, [[maybe_unused]] cycle_ptr::cycle_gptr<raw_db_type> raw_db, CompletionToken&& token) {
    return async_read(
        stream,
        bplus_tree_intr::continue_xdr_(::earnest::xdr_reader<>(), *this),
        std::forward<CompletionToken>(token));
  }

  template<typename X>
  static auto continue_xdr_(::earnest::xdr<X>&& x, typename ::earnest::xdr<X>::template typed_function_arg<bplus_tree_intr> y) {
    return std::move(x) & y.hdr_ & xdr_dynamic_raw_bytes(asio::buffer(y.bytes_));
  }

  auto element0_offset() const noexcept -> std::size_t { return 0u; }
  auto element_length() const noexcept -> std::size_t { return sizeof(std::uint64_t); }
  auto element_offset(std::size_t idx) const noexcept -> std::size_t { return element0_offset() + idx * element_length(); }
  auto augment0_offset() const noexcept -> std::size_t { return element_offset(this->spec->child_pages_per_intr); }
  auto augment_length() const noexcept -> std::size_t { return this->spec->element.padded_augment_bytes(); }
  auto augment_offset(std::size_t idx) const noexcept -> std::size_t { return augment0_offset() + idx * augment_length(); }
  auto key0_offset() const noexcept -> std::size_t { return element_offset(this->spec->child_pages_per_intr); }
  auto key_length() const noexcept -> std::size_t { return this->spec->element.padded_augment_bytes(); }
  auto key_offset(std::size_t idx) const noexcept -> std::size_t { return key0_offset() + idx * key_length(); }

  auto element_span(std::size_t idx) const noexcept -> std::span<const std::byte> {
    assert(idx < this->spec->child_pages_per_intr);
    assert(bytes_.size() >= element_offset(this->spec->child_pages_per_intr));
    return std::span<const std::byte>(&bytes_[element_offset(idx)], element_length());
  }

  auto element_span(std::size_t idx) noexcept -> std::span<std::byte> {
    assert(idx < this->spec->child_pages_per_intr);
    assert(bytes_.size() >= element_offset(this->spec->child_pages_per_intr));
    return std::span<std::byte>(&bytes_[element_offset(idx)], element_length());
  }

  auto element(std::size_t idx) const noexcept -> std::uint64_t {
    std::uint64_t offset;
    auto e_span = element_span(idx);
    assert(e_span.size() == sizeof(offset));
    std::memcpy(&offset, e_span.data(), e_span.size());
    boost::endian::big_to_native_inplace(offset);
    return offset;
  }

  auto augment_span(std::size_t idx) const noexcept -> std::span<const std::byte> {
    assert(idx < this->spec->child_pages_per_intr);
    assert(bytes_.size() >= augment_offset(this->spec->child_pages_per_intr));
    return std::span<const std::byte>(&bytes_[augment_offset(idx)], augment_length());
  }

  auto augment_span(std::size_t idx) noexcept -> std::span<std::byte> {
    assert(idx < this->spec->child_pages_per_intr);
    assert(bytes_.size() >= augment_offset(this->spec->child_pages_per_intr));
    return std::span<std::byte>(&bytes_[augment_offset(idx)], augment_length());
  }

  auto key_span(std::size_t idx) const noexcept -> std::span<const std::byte> {
    assert(idx < this->spec->child_pages_per_intr - 1u);
    assert(bytes_.size() >= key_offset(this->spec->child_pages_per_intr - 1u));
    return std::span<const std::byte>(&bytes_[key_offset(idx)], key_length());
  }

  auto key_span(std::size_t idx) noexcept -> std::span<std::byte> {
    assert(idx < this->spec->child_pages_per_intr - 1u);
    assert(bytes_.size() >= key_offset(this->spec->child_pages_per_intr - 1u));
    return std::span<std::byte>(&bytes_[key_offset(idx)], key_length());
  }

  auto async_compute_page_augments_impl_(std::function<void(std::vector<std::byte>)> callback) const -> void override {
    std::vector<std::span<const std::byte>> inputs;
    inputs.reserve(hdr_.size);
    for (std::size_t i = 0; i < hdr_.size; ++i)
      inputs.push_back(augment_span(i));
    callback(this->combine_augments(std::move(inputs)));
  }

  std::vector<std::byte, rebind_alloc<std::byte>> bytes_;
  bplus_tree_intr_header hdr_;
};


template<typename Executor, typename Allocator>
class bplus_tree_leaf
: public bplus_tree_page<Executor, Allocator>,
  private cycle_ptr::cycle_base
{
  template<typename, typename> friend class bplus_tree_page;

  public:
  static inline constexpr std::uint32_t magic = 0x65cc'612dU;
  using raw_db_type = typename bplus_tree_page<Executor, Allocator>::raw_db_type;
  using executor_type = typename bplus_tree_page<Executor, Allocator>::executor_type;
  using allocator_type = typename bplus_tree_page<Executor, Allocator>::allocator_type;

  private:
  template<typename T> using rebind_alloc = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;

  public:
  struct element {
    element(executor_type ex, std::size_t index, cycle_ptr::cycle_gptr<bplus_tree_leaf> owner, typename raw_db_type::allocator_type alloc)
    : index(std::move(index)),
      element_lock(std::move(ex), std::move(alloc)),
      owner(std::move(owner))
    {}

    auto type() const -> bplus_tree_leaf_use_element {
      return owner->use_list_span()[index];
    }

    std::size_t index;
    monitor<executor_type, typename raw_db_type::allocator_type> element_lock;
    cycle_ptr::cycle_member_ptr<bplus_tree_leaf> owner;
  };

  private:
  using element_ptr = cycle_ptr::cycle_member_ptr<element>;

  using element_vector = std::vector<
      element_ptr,
      cycle_ptr::cycle_allocator<rebind_alloc<element_ptr>>>;

  public:
  bplus_tree_leaf(std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address, std::uint64_t parent_offset = bplus_tree_page<Executor, Allocator>::nil_page, std::uint64_t predecessor_offset = bplus_tree_page<Executor, Allocator>::nil_page, std::uint64_t successor_offset = bplus_tree_page<Executor, Allocator>::nil_page)
  : bplus_tree_page<Executor, Allocator>(spec, std::move(raw_db), std::move(address), parent_offset),
    bytes_(spec->payload_bytes_per_leaf(), this->get_allocator()),
    elements_(cycle_ptr::cycle_allocator<rebind_alloc<element>>(*this, this->get_allocator())),
    hdr_{
      .successor_page=successor_offset,
      .predecessor_page=predecessor_offset,
    }
  {
    assert(spec->payload_bytes_per_leaf() == element_offset(spec->elements_per_leaf));
    elements_.reserve(spec->payload_bytes_per_leaf());
  }

  ~bplus_tree_leaf() override = default;

  template<typename TxAlloc, typename DbAlloc, typename CompletionToken>
  static auto async_new_page(
      typename raw_db_type::template fdb_transaction<TxAlloc> tx,
      DbAlloc db_alloc,
      std::shared_ptr<const bplus_tree_spec> spec,
      cycle_ptr::cycle_gptr<raw_db_type> raw_db,
      std::uint64_t parent_offset,
      std::uint64_t pred_sibling,
      std::uint64_t succ_sibling,
      bool create_before_first_and_after_last,
      CompletionToken&& token) {
    using tx_file_type = typename raw_db_type::template fdb_transaction<TxAlloc>::file;
    using stream_type = positional_stream_adapter<tx_file_type>;
    using page_type = bplus_tree_page<Executor, Allocator>;

    return asio::async_initiate<CompletionToken, void(std::error_code, cycle_ptr::cycle_gptr<bplus_tree_leaf>)>(
        [](auto handler, typename raw_db_type::template fdb_transaction<TxAlloc> tx, DbAlloc db_alloc, std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, std::uint64_t parent_offset, std::uint64_t pred_sibling, std::uint64_t succ_sibling, bool create_before_first_and_after_last) -> void {
          db_alloc.async_allocate(tx, spec->bytes_per_leaf(), asio::deferred)
          | asio::deferred(
              [tx, raw_db, spec, parent_offset, pred_sibling, succ_sibling, create_before_first_and_after_last](std::error_code ec, db_address addr) mutable {
                cycle_ptr::cycle_gptr<bplus_tree_leaf> new_page;
                if (!ec) {
                  new_page = cycle_ptr::allocate_cycle<bplus_tree_leaf>(raw_db->get_cache_allocator(), spec, raw_db, std::move(addr), parent_offset, pred_sibling, succ_sibling);

                  if (create_before_first_and_after_last) [[unlikely]] { // Unlikey, because only first root page needs this.
                    new_page->use_list_span().front() = bplus_tree_leaf_use_element::before_first;
                    new_page->use_list_span().back() = bplus_tree_leaf_use_element::after_last;

                    assert(new_page->elements_.empty());
                    new_page->elements_.emplace_back(cycle_ptr::allocate_cycle<element>(new_page->get_allocator(), new_page->get_executor(), 0u, new_page, new_page->get_allocator()));
                    new_page->elements_.emplace_back(cycle_ptr::allocate_cycle<element>(new_page->get_allocator(), new_page->get_executor(), new_page->use_list_span().size() - 1u, new_page, new_page->get_allocator()));
                  }

                  tx.on_commit(
                      [raw_db, new_page]() {
                        raw_db->template emplace<page_type>(
                            typename raw_db_type::template key_type<page_type>{
                              .file=new_page->address.file,
                              .offset=new_page->address.offset
                            },
                            new_page);
                      });
                }

                return asio::deferred.when(!ec)
                    .then(
                        asio::deferred.values(tx, new_page)
                        | asio::deferred(
                            [](typename raw_db_type::template fdb_transaction<TxAlloc> tx, cycle_ptr::cycle_gptr<bplus_tree_leaf> new_page) {
                              auto stream = std::allocate_shared<stream_type>(tx.get_allocator(), tx[new_page->address.file], new_page->address.offset);
                              return async_write(
                                  *stream,
                                  (::earnest::xdr_writer<>() & xdr_constant(bplus_tree_page_header{.magic=magic, .parent=new_page->parent_page_offset()}))
                                  + bplus_tree_leaf::continue_xdr_(::earnest::xdr_writer<>(), *new_page),
                                  asio::append(asio::deferred, new_page, stream));
                            })
                        | asio::deferred(
                            [](std::error_code ec, cycle_ptr::cycle_gptr<bplus_tree_leaf> new_page, [[maybe_unused]] auto stream) {
                              return asio::deferred.values(std::move(ec), std::move(new_page));
                            }))
                    .otherwise(asio::deferred.values(ec, new_page));
              })
          | std::move(handler);
        },
        token, std::move(tx), std::move(db_alloc), std::move(spec), std::move(raw_db), std::move(parent_offset), std::move(pred_sibling), std::move(succ_sibling), std::move(create_before_first_and_after_last));
  }

  auto elements() const noexcept -> const element_vector& {
    return elements_;
  }

  auto next_page_address() const -> db_address {
    db_address result = this->address;
    result.offset = hdr_.successor_page;
    return result;
  }

  auto prev_page_address() const -> db_address {
    db_address result = this->address;
    result.offset = hdr_.predecessor_page;
    return result;
  }

  // Accepts a completion-handler of the form:
  // void(std::error_code, cycle_ptr::cycle_gptr<bplus_tree_leaf>, monitor::{shared|exclusive}_lock)
  template<bool ForWrite>
  auto next_page_op(std::bool_constant<ForWrite> for_write) const {
    return nextprev_page_op_(std::move(for_write), &bplus_tree_leaf::next_page_address, &bplus_tree_leaf::prev_page_address);
  }

  // Accepts a completion-handler of the form:
  // void(std::error_code, cycle_ptr::cycle_gptr<bplus_tree_leaf>, monitor::{shared|exclusive}_lock)
  template<bool ForWrite, typename Lock>
  auto next_page_op(std::bool_constant<ForWrite> for_write, Lock lock) const {
    return this->page_variant_op(
        for_write, lock,
        // get-address function:
        [](const auto& self) {
          return boost::polymorphic_pointer_downcast<cycle_ptr::cycle_gptr<const bplus_tree_leaf>>(self)->next_page_address();
        },
        // validation function:
        [](const auto& target_ptr, const auto& self_ptr) {
          return std::invoke(
              overload(
                  [](const bplus_tree_leaf& leaf, const auto& self_ptr) {
                    return leaf.prev_page_address() == self_ptr->address;
                  },
                  []([[maybe_unused]] const bplus_tree_intr<Executor, Allocator>& intr, [[maybe_unused]] const auto& self_ptr) {
                    return true; // It's actually the wrong type, but that page (intr) was loaded while this page (self_ptr) was locked, so we know it was the actual pointer.
                                 // Next, the variant_to_pointer_op will cause this to be rejected.
                  }),
              *target_ptr, self_ptr);
        },
        // restart function:
        [self=this->shared_from_this(this), for_write, lock]() mutable {
          return self->next_page_op(std::move(for_write), std::move(lock));
        })
    | variant_to_pointer_op<bplus_tree_leaf>();
  }

  // Accepts a completion-handler of the form:
  // void(std::error_code, cycle_ptr::cycle_gptr<bplus_tree_leaf>, monitor::{shared|exclusive}_lock)
  template<bool ForWrite>
  auto prev_page_op(std::bool_constant<ForWrite> for_write) const {
    return nextprev_page_op_(std::move(for_write), &bplus_tree_leaf::prev_page_address, &bplus_tree_leaf::next_page_address);
  }

  private:
  template<typename Stream, typename CompletionToken>
  auto continue_load_(Stream& stream, cycle_ptr::cycle_gptr<raw_db_type> raw_db, CompletionToken&& token) {
    return async_read(
        stream,
        bplus_tree_leaf::continue_xdr_(::earnest::xdr_reader<>(), *this)
        & xdr_processor(
            [this, raw_db]() -> std::error_code {
              assert(this->elements_.empty());
              const auto use_list = this->use_list_span();
              for (std::size_t i = 0; i < use_list.size(); ++i) {
                if (use_list[i] != bplus_tree_leaf_use_element::unused) {
                  this->elements_.push_back(
                      cycle_ptr::allocate_cycle<element>(
                          this->get_allocator(),
                          this->get_executor(),
                          i,
                          this->shared_from_this(this),
                          raw_db->get_allocator()));
                }
              }
              return {};
            }),
        std::forward<CompletionToken>(token));
  }

  template<typename X>
  static auto continue_xdr_(::earnest::xdr<X>&& x, typename ::earnest::xdr<X>::template typed_function_arg<bplus_tree_leaf> y) {
    return std::move(x) & y.hdr_ & xdr_dynamic_raw_bytes(asio::buffer(y.bytes_));
  }

  template<bool ForWrite, typename GetAddrFn, typename InverseGetAddrFn>
  auto nextprev_page_op_(std::bool_constant<ForWrite> for_write, GetAddrFn get_addr_fn, InverseGetAddrFn inverse_get_addr_fn) {
    return this->page_lock.async_shared(asio::deferred)
    | asio::deferred(
        [ self=this->shared_from_this(this),
          for_write=std::move(for_write),
          get_addr_fn=std::move(get_addr_fn),
          inverse_get_addr_fn=std::move(inverse_get_addr_fn)
        ](auto lock) mutable {
          return self->page_variant_op(
              for_write, std::move(lock),
              // get-address function:
              [get_addr_fn](const auto& self) {
                return std::invoke(get_addr_fn, boost::polymorphic_pointer_downcast<cycle_ptr::cycle_gptr<bplus_tree_leaf>>(self));
              },
              // validation function:
              [inverse_get_addr_fn](const auto& target_ptr, const auto& self_ptr) {
                return std::invoke(
                    overload(
                        [&inverse_get_addr_fn](const bplus_tree_leaf& leaf, const auto& self_ptr) -> bool {
                          return std::invoke(inverse_get_addr_fn, &leaf) == self_ptr->address;
                        },
                        []([[maybe_unused]] const bplus_tree_intr<Executor, Allocator>& intr, [[maybe_unused]] const auto& self_ptr) -> bool {
                          return true; // It's actually the wrong type, but that page (intr) was loaded while this page (self_ptr) was locked, so we know it was the actual pointer.
                                       // Next, the variant_to_pointer_op will cause this to be rejected.
                        }),
                    *target_ptr, self_ptr);
              },
              // restart function:
              [self, for_write, get_addr_fn, inverse_get_addr_fn]() mutable {
                return self->nextprev_page_op_(std::move(for_write), std::move(get_addr_fn), std::move(inverse_get_addr_fn));
              });
        })
    | variant_to_pointer_op<bplus_tree_leaf>();
  }

  auto use_list_offset() const noexcept -> std::size_t { return 0u; }
  auto use_list_length() const noexcept -> std::size_t { return this->spec->elements_per_leaf; }
  auto element0_offset() const noexcept -> std::size_t { return use_list_offset() + round_up(use_list_length(), 4u); }
  auto element_length() const noexcept -> std::size_t { return this->spec->element.key.padded_bytes() + this->spec->element.value.padded_bytes(); }
  auto element_offset(std::size_t idx) const noexcept -> std::size_t { return element0_offset() + idx * element_length(); }

  auto use_list_span() const noexcept -> std::span<const bplus_tree_leaf_use_element> {
    assert(bytes_.size() >= use_list_offset() + use_list_length());
    const auto use_list_bytes = std::span<const std::byte>(bytes_).subspan(use_list_offset(), use_list_length());
    return {reinterpret_cast<const bplus_tree_leaf_use_element*>(use_list_bytes.data()), use_list_bytes.size()};
  }

  auto use_list_span() noexcept -> std::span<bplus_tree_leaf_use_element> {
    assert(bytes_.size() >= use_list_offset() + use_list_length());
    const auto use_list_bytes = std::span<std::byte>(bytes_).subspan(use_list_offset(), use_list_length());
    return {reinterpret_cast<bplus_tree_leaf_use_element*>(use_list_bytes.data()), use_list_bytes.size()};
  }

  auto key_span(std::size_t idx) const noexcept -> std::span<const std::byte> {
    assert(idx < this->spec->elements_per_leaf);
    assert(bytes_.size() >= element_offset(this->spec->elements_per_leaf));
    return { &bytes_[element_offset(idx)], this->spec->element.key.bytes };
  }

  auto key_span(std::size_t idx) noexcept -> std::span<std::byte> {
    assert(idx < this->spec->elements_per_leaf);
    assert(bytes_.size() >= element_offset(this->spec->elements_per_leaf));
    return { &bytes_[element_offset(idx)], this->spec->element.key.bytes };
  }

  auto value_span(std::size_t idx) const noexcept -> std::span<const std::byte> {
    assert(idx < this->spec->elements_per_leaf);
    assert(bytes_.size() >= element_offset(this->spec->elements_per_leaf));
    return { &bytes_[element_offset(idx) + this->spec->element.key.padded_bytes()], this->spec->element.value.bytes };
  }

  auto value_span(std::size_t idx) noexcept -> std::span<std::byte> {
    assert(idx < this->spec->elements_per_leaf);
    assert(bytes_.size() >= element_offset(this->spec->elements_per_leaf));
    return { &bytes_[element_offset(idx) + this->spec->element.key.padded_bytes()], this->spec->element.value.bytes };
  }

  auto async_compute_page_augments_impl_(std::function<void(std::vector<std::byte>)> callback) const -> void override {
    using byte_vector = std::vector<std::byte>;
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;

    const auto augments = std::make_shared<std::vector<byte_vector>>(this->elements_.size());
    auto barrier = make_completion_barrier(
        [ augments, callback=std::move(callback),
          self=this->shared_from_this(this)
        ](std::error_code ec) {
          assert(!ec);

          auto augment_spans = std::vector<std::span<const std::byte>>(augments->size());
          std::transform(
              augments->begin(), augments->end(),
              augment_spans.begin(),
              [](const byte_vector& v) { return std::span<const std::byte>(v); });
          callback(self->combine_augments(std::move(augment_spans)));
        },
        this->get_executor());

    for (std::size_t i = 0; i < this->elements_.size(); ++i) {
      const cycle_ptr::cycle_gptr<element> eptr = this->elements_[i];
      eptr->element_lock.dispatch_shared(
          completion_wrapper<void(monitor_shlock_type)>(
              ++barrier,
              [ eptr, i, augments,
                self=this->shared_from_this(this)
              ](auto handler, monitor_shlock_type lock) -> void {
                (*augments)[i] = self->augments_for_value_(*eptr);
                lock.reset();
                std::invoke(handler, std::error_code());
              }));
    }

    std::invoke(barrier, std::error_code());
  }

  auto augments_for_value_(const element& e) const -> std::vector<std::byte> {
    using byte_vector = std::vector<std::byte>;

    auto augments = byte_vector(this->spec->element.padded_augment_bytes(), std::byte(0));
    std::span<std::byte> data_span = std::span(augments);
    const std::span<const std::byte> key_span = this->key_span(e.index);
    const std::span<const std::byte> value_span = this->value_span(e.index);

    for (const bplus_tree_augment_spec& spec : this->spec->element.augments) {
      assert(data_span.size() >= spec.padded_bytes());
      spec.augment_value(data_span.subspan(0, spec.bytes), key_span, value_span);
      data_span = data_span.subspan(spec.padded_bytes());
    }
    assert(data_span.empty());

    return augments;
  }

  std::vector<std::byte, rebind_alloc<std::byte>> bytes_;
  element_vector elements_;
  bplus_tree_leaf_header hdr_;
};


template<typename Executor, typename DBAllocator, typename Allocator>
class bplus_tree
: public db_cache_value,
  private cycle_ptr::cycle_base
{
  public:
  static inline constexpr std::uint32_t magic = bplus_tree_header::magic;
  using raw_db_type = raw_db<Executor, Allocator>;
  using executor_type = typename raw_db_type::executor_type;
  using allocator_type = typename raw_db_type::cache_allocator_type;

  private:
  using page_type = bplus_tree_page<Executor, Allocator>;
  using intr_type = bplus_tree_intr<Executor, Allocator>;
  using leaf_type = bplus_tree_leaf<Executor, Allocator>;

  public:
  using element = typename leaf_type::element;

  static inline constexpr std::uint64_t nil_page = page_type::nil_page;

  bplus_tree(std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address, DBAllocator db_allocator) noexcept(std::is_nothrow_move_constructible_v<db_address>)
  : address(std::move(address)),
    raw_db(raw_db),
    spec(std::move(spec)),
    page_lock(raw_db->get_executor(), raw_db->get_allocator()),
    ex_(raw_db->get_executor()),
    alloc_(raw_db->get_cache_allocator()),
    db_alloc_(std::move(db_allocator))
  {}

  ~bplus_tree() override = default;

  auto get_executor() const -> executor_type { return ex_; }
  auto get_allocator() const -> allocator_type { return alloc_; }

  template<typename AcceptanceFn>
  static auto async_load_op(std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address, DBAllocator db_allocator, AcceptanceFn&& acceptance_fn) {
    return raw_db->async_get(
        typename raw_db_type::template key_type<bplus_tree>{.file=address.file, .offset=address.offset},
        asio::deferred,
        [](auto stream, std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address, DBAllocator db_allocator) {
          auto stream_ptr = std::make_shared<decltype(stream)>(std::move(stream));
          return raw_load_op(*stream_ptr, std::move(spec), std::move(raw_db), std::move(address), std::move(db_allocator))
          | asio::deferred(
              [stream_ptr](std::error_code ec, cycle_ptr::cycle_gptr<bplus_tree> tree) mutable {
                stream_ptr.reset(); // Using `stream_ptr` like this silences compiler warnings.
                return asio::deferred.values(std::move(ec), std::move(tree));
              });
        },
        std::forward<AcceptanceFn>(acceptance_fn),
        spec, raw_db, address, std::move(db_allocator));
  }

  template<typename BytesAllocator = std::allocator<std::byte>>
  static auto creation_bytes(BytesAllocator allocator = BytesAllocator()) -> std::vector<std::byte, typename std::allocator_traits<BytesAllocator>::template rebind_alloc<std::byte>> {
    asio::io_context ioctx;
    auto stream = byte_stream<asio::io_context::executor_type, typename std::allocator_traits<Allocator>::template rebind_alloc<std::byte>>(ioctx.get_executor(), allocator);
    std::error_code error;
    async_write(
        stream,
        ::earnest::xdr_writer<>() & xdr_constant(bplus_tree_header{.root=nil_page}),
        [&error](std::error_code ec) { error = ec; });
    ioctx.run();
    if (error) throw std::system_error(error);
    return std::move(stream).data();
  }

  private:
  template<typename TxAlloc>
  struct tree_path {
    using allocator_type = TxAlloc;

    template<typename PageType>
    struct versioned_page {
      constexpr versioned_page() noexcept = default;

      explicit versioned_page(cycle_ptr::cycle_gptr<PageType> page) noexcept
      : page(std::move(page)),
        versions(this->page->versions)
      {}

      auto assign(cycle_ptr::cycle_gptr<PageType> page) noexcept -> void {
        *this = versioned_page(std::move(page));
      }

      cycle_ptr::cycle_gptr<PageType> page;
      bplus_tree_versions versions;
    };

    tree_path() = default;

    explicit tree_path(cycle_ptr::cycle_gptr<bplus_tree> tree, allocator_type alloc = allocator_type())
    : tree(std::move(tree)),
      interior_pages(std::move(alloc))
    {}

    auto get_allocator() const -> allocator_type { return interior_pages.get_allocator(); }

    cycle_ptr::cycle_gptr<bplus_tree> tree;
    std::vector<versioned_page<intr_type>, typename std::allocator_traits<TxAlloc>::template rebind_alloc<versioned_page<intr_type>>> interior_pages;
    versioned_page<leaf_type> leaf_page;
  };

  template<typename Stream>
  static auto raw_load_op(Stream& stream, std::shared_ptr<const bplus_tree_spec> spec, cycle_ptr::cycle_gptr<raw_db_type> raw_db, db_address address, DBAllocator db_allocator) {
    auto result = cycle_ptr::allocate_cycle<bplus_tree>(raw_db->get_cache_allocator(), std::move(spec), raw_db, std::move(address), std::move(db_allocator));
    return async_read(
        stream,
        ::earnest::xdr_reader<>() & result->hdr_,
        asio::append(asio::deferred, result));
  }

  // Install a root page.
  // Callback arguments:
  // - std::error_code ec: error code
  // - cycle_ptr::cycle_gptr<leaf_type> leaf: newly installed root page
  // - monitor<executor_type, raw_db_type::allocator_type>::exclusive_lock exlock: exclusive lock on the tree
  //
  // Call may fail with bplus_tree_errc::root_page_present, in which case a root page already exists.
  // In this case, the lock will be held.
  template<typename TxAlloc, typename CompletionToken>
  auto install_root_page_(TxAlloc tx_alloc, CompletionToken&& token) {
    using tx_file_type = typename raw_db_type::template fdb_transaction<TxAlloc>::file;
    using stream_type = positional_stream_adapter<tx_file_type>;
    using monitor_exlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock;

    return asio::async_initiate<CompletionToken, void(std::error_code, cycle_ptr::cycle_gptr<leaf_type>, monitor_exlock_type)>(
        [](auto handler, TxAlloc tx_alloc, cycle_ptr::cycle_gptr<bplus_tree> self) -> void {
          cycle_ptr::cycle_gptr<raw_db_type> raw_db = self->raw_db.lock();
          if (raw_db == nullptr) {
            std::invoke(handler, make_error_code(db_errc::data_expired), nullptr, monitor_exlock_type{});
            return;
          }
          auto tx = raw_db->fdb_tx_begin(isolation::read_commited, tx_mode::read_write, tx_alloc);

          leaf_type::async_new_page(tx, self->db_alloc_, self->spec, raw_db, nil_page, nil_page, nil_page, true, asio::deferred)
          | asio::deferred(
              // Verify the allocator didn't accidentally allocate in a different file.
              [self](std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> leaf) {
                if (!ec && self->address.file != leaf->address.file) [[unlikely]] {
                  std::clog << "bplus-tree: tree lives in " << self->address.file << ", but allocator allocates to " << leaf->address.file << "; will fail root-page construction since bplus-tree cannot span multiple files.\n";
                  ec = make_error_code(bplus_tree_errc::allocator_file_mismatch);
                }

                return asio::deferred.values(std::move(ec), std::move(leaf));
              })
          | asio::deferred(
              // Write an updated bplus_tree record.
              [tx, self](std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> leaf) {
                return asio::deferred.when(!ec)
                    .then(
                        asio::deferred.values(tx, leaf)
                        | asio::deferred(
                            [self](auto tx, cycle_ptr::cycle_gptr<leaf_type> leaf) {
                              auto stream = std::allocate_shared<stream_type>(tx.get_allocator(), tx[self->address.file], self->address.offset);
                              auto new_hdr = self->hdr_;
                              new_hdr.root = leaf->address.offset;
                              return async_write(
                                  *stream,
                                  ::earnest::xdr_writer<>() & xdr_constant(std::move(new_hdr)),
                                  asio::append(asio::deferred, leaf, stream))
                              | asio::deferred(
                                  // We need to capture stream, to ensure it remains valid until the write-call completes.
                                  [](std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> leaf, [[maybe_unused]] auto stream) {
                                    return asio::deferred.values(std::move(ec), std::move(leaf));
                                  });
                            }))
                    .otherwise(asio::deferred.values(ec, leaf));
              })
          | asio::deferred(
              [self](std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> leaf) {
                return asio::deferred.when(!ec)
                    .then(self->page_lock.dispatch_exclusive(asio::append(asio::deferred, ec, leaf)))
                    .otherwise(asio::deferred.values(monitor_exlock_type(), ec, leaf));
              })
          | asio::deferred(
              // Re-order arguments, because having `ec` not be the first argument is weird.
              [](monitor_exlock_type lock, std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> leaf) {
                return asio::deferred.values(std::move(ec), std::move(leaf), std::move(lock));
              })
          | asio::deferred(
              [self](std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> leaf, monitor_exlock_type lock) {
                if (!ec && self->erased_) [[unlikely]]
                  ec = make_error_code(db_errc::data_expired);
                if (!ec && self->hdr_.root != nil_page)
                  ec = make_error_code(bplus_tree_errc::root_page_present);

                if (!ec) self->hdr_.root = leaf->address.offset;
                return asio::deferred.values(std::move(ec), std::move(leaf), std::move(lock));
              })
          | asio::deferred(
              [tx](std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> leaf, monitor_exlock_type lock) mutable {
                return asio::deferred.when(!ec)
                    .then(tx.async_commit(asio::append(asio::deferred, leaf, lock)))
                    .otherwise(asio::deferred.values(ec, leaf, lock));
              })
          | std::move(handler);
        },
        token, std::move(tx_alloc), this->shared_from_this(this));
  }

  auto maybe_load_root_page_nocreate_op_() {
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;
    using page_variant = std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>>;

    auto self = this->shared_from_this(this);

    return self->page_lock.dispatch_shared(asio::deferred)
    | asio::deferred(
        [self](monitor_shlock_type tree_lock) {
          const auto load_offset = self->hdr_.root;
          tree_lock.reset();

          std::error_code ec;
          auto raw_db = self->raw_db.lock();
          if (raw_db == nullptr) ec = make_error_code(db_errc::data_expired);

          return asio::deferred.when(!ec && load_offset != nil_page)
              .then(
                  page_type::async_load_op(
                      self->spec,
                      raw_db,
                      db_address(self->address.file, load_offset),
                      [self, load_offset]() {
                        return self->page_lock.dispatch_shared(asio::deferred)
                        | asio::deferred(
                            [self, load_offset]([[maybe_unused]] monitor_shlock_type tree_lock) {
                              std::error_code ec;
                              if (self->erased_ || self->hdr_.root != load_offset)
                                ec = make_error_code(bplus_tree_errc::restart);
                              return asio::deferred.values(ec);
                            });
                      })
                  | asio::deferred(
                      [self, load_offset](std::error_code ec, page_variant page) {
                        return asio::deferred.when(!ec)
                            .then(
                                self->page_lock.dispatch_shared(asio::append(asio::deferred, page))
                                | asio::deferred(
                                    [self, load_offset](monitor_shlock_type tree_lock, page_variant page) {
                                      std::error_code ec;
                                      if (self->erased_) [[unlikely]]
                                        ec = make_error_code(db_errc::data_expired);
                                      else if (self->hdr_.root != load_offset)
                                        ec = make_error_code(bplus_tree_errc::restart);

                                      return asio::deferred.values(std::move(ec), std::move(page), std::move(tree_lock));
                                    }
                                ))
                            .otherwise(asio::deferred.values(ec, page, monitor_shlock_type{}));
                      }))
              .otherwise(asio::deferred.values(std::error_code(), page_variant(), tree_lock));
        });
  }

  auto load_root_page_nocreate_op_() {
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;
    using page_variant = std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>>;

    return asio::async_initiate<decltype(asio::deferred), void(std::error_code, page_variant, monitor_shlock_type)>(
        []<typename Handler>(Handler handler, cycle_ptr::cycle_gptr<bplus_tree> self) -> void {
          struct op {
            explicit op(cycle_ptr::cycle_gptr<bplus_tree> self) noexcept
            : self(std::move(self))
            {}

            auto operator()(Handler handler) const -> void {
              self->maybe_load_root_page_nocreate_op_()
              | completion_wrapper<void(std::error_code, page_variant, monitor_shlock_type)>(
                  std::move(handler),
                  [op=*this](auto handler, std::error_code ec, page_variant page, monitor_shlock_type tree_lock) mutable {
                    if (ec == make_error_code(bplus_tree_errc::restart))
                      return std::invoke(op, std::move(handler));
                    else
                      std::invoke(handler, std::move(ec), std::move(page), std::move(tree_lock));
                  });
            }

            private:
            cycle_ptr::cycle_gptr<bplus_tree> self;
          };

          std::invoke(op(std::move(self)), std::move(handler));
        },
        asio::deferred, this->shared_from_this(this));
  }

  template<typename TxAlloc>
  auto install_or_get_root_page_op_(TxAlloc tx_alloc) {
    using monitor_exlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock;
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;
    using lock_variant = std::variant<monitor_exlock_type, monitor_shlock_type>;
    using page_variant = std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>>;

    return asio::async_initiate<decltype(asio::deferred), void(std::error_code, page_variant, lock_variant /*tree_lock*/)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree> self, TxAlloc tx_alloc) -> void {
          auto wrapped_handler = completion_handler_fun(std::move(handler), self->get_executor());
          using handler_type = decltype(wrapped_handler);

          struct op {
            explicit op(cycle_ptr::cycle_gptr<bplus_tree> tree, handler_type handler, TxAlloc tx_alloc)
            : handler(std::move(handler)),
              tx_alloc(std::move(tx_alloc)),
              tree(std::move(tree))
            {}

            auto operator()() -> void {
              const cycle_ptr::cycle_gptr<bplus_tree> tree = this->tree; // copy, so we can move self_op.
              tree->load_root_page_nocreate_op_()
              | [ self_op=std::move(*this)
                ](std::error_code ec, page_variant root_page, monitor_shlock_type tree_lock) mutable {
                  if (ec || std::visit([](const auto& pg) { return pg != nullptr; }, root_page)) {
                    std::invoke(self_op.handler, std::move(ec), std::move(root_page), lock_variant(std::move(tree_lock)));
                  } else {
                    const cycle_ptr::cycle_gptr<bplus_tree> tree = self_op.tree; // copy, so we can move self_op.
                    TxAlloc tx_alloc = self_op.tx_alloc; // copy, so we can move self_op.
                    tree->install_root_page_(
                        tx_alloc,
                        [ self_op=std::move(self_op)
                        ](std::error_code ec, cycle_ptr::cycle_gptr<leaf_type> leaf, monitor_exlock_type tree_lock) mutable {
                          if (ec == make_error_code(bplus_tree_errc::root_page_present))
                            std::invoke(self_op);
                          else
                            std::invoke(self_op.handler, std::move(ec), page_variant(std::move(leaf)), lock_variant(std::move(tree_lock)));
                        });
                  }
                };
            }

            private:
            handler_type handler;
            TxAlloc tx_alloc;
            cycle_ptr::cycle_gptr<bplus_tree> tree;
          };

          std::invoke(op(std::move(self), std::move(wrapped_handler), std::move(tx_alloc)));
        },
        asio::deferred, this->shared_from_this(this), std::move(tx_alloc));
  }

  template<typename LeafLockType = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock, typename AddressSelectorFn, typename TxAlloc, typename CompletionToken>
  requires std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock> || std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock>
  auto tree_walk_(AddressSelectorFn address_selector_fn, TxAlloc tx_alloc, CompletionToken&& token) {
    using monitor_exlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock;
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;
    using lock_variant = std::variant<monitor_exlock_type, monitor_shlock_type>;
    using page_variant = std::variant<cycle_ptr::cycle_gptr<intr_type>, cycle_ptr::cycle_gptr<leaf_type>>;

    return asio::async_initiate<CompletionToken, void(std::error_code, tree_path<TxAlloc>, LeafLockType)>(
        [](auto handler, AddressSelectorFn address_selector_fn, TxAlloc tx_alloc, cycle_ptr::cycle_gptr<bplus_tree> self) {
          using handler_type = decltype(handler);

          struct op {
            explicit op(AddressSelectorFn address_selector_fn, handler_type handler, TxAlloc tx_alloc)
            : tx_alloc(std::move(tx_alloc)),
              handler(std::move(handler)),
              address_selector_fn(std::move(address_selector_fn))
            {}

            auto start(cycle_ptr::cycle_gptr<bplus_tree> tree) -> void {
              TxAlloc tx_alloc = this->tx_alloc; // Copy, because we'll move *this.
              tree->install_or_get_root_page_op_(std::move(tx_alloc))
              | [ self_op=std::move(*this),
                  tree
                ](std::error_code ec, page_variant page, lock_variant parent_lock) mutable -> void {
                  if (ec) {
                    self_op.error_invoke_(ec);
                  } else {
                    std::visit(
                        [&](auto page) -> void {
                          self_op.search(tree_path<TxAlloc>(std::move(tree), self_op.tx_alloc), std::move(page), std::move(parent_lock));
                        },
                        std::move(page));
                  }
                };
            }

            private:
            auto search(tree_path<TxAlloc> path, cycle_ptr::cycle_gptr<leaf_type> page, lock_variant parent_lock) -> void {
              auto callback =
                  [ self_op=std::move(*this),
                    path=std::move(path),
                    page,
                    parent_lock=std::move(parent_lock)
                  ](auto page_lock) mutable -> void {
                    std::visit([](auto& lock) { lock.reset(); }, parent_lock); // No longer needed, now that `page_lock` is locked.
                    path.leaf_page.assign(page);
                    self_op.search_with_page_locked(std::move(path), std::move(page), std::move(page_lock));
                  };
              if constexpr(std::is_same_v<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock>) {
                page->page_lock.dispatch_shared(std::move(callback));
              } else {
                static_assert(std::is_same_v<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock>);
                page->page_lock.dispatch_exclusive(std::move(callback));
              }
            }

            auto search(tree_path<TxAlloc> path, cycle_ptr::cycle_gptr<intr_type> page, lock_variant parent_lock) -> void {
              page->page_lock.dispatch_shared(
                  [ self_op=std::move(*this),
                    path=std::move(path),
                    page,
                    parent_lock=std::move(parent_lock)
                  ](auto page_lock) mutable -> void {
                    std::visit([](auto& lock) { lock.reset(); }, parent_lock); // No longer needed, now that `page_lock` is locked.
                    path.interior_pages.emplace_back(page);
                    self_op.search_with_page_locked(std::move(path), std::move(page), std::move(page_lock));
                  });
            }

            auto search_with_page_locked(tree_path<TxAlloc> path, [[maybe_unused]] cycle_ptr::cycle_gptr<leaf_type> page, LeafLockType page_lock) -> void {
              assert(path.leaf_page.page == page);
              std::invoke(handler, std::error_code(), std::move(path), std::move(page_lock));
            }

            auto search_with_page_locked(tree_path<TxAlloc> path, cycle_ptr::cycle_gptr<intr_type> intr, monitor_shlock_type page_lock) -> void {
              auto intr_version = intr->versions.page_split;
              db_address child_page_address;
              cycle_ptr::cycle_gptr<raw_db_type> raw_db;
              {
                std::error_code ec;
                std::visit(
                    overload(
                        [&ec](std::error_code addr_selection_ec) {
                          if (!ec) throw std::logic_error("address selector returned an error without error state");
                          ec = addr_selection_ec;
                        },
                        [&child_page_address](db_address addr) {
                          child_page_address = std::move(addr);
                        }),
                    std::invoke(address_selector_fn, intr));
                page_lock.reset(); // No longer need the lock.

                if (!ec) [[likely]] {
                  raw_db = intr->raw_db.lock();
                  if (raw_db == nullptr) [[unlikely]] ec = make_error_code(db_errc::data_expired);
                }

                if (ec) {
                  error_invoke_(ec);
                  return;
                }
              }

              page_type::async_load_op(
                  intr->spec, std::move(raw_db), child_page_address,
                  [intr, child_page_address]() {
                    return intr->page_lock.dispatch_shared(asio::deferred)
                    | asio::deferred(
                        [intr, child_page_address]([[maybe_unused]] monitor_shlock_type intr_lock) {
                          std::error_code ec;
                          if (!intr->contains(child_page_address))
                            ec = make_error_code(bplus_tree_errc::restart);
                          return asio::deferred.values(ec);
                        });
                  })
              | asio::deferred(
                  [intr](std::error_code ec, page_variant child_page) {
                    return asio::deferred.when(!ec)
                        .then(intr->page_lock.dispatch_shared(asio::append(asio::deferred, ec, child_page)))
                        .otherwise(asio::deferred.values(monitor_shlock_type{}, ec, child_page));
                  })
              | [ self_op=std::move(*this),
                  path=std::move(path),
                  intr, intr_version
                ](monitor_shlock_type intr_lock, std::error_code ec, page_variant child_page) mutable -> void {
                  if (!ec) [[likely]] {
                    if (intr->erased_ || intr->versions.page_split != intr_version) {
                      ec = make_error_code(bplus_tree_errc::restart);
                    } else {
                      std::visit(
                          overload(
                              [&ec](std::error_code addr_selection_ec) {
                                if (!ec) throw std::logic_error("address selector returned an error without error state");
                                ec = addr_selection_ec;
                              },
                              [&ec, &child_page](const db_address& addr) {
                                const db_address& child_address = std::visit<const db_address&>(
                                    [](const auto& child_ptr) -> const db_address& {
                                      return child_ptr->address;
                                    },
                                    child_page);
                                if (addr != child_address) [[unlikely]]
                                  ec = make_error_code(bplus_tree_errc::restart);
                              }),
                          std::invoke(self_op.address_selector_fn, intr));
                    }
                  }

                  if (ec == make_error_code(bplus_tree_errc::restart)) {
                    self_op.restart(std::move(path));
                  } else if (ec) {
                    self_op.error_invoke_(std::move(ec));
                  } else {
                    std::visit(
                        [&](auto page) -> void {
                          self_op.search(std::move(path), std::move(page), std::move(intr_lock));
                        },
                        std::move(child_page));
                  }
                };
            }

            auto restart(tree_path<TxAlloc> path) -> void {
              if (path.interior_pages.empty()) {
                auto tx_alloc = path.get_allocator();
                path.tree->install_or_get_root_page_op_(tx_alloc)
                | [ self_op=std::move(*this),
                    path=std::move(path)
                  ](std::error_code ec, page_variant page, lock_variant parent_lock) mutable -> void {
                    if (ec) {
                      self_op.error_invoke_(std::move(ec));
                    } else {
                      std::visit(
                          [&](auto page) -> void {
                            self_op.search(std::move(path), std::move(page), std::move(parent_lock));
                          },
                          std::move(page));
                    }
                  };
              } else { // !path.interior_pages.empty()
                path.interior_pages.back().page->page_lock.dispatch_shared(asio::deferred)
                | [ self_op=std::move(*this),
                    path=std::move(path)
                  ](monitor_shlock_type last_page_lock) mutable -> void {
                    auto& last_page = path.interior_pages.back();
                    if (last_page.page->erased_ || last_page.page->versions.page_split != last_page.versions.page_split) {
                      // We want to restart the search from the most recent page that hasn't been split/merged since we last observed it.
                      path.interior_pages.pop_back();
                      self_op.restart(std::move(path));
                    } else {
                      self_op.search_with_page_locked(std::move(path), path.interior_pages.back().page, std::move(last_page_lock));
                    }
                  };
              }
            }

            auto error_invoke_(std::error_code ec) -> void {
              std::invoke(handler, ec, tree_path<TxAlloc>(nullptr, std::move(tx_alloc)), LeafLockType{});
            }

            TxAlloc tx_alloc;
            handler_type handler;
            AddressSelectorFn address_selector_fn;
          };

          op(std::move(address_selector_fn), std::move(handler), std::move(tx_alloc)).start(std::move(self));
        },
        token, std::move(address_selector_fn), std::move(tx_alloc), this->shared_from_this(this));
  }

  template<typename LeafLockType = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock, typename TxAlloc>
  requires std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock> || std::same_as<LeafLockType, typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock>
  auto find_leaf_page_for_insert_op_(TxAlloc tx_alloc, std::span<const std::byte> key) {
    return tree_walk_<LeafLockType>(
        [key](cycle_ptr::cycle_gptr<intr_type> intr) -> std::variant<db_address, std::error_code> {
          std::optional<db_address> opt_child_page_address = intr->key_find(key);
          if (opt_child_page_address.has_value())
            return std::move(opt_child_page_address).value();
          else
            return make_error_code(bplus_tree_errc::bad_tree);
        },
        std::move(tx_alloc),
        asio::deferred);
  }

  public:
  template<typename TxAlloc, typename CompletionToken>
  auto async_before_first_element(TxAlloc tx_alloc, CompletionToken&& token) {
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;

    return asio::async_initiate<CompletionToken, void(std::error_code, cycle_ptr::cycle_gptr<element>)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree> self, TxAlloc tx_alloc) {
          self->tree_walk_(
              [](cycle_ptr::cycle_gptr<intr_type> intr) -> std::variant<db_address, std::error_code> {
                std::optional<db_address> addr = intr->get_index(0);
                if (addr.has_value())
                  return std::move(addr).value();
                else
                  return make_error_code(bplus_tree_errc::bad_tree);
              },
              std::move(tx_alloc),
              completion_wrapper<void(std::error_code, tree_path<TxAlloc>, monitor_shlock_type)>(
                  std::move(handler),
                  [](auto handler, std::error_code ec, tree_path<TxAlloc> path, monitor_shlock_type leaf_lock) {
                    cycle_ptr::cycle_gptr<element> elem_ptr;

                    if (!ec && path.leaf_page.page->elements().empty()) [[unlikely]]
                      ec = make_error_code(bplus_tree_errc::bad_tree);

                    if (!ec) {
                      elem_ptr = path.leaf_page.page->elements().front();
                      if (elem_ptr->type() != bplus_tree_leaf_use_element::before_first) [[unlikely]] {
                        ec = make_error_code(bplus_tree_errc::bad_tree);
                        elem_ptr.reset();
                      }
                    }

                    leaf_lock.reset(); // No longer need the lock.
                    std::invoke(handler, ec, elem_ptr);
                  }));
        },
        token, this->shared_from_this(this), std::move(tx_alloc));
  }

  template<typename TxAlloc, typename CompletionToken>
  auto async_after_last_element(TxAlloc tx_alloc, CompletionToken&& token) {
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;

    return asio::async_initiate<CompletionToken, void(std::error_code, cycle_ptr::cycle_gptr<element>)>(
        [](auto handler, cycle_ptr::cycle_gptr<bplus_tree> self, TxAlloc tx_alloc) {
          self->tree_walk_(
              [](cycle_ptr::cycle_gptr<intr_type> intr) -> std::variant<db_address, std::error_code> {
                std::optional<db_address> addr = intr->get_index(intr->index_size() - 1u);
                if (addr.has_value())
                  return std::move(addr).value();
                else
                  return make_error_code(bplus_tree_errc::bad_tree);
              },
              std::move(tx_alloc),
              completion_wrapper<void(std::error_code, tree_path<TxAlloc>, monitor_shlock_type)>(
                  std::move(handler),
                  [](auto handler, std::error_code ec, tree_path<TxAlloc> path, monitor_shlock_type leaf_lock) {
                    cycle_ptr::cycle_gptr<element> elem_ptr;

                    if (!ec && path.leaf_page.page->elements().empty()) [[unlikely]]
                      ec = make_error_code(bplus_tree_errc::bad_tree);

                    if (!ec) {
                      elem_ptr = path.leaf_page.page->elements().back();
                      if (elem_ptr->type() != bplus_tree_leaf_use_element::after_last) [[unlikely]] {
                        ec = make_error_code(bplus_tree_errc::bad_tree);
                        elem_ptr.reset();
                      }
                    }

                    leaf_lock.reset(); // No longer need the lock.
                    std::invoke(handler, ec, elem_ptr);
                  }));
        },
        token, this->shared_from_this(this), std::move(tx_alloc));
  }

  auto TEST_FN() {
    using monitor_exlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::exclusive_lock;
    using monitor_shlock_type = typename monitor<executor_type, typename raw_db_type::allocator_type>::shared_lock;
    using lock_variant = std::variant<monitor_exlock_type, monitor_shlock_type>;

    find_leaf_page_for_insert_op_(std::allocator<std::byte>(), std::span<const std::byte>())
    | [](std::error_code ec, tree_path<std::allocator<std::byte>> path, [[maybe_unused]] lock_variant parent_lock) {
        std::clog << "find_leaf_page_for_insert_op_ yields:\n"
            << "  ec=" << ec << " (" << ec.message() << ")\n"
            << "  path{tree=" << path.tree << ", interior has " << path.interior_pages.size() << " levels, leaf=" << path.leaf_page.page << "}\n";
        if (ec) throw std::system_error(ec, "find_leaf_page_for_insert_op");
      };

    async_before_first_element(std::allocator<std::byte>(),
        [](std::error_code ec, cycle_ptr::cycle_gptr<element> elem) {
          std::clog << "before_first:\n"
              << "  ec=" << ec << " (" << ec.message() << ")\n"
              << "  elem=" << elem << "\n";
          if (ec) throw std::system_error(ec, "async_before_first_element");
        });

    async_after_last_element(std::allocator<std::byte>(),
        [](std::error_code ec, cycle_ptr::cycle_gptr<element> elem) {
          std::clog << "after_last:\n"
              << "  ec=" << ec << " (" << ec.message() << ")\n"
              << "  elem=" << elem << "\n";
          if (ec) throw std::system_error(ec, "async_after_last_element");
        });
  }

  public:
  const db_address address;

  protected:
  const cycle_ptr::cycle_weak_ptr<raw_db_type> raw_db;
  const std::shared_ptr<const bplus_tree_spec> spec;
  monitor<executor_type, typename raw_db_type::allocator_type> page_lock;

  private:
  executor_type ex_;
  allocator_type alloc_;
  bplus_tree_header hdr_;
  DBAllocator db_alloc_;
  bool erased_ = false;
};


} /* namespace earnest::detail */
