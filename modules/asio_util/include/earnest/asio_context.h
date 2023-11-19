#pragma once

#include <cstdint>
#include <memory>
#include <string_view>
#include <type_traits>
#include <utility>
#include <version>
#include <tuple>
#include <variant>

#include <asio/deferred.hpp>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/span_startoptions.h>
#include <opentelemetry/trace/tracer_provider.h>

#include <earnest/detail/err_deferred.h>
#include <earnest/detail/overload.h>

#if __cpp_lib_source_location >= 201907L
# include <source_location>
#endif

namespace earnest {


class asio_context;
using asio_context_ptr = std::shared_ptr<asio_context>;


class asio_context
: public std::enable_shared_from_this<asio_context>
{
  private:
#if __cpp_lib_source_location >= 201907L
  using source_location = std::source_location;
#else
  struct source_location {
    constexpr source_location() noexcept = default;

    static constexpr auto current() noexcept -> source_location {
      return {};
    }

    constexpr auto line() const noexcept -> std::uint_least32_t { return 0; } // OTEL attribute: code.lineno
    constexpr auto column() const noexcept -> std::uint_least32_t { return 0; } // OTEL attribute: code.column
    constexpr auto file_name() const noexcept -> const char* { return ""; } // OTEL attribute: code.filepath
    constexpr auto function_name() const noexcept -> const char* { return ""; } // OTEL attribute: code.function
  };
#endif

  public:
  template<typename...> class deferred_values;

  private:
  template<typename...> class decorated_deferred_values;

  static auto sv_to_otel(const std::string_view& s) noexcept -> opentelemetry::nostd::string_view {
    return opentelemetry::nostd::string_view(s.data(), s.size());
  }

  template<typename... T>
  static auto decorate_(asio_context_ptr ctx, deferred_values<T...> values) {
    return decorated_deferred_values(std::move(ctx), std::move(values));
  }

  template<typename... T>
  static auto decorate_([[maybe_unused]] asio_context_ptr ctx, decorated_deferred_values<T...>&& values) -> decorated_deferred_values<T...>&& {
    return std::move(values);
  }

  template<typename... T>
  static auto decorate_([[maybe_unused]] asio_context_ptr ctx, const decorated_deferred_values<T...>& values) -> decorated_deferred_values<T...> {
    return values;
  }

  template<typename Fn>
  class fn_wrapper {
    public:
    explicit fn_wrapper(Fn&& fn) noexcept(std::is_nothrow_move_constructible_v<Fn>)
    : fn(std::move(fn))
    {}

    explicit fn_wrapper(const Fn& fn) noexcept(std::is_nothrow_copy_constructible_v<Fn>)
    : fn(fn)
    {}

    template<typename... Args>
    auto operator()(asio_context_ptr ctx, Args&&... args) {
      using rv_type = decltype(
          decorate_(
              std::declval<asio_context_ptr>(),
              std::invoke(std::declval<Fn&>(), std::declval<asio_context&>(), std::declval<Args>()...)));

      auto scope = ctx->tracer->WithActiveSpan(ctx->span);

      try {
        auto rv = std::invoke(fn, *ctx, std::forward<Args>(args)...);
        return decorate_(std::move(ctx), std::move(rv));
      } catch (...) {
        return decorate_(std::move(ctx), rv_type(std::current_exception()));
      }
    }

    private:
    Fn fn;
  };

  class null_context_impl;
  class low_resolution_context_impl;
  template<typename Allocator> class high_resolution_context_impl;

  protected:
  inline asio_context(std::string_view span_name, [[maybe_unused]] const source_location& sloc)
  : span(tracer->StartSpan(sv_to_otel(span_name)))
  {
#if __cpp_lib_source_location >= 201907L
    span->SetAttribute("code.lineno", sloc.line());
    span->SetAttribute("code.column", sloc.column());
    span->SetAttribute("code.filepath", sloc.file_name());
    span->SetAttribute("code.function", sloc.function_name());
#endif
  }

  inline asio_context(const asio_context_ptr& parent, std::string_view span_name, [[maybe_unused]] const source_location& sloc)
  : span(tracer->StartSpan(sv_to_otel(span_name), opentelemetry::trace::StartSpanOptions{.parent=parent->span->GetContext()}))
  {
#if __cpp_lib_source_location >= 201907L
    span->SetAttribute("code.lineno", sloc.line());
    span->SetAttribute("code.column", sloc.column());
    span->SetAttribute("code.filepath", sloc.file_name());
    span->SetAttribute("code.function", sloc.function_name());
#endif
  }

  public:
  virtual ~asio_context() noexcept = default;

  template<typename Allocator = std::allocator<std::byte>>
  static auto null_context(Allocator alloc = Allocator()) -> asio_context_ptr {
    return std::allocate_shared<null_context_impl>(alloc);
  }

  template<typename Allocator = std::allocator<std::byte>>
  static auto low_resolution_context(std::string_view span_name, Allocator alloc = Allocator(), const source_location& sloc = source_location::current()) -> asio_context_ptr {
    return std::allocate_shared<low_resolution_context>(alloc, span_name, sloc);
  }

  template<typename Allocator = std::allocator<std::byte>>
  static auto high_resolution_context(std::string_view span_name, Allocator alloc = Allocator(), const source_location& sloc = source_location::current()) -> asio_context_ptr {
    return std::allocate_shared<low_resolution_context>(alloc, span_name, sloc, alloc);
  }

  virtual auto child_context(std::string span_name, const source_location& sloc = source_location::current()) -> asio_context_ptr = 0;
  virtual auto parent() -> asio_context_ptr = 0;

  template<typename Fn>
  auto deferred(Fn&& fn) {
    return asio::deferred(wrap_(std::forward<Fn>(fn)));
  }

  template<typename Fn>
  auto err_deferred(Fn&& fn) {
    return detail::err_deferred(wrap_(std::forward<Fn>(fn)));
  }

  private:
  template<typename Fn>
  auto wrap_(Fn&& fn) -> fn_wrapper<std::remove_cvref_t<Fn>> {
    return fn_wrapper<std::remove_cvref_t<Fn>>(std::forward<Fn>(fn));
  }

  const opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider> provider = opentelemetry::trace::Provider::GetTracerProvider();
  const opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer = provider->GetTracer("earnest");
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span;
};


class asio_context::null_context_impl
: public asio_context
{
  public:
  null_context_impl()
  : asio_context(std::string_view{}, source_location{})
  {}

  auto child_context([[maybe_unused]] std::string span_name, [[maybe_unused]] const source_location& sloc = source_location::current()) -> asio_context_ptr override {
    return this->shared_from_this();
  }

  auto parent() -> asio_context_ptr override {
    return this->shared_from_this();
  }
};


class asio_context::low_resolution_context_impl
: public asio_context
{
  public:
  explicit low_resolution_context_impl(std::string span_name, const source_location& sloc)
  : asio_context(span_name, sloc)
  {}

  auto child_context(std::string span_name, const source_location& sloc = source_location::current()) -> asio_context_ptr override {
    return this->shared_from_this();
  }

  auto parent() -> asio_context_ptr override {
    return this->shared_from_this();
  }
};


template<typename Allocator>
class asio_context::high_resolution_context_impl
: public asio_context
{
  public:
  using allocator_type = Allocator;

  private:
  class child_context_impl final
  : public asio_context
  {
    public:
    explicit child_context_impl(asio_context_ptr parent, std::string_view span_name, const source_location& sloc, allocator_type alloc)
    : asio_context(parent, span_name, sloc),
      parent_(std::move(parent)),
      alloc_(alloc)
    {}

    auto child_context(std::string span_name, const source_location& sloc = source_location::current()) -> asio_context_ptr override {
      return std::allocate_shared<child_context_impl>(get_allocator(), this->shared_from_this(), std::move(span_name), sloc, get_allocator());
    }

    auto parent() -> asio_context_ptr override {
      return parent_;
    }

    auto get_allocator() const -> allocator_type {
      return alloc_;
    }

    private:
    const asio_context_ptr parent_;
    const std::string span_name_;
    allocator_type alloc_;
  };

  public:
  high_resolution_context_impl(std::string span_name, const source_location& sloc, Allocator alloc = Allocator())
  : asio_context(span_name, sloc),
    alloc_(std::move(alloc))
  {}

  auto child_context(std::string span_name, const source_location& sloc = source_location::current()) -> asio_context_ptr override {
    return std::allocate_shared<child_context_impl>(this->shared_from_this(), std::move(span_name), sloc);
  }

  auto parent() -> asio_context_ptr override {
    throw std::logic_error("parent called on asio_context, but no parent available");
  }

  auto get_allocator() const -> allocator_type {
    return alloc_;
  }

  private:
  allocator_type alloc_;
};


template<typename... T>
class asio_context::deferred_values {
  public:
  using tuple_type = std::tuple<T...>;
  using variant_type = std::variant<
      tuple_type,
      std::error_code,
      std::exception_ptr>;

  static_assert(!std::is_same_v<std::decay_t<std::tuple_element_t<0, tuple_type>>, std::error_code>,
      "std::error_code cannot be a deferred-value: it indicates an error state");
  static_assert(!std::is_same_v<std::decay_t<std::tuple_element_t<0, tuple_type>>, std::exception_ptr>,
      "std::exception_ptr cannot be a deferred-value: it indicates an error state");

  deferred_values(std::error_code ec) noexcept
  : v_(std::in_place_index<1>, ec)
  {
    assert(ec != std::error_code{});
  }

  deferred_values(std::exception_ptr ex) noexcept
  : v_(std::in_place_index<2>, std::move(ex))
  {
    assert(std::get<2>(v_) != nullptr);
  }

  deferred_values(T... v) noexcept(std::conjunction_v<std::is_nothrow_move_constructible<T>...>)
  : v_(std::in_place_index<0>, std::move(v)...)
  {}

  auto ok() const noexcept -> bool {
    return v_.index() == 0;
  }

  auto error() const noexcept -> bool {
    return !ok();
  }

  auto values() const & -> const tuple_type& {
    assert(ok());
    return std::get<0>(v_);
  }

  auto values() & -> tuple_type& {
    assert(ok());
    return std::get<0>(v_);
  }

  auto values() && -> tuple_type&& {
    assert(ok());
    return std::get<0>(std::move(v_));
  }

  template<typename Fn>
  auto apply(Fn&& fn) & {
    return std::apply(std::forward<Fn>(fn), values());
  }

  template<typename Fn>
  auto apply(Fn&& fn) const & {
    return std::apply(std::forward<Fn>(fn), values());
  }

  template<typename Fn>
  auto apply(Fn&& fn) && {
    return std::apply(std::forward<Fn>(fn), std::move(*this).values());
  }

  template<typename Fn>
  auto visit(Fn&& fn) & {
    return std::visit(std::forward<Fn>(fn), v_);
  }

  template<typename Fn>
  auto visit(Fn&& fn) const & {
    return std::visit(std::forward<Fn>(fn), v_);
  }

  template<typename Fn>
  auto visit(Fn&& fn) && {
    return std::visit(std::forward<Fn>(fn), std::move(v_));
  }

  private:
  variant_type v_;
};

template<typename... T>
class asio_context::decorated_deferred_values {
  private:
  using tuple_type = typename deferred_values<T...>::tuple_type;

  static_assert(!std::is_same_v<std::decay_t<std::tuple_element_t<0, tuple_type>>, std::error_code>,
      "std::error_code cannot be a deferred-value: it indicates an error state");
  static_assert(!std::is_same_v<std::decay_t<std::tuple_element_t<0, tuple_type>>, std::exception_ptr>,
      "std::exception_ptr cannot be a deferred-value: it indicates an error state");

  struct invoke_ {
    template<typename Fn>
    auto operator()(Fn&& fn, asio_context_ptr&& ctx, deferred_values<T...>&& dv) const -> void {
      std::visit(
          detail::overload(
              [&fn, &ctx](tuple_type&& values) -> void {
                std::apply(
                    [&fn, &ctx]<typename... Args>(Args&&... args) -> void {
                      std::invoke(fn, std::move(ctx), std::forward<Args>(args)...);
                    },
                    std::move(values));
              },
              [&fn, &ctx](std::error_code&& ec) -> void {
                std::invoke(fn, std::move(ctx), std::move(ec));
              },
              [&fn, &ctx](std::exception_ptr&& ex) -> void {
                std::invoke(fn, std::move(ctx), std::move(ex));
              }
          ),
          std::move(dv).values());
    }

    template<typename Fn>
    auto operator()(Fn&& fn, const asio_context_ptr& ctx, const deferred_values<T...>& dv) const -> void {
      std::visit(
          detail::overload(
              [&fn, &ctx](tuple_type&& values) -> void {
                std::apply(
                    [&fn, &ctx]<typename... Args>(Args&&... args) -> void {
                      std::invoke(fn, ctx, std::forward<Args>(args)...);
                    },
                    values);
              },
              [&fn, &ctx](std::error_code&& ec) -> void {
                std::invoke(fn, ctx, std::move(ec));
              },
              [&fn, &ctx](std::exception_ptr&& ex) -> void {
                std::invoke(fn, ctx, std::move(ex));
              }
          ),
          std::move(dv).values());
    }
  };

  public:
  decorated_deferred_values(asio_context_ptr ctx, deferred_values<T...>&& dv) noexcept(std::is_nothrow_move_constructible_v<deferred_values<T...>>)
  : values(std::move(dv)),
    ctx(std::move(ctx))
  {}

  decorated_deferred_values(asio_context_ptr ctx, const deferred_values<T...>& dv) noexcept(std::is_nothrow_copy_constructible_v<deferred_values<T...>>)
  : values(dv),
    ctx(std::move(ctx))
  {}

  template<typename CompletionToken>
  auto operator()(CompletionToken&& token) && -> typename asio::async_result<CompletionToken, void(asio_context&, T...), void(std::error_code), void(std::exception_ptr)>::return_type {
    return asio::async_initiate<CompletionToken, void(asio_context&, T...), void(std::error_code), void(std::exception_ptr)>(
        invoke_{}, token, std::move(ctx), std::move(values));
  }

  template<typename CompletionToken>
  auto operator()(CompletionToken&& token) const & -> typename asio::async_result<CompletionToken, void(asio_context&, T...), void(std::error_code), void(std::exception_ptr)>::return_type {
    return asio::async_initiate<CompletionToken, void(asio_context&, T...), void(std::error_code), void(std::exception_ptr)>(
        invoke_{}, token, ctx, values);
  }

  private:
  deferred_values<T...> values;
  asio_context_ptr ctx;
};


} /* namespace earnest */

namespace asio {


template<typename... T>
struct is_deferred<::earnest::asio_context::deferred_values<T...>>
: std::true_type
{};

template<typename... T>
struct is_deferred<::earnest::asio_context::decorated_deferred_values<T...>>
: std::true_type
{};


} /* namespace asio */
