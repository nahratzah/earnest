#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#if __cpp_concepts >= 201907L
# include <concepts>
#endif

#include <asio/buffer.hpp>

#include <boost/endian.hpp>

#include <earnest/detail/array_cat.h>


namespace earnest {


enum class xdr_errc {
  decoding_error = 1,
  encoding_error,
};

auto xdr_category() -> const std::error_category&;
auto make_error_code(xdr_errc e) -> std::error_code;


} /* namespace earnest */


namespace std {


template<>
struct is_error_code_enum<::earnest::xdr_errc>
: std::true_type
{};


} /* namespace std */


namespace earnest::detail::xdr_reader {


template<typename T> struct extract_reader;
template<typename... Factories> class reader;


} /* namespace earnest::detail::xdr_reader */


namespace earnest::detail::xdr_writer {


template<typename T> struct extract_writer;
template<typename... Factories> class writer;


} /* namespace earnest::detail::xdr_writer */


namespace earnest {


template<typename Impl>
class xdr {
  template<typename> friend class xdr;
  template<typename T> friend struct ::earnest::detail::xdr_reader::extract_reader;
  template<typename T> friend struct ::earnest::detail::xdr_writer::extract_writer;

  public:
  static constexpr bool is_reader = Impl::is_reader;
  static constexpr bool is_writer = Impl::is_writer;

  xdr();
  explicit xdr(Impl&& impl) noexcept(std::is_nothrow_move_constructible_v<Impl>);

  private:
  Impl impl_;
};


template<typename... X, typename... Y>
auto operator+(xdr<detail::xdr_reader::reader<X...>>&& x, xdr<detail::xdr_reader::reader<Y...>>&& y) -> xdr<detail::xdr_reader::reader<X..., Y...>>;

template<typename... X, typename... Y>
auto operator+(xdr<detail::xdr_writer::writer<X...>>&& x, xdr<detail::xdr_writer::writer<Y...>>&& y) -> xdr<detail::xdr_writer::writer<X..., Y...>>;


} /* namespace earnest */


namespace earnest::detail::xdr_reader {


template<typename... Factories> class reader;


#if __cpp_concepts >= 201907L
template<typename T> struct is_an_xdr_reader_ : std::false_type {};
template<typename... T> struct is_an_xdr_reader_<xdr<reader<T...>>> : std::true_type {};

template<typename T>
concept xdr_reader = is_an_xdr_reader_<T>::value;

template<typename T>
concept readable = requires(xdr<reader<>> r, T v) {
  { std::move(r) & v } -> xdr_reader;
};

template<typename Fn, typename... Args>
concept callback = requires(Fn fn, Args... args) {
  requires std::invocable<Fn, Args...>;
  { std::invoke(fn, args...) } -> std::convertible_to<std::error_code>;
};

template<typename Fn, typename T>
concept setter = std::invocable<Fn, T>;
#endif // __cpp_concepts


template<typename... Factories>
struct extract_reader<xdr<reader<Factories...>>> {
  using type = reader<Factories...>;

  auto operator()(xdr<reader<Factories...>>&& x) noexcept -> reader<Factories...>&&;
  auto operator()(const xdr<reader<Factories...>>& x) noexcept -> const reader<Factories...>&;
};


class buffer_factory {
  public:
  using known_size = std::false_type;

  explicit buffer_factory(asio::mutable_buffer buf) noexcept;

  auto buffers() const -> std::array<asio::mutable_buffer, 1>;

  private:
  asio::mutable_buffer buf_;
};

template<std::size_t Bytes>
class constant_size_buffer_factory {
  public:
  static constexpr std::size_t bytes = Bytes;
  using known_size = std::true_type;

  explicit constant_size_buffer_factory(asio::mutable_buffer buf);

  auto buffers() const -> std::array<asio::mutable_buffer, 1>;

  private:
  void* buf_;
};


template<typename Fn>
class post_processor {
  public:
  static constexpr std::size_t bytes = 0;
  using known_size = std::true_type;

  explicit post_processor(Fn&& fn) noexcept(std::is_nothrow_move_constructible_v<Fn>);
  explicit post_processor(const Fn& fn) noexcept(std::is_nothrow_copy_constructible_v<Fn>);

  auto callback() const & -> const Fn&;
  auto callback() && -> Fn&&;

  private:
  Fn fn_;
};

template<typename Fn>
auto make_post_processor(Fn&& fn) -> post_processor<std::decay_t<Fn>>;

template<typename Fn>
#if __cpp_concepts >= 201907L
requires callback<std::decay_t<Fn>>
#endif
auto make_noarg_post_processor(Fn&& fn);

template<typename IntType>
#if __cpp_concepts >= 201907L
requires std::integral<IntType>
#endif
auto maybe_big_endian_to_native_post_processor_tpl(IntType& v);


template<std::size_t TupleIndex, typename ExpectedType, typename Fn>
#if __cpp_concepts >= 201907L
requires callback<Fn, ExpectedType>
#endif
auto make_temporary_post_processor_(Fn&& fn);

struct noop_callback {
  template<typename T>
  auto operator()(const T& v) const -> std::error_code;
};

template<typename T, typename Fn>
#if __cpp_concepts >= 201907L
requires readable<T> && callback<Fn, T>
#endif
class temporary {
  public:
  using temporary_type = T;

  private:
  using reader_type = typename extract_reader<std::remove_cvref_t<decltype(std::declval<xdr<reader<>>>() & std::declval<temporary_type&>())>>::type;

  public:
  explicit temporary(Fn&& fn, temporary_type initial) noexcept(std::is_nothrow_move_constructible_v<Fn>);
  explicit temporary(const Fn& fn, temporary_type initial) noexcept(std::is_nothrow_copy_constructible_v<Fn>);

  auto initial() const & -> const temporary_type&;
  auto initial() && -> temporary_type&&;
  auto fn() && -> Fn&& { return std::move(fn_); }

  static constexpr std::size_t bytes = reader_type::bytes.value_or(std::size_t(0));
  using known_size = typename reader_type::known_size_at_compile_time;

  private:
  temporary_type initial_;
  Fn fn_;
};

template<typename T, typename Fn>
#if __cpp_concepts >= 201907L
requires readable<T> && callback<std::decay_t<Fn>, T>
#endif
auto make_temporary(Fn&& fn, T v = T()) -> temporary<T, std::decay_t<Fn>>;


auto buffers_1_(const buffer_factory& bf) -> std::array<asio::mutable_buffer, 1>;
template<std::size_t Bytes> auto buffers_1_(const constant_size_buffer_factory<Bytes>& bf) -> std::array<asio::mutable_buffer, 1>;
template<typename Fn> auto buffers_1_([[maybe_unused]] const post_processor<Fn>& pp) -> std::array<asio::mutable_buffer, 0>;
template<typename... Factories, std::size_t... Idx> auto buffers_(const std::tuple<Factories...>& factories, [[maybe_unused]] std::index_sequence<Idx...> seq);


auto callbacks_n_();
template<typename... Tail> auto callbacks_n_([[maybe_unused]] const buffer_factory& bf, Tail&&... tail);
template<std::size_t Bytes, typename... Tail> auto callbacks_n_([[maybe_unused]] const constant_size_buffer_factory<Bytes>& bf, Tail&&... tail);
template<typename Fn, typename... Tail> auto callbacks_n_(const post_processor<Fn>& pp, Tail&&... tail);
template<typename... Factories, std::size_t... Idx> auto callbacks_(const std::tuple<Factories...>& factories, [[maybe_unused]] std::index_sequence<Idx...> seq);


template<typename... T>
class temporaries_factory {
  public:
  // We add this to the tuple, to make the tuple immovable.
  // That way, the compiler informs us if we introduce a bug. :)
  class prevent_copy_and_move {
    public:
    constexpr prevent_copy_and_move() noexcept = default;
    prevent_copy_and_move(const prevent_copy_and_move&) = delete;
    prevent_copy_and_move(prevent_copy_and_move&&) = delete;
    prevent_copy_and_move& operator=(const prevent_copy_and_move&) = delete;
    prevent_copy_and_move& operator=(prevent_copy_and_move&&) = delete;
  };

  using type = std::tuple<T..., prevent_copy_and_move>;

  constexpr temporaries_factory() = default;

  template<typename Alloc> auto operator()(Alloc&& alloc) const -> std::shared_ptr<type>;
  auto operator()() const -> std::shared_ptr<type>;
};

// When there are no temporaries requires, we want to elide the allocation
// of the temporaries tuple.
// So we use this instead.
// It's easier to keep the algorithm in terms of a (constrained) shared-ptr,
// than to write two implementations, where one doesn't allocate a pointer.
template<>
class temporaries_factory<> {
  public:
  using type = std::tuple<>;

  constexpr temporaries_factory() = default;

  // This is not really a pointer, but fulfills enough of the temporaries
  // logic that it'll be acceptable as a stand-in for
  // std::shared_ptr<std::tuple<>>.
  class pretend_pointer {
    static_assert(std::is_empty_v<std::tuple<>>);

    public:
    auto operator*() const noexcept -> std::tuple<>&;
    auto operator->() const noexcept -> std::tuple<>*;

    private:
    mutable std::tuple<> t_;
  };

  template<typename Alloc> auto operator()(Alloc&& alloc) const -> pretend_pointer;
  auto operator()() const -> pretend_pointer;
};

template<typename... X, typename... Y>
constexpr auto operator+(const temporaries_factory<X...>& x, const temporaries_factory<Y...>& y) -> temporaries_factory<X..., Y...>;


template<typename T, typename Fn, typename Temporary>
#if __cpp_concepts >= 201907L
// requires std::invocable<UnspecifiedStream, UnspecifiedCallback>
#endif
struct decoder {
  static constexpr std::size_t bytes = 0;
  using known_size = std::false_type;

  explicit decoder(const Fn& fn, T initial = T());
  explicit decoder(Fn&& fn, T initial = T());

  Fn fn;
  T initial;
};

template<typename Fn, typename Temporary>
#if __cpp_concepts >= 201907L
// requires std::invocable<UnspecifiedStream, UnspecifiedCallback>
#endif
struct decoder<void, Fn, Temporary> {
  static constexpr std::size_t bytes = 0;
  using known_size = std::false_type;

  explicit decoder(const Fn& fn);
  explicit decoder(Fn&& fn);

  Fn fn;
};

template<typename T, typename Temporary = void, typename Fn>
#if __cpp_concepts >= 201907L
// requires std::invocable<UnspecifiedStream, UnspecifiedCallback>
#endif
auto make_decoder(Fn&& fn) -> decoder<T, std::decay_t<Fn>, Temporary>;

template<typename Temporary = void, typename T, typename Fn>
#if __cpp_concepts >= 201907L
// requires std::invocable<UnspecifiedStream, UnspecifiedCallback>
#endif
auto make_decoder(Fn&& fn, T&& initial) -> decoder<std::decay_t<T>, std::decay_t<Fn>, Temporary>;


template<typename DoneCb, bool HasTemporaries>
#if __cpp_concepts >= 201907L
requires std::invocable<DoneCb, std::error_code>
#endif
class completion_fn_wrapper_ {
  public:
  explicit completion_fn_wrapper_(DoneCb&& done_cb, std::shared_ptr<void> temporaries);
  explicit completion_fn_wrapper_(const DoneCb& done_cb, std::shared_ptr<void> temporaries);

  template<typename Stream, typename... T>
  void operator()([[maybe_unused]] Stream& stream, std::tuple<T...>& temporaries, std::error_code ec);

  private:
  DoneCb done_cb_;
  [[maybe_unused]] std::shared_ptr<void> temporaries_;
};

template<typename DoneCb>
#if __cpp_concepts >= 201907L
requires std::invocable<DoneCb, std::error_code>
#endif
class completion_fn_wrapper_<DoneCb, false> {
  public:
  template<typename Temporaries>
  explicit completion_fn_wrapper_(DoneCb&& done_cb, const Temporaries& temporaries);
  template<typename Temporaries>
  explicit completion_fn_wrapper_(const DoneCb& done_cb, const Temporaries& temporaries);

  template<typename Stream>
  void operator()([[maybe_unused]] Stream& stream, std::tuple<>& temporaries, std::error_code ec);

  private:
  DoneCb done_cb_;
};


template<typename Dfn, typename Nested, typename Alloc = std::allocator<std::byte>>
class decoder_callback {
  private:
  using reader_type = typename Nested::reader_type;
  using completion_fn = typename Nested::completion_fn;

  public:
  using allocator_type = Alloc;

  decoder_callback(Dfn&& dfn, std::tuple<reader_type, completion_fn>&& rc, allocator_type alloc = allocator_type());

  auto get_allocator() const -> allocator_type;

  template<typename Stream, typename... T>
  void operator()(Stream& stream, std::tuple<T...>& temporaries, std::error_code ec);

  private:
  template<typename Stream, typename... T>
  void continue_(Stream& stream, std::tuple<T...>& temporaries, std::error_code ec);

  allocator_type alloc;
  Dfn dfn_;
  reader_type r_;
  completion_fn cfn_;
};


template<typename DoneCb, std::size_t AccumulatedTupleSize, typename... Tail> struct resolve_n_;

template<typename DoneCb, std::size_t AccumulatedTupleSize>
struct resolve_n_<DoneCb, AccumulatedTupleSize> {
  using temporaries = temporaries_factory<>;
  using reader_type = reader<>;
  using completion_fn = completion_fn_wrapper_<DoneCb, AccumulatedTupleSize != 0u>;

  template<typename TTPointer>
  auto operator()(DoneCb&& done_cb, [[maybe_unused]] const TTPointer& temporaries_tuple) const -> std::tuple<reader_type, completion_fn>;
};

template<typename DoneCb, std::size_t AccumulatedTupleSize, typename... Tail>
struct resolve_n_<DoneCb, AccumulatedTupleSize, buffer_factory, Tail...> {
  using nested_t = resolve_n_<DoneCb, AccumulatedTupleSize, Tail...>;

  using temporaries = typename nested_t::temporaries;
  using reader_type = std::remove_cvref_t<decltype(std::declval<reader<buffer_factory>>() + std::declval<typename nested_t::reader_type>())>;
  using completion_fn = typename nested_t::completion_fn;

  template<typename TTPointer>
  auto operator()(DoneCb&& done_cb, const TTPointer& temporaries_tuple, buffer_factory&& bf, Tail&&... tail) -> std::tuple<reader_type, completion_fn>;
};

template<typename DoneCb, std::size_t AccumulatedTupleSize, std::size_t Bytes, typename... Tail>
struct resolve_n_<DoneCb, AccumulatedTupleSize, constant_size_buffer_factory<Bytes>, Tail...> {
  using nested_t = resolve_n_<DoneCb, AccumulatedTupleSize, Tail...>;

  using temporaries = typename nested_t::temporaries;
  using reader_type = std::remove_cvref_t<decltype(std::declval<reader<constant_size_buffer_factory<Bytes>>>() + std::declval<typename nested_t::reader_type>())>;
  using completion_fn = typename nested_t::completion_fn;

  template<typename TTPointer>
  auto operator()(DoneCb&& done_cb, const TTPointer& temporaries_tuple, constant_size_buffer_factory<Bytes>&& bf, Tail&&... tail) -> std::tuple<reader_type, completion_fn>;
};

template<typename DoneCb, std::size_t AccumulatedTupleSize, typename Fn, typename... Tail>
struct resolve_n_<DoneCb, AccumulatedTupleSize, post_processor<Fn>, Tail...> {
  using nested_t = resolve_n_<DoneCb, AccumulatedTupleSize, Tail...>;

  using temporaries = typename nested_t::temporaries;
  using reader_type = std::remove_cvref_t<decltype(std::declval<reader<post_processor<Fn>>>() + std::declval<typename nested_t::reader_type>())>;
  using completion_fn = typename nested_t::completion_fn;

  template<typename TTPointer>
  auto operator()(DoneCb&& done_cb, const TTPointer& temporaries_tuple, post_processor<Fn>&& pp, Tail&&... tail) -> std::tuple<reader_type, completion_fn>;
};

template<typename DoneCb, std::size_t AccumulatedTupleSize, typename Tpl, typename... Tail> struct prepend_tuple_to_resolve_n_;
template<typename DoneCb, std::size_t AccumulatedTupleSize, typename... TplElements, typename... Tail>
struct prepend_tuple_to_resolve_n_<DoneCb, AccumulatedTupleSize, std::tuple<TplElements...>, Tail...> {
  using type = resolve_n_<DoneCb, AccumulatedTupleSize, TplElements..., Tail...>;
};

template<typename DoneCb, std::size_t AccumulatedTupleSize, typename T, typename Fn, typename... Tail>
struct resolve_n_<DoneCb, AccumulatedTupleSize, temporary<T, Fn>, Tail...> {
  private:
  using temporary_type = typename temporary<T, Fn>::temporary_type;
  using post_processor_type = std::remove_cvref_t<decltype(make_temporary_post_processor_<AccumulatedTupleSize, temporary_type>(std::declval<Fn>()))>;
  using temporary_reader_type = typename extract_reader<std::remove_cvref_t<decltype(std::declval<xdr<reader<>>>() & std::declval<temporary_type&>())>>::type;
  using temporary_factories_tuple = typename temporary_reader_type::factories_tuple;
  using nested_t = typename prepend_tuple_to_resolve_n_<DoneCb, AccumulatedTupleSize + 1u, temporary_factories_tuple, post_processor_type, Tail...>::type;

  public:
  using temporaries = std::remove_cvref_t<decltype(std::declval<temporaries_factory<temporary_type>>() + std::declval<typename nested_t::temporaries>())>;
  using reader_type = typename nested_t::reader_type;
  using completion_fn = typename nested_t::completion_fn;

  template<typename... TT>
  auto operator()(DoneCb&& done_cb, const std::shared_ptr<std::tuple<TT...>>& temporaries_tuple, temporary<T, Fn>&& tmp, Tail&&... tail) -> std::tuple<reader_type, completion_fn>;
};

template<std::size_t TupleOffset, typename Decoder> class decoder_resolve_helper_;

template<std::size_t TupleOffset, typename T, typename Fn, typename Temporary>
class decoder_resolve_helper_<TupleOffset, decoder<T, Fn, Temporary>> {
  public:
  using reader_type = typename extract_reader<std::remove_cvref_t<decltype(std::declval<xdr<reader<>>>() & std::declval<T&>())>>::type;

  explicit decoder_resolve_helper_(decoder<T, Fn, Temporary>&& d)
  : fn(std::move(d.fn))
  {}

  template<typename Stream, typename... TT, typename Callback>
  auto operator()(Stream& s, std::tuple<TT...>& temporaries, Callback&& callback) -> void {
    static_assert(std::is_same_v<T, std::tuple_element_t<TupleOffset, std::tuple<TT...>>>);
    static_assert(std::is_same_v<Temporary, std::tuple_element_t<TupleOffset + 1u, std::tuple<TT...>>>);

    std::invoke(fn, std::get<TupleOffset>(temporaries), s, std::forward<Callback>(callback), std::get<TupleOffset + 1u>(temporaries));
  }

  private:
  Fn fn;
};

template<std::size_t TupleOffset, typename Fn, typename Temporary>
class decoder_resolve_helper_<TupleOffset, decoder<void, Fn, Temporary>> {
  public:
  using reader_type = reader<>;

  explicit decoder_resolve_helper_(decoder<void, Fn, Temporary>&& d)
  : fn(std::move(d.fn))
  {}

  template<typename Stream, typename... TT, typename Callback>
  auto operator()(Stream& s, std::tuple<TT...>& temporaries, Callback&& callback) -> void {
    static_assert(std::is_same_v<Temporary, std::tuple_element_t<TupleOffset, std::tuple<TT...>>>);

    std::invoke(fn, s, std::forward<Callback>(callback), std::get<TupleOffset>(temporaries));
  }

  private:
  Fn fn;
};

template<std::size_t TupleOffset, typename T, typename Fn>
class decoder_resolve_helper_<TupleOffset, decoder<T, Fn, void>> {
  public:
  using reader_type = typename extract_reader<std::remove_cvref_t<decltype(std::declval<xdr<reader<>>>() & std::declval<T&>())>>::type;

  explicit decoder_resolve_helper_(decoder<T, Fn, void>&& d)
  : fn(std::move(d.fn))
  {}

  template<typename Stream, typename... TT, typename Callback>
  auto operator()(Stream& s, std::tuple<TT...>& temporaries, Callback&& callback) -> void {
    static_assert(std::is_same_v<T, std::tuple_element_t<TupleOffset, std::tuple<TT...>>>);

    std::invoke(fn, std::get<TupleOffset>(temporaries), s, std::forward<Callback>(callback));
  }

  private:
  Fn fn;
};

template<std::size_t TupleOffset, typename Fn>
class decoder_resolve_helper_<TupleOffset, decoder<void, Fn, void>> {
  public:
  using reader_type = reader<>;

  explicit decoder_resolve_helper_(decoder<void, Fn, void>&& d)
  : fn(std::move(d.fn))
  {}

  template<typename Stream, typename... TT, typename Callback>
  auto operator()(Stream& s, std::tuple<TT...>& temporaries, Callback&& callback) -> void {
    std::invoke(fn, s, std::forward<Callback>(callback));
  }

  private:
  Fn fn;
};

struct direct_completion {
  using known_size = std::false_type;
};

template<typename DoneCb, std::size_t AccumulatedTupleSize, typename... Tail>
struct resolve_n_<DoneCb, AccumulatedTupleSize, direct_completion, Tail...> {
  static_assert(sizeof...(Tail) == 0u);

  using temporaries = temporaries_factory<>;
  using reader_type = reader<>;
  using completion_fn = DoneCb; // Don't wrap this!

  template<typename TTPointer>
  auto operator()(DoneCb&& done_cb, [[maybe_unused]] const TTPointer& temporaries_tuple, direct_completion&& d, [[maybe_unused]] Tail&&... tail) -> std::tuple<reader_type, completion_fn>;
};

template<typename DoneCb, std::size_t AccumulatedTupleSize, typename T, typename Fn, typename Temporary, typename... Tail>
struct resolve_n_<DoneCb, AccumulatedTupleSize, decoder<T, Fn, Temporary>, Tail...> {
  static constexpr std::size_t tail_accumulated_tuple_size = AccumulatedTupleSize + (std::is_void_v<T> ? 0u : 1u) + (std::is_void_v<Temporary> ? 0u : 1u);
  using helper = decoder_resolve_helper_<AccumulatedTupleSize, decoder<T, Fn, Temporary>>;
  using nested_t = resolve_n_<DoneCb, tail_accumulated_tuple_size, Tail...>;
  using completion_fn = decoder_callback<helper, nested_t>;
  using temporary_resolve_n = typename prepend_tuple_to_resolve_n_<completion_fn, tail_accumulated_tuple_size + std::tuple_size_v<typename nested_t::temporaries::type>, typename helper::reader_type::factories_tuple, direct_completion>::type;

  using temporaries = std::remove_cvref_t<decltype(
      std::declval<std::conditional_t<std::is_void_v<T>, temporaries_factory<>, temporaries_factory<T>>>()
      +
      std::declval<std::conditional_t<std::is_void_v<Temporary>, temporaries_factory<>, temporaries_factory<Temporary>>>()
      +
      std::declval<typename nested_t::temporaries>()
      +
      std::declval<typename temporary_resolve_n::temporaries>())>;
  using reader_type = typename temporary_resolve_n::reader_type;

  template<typename TTPointer>
  auto operator()(DoneCb&& done_cb, const TTPointer& temporaries_tuple, decoder<T, Fn, Temporary>&& d, Tail&&... tail) -> std::tuple<reader_type, completion_fn>;
};

template<typename Alloc, typename DoneCb, typename... Factories, std::size_t... Idx, std::size_t StartIdx>
auto resolve_(
    [[maybe_unused]] std::allocator_arg_t use_alloc, Alloc&& alloc, DoneCb&& done_cb,
    std::tuple<Factories...>&& factories,
    [[maybe_unused]] std::index_sequence<Idx...> seq,
    std::integral_constant<std::size_t, StartIdx> start_idx);


template<typename... Factories>
auto make_reader(std::tuple<Factories...>&& factories) -> reader<Factories...>;

template<typename... X, typename... Y>
auto operator+(reader<X...>&& x, reader<Y...>&& y) -> reader<X..., Y...>;

template<typename... Factories>
class reader {
  public:
  using factories_tuple = std::tuple<Factories...>;
  // std::true_type if encoded-bytes is known at compile-time, std::false_type otherwise.
  using known_size_at_compile_time = std::conjunction<typename Factories::known_size...>;
  // number of encoded-bytes, if known in advance. If the number of bytes depends on the input, then an empty optional.
  static constexpr auto bytes() noexcept -> std::optional<std::size_t>;
  static constexpr bool is_reader = true;
  static constexpr bool is_writer = false;

  template<bool IsZeroElement = (sizeof...(Factories) == 0)>
  explicit reader(std::enable_if_t<IsZeroElement, int> = 0) {}

  explicit reader(factories_tuple&& factories) noexcept(std::is_nothrow_move_constructible_v<factories_tuple>);

  template<typename Fn>
  auto post_processor(Fn&& fn) &&;

  auto get_factories() && -> factories_tuple&&;
  auto get_factories() const & -> const factories_tuple&;

  private:
  factories_tuple factories_;
};

template<typename Alloc, typename DoneCb, typename... Factories, std::size_t StartIdx = 0>
auto resolve(std::allocator_arg_t aa, Alloc&& alloc, DoneCb&& done_cb, reader<Factories...>&& r, std::integral_constant<std::size_t, StartIdx> start_idx = {});

template<typename... Factories>
auto buffers(const reader<Factories...>& r);

template<typename... Factories>
auto callbacks(const reader<Factories...>& r);


} /* namespace earnest::detail::xdr_reader */


namespace earnest::detail::xdr_writer {


template<typename... Factories> class writer;


#if __cpp_concepts >= 201907L
template<typename T> struct is_an_xdr_writer_ : std::false_type {};
template<typename... T> struct is_an_xdr_writer_<xdr<writer<T...>>> : std::true_type {};

template<typename T>
concept xdr_writer = is_an_xdr_writer_<T>::value;

template<typename T>
concept writeable = requires(xdr<writer<>> w, T v) {
  { std::move(w) & v } -> xdr_writer;
};

template<typename Fn, typename... Args>
concept callback = requires(Fn fn, Args... args) {
  requires std::invocable<Fn, Args...>;
  { std::invoke(fn, args...) } -> std::convertible_to<std::error_code>;
};

template<typename Fn, typename T>
concept getter = requires(Fn fn) {
  requires std::invocable<Fn>;
  { std::invoke(fn) } -> std::convertible_to<T>;
};
#endif // __cpp_concepts


template<typename... Factories>
struct extract_writer<xdr<writer<Factories...>>> {
  using type = writer<Factories...>;

  auto operator()(xdr<writer<Factories...>>&& x) noexcept -> writer<Factories...>&&;
  auto operator()(const xdr<writer<Factories...>>& x) noexcept -> const writer<Factories...>&;
};


class buffer_factory {
  public:
  using known_size = std::false_type;

  explicit buffer_factory(asio::const_buffer buf) noexcept;

  auto buffers() const -> std::array<asio::const_buffer, 1>;

  private:
  asio::const_buffer buf_;
};


template<std::size_t Bytes>
class constant_size_buffer_factory {
  public:
  static constexpr std::size_t bytes = Bytes;
  using known_size = std::true_type;

  explicit constant_size_buffer_factory(asio::const_buffer buf);

  auto buffers() const -> std::array<asio::const_buffer, 1>;

  private:
  const void* buf_;
};


template<typename Fn>
class pre_processor {
  public:
  static constexpr std::size_t bytes = 0;
  using known_size = std::true_type;

  explicit pre_processor(Fn&& fn) noexcept(std::is_nothrow_move_constructible_v<Fn>);
  explicit pre_processor(const Fn& fn) noexcept(std::is_nothrow_copy_constructible_v<Fn>);

  auto callback() const & -> const Fn&;
  auto callback() && -> Fn&&;

  private:
  Fn fn_;
};

template<typename Fn>
auto make_pre_processor(Fn&& fn) -> pre_processor<std::decay_t<Fn>>;

template<typename Fn>
#if __cpp_concepts >= 201907L
requires callback<std::decay_t<Fn>>
#endif
auto make_noarg_pre_processor(Fn&& fn);


template<std::size_t TupleIndex, typename ExpectedType, typename Fn>
#if __cpp_concepts >= 201907L
requires callback<Fn, ExpectedType&>
#endif
auto make_temporary_pre_processor_(Fn&& fn);

template<typename T, typename Fn>
#if __cpp_concepts >= 201907L
requires writeable<T> && callback<Fn, T&>
#endif
class temporary {
  public:
  using temporary_type = T;

  private:
  using writer_type = typename extract_writer<std::remove_cvref_t<decltype(std::declval<xdr<writer<>>>() & std::declval<const temporary_type&>())>>::type;

  public:
  explicit temporary(Fn&& fn, temporary_type initial);
  explicit temporary(const Fn& fn, temporary_type initial);

  auto initial() const & -> const temporary_type&;
  auto initial() && -> temporary_type&&;
  auto fn() && -> Fn&& { return std::move(fn_); }

  static constexpr std::size_t bytes = writer_type::bytes.value_or(std::size_t(0));
  using known_size = typename writer_type::known_size_at_compile_time;

  private:
  temporary_type initial_;
  Fn fn_;
};

template<typename T, typename Fn>
#if __cpp_concepts >= 201907L
requires writeable<T> && callback<std::decay_t<Fn>, T&>
#endif
auto make_temporary(Fn&& fn, T v = T()) -> temporary<T, std::decay_t<Fn>>;


auto buffers_1_(const buffer_factory& bf) -> std::array<asio::const_buffer, 1>;
template<std::size_t Bytes> auto buffers_1_(const constant_size_buffer_factory<Bytes>& bf) -> std::array<asio::const_buffer, 1>;
template<typename Fn> auto buffers_1_([[maybe_unused]] const pre_processor<Fn>& pp) -> std::array<asio::const_buffer, 0>;
template<typename... Factories, std::size_t... Idx> auto buffers_(const std::tuple<Factories...>& factories, [[maybe_unused]] std::index_sequence<Idx...> seq);


auto callbacks_n_();
template<typename... Tail> auto callbacks_n_(const buffer_factory& bf, Tail&&... tail);
template<std::size_t Bytes, typename... Tail> auto callbacks_n_(const constant_size_buffer_factory<Bytes>& bf, Tail&&... tail);
template<typename Fn, typename... Tail> auto callbacks_n_(const pre_processor<Fn>& pp, Tail&&... tail);
template<typename... Factories, std::size_t... Idx> auto callbacks_(const std::tuple<Factories...>& factories, std::index_sequence<Idx...> seq);


using xdr_reader::noop_callback;
using xdr_reader::temporaries_factory;
using xdr_reader::completion_fn_wrapper_;


template<typename Fn, typename Temporary>
#if __cpp_concepts >= 201907L
// requires std::invocable<UnspecifiedStream, UnspecifiedCallback>
#endif
struct encoder {
  static constexpr std::size_t bytes = 0;
  using known_size = std::false_type;

  explicit encoder(const Fn& fn);
  explicit encoder(Fn&& fn);

  Fn fn;
};

template<typename Temporary = void, typename Fn>
#if __cpp_concepts >= 201907L
// requires std::invocable<UnspecifiedStream, UnspecifiedCallback>
#endif
auto make_encoder(Fn&& fn) -> encoder<std::decay_t<Fn>, Temporary>;


template<typename Dfn, typename Nested, typename Alloc = std::allocator<std::byte>>
class encoder_callback {
  private:
  using writer_type = typename Nested::writer_type;
  using completion_fn = typename Nested::completion_fn;

  public:
  using allocator_type = Alloc;

  encoder_callback(Dfn&& dfn, std::tuple<writer_type, completion_fn>&& rc, allocator_type alloc = allocator_type());

  auto get_allocator() const -> allocator_type;

  template<typename Stream, typename... T>
  void operator()(Stream& stream, std::tuple<T...>& temporaries, std::error_code ec);

  private:
  template<typename Stream, typename... T>
  void continue_(Stream& stream, std::tuple<T...>& temporaries, std::error_code ec);

  allocator_type alloc;
  Dfn dfn_;
  writer_type r_;
  completion_fn cfn_;
};


template<typename DoneCb, std::size_t AccumulatedTupleSize, typename... Tail> struct resolve_n_;

template<typename DoneCb, std::size_t AccumulatedTupleSize>
struct resolve_n_<DoneCb, AccumulatedTupleSize> {
  using temporaries = temporaries_factory<>;
  using writer_type = writer<>;
  using completion_fn = completion_fn_wrapper_<DoneCb, AccumulatedTupleSize != 0u>;

  template<typename TTPointer>
  auto operator()(DoneCb&& done_cb, [[maybe_unused]] const TTPointer& temporaries_tuple) const -> std::tuple<writer_type, completion_fn>;
};

template<typename DoneCb, std::size_t AccumulatedTupleSize, typename... Tail>
struct resolve_n_<DoneCb, AccumulatedTupleSize, buffer_factory, Tail...> {
  using nested_t = resolve_n_<DoneCb, AccumulatedTupleSize, Tail...>;

  using temporaries = typename nested_t::temporaries;
  using writer_type = std::remove_cvref_t<decltype(std::declval<writer<buffer_factory>>() + std::declval<typename nested_t::writer_type>())>;
  using completion_fn = typename nested_t::completion_fn;

  template<typename TTPointer>
  auto operator()(DoneCb&& done_cb, const TTPointer& temporaries_tuple, buffer_factory&& bf, Tail&&... tail) -> std::tuple<writer_type, completion_fn>;
};

template<typename DoneCb, std::size_t AccumulatedTupleSize, std::size_t Bytes, typename... Tail>
struct resolve_n_<DoneCb, AccumulatedTupleSize, constant_size_buffer_factory<Bytes>, Tail...> {
  using nested_t = resolve_n_<DoneCb, AccumulatedTupleSize, Tail...>;

  using temporaries = typename nested_t::temporaries;
  using writer_type = std::remove_cvref_t<decltype(std::declval<writer<constant_size_buffer_factory<Bytes>>>() + std::declval<typename nested_t::writer_type>())>;
  using completion_fn = typename nested_t::completion_fn;

  template<typename TTPointer>
  auto operator()(DoneCb&& done_cb, const TTPointer& temporaries_tuple, constant_size_buffer_factory<Bytes>&& bf, Tail&&... tail) -> std::tuple<writer_type, completion_fn>;
};

template<typename DoneCb, std::size_t AccumulatedTupleSize, typename Fn, typename... Tail>
struct resolve_n_<DoneCb, AccumulatedTupleSize, pre_processor<Fn>, Tail...> {
  using nested_t = resolve_n_<DoneCb, AccumulatedTupleSize, Tail...>;

  using temporaries = typename nested_t::temporaries;
  using writer_type = std::remove_cvref_t<decltype(std::declval<writer<pre_processor<Fn>>>() + std::declval<typename nested_t::writer_type>())>;
  using completion_fn = typename nested_t::completion_fn;

  template<typename TTPointer>
  auto operator()(DoneCb&& done_cb, const TTPointer& temporaries_tuple, pre_processor<Fn>&& pp, Tail&&... tail) -> std::tuple<writer_type, completion_fn>;
};

template<typename DoneCb, std::size_t AccumulatedTupleSize, typename Tpl, typename... Tail> struct prepend_tuple_to_resolve_n_;
template<typename DoneCb, std::size_t AccumulatedTupleSize, typename... TplElements, typename... Tail>
struct prepend_tuple_to_resolve_n_<DoneCb, AccumulatedTupleSize, std::tuple<TplElements...>, Tail...> {
  using type = resolve_n_<DoneCb, AccumulatedTupleSize, TplElements..., Tail...>;
};

template<typename DoneCb, std::size_t AccumulatedTupleSize, typename T, typename Fn, typename... Tail>
struct resolve_n_<DoneCb, AccumulatedTupleSize, temporary<T, Fn>, Tail...> {
  private:
  using temporary_type = typename temporary<T, Fn>::temporary_type;
  using pre_processor_type = std::remove_cvref_t<decltype(make_temporary_pre_processor_<AccumulatedTupleSize, temporary_type>(std::declval<Fn>()))>;
  using temporary_writer_type = typename extract_writer<std::remove_cvref_t<decltype(std::declval<xdr<writer<>>>() & std::declval<temporary_type&>())>>::type;
  using temporary_factories_tuple = typename temporary_writer_type::factories_tuple;
  using nested_t = typename prepend_tuple_to_resolve_n_<DoneCb, AccumulatedTupleSize + 1u,
        std::remove_cvref_t<decltype(std::tuple_cat(std::declval<std::tuple<pre_processor_type>>(), std::declval<temporary_factories_tuple>()))>,
        Tail...>::type;

  public:
  using temporaries = std::remove_cvref_t<decltype(std::declval<temporaries_factory<temporary_type>>() + std::declval<typename nested_t::temporaries>())>;
  using writer_type = typename nested_t::writer_type;
  using completion_fn = typename nested_t::completion_fn;

  template<typename... TT>
  auto operator()(DoneCb&& done_cb, const std::shared_ptr<std::tuple<TT...>>& temporaries_tuple, temporary<T, Fn>&& tmp, Tail&&... tail) -> std::tuple<writer_type, completion_fn>;
};

template<std::size_t TupleOffset, typename Encoder> class encoder_resolve_helper_;

template<std::size_t TupleOffset, typename Fn, typename Temporary>
class encoder_resolve_helper_<TupleOffset, encoder<Fn, Temporary>> {
  public:
  using writer_type = writer<>;

  explicit encoder_resolve_helper_(encoder<Fn, Temporary>&& e)
  : fn(std::move(e.fn))
  {}

  template<typename Stream, typename... TT, typename Callback>
  auto operator()(Stream& s, std::tuple<TT...>& temporaries, Callback&& callback) -> void {
    static_assert(std::is_same_v<Temporary, std::tuple_element_t<TupleOffset, std::tuple<TT...>>>);

    std::invoke(fn, s, std::forward<Callback>(callback), std::get<TupleOffset>(temporaries));
  }

  private:
  Fn fn;
};

template<std::size_t TupleOffset, typename Fn>
class encoder_resolve_helper_<TupleOffset, encoder<Fn, void>> {
  public:
  using writer_type = writer<>;

  explicit encoder_resolve_helper_(encoder<Fn, void>&& e)
  : fn(std::move(e.fn))
  {}

  template<typename Stream, typename... TT, typename Callback>
  auto operator()(Stream& s, std::tuple<TT...>& temporaries, Callback&& callback) -> void {
    std::invoke(fn, s, std::forward<Callback>(callback));
  }

  private:
  Fn fn;
};

struct direct_completion {
  using known_size = std::false_type;
};

template<typename DoneCb, std::size_t AccumulatedTupleSize, typename... Tail>
struct resolve_n_<DoneCb, AccumulatedTupleSize, direct_completion, Tail...> {
  static_assert(sizeof...(Tail) == 0u);

  using temporaries = temporaries_factory<>;
  using writer_type = writer<>;
  using completion_fn = DoneCb; // Don't wrap this!

  template<typename TTPointer>
  auto operator()(DoneCb&& done_cb, [[maybe_unused]] const TTPointer& temporaries_tuple, direct_completion&& d, [[maybe_unused]] Tail&&... tail) -> std::tuple<writer_type, completion_fn>;
};

template<typename DoneCb, std::size_t AccumulatedTupleSize, typename Fn, typename Temporary, typename... Tail>
struct resolve_n_<DoneCb, AccumulatedTupleSize, encoder<Fn, Temporary>, Tail...> {
  static constexpr std::size_t tail_accumulated_tuple_size = AccumulatedTupleSize + (std::is_void_v<Temporary> ? 0u : 1u);
  using helper = encoder_resolve_helper_<AccumulatedTupleSize, encoder<Fn, Temporary>>;
  using nested_t = resolve_n_<DoneCb, tail_accumulated_tuple_size, Tail...>;
  using completion_fn = encoder_callback<helper, nested_t>;
  using temporary_resolve_n = typename prepend_tuple_to_resolve_n_<completion_fn, tail_accumulated_tuple_size + std::tuple_size_v<typename nested_t::temporaries::type>, typename helper::writer_type::factories_tuple, direct_completion>::type;

  using temporaries = std::remove_cvref_t<decltype(
      std::declval<std::conditional_t<std::is_void_v<Temporary>, temporaries_factory<>, temporaries_factory<Temporary>>>()
      +
      std::declval<typename nested_t::temporaries>()
      +
      std::declval<typename temporary_resolve_n::temporaries>())>;
  using writer_type = typename temporary_resolve_n::writer_type;

  template<typename TTPointer>
  auto operator()(DoneCb&& done_cb, const TTPointer& temporaries_tuple, encoder<Fn, Temporary>&& d, Tail&&... tail) -> std::tuple<writer_type, completion_fn>;
};

template<typename Alloc, typename DoneCb, typename... Factories, std::size_t... Idx, std::size_t StartIdx>
auto resolve_(
    std::allocator_arg_t use_alloc, Alloc&& alloc, DoneCb&& done_cb,
    std::tuple<Factories...>&& factories,
    std::index_sequence<Idx...> seq,
    std::integral_constant<std::size_t, StartIdx> start_idx);


template<typename T>
struct network_byte_order_int_ {
  T value;
};

template<typename T>
auto operator&(xdr<writer<>>&& w, const network_byte_order_int_<T>& v);


template<typename... Factories>
class writer {
  public:
  using factories_tuple = std::tuple<Factories...>;
  // std::true_type if encoded-bytes is known at compile-time, std::false_type otherwise.
  using known_size_at_compile_time = std::conjunction<typename Factories::known_size...>;
  // number of encoded-bytes, if known in advance. If the number of bytes depends on the input, then an empty optional.
  static constexpr auto bytes() noexcept -> std::optional<std::size_t>;
  static constexpr bool is_reader = false;
  static constexpr bool is_writer = true;

  template<bool IsZeroElement = (sizeof...(Factories) == 0)>
  explicit writer(std::enable_if_t<IsZeroElement, int> = 0) {}

  explicit writer(factories_tuple&& factories) noexcept(std::is_nothrow_move_constructible_v<factories_tuple>);

  template<typename Fn>
  auto pre_processor(Fn&& fn) &&;

  auto get_factories() const & -> const factories_tuple&;
  auto get_factories() && -> factories_tuple&&;

  private:
  factories_tuple factories_;
};

template<typename... Factories>
auto make_writer(std::tuple<Factories...>&& factories) -> writer<Factories...>;

template<typename... X, typename... Y>
auto operator+(writer<X...>&& x, writer<Y...>&& y) -> writer<X..., Y...>;


template<typename Alloc, typename DoneCb, typename... Factories, std::size_t StartIdx = 0>
inline auto resolve(std::allocator_arg_t aa, Alloc&& alloc, DoneCb&& done_cb, writer<Factories...>&& w, std::integral_constant<std::size_t, StartIdx> start_idx = {});

template<typename... Factories>
auto buffers(const writer<Factories...>& w);

template<typename... Factories>
auto callbacks(const writer<Factories...>& w);


} /* namespace earnest::detail::xdr_writer */


namespace earnest {


template<typename AsioAsyncReadStream, typename... Factories, typename CompletionToken>
auto async_read(
    AsioAsyncReadStream& stream,
    xdr<detail::xdr_reader::reader<Factories...>>&& r,
    CompletionToken&& token);

template<typename AsioAsyncReadStream, typename... Factories>
auto async_read(
    AsioAsyncReadStream& stream,
    xdr<detail::xdr_reader::reader<Factories...>>&& r);

template<typename AsioAsyncWriteStream, typename... Factories, typename CompletionToken>
auto async_write(
    AsioAsyncWriteStream& stream,
    xdr<detail::xdr_writer::writer<Factories...>>&& w,
    CompletionToken&& token);

template<typename AsioAsyncWriteStream, typename... Factories>
auto async_write(
    AsioAsyncWriteStream& stream,
    xdr<detail::xdr_writer::writer<Factories...>>&& r);


template<typename... X>
using xdr_reader = xdr<detail::xdr_reader::reader<X...>>;
template<typename... X>
using xdr_writer = xdr<detail::xdr_writer::writer<X...>>;


} /* namespace earnest */


namespace earnest::detail::xdr_types {


template<typename SimulatedInt, typename ActualInt, typename Arg>
#if __cpp_concepts >= 201907L
requires std::integral<SimulatedInt> && std::integral<ActualInt> && std::integral<Arg>
#endif
auto xdr_convert_int_(ActualInt& v, Arg x) -> std::error_code;


template<typename X>
auto add_raw(xdr<X>&& x) -> xdr<X>;

template<typename... X, typename Y, typename... Tail>
auto add_raw(xdr<xdr_reader::reader<X...>>&& x, Y&& y, Tail&&... tail);
template<typename... X, typename Y, typename... Tail>
auto add_raw(xdr<xdr_writer::writer<X...>>&& x, Y&& y, Tail&&... tail);


// ------------------------------------------------------------------------
// XDR identity type.
// ------------------------------------------------------------------------

struct xdr_identity_t {
  template<typename T>
  auto operator()(const T& v) const noexcept -> const T&;
  template<typename T>
  auto operator()(T& v) const noexcept -> T&;
  template<typename T>
  auto operator()(T&& v) const noexcept -> T&&;
};

inline constexpr xdr_identity_t xdr_identity;


// ------------------------------------------------------------------------
// XDR for ints.
// ------------------------------------------------------------------------

template<typename SimulatedInt, typename ActualInt, typename T>
#if __cpp_concepts >= 201907L
requires std::integral<std::remove_const_t<T>> &&
    (
      (std::signed_integral<SimulatedInt> && std::signed_integral<ActualInt>)
      ||
      (std::unsigned_integral<SimulatedInt> && std::unsigned_integral<ActualInt>)
    )
#endif
class xdr_integral_ref {
  public:
  explicit xdr_integral_ref(T& ref) noexcept;

  T& ref;
};


template<typename X, typename SimulatedInt, typename ActualInt, typename T>
auto operator&(xdr<X>&& x, xdr_integral_ref<SimulatedInt, ActualInt, T> v);


struct xdr_uint8_t {
  template<typename T>
#if __cpp_concepts >= 201907L
  requires std::integral<T>
#endif
  auto operator()(T& ref) const -> std::enable_if_t<std::is_integral_v<std::remove_const_t<T>>, xdr_integral_ref<std::uint8_t, std::uint32_t, T>>;
};

struct xdr_uint16_t {
  template<typename T>
#if __cpp_concepts >= 201907L
  requires std::integral<T>
#endif
  auto operator()(T& ref) const -> std::enable_if_t<std::is_integral_v<std::remove_const_t<T>>, xdr_integral_ref<std::uint16_t, std::uint32_t, T>>;
};

struct xdr_uint32_t {
  template<typename T>
#if __cpp_concepts >= 201907L
  requires std::integral<T>
#endif
  auto operator()(T& ref) const -> std::enable_if_t<std::is_integral_v<std::remove_const_t<T>>, xdr_integral_ref<std::uint32_t, std::uint32_t, T>>;
};

struct xdr_uint64_t {
  template<typename T>
#if __cpp_concepts >= 201907L
  requires std::integral<T>
#endif
  auto operator()(T& ref) const -> std::enable_if_t<std::is_integral_v<std::remove_const_t<T>>, xdr_integral_ref<std::uint64_t, std::uint64_t, T>>;
};


struct xdr_int8_t {
  template<typename T>
#if __cpp_concepts >= 201907L
  requires std::integral<T>
#endif
  auto operator()(T& ref) const -> std::enable_if_t<std::is_integral_v<std::remove_const_t<T>>, xdr_integral_ref<std::int8_t, std::int32_t, T>>;
};

struct xdr_int16_t {
  template<typename T>
#if __cpp_concepts >= 201907L
  requires std::integral<T>
#endif
  auto operator()(T& ref) const -> std::enable_if_t<std::is_integral_v<std::remove_const_t<T>>, xdr_integral_ref<std::int16_t, std::int32_t, T>>;
};

struct xdr_int32_t {
  template<typename T>
#if __cpp_concepts >= 201907L
  requires std::integral<T>
#endif
  auto operator()(T& ref) const -> std::enable_if_t<std::is_integral_v<std::remove_const_t<T>>, xdr_integral_ref<std::int32_t, std::int32_t, T>>;
};

struct xdr_int64_t {
  template<typename T>
#if __cpp_concepts >= 201907L
  requires std::integral<T>
#endif
  auto operator()(T& ref) const -> std::enable_if_t<std::is_integral_v<std::remove_const_t<T>>, xdr_integral_ref<std::int64_t, std::int64_t, T>>;
};


inline constexpr xdr_uint8_t xdr_uint8;
inline constexpr xdr_uint16_t xdr_uint16;
inline constexpr xdr_uint32_t xdr_uint32;
inline constexpr xdr_uint64_t xdr_uint64;

inline constexpr xdr_int8_t xdr_int8;
inline constexpr xdr_int16_t xdr_int16;
inline constexpr xdr_int32_t xdr_int32;
inline constexpr xdr_int64_t xdr_int64;


template<typename T> struct int_spec_selector_;
template<> struct int_spec_selector_<std::uint32_t> {
  using type = xdr_uint32_t;
  static constexpr auto value() -> type { return xdr_uint32; }
};
template<> struct int_spec_selector_<std::int32_t> {
  using type = xdr_int32_t;
  static constexpr auto value() -> type { return xdr_int32; }
};
template<> struct int_spec_selector_<std::uint64_t> {
  using type = xdr_uint64_t;
  static constexpr auto value() -> type { return xdr_uint64; }
};
template<> struct int_spec_selector_<std::int64_t> {
  using type = xdr_int64_t;
  static constexpr auto value() -> type { return xdr_int64; }
};


// ------------------------------------------------------------------------
// XDR for getter/setter.
// ------------------------------------------------------------------------

template<typename T, typename Spec = xdr_identity_t>
struct temporary_with_spec {
  temporary_with_spec() = default;
  explicit temporary_with_spec(Spec&& spec);
  explicit temporary_with_spec(const Spec& spec);
  temporary_with_spec(Spec&& spec, const T& value);
  temporary_with_spec(const Spec& spec, const T& value);

  Spec spec;
  T value;
};

template<typename Fn, typename T, typename Spec>
#if __cpp_concepts >= 201907L
requires xdr_writer::getter<Fn, T> || xdr_reader::setter<Fn, T>
#endif
struct getset_impl {
  explicit constexpr getset_impl(Fn fn, Spec spec);

  Fn fn;
  Spec spec;
};

template<typename Fn, typename T>
#if __cpp_concepts >= 201907L
requires xdr_writer::getter<Fn, T> || xdr_reader::setter<Fn, T>
#endif
struct getset_impl<Fn, T, xdr_identity_t> {
  explicit constexpr getset_impl(Fn fn, xdr_identity_t spec);
  template<typename OtherSpec>
  constexpr auto as(OtherSpec&& other_spec) const & -> getset_impl<Fn, T, std::decay_t<OtherSpec>>;
  template<typename OtherSpec>
  constexpr auto as(OtherSpec&& other_spec) && -> getset_impl<Fn, T, std::decay_t<OtherSpec>>;

  Fn fn;
  xdr_identity_t spec;
};


template<typename... X, typename T, typename Spec>
auto operator&(xdr<xdr_reader::reader<X...>>&& x, temporary_with_spec<T, Spec>& tws);

template<typename... X, typename T, typename Spec>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, const temporary_with_spec<T, Spec>& tws);


template<typename... X, typename Fn, typename T, typename Spec>
auto operator&(xdr<xdr_reader::reader<X...>>&& x, getset_impl<Fn, T, Spec> gs);

template<typename... X, typename Fn, typename T, typename Spec>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, getset_impl<Fn, T, Spec> gs);


template<typename T, typename Spec = xdr_identity_t>
struct xdr_getset_t {
  public:
  explicit constexpr xdr_getset_t(Spec spec = Spec());

  template<typename Fn>
  auto operator()(Fn&& fn) const -> getset_impl<std::decay_t<Fn>, T, Spec>;

  private:
  Spec spec;
};

template<typename T>
inline constexpr xdr_getset_t<T> xdr_getset;


// ------------------------------------------------------------------------
// XDR for floating point values.
// ------------------------------------------------------------------------

template<typename T>
#if __cpp_concepts >= 201907L
requires std::floating_point<std::remove_const_t<T>>
#endif
class xdr_float32_ref {
  public:
  explicit xdr_float32_ref(T& ref) noexcept;
  auto ref() const noexcept -> T&;

  private:
  T& ref_;
};

template<typename T>
#if __cpp_concepts >= 201907L
requires std::floating_point<std::remove_const_t<T>>
#endif
class xdr_float64_ref {
  public:
  explicit xdr_float64_ref(T& ref) noexcept;
  auto ref() const noexcept -> T&;

  private:
  T& ref_;
};


template<typename... X, typename T>
#if __cpp_concepts >= 201907L
requires (!std::is_const_v<T>)
#endif
auto operator&(xdr<xdr_reader::reader<X...>>&& x, const xdr_float32_ref<T>& v);

template<typename... X, typename T>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, const xdr_float32_ref<T>& v);

template<typename... X, typename T>
#if __cpp_concepts >= 201907L
requires (!std::is_const_v<T>)
#endif
auto operator&(xdr<xdr_reader::reader<X...>>&& x, const xdr_float64_ref<T>& v);

template<typename... X, typename T>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, const xdr_float64_ref<T>& v);


struct xdr_float32_t {
  template<typename T>
#if __cpp_concepts >= 201907L
  requires std::floating_point<T>
#endif
  auto operator()(T& ref) const -> std::enable_if_t<std::is_floating_point_v<std::remove_const_t<T>>, xdr_float32_ref<T>>;
};

struct xdr_float64_t {
  template<typename T>
#if __cpp_concepts >= 201907L
  requires std::floating_point<T>
#endif
  auto operator()(T& ref) const -> std::enable_if_t<std::is_floating_point_v<std::remove_const_t<T>>, xdr_float64_ref<T>>;
};

inline constexpr xdr_float32_t xdr_float32;
inline constexpr xdr_float64_t xdr_float64;


// ------------------------------------------------------------------------
// Padding IO
// ------------------------------------------------------------------------

class dynamic_padding {
  public:
  explicit dynamic_padding(std::size_t size = 0);

  auto buffer() const noexcept -> asio::const_buffer;
  auto buffer() noexcept -> asio::mutable_buffer;
  auto validate() const noexcept -> std::error_code;

  private:
  std::size_t size_;
  std::array<std::byte, 3> buf_;
};

template<std::size_t N>
class constant_size_padding {
  static_assert(N < 4, "XDR align to 4 byte, so padding must always be less than 4 bytes");

  public:
  constant_size_padding() noexcept;

  auto buffer() const noexcept -> asio::const_buffer;
  auto buffer() noexcept -> asio::mutable_buffer;
  auto validate() const noexcept -> std::error_code;

  private:
  std::array<std::byte, N> buf_;
};


class dynamic_padding_holder {
  public:
  explicit dynamic_padding_holder(std::size_t size);
  auto size() const noexcept -> std::size_t;

  private:
  std::size_t size_;
};

template<std::size_t N>
class constant_size_padding_holder {
  static_assert(N < 4, "XDR align to 4 byte, so padding must always be less than 4 bytes");

  public:
  constexpr constant_size_padding_holder() noexcept = default;
};


template<typename... X>
auto operator&(xdr<xdr_reader::reader<X...>>&& x, dynamic_padding& p);
template<typename... X>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, const dynamic_padding& p);

template<typename... X, std::size_t N>
auto operator&(xdr<xdr_reader::reader<X...>>&& x, constant_size_padding<N>& p);
template<typename... X, std::size_t N>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, const constant_size_padding<N>& p);


template<typename... X>
auto operator&(xdr<xdr_reader::reader<X...>>&& x, const dynamic_padding_holder& h);
template<typename... X>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, const dynamic_padding_holder& h);
template<typename... X, std::size_t N>
auto operator&(xdr<xdr_reader::reader<X...>>&& x, const constant_size_padding_holder<N>& h);
template<typename... X, std::size_t N>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, const constant_size_padding_holder<N>& h);


auto xdr_dynamic_padding(std::size_t size) -> dynamic_padding_holder;

template<std::size_t N>
#if __cpp_concepts >= 201907L
requires (N < 4)
#endif
inline constexpr constant_size_padding_holder<N> xdr_padding = {};


// ------------------------------------------------------------------------
// Raw byte buffers
// ------------------------------------------------------------------------

template<typename Buffer>
class dynamic_buffer {
  public:
  explicit dynamic_buffer(const Buffer& buf);
  auto buffer() const noexcept -> const Buffer&;

  private:
  Buffer buf_;
};

template<std::size_t Bytes>
class constant_size_mutable_buffer {
  public:
  explicit constant_size_mutable_buffer(const asio::mutable_buffer& buf);
  auto buffer() const noexcept -> asio::mutable_buffer;

  private:
  void* addr_;
};

template<std::size_t Bytes>
class constant_size_const_buffer {
  public:
  explicit constant_size_const_buffer(const asio::const_buffer& buf);
  constant_size_const_buffer(const constant_size_mutable_buffer<Bytes>& b) noexcept;
  auto buffer() const noexcept -> asio::const_buffer;

  private:
  const void* addr_;
};


template<typename... X, typename Buffer>
auto operator&(xdr<xdr_reader::reader<X...>>&& x, const dynamic_buffer<Buffer>& b);

template<typename... X, typename Buffer>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, const dynamic_buffer<Buffer>& b);

template<typename... X, std::size_t Bytes>
auto operator&(xdr<xdr_reader::reader<X...>>&& x, const constant_size_mutable_buffer<Bytes>& b);

template<typename... X, std::size_t Bytes>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, const constant_size_const_buffer<Bytes>& b);

template<typename... X, std::size_t Bytes>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, const constant_size_mutable_buffer<Bytes>& b);


struct xdr_dynamic_raw_bytes_t {
  auto operator()(asio::mutable_buffer buf) const -> dynamic_buffer<asio::mutable_buffer>;
  auto operator()(asio::const_buffer buf) const -> dynamic_buffer<asio::const_buffer>;
};

template<std::size_t Bytes>
struct xdr_raw_bytes_t {
  template<typename T, typename Alloc>
#if __cpp_concepts >= 201907L
  requires (std::is_pod_v<T>)
#endif
  auto operator()(std::vector<T, Alloc>& buf) const -> constant_size_mutable_buffer<Bytes>;

  template<typename T, typename Alloc>
#if __cpp_concepts >= 201907L
  requires (std::is_pod_v<T>)
#endif
  auto operator()(const std::vector<T, Alloc>& buf) const -> constant_size_const_buffer<Bytes>;

  template<typename T, typename CharT, typename Alloc>
#if __cpp_concepts >= 201907L
  requires (std::is_pod_v<T>)
#endif
  auto operator()(std::basic_string<T, CharT, Alloc>& buf) const -> constant_size_mutable_buffer<Bytes>;

  template<typename T, typename CharT, typename Alloc>
#if __cpp_concepts >= 201907L
  requires (std::is_pod_v<T>)
#endif
  auto operator()(const std::basic_string<T, CharT, Alloc>& buf) const -> constant_size_const_buffer<Bytes>;

  auto operator()(asio::mutable_buffer buf) const -> constant_size_mutable_buffer<Bytes>;
  auto operator()(asio::const_buffer buf) const -> constant_size_const_buffer<Bytes>;
};


inline constexpr xdr_dynamic_raw_bytes_t xdr_dynamic_raw_bytes;
template<std::size_t Bytes>
inline constexpr xdr_raw_bytes_t<Bytes> xdr_raw_bytes;


// ------------------------------------------------------------------------
// Regular buffers
// ------------------------------------------------------------------------

template<typename ContiguousCollection>
#if __cpp_concepts >= 201907L
requires (std::is_pod_v<typename ContiguousCollection::value_type>)
#endif
class sized_buffer {
  static_assert(std::is_pod_v<typename ContiguousCollection::value_type>,
      "sized-buffer may only be used with collection-of-POD types");

  template<typename OtherCollection>
#if __cpp_concepts >= 201907L
  requires (std::is_pod_v<typename OtherCollection::value_type>)
#endif
  friend class sized_const_buffer;

  public:
  explicit sized_buffer(ContiguousCollection& buf, std::size_t max_size = std::numeric_limits<std::size_t>::max());
  auto max_size() const noexcept -> std::size_t;
  auto buffer() const -> asio::mutable_buffer;
  void resize(std::size_t len);

  private:
  std::size_t max_size_;
  ContiguousCollection* buf_;
};

template<typename ContiguousCollection>
#if __cpp_concepts >= 201907L
requires (std::is_pod_v<typename ContiguousCollection::value_type>)
#endif
class sized_const_buffer {
  static_assert(std::is_pod_v<typename ContiguousCollection::value_type>,
      "sized-buffer may only be used with collection-of-POD types");

  public:
  explicit sized_const_buffer(const ContiguousCollection& buf, std::size_t max_size = std::numeric_limits<std::size_t>::max());
  sized_const_buffer(const sized_buffer<ContiguousCollection>& b);
  auto size() const noexcept -> std::size_t;
  auto max_size() const noexcept -> std::size_t;
  auto buffer() const -> asio::const_buffer;

  private:
  std::size_t max_size_;
  const ContiguousCollection* buf_;
};


template<typename... X, typename ContiguousCollection>
auto operator&(xdr<xdr_reader::reader<X...>>&& x, const sized_buffer<ContiguousCollection>& b);

template<typename... X, typename ContiguousCollection>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, const sized_const_buffer<ContiguousCollection>& b);

template<typename... X, typename ContiguousCollection>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, const sized_buffer<ContiguousCollection>& b);


struct xdr_bytes_t {
  template<typename T, typename Alloc>
#if __cpp_concepts >= 201907L
  requires (std::is_pod_v<T>)
#endif
  auto operator()(std::vector<T, Alloc>& v, std::size_t max_size = std::numeric_limits<std::size_t>::max()) const -> sized_buffer<std::vector<T, Alloc>>;

  template<typename T, typename Alloc>
#if __cpp_concepts >= 201907L
  requires (std::is_pod_v<T>)
#endif
  auto operator()(const std::vector<T, Alloc>& v, std::size_t max_size = std::numeric_limits<std::size_t>::max()) const -> sized_const_buffer<std::vector<T, Alloc>>;

  template<typename T, typename CharT, typename Alloc>
#if __cpp_concepts >= 201907L
  requires (std::is_pod_v<T>)
#endif
  auto operator()(std::basic_string<T, CharT, Alloc>& v, std::size_t max_size = std::numeric_limits<std::size_t>::max()) const -> sized_buffer<std::basic_string<T, CharT, Alloc>>;

  template<typename T, typename CharT, typename Alloc>
#if __cpp_concepts >= 201907L
  requires (std::is_pod_v<T>)
#endif
  auto operator()(const std::basic_string<T, CharT, Alloc>& v, std::size_t max_size = std::numeric_limits<std::size_t>::max()) const -> sized_const_buffer<std::basic_string<T, CharT, Alloc>>;
};

inline constexpr xdr_bytes_t xdr_bytes;


// ------------------------------------------------------------------------
// Boolean
// ------------------------------------------------------------------------

class bool_ref {
  public:
  explicit bool_ref(bool& b) noexcept;
  auto address() const noexcept -> bool*;

  private:
  bool* ref_;
};

class const_bool_ref {
  public:
  explicit const_bool_ref(const bool& b) noexcept;
  const_bool_ref(const bool_ref& b) noexcept;
  auto address() const noexcept -> const bool*;

  private:
  const bool* ref_;
};

struct tmp_bool {
  bool value;
};


template<typename... X>
auto operator&(xdr<xdr_reader::reader<X...>>&& x, bool_ref b);

template<typename... X>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, const_bool_ref b);

template<typename... X>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, bool_ref b);


struct xdr_bool_t {
  auto operator()(bool& b) const -> bool_ref;
  auto operator()(const bool& b) const -> const_bool_ref;
};

inline constexpr xdr_bool_t xdr_bool;


template<typename... X>
inline auto operator&(xdr<xdr_reader::reader<X...>>&& x, tmp_bool& b);

template<typename... X>
inline auto operator&(xdr<xdr_writer::writer<X...>>&& x, const tmp_bool& b);


// ------------------------------------------------------------------------
// Special optional types.
// ------------------------------------------------------------------------

template<typename T, typename Spec = xdr_identity_t>
struct opt_ref {
  explicit opt_ref(std::optional<T>& ref, Spec spec = Spec()) noexcept(std::is_nothrow_move_constructible_v<Spec>);

  std::optional<T>& ref;
  Spec spec;
};

template<typename T, typename Spec = xdr_identity_t>
struct opt_cref {
  explicit opt_cref(const std::optional<T>& ref, Spec spec = Spec()) noexcept(std::is_nothrow_move_constructible_v<Spec>);
  explicit opt_cref(const opt_ref<T, Spec>& r) noexcept(std::is_nothrow_copy_constructible_v<Spec>);
  explicit opt_cref(opt_ref<T, Spec>&& r) noexcept(std::is_nothrow_move_constructible_v<Spec>);

  const std::optional<T>& ref;
  Spec spec;
};

template<typename T>
struct opt_ref<T, xdr_identity_t> {
  explicit opt_ref(std::optional<T>& ref, xdr_identity_t spec = xdr_identity) noexcept;

  template<typename OtherSpec>
  auto as(OtherSpec&& other_spec) const -> opt_ref<T, std::decay_t<OtherSpec>>;

  std::optional<T>& ref;
  xdr_identity_t spec;
};

template<typename T>
struct opt_cref<T, xdr_identity_t> {
  explicit opt_cref(const std::optional<T>& ref, xdr_identity_t spec = xdr_identity) noexcept;
  explicit opt_cref(const opt_ref<T, xdr_identity_t>& r) noexcept;
  explicit opt_cref(opt_ref<T, xdr_identity_t>&& r) noexcept;

  template<typename OtherSpec>
  auto as(OtherSpec&& other_spec) const -> opt_cref<T, std::decay_t<OtherSpec>>;

  const std::optional<T>& ref;
  xdr_identity_t spec;
};


template<typename... X, typename T, typename Spec>
auto operator&(xdr<xdr_reader::reader<X...>>&& x, opt_ref<T, Spec> s);

template<typename... X, typename T, typename Spec>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, opt_cref<T, Spec> s);

template<typename... X, typename T, typename Spec>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, opt_ref<T, Spec> s);


template<typename Spec = xdr_identity_t>
struct xdr_opt_t {
  explicit xdr_opt_t(Spec spec);

  template<typename T>
  auto operator()(std::optional<T>& opt) const -> opt_ref<T>;
  template<typename T>
  auto operator()(const std::optional<T>& opt) const -> opt_cref<T>;

  Spec spec;
};

template<>
struct xdr_opt_t<xdr_identity_t> {
  template<typename T>
  auto operator()(std::optional<T>& opt) const -> opt_ref<T>;
  template<typename T>
  auto operator()(const std::optional<T>& opt) const -> opt_cref<T>;

  template<typename Spec>
  auto of(Spec&& spec) const -> xdr_opt_t<std::decay_t<Spec>>;
};

inline constexpr xdr_opt_t<> xdr_opt;


// ------------------------------------------------------------------------
// Lists
// ------------------------------------------------------------------------

template<typename Iter, typename U>
auto unpack_emplace_result_(const std::pair<Iter, U>& result) noexcept -> const U&;

template<typename Iter>
auto unpack_emplace_result_(const Iter& result) noexcept -> bool;


#if __cpp_concepts >= 201907L
template<typename Collection>
concept sequence = requires(Collection c, const Collection cc, std::size_t sz) {
  typename Collection::value_type;
  { c.resize(sz) };
  { cc.size() } -> std::convertible_to<std::size_t>;
  // { using std::begin; begin(c) };
  // { using std::end;   end(c) };
};

template<typename Collection>
concept set = requires(Collection c, const Collection cc, typename Collection::value_type element) {
  typename Collection::value_type;
  { cc.size() } -> std::convertible_to<std::size_t>;
  { unpack_emplace_result_(c.emplace(std::move(element))) };
  { c.clear() };
  // { using std::begin; begin(cc) };
  // { using std::end;   end(cc) };
};

template<typename Collection>
concept map = requires(Collection c, const Collection cc, std::pair<typename Collection::key_type, typename Collection::value_type> element) {
  typename Collection::key_type;
  typename Collection::mapped_type;
  { cc.size() } -> std::convertible_to<std::size_t>;
  { unpack_emplace_result_(c.emplace(std::move(element))) };
  { c.clear() };
  // { using std::begin; begin(cc) };
  // { using std::end;   end(cc) };
};
#endif


template<typename Collection, typename Spec = xdr_identity_t>
#if __cpp_concepts >= 201907L
requires sequence<Collection>
#endif
class sequence_holder {
  public:
  sequence_holder(Collection& collection, std::size_t max_size, Spec spec);
  auto max_size() const noexcept -> std::size_t;
  auto collection() const noexcept -> Collection&;
  auto spec() const & noexcept -> const Spec&;
  auto spec() && noexcept -> Spec&&;

  private:
  std::size_t max_size_;
  Collection* collection_;
  Spec spec_;
};

template<typename Collection>
#if __cpp_concepts >= 201907L
requires sequence<Collection>
#endif
class sequence_holder<Collection, xdr_identity_t> {
  public:
  sequence_holder(Collection& collection, std::size_t max_size);
  auto max_size() const noexcept -> std::size_t;
  auto collection() const noexcept -> Collection&;
  auto spec() const noexcept -> xdr_identity_t;

  template<typename OtherSpec>
  auto of(OtherSpec&& other_spec) const -> sequence_holder<Collection, std::decay_t<OtherSpec>>;

  private:
  std::size_t max_size_;
  Collection* collection_;
};

template<typename Collection, typename Spec = xdr_identity_t>
#if __cpp_concepts >= 201907L
requires sequence<Collection>
#endif
class const_sequence_holder {
  public:
  const_sequence_holder(const Collection& collection, std::size_t max_size, Spec spec);
  const_sequence_holder(sequence_holder<Collection, Spec> c) noexcept;
  auto max_size() const noexcept -> std::size_t;
  auto collection() const noexcept -> const Collection&;
  auto spec() const & noexcept -> const Spec&;
  auto spec() && noexcept -> Spec&&;

  private:
  std::size_t max_size_;
  const Collection* collection_;
  Spec spec_;
};

template<typename Collection>
#if __cpp_concepts >= 201907L
requires sequence<Collection>
#endif
class const_sequence_holder<Collection, xdr_identity_t> {
  public:
  const_sequence_holder(const Collection& collection, std::size_t max_size);
  const_sequence_holder(const sequence_holder<Collection, xdr_identity_t>& c) noexcept;
  auto max_size() const noexcept -> std::size_t;
  auto collection() const noexcept -> const Collection&;
  auto spec() const noexcept -> xdr_identity_t;

  template<typename OtherSpec>
  auto of(OtherSpec&& other_spec) const -> const_sequence_holder<Collection, std::decay_t<OtherSpec>>;

  private:
  std::size_t max_size_;
  const Collection* collection_;
};


template<typename Collection, typename Spec = xdr_identity_t>
#if __cpp_concepts >= 201907L
requires set<Collection>
#endif
class set_holder {
  public:
  set_holder(Collection& collection, std::size_t max_size, Spec spec);
  auto max_size() const noexcept -> std::size_t;
  auto collection() const noexcept -> Collection&;
  auto spec() const & noexcept -> const Spec&;
  auto spec() && noexcept -> Spec&&;

  private:
  std::size_t max_size_;
  Collection* collection_;
  Spec spec_;
};

template<typename Collection>
#if __cpp_concepts >= 201907L
requires set<Collection>
#endif
class set_holder<Collection, xdr_identity_t> {
  public:
  set_holder(Collection& collection, std::size_t max_size);
  auto max_size() const noexcept -> std::size_t;
  auto collection() const noexcept -> Collection&;
  auto spec() const noexcept -> xdr_identity_t;

  template<typename OtherSpec>
  auto of(OtherSpec&& other_spec) const -> set_holder<Collection, std::decay_t<OtherSpec>>;

  private:
  std::size_t max_size_;
  Collection* collection_;
};

template<typename Collection, typename Spec = xdr_identity_t>
#if __cpp_concepts >= 201907L
requires set<Collection>
#endif
class const_set_holder {
  public:
  const_set_holder(const Collection& collection, std::size_t max_size, Spec spec);
  const_set_holder(set_holder<Collection, Spec> c) noexcept;
  auto max_size() const noexcept -> std::size_t;
  auto collection() const noexcept -> const Collection&;
  auto spec() const & noexcept -> const Spec&;
  auto spec() && noexcept -> Spec&&;

  private:
  std::size_t max_size_;
  const Collection* collection_;
  Spec spec_;
};

template<typename Collection>
#if __cpp_concepts >= 201907L
requires set<Collection>
#endif
class const_set_holder<Collection, xdr_identity_t> {
  public:
  const_set_holder(const Collection& collection, std::size_t max_size);
  const_set_holder(const set_holder<Collection, xdr_identity_t>& c) noexcept;
  auto max_size() const noexcept -> std::size_t;
  auto collection() const noexcept -> const Collection&;
  auto spec() const noexcept -> xdr_identity_t;

  template<typename OtherSpec>
  auto of(OtherSpec&& other_spec) const -> const_set_holder<Collection, std::decay_t<OtherSpec>>;

  private:
  std::size_t max_size_;
  const Collection* collection_;
};


template<typename Collection, typename KeySpec = xdr_identity_t, typename MappedSpec = xdr_identity_t>
#if __cpp_concepts >= 201907L
requires map<Collection>
#endif
class map_holder {
  public:
  map_holder(Collection& collection, std::size_t max_size, KeySpec key_spec, MappedSpec mapped_spec);
  auto max_size() const noexcept -> std::size_t;
  auto collection() const noexcept -> Collection&;
  auto key_spec() const & noexcept -> const KeySpec&;
  auto key_spec() && noexcept -> KeySpec&&;
  auto mapped_spec() const & noexcept -> const MappedSpec&;
  auto mapped_spec() && noexcept -> MappedSpec&&;

  private:
  std::size_t max_size_;
  Collection* collection_;
  KeySpec key_spec_;
  MappedSpec mapped_spec_;
};

template<typename Collection>
#if __cpp_concepts >= 201907L
requires map<Collection>
#endif
class map_holder<Collection, xdr_identity_t, xdr_identity_t> {
  public:
  map_holder(Collection& collection, std::size_t max_size);
  auto max_size() const noexcept -> std::size_t;
  auto collection() const noexcept -> Collection&;
  auto key_spec() const noexcept -> xdr_identity_t;
  auto mapped_spec() const noexcept -> xdr_identity_t;

  template<typename KeySpec, typename MappedSpec>
  auto of(KeySpec&& key_spec, MappedSpec&& mapped_spec) const -> map_holder<Collection, std::decay_t<KeySpec>, std::decay_t<MappedSpec>>;

  private:
  std::size_t max_size_;
  Collection* collection_;
};

template<typename Collection, typename KeySpec = xdr_identity_t, typename MappedSpec = xdr_identity_t>
#if __cpp_concepts >= 201907L
requires map<Collection>
#endif
class const_map_holder {
  public:
  const_map_holder(const Collection& collection, std::size_t max_size, KeySpec key_spec, MappedSpec mapped_spec);
  const_map_holder(map_holder<Collection, KeySpec, MappedSpec> c) noexcept;
  auto max_size() const noexcept -> std::size_t;
  auto collection() const noexcept -> const Collection&;
  auto key_spec() const & noexcept -> const KeySpec&;
  auto key_spec() && noexcept -> KeySpec&&;
  auto mapped_spec() const & noexcept -> const MappedSpec&;
  auto mapped_spec() && noexcept -> MappedSpec&&;

  private:
  std::size_t max_size_;
  const Collection* collection_;
  KeySpec key_spec_;
  MappedSpec mapped_spec_;
};

template<typename Collection>
#if __cpp_concepts >= 201907L
requires map<Collection>
#endif
class const_map_holder<Collection, xdr_identity_t, xdr_identity_t> {
  public:
  const_map_holder(const Collection& collection, std::size_t max_size);
  const_map_holder(const map_holder<Collection, xdr_identity_t, xdr_identity_t>& c) noexcept;
  auto max_size() const noexcept -> std::size_t;
  auto collection() const noexcept -> const Collection&;
  auto key_spec() const noexcept -> xdr_identity_t;
  auto mapped_spec() const noexcept -> xdr_identity_t;

  template<typename KeySpec, typename MappedSpec>
  auto of(KeySpec&& key_spec, MappedSpec&& mapped_spec) const -> const_map_holder<Collection, std::decay_t<KeySpec>, std::decay_t<MappedSpec>>;

  private:
  std::size_t max_size_;
  const Collection* collection_;
};


template<typename Stream, typename Iter, typename Sentinel, typename Callback, typename Spec>
void read_sequence_(Stream& stream, Iter b, Sentinel e, Callback callback, const Spec& spec);

template<typename Stream, typename Iter, typename Sentinel, typename Callback, typename Spec>
void write_sequence_(Stream& stream, Iter b, Sentinel e, Callback callback, const Spec& spec);

template<typename Stream, typename Set, typename Callback, typename Spec>
void read_set_(Stream& stream, Set c, Callback callback, const Spec& spec);

template<typename Stream, typename Set, typename Callback, typename Spec>
void write_set_(Stream& stream, const Set& c, Callback callback, const Spec& spec);

template<typename Stream, typename Map, typename Callback, typename KeySpec, typename MappedSpec>
void read_map_(Stream& stream, Map c, Callback callback, const KeySpec& key_spec, const MappedSpec& mapped_spec);

template<typename Stream, typename Map, typename Callback, typename Spec>
void write_map_(Stream& stream, const Map& c, Callback callback, const Spec& spec);


template<typename... X, typename Collection, typename Spec>
auto operator&(xdr<xdr_reader::reader<X...>>&& x, sequence_holder<Collection, Spec> c);
template<typename... X, typename Collection, typename Spec>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, const_sequence_holder<Collection, Spec> c);
template<typename... X, typename Collection, typename Spec>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, sequence_holder<Collection, Spec> c);

template<typename... X, typename Collection, typename Spec>
auto operator&(xdr<xdr_reader::reader<X...>>&& x, set_holder<Collection, Spec> c);
template<typename... X, typename Collection, typename Spec>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, const_set_holder<Collection, Spec> c);
template<typename... X, typename Collection, typename Spec>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, set_holder<Collection, Spec> c);

template<typename... X, typename Collection, typename KeySpec, typename MappedSpec>
auto operator&(xdr<xdr_reader::reader<X...>>&& x, map_holder<Collection, KeySpec, MappedSpec> c);
template<typename... X, typename Collection, typename KeySpec, typename MappedSpec>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, const_map_holder<Collection, KeySpec, MappedSpec> c);
template<typename... X, typename Collection, typename KeySpec, typename MappedSpec>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, map_holder<Collection, KeySpec, MappedSpec> c);


template<typename Spec = xdr_identity_t>
struct xdr_sequence_t {
  explicit xdr_sequence_t(Spec spec);

  template<typename Collection>
  auto operator()(Collection& c, std::size_t max_size = std::numeric_limits<std::size_t>::max()) const -> sequence_holder<Collection, Spec>;
  template<typename Collection>
  auto operator()(const Collection& c, std::size_t max_size = std::numeric_limits<std::size_t>::max()) const -> const_sequence_holder<Collection, Spec>;

  Spec spec;
};

template<>
struct xdr_sequence_t<xdr_identity_t> {
  template<typename Collection>
  auto operator()(Collection& c, std::size_t max_size = std::numeric_limits<std::size_t>::max()) const -> sequence_holder<Collection>;
  template<typename Collection>
  auto operator()(const Collection& c, std::size_t max_size = std::numeric_limits<std::size_t>::max()) const -> const_sequence_holder<Collection>;

  template<typename Spec>
  auto of(Spec&& spec) const -> xdr_sequence_t<std::decay_t<Spec>>;
};

template<typename Spec = xdr_identity_t>
struct xdr_set_t {
  explicit xdr_set_t(Spec spec);

  template<typename Collection>
  auto operator()(Collection& c, std::size_t max_size = std::numeric_limits<std::size_t>::max()) const -> set_holder<Collection, Spec>;
  template<typename Collection>
  auto operator()(const Collection& c, std::size_t max_size = std::numeric_limits<std::size_t>::max()) const -> const_set_holder<Collection, Spec>;

  Spec spec;
};

template<>
struct xdr_set_t<xdr_identity_t> {
  template<typename Collection>
  auto operator()(Collection& c, std::size_t max_size = std::numeric_limits<std::size_t>::max()) const -> set_holder<Collection>;
  template<typename Collection>
  auto operator()(const Collection& c, std::size_t max_size = std::numeric_limits<std::size_t>::max()) const -> const_set_holder<Collection>;

  template<typename Spec>
  auto of(Spec&& spec) const -> xdr_set_t<std::decay_t<Spec>>;
};

template<typename KeySpec = xdr_identity_t, typename MappedSpec = xdr_identity_t>
struct xdr_map_t {
  explicit xdr_map_t(KeySpec key_spec, MappedSpec mapped_spec);

  template<typename Collection>
  auto operator()(Collection& c, std::size_t max_size = std::numeric_limits<std::size_t>::max()) const -> map_holder<Collection, KeySpec, MappedSpec>;
  template<typename Collection>
  auto operator()(const Collection& c, std::size_t max_size = std::numeric_limits<std::size_t>::max()) const -> const_map_holder<Collection, KeySpec, MappedSpec>;

  KeySpec key_spec;
  MappedSpec mapped_spec;
};

template<>
struct xdr_map_t<xdr_identity_t, xdr_identity_t> {
  template<typename Collection>
  auto operator()(Collection& c, std::size_t max_size = std::numeric_limits<std::size_t>::max()) const -> map_holder<Collection>;
  template<typename Collection>
  auto operator()(const Collection& c, std::size_t max_size = std::numeric_limits<std::size_t>::max()) const -> const_map_holder<Collection>;

  template<typename KeySpec, typename MappedSpec>
  auto of(KeySpec&& key_spec, MappedSpec&& mapped_spec) const -> xdr_map_t<std::decay_t<KeySpec>, std::decay_t<MappedSpec>>;
};

inline constexpr xdr_sequence_t<> xdr_sequence;
inline constexpr xdr_set_t<> xdr_set;
inline constexpr xdr_map_t<> xdr_map;


// ------------------------------------------------------------------------
// Pairs/tuples
// ------------------------------------------------------------------------

template<typename TupleOrPair, typename... Spec>
#if __cpp_concepts >= 201907L
requires (std::tuple_size_v<TupleOrPair> == sizeof...(Spec) || sizeof...(Spec) == 0)
#endif
struct tuple_holder {
  TupleOrPair& ref;
  std::tuple<Spec...> specs;
};

template<typename TupleOrPair>
struct tuple_holder<TupleOrPair> {
  template<typename... Spec>
  auto of(Spec&&... spec) -> std::enable_if_t<sizeof...(Spec) == std::tuple_size_v<std::remove_const_t<TupleOrPair>>, tuple_holder<TupleOrPair, std::decay_t<Spec>...>>;

  TupleOrPair& ref;
};


template<typename X, typename TupleOrPair, typename... Spec, std::size_t... Idx>
auto do_tuple_(xdr<X>&& x, const tuple_holder<TupleOrPair, Spec...>& t, std::index_sequence<Idx...> seq);

template<typename X, typename TupleOrPair, typename... Spec>
auto operator&(xdr<X>&& x, const tuple_holder<TupleOrPair, Spec...>& t);


template<typename FirstSpec = xdr_identity_t, typename SecondSpec = xdr_identity_t>
struct xdr_pair_t {
  template<typename T, typename U>
  auto operator()(std::pair<T, U>& tpl) const -> tuple_holder<std::pair<T, U>, FirstSpec, SecondSpec>;
  template<typename T, typename U>
  auto operator()(const std::pair<T, U>& tpl) const -> tuple_holder<const std::pair<T, U>, FirstSpec, SecondSpec>;

  FirstSpec first_spec;
  SecondSpec second_spec;
};

template<>
struct xdr_pair_t<xdr_identity_t, xdr_identity_t> {
  template<typename T, typename U>
  auto operator()(std::pair<T, U>& tpl) const -> tuple_holder<std::pair<T, U>>;
  template<typename T, typename U>
  auto operator()(const std::pair<T, U>& tpl) const -> tuple_holder<const std::pair<T, U>>;

  template<typename FirstSpec, typename SecondSpec>
  auto of(FirstSpec&& first_spec, SecondSpec&& second_spec) const -> xdr_pair_t<std::decay_t<FirstSpec>, std::decay_t<SecondSpec>>;
};

template<typename... Spec>
struct xdr_tuple_t {
  template<typename... T>
#if __cpp_concepts >= 201907L
  requires (sizeof...(Spec) == sizeof...(T))
#endif
  auto operator()(std::tuple<T...>& tpl) const -> tuple_holder<std::tuple<T...>, Spec...>;

  template<typename... T>
#if __cpp_concepts >= 201907L
  requires (sizeof...(Spec) == sizeof...(T))
#endif
  auto operator()(const std::tuple<T...>& tpl) const -> tuple_holder<const std::tuple<T...>, Spec...>;

  std::tuple<Spec...> specs;
};

template<>
struct xdr_tuple_t<> {
  template<typename... T>
  auto operator()(std::tuple<T...>& tpl) const -> tuple_holder<std::tuple<T...>>;

  template<typename... T>
  auto operator()(const std::tuple<T...>& tpl) const -> tuple_holder<const std::tuple<T...>>;

  template<typename... Spec>
  auto of(Spec&&... specs) const -> xdr_tuple_t<std::decay_t<Spec>...>;
};


inline constexpr xdr_pair_t<> xdr_pair;
inline constexpr xdr_tuple_t<> xdr_tuple;


// ------------------------------------------------------------------------
// Constant serialization
// ------------------------------------------------------------------------

template<typename T, typename Spec = xdr_identity_t>
struct constant_holder {
  T value;
  Spec spec;
};

template<typename T>
struct constant_holder<T, xdr_identity_t> {
  T value;
  xdr_identity_t spec;

  template<typename Spec>
  auto as(Spec&& spec) const & -> constant_holder<T, std::decay_t<Spec>>;
  template<typename Spec>
  auto as(Spec&& spec) && -> constant_holder<T, std::decay_t<Spec>>;
};

template<typename... X, typename T, typename Spec>
auto operator&(xdr<xdr_reader::reader<X...>>&& x, const constant_holder<T, Spec>& c);

template<typename... X, typename T, typename Spec>
auto operator&(xdr<xdr_writer::writer<X...>>&& x, const constant_holder<T, Spec>& c);


template<typename Spec = xdr_identity_t>
struct xdr_constant_t {
  template<typename T>
  auto operator()(T&& v) const -> constant_holder<std::decay_t<T>>;

  Spec spec;
};

template<>
struct xdr_constant_t<xdr_identity_t> {
  template<typename T>
  auto operator()(T&& v) const -> constant_holder<std::decay_t<T>>;

  template<typename OtherSpec>
  auto as(OtherSpec&& other_spec) const -> xdr_constant_t<std::decay_t<OtherSpec>>;

  xdr_identity_t spec;
};


inline constexpr xdr_constant_t<> xdr_constant;


// ------------------------------------------------------------------------
// Inline processors.
// ------------------------------------------------------------------------

template<typename Fn>
struct processor_holder {
  Fn fn;
};

template<typename X, typename Fn>
auto operator&(xdr<X>&& x, processor_holder<Fn> p);

struct xdr_processor_t {
  template<typename Fn>
#if __cpp_concepts >= 201907L
  requires requires(std::decay_t<Fn> fn) {
    { std::invoke(fn) } -> std::convertible_to<std::error_code>;
  }
#endif
  auto operator()(Fn&& fn) const -> processor_holder<std::decay_t<Fn>>;
};


inline constexpr xdr_processor_t xdr_processor;


} /* namespace earnest::detail::xdr_types */


namespace earnest {


using detail::xdr_types::xdr_identity;

using detail::xdr_types::xdr_uint8;
using detail::xdr_types::xdr_uint16;
using detail::xdr_types::xdr_uint32;
using detail::xdr_types::xdr_uint64;
using detail::xdr_types::xdr_int8;
using detail::xdr_types::xdr_int16;
using detail::xdr_types::xdr_int32;
using detail::xdr_types::xdr_int64;

using detail::xdr_types::xdr_getset;

using detail::xdr_types::xdr_float32;
using detail::xdr_types::xdr_float64;

using detail::xdr_types::xdr_dynamic_padding;
using detail::xdr_types::xdr_padding;

using detail::xdr_types::xdr_opt;

using detail::xdr_types::xdr_dynamic_raw_bytes;
using detail::xdr_types::xdr_raw_bytes;

using detail::xdr_types::xdr_bytes;

using detail::xdr_types::xdr_bool;

using detail::xdr_types::xdr_sequence;
using detail::xdr_types::xdr_set;
using detail::xdr_types::xdr_map;

using detail::xdr_types::xdr_pair;
using detail::xdr_types::xdr_tuple;

using detail::xdr_types::xdr_constant;

using detail::xdr_types::xdr_processor;


template<typename... X, typename T>
#if __cpp_concepts >= 201907L
requires detail::xdr_reader::readable<T>
#endif
auto operator&(xdr<detail::xdr_reader::reader<X...>>&& x, std::optional<T>& o);

template<typename... X, typename T>
#if __cpp_concepts >= 201907L
requires detail::xdr_writer::writeable<T>
#endif
auto operator&(xdr<detail::xdr_writer::writer<X...>>&& x, const std::optional<T>& p);


template<typename... X, typename T, typename U>
#if __cpp_concepts >= 201907L
requires detail::xdr_reader::readable<T> && detail::xdr_reader::readable<U>
#endif
auto operator&(xdr<detail::xdr_reader::reader<X...>>&& x, std::pair<T, U>& p);

template<typename... X, typename T, typename U>
#if __cpp_concepts >= 201907L
requires detail::xdr_writer::writeable<T> && detail::xdr_writer::writeable<U>
#endif
auto operator&(xdr<detail::xdr_writer::writer<X...>>&& x, const std::pair<T, U>& p);


template<typename... X, typename... T>
#if __cpp_concepts >= 201907L
requires (detail::xdr_reader::readable<T> &&...)
#endif
auto operator&(xdr<detail::xdr_reader::reader<X...>>&& x, std::tuple<T...>& p);

template<typename... X, typename... T>
#if __cpp_concepts >= 201907L
requires (detail::xdr_writer::writeable<T> &&...)
#endif
auto operator&(xdr<detail::xdr_writer::writer<X...>>&& x, const std::tuple<T...>& p);


} /* namespace earnest */

#include "xdr.ii"
