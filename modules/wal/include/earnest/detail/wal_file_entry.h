#pragma once

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <optional>
#include <ranges>
#include <scoped_allocator>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <vector>

#include <asio/buffer.hpp>
#include <asio/strand.hpp>

#include <earnest/detail/buffered_readstream_adapter.h>
#include <earnest/detail/byte_stream.h>
#include <earnest/detail/fanout.h>
#include <earnest/detail/fanout_barrier.h>
#include <earnest/detail/move_only_function.h>
#include <earnest/detail/overload.h>
#include <earnest/detail/positional_stream_adapter.h>
#include <earnest/detail/wal_flusher.h>
#include <earnest/detail/wal_logger.h>
#include <earnest/detail/wal_records.h>
#include <earnest/dir.h>
#include <earnest/fd.h>
#include <earnest/wal_error.h>
#include <earnest/xdr.h>
#include <earnest/xdr_v2.h>

namespace earnest::detail {


enum class wal_file_entry_state {
  uninitialized,
  opening,
  ready,
  sealing,
  sealed,
  failed = -1
};

auto operator<<(std::ostream& out, wal_file_entry_state state) -> std::ostream&;


struct no_transaction_validation {};


template<typename Executor, typename Allocator>
class wal_file_entry
: public std::enable_shared_from_this<wal_file_entry<Executor, Allocator>>
{
  public:
  static inline constexpr std::uint_fast32_t max_version = 0;
  static inline constexpr std::size_t read_buffer_size = 2u * 1024u * 1024u;
  using executor_type = Executor;
  using allocator_type = Allocator;
  using link_done_event_type = fanout<executor_type, void(std::error_code), allocator_type>;

  private:
  template<typename T>
  using rebind_alloc = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;

  public:
  using variant_type = wal_record_variant<Executor>;
  using write_variant_type = record_write_type_t<variant_type>;
  using records_vector = std::vector<variant_type, rebind_alloc<variant_type>>;
  using write_records_vector = std::vector<write_variant_type, std::scoped_allocator_adaptor<rebind_alloc<write_variant_type>>>;
  using fd_type = fd<executor_type>;

  private:
  struct write_record_with_offset {
    write_variant_type record;
    std::uint64_t offset;
  };
  using write_record_with_offset_vector = std::vector<write_record_with_offset, std::scoped_allocator_adaptor<rebind_alloc<write_record_with_offset>>>;

  struct unwritten_link {
    unwritten_link(executor_type ex, std::uint64_t offset, std::array<std::byte, 4> data, allocator_type alloc)
    : offset(offset),
      data(data),
      done(ex, alloc)
    {}

    std::uint64_t offset;
    std::array<std::byte, 4> data;
    fanout<executor_type, void(std::error_code), allocator_type> done;
  };
  using unwritten_link_vector = std::vector<unwritten_link, rebind_alloc<unwritten_link>>;

  public:
  wal_file_entry(const executor_type& ex, allocator_type alloc);

  wal_file_entry(const wal_file_entry&) = delete;
  wal_file_entry(wal_file_entry&&) = delete;
  wal_file_entry& operator=(const wal_file_entry&) = delete;
  wal_file_entry& operator=(wal_file_entry&&) = delete;

  auto get_executor() const -> executor_type { return file.get_executor(); }
  auto get_allocator() const -> allocator_type { return alloc_; }
  auto state() const noexcept -> wal_file_entry_state { return state_; }

  private:
  struct xdr_header_invocation;
  auto header_reader_();
  auto header_writer_() const;

  template<typename Stream, typename Acceptor, typename Callback>
  auto read_records_until_(Stream& stream, Acceptor&& acceptor, Callback callback, typename fd_type::offset_type end_offset) const -> void;

  auto decorate_(variant_type& v) const -> void;

  // XDR-invocation for write_records_to_buffer_.
  struct xdr_invocation_ {
    explicit xdr_invocation_(write_record_with_offset_vector& wrwo_vec) noexcept
    : wrwo_vec(wrwo_vec)
    {}

    auto write(const write_variant_type& r) const {
      return xdr_v2::manual.write(
          [&r, self=*this](auto& stream) {
            self.update_wrwo_vec(stream, r);
            return execution::just(std::move(stream));
          })
      | xdr_v2::identity.write(r);
    }

    private:
    template<typename Ex, typename Alloc>
    auto update_wrwo_vec(const byte_stream<Ex, Alloc>& stream, const write_variant_type& r) const -> void {
      wrwo_vec.push_back({
            .record=r,
            .offset=stream.data().size(),
          });
    }

    template<typename T>
    auto update_wrwo_vec(const std::reference_wrapper<T>& stream, const write_variant_type& r) const -> void {
      update_wrwo_vec(stream.get(), r);
    }

    write_record_with_offset_vector& wrwo_vec;
  };

  auto write_records_to_buffer_(write_records_vector&& records) const {
    using byte_vector = std::vector<std::byte, rebind_alloc<std::byte>>;
    using byte_stream = byte_stream<executor_type, rebind_alloc<std::byte>>;
    using namespace execution;

    return just(
        byte_stream(get_executor(), get_allocator()),
        write_record_with_offset_vector(get_allocator()),
        std::move(records))
    | let_value(
        [](auto& fd, auto& wrwo_vec, auto& records) {
          return xdr_v2::write(std::ref(fd), records, xdr_v2::fixed_collection(xdr_invocation_(wrwo_vec)))
          | then(
              [&wrwo_vec](byte_stream& fd) {
                return std::make_tuple(std::move(fd).data(), std::move(wrwo_vec));
              })
          | explode_tuple();
        });
  }

  template<typename CompletionToken>
  auto write_records_to_buffer_(write_records_vector&& records, CompletionToken&& token) const {
    using byte_vector = std::vector<std::byte, rebind_alloc<std::byte>>;
    using byte_stream = byte_stream<executor_type, rebind_alloc<std::byte>>;
    using namespace execution;

    return asio::async_initiate<CompletionToken, void(std::error_code, byte_vector, write_record_with_offset_vector)>(
        [](auto completion_handler, std::shared_ptr<const wal_file_entry> wf, auto records) {
          start_detached(
              wf->write_records_to_buffer_(std::move(records))
              | then(
                  [](auto bytes, auto wrwo_vec) {
                    return std::make_tuple(std::error_code{}, std::move(bytes), std::move(wrwo_vec));
                  })
              | upon_error(
                  [alloc=wf->get_allocator()](auto err) -> std::tuple<std::error_code, byte_vector, write_record_with_offset_vector> {
                    if constexpr(std::same_as<std::exception_ptr, decltype(err)>)
                      std::rethrow_exception(err);
                    else
                      return std::make_tuple(std::error_code{}, byte_vector(alloc), write_record_with_offset_vector(alloc));
                  })
              | explode_tuple()
              | then(completion_handler_fun(std::move(completion_handler), wf->get_executor())));
        },
        token, this->shared_from_this(), std::move(records));
  }

  public:
  auto async_open(const dir& d, const std::filesystem::path& name)
  -> execution::type_erased_sender<
      std::variant<std::tuple<std::shared_ptr<wal_file_entry>>>,
      std::variant<std::exception_ptr, std::error_code>,
      false> {
    using namespace execution;
    using pos_stream = positional_stream_adapter<fd_type&>;

    return just(this->shared_from_this(), d, name)
    | lazy_observe_value(
        [logger=this->logger](const std::shared_ptr<wal_file_entry>& wf,
            const dir& d,
            const std::filesystem::path& name) {
          logger->info("opening wal file {}", name.native());
        })
    | validation(
        [](const std::shared_ptr<wal_file_entry>& wf,
            [[maybe_unused]] const dir& d,
            const std::filesystem::path& name) -> std::optional<std::exception_ptr> {
          if (name.has_parent_path()) return std::make_exception_ptr(std::runtime_error("wal file must be a filename"));
          if (wf->file.is_open()) return std::make_exception_ptr(std::logic_error("wal is already open"));
          return std::nullopt;
        })
    | validation(
        [](const std::shared_ptr<wal_file_entry>& wf,
            [[maybe_unused]] const dir& d,
            [[maybe_unused]] const std::filesystem::path& name) -> std::optional<std::error_code> {
          if (wf->state_ != wal_file_entry_state::uninitialized) return make_error_code(wal_errc::bad_state);
          return std::nullopt;
        })
    | then(
        [](std::shared_ptr<wal_file_entry> wf,
            [[maybe_unused]] const dir& d,
            [[maybe_unused]] const std::filesystem::path& name) {
          wf->state_ = wal_file_entry_state::opening;
          std::error_code ec;
          wf->file.open(d, name, fd_type::READ_WRITE, ec);
          return std::make_tuple(ec, wf, name);
        })
    | explode_tuple()
    | validation(
        [](std::error_code ec, [[maybe_unused]] const std::shared_ptr<wal_file_entry>& wf, [[maybe_unused]] const std::filesystem::path& name) -> std::optional<std::error_code> {
          if (ec) return ec;
          return std::nullopt;
        })
    | then(
        []([[maybe_unused]] std::error_code ec, std::shared_ptr<wal_file_entry> wf, const std::filesystem::path& name) {
          return std::make_tuple(wf, name);
        })
    | explode_tuple()
    | validation(
        [](const std::shared_ptr<wal_file_entry>& wf, [[maybe_unused]] const std::filesystem::path& name) -> std::optional<std::error_code> {
          std::error_code ec;
          bool was_locked = wf->file.ftrylock(ec);
          if (ec) return ec;
          if (!was_locked) return make_error_code(wal_errc::unable_to_lock);
          return std::nullopt;
        })
    | then(
        [](std::shared_ptr<wal_file_entry> wf, std::filesystem::path name) {
          wf->name = name;
          return wf;
        })
    | let_value(
        [](std::shared_ptr<wal_file_entry>& wf) -> sender_of<std::shared_ptr<wal_file_entry>> auto {
          return xdr_v2::read(pos_stream(wf->file), *wf, xdr_header_invocation{})
          | then(
              [&wf](pos_stream&& stream) {
                struct repeat_state {
                  std::shared_ptr<wal_file_entry>& wf;
                  bool done;
                };
                return std::make_tuple(std::move(stream), repeat_state{wf, false});
              })
          | explode_tuple()
          | repeat(
              []([[maybe_unused]] std::size_t idx, pos_stream& stream, auto& repeat_state) {
                auto factory = [&]() {
                  repeat_state.wf->link_offset_ = stream.position();

                  return std::make_optional(
                      xdr_v2::read_into<variant_type>(std::ref(stream))
                      | observe_value(
                          [&repeat_state]([[maybe_unused]] const auto& stream, const variant_type& v) {
                            repeat_state.wf->logger->debug("read {}", v);
                          })
                      | then(
                          [&repeat_state](std::reference_wrapper<pos_stream> stream, variant_type&& v) -> std::error_code {
                            using namespace std::literals;

                            wal_file_entry& wf = *repeat_state.wf;
                            wf.write_offset_ = stream.get().position();
                            repeat_state.done = std::holds_alternative<wal_record_end_of_records>(v);

                            if (!repeat_state.done) {
                              if (wf.state_ == wal_file_entry_state::sealed) {
                                wf.logger->error("file {} is bad: record after seal", wf.name.native());
                                return make_error_code(wal_errc::unrecoverable);
                              } else if (std::holds_alternative<wal_record_seal>(v)) {
                                wf.state_ = wal_file_entry_state::sealed;
                              }
                            }
                            return std::error_code{};
                          })
                      | validation(
                          [](std::error_code err) -> std::optional<std::error_code> {
                            if (err)
                              return err;
                            else
                              return std::nullopt;
                          }));
                };
                using optional_type = decltype(factory());

                if (repeat_state.done)
                  return optional_type(std::nullopt);
                else
                  return factory();
              })
          | then(
              []([[maybe_unused]] pos_stream&& stream, auto&& repeat_state) -> std::shared_ptr<wal_file_entry> {
                return std::move(repeat_state.wf);
              });
        })
    | let_value(
        [](std::shared_ptr<wal_file_entry>& wf) -> sender_of<std::shared_ptr<wal_file_entry>> auto {
          return wf->wal_flusher_.lazy_flush(false)
          | then(
              [&wf]() -> std::shared_ptr<wal_file_entry>&& {
                return std::move(wf);
              });
        })
    | then(
        [](std::shared_ptr<wal_file_entry> wf) {
          if (wf->state_ == wal_file_entry_state::opening) wf->state_ = wal_file_entry_state::ready;
          wf->link_done_event_(std::error_code{});
          return wf;
        })
    // If anything fails, we set the state to uninitialized.
    // (But I want to move to a system where the sender-chain allocates a new wf,
    // thus not requiring that, since the error-channel will never hold a wf.
    // But first, we have to complete the refactoring.)
    | let_error(
        [wf=this->shared_from_this()](auto error) {
          if (wf->state_ == wal_file_entry_state::opening) wf->state_ = wal_file_entry_state::uninitialized;
          wf->file.close();

          return just(std::move(error))
          | validation(
              [](auto err) -> std::optional<std::error_code> {
                if constexpr(std::same_as<std::exception_ptr, decltype(err)>)
                  std::rethrow_exception(err);
                else
                  return err;
              })
          | then( // Fix up the type.
              [](auto err) -> std::shared_ptr<wal_file_entry> {
                return nullptr;
              });
        });
  }

  template<typename CompletionToken>
  [[deprecated]]
  auto async_open(const dir& d, const std::filesystem::path& name, CompletionToken&& token) {
    using namespace execution;

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto completion_handler, std::shared_ptr<wal_file_entry> wf, const dir& d, const std::filesystem::path& name) -> void {
          start_detached(
              wf->async_open(d, name)
              | then(
                  []([[maybe_unused]] std::shared_ptr<wal_file_entry> wf) -> std::error_code {
                    return std::error_code{};
                  })
              | upon_error(
                  [logger=wf->logger](auto err) -> std::error_code {
                    if constexpr(std::same_as<std::exception_ptr, decltype(err)>) {
                      logger->error("exception");
                      std::rethrow_exception(err);
                    } else {
                      logger->error("error: {}", err.message());
                      return err;
                    }
                  })
              | then(completion_handler_fun(std::move(completion_handler), wf->get_executor())));
        },
        token, this->shared_from_this(), d, name);
  }

  auto async_create(const dir& d, const std::filesystem::path& name, std::uint_fast64_t sequence)
  -> execution::type_erased_sender<
      std::variant<std::tuple<std::shared_ptr<wal_file_entry>>>,
      std::variant<std::exception_ptr, std::error_code>,
      false>
  {
    using type_erased_sender = execution::type_erased_sender<
        std::variant<std::tuple<std::shared_ptr<wal_file_entry>>>,
        std::variant<std::exception_ptr, std::error_code>,
        false>;
    using namespace execution;
    using pos_stream = positional_stream_adapter<fd_type&>;

    return just(this->shared_from_this(), d, name, sequence)
    | validation(
        [](const std::shared_ptr<wal_file_entry>& wf,
            [[maybe_unused]] const dir& d,
            const std::filesystem::path& name,
            [[maybe_unused]] std::uint_fast64_t sequence) -> std::optional<std::exception_ptr> {
          if (name.has_parent_path()) return std::make_exception_ptr(std::runtime_error("wal file must be a filename"));
          if (wf->file.is_open()) return std::make_exception_ptr(std::logic_error("wal is already open"));
          return std::nullopt;
        })
    | validation(
        [](const std::shared_ptr<wal_file_entry>& wf,
            [[maybe_unused]] const dir& d,
            [[maybe_unused]] const std::filesystem::path& name,
            [[maybe_unused]] std::uint_fast64_t sequence) -> std::optional<std::error_code> {
          if (wf->state_ != wal_file_entry_state::uninitialized) return make_error_code(wal_errc::bad_state);
          return std::nullopt;
        })
    | then(
        [](std::shared_ptr<wal_file_entry> wf,
            [[maybe_unused]] const dir& d,
            [[maybe_unused]] const std::filesystem::path& name,
            [[maybe_unused]] std::uint_fast64_t sequence) {
          wf->state_ = wal_file_entry_state::opening;
          std::error_code ec;
          wf->file.create(d, name, ec);
          return std::make_tuple(ec, wf, name, sequence);
        })
    | explode_tuple()
    | validation(
        [](std::error_code ec, [[maybe_unused]] const std::shared_ptr<wal_file_entry>& wf, [[maybe_unused]] const std::filesystem::path& name, [[maybe_unused]] std::uint_fast64_t sequence) -> std::optional<std::error_code> {
          if (ec) return ec;
          return std::nullopt;
        })
    | then(
        []([[maybe_unused]] std::error_code ec, std::shared_ptr<wal_file_entry> wf, const std::filesystem::path& name, std::uint_fast64_t sequence) {
          return std::make_tuple(wf, name, sequence);
        })
    | explode_tuple()
    | validation(
        [](const std::shared_ptr<wal_file_entry>& wf, [[maybe_unused]] const std::filesystem::path& name, [[maybe_unused]] std::uint_fast64_t sequence) -> std::optional<std::error_code> {
          std::error_code ec;
          const bool was_locked = wf->file.ftrylock(ec);
          if (ec) return ec;
          if (!was_locked) return make_error_code(wal_errc::unable_to_lock);
          return std::nullopt;
        })
    | then(
        [](std::shared_ptr<wal_file_entry> wf, std::filesystem::path name, std::uint_fast64_t sequence) {
          wf->name = name;
          wf->version = max_version;
          wf->sequence = sequence;
          return wf;
        })
    | let_value(
        [](std::shared_ptr<wal_file_entry>& wf) -> sender_of<std::shared_ptr<wal_file_entry>> auto {
          return xdr_v2::write(pos_stream(wf->file), *wf, xdr_header_invocation{})
          | then(
              [&wf](pos_stream&& stream) -> pos_stream&& {
                wf->link_offset_ = stream.position();
                return std::move(stream);
              })
          | xdr_v2::write(xdr_v2::no_fd, write_variant_type(), xdr_v2::constant(xdr_v2::identity))
          | then(
              [&wf](pos_stream&& stream) -> std::shared_ptr<wal_file_entry> {
                wf->write_offset_ = stream.position();
                return std::move(wf);
              });
        })
    | let_value(
        [](std::shared_ptr<wal_file_entry>& wf) -> sender_of<std::shared_ptr<wal_file_entry>> auto {
          return wf->wal_flusher_.lazy_flush(false)
          | then(
              [&wf]() -> std::shared_ptr<wal_file_entry>&& {
                return std::move(wf);
              });
        })
    | then(
        [](std::shared_ptr<wal_file_entry> wf) {
          wf->state_ = wal_file_entry_state::ready;
          return wf;
        });
  }

  template<typename CompletionToken>
  [[deprecated]]
  auto async_create(const dir& d, const std::filesystem::path& name, std::uint_fast64_t sequence, CompletionToken&& token) {
    using namespace execution;

    return asio::async_initiate<CompletionToken, void(std::error_code, link_done_event_type)>(
        [](auto completion_handler, std::shared_ptr<wal_file_entry> wf, const dir& d, const std::filesystem::path& name, std::uint_fast64_t sequence) -> void {
          start_detached(
              wf->async_create(d, name, sequence)
              | then(
                  [](std::shared_ptr<wal_file_entry> wf) -> std::tuple<std::error_code, link_done_event_type> {
                    return std::make_tuple(std::error_code{}, wf->link_done_event_);
                  })
              | upon_error(
                  [link_done_event=wf->link_done_event_](auto err) -> std::tuple<std::error_code, link_done_event_type> {
                    if constexpr(std::same_as<std::exception_ptr, decltype(err)>)
                      std::rethrow_exception(err);
                    else
                      return std::make_tuple(err, link_done_event);
                  })
              | explode_tuple()
              | then(completion_handler_fun(std::move(completion_handler), wf->get_executor())));
        },
        token, this->shared_from_this(), d, name, sequence);
  }

  template<typename Range, typename CompletionToken, typename TransactionValidator = no_transaction_validation>
#if __cpp_concepts >= 201907L
  requires std::ranges::input_range<std::remove_reference_t<Range>>
#endif
  auto async_append(Range&& records, CompletionToken&& token, TransactionValidator&& transaction_validator = no_transaction_validation(), move_only_function<void(records_vector)> on_successful_write_callback = move_only_function<void(records_vector)>(), bool delay_flush = false);

  template<typename CompletionToken, typename TransactionValidator = no_transaction_validation>
  auto async_append(write_records_vector records, CompletionToken&& token, TransactionValidator&& transaction_validator = no_transaction_validation(), move_only_function<void(records_vector)> on_successful_write_callback = move_only_function<void(records_vector)>(), bool delay_flush = false);

  private:
  template<typename OnSpaceAssigned, typename TransactionValidator>
  auto async_append_impl_(write_records_vector records, move_only_function<void(std::error_code)>&& completion_handler, OnSpaceAssigned&& on_space_assigned, TransactionValidator&& transaction_validator, move_only_function<void(records_vector)> on_successful_write_callback, bool delay_flush = false) -> void;

  public:
  template<typename CompletionToken>
  auto async_seal(CompletionToken&& token);

  template<typename CompletionToken>
  auto async_discard_all(CompletionToken&& token);

  auto write_offset() const noexcept -> typename fd_type::offset_type;
  auto link_offset() const noexcept -> typename fd_type::offset_type;
  auto has_unlinked_data() const -> bool;

  template<typename Acceptor, typename CompletionToken>
  auto async_records(Acceptor&& acceptor, CompletionToken&& token) const;

  template<typename CompletionToken>
  [[deprecated]]
  auto async_records(CompletionToken&& token) const;

  private:
  template<typename StreamAllocator>
  class async_records_reader
  : private buffered_readstream_adapter<positional_stream_adapter<const fd_type&>, StreamAllocator>
  {
    private:
    static constexpr auto begin_offset = std::remove_cvref_t<decltype(std::declval<wal_file_entry>().header_reader_())>::bytes.value();
    using parent_type = buffered_readstream_adapter<positional_stream_adapter<const fd_type&>, StreamAllocator>;

    public:
    using offset_type = typename parent_type::offset_type;
    using executor_type = typename parent_type::executor_type;

    explicit async_records_reader(std::shared_ptr<const wal_file_entry> wf)
    : parent_type(typename parent_type::next_layer_type(wf->file, begin_offset)),
      wf_(std::move(wf))
    {}

    explicit async_records_reader(std::shared_ptr<const wal_file_entry> wf, StreamAllocator alloc)
    : parent_type(typename parent_type::next_layer_type(wf->file, begin_offset), alloc),
      wf_(std::move(wf))
    {}

    using parent_type::position;
    using parent_type::get_executor;
    using parent_type::skip;

    template<typename MutableBufferSequence, typename CompletionToken>
    auto async_read_some(const MutableBufferSequence& buffers, CompletionToken&& token) {
      return asio::async_initiate<CompletionToken, void(std::error_code, std::size_t)>(
          [this](auto completion_handler, auto buffers) {
            asio::dispatch(
                completion_wrapper<void()>(
                    completion_handler_fun(std::move(completion_handler), wf_->get_executor(), wf_->strand_),
                    [this, buffers](auto handler) mutable -> void {
                      assert(wf_->strand_.running_in_this_thread());

                      auto next = std::upper_bound(wf_->unwritten_links_.cbegin(), wf_->unwritten_links_.cend(), this->position(),
                          overload(
                              [](offset_type x, const unwritten_link& y) {
                                return x < y.offset + y.data.size();
                              },
                              [](const unwritten_link& x, offset_type y) {
                                return x.offset + x.data.size() < y;
                              }));

                      if (next == wf_->unwritten_links_.cend()) {
                        this->parent_type::async_read_some(std::move(buffers), std::move(handler).inner_handler());
                        return;
                      }

                      assert(next->offset + next->data.size() > this->position());
                      if (next->offset <= this->position()) {
                        asio::const_buffer source_buffer = asio::buffer(next->data) + (this->position() - next->offset);
                        auto sz = asio::buffer_copy(buffers, source_buffer);
                        this->skip(sz);
                        std::invoke(std::move(handler), std::error_code{}, sz);
                        return;
                      }

                      auto next_avail = next->offset - this->position();
                      auto buf_end_iter = buffers.begin();
                      while (next_avail > 0 && buf_end_iter != buffers.end()) {
                        *buf_end_iter = asio::buffer(*buf_end_iter, next_avail);
                        next_avail -= buf_end_iter->size();
                        ++buf_end_iter;
                      }
                      buffers.erase(buf_end_iter, buffers.end());
                      assert(buffer_size(buffers) <= next_avail);
                      this->parent_type::async_read_some(std::move(buffers), std::move(handler).inner_handler());
                    }));
          },
          token, std::vector<asio::mutable_buffer>(asio::buffer_sequence_begin(buffers), asio::buffer_sequence_end(buffers)));
    }

    private:
    std::shared_ptr<const wal_file_entry> wf_;
  };

  template<typename HandlerAllocator>
  auto async_records_impl_(move_only_function<std::error_code(variant_type)> acceptor, move_only_function<void(std::error_code)> completion_handler, HandlerAllocator handler_alloc) const;

  template<typename CompletionToken, typename OnSpaceAssigned, typename TransactionValidator>
  auto append_bytes_(std::vector<std::byte, rebind_alloc<std::byte>>&& bytes, CompletionToken&& token, OnSpaceAssigned&& space_assigned_event, std::error_code ec, wal_file_entry_state expected_state, TransactionValidator&& transaction_validator, move_only_function<void(records_vector)> on_successful_write_callback, write_record_with_offset_vector records, bool delay_flush = false);

  template<typename CompletionToken, typename Barrier, typename Fanout, typename DeferredTransactionValidator>
  auto append_bytes_at_(
      typename fd_type::offset_type write_offset,
      std::vector<std::byte, rebind_alloc<std::byte>>&& bytes,
      CompletionToken&& token,
      Barrier&& barrier, Fanout&& f,
      DeferredTransactionValidator&& deferred_transaction_validator,
      std::shared_ptr<records_vector> converted_records,
      bool delay_flush = false);

  template<typename CompletionToken>
  auto write_skip_record_(
      typename fd_type::offset_type write_offset,
      std::size_t bytes, CompletionToken&& token);

  template<typename CompletionToken>
  auto write_link_(
      typename fd_type::offset_type write_offset,
      std::array<std::byte, 4> bytes, CompletionToken&& token,
      bool delay_flush = false);

  public:
  std::filesystem::path name;
  fd_type file;
  std::uint_fast32_t version;
  std::uint_fast64_t sequence;

  private:
  allocator_type alloc_;
  typename fd_type::offset_type write_offset_;
  typename fd_type::offset_type link_offset_;
  typename fd_type::offset_type must_flush_offset_ = 0;
  asio::strand<executor_type> strand_;
  wal_file_entry_state state_ = wal_file_entry_state::uninitialized;
  link_done_event_type link_done_event_;
  wal_flusher<fd_type&, allocator_type> wal_flusher_;
  unwritten_link_vector unwritten_links_;

  // Seal barrier becomes ready when:
  // 1. all pending writes have their space allocated.
  // 2. a seal has been requested.
  fanout_barrier<executor_type, allocator_type> seal_barrier_;

  const std::shared_ptr<spdlog::logger> logger = get_wal_logger();
};

template<typename Executor, typename Allocator>
struct wal_file_entry<Executor, Allocator>::xdr_header_invocation {
  private:
  static constexpr auto magic() noexcept -> std::array<char, 13> {
    return std::array<char, 13>{ '\013', '\013', 'e', 'a', 'r', 'n', 'e', 's', 't', '.', 'w', 'a', 'l' };
  }

  public:
  auto read(wal_file_entry& self) const {
    using namespace std::literals;

    return xdr_v2::constant.read(magic(), xdr_v2::fixed_byte_string)
    | xdr_v2::uint32.read(self.version)
    | xdr_v2::validation.read(
        [&self]() -> std::optional<std::error_code> {
          if (self.version <= max_version)
            return std::nullopt;
          else
            return make_error_code(wal_errc::bad_version);
        })
    | xdr_v2::uint64.read(self.sequence);
  }

  auto write(const wal_file_entry& self) const {
    using namespace std::literals;

    return xdr_v2::constant.write(magic(), xdr_v2::fixed_byte_string)
    | xdr_v2::validation.write(
        [&self]() -> std::optional<std::error_code> {
          if (self.version <= max_version)
            return std::nullopt;
          else
            return make_error_code(wal_errc::bad_version);
        })
    | xdr_v2::uint32.write(self.version)
    | xdr_v2::uint64.write(self.sequence);
  }
};


} /* namespace earnest::detail */

#include "wal_file_entry.ii"
