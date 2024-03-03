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
#include <asio/post.hpp>
#include <spdlog/spdlog.h>

#include <earnest/detail/asio_execution_scheduler.h>
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
#include <earnest/execution.h>
#include <earnest/execution_util.h>
#include <earnest/fd.h>
#include <earnest/strand.h>
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

inline auto operator<<(std::ostream& out, wal_file_entry_state state) -> std::ostream& {
  using namespace std::literals;

  switch (state) {
    default:
      out << "wal_file_entry_state{"sv << static_cast<std::underlying_type_t<wal_file_entry_state>>(state) << "}"sv;
      break;
    case wal_file_entry_state::uninitialized:
      out << "wal_file_entry_state{uninitialized}"sv;
      break;
    case wal_file_entry_state::opening:
      out << "wal_file_entry_state{opening}"sv;
      break;
    case wal_file_entry_state::ready:
      out << "wal_file_entry_state{ready}"sv;
      break;
    case wal_file_entry_state::sealing:
      out << "wal_file_entry_state{sealing}"sv;
      break;
    case wal_file_entry_state::sealed:
      out << "wal_file_entry_state{sealed}"sv;
      break;
    case wal_file_entry_state::failed:
      out << "wal_file_entry_state{failed}"sv;
      break;
  }
  return out;
}


struct no_transaction_validation {};


// Buffers both records and their on-disk representation.
//
// The records vector has the offset attached.
template<typename Executor, typename Allocator = std::allocator<std::byte>>
class write_records_buffer {
  private:
  template<typename T>
  using allocator_for = typename std::allocator_traits<Allocator>::template rebind_alloc<T>;

  struct xdr_invocation_;

  public:
  using variant_type = wal_record_variant<Executor>;
  using write_variant_type = record_write_type_t<variant_type>;
  using executor_type = Executor;
  using allocator_type = allocator_for<std::byte>;
  using byte_vector = std::vector<std::byte, allocator_for<std::byte>>;

  private:
  using byte_stream = byte_stream<Executor, allocator_for<std::byte>>;

  public:
  struct write_record_with_offset {
    write_variant_type record;
    std::uint64_t offset;
  };
  using write_record_with_offset_vector = std::vector<write_record_with_offset, allocator_for<write_record_with_offset>>;

  explicit write_records_buffer(executor_type ex, allocator_type alloc = allocator_type())
  : bytes_(ex, alloc),
    records_(alloc)
  {}

  explicit write_records_buffer(std::initializer_list<write_variant_type> il, executor_type ex, allocator_type alloc = allocator_type())
  : write_records_buffer(ex, std::move(alloc))
  {
    std::for_each(il.begin(), il.end(),
        [this](write_variant_type v) {
          this->push_back(v);
        });
  }

  auto get_allocator() const -> allocator_type {
    return bytes_.get_allocator();
  }

  template<typename Record>
  auto push_back(const Record& v) -> void {
    execution::sync_wait(
        xdr_v2::write(std::ref(bytes_), v, xdr_invocation_(records_))
        | execution::io::ec_to_exception());
  }

  template<std::ranges::input_range Range>
  auto push_back_range(Range&& range) -> void {
    execution::sync_wait(
        xdr_v2::write(std::ref(bytes_), std::forward<Range>(range), xdr_v2::fixed_collection(xdr_invocation_(records_)))
        | execution::io::ec_to_exception());
  }

  auto bytes() const & noexcept -> const byte_vector& {
    return bytes_.data();
  }

  auto bytes() & noexcept -> byte_vector& {
    return bytes_.data();
  }

  auto bytes() && noexcept -> byte_vector&& {
    return std::move(bytes_).data();
  }

  auto write_records_with_offset() const & noexcept -> const write_record_with_offset_vector& {
    return records_;
  }

  auto write_records_with_offset() & noexcept -> write_record_with_offset_vector& {
    return records_;
  }

  auto write_records_with_offset() && noexcept -> write_record_with_offset_vector&& {
    return std::move(records_);
  }

  auto converted_records(gsl::not_null<std::shared_ptr<const earnest::fd<Executor>>> fd, execution::io::offset_type offset) const
  -> std::vector<variant_type, allocator_for<variant_type>> {
    std::vector<variant_type, allocator_for<variant_type>> converted_records(get_allocator());
    converted_records.reserve(write_records_with_offset().size());

    std::ranges::copy(
        write_records_with_offset()
        | std::ranges::views::transform(
            [&](const auto& r) {
              wal_record_write_to_read_converter<earnest::fd<Executor>, variant_type> convert(fd, offset + r.offset);
              return convert(r.record);
            }),
        std::back_inserter(converted_records));

    return converted_records;
  }

  private:
  byte_stream bytes_;
  write_record_with_offset_vector records_;
};


// Write record operation for the write-records-buffer.
//
// For each record that is written, it records the offset and the converted record.
template<typename Executor, typename Allocator>
struct write_records_buffer<Executor, Allocator>::xdr_invocation_ {
  explicit xdr_invocation_(write_record_with_offset_vector& records)
  : wrwo_vec(records)
  {}

  auto write(const write_variant_type& r) const {
    return xdr_v2::manual.write(
        [&r, self=*this](auto& stream) {
          self.update_wrwo_vec(stream, r);
          return execution::just(std::move(stream));
        })
    | xdr_v2::identity.write(r);
  }

  template<typename Record>
  requires requires {
    { std::bool_constant<(Record::opcode, true)>{} }; // Must have an opcode.
  }
  auto write(const Record& r) const {
    return xdr_v2::manual.write(
        [&r, self=*this](auto& stream) {
          self.update_wrwo_vec(stream, r);
          return execution::just(std::move(stream));
        })
    | xdr_v2::constant.write(Record::opcode, xdr_v2::uint32)
    | xdr_v2::identity.write(r);
  }

  private:
  auto update_wrwo_vec(const byte_stream& stream, const write_variant_type& r) const -> void {
    wrwo_vec.push_back({
          .record=r,
          .offset=stream.data().size(),
        });
  }

  write_record_with_offset_vector& wrwo_vec;
};


static auto make_link_sender(execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>> input) -> execution::sender_of<> auto {
  using namespace execution;

  // We _must_ start the chain:
  // it'll contain shared-pointer to this.
  // And the link-sender will be installed on this.
  // So in order to ensure the links don't start keeping this alive, we must immediately start the operation, and chain-break it.
  return split(ensure_started(std::move(input) | chain_breaker()));
}

using link_sender = decltype(make_link_sender(std::declval<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>>()));


// A single WAL file.
//
// This class manages the read and write operations.
// - any read is atomic
// - any write is append-only and atomic
//
// The invariant is that at the end of the file, holds a wal_record_end_of_records.
// Note that the end-of-file is tracked with end_offset, not with the file-size.
template<typename Executor, typename Allocator>
class wal_file_entry_file {
  public:
  using executor_type = Executor;
  using allocator_type = Allocator;
  using offset_type = execution::io::offset_type;

  using callback = earnest::move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>, true>()>;

  private:
  template<typename T> using allocator_for = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;
  using fd_type = fd<executor_type>;
  using commit_space_late_binding_t = execution::late_binding_t<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>;

  public:
  class prepared_space {
    public:
    prepared_space(
        gsl::not_null<std::shared_ptr<wal_file_entry_file>> self,
        offset_type write_offset,
        std::size_t write_len,
        commit_space_late_binding_t::acceptor acceptor,
        link_sender preceding_link, link_sender new_link)
    : self(self.get()),
      write_offset(write_offset),
      write_len(write_len),
      acceptor(std::move(acceptor)),
      preceding_link(std::move(preceding_link)),
      new_link(std::move(new_link))
    {}

    ~prepared_space() {
      if (must_succeed && acceptor) {
        try {
          throw std::system_error(wal_errc::unrecoverable, "must-succeed write never happened -- WAL state and in-memory state are now unreconcilable");
        } catch (...) {
          std::move(acceptor).assign_error(std::current_exception());
        }
      }
    }

    prepared_space(const prepared_space&) = delete;
    prepared_space(prepared_space&&) noexcept = default;

    auto offset() const noexcept -> offset_type {
      return write_offset;
    }

    auto size() const noexcept -> std::size_t {
      return write_len;
    }

    // must_succeed: if set, this indicates that the write mustn't fail.
    // If the write does fail, it'll be treated as a non-recoverable error.
    // Writes can still be canceled.
    auto set_must_succeed(bool must_succeed) -> void {
      this->must_succeed = must_succeed;
    }

    // commit_check: an optional function that allows for a check to commit or cancel the write.
    // The commit check will be executed when all preceding writes have been written to durable storage,
    // and will not allow subsequent writes to be written to durable storage until the operation completes.
    // Must be nullptr if not needed.
    auto set_commit_check(callback&& commit_check) -> void {
      this->commit_check = std::move(commit_check);
    }

    // update_callback: this callback will be executed after this write and all preceding writes have been written
    // to durable storage. Update-callbacks will be executed in order-of-write.
    auto set_update_callback(callback&& update_callback) -> void {
      this->update_callback = std::move(update_callback);
    }

    // This performs an atomic write of some bytes of data.
    // The data to-be-written must be at least 4 bytes.
    //
    // - data: the data that is to be written.
    // - delay_flush: allows for flush-to-disk to be delayed, in order to combine more writes into a single flush operation.
    auto write(std::span<const std::byte> data, bool delay_flush = false) &&
    -> execution::sender_of<> auto {
      using namespace execution;

      if (data.size() != write_len) throw std::range_error("data size does not match reserved size");
      return std::move(*this).write_(data, delay_flush)
      | chain_breaker(); // Now we can release any references to this.
                         // Also means we won't hang on to the acceptor granted us by commit_space_,
                         // which is of critical importance, because releasing it will ensure
                         // the recovery-path can start (if that is needed).
    }

    private:
    // Implementation for write.
    // Must be run with a scheduler wrapped by a strand_.
    //
    // The operation does:
    //
    // [ preceding link ]                                              [ prepared-space ]
    //         |                                                               |
    //         |                                                               V
    //         |                                                       [ stage-1 write ]
    //         |                                                               |
    //         |                                                               V
    //         |                                                   [ stage-1 flush-to-disk ]
    //         |                                                               |
    //         |                                                               V
    //         |`------------------> [ commit-check ] ---------------> [ stage-2 write ]
    //         |                   (omitted if nullptr)                        |
    //         |                                                               V
    //         |                                                   [ stage-2 flush-to-disk ]
    //         |                                                               |
    //         |                                                               V
    //         |`---------------------------------------------------> [ update callback ]
    //         |                                                               |
    //         |    ,----------------------------------------------------------'
    //         |    |
    //         V    V
    // [ notify commit-space ]
    // [    acceptor         ]
    //         |
    //         V
    // [    next link   ] ------------------------------------------> [ returned sender ]
    //
    // The "notify commit-space acceptor" is done by completing the acceptor returned by commit-space.
    // This code handles updating the link-offset.
    // If the acceptor isn't completed, it'll kick off the recovery code in the background.
    auto write_(std::span<const std::byte> data, bool delay_flush = false) && -> execution::sender_of<> auto {
      using namespace execution;

      if (data.size() != write_len) {
        try {
          throw std::range_error("data size does not match reserved size");
        } catch (...) {
          if (must_succeed) std::move(acceptor).assign_error(std::current_exception());
          throw;
        }
      }

      return just(std::move(*this), data, delay_flush)
      | let_value(
          [](prepared_space& space, std::span<const std::byte> data, bool delay_flush) {
            if (!delay_flush && space.self->sync_offset < space.write_offset) space.self->sync_offset = space.write_offset; // Never throws

            // We wrap the commit-check here.
            // The reason is that, if the check is required, we need to sync on the previous link before running it.
            // But that also means the commit-check creates a bottle-neck, reducing our ability to pipeline
            // (all preceding transactions must complete, prior to our check).
            //
            // If there is no check (commit_check == nullptr), we can skip the check, and thus don't need to wait
            // for the preceding transactions to complete either.
            auto transaction_commit = just()
            | let_variant(
                [&space]() {
                  auto do_the_check = [&]() {
                    return space.preceding_link
                    | let_value(std::ref(space.commit_check));
                  };
                  auto skip_the_check = []() {
                    return just();
                  };
                  using variant_type = std::variant<decltype(do_the_check()), decltype(skip_the_check())>;

                  if (space.commit_check == nullptr)
                    return variant_type(skip_the_check());
                  else
                    return variant_type(do_the_check());
                });

            // We do the write in two stages.
            //
            // This is the first stage:
            // we write everything, except the first 4 bytes.
            // By skipping those bytes, they'll be zeroed (meaning they'll read as wal_record_end_of_records).
            //
            // The first stage, since it never results in observable effect prior to completing the link,
            // we can start immediately.
            auto first_stage = io::write_at_ec(space.self->file, space.write_offset + 4u, data.subspan(4))
            | let_value(
                // After the first write, we flush-to-disk the written data.
                [&space, delay_flush]([[maybe_unused]] std::size_t wlen) {
                  return space.self->wal_flusher_.lazy_flush(true, space.self->delay_flush(space.write_offset, delay_flush));
                });

            // Second stage of the write, we can only start if the transaction is allowed to succeed.
            auto second_stage = when_all(std::move(transaction_commit), std::move(first_stage))
            | let_value(
                // Now that the new data is guaranteed on-disk, we can install the link.
                [ link_data=std::span<const std::byte, 4>(std::span<const std::byte>(data).subspan(0, 4)),
                  &space
                ]() {
                  return io::write_at_ec(space.self->file, space.write_offset, link_data);
                })
            | let_value(
                // Now that the link is written, we need to flush that to disk.
                [&space, delay_flush]([[maybe_unused]] std::size_t num_bytes_written) {
                  return space.self->wal_flusher_.lazy_flush(true, space.self->delay_flush(space.write_offset, delay_flush));
                });

            return std::move(second_stage)
            | let_variant(
                // If an update is requested, we'll link it in.
                // The update:
                // - must wait for the previous links to all be in place
                // - must block successive link from becoming ready, until it completes
                // Note that if the update fails, the write will be reverted.
                [&space]() mutable {
                  auto do_the_update = [&]() {
                    return std::move(space.preceding_link)
                    | let_value(std::ref(space.update_callback));
                  };
                  auto nothing_to_update = []() {
                    return just();
                  };
                  using variant_type = std::variant<decltype(do_the_update()), decltype(nothing_to_update())>;

                  if (space.update_callback == nullptr)
                    return variant_type(nothing_to_update());
                  else
                    return variant_type(do_the_update());
                })
            | observe_value(
                // Now that everything has been written, we notify the acceptor
                // (for the previously commited space) of our successful completion.
                // If the operation doesn't succeed, or is never started, the cancelation
                // caused by the destruction of the acceptor, will trip recovery procedure.
                [&space]() {
                  space.acceptor.assign_values();
                })
            | observe_error(
                [&space](auto error) noexcept {
                  // If we're not allowed to fail, then we need to propagate the error to the next link.
                  //
                  // Note that if we are allowed to fail, we simply let the acceptor fall out of scope.
                  // And it's destructor will cause the sender to be canceled.
                  // (Cancelation then initiates the recovery process.)
                  if (space.must_succeed) {
                    try {
                      // First, we need to rethrow the error,
                      // so our catch block can create a throw_with_nested error.
                      if constexpr(std::is_same_v<decltype(error), std::exception_ptr>)
                        std::rethrow_exception(std::move(error));
                      else
                        throw std::system_error(std::move(error));
                    } catch (...) {
                      // Now that our error is throw-catch, we can use throw_with_nested
                      // to wrap the error in a message indicating the pipeline is corrupted.
                      try {
                        // throw_with_nested will wrap our current exception inside a nested_exception.
                        // And then throw it.
                        // So we require yet another catch block to capture it.
                        std::throw_with_nested(std::system_error(wal_errc::unrecoverable, "must-succeed write failed -- WAL state and in-memory state are now unreconcilable"));
                      } catch (...) {
                        // Now we have a wrapped exception, that's suitable for propagation.
                        std::move(space.acceptor).assign_error(std::current_exception()); // Never throws
                      }
                    }
                  }
                })
            | let_value(
                // The whole operation is only completed, once the link is completed.
                [&space]() {
                  return std::move(space.new_link);
                });
          });
    }

    std::shared_ptr<wal_file_entry_file> self;
    offset_type write_offset;
    std::size_t write_len;
    commit_space_late_binding_t::acceptor acceptor;
    link_sender preceding_link, new_link;
    callback commit_check = nullptr;
    callback update_callback = nullptr;
    bool must_succeed = false;
  };

  static_assert(std::is_move_constructible_v<prepared_space>);

  wal_file_entry_file(
      fd_type&& file, offset_type end_offset,
      link_sender initial_link,
      allocator_type alloc = allocator_type())
  : file(std::move(file)),
    end_offset(end_offset),
    sync_offset(end_offset - 4u),
    link_offset(end_offset - 4u),
    wal_flusher_(this->file, alloc),
    link(std::move(initial_link))
  {
    if (end_offset < 4) throw std::range_error("bug: wal file end-offset too low");
  }

  wal_file_entry_file(
      fd_type&& file, offset_type end_offset,
      allocator_type alloc = allocator_type())
  : wal_file_entry_file(std::move(file), end_offset, execution::just(), alloc)
  {}

  auto close() -> void {
    file.close();
  }

  auto is_open() const noexcept -> bool {
    return file.is_open();
  }

  auto get_end_offset() const noexcept -> offset_type { return end_offset; }
  auto get_link_offset() const noexcept -> offset_type { return link_offset; }

  private:
  template<typename T>
  auto recovery_record_bytes_(T record) -> auto {
    auto [stream] = execution::sync_wait(
        xdr_v2::write(
            byte_stream<Executor>(file.get_executor()),
            std::make_tuple(T::opcode, record),
            xdr_v2::constant(xdr_v2::tuple(xdr_v2::uint32, xdr_v2::identity)))
    ).value();
    return std::move(stream).data();
  }

  // XXX max size of the returned buffer is 12 bytes,
  // so it would be nice to have a non-allocating vector
  // that has space for up to 12 bytes.
  auto recovery_record_(std::size_t write_len) -> auto {
    assert(write_len >= 4u);
    if (write_len == 4u) {
      return recovery_record_bytes_(wal_record_noop{});
    } else if (write_len - 8u <= 0xffff'ffffu) {
      assert(write_len >= 8u);
      return recovery_record_bytes_(wal_record_skip32{static_cast<std::uint32_t>(write_len - 8u)});
    } else {
      assert(write_len - 12u <= 0xffff'ffff'ffff'ffffull);
      assert(write_len >= 12u);
      return recovery_record_bytes_(wal_record_skip64{static_cast<std::uint64_t>(write_len - 12u)});
    }
  }

  public:
  // Reserve space for a write operation.
  //
  // When this sender completes, the space allocated will be used.
  // If the prepared-space isn't written to (or the write fails),
  // a skip record will be placed.
  //
  // Note that, when this function returns, the space allocated will be used.
  // If the acceptor isn't completed successfully, a recovery-operation will write
  // a skip record to skip the space when reading it back.
  static auto prepare_space(gsl::not_null<std::shared_ptr<wal_file_entry_file>> self, std::size_t write_len) -> prepared_space {
    using namespace execution;

    if (write_len < 4) throw std::range_error("cannot write data of less than 4 bytes");
    if (write_len % 4u != 0) throw std::range_error("expect data to be a multiple of 4 bytes");

    offset_type write_offset = self->end_offset - 4u;
    offset_type new_end_offset = self->end_offset + write_len;
    offset_type new_link_offset = write_offset + write_len;

    // This recovery-chain writes the appropriate skip-record.
    // It's only invoked when the actual write failed.
    auto make_recovery_chain = [self, write_offset, write_len]([[maybe_unused]] const auto&...) {
      return just(self, write_offset, write_len)
      | then(
          [](gsl::not_null<std::shared_ptr<wal_file_entry_file>> self, offset_type write_offset, std::size_t write_len) {
            auto data = self->recovery_record_(write_len);
            assert(data.size() <= write_len); // It must fit.
            return std::make_tuple(self, write_offset, data, write_len);
          })
      | explode_tuple()
      | let_value(
          [](const gsl::not_null<std::shared_ptr<wal_file_entry_file>>& shared_self, offset_type write_offset, std::span<const std::byte> data, std::size_t write_len) {
            // We store the non-shared-pointer version, because it seems wasteful to install many copies of shared_self,
            // when the let_value-caller will guarantee its liveness anyway.
            const gsl::not_null<wal_file_entry_file*> self = shared_self.get().get();

            // Just like in regular writes, we write the data in two phases.
            // First we write everything except the first four bytes.
            // After flushing those to disk, we write the first four bytes (which will link the data together).
            return io::write_at_ec(self->file, write_offset + 4u, data.subspan(4))
            | let_value(
                // After the first write, we flush-to-disk the written data.
                [self]([[maybe_unused]] std::size_t wlen) {
                  return self->wal_flusher_.lazy_flush(true);
                })
            | let_value(
                // Now that the skip record's data is guaranteed on disk, we can write the link.
                [ link_span=std::span<const std::byte, 4>(data.subspan(0, 4)),
                  self, write_offset
                ]() {
                  return io::write_at_ec(self->file, write_offset, link_span);
                })
            | let_value(
                // After the second write, we flush-to-disk the written data.
                [self]([[maybe_unused]] std::size_t wlen) {
                  return self->wal_flusher_.lazy_flush(true);
                });
          });
    };

    auto [acceptor, sender] = commit_space_late_binding_t{}();

    auto self_link = self->link;
    auto link_completion = when_all(
        std::move(self_link), // Our link is not actually reachable, unless all preceding links have also been written.
        std::move(sender) | let_done(make_recovery_chain))
    | then(
        [self, new_link_offset, write_offset]() -> void {
          assert(self->link_offset == write_offset);
          self->link_offset = new_link_offset;
        })
    | chain_breaker(); // needed to break all those shared-pointers in the chain
                       // and also because we have a reference to the previous write (via this->link),
                       // and don't want that to be lingering forever.

    self->file.truncate(new_end_offset); // Now we change the file size. This can fail.
                                         // Failure is fine: we've not commited to anything yet.
    link_sender new_link = make_link_sender(std::move(link_completion)); // This can fail.
                                                                         // Failure is fine: we've merely grown the file, and this class
                                                                         // is designed to operate correctly even if the file has been grown.
    link_sender preceding_link = std::exchange(self->link, new_link); // Must not throw. XXX confirm that it doesn't throw
    self->end_offset = new_end_offset; // Never throws
    return prepared_space(self, write_offset, write_len, std::move(acceptor), preceding_link, new_link);
  }

  static auto read_some_at(gsl::not_null<std::shared_ptr<const wal_file_entry_file>> self, offset_type offset, std::span<std::byte> buf) -> execution::sender_of<std::size_t> auto {
    using namespace execution;

    return just(self, offset, buf)
    | lazy_validation(
        [](gsl::not_null<std::shared_ptr<const wal_file_entry_file>> self, offset_type offset, std::span<std::byte> buf) -> std::optional<std::error_code> {
          if (offset >= self->link_offset && !buf.empty())
            return make_error_code(execution::io::errc::eof);
          else
            return std::nullopt;
        })
    | lazy_let_value(
        [](gsl::not_null<std::shared_ptr<const wal_file_entry_file>> self, offset_type offset, std::span<std::byte> buf) {
          if (buf.size() > self->end_offset - offset) buf = buf.subspan(0, self->end_offset - offset);
          return io::read_some_at_ec(self->file, offset, buf);
        });
  }

  auto file_ref() const & -> const fd_type& {
    return file;
  }

  auto file_ref() & -> fd_type& {
    return file;
  }

  private:
  auto delay_flush(offset_type write_offset, bool delay_flush_requested) const noexcept -> bool {
    return delay_flush_requested && (write_offset > sync_offset);
  }

  fd_type file;
  offset_type end_offset; // End of the file. New appends happen at end_offset-4.
  offset_type sync_offset; // Highest write-offset for which a sync is requested.
  offset_type link_offset; // The end of fully-written data.
  wal_flusher<fd_type&, allocator_type> wal_flusher_;
  link_sender link;
};


template<typename Allocator>
class wal_file_entry_unwritten_data {
  public:
  using allocator_type = Allocator;
  using offset_type = execution::io::offset_type;

  private:
  template<typename T> using allocator_for = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;
  using byte_span = std::span<const std::byte>;
  using byte_vector = std::vector<std::byte, allocator_for<std::byte>>;

  struct record {
    record() = default;

    record(offset_type offset, byte_vector bytes)
    : offset(offset),
      bytes(std::move(bytes))
    {}

    // Moveable only, because we require that the byte_vector data remains at a constant memory address.
    record(const record&) = delete;
    record(record&&) = default;
    record& operator=(const record&) = delete;
    record& operator=(record&&) = default;

    auto end_offset() const noexcept -> offset_type {
      return offset + bytes.size();
    }

    offset_type offset;
    byte_vector bytes;
  };
  using record_vector = std::vector<record, allocator_for<record>>;

  // Less-compare that checks offsets.
  struct offset_compare {
    auto operator()(offset_type x, offset_type y) const noexcept -> bool {
      return x < y;
    }

    auto operator()(const record& x, const record& y) const noexcept -> bool {
      return (*this)(x.offset, y.offset);
    }

    auto operator()(offset_type x, const record& y) const noexcept -> bool {
      return (*this)(x, y.offset);
    }

    auto operator()(const record& x, offset_type y) const noexcept -> bool {
      return (*this)(x.offset, y);
    }
  };

  // End-offset-compare.
  // Compares an offset to the end of a record.
  struct search_compare {
    auto operator()(offset_type x, const record& y) const noexcept -> bool {
      return x < y.end_offset();
    }

    auto operator()(const record& x, offset_type y) const noexcept -> bool {
      return x.end_offset() <= y;
    }
  };

  public:
  explicit wal_file_entry_unwritten_data(allocator_type alloc = allocator_type())
  : records(alloc)
  {
    assert(invariant());
  }

  auto add(offset_type offset, byte_vector bytes) -> byte_span {
    if (bytes.empty()) throw std::range_error("no data to record");

    typename record_vector::const_iterator insert_position;
    if (records.empty() || records.back().offset < offset)
      insert_position = records.end();
    else
      insert_position = std::upper_bound(records.cbegin(), records.cend(), offset, offset_compare{});

    // Check that we don't overlap our ranges.
    if (insert_position != records.begin()) {
      const record& prev = *std::prev(insert_position);
      if (!(prev.end_offset() <= offset))
        throw std::range_error("bug: data overlaps with preceding record");
    }
    if (insert_position != records.end() && !(offset + bytes.size() <= insert_position->offset))
      throw std::range_error("bug: data overlaps with successive record");

    auto inserted = records.emplace(insert_position, offset, bytes);
    assert(invariant());
    return inserted->bytes;
  }

  void remove(offset_type offset, byte_span bytes) {
    typename record_vector::const_iterator erase_position;
    if (!records.empty() && records.front().offset == offset) {
      erase_position = records.cbegin();
    } else {
      erase_position = std::lower_bound(records.cbegin(), records.cend(), offset, offset_compare{});
      if (erase_position->offset != offset) erase_position = records.cend();
    }

    // Check that this is indeed the record to be erased.
    if (erase_position == records.cend())
      throw std::range_error("record not found");
    if (erase_position->bytes.data() != bytes.data() || erase_position->bytes.size() != bytes.size())
      throw std::range_error("record data mismatch");

    records.erase(erase_position);
    assert(invariant());
  }

  // Returns a sender for a read operation.
  // Note: this function is non-lazy.
  //
  // - offset: the offset at which the read is to happen
  // - buf: the buffer into which the read data is to be copied
  // - overflow_read: a read function that'll be invoked, if there's no data available at the offset.
  //   Must return a sender_of<std::size_t>.
  template<std::invocable<offset_type, std::span<std::byte>> OverflowFn>
  auto read_some_at(offset_type offset, std::span<std::byte> buf, OverflowFn&& overflow_read) const -> execution::sender_of<std::size_t> auto {
    // Figure out the return types used.
    using self_type = decltype(execution::just(std::declval<std::size_t>()));
    using overflow_type = std::invoke_result_t<OverflowFn, offset_type, std::span<std::byte>>;
    constexpr bool same_types = std::is_same_v<self_type, overflow_type>;
    // Figure out the variant type.
    using variant_type = std::conditional_t<same_types, self_type, std::variant<self_type, overflow_type>>;

    // A small wrapper that'll turn an lvalue-reference into an rvalue-reference.
    auto move_variant_type = [](variant_type& v) -> variant_type&& { return std::move(v); };
    // A small wrapper that wraps the return-sender into a variant-sender.
    auto make_return_value = [move_variant_type](auto return_sender) -> execution::sender_of<std::size_t> auto {
      if constexpr(same_types) {
        return return_sender;
      } else {
        return execution::just(variant_type(std::move(return_sender)))
        | execution::let_variant(move_variant_type);
      }
    };

    // Find the element that ends after the offset.
    typename record_vector::const_iterator elem = std::upper_bound(records.cbegin(), records.cend(), offset, search_compare{});
    if (elem == records.end()) {
      // There are no more records at/after the offset.
      // So we forward all calls to the overflow reader.
      return make_return_value(std::invoke(std::forward<OverflowFn>(overflow_read), offset, buf));
    } else if (elem->offset <= offset) {
      // We have local data for the given offset.
      // So we'll handle it locally.
      assert(offset < elem->end_offset());

      // Clamp the read-range.
      std::span<std::byte> destination = buf;
      if (destination.size() > elem->end_offset() - offset)
        destination = destination.subspan(0, elem->end_offset() - offset);
      // Figure out the source range.
      // We make sure it's equal-length to the destination-range.
      auto origin_span = std::span<const std::byte>(elem->bytes).subspan(offset - elem->offset, destination.size());
      // Now copy the bytes.
      assert(origin_span.size() == destination.size());
      std::copy(origin_span.begin(), origin_span.end(), destination.begin());
      // Record that we managed to read data.
      return make_return_value(execution::just(destination.size()));
    } else {
      // We have an element after the read.
      // So we'll have to pass it on to the overflow read,
      // but we want to make sure the overflow read won't read
      // the data we are to provide.
      assert(offset < elem->offset);

      // Clamp the buf...
      if (buf.size() > elem->offset - offset)
        buf = buf.subspan(elem->offset - offset);
      // ... and then we let the overflow deal with it.
      return make_return_value(std::invoke(std::forward<OverflowFn>(overflow_read), offset, buf));
    }
  }

  private:
  auto invariant() const noexcept -> bool {
    // Confirm that none of the ranges is empty.
    if (std::any_of(records.cbegin(), records.cend(),
            [](const record& r) noexcept -> bool {
              return r.bytes.empty();
            }))
      return false;

    // Confirm the range is sorted.
    if (!std::is_sorted(records.cbegin(), records.cend(), offset_compare{}))
      return false;

    // Confirm that no two successive elements overlap.
    offset_type preceding_end_offset = 0;
    for (const record& r : records) {
      if (r.offset < preceding_end_offset)
        return false;
      preceding_end_offset = r.offset + r.bytes.size();
    }

    // Confirm none of the offset+bytes.size() overflows.
    for (const record& r : records) {
      if (std::numeric_limits<offset_type>::max() - r.offset < r.bytes.size())
        return false;
    }

    return true;
  }

  // All the records for unwritten data.
  // They're kept in ascending order.
  record_vector records;
};


template<typename Executor, typename Allocator>
class wal_file_entry
: public std::enable_shared_from_this<wal_file_entry<Executor, Allocator>>
{
  public:
  static inline constexpr std::uint_fast32_t max_version = 0;
  static inline constexpr std::size_t read_buffer_size = 2u * 1024u * 1024u;
  using executor_type = Executor;
  using allocator_type = Allocator;
  using link_done_event_type = execution::late_binding_t<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>::acceptor;
  using write_records_buffer_t = write_records_buffer<Executor, Allocator>;

  private:
  using link_done_event_sender = execution::late_binding_t<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>::sender;

  template<typename T>
  using rebind_alloc = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;

  using xdr_header_tuple = std::tuple<std::uint32_t /*version*/, std::uint64_t /*sequence*/>;
  struct xdr_header_invocation;

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

  struct unwritten_data {
    using allocator_type = rebind_alloc<std::byte>;

    template<std::ranges::range Bytes>
    requires std::convertible_to<std::ranges::range_reference_t<Bytes>, std::byte>
    unwritten_data(std::uint64_t offset, Bytes&& data, allocator_type alloc)
    : offset(offset),
      data(std::ranges::begin(data), std::ranges::end(data), std::move(alloc))
    {
      if (this->data.size() < 4u)
        throw std::range_error("bug: unwritten_data too short");
      if (this->data.size() % 4u)
        throw std::range_error("bug: unwritten_data length should be a multiple of 4"); // XDR format always uses multiples of 4-byte
    }

    std::uint64_t offset;
    std::vector<std::byte, allocator_type> data;
  };
  using unwritten_data_vector = std::vector<unwritten_data, rebind_alloc<unwritten_data>>;

  public:
  wal_file_entry(fd_type fd, std::filesystem::path name, xdr_header_tuple hdr, execution::io::offset_type end_offset, link_sender link, bool sealed, gsl::not_null<std::shared_ptr<spdlog::logger>> logger, allocator_type alloc)
  : name(name),
    version(std::get<0>(hdr)),
    sequence(std::get<1>(hdr)),
    alloc_(alloc),
    state_(sealed ? wal_file_entry_state::sealed : wal_file_entry_state::ready),
    file(std::move(fd), end_offset, link, alloc),
    unwritten_data(alloc),
    fake_link(link),
    strand_(alloc),
    logger(logger)
  {}

  wal_file_entry(const wal_file_entry&) = delete;
  wal_file_entry(wal_file_entry&&) = delete;
  wal_file_entry& operator=(const wal_file_entry&) = delete;
  wal_file_entry& operator=(wal_file_entry&&) = delete;

  auto get_executor() const -> executor_type { return file.get_executor(); }
  auto get_allocator() const -> allocator_type { return alloc_; }

  auto state() const noexcept -> wal_file_entry_state {
    std::lock_guard lck{strand_};
    return state_;
  }

  auto is_open() const noexcept -> bool {
    std::lock_guard lck{strand_};
    return file.is_open();
  }

  private:
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
  [[nodiscard]]
  static auto open(executor_type ex, dir d, std::filesystem::path name, allocator_type alloc)
  -> execution::type_erased_sender<
      std::variant<std::tuple<gsl::not_null<std::shared_ptr<wal_file_entry>>>>,
      std::variant<std::exception_ptr, std::error_code>,
      false> {
    using namespace execution;
    using pos_stream = positional_stream_adapter<fd_type&>;

    return just(ex, get_wal_logger(), d, name, alloc)
    | lazy_observe_value(
        [](
            [[maybe_unused]] const executor_type& ex,
            const gsl::not_null<std::shared_ptr<spdlog::logger>>& logger,
            [[maybe_unused]] const dir& d,
            const std::filesystem::path& name,
            [[maybe_unused]] const allocator_type& alloc) {
          logger->info("opening wal file {}", name.native());
        })
    | lazy_observe_value(
        [](
            [[maybe_unused]] const executor_type& ex,
            [[maybe_unused]] const gsl::not_null<std::shared_ptr<spdlog::logger>>& logger,
            [[maybe_unused]] const dir& d,
            const std::filesystem::path& name,
            [[maybe_unused]] const allocator_type& alloc) {
          if (name.has_parent_path()) throw std::runtime_error("wal file must be a filename");
        })
    | then(
        [](
            executor_type ex,
            gsl::not_null<std::shared_ptr<spdlog::logger>> logger,
            const dir& d,
            std::filesystem::path name,
            allocator_type alloc) {
          fd_type fd{ex};
          std::error_code ec;
          fd.open(d, name, fd_type::READ_WRITE, ec);
          return std::make_tuple(ec, logger, std::move(fd), name, alloc);
        })
    | explode_tuple()
    | validation(
        [](
            std::error_code ec,
            [[maybe_unused]] gsl::not_null<std::shared_ptr<spdlog::logger>> logger,
            [[maybe_unused]] const fd_type& fd,
            [[maybe_unused]] const std::filesystem::path& name,
            [[maybe_unused]] const allocator_type& alloc) -> std::optional<std::error_code> {
          if (ec) [[unlikely]] return ec;
          return std::nullopt;
        })
    | then(
        [](
            [[maybe_unused]] std::error_code ec,
            gsl::not_null<std::shared_ptr<spdlog::logger>> logger,
            fd_type fd,
            std::filesystem::path name,
            allocator_type alloc) {
          return std::make_tuple(logger, std::move(fd), name, alloc);
        })
    | explode_tuple()
    | then(
        [](
            gsl::not_null<std::shared_ptr<spdlog::logger>> logger,
            fd_type fd,
            std::filesystem::path name,
            allocator_type alloc) {
          std::error_code ec;
          bool was_locked = fd.ftrylock(ec);
          if (!ec && !was_locked) ec = make_error_code(wal_errc::unable_to_lock);
          return std::make_tuple(ec, logger, std::move(fd), std::move(name), std::move(alloc));
        })
    | explode_tuple()
    | validation(
        [](
            std::error_code ec,
            [[maybe_unused]] const gsl::not_null<std::shared_ptr<spdlog::logger>>& logger,
            [[maybe_unused]] const fd_type& fd,
            [[maybe_unused]] const std::filesystem::path& name,
            [[maybe_unused]] const allocator_type& alloc) -> std::optional<std::error_code> {
          if (ec) return ec;
          return std::nullopt;
        })
    | then(
        [](
            [[maybe_unused]] std::error_code ec,
            gsl::not_null<std::shared_ptr<spdlog::logger>> logger,
            fd_type fd,
            std::filesystem::path name,
            allocator_type alloc) {
          return std::make_tuple(logger, std::move(fd), std::move(name), std::move(alloc));
        })
    | explode_tuple()
    | let_value(
        [](
            gsl::not_null<std::shared_ptr<spdlog::logger>>& logger,
            fd_type& fd,
            std::filesystem::path& name,
            allocator_type& alloc) -> sender_of<gsl::not_null<std::shared_ptr<wal_file_entry>>> auto {
          return xdr_v2::read_into<xdr_header_tuple>(pos_stream(fd), xdr_header_invocation{})
          | then(
              [&name, &logger, &alloc](pos_stream&& stream, xdr_header_tuple hdr) {
                struct repeat_state {
                  bool done = false;
                  bool sealed = false;
                  xdr_header_tuple hdr;
                  io::offset_type end_offset = 0;

                  gsl::not_null<std::shared_ptr<spdlog::logger>> logger;
                  std::filesystem::path name;
                  allocator_type alloc;
                };
                return std::make_tuple(
                    std::move(stream),
                    repeat_state{
                      .hdr=hdr,
                      .logger=logger,
                      .name=name,
                      .alloc=alloc,
                    });
              })
          | explode_tuple()
          | repeat(
              []([[maybe_unused]] std::size_t idx, pos_stream& stream, auto& repeat_state) {
                auto factory = [&]() {
                  return std::make_optional(
                      xdr_v2::read_into<variant_type>(std::ref(stream))
                      | observe_value(
                          [&repeat_state]([[maybe_unused]] const auto& stream, const variant_type& v) {
                            repeat_state.logger->debug("read {}", v);
                          })
                      | then(
                          [&repeat_state](std::reference_wrapper<pos_stream> stream, const variant_type& v) -> std::error_code {
                            repeat_state.end_offset = stream.get().position();

                            if (std::holds_alternative<wal_record_end_of_records>(v)) {
                              repeat_state.done = true;
                            } else if (repeat_state.sealed) {
                              repeat_state.logger->error("file {} is bad: record after seal", repeat_state.name.native());
                              return make_error_code(wal_errc::unrecoverable);
                            } else if (std::holds_alternative<wal_record_seal>(v)) {
                              repeat_state.sealed = true;
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
              [](pos_stream&& stream, auto&& repeat_state) -> gsl::not_null<std::shared_ptr<wal_file_entry>> {
                return std::allocate_shared<wal_file_entry>(
                    repeat_state.alloc,
                    std::move(stream.next_layer()),
                    std::move(repeat_state.name),
                    repeat_state.hdr,
                    repeat_state.end_offset,
                    make_link_sender(just()),
                    repeat_state.sealed,
                    repeat_state.logger,
                    repeat_state.alloc);
              });
        });
  }

  [[nodiscard]]
  static auto create(executor_type ex, dir d, const std::filesystem::path& name, std::uint_fast64_t sequence, allocator_type alloc)
  -> execution::type_erased_sender<
      std::variant<std::tuple<
          gsl::not_null<std::shared_ptr<wal_file_entry>>,
          execution::late_binding_t<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>::acceptor>>,
      std::variant<std::exception_ptr, std::error_code>,
      false> {
    using namespace execution;
    using pos_stream = positional_stream_adapter<fd_type&>;

    return just(ex, get_wal_logger(), d, name, sequence, alloc)
    | lazy_observe_value(
        [](
            [[maybe_unused]] const executor_type& ex,
            const gsl::not_null<std::shared_ptr<spdlog::logger>>& logger,
            [[maybe_unused]] const dir& d,
            const std::filesystem::path& name,
            std::uint_fast64_t sequence,
            [[maybe_unused]] const allocator_type& alloc) {
          logger->info("creating wal file[{}] {}", sequence, name.native());
        })
    | validation(
        [](
            [[maybe_unused]] const executor_type& ex,
            [[maybe_unused]] const gsl::not_null<std::shared_ptr<spdlog::logger>>& logger,
            [[maybe_unused]] const dir& d,
            const std::filesystem::path& name,
            [[maybe_unused]] std::uint_fast64_t sequence,
            [[maybe_unused]] const allocator_type& alloc) -> std::optional<std::exception_ptr> {
          if (name.has_parent_path()) return std::make_exception_ptr(std::runtime_error("wal file must be a filename"));
          return std::nullopt;
        })
    | then(
        [](
            executor_type ex,
            gsl::not_null<std::shared_ptr<spdlog::logger>> logger,
            const dir& d,
            std::filesystem::path name,
            std::uint_fast64_t sequence,
            allocator_type alloc) {
          fd_type fd{ex};
          std::error_code ec;
          fd.create(d, name, ec);
          return std::make_tuple(ec, logger, std::move(fd), name, sequence, alloc);
        })
    | explode_tuple()
    | validation(
        [](
            std::error_code ec,
            [[maybe_unused]] const gsl::not_null<std::shared_ptr<spdlog::logger>>& logger,
            [[maybe_unused]] const fd_type& fd,
            [[maybe_unused]] const std::filesystem::path& name,
            [[maybe_unused]] std::uint_fast64_t sequence,
            [[maybe_unused]] const allocator_type alloc) -> std::optional<std::error_code> {
          if (ec) return ec;
          return std::nullopt;
        })
    | then(
        [](
            [[maybe_unused]] std::error_code ec,
            gsl::not_null<std::shared_ptr<spdlog::logger>> logger,
            fd_type fd,
            std::filesystem::path name,
            std::uint_fast64_t sequence,
            allocator_type alloc) {
          return std::make_tuple(logger, std::move(fd), name, sequence, alloc);
        })
    | explode_tuple()
    | then(
        [](
            gsl::not_null<std::shared_ptr<spdlog::logger>> logger,
            fd_type fd,
            std::filesystem::path name,
            std::uint_fast64_t sequence,
            allocator_type alloc) {
          std::error_code ec;
          bool was_locked = fd.ftrylock(ec);
          if (!ec && !was_locked) ec = make_error_code(wal_errc::unable_to_lock);
          return std::make_tuple(ec, logger, std::move(fd), std::move(name), sequence, std::move(alloc));
        })
    | explode_tuple()
    | validation(
        [](
            std::error_code ec,
            [[maybe_unused]] const gsl::not_null<std::shared_ptr<spdlog::logger>>& logger,
            [[maybe_unused]] const fd_type& fd,
            [[maybe_unused]] const std::filesystem::path& name,
            [[maybe_unused]] std::uint_fast64_t sequence,
            [[maybe_unused]] const allocator_type& alloc) -> std::optional<std::error_code> {
          if (ec) return ec;
          return std::nullopt;
        })
    | then(
        [](
            [[maybe_unused]] std::error_code ec,
            gsl::not_null<std::shared_ptr<spdlog::logger>> logger,
            fd_type fd,
            std::filesystem::path name,
            std::uint_fast64_t sequence,
            allocator_type alloc) {
          return std::make_tuple(logger, std::move(fd), std::move(name), sequence, std::move(alloc));
        })
    | explode_tuple()
    | let_value(
        [](
            gsl::not_null<std::shared_ptr<spdlog::logger>>& logger,
            fd_type& fd,
            std::filesystem::path& name,
            std::uint_fast64_t sequence,
            allocator_type& alloc)
        -> sender_of<
            gsl::not_null<std::shared_ptr<wal_file_entry>>,
            late_binding_t<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>::acceptor> auto {
          return xdr_v2::write(pos_stream(fd),
              std::make_tuple(
                  xdr_header_tuple(max_version, sequence),
                  wal_record_end_of_records::opcode,
                  wal_record_end_of_records{}),
              xdr_v2::constant(
                  xdr_v2::tuple(
                      xdr_header_invocation{},
                      xdr_v2::uint32,
                      xdr_v2::identity)))
          | let_value(
              [](pos_stream& stream) {
                return io::lazy_sync(stream.next_layer())
                | then(
                    [&stream]() -> pos_stream&& {
                      return std::move(stream);
                    });
              })
          | then(
              [&alloc, &name, sequence, &logger](pos_stream&& stream) {
                auto end_offset = stream.position();
                auto [link_acceptor, link_sender] = late_binding<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>();
                gsl::not_null<std::shared_ptr<wal_file_entry>> wf = std::allocate_shared<wal_file_entry>(
                    alloc,
                    std::move(stream.next_layer()),
                    std::move(name),
                    xdr_header_tuple(max_version, sequence),
                    end_offset,
                    make_link_sender(std::move(link_sender)),
                    false, // new file is not sealed
                    logger,
                    alloc);
                return std::make_tuple(wf, std::move(link_acceptor));
              })
          | explode_tuple();
        });
  }

#if 0
  template<typename Range, typename CompletionToken, typename TransactionValidator = no_transaction_validation>
#if __cpp_concepts >= 201907L
  requires std::ranges::input_range<std::remove_reference_t<Range>>
#endif
  auto async_append(Range&& records, CompletionToken&& token, TransactionValidator&& transaction_validator = no_transaction_validation(), move_only_function<void(records_vector)> on_successful_write_callback = move_only_function<void(records_vector)>(), bool delay_flush = false);

  template<typename CompletionToken, typename TransactionValidator = no_transaction_validation>
  auto async_append(write_records_vector records, CompletionToken&& token, TransactionValidator&& transaction_validator = no_transaction_validation(), move_only_function<void(records_vector)> on_successful_write_callback = move_only_function<void(records_vector)>(), bool delay_flush = false);
#else
  private:
  template<typename Space>
  static auto durable_append_(
      gsl::not_null<std::shared_ptr<wal_file_entry>> wf,
      write_records_buffer_t& records,
      typename wal_file_entry_file<Executor, Allocator>::callback transaction_validation,
      earnest::move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>, true>(records_vector)> on_successful_write_callback,
      Space space)
  -> execution::sender_of<> auto {
    using namespace execution;

    if (transaction_validation != nullptr) {
      // Start transaction-validation as early as possible,
      // in case it's slow.
      // Also means it'll execute in parallel.
      auto tx_validation = ensure_started(
          wf->fake_link
          | let_value(std::move(transaction_validation)));
      space.set_commit_check(
          [ tx_validation=std::move(tx_validation)
          ]() mutable {
            return std::move(tx_validation);
          });
    }

    if (on_successful_write_callback != nullptr) {
      gsl::not_null<std::shared_ptr<const fd_type>> fd = std::shared_ptr<const fd_type>(wf.get(), &std::as_const(wf->file).file_ref());
      auto write_offset = space.offset();
      space.set_update_callback(
          [ on_successful_write_callback=std::move(on_successful_write_callback),
            write_offset=space.offset(),
            fake_link_in=wf->fake_link,
            &records, fd
          ]() mutable {
            return fake_link_in
            | let_value(
                [ on_successful_write_callback=std::move(on_successful_write_callback),
                  write_offset, records, fd
                ]() mutable {
                  return on_successful_write_callback(records.converted_records(fd, write_offset));
                });
          });
    }

    auto write_op = make_link_sender(std::move(space).write(std::as_const(records).bytes()));
    wf->fake_link = write_op;
    return write_op;
  }

  template<typename Space>
  static auto non_durable_append_(
      gsl::not_null<std::shared_ptr<wal_file_entry>> wf,
      write_records_buffer_t records,
      typename wal_file_entry_file<Executor, Allocator>::callback transaction_validation,
      earnest::move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>, true>(records_vector)> on_successful_write_callback,
      Space space)
  -> execution::sender_of<> auto {
    using namespace execution;

    return just(wf->fake_link, std::move(transaction_validation), wf, records, std::move(on_successful_write_callback), std::move(space))
    | let_variant(
        // We perform transaction validation first.
        // This does delay the actual writes.
        // But we need it, because our delayed-write permits no failures (and transaction-validation failing is considered a failure).
        [](auto& fake_link_in, auto& transaction_validation, auto&... args) {
          auto do_validation = [&]() {
            return std::move(fake_link_in)
            | let_value(std::move(transaction_validation))
            | then(
                [&args...]() {
                  return std::make_tuple(std::move(args)...);
                })
            | explode_tuple();
          };
          auto no_validation_needed = [&args...]() {
            return just(std::move(args)...);
          };

          using variant_type = std::variant<decltype(do_validation()), decltype(no_validation_needed())>;
          if (transaction_validation != nullptr)
            return variant_type(do_validation());
          else
            return variant_type(no_validation_needed());
        })
    | let_value(
        [](gsl::not_null<std::shared_ptr<wal_file_entry>>& wf, write_records_buffer_t& records, auto& on_successful_write_callback, auto& space) {
          const auto write_offset = space.offset();
          auto bytes = wf->unwritten_data.add(write_offset, std::move(records).bytes());
          space.set_must_succeed(true); // Ensures even cancellation will cause a fail-state.

          if (on_successful_write_callback != nullptr) {
            // We need to do the write-callbacks sequentially in order.
            // We do this, by following the fake-links.
            //
            // Note that the new fake-link will happily propagate errors.
            // This is fine, as it'll only happen if the write fails,
            // at which point the WAL is unrecoverable.
            auto [fake_link_acceptor, fake_link_out] = execution::late_binding<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>();
            auto converted_records = records.converted_records(std::shared_ptr<const fd_type>(wf.get(), &std::as_const(wf->file).file_ref()), write_offset);

            auto fake_link_in = std::exchange(wf->fake_link, make_link_sender(std::move(fake_link_out)));
            start_detached(
                std::move(fake_link_in)
                | let_value(
                    [ on_successful_write_callback=std::move(on_successful_write_callback),
                      converted_records=std::move(converted_records),
                      fake_link_acceptor=std::move(fake_link_acceptor)
                    ]() mutable {
                      auto cb = lazy_split(on_successful_write_callback(std::move(converted_records)));
                      try {
                        std::move(fake_link_acceptor).assign(cb);
                      } catch (...) {
                        std::move(fake_link_acceptor).assign_error(std::current_exception());
                        throw;
                      }
                      return cb;
                    }));
          }

          // In a deferred-write, we pretend that the write succeeded.
          // It's fine, because our caller says the durability guarantee is optional.
          // And this way, the (potentially slow) write can happen in the background,
          // while the code continues onward.
          //
          // This paragraph does the actual write, and then cleans up the unwritten-data.
          start_detached(
              on(
                  wf->strand_,
                  std::move(space).write(bytes, true)
                  | observe_value(
                      // We can now rely on file reads to provide this data.
                      // (We only do this for successful writes. If the write fails,
                      // we'll maintain read access via the unwritten-data, in order
                      // not to upset the read invariant.)
                      [wf, write_offset, bytes]() -> void {
                        assert(wf->strand_.running_in_this_thread());
                        wf->unwritten_data.remove(write_offset, bytes);
                      })
                  | let_error(
                      // If there are errors, we swallow them here:
                      // - write errors will have updated the WAL state to be unrecoverable, so this is already handled.
                      // - if the error was with post-write cleanup (unwritten-data cleanup) we rather pay the cost of a couple
                      //   of bytes, so this is harmless.
                      //
                      // Also like, if we didn't swallow errors here, we would terminate the program (via std::unexpected),
                      // and it's just bad manners to do that, when the error has already been handled/is harmless.
                      [](auto error) {
                        return just();
                      })));

          // We advertise this write as successful.
          // (And we also advertise any other preceding delay-flush writes as having succeeded.)
          // But they're not actually reachable by read, unless all preceding immediate writes have completed.
          // Se we must return the fake-links here.
          return wf->fake_link;
        });
  }

  public:
  auto append(
      write_records_buffer_t records,
      typename wal_file_entry_file<Executor, Allocator>::callback transaction_validation = nullptr,
      earnest::move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>, true>(records_vector)> on_successful_write_callback = nullptr,
      bool delay_flush = false)
  -> execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>> {
    using namespace execution;

    gsl::not_null<std::shared_ptr<wal_file_entry>> wf = this->shared_from_this();
    return on(
        strand_,
        just(wf, std::move(records), delay_flush, std::move(transaction_validation), std::move(on_successful_write_callback))
        | lazy_validation(
            [](gsl::not_null<std::shared_ptr<wal_file_entry>> wf, [[maybe_unused]] const auto&... ignored_args) -> std::optional<std::error_code> {
              // We only permit writes when we're in the ready state.
              if (wf->state_ != wal_file_entry_state::ready) return make_error_code(wal_errc::bad_state);
              return std::nullopt;
            })
        | let_variant(
            [](gsl::not_null<std::shared_ptr<wal_file_entry>> wf, write_records_buffer_t& records,
                bool delay_flush, auto& transaction_validation, auto& on_successful_write_callback) {
              auto space = wf->file.prepare_space(
                  std::shared_ptr<wal_file_entry_file<Executor, Allocator>>(wf.get(), &wf->file),
                  std::as_const(records).bytes().size());

              // Durable writes are "immediate", meaning the sender-chain is linked to the write-to-disk operation completing.
              //
              // Note that durable-append takes the records by reference, since it yields only a single chain, and thus can be guaranteed
              // the records remain valid for the duration of the chain.
              auto immediate = [&]() {
                assert(delay_flush == false);
                return durable_append_(wf, records, std::move(transaction_validation), std::move(on_successful_write_callback), std::move(space));
              };

              // Non-durable writes are "deferred", meaning the sender-chain only writes the operation to memory,
              // deferring the write-to-disk operation.
              // Deferred operations are a lot faster, because they don't require flush-to-disk.
              // But they can be lost during failure.
              auto deferred = [&]() {
                assert(delay_flush == true);
                return non_durable_append_(wf, std::move(records), std::move(transaction_validation), std::move(on_successful_write_callback), std::move(space));
              };

              using variant_type = std::variant<decltype(immediate()), decltype(deferred())>;
              if (delay_flush)
                return variant_type(deferred());
              else
                return variant_type(immediate());
            }));
  }

  template<std::ranges::input_range Records, typename CompletionToken, typename TransactionValidator = no_transaction_validation>
  [[deprecated]]
  auto async_append(Records&& records, CompletionToken&& token, TransactionValidator&& transaction_validator = no_transaction_validation(), move_only_function<void(records_vector)> on_successful_write_callback = move_only_function<void(records_vector)>(), bool delay_flush = false) {
    using namespace execution;

    write_records_buffer_t wrb(get_executor(), get_allocator());
    wrb.push_back_range(std::forward<Records>(records));

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto completion_handler, gsl::not_null<std::shared_ptr<wal_file_entry>> wf, write_records_buffer<executor_type, allocator_type> wrb, auto transaction_validator, auto on_successful_write_callback, bool delay_flush) -> void {
          auto convert_transaction_validator = [&transaction_validator]() -> typename wal_file_entry_file<Executor, Allocator>::callback {
            if constexpr(std::is_same_v<no_transaction_validation, std::remove_cvref_t<decltype(transaction_validator)>>) {
              return nullptr;
            } else {
              return [transaction_validator=std::move(transaction_validator)]() mutable {
                auto [acceptor, sender] = execution::late_binding<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>();
                transaction_validator()
                | [acceptor=std::move(acceptor)](std::error_code ec) mutable {
                    if (!ec)
                      std::move(acceptor).assign_values();
                    else
                      std::move(acceptor).assign_error(std::make_exception_ptr(std::system_error(ec)));
                  };
                return sender;
              };
            }
          };

          auto convert_on_successful_write_callback = [&on_successful_write_callback](records_vector rv) -> earnest::move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>, true>(records_vector)> {
            if (on_successful_write_callback == nullptr) {
              return nullptr;
            } else {
              return [on_successful_write_callback=std::move(on_successful_write_callback)](records_vector rv) mutable {
                on_successful_write_callback(std::move(rv));
                return just();
              };
            }
          };

          start_deferred(
              wf->append(
                  wf->asio_strand_,
                  std::move(wrb),
                  convert_transaction_validator(),
                  convert_on_successful_write_callback(),
                  delay_flush)
              | observe_value(
                  [ completion_handler=std::move(completion_handler)
                  ]() {
                    completion_handler(std::error_code{});
                  }));
        },
        token, this->shared_from_this(), std::move(wrb), std::forward<TransactionValidator>(transaction_validator), std::move(on_successful_write_callback), delay_flush);
  }
#endif

  private:
  template<typename OnSpaceAssigned, typename TransactionValidator>
  auto async_append_impl_(write_records_vector records, move_only_function<void(std::error_code)>&& completion_handler, OnSpaceAssigned&& on_space_assigned, TransactionValidator&& transaction_validator, move_only_function<void(records_vector)> on_successful_write_callback, bool delay_flush = false) -> void;

  public:
  template<typename CompletionToken>
  auto async_seal(CompletionToken&& token);

  template<typename CompletionToken>
  auto async_discard_all(CompletionToken&& token);

  auto end_offset() const noexcept {
    std::lock_guard lck{strand_};
    return file.get_end_offset();
  }

  auto link_offset() const noexcept {
    std::lock_guard lck{strand_};
    return file.get_link_offset();
  }

  auto records(move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>(variant_type)> acceptor) const
  -> execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>> {
    using namespace execution;
    using namespace execution::io;

    struct state {
      async_records_reader<allocator_type> reader;
      move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>(variant_type)> acceptor;
      bool should_stop = false;
    };

    return on(
        strand_,
        just(
            state{
              .reader=async_records_reader<allocator_type>(this->shared_from_this(), this->get_allocator()),
              .acceptor=std::move(acceptor)
            })
        | lazy_let_value(
            // Read the header, since we want to skip it.
            [](state& st) {
              return xdr_v2::read_into<xdr_header_tuple>(std::ref(st.reader), xdr_header_invocation{})
              | then(
                  [&st]([[maybe_unused]] const auto& reader, [[maybe_unused]] xdr_header_tuple hdr) -> state&& {
                    // Discard header, and return the state.
                    return std::move(st);
                  });
            })
        | lazy_repeat(
            []([[maybe_unused]] std::size_t idx, state& st) {
              auto do_next = [&st]() {
                const auto read_pos = st.reader.position();

                return xdr_v2::read_into<variant_type>(std::ref(st.reader))
                | lazy_let<set_value_t, set_error_t>(
                    overload(
                        [&st]([[maybe_unused]] set_value_t tag, [[maybe_unused]] const auto& stream_ref, variant_type& record) {
                          return std::invoke(st.acceptor, std::move(record));
                        },
                        [&st, read_pos]([[maybe_unused]] set_error_t tag, std::error_code ec) {
                          return just(std::ref(st), ec, read_pos)
                          | validation(
                              [](state& st, std::error_code ec, auto read_pos) -> std::optional<std::error_code> {
                                if (ec == execution::io::errc::eof && st.reader.position() == read_pos) {
                                  st.should_stop = true;
                                  return std::nullopt;
                                }
                                return std::make_optional(ec);
                              });
                        },
                        []([[maybe_unused]] set_error_t tag, std::exception_ptr ex) {
                          return just(std::move(ex))
                          | validation(
                              [](std::exception_ptr ex) {
                                return std::make_optional(ex);
                              });
                        }));
              };

              using opt_type = std::optional<decltype(do_next())>;
              if (st.should_stop)
                return opt_type(std::nullopt);
              else
                return opt_type(do_next());
            })
        | then(
            []([[maybe_unused]] const state& st) -> void {
              return; // discard `st'
            }));
  }

  auto records() const -> execution::type_erased_sender<std::variant<std::tuple<records_vector>>, std::variant<std::exception_ptr, std::error_code>> {
    using namespace execution;

    // We don't use `on(strand_, ...)` here, because the `records(acceptor)` specialization will handle that for us.
    return just(gsl::not_null<std::shared_ptr<const wal_file_entry>>(this->shared_from_this()))
    | lazy_then(
        [](gsl::not_null<std::shared_ptr<const wal_file_entry>> wf) {
          return std::make_tuple(wf, records_vector(wf->get_allocator()));
        })
    | explode_tuple()
    | lazy_let_value(
        [](const gsl::not_null<std::shared_ptr<const wal_file_entry>>& wf, records_vector& records) {
          return wf->records(
              [&records](variant_type record) {
                records.push_back(std::move(record));
                return just();
              })
          | then(
              [&records]() -> records_vector&& {
                return std::move(records);
              });
        });
  }

  template<std::invocable<variant_type> Acceptor, typename CompletionToken>
  [[deprecated]]
  auto async_records(Acceptor&& acceptor, CompletionToken&& token) const {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto completion_handler, gsl::not_null<std::shared_ptr<const wal_file_entry>> wf, std::remove_cvref_t<Acceptor> acceptor) -> void {
          using namespace execution;

          start_deferred(
              just(wf, std::move(acceptor))
              | let_value(
                  [](const gsl::not_null<std::shared_ptr<const wal_file_entry>>& wf, std::remove_cvref_t<Acceptor>& acceptor) {
                    wf->records(
                        [&acceptor](variant_type record) {
                          return just(std::invoke(acceptor, std::move(record)))
                          | validation(
                              [](std::error_code ec) -> std::optional<std::error_code> {
                                if (!ec) return std::nullopt;
                                return std::make_optional(ec);
                              });
                        });
                  })
              | then(
                  []() {
                    return std::error_code{};
                  })
              | then(std::move(completion_handler)));
        },
        token, this->shared_from_this(), std::forward<Acceptor>(acceptor));
  }

  template<typename CompletionToken>
  [[deprecated]]
  auto async_records(CompletionToken&& token) const {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto completion_handler, gsl::not_null<std::shared_ptr<const wal_file_entry>> wf) -> void {
          using namespace execution;

          start_deferred(
              just(wf)
              | let_value(
                  [](const gsl::not_null<std::shared_ptr<const wal_file_entry>>& wf) {
                    return wf->records();
                  })
              | then(
                  [](records_vector records) {
                    return std::make_tuple(std::error_code{}, std::move(records));
                  })
              | explode_tuple()
              | then(std::move(completion_handler)));
        },
        token, this->shared_from_this());
  }

  private:
  // Adapter class.
  // Provides a readable interface for the wal-file-entry.
  class reader_impl_ {
    public:
    explicit reader_impl_(gsl::not_null<std::shared_ptr<const wal_file_entry>> wf)
    : wf(wf)
    {}

    using executor_type = Executor;

    auto get_executor() const noexcept -> decltype(auto) {
      return wf->get_executor();
    }

    friend auto tag_invoke([[maybe_unused]] execution::io::lazy_read_some_at_ec_t tag, const reader_impl_& self, execution::io::offset_type offset, std::span<std::byte> buffer) -> execution::sender_of<std::size_t> auto {
      using namespace execution;
      using namespace execution::io;

      return just(self.wf, offset, buffer)
      | lazy_let_value(
          [](gsl::not_null<std::shared_ptr<const wal_file_entry>> wf, offset_type offset, std::span<std::byte> buffer) {
            auto wff = std::shared_ptr<const wal_file_entry_file<Executor, Allocator>>(wf.get(), &wf->file);
            return wf->unwritten_data.read_some_at(
                offset, buffer,
                [wff](offset_type offset, std::span<std::byte> buffer) {
                  return wal_file_entry_file<Executor, Allocator>::read_some_at(wff, offset, buffer);
                });
          });
    }

    private:
    gsl::not_null<std::shared_ptr<const wal_file_entry>> wf;
  };

  template<typename StreamAllocator>
  class async_records_reader
  : public buffered_readstream_adapter<positional_stream_adapter<reader_impl_>, StreamAllocator>
  {
    private:
    using parent_type = buffered_readstream_adapter<positional_stream_adapter<reader_impl_>, StreamAllocator>;

    public:
    explicit async_records_reader(gsl::not_null<std::shared_ptr<const wal_file_entry>> wf)
    : parent_type(typename parent_type::next_layer_type(reader_impl_(wf)))
    {}

    explicit async_records_reader(gsl::not_null<std::shared_ptr<const wal_file_entry>> wf, StreamAllocator alloc)
    : parent_type(typename parent_type::next_layer_type(reader_impl_(wf)), alloc)
    {}
  };

#if 0
  template<typename HandlerAllocator>
  auto async_records_impl_(move_only_function<std::error_code(variant_type)> acceptor, move_only_function<void(std::error_code)> completion_handler, HandlerAllocator handler_alloc) const;
#endif

#if 0
  template<typename CompletionToken, typename OnSpaceAssigned, typename TransactionValidator>
  auto append_bytes_(std::vector<std::byte, rebind_alloc<std::byte>>&& bytes, CompletionToken&& token, OnSpaceAssigned&& space_assigned_event, std::error_code ec, wal_file_entry_state expected_state, TransactionValidator&& transaction_validator, move_only_function<void(records_vector)> on_successful_write_callback, write_record_with_offset_vector records, bool delay_flush = false);
#endif

#if 0
  template<typename CompletionToken, typename Barrier, typename Fanout, typename DeferredTransactionValidator>
  auto append_bytes_at_(
      typename fd_type::offset_type write_offset,
      std::vector<std::byte, rebind_alloc<std::byte>>&& bytes,
      CompletionToken&& token,
      Barrier&& barrier, Fanout&& f,
      DeferredTransactionValidator&& deferred_transaction_validator,
      std::shared_ptr<records_vector> converted_records,
      bool delay_flush = false);
#elif 0
  template<typename Barrier, typename Fanout, typename DeferredTransactionValidator>
  auto append_bytes_at_(
      typename fd_type::offset_type write_offset,
      std::vector<std::byte, rebind_alloc<std::byte>>&& bytes,
      Barrier&& barrier, Fanout&& f,
      DeferredTransactionValidator&& deferred_transaction_validator,
      std::shared_ptr<records_vector> converted_records,
      bool delay_flush = false)
  -> execution::type_erased_sender<
      std::variant<std::tuple<>>,
      std::variant<std::exception_ptr, std::error_code>,
      false> {
    using namespace execution;

    if (bytes.empty()) [[unlikely]] return just();

    const auto bytes_size = bytes.size();
    auto [write_operation_acceptor, write_operation_sender] = late_binding<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>();
    auto write_data = split(
        when_all(
            std::move(link_done_event_),
            just(this->shared_from_this(), write_offset, std::move(bytes))
            | lazy_let_value(
                [](std::shared_ptr<wal_file_entry>& wf, typename fd_type::offset_type write_offset, const std::vector<std::byte, rebind_alloc<std::byte>>& bytes) {
                  return io::lazy_write_at_ec(
                      wf->file,
                      write_offset,
                      std::span<const std::byte>(bytes).subspan(4))
                  | lazy_then(
                      [&wf, write_offset, &bytes]([[maybe_unused]] std::size_t wlen) {
                        assert(bytes.size() >= 4);
                        auto link_bytes = std::array<std::byte, 4>{
                          bytes[0],
                          bytes[1],
                          bytes[2],
                          bytes[3],
                        };
                        return std::make_tuple(std::move(wf), write_offset, std::move(link_bytes));
                      })
                  | explode_tuple();
                })
            | lazy_let_value(
                [](std::shared_ptr<wal_file_entry>& wf, typename fd_type::offset_type write_offset, std::array<std::byte, 4> link_bytes) {
                  return wf->write_link_(write_offset, link_bytes);
                })
            | observe_value(
                [write_operation_acceptor=std::move(write_operation_acceptor)](const auto&... args) mutable {
                  std::move(write_operation_acceptor).assign(just());
                }))
        | chain_breaker()); // In order to drop the write_operation_acceptor:
                            // this'll cause a cancelation, marking the write_operation_acceptor as canceled,
                            // thus tripping the recovery code.

    auto [link_acceptor, link_sender] = late_binding<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>();
    auto between_links = std::move(write_operation_sender)
    | let<set_error_t, set_done_t>(
        [wf=this->shared_from_this(), write_offset, bytes_size, converted_records, barrier]([[maybe_unused]] const auto& tag, [[maybe_unused]] const auto&... args) {
          converted_records->clear(); // So that the barrier-invocation won't send those to a successful-write-callback.
                                      // XXX I dislike this, so after refactoring, see if we can pass this forward instead.

          assert(bytes_size >= 4);
          return wf->write_skip_record_(write_offset, bytes_size - 4u)
          | then(std::move(barrier));
        });

    std::move(link_acceptor).assign(
        when_all(std::move(between_links), std::exchange(link_done_event_, std::move(link_sender)))
        | chain_breaker());

    write_offset_ += bytes_size;

    return std::move(write_data);
  }

  template<typename CompletionToken, typename Barrier, typename Fanout, typename DeferredTransactionValidator>
  auto append_bytes_at_(
      typename fd_type::offset_type write_offset,
      std::vector<std::byte, rebind_alloc<std::byte>>&& bytes,
      CompletionToken&& token,
      Barrier&& barrier, Fanout&& f,
      DeferredTransactionValidator&& deferred_transaction_validator,
      std::shared_ptr<records_vector> converted_records,
      bool delay_flush = false) {
    using namespace execution;

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto completion_handler, std::shared_ptr<wal_file_entry> wf, typename fd_type::offset_type write_offset, std::vector<std::byte, rebind_alloc<std::byte>>&& bytes, auto barrier, auto f, auto deferred_transaction_validator, std::shared_ptr<records_vector> converted_records, bool delay_flush) -> void {
          start_detached(
              wf->append_bytes_at_(write_offset, std::move(bytes), std::move(barrier), std::move(f), std::move(deferred_transaction_validator), std::move(converted_records), delay_flush)
              | then(
                  []() noexcept -> std::error_code {
                    return std::error_code{};
                  })
              | upon_error(
                  [](auto err) -> std::error_code {
                    if constexpr(std::same_as<std::exception_ptr, decltype(err)>)
                      std::rethrow_exception(err);
                    else
                      return err;
                  })
              | then(completion_handler_fun(std::move(completion_handler), wf->get_executor())));
        },
        token, this->shared_from_this(), write_offset, std::move(bytes),
        std::forward<Barrier>(barrier), std::forward<Fanout>(f),
        std::forward<DeferredTransactionValidator>(deferred_transaction_validator), std::move(converted_records), delay_flush);
  }
#endif

#if 0
  auto write_skip_record_(typename fd_type::offset_type write_offset, std::size_t bytes)
  -> execution::type_erased_sender<
      std::variant<std::tuple<>>,
      std::variant<std::exception_ptr, std::error_code>,
      false> {
    using namespace execution;
    using pos_stream = positional_stream_adapter<fd_type&>;

    auto zero_bytes = [](std::shared_ptr<wal_file_entry> wf, typename fd_type::offset_type write_offset, [[maybe_unused]] std::size_t bytes) {
      assert(bytes == 0);
      constexpr std::array<std::byte, 4> opcode{
        std::byte{(wal_record_noop::opcode >> 24u) & 0xffu},
        std::byte{(wal_record_noop::opcode >> 16u) & 0xffu},
        std::byte{(wal_record_noop::opcode >>  8u) & 0xffu},
        std::byte{(wal_record_noop::opcode       ) & 0xffu},
      };
      return just(wf, write_offset, opcode);
    };
    auto uint32_bytes = [](std::shared_ptr<wal_file_entry> wf, typename fd_type::offset_type write_offset, std::size_t bytes) {
      assert(bytes >= 4u);
      return xdr_v2::write(
          pos_stream(wf->file, write_offset),
          bytes - 4u,
          xdr_v2::constant(xdr_v2::uint32))
      | then(
          [wf, write_offset]([[maybe_unused]] pos_stream&& stream) {
            constexpr std::array<std::byte, 4> opcode{
              std::byte{(wal_record_skip32::opcode >> 24u) & 0xffu},
              std::byte{(wal_record_skip32::opcode >> 16u) & 0xffu},
              std::byte{(wal_record_skip32::opcode >>  8u) & 0xffu},
              std::byte{(wal_record_skip32::opcode       ) & 0xffu},
            };
            return std::make_tuple(wf, write_offset, opcode);
          })
      | explode_tuple();
    };
    auto uint64_bytes = [](std::shared_ptr<wal_file_entry> wf, typename fd_type::offset_type write_offset, std::size_t bytes) {
      assert(bytes >= 8u);
      return xdr_v2::write(
          pos_stream(wf->file, write_offset),
          bytes - 8u,
          xdr_v2::constant(xdr_v2::uint64))
      | then(
          [wf, write_offset]([[maybe_unused]] pos_stream&& stream) {
            constexpr std::array<std::byte, 4> opcode{
              std::byte{(wal_record_skip64::opcode >> 24u) & 0xffu},
              std::byte{(wal_record_skip64::opcode >> 16u) & 0xffu},
              std::byte{(wal_record_skip64::opcode >>  8u) & 0xffu},
              std::byte{(wal_record_skip64::opcode       ) & 0xffu},
            };
            return std::make_tuple(wf, write_offset, opcode);
          })
      | explode_tuple();
    };

    using sender_variant_type = std::variant<
        decltype(zero_bytes(std::declval<std::shared_ptr<wal_file_entry>>(), std::declval<typename fd_type::offset_type>(), std::declval<std::size_t>())),
        decltype(uint32_bytes(std::declval<std::shared_ptr<wal_file_entry>>(), std::declval<typename fd_type::offset_type>(), std::declval<std::size_t>())),
        decltype(uint64_bytes(std::declval<std::shared_ptr<wal_file_entry>>(), std::declval<typename fd_type::offset_type>(), std::declval<std::size_t>()))
    >;

    return just(this->shared_from_this(), write_offset, bytes)
    | let_variant(
        [zero_bytes, uint32_bytes, uint64_bytes](std::shared_ptr<wal_file_entry> wf, typename fd_type::offset_type write_offset, std::size_t bytes) -> sender_variant_type {
          if (bytes == 0)
            return zero_bytes(wf, write_offset, bytes);
          else if (bytes <= 0xffff'ffffu)
            return uint32_bytes(wf, write_offset, bytes);
          else
            return uint64_bytes(wf, write_offset, bytes);
        })
    | let_value(
        [](std::shared_ptr<wal_file_entry> wf, typename fd_type::offset_type write_offset, std::array<std::byte, 4> opcode) {
          return wf->write_link_(write_offset, opcode);
        });
  }

  template<typename CompletionToken>
  [[deprecated]]
  auto write_skip_record_(
      typename fd_type::offset_type write_offset,
      std::size_t bytes, CompletionToken&& token) {
    using namespace execution;

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto completion_handler, std::shared_ptr<wal_file_entry> wf, typename fd_type::offset_type write_offset, std::size_t bytes) -> void {
          start_detached(
              wf->write_skip_record_(write_offset, bytes)
              | then(
                  []() noexcept -> std::error_code {
                    return std::error_code{};
                  })
              | upon_error(
                  [](auto err) -> std::error_code {
                    if constexpr(std::same_as<std::exception_ptr, decltype(err)>)
                      std::rethrow_exception(err);
                    else
                      return err;
                  })
              | then(completion_handler_fun(std::move(completion_handler), wf->get_executor())));
        },
        token, this->shared_from_this(), write_offset, bytes);
  }

  auto write_link_(
      typename fd_type::offset_type write_offset,
      std::array<std::byte, 4> bytes,
      bool delay_flush = false)
  -> execution::type_erased_sender<
      std::variant<std::tuple<>>,
      std::variant<std::exception_ptr, std::error_code>,
      false> {
    using namespace execution;
    using pos_stream = positional_stream_adapter<fd_type&>;

    return wal_flusher_.lazy_flush(true, delay_flush)
    | lazy_let_value(
        [this, write_offset, bytes]() {
          return xdr_v2::write(pos_stream(this->file, write_offset - 4u), bytes, xdr_v2::constant(xdr_v2::fixed_byte_string))
          | then(
              []([[maybe_unused]] pos_stream&& stream) -> void {
                return;
              });
        })
    | lazy_transfer(strand_.scheduler(asio_strand_))
    | lazy_let_value(
        [this, write_offset, delay_flush]() {
          assert(this->strand_.running_in_this_thread());
          assert(this->asio_strand_.get_executor().running_in_this_thread());

          return this->wal_flusher_.lazy_flush(true, delay_flush && this->must_flush_offset_ < write_offset);
        });
  }

  template<typename CompletionToken>
  [[deprecated]]
  auto write_link_(
      typename fd_type::offset_type write_offset,
      std::array<std::byte, 4> bytes, CompletionToken&& token,
      bool delay_flush = false) {
    using namespace execution;

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto completion_handler, std::shared_ptr<wal_file_entry> wf, typename fd_type::offset_type write_offset, std::array<std::byte, 4> bytes, bool delay_flush) -> void {
          start_detached(
              wf->write_link_(write_offset, bytes, delay_flush)
              | then(
                  []() noexcept -> std::error_code {
                    return std::error_code{};
                  })
              | upon_error(
                  [](auto err) -> std::error_code {
                    if constexpr(std::same_as<std::exception_ptr, decltype(err)>)
                      std::rethrow_exception(err);
                    else
                      return err;
                  })
              | then(completion_handler_fun(std::move(completion_handler), wf->get_executor())));
        },
        token, this->shared_from_this(), write_offset, bytes, delay_flush);
  }
#endif

  public:
  const std::filesystem::path name;
  const std::uint_fast32_t version;
  const std::uint_fast64_t sequence;

  private:
#if 0 // old stuff
  fd_type file; // XXX name-clash
  typename fd_type::offset_type write_offset_;
  typename fd_type::offset_type link_offset_;
  typename fd_type::offset_type must_flush_offset_ = 0;
  asio_execution_scheduler<asio::strand<executor_type>> asio_strand_;
  execution::strand<> strand_;
  link_done_event_sender link_done_event_;
  wal_flusher<fd_type&, allocator_type> wal_flusher_;
  unwritten_data_vector unwritten_data_;
#endif

  // new stuff
  allocator_type alloc_;
  wal_file_entry_state state_ = wal_file_entry_state::uninitialized;
  wal_file_entry_file<Executor, Allocator> file;
  wal_file_entry_unwritten_data<Allocator> unwritten_data;
  link_sender fake_link;
  mutable execution::strand<allocator_type> strand_;

  const gsl::not_null<std::shared_ptr<spdlog::logger>> logger;
};

template<typename Executor, typename Allocator>
struct wal_file_entry<Executor, Allocator>::xdr_header_invocation {
  private:
  static constexpr auto magic() noexcept -> std::array<char, 13> {
    return std::array<char, 13>{ '\013', '\013', 'e', 'a', 'r', 'n', 'e', 's', 't', '.', 'w', 'a', 'l' };
  }

  public:
  auto read(xdr_header_tuple& tpl) const {
    return xdr_v2::constant.read(magic(), xdr_v2::fixed_byte_string)
    | xdr_v2::uint32.read(std::get<0>(tpl))
    | xdr_v2::validation.read(
        [&tpl]() -> std::optional<std::error_code> {
          if (std::get<0>(tpl) <= max_version)
            return std::nullopt;
          else
            return make_error_code(wal_errc::bad_version);
        })
    | xdr_v2::uint64.read(std::get<1>(tpl));
  }

  auto write(const xdr_header_tuple& tpl) const {
    return xdr_v2::constant.write(magic(), xdr_v2::fixed_byte_string)
    | xdr_v2::validation.write(
        [&tpl]() -> std::optional<std::error_code> {
          if (std::get<0>(tpl) <= max_version)
            return std::nullopt;
          else
            return make_error_code(wal_errc::bad_version);
        })
    | xdr_v2::uint32.write(std::get<0>(tpl))
    | xdr_v2::uint64.write(std::get<1>(tpl));
  }
};


} /* namespace earnest::detail */

namespace fmt {


template<>
struct formatter<earnest::detail::wal_file_entry_state>
: formatter<std::string>
{
  auto format(earnest::detail::wal_file_entry_state state, format_context& ctx) -> decltype(ctx.out()) {
    using namespace std::literals;

    switch (state) {
      default:
        return fmt::format_to(ctx.out(), "wal_file_entry_state({})", static_cast<std::underlying_type_t<earnest::detail::wal_file_entry_state>>(state));
      case earnest::detail::wal_file_entry_state::uninitialized:
        return fmt::format_to(ctx.out(), "uninitialized");
      case earnest::detail::wal_file_entry_state::opening:
        return fmt::format_to(ctx.out(), "opening");
      case earnest::detail::wal_file_entry_state::ready:
        return fmt::format_to(ctx.out(), "ready");
      case earnest::detail::wal_file_entry_state::sealing:
        return fmt::format_to(ctx.out(), "sealing");
      case earnest::detail::wal_file_entry_state::sealed:
        return fmt::format_to(ctx.out(), "sealed");
      case earnest::detail::wal_file_entry_state::failed:
        return fmt::format_to(ctx.out(), "failed");
    }
  }
};


} /* namespace fmt */

#include "wal_file_entry.ii"
