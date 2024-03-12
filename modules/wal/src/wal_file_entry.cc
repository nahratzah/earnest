#include <earnest/detail/wal_file_entry.h>

namespace earnest::detail {


class wal_file_entry_file::prepared_space {
  public:
  prepared_space(
      gsl::not_null<std::shared_ptr<wal_file_entry_file>> self,
      execution::strand<allocator_type> strand,
      offset_type write_offset,
      std::size_t write_len,
      commit_space_late_binding_t::acceptor acceptor,
      link_sender preceding_link, link_sender new_link)
  : self(self.get()),
    strand(strand),
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

  auto get_new_link() const -> link_sender {
    return new_link;
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

  // Implementation for write.
  // This performs an atomic write of some bytes of data.
  // The data to-be-written must be at least 4 bytes.
  // Must be run with a scheduler wrapped by a strand_.
  //
  // - data: the data that is to be written.
  // - delay_flush: allows for flush-to-disk to be delayed, in order to combine more writes into a single flush operation.
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
  //         |`---------------------------------------------------> [ update callback ]  (omitted if nullptr)
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
  auto write(std::span<const std::byte> data, bool delay_flush = false) && -> execution::sender_of<> auto {
    using namespace execution;

    if (data.size() != write_len) throw std::range_error("data size does not match reserved size");

    assert(strand.running_in_this_thread());
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
          assert(space.strand.running_in_this_thread());
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
                assert(space.strand.running_in_this_thread());

                auto do_the_check = [&]() {
                  return not_on_strand(
                      space.strand,
                      space.preceding_link | let_value(std::ref(space.commit_check)));
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
                assert(space.strand.running_in_this_thread());
                return space.self->wal_flusher_.lazy_flush(true, space.self->delay_flush(space.write_offset, delay_flush));
              });

          // Second stage of the write, we can only start if the transaction is allowed to succeed.
          auto second_stage = when_all(std::move(transaction_commit), std::move(first_stage))
          | let_value(
              // Now that the new data is guaranteed on-disk, we can install the link.
              [ link_data=std::span<const std::byte, 4>(std::span<const std::byte>(data).subspan(0, 4)),
                &space
              ]() {
                assert(space.strand.running_in_this_thread());
                return io::write_at_ec(space.self->file, space.write_offset, link_data);
              })
          | let_value(
              // Now that the link is written, we need to flush that to disk.
              [&space, delay_flush]([[maybe_unused]] std::size_t num_bytes_written) {
                assert(space.strand.running_in_this_thread());
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
                  return not_on_strand(
                      space.strand,
                      std::move(space.preceding_link) | let_value(std::ref(space.update_callback)));
                };
                auto nothing_to_update = [&]() {
                  // We must wait on the preceding-link, because the space.acceptor may not complete until the preceding link completes.
                  return not_on_strand(
                      space.strand,
                      std::move(space.preceding_link));
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
                return not_on_strand(space.strand, std::move(space.new_link));
              });
        })
    | chain_breaker(); // Now we can release any references to this.
                       // Also means we won't hang on to the acceptor granted us by commit_space_,
                       // which is of critical importance, because releasing it will ensure
                       // the recovery-path can start (if that is needed).
  }

  private:
  std::shared_ptr<wal_file_entry_file> self;
  execution::strand<allocator_type> strand;
  offset_type write_offset;
  std::size_t write_len;
  commit_space_late_binding_t::acceptor acceptor;
  link_sender preceding_link, new_link;
  callback commit_check = nullptr;
  callback update_callback = nullptr;
  bool must_succeed = false;
};

static_assert(std::is_move_constructible_v<wal_file_entry_file::prepared_space>);


auto wal_file_entry_file::prepare_space(gsl::not_null<std::shared_ptr<wal_file_entry_file>> self, execution::strand<allocator_type> strand, std::size_t write_len) -> prepared_space {
  using namespace execution;

  if (write_len < 4) throw std::range_error("cannot write data of less than 4 bytes");
  if (write_len % 4u != 0) throw std::range_error("expect data to be a multiple of 4 bytes");

  offset_type write_offset = self->end_offset - 4u;
  offset_type new_end_offset = self->end_offset + write_len;
  offset_type new_link_offset = write_offset + write_len;

  // This recovery-chain writes the appropriate skip-record.
  // It's only invoked when the actual write failed.
  auto make_recovery_chain = [self, strand, write_offset, write_len]([[maybe_unused]] const auto&...) {
    return just(self, strand, write_offset, write_len)
    | then(
        [](gsl::not_null<std::shared_ptr<wal_file_entry_file>> self, auto strand, offset_type write_offset, std::size_t write_len) {
          auto data = self->recovery_record_(write_len);
          assert(data.size() <= write_len); // It must fit.
          return std::make_tuple(self, strand, write_offset, data, write_len);
        })
    | explode_tuple()
    | let_value(
        [](const gsl::not_null<std::shared_ptr<wal_file_entry_file>>& shared_self, [[maybe_unused]] auto strand, offset_type write_offset, std::span<const std::byte> data, std::size_t write_len) {
          assert(strand.running_in_this_thread());

          // We store the non-shared-pointer version, because it seems wasteful to install many copies of shared_self,
          // when the let_value-caller will guarantee its liveness anyway.
          const gsl::not_null<wal_file_entry_file*> self = shared_self.get().get();

          // Just like in regular writes, we write the data in two phases.
          // First we write everything except the first four bytes.
          // After flushing those to disk, we write the first four bytes (which will link the data together).
          return when_all(io::write_at_ec(self->file, write_offset + 4u, data.subspan(4)), just(strand))
          | let_value(
              // After the first write, we flush-to-disk the written data.
              [self]([[maybe_unused]] std::size_t wlen, auto strand) {
                assert(strand.running_in_this_thread());
                return when_all(self->wal_flusher_.lazy_flush(true), just(strand)); // XXX pass strand to flusher?
              })
          | let_value(
              // Now that the skip record's data is guaranteed on disk, we can write the link.
              [ link_span=std::span<const std::byte, 4>(data.subspan(0, 4)),
                self, write_offset
              ](auto strand) {
                assert(strand.running_in_this_thread());
                return when_all(io::write_at_ec(self->file, write_offset, link_span), just(strand));
              })
          | let_value(
              // After the second write, we flush-to-disk the written data.
              [self]([[maybe_unused]] std::size_t wlen, [[maybe_unused]] auto strand) {
                assert(strand.running_in_this_thread());
                return self->wal_flusher_.lazy_flush(true);
              });
        });
  };

  auto [acceptor, sender] = commit_space_late_binding_t{}();

  auto self_link = self->link;
  auto link_completion = when_all(
      not_on_strand(strand, std::move(self_link)), // Our link is not actually reachable, unless all preceding links have also been written.
      not_on_strand(strand, std::move(sender)) | let_done(make_recovery_chain))
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
  link_sender new_link = make_link_sender(on(strand, std::move(link_completion))); // This can fail.
                                                                                   // Failure is fine: we've merely grown the file, and this class
                                                                                   // is designed to operate correctly even if the file has been grown.
  link_sender preceding_link = std::exchange(self->link, new_link); // Must not throw. XXX confirm that it doesn't throw
  self->end_offset = new_end_offset; // Never throws
  return prepared_space(self, strand, write_offset, write_len, std::move(acceptor), preceding_link, new_link);
}


auto wal_file_entry::open(dir d, std::filesystem::path name, allocator_type alloc)
-> execution::type_erased_sender<
    std::variant<std::tuple<gsl::not_null<std::shared_ptr<wal_file_entry>>>>,
    std::variant<std::exception_ptr, std::error_code>,
    false> {
  using namespace execution;
  using pos_stream = positional_stream_adapter<fd_type&>;

  return just(get_wal_logger(), d, name, alloc)
  | lazy_observe_value(
      [](
          const gsl::not_null<std::shared_ptr<spdlog::logger>>& logger,
          [[maybe_unused]] const dir& d,
          const std::filesystem::path& name,
          [[maybe_unused]] const allocator_type& alloc) {
        logger->info("opening wal file {}", name.native());
      })
  | lazy_observe_value(
      [](
          [[maybe_unused]] const gsl::not_null<std::shared_ptr<spdlog::logger>>& logger,
          [[maybe_unused]] const dir& d,
          const std::filesystem::path& name,
          [[maybe_unused]] const allocator_type& alloc) {
        if (name.has_parent_path()) throw std::runtime_error("wal file must be a filename");
      })
  | then(
      [](
          gsl::not_null<std::shared_ptr<spdlog::logger>> logger,
          const dir& d,
          std::filesystem::path name,
          allocator_type alloc) {
        fd_type fd;
        std::error_code ec;
        fd.open(d, name, fd_type::READ_WRITE, ec);
        return std::make_tuple(ec, logger, std::move(fd), name, alloc);
      })
  | explode_tuple()
  | handle_error_code()
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
  | handle_error_code()
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
                    | handle_error_code());
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


auto wal_file_entry::create(dir d, const std::filesystem::path& name, std::uint_fast64_t sequence, allocator_type alloc)
-> execution::type_erased_sender<
    std::variant<std::tuple<
        gsl::not_null<std::shared_ptr<wal_file_entry>>,
        execution::late_binding_t<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>::acceptor>>,
    std::variant<std::exception_ptr, std::error_code>,
    false> {
  using namespace execution;
  using pos_stream = positional_stream_adapter<fd_type&>;

  return just(get_wal_logger(), d, name, sequence, alloc)
  | lazy_observe_value(
      [](
          const gsl::not_null<std::shared_ptr<spdlog::logger>>& logger,
          [[maybe_unused]] const dir& d,
          const std::filesystem::path& name,
          std::uint_fast64_t sequence,
          [[maybe_unused]] const allocator_type& alloc) {
        logger->info("creating wal file[{}] {}", sequence, name.native());
      })
  | validation(
      [](
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
          gsl::not_null<std::shared_ptr<spdlog::logger>> logger,
          const dir& d,
          std::filesystem::path name,
          std::uint_fast64_t sequence,
          allocator_type alloc) {
        fd_type fd;
        std::error_code ec;
        fd.create(d, name, ec);
        return std::make_tuple(ec, logger, std::move(fd), name, sequence, alloc);
      })
  | explode_tuple()
  | handle_error_code()
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
  | handle_error_code()
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

auto wal_file_entry::durable_append_(
    gsl::not_null<std::shared_ptr<wal_file_entry>> wf,
    write_records_buffer_t&& records,
    typename wal_file_entry_file::callback transaction_validation,
    earnest::move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>, false>(records_vector)> on_successful_write_callback,
    wal_file_entry_file::prepared_space&& space)
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
    auto converted_records = records.converted_records(fd, write_offset);
    space.set_update_callback(
        [ on_successful_write_callback=std::move(on_successful_write_callback),
          write_offset=space.offset(),
          fake_link_in=wf->fake_link,
          converted_records=std::move(converted_records),
          fd
        ]() mutable {
          return fake_link_in
          | let_value(
              [ on_successful_write_callback=std::move(on_successful_write_callback),
                converted_records=std::move(converted_records)
              ]() mutable {
                return on_successful_write_callback(std::move(converted_records));
              });
        });
  }

  auto new_link = space.get_new_link();
  auto write_op =
      just(std::move(space), std::move(records).bytes())
      | lazy_let_value(
          [](wal_file_entry_file::prepared_space& space, auto& records_bytes) {
            return std::move(space).write(records_bytes);
          });
  wf->fake_link = std::move(new_link);
  return write_op;
}

auto wal_file_entry::non_durable_append_(
    gsl::not_null<std::shared_ptr<wal_file_entry>> wf,
    write_records_buffer_t&& records,
    typename wal_file_entry_file::callback transaction_validation,
    earnest::move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>, false>(records_vector)> on_successful_write_callback,
    wal_file_entry_file::prepared_space&& space)
-> execution::sender_of<> auto {
  using namespace execution;

  // Make a copy of fake-link.
  // (Not really required, but it's easier to read the code if we use the name consistently.
  // Compiler will most-likely optimize the copy away.)
  auto fake_link_in = std::as_const(wf->fake_link);
  // Create a deferred link.
  // We would prefer using execution::split, but that eats away the scheduler.
  // And we really require the scheduler to propagate.
  auto [fake_link_out_acceptor, fake_link_out] = execution::late_binding<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>();

  // We need to capture the validation-result,
  // so we can do our write, and let it re-emerge later.
  using captured_validation_result_t = std::variant<std::tuple<set_value_t>, std::tuple<set_error_t, std::error_code>, std::tuple<set_error_t, std::exception_ptr>, std::tuple<set_done_t>>;

  // We need the operation to run regardless of if the caller wants it to run.
  // So we use a late-binding connection, to allow the operation to be canceled, yet do run.
  auto [operation_acceptor, operation_sender] = execution::late_binding<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>();

  auto operation = lazy_split(
      on(wf->strand_,
          not_on_strand(wf->strand_,
              when_all(
                  std::move(operation_sender)
                  | let_variant(
                      [fake_link_in, transaction_validation=std::move(transaction_validation)]() mutable {
                        auto do_validation = [&]() {
                          return std::move(fake_link_in)
                          | let_value([&transaction_validation]() { return std::invoke(transaction_validation); });
                        };
                        auto skip_validation = []() { return just(); };
                        using variant_type = std::variant<decltype(do_validation()), decltype(skip_validation())>;
                        if (transaction_validation == nullptr)
                          return variant_type(skip_validation());
                        else
                          return variant_type(do_validation());
                      })
                  | let<set_value_t, set_error_t, set_done_t>(
                      [](auto tag, auto&... args) {
                        return just(
                            captured_validation_result_t(
                                std::in_place_type<std::tuple<std::remove_cvref_t<decltype(tag)>, std::remove_cvref_t<decltype(args)>...>>,
                                tag, std::move(args)...));
                      }),
                  just(fake_link_in, wf, std::move(records), std::move(on_successful_write_callback), std::move(space)))
              | then(
                  // Handle cancelation/error.
                  // In this case, we want to not do a write.
                  // But we must do a write.
                  // So instead, we write a skip-record.
                  [](captured_validation_result_t captured_validation_result, auto fake_link_in, gsl::not_null<std::shared_ptr<wal_file_entry>> wf, write_records_buffer_t records, auto on_successful_write_callback, auto space) {
                    if (!std::holds_alternative<std::tuple<set_value_t>>(captured_validation_result)) {
                      // When the write is canceled, we want to pretend a skip-record is to be written instead.
                      // So we replace the records with that record.
                      const auto records_bufsize = records.bytes().size();
                      records.clear();
                      if (records_bufsize == 4) {
                        records.push_back(wal_record_noop{});
                      } else if (records_bufsize - 8u <= 0xffff'ffffu) {
                        assert(records_bufsize >= 8);
                        records.push_back(wal_record_skip32{std::uint32_t(records_bufsize) - 8u});
                      } else {
                        assert(records_bufsize - 12u <= 0xffff'ffff'ffff'ffffull);
                        assert(records_bufsize >= 12);
                        records.push_back(wal_record_skip64{records_bufsize - 12u});
                      }
                      assert(records_bufsize == records.bytes().size()); // We want the undo record to take up the space of the original record.

                      // We're not going to do a write-callback, seeing as the write is canceled.
                      // So might as well drop the callback now.
                      on_successful_write_callback = nullptr;
                    }

                    return std::make_tuple(
                        captured_validation_result,
                        fake_link_in,
                        wf,
                        std::move(records),
                        std::move(on_successful_write_callback),
                        std::move(space));
                  })
              | explode_tuple())
          | let_variant(
              [](captured_validation_result_t captured_validation_result, auto& fake_link_in, gsl::not_null<std::shared_ptr<wal_file_entry>> wf, write_records_buffer_t& records, auto& on_successful_write_callback, auto& space) {
                assert(wf->strand_.running_in_this_thread());

                const auto write_offset = space.offset();
                std::span<const std::byte> bytes = wf->unwritten_data.add(write_offset, std::move(records).bytes());

                // Ensures even cancellation will cause a fail-state (needed, because we've published the write in unwritten-data).
                // We only do this if we write is to happen.
                // If the write is canceled, and our cancelation record fails,
                // the space-operation will write its own undo record which is indistinguishable from the one we're writing
                // (but it has less good performance).
                if (std::holds_alternative<std::tuple<set_value_t>>(captured_validation_result))
                  space.set_must_succeed(true);

                auto do_write_callback = [&]() {
                  auto converted_records = records.converted_records(std::shared_ptr<const fd_type>(wf.get(), &std::as_const(wf->file).file_ref()), write_offset);
                  return not_on_strand(wf->strand_,
                      fake_link_in // Have to wait for preceding writes to complete, before we can do the write.
                      | let_value(
                          [ on_successful_write_callback=std::move(on_successful_write_callback),
                            converted_records=std::move(converted_records)
                          ]() mutable {
                            return std::invoke(on_successful_write_callback, std::move(converted_records));
                          })
                      | then(
                          [captured_validation_result]() mutable -> captured_validation_result_t {
                            return std::move(captured_validation_result);
                          }));
                };
                auto skip_write_callback = [&]() {
                  // We must wait for preceding writes to complete.
                  // (If we don't, we claim the fake-write is done, but it won't be reachable.)
                  return not_on_strand(wf->strand_,
                      fake_link_in
                      | then(
                          [captured_validation_result]() mutable -> captured_validation_result_t {
                            return std::move(captured_validation_result);
                          }));
                };
                using variant_type = std::variant<decltype(do_write_callback()), decltype(skip_write_callback())>;

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
                        | observe_error(
                            [wf]([[maybe_unused]] const auto& error) {
                              if (wf->state_ == wal_file_entry_state::ready) wf->state_ = wal_file_entry_state::failed;
                            })
                        | observe_value(
                            // We can now rely on file reads to provide this data.
                            // (We only do this for successful writes. If the write fails,
                            // we'll maintain read access via the unwritten-data, in order
                            // not to upset the read invariant.)
                            [wf, write_offset, bytes]() -> void {
                              assert(wf->strand_.running_in_this_thread());
                              wf->unwritten_data.remove(write_offset, bytes);
                            }))
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
                        }));

                if (on_successful_write_callback == nullptr)
                  return variant_type(skip_write_callback());
                else
                  return variant_type(do_write_callback());
              })));

  // Install the new link.
  // We don't actually care about the captured-validation-result
  // (because that's for the code that did the write, not for the links).
  //
  // Note: we have the new link depend on the preceding link, by having the operation not-complete until the preceding link completes.
  // So we've maintained the invariant that any link depends on its preceding link.
  wf->fake_link = make_link_sender(
      on(wf->strand_,
          operation
          | then([]([[maybe_unused]] captured_validation_result_t) -> void {})));

  // The returned sender needs to re-instate the captured-validation-result.
  return not_on_strand(wf->strand_,
      just(std::move(operation_acceptor), operation)
      | then(
          [](auto operation_acceptor, auto operation) {
            operation_acceptor.assign_values(); // Start the operation.
            return operation;
          })
      | let_value(
          [](auto& operation) -> decltype(auto) {
            return std::move(operation);
          })
      | let_variant(
          // Unwrap the captured-validation-result.
          [](captured_validation_result_t captured_validation_result) {
            auto value_fn = []([[maybe_unused]] set_value_t tag) {
              return just();
            };
            auto done_fn = []([[maybe_unused]] set_done_t tag) {
              return just_done<>();
            };
            auto exception_fn = []([[maybe_unused]] set_error_t tag, std::exception_ptr ex) {
              return just_error<>(std::move(ex));
            };
            auto errorcode_fn = []([[maybe_unused]] set_error_t tag, std::error_code ec) {
              return just_error<>(std::move(ec));
            };
            using variant_type = std::variant<
                decltype(value_fn(std::declval<set_value_t>())),
                decltype(done_fn(std::declval<set_done_t>())),
                decltype(exception_fn(std::declval<set_error_t>(), std::declval<std::exception_ptr>())),
                decltype(errorcode_fn(std::declval<set_error_t>(), std::declval<std::error_code>()))>;

            return std::visit(
                [&](auto&& arg_tuple) -> variant_type {
                  return std::apply(
                      overload(value_fn, done_fn, exception_fn, errorcode_fn),
                      arg_tuple);
                },
                std::move(captured_validation_result));
          }));
}

auto wal_file_entry::append(
    write_records_buffer_t records,
    typename wal_file_entry_file::callback transaction_validation,
    earnest::move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>, false>(records_vector)> on_successful_write_callback,
    bool delay_flush)
-> execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>> {
  using namespace execution;

  gsl::not_null<std::shared_ptr<wal_file_entry>> wf = this->shared_from_this();
  return on(
      strand_,
      just(wf, std::move(records), delay_flush, std::move(transaction_validation), std::move(on_successful_write_callback))
      | lazy_validation(
          [](gsl::not_null<std::shared_ptr<wal_file_entry>> wf, [[maybe_unused]] const auto&... ignored_args) -> std::optional<std::error_code> {
            // We only permit writes when we're in the ready state.
            switch (wf->state_) {
              default:
                return make_error_code(wal_errc::bad_state);
              case wal_file_entry_state::failed:
                return make_error_code(wal_errc::unrecoverable);
              case wal_file_entry_state::ready:
                return std::nullopt;
            }
#if __cpp_lib_unreachable >= 202202L
            std::unreachable();
#endif
          })
      | let_variant(
          [](gsl::not_null<std::shared_ptr<wal_file_entry>> wf, write_records_buffer_t& records,
              bool delay_flush, auto& transaction_validation, auto& on_successful_write_callback) {
            auto space = wf->file.prepare_space(
                std::shared_ptr<wal_file_entry_file>(wf.get(), &wf->file),
                wf->strand_,
                std::as_const(records).bytes().size());

            // Durable writes are "immediate", meaning the sender-chain is linked to the write-to-disk operation completing.
            //
            // Note that durable-append takes the records by reference, since it yields only a single chain, and thus can be guaranteed
            // the records remain valid for the duration of the chain.
            auto immediate = [&]() {
              assert(delay_flush == false);
              return durable_append_(wf, std::move(records), std::move(transaction_validation), std::move(on_successful_write_callback), std::move(space));
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

auto wal_file_entry::seal()
-> execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>> {
  using namespace execution;

  gsl::not_null<std::shared_ptr<wal_file_entry>> wf = this->shared_from_this();
  return on(
      strand_,
      just(wf)
      | lazy_validation(
          [](gsl::not_null<std::shared_ptr<wal_file_entry>> wf) -> std::optional<std::error_code> {
            // We only permit seals when we're in the ready state.
            switch (wf->state_) {
              default:
                return make_error_code(wal_errc::bad_state);
              case wal_file_entry_state::failed:
                return make_error_code(wal_errc::unrecoverable);
              case wal_file_entry_state::ready:
                return std::nullopt;
            }
#if __cpp_lib_unreachable >= 202202L
            std::unreachable();
#endif
          })
      | then(
          [](gsl::not_null<std::shared_ptr<wal_file_entry>> wf) {
            write_records_buffer_t records(wf->get_allocator());
            records.push_back(wal_record_seal{});
            return std::make_tuple(wf, records);
          })
      | explode_tuple()
      | let_value(
          [](gsl::not_null<std::shared_ptr<wal_file_entry>> wf, write_records_buffer_t& records) {
            auto space = wf->file.prepare_space(
                std::shared_ptr<wal_file_entry_file>(wf.get(), &wf->file),
                wf->strand_,
                std::as_const(records).bytes().size());

            auto sender_chain = durable_append_(wf, std::move(records), nullptr, nullptr, std::move(space))
            | observe<set_value_t, set_error_t, set_done_t>(
                [wf]<typename Tag>(Tag tag, [[maybe_unused]] const auto&... args) {
                  if (wf->state_ == wal_file_entry_state::failed) return;

                  assert(wf->state_ == wal_file_entry_state::sealing);
                  if constexpr(std::same_as<set_value_t, Tag>)
                    wf->state_ = wal_file_entry_state::sealed;
                  else
                    wf->state_ = wal_file_entry_state::ready;
                })
            | validation(
                [wf]() -> std::optional<std::error_code> {
                  if (wf->state_ == wal_file_entry_state::failed) return make_error_code(wal_errc::unrecoverable);
                  return std::nullopt;
                });

            wf->state_ = wal_file_entry_state::sealing;
            return sender_chain;
          }));
}

auto wal_file_entry::records(move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>(variant_type)> acceptor) const
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

auto wal_file_entry::records() const -> execution::type_erased_sender<std::variant<std::tuple<records_vector>>, std::variant<std::exception_ptr, std::error_code>> {
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


} /* namespace earnest::detail */