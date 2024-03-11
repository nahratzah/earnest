#include <earnest/detail/wal_file_entry.h>

namespace earnest::detail {


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
