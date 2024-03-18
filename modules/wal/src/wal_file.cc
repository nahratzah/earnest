#include <earnest/detail/wal_file.h>

#include <stdexcept>

namespace earnest::detail {


auto wal_file::tx_lock::do_reset_(const state& s) noexcept -> void {
  using namespace execution;

  // We use an operation-state to ensure the state is unlocked exactly once.
  class operation_state
  : private state
  {
    public:
    explicit operation_state(const state& s) noexcept
    : state(s)
    {}

    operation_state(const operation_state&) = delete;

    operation_state(operation_state&& y) noexcept
    : state(y),
      need_to_run(std::exchange(y.need_to_run, false))
    {}

    ~operation_state() {
      if (need_to_run) {
        std::lock_guard lck{wf->strand_};
        strand_release();
      }
    }

    auto strand_release() noexcept -> void {
      assert(wf->strand_.running_in_this_thread());
      assert(need_to_run);
      if (--write_entry->wal_file_data.locks == 0u) wf->maybe_run_apply_();
      if (--read_entry->wal_file_data.read_locks == 0u) wf->maybe_release_entries_();
      need_to_run = false;
    }

    private:
    bool need_to_run = true;
  };

  operation_state os{s}; // Ensure the release will actually happen, and exactly once.

  // We'll try to run the operation-state release on a strand.
  // But if we fail at that, we'll let the operation-state destructor do a blocking release.
  try {
    start_detached(
        s.wf->strand_.wrap(
            just(std::move(os))
            | then(
                [](operation_state&& os) {
                  os.strand_release();
                }))
        | upon_error(
            [](const auto& error) -> void {
              return; // Swallow errors. Operation_state will handle the release.
            }));
  } catch (...) {
    // Discard errors.
    // The operation-state will use a blocking implementation to release the locks.
  }
}


auto wal_file::append(
    write_records_buffer records,
    typename wal_file_entry_file::callback transaction_validation,
    bool delay_flush)
-> execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>> {
  using namespace execution;

  gsl::not_null<std::shared_ptr<wal_file>> wf = this->shared_from_this();
  return wf->strand_.wrap(
      just(wf, std::move(records), std::move(transaction_validation), delay_flush)
      | lazy_let_value(
          [](
              gsl::not_null<std::shared_ptr<wal_file>> wf,
              write_records_buffer& records,
              typename wal_file_entry_file::callback& transaction_validation,
              bool delay_flush) {
            return when_all(
                just(wf, wf->active),
                wf->active->append(
                    std::move(records),
                    std::move(transaction_validation),
                    [wf](wal_file_entry::records_vector records) {
                      return on(wf->strand_,
                          just(wf, std::move(records))
                          | then(
                              [](gsl::not_null<std::shared_ptr<wal_file>> wf, wal_file_entry::records_vector records) {
                                wf->update_cached_file_replacements_(
                                    records
                                    | std::views::filter(
                                        [](wal_file_entry::records_vector::const_reference r) -> bool {
                                          return !wal_record_is_bookkeeping(r);
                                        })
                                    | std::views::transform(
                                        [](wal_file_entry::records_vector::const_reference r) -> nb_variant_type {
                                          return make_wal_record_no_bookkeeping(r);
                                        }));
                              }));
                    },
                    delay_flush));
          })
      | lazy_then(
          [](gsl::not_null<std::shared_ptr<wal_file>> wf, gsl::not_null<std::shared_ptr<wal_file_entry>> wf_active, auto append_op) {
            wf->maybe_auto_rollover_(wf_active, append_op.file_size);
            return append_op;
          }))
  | lazy_let_value([](auto& append_op) { return std::invoke(std::move(append_op)); });
}

auto wal_file::open(dir d, allocator_type alloc)
-> execution::type_erased_sender<
    std::variant<std::tuple<gsl::not_null<std::shared_ptr<wal_file>>>>,
    std::variant<std::exception_ptr, std::error_code>,
    false> {
  using namespace execution;

  struct rollover_state {
    std::unordered_set<std::filesystem::path, fs_path_hash> intent;
    std::optional<std::filesystem::path> ready;
  };

  struct attempted_wal_file_or_error {
    std::variant<gsl::not_null<std::shared_ptr<wal_file_entry>>, std::error_code> file_or_error;
    std::filesystem::path name;
    rollover_state rollover;
  };

  struct attempted_wal_file {
    gsl::not_null<std::shared_ptr<wal_file_entry>> file;
    rollover_state rollover;
  };

  struct bad_file_info {
    std::error_code ec;
    bool intent_declared = false;
    bool rollover_ready = false;
  };
  using bad_files_t = std::unordered_map<std::filesystem::path, bad_file_info, fs_path_hash>;

  return just(get_wal_logger(), std::move(d), std::move(alloc))
  | lazy_let_value(
      [](gsl::not_null<std::shared_ptr<spdlog::logger>>& logger, dir& d, allocator_type& alloc) {
        return gather(
            std::vector<dir::entry>(d.begin(), d.end())
            | std::views::filter([](const dir::entry& e) -> bool { return e.is_regular_file() && e.path().extension() == wal_file_extension; })
            | std::views::transform([](const dir::entry& e) -> std::filesystem::path { return e.path(); })
            | std::views::transform(
                [&](const std::filesystem::path& filename) {
                  return wal_file_entry::open(d, filename, alloc)
                  | let<set_value_t, set_error_t>(
                      overload(
                          [filename]([[maybe_unused]] set_error_t, std::error_code ec) { // Error codes are recorded.
                            return just(
                                attempted_wal_file_or_error{
                                  .file_or_error=ec,
                                  .name=filename,
                                });
                          },
                          []([[maybe_unused]] set_error_t, std::exception_ptr ex) { // Exceptions are passed on unchanged.
                            return just_error<attempted_wal_file_or_error>(ex);
                          },
                          [logger, filename]([[maybe_unused]] set_value_t, gsl::not_null<std::shared_ptr<wal_file_entry>> file) {
                            // Successfully opened a file! Let's fill in the rollover state.
                            return just(
                                file,
                                attempted_wal_file_or_error{
                                  .file_or_error=file,
                                  .name=file->name,
                                })
                            | lazy_let_value(
                                [](gsl::not_null<std::shared_ptr<wal_file_entry>>& file, attempted_wal_file_or_error& wf) {
                                  return file->records(
                                      [&wf](wal_file_entry::variant_type record_variant) {
                                        if (auto intent = std::get_if<wal_record_rollover_intent>(&record_variant))
                                          wf.rollover.intent.insert(intent->filename);

                                        if (auto ready = std::get_if<wal_record_rollover_ready>(&record_variant)) {
                                          if (wf.rollover.ready.has_value()) throw std::runtime_error("rollover declared ready more than once");
                                          wf.rollover.ready = ready->filename;
                                        }

                                        return just();
                                      })
                                  | then([&wf]() { return std::move(wf); });
                                })
                            | observe_error(
                                [logger, filename](const auto& error) {
                                  logger->error("error reading file {}", filename.native());
                                });
                          }));
                }))
        | then(
            [&logger, &d](fixed_vector<attempted_wal_file_or_error> files) {
              // Move all successfully-opened-files to the front, and all failed ones to the rear.
              auto first_bad_file = std::partition(
                  files.begin(), files.end(),
                  [](const attempted_wal_file_or_error& wf) -> bool {
                    return std::holds_alternative<gsl::not_null<std::shared_ptr<wal_file_entry>>>(wf.file_or_error);
                  });

              auto good_files_view = std::ranges::subrange(std::make_move_iterator(files.begin()), std::make_move_iterator(first_bad_file))
              | std::views::transform(
                  [](const attempted_wal_file_or_error& x) {
                    return attempted_wal_file{
                      .file=std::get<0>(x.file_or_error),
                      .rollover=x.rollover,
                    };
                  });

              auto bad_files_view = std::ranges::subrange(first_bad_file, files.end())
              | std::views::transform(
                  [](const attempted_wal_file_or_error& x) {
                    return std::make_pair(x.name, bad_file_info{ .ec=std::get<1>(x.file_or_error) });
                  });

              return std::make_tuple(
                  logger,
                  std::move(d),
                  fixed_vector<attempted_wal_file>(std::ranges::begin(good_files_view), std::ranges::end(good_files_view)),
                  bad_files_t(std::ranges::begin(bad_files_view), std::ranges::end(bad_files_view)));
            })
        | explode_tuple()
        | then(
            [](gsl::not_null<std::shared_ptr<spdlog::logger>> logger, dir d, auto good_files, auto bad_files) {
              std::sort(
                  good_files.begin(), good_files.end(),
                  [](const attempted_wal_file& x, const attempted_wal_file& y) {
                    auto x_sequence = x.file->sequence;
                    auto y_sequence = y.file->sequence;
                    return x_sequence < y_sequence;
                  });
              return std::make_tuple(logger, std::move(d), std::move(good_files), std::move(bad_files));
            })
        | explode_tuple()
        | validation( // Ensure we don't have duplicate sequences.
            [](const gsl::not_null<std::shared_ptr<spdlog::logger>>& logger, [[maybe_unused]] const dir& d, const auto& good_files, [[maybe_unused]] const auto& bad_files) -> std::optional<std::error_code> {
              const auto same_sequence_iter = std::adjacent_find(
                  good_files.begin(), good_files.end(),
                  [](const attempted_wal_file& x, const attempted_wal_file& y) {
                    auto x_sequence = x.file->sequence;
                    auto y_sequence = y.file->sequence;
                    return x_sequence == y_sequence;
                  });
              if (same_sequence_iter != good_files.end()) {
                logger->error("cannot open WAL: files {} and {} have the same sequence number {}",
                    same_sequence_iter->file->name.native(),
                    std::next(same_sequence_iter)->file->name.native(),
                    same_sequence_iter->file->sequence);
                return make_error_code(wal_errc::unrecoverable);
              }
              return std::nullopt;
            })
        | then(
            [](gsl::not_null<std::shared_ptr<spdlog::logger>> logger, dir d, auto good_files, auto bad_files) {
              bool has_ready_bad_files = false;
              std::ranges::for_each(
                  std::as_const(good_files)
                  | std::views::transform([](const attempted_wal_file& wf) -> const rollover_state& { return wf.rollover; })
                  | std::views::transform([](const rollover_state& rs) -> std::optional<std::filesystem::path> { return rs.ready; })
                  | std::views::filter([](const std::optional<std::filesystem::path>& rollover_ready_filename) -> bool { return rollover_ready_filename.has_value(); })
                  | std::views::transform([](const std::optional<std::filesystem::path>& rollover_ready_filename) { return rollover_ready_filename.value(); }),
                  [&bad_files, &has_ready_bad_files, &logger](const std::filesystem::path& rollover_ready_filename) -> void {
                    auto bad_files_ref = bad_files.find(rollover_ready_filename);
                    if (bad_files_ref != bad_files.end()) {
                      logger->error("cannot open WAL: {} should be ready, but failed at reading it: {}", rollover_ready_filename.native(), bad_files_ref->second.ec.message());
                      bad_files_ref->second.rollover_ready = true;
                      has_ready_bad_files = true;
                    }
                  });

              std::ranges::for_each(
                  std::as_const(good_files)
                  | std::views::transform([](const attempted_wal_file& wf) -> const rollover_state& { return wf.rollover; })
                  | std::views::transform([](const rollover_state& rs) -> decltype(auto) { return rs.intent; }),
                  [&bad_files](const auto& intent) {
                    std::ranges::for_each( // Nested for-each, because we don't have join.
                        intent,
                        [&bad_files](const std::filesystem::path& rollover_intent_filename) -> void {
                          auto bad_files_ref = bad_files.find(rollover_intent_filename);
                          if (bad_files_ref != bad_files.end()) bad_files_ref->second.intent_declared = true;
                        });
                  });

              return std::make_tuple(logger, std::move(d), std::move(good_files), std::move(bad_files), std::move(has_ready_bad_files));
            })
        | explode_tuple()
        | validation(
            [](
                [[maybe_unused]] gsl::not_null<std::shared_ptr<spdlog::logger>> logger,
                [[maybe_unused]] const dir& d,
                [[maybe_unused]] const auto& good_files,
                [[maybe_unused]] const auto& bad_files,
                bool has_ready_bad_files)
            -> std::optional<std::error_code> {
              if (has_ready_bad_files) return std::error_code(wal_errc::unrecoverable);
              return std::nullopt;
            })
        | then(
            [](gsl::not_null<std::shared_ptr<spdlog::logger>> logger, dir d, auto good_files, auto bad_files, [[maybe_unused]] bool has_ready_bad_files) {
              return std::make_tuple(logger, std::move(d), std::move(good_files), std::move(bad_files));
            })
        | explode_tuple()
        | then( // Erase any bad files that are declared but not used.
            [](gsl::not_null<std::shared_ptr<spdlog::logger>> logger, dir d, auto good_files, auto bad_files) {
              bool erase_fail = false;
              std::ranges::for_each(
                  bad_files
                  | std::views::filter([](const auto& bad_file_pair) { return bad_file_pair.second.intent_declared; })
                  | std::views::filter([](const auto& bad_file_pair) { return !bad_file_pair.second.rollover_ready; })
                  | std::views::transform([](const auto& bad_file_pair) { return bad_file_pair.first; }),
                  [&logger, &d, &erase_fail](const std::filesystem::path& filename) {
                    logger->warn("erasing declared-but-never-ready invalid WAL file {}", filename.native());
                    try {
                      d.erase(filename);
                    } catch (const std::system_error& ex) {
                      logger->error("unable to erase invalid WAL file {}: {}", filename.native(), ex.what());
                      erase_fail = true;
                    }
                  });

              // Emit a warning about any remaining bad files.
              std::ranges::for_each(
                  bad_files
                  | std::views::filter([](const auto& bad_file_pair) { return bad_file_pair.second.intent_declared; })
                  | std::views::filter([](const auto& bad_file_pair) { return !bad_file_pair.second.rollover_ready; })
                  | std::views::transform([](const auto& bad_file_pair) { return bad_file_pair.first; }),
                  [&logger, &d](const std::filesystem::path& filename) {
                    logger->warn("invalid WAL file {} was never declared, will ignore", filename.native());
                  });

              return std::make_tuple(logger, std::move(d), std::move(good_files), erase_fail);
            })
        | explode_tuple()
        | validation(
            [](
                [[maybe_unused]] gsl::not_null<std::shared_ptr<spdlog::logger>> logger,
                [[maybe_unused]] const dir& d,
                [[maybe_unused]] const auto& good_files,
                bool erase_fail)
            -> std::optional<std::error_code> {
              if (erase_fail) return make_error_code(wal_errc::recovery_failure);
              return std::nullopt;
            })
        | then(
            [](gsl::not_null<std::shared_ptr<spdlog::logger>> logger, dir d, auto good_files, [[maybe_unused]] bool erase_fail) {
              return std::make_tuple(logger, std::move(d), std::move(good_files));
            })
        | explode_tuple()
        | validation(
            [](
                [[maybe_unused]] gsl::not_null<std::shared_ptr<spdlog::logger>> logger,
                [[maybe_unused]] const dir& d,
                const auto& good_files)
            -> std::optional<std::error_code> {
              if (good_files.empty()) return make_error_code(wal_errc::no_data_files);
              return std::nullopt;
            })
        | validation(
            [](
                [[maybe_unused]] gsl::not_null<std::shared_ptr<spdlog::logger>> logger,
                [[maybe_unused]] const dir& d,
                const auto& good_files)
            -> std::optional<std::error_code> {
              bool bad = false;
              std::for_each( // using for_each instead of any_of, so we can report on all files.
                  good_files.cbegin(), good_files.cend(),
                  [&bad, &logger](const attempted_wal_file& wf) {
                    if (wf.rollover.ready.has_value()) return;
                    if (wf.file->state() != wal_file_entry_state::sealed) return;

                    logger->error("recovery failure: {} is sealed, but did not declare a successor", wf.file->name.native());
                    bad = true;
                  });
              if (bad) return make_error_code(wal_errc::recovery_failure);
              return std::nullopt;
            })
        | let_variant(
            [](gsl::not_null<std::shared_ptr<spdlog::logger>> logger, dir& d, auto& good_files) {
              using good_files_t = std::remove_cvref_t<decltype(good_files)>;

              // Newly opened files can't be in the sealing state.
              assert(std::none_of(
                      good_files.begin(), good_files.end(),
                      [](const attempted_wal_file& wf) { return wf.file->state() == wal_file_entry_state::sealing; }));

              // Find the first unsealed file.
              auto first_unsealed = std::find_if(
                  good_files.begin(), good_files.end(),
                  [](const attempted_wal_file& wf) { return wf.file->state() == wal_file_entry_state::ready; });

              auto do_nothing = [&]() {
                return just(logger, std::move(d), std::move(good_files));
              };
              auto fail = [&]() {
                return just_error<std::remove_cvref_t<decltype(logger)>, std::remove_cvref_t<decltype(d)>, std::remove_cvref_t<decltype(good_files)>>(
                    make_error_code(wal_errc::recovery_failure));
              };
              auto multiple_unsealed = [&]() {
                assert(!first_unsealed->rollover.ready.has_value());

                // Erase any of the files that cannot hold commited data.
                bool erase_fail = false;
                std::ranges::for_each(
                    std::ranges::subrange(std::next(first_unsealed), good_files.end())
                    | std::views::transform([](const attempted_wal_file& wf) { return wf.file; }),
                    [&logger, &d, &erase_fail](auto file) {
                      logger->warn("erasing ({}) WAL file {} because it only has uncommited records", file->state(), file->name.native());
                      file->close();

                      try {
                        d.erase(file->name);
                      } catch (const std::system_error& ex) {
                        logger->error("unable to erase WAL file {}: {}", file->name.native(), ex.what());
                        erase_fail = true;
                      }
                    });

                return just(
                    logger,
                    std::move(d),
                    good_files_t(
                        std::make_move_iterator(good_files.begin()), std::make_move_iterator(std::next(first_unsealed)),
                        good_files.get_allocator()))
                | validation(
                    [erase_fail]([[maybe_unused]] const auto&... args) -> std::optional<std::error_code> {
                      if (erase_fail) return make_error_code(wal_errc::recovery_failure);
                      return std::nullopt;
                    });
              };
              auto seal_and_discard = [&]() {
                return when_all(
                    // We discard all data in the successor...
                    std::prev(first_unsealed)->file->discard_all(write_records_buffer{}, false)
                    // ... and (only once the discarding completes) seal the active file.
                    | lazy_let_value([file=first_unsealed->file]() { return file->seal(); }),
                    just(
                        logger,
                        std::move(d),
                        good_files_t(
                            std::make_move_iterator(good_files.begin()), std::make_move_iterator(std::next(first_unsealed)),
                            good_files.get_allocator())));
              };

              using variant_type = std::variant<
                  decltype(do_nothing()),
                  decltype(fail()),
                  decltype(multiple_unsealed()),
                  decltype(seal_and_discard())>;

              if (first_unsealed == good_files.end()) {
                logger->error("recovery failure: missing WAL file {}", good_files.back().rollover.ready.value().native());
                return variant_type(fail());
              } else if (std::next(first_unsealed) == good_files.end()) {
                if (first_unsealed->rollover.ready.has_value()) {
                  logger->error("recovery failure: missing WAL file {}", first_unsealed->rollover.ready.value().native());
                  return variant_type(fail());
                }
                logger->info("up to date (last file {} is accepting writes)", good_files.back().file->name.native());
                return variant_type(do_nothing());
              } else if (!first_unsealed->rollover.ready.has_value()) {
                logger->warn("multiple unsealed files, recovery required");
                return variant_type(multiple_unsealed());
              } else {
                const auto& expected_name = first_unsealed->rollover.ready.value();
                const auto expected_seq = first_unsealed->file->sequence + 1u;
                auto need_sealing = first_unsealed->file;
                ++first_unsealed;

                if (first_unsealed->file->name != expected_name) {
                  logger->error("recovery failure: expected {} (seq={}), but next file is {} (seq={})",
                      expected_name.native(), expected_seq,
                      first_unsealed->file->name.native(), first_unsealed->file->sequence);
                  return variant_type(fail());
                } else if (need_sealing->sequence + 1u != first_unsealed->file->sequence) {
                  logger->error("recovery failure: invalid sequence progression ({} seq={}, {} seq={})",
                      need_sealing->name.native(), need_sealing->sequence,
                      first_unsealed->file->name.native(), first_unsealed->file->sequence);
                  return variant_type(fail());
                }

                logger->info("will seal {} (seq {}) and discard {} (seq {})",
                    need_sealing->name.native(), need_sealing->sequence,
                    first_unsealed->file->name.native(), first_unsealed->file->sequence);
                return variant_type(seal_and_discard());
              }
            })
        | let_value(
            [alloc](gsl::not_null<std::shared_ptr<spdlog::logger>>& logger, dir& d, auto& good_files) {
              auto entries_end = std::prev(good_files.end());

              auto entries_start = entries_end;
              while (entries_start != good_files.begin() && std::prev(entries_start)->file->sequence + 1u == entries_start->file->sequence)
                --entries_start;

              // We now have three ranges:
              // [ good_files.begin() .. entries_start    )  -- old entries that should be cleaned up
              // [ entries_start      .. entries_end      )  -- entries that were active and need to be applied
              // [ entries_end        .. good_files.end() )  -- single entry that's the active entry

              return compute_cached_file_replacements_(
                  std::ranges::subrange(entries_start, good_files.end())
                  | std::views::transform([](const attempted_wal_file& wf) { return wf.file; }),
                  alloc)
              | then(
                  [&logger, &d, &good_files, &alloc, entries_start, entries_end](
                      gsl::not_null<std::shared_ptr<file_replacements>> cached_file_replacements)
                  -> gsl::not_null<std::shared_ptr<wal_file>> {
                    return std::allocate_shared<wal_file>(alloc,
                        std::move(d),
                        std::ranges::subrange(good_files.begin(), entries_start) // old entries (never been applied)
                        | std::views::transform([](const auto& x) { return x.file->name; }),
                        std::ranges::subrange(entries_start, entries_end) // new entries (to be applied)
                        | std::views::transform([](const auto& x) { return x.file; }),
                        entries_end->file, // active
                        cached_file_replacements,
                        logger, alloc);
                  });
            })
        | observe_value(
            [](gsl::not_null<std::shared_ptr<wal_file>> wf) -> void {
              std::lock_guard lck{wf->strand_}; // Won't block.
              wf->maybe_auto_rollover_(wf->active, wf->active->end_offset());
              wf->maybe_run_apply_();
            });
      });
}

auto wal_file::create(dir d, allocator_type alloc)
-> execution::type_erased_sender<
    std::variant<std::tuple<gsl::not_null<std::shared_ptr<wal_file>>>>,
    std::variant<std::exception_ptr, std::error_code>,
    false> {
  using namespace execution;

  std::filesystem::path new_filename = fmt::format(std::locale::classic(), "earnest-wal-{:016}{}", 0, wal_file_extension);
  return when_all(wal_file_entry::create(d, new_filename, 0, alloc), just(d, alloc))
  | then(
      [](gsl::not_null<std::shared_ptr<wal_file_entry>> wfe, auto acceptor, dir d, allocator_type alloc) -> gsl::not_null<std::shared_ptr<wal_file>> {
        acceptor.assign_values();
        return std::allocate_shared<wal_file>(alloc, std::move(d), wfe, get_wal_logger(), alloc);
      });
}

auto wal_file::records(move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>(record_type)> acceptor) const
-> execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>> {
  using namespace execution;

  gsl::not_null<std::shared_ptr<const wal_file>> wf = this->shared_from_this();
  return strand_.wrap(
      just(wf, std::move(acceptor))
      | lazy_then(
          [](gsl::not_null<std::shared_ptr<const wal_file>> wf, move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>(record_type)> acceptor) {
            struct repeat_state {
              std::vector<gsl::not_null<std::shared_ptr<const wal_file_entry>>> entries;
              move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>(record_type)> acceptor;
            };

            repeat_state rs{
              .acceptor=std::move(acceptor),
            };
            rs.entries.reserve(wf->entries.size() + 1u);
            std::ranges::copy(wf->entries, std::back_inserter(rs.entries));
            rs.entries.emplace_back(wf->active);

            return rs;
          }))
  | lazy_repeat(
      [](std::size_t idx, auto& rs) {
        auto do_the_thing = [&]() {
          assert(idx < rs.entries.size());
          return rs.entries[idx]->records(
              [&rs](wal_file_entry::variant_type r) {
                if (!wal_record_is_bookkeeping(r))
                  rs.acceptor(make_wal_record_no_bookkeeping(std::move(r)));
                return just();
              });
        };
        using optional_type = std::optional<decltype(do_the_thing())>;

        if (idx == rs.entries.size())
          return optional_type(std::nullopt);
        else
          return optional_type(do_the_thing());
      })
  | discard_value();
}

auto wal_file::raw_records(move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>(wal_file_entry::variant_type)> acceptor) const
-> execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>> {
  using namespace execution;

  gsl::not_null<std::shared_ptr<const wal_file>> wf = this->shared_from_this();
  return strand_.wrap(
      just(wf, std::move(acceptor))
      | lazy_then(
          [](gsl::not_null<std::shared_ptr<const wal_file>> wf, move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>(wal_file_entry::variant_type)> acceptor) {
            struct repeat_state {
              std::vector<gsl::not_null<std::shared_ptr<const wal_file_entry>>> entries;
              move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>(wal_file_entry::variant_type)> acceptor;
            };

            repeat_state rs{
              .acceptor=std::move(acceptor),
            };
            rs.entries.reserve(wf->entries.size() + 1u);
            std::ranges::copy(wf->entries, std::back_inserter(rs.entries));
            rs.entries.emplace_back(wf->active);

            return rs;
          }))
  | lazy_repeat(
      [](std::size_t idx, auto& rs) {
        auto do_the_thing = [&]() {
          assert(idx < rs.entries.size());
          return rs.entries[idx]->records(std::ref(rs.acceptor));
        };
        using optional_type = std::optional<decltype(do_the_thing())>;

        if (idx == rs.entries.size())
          return optional_type(std::nullopt);
        else
          return optional_type(do_the_thing());
      })
  | discard_value();
}

auto wal_file::maybe_run_apply_() const noexcept -> void {
  using namespace execution;

  assert(strand_.running_in_this_thread());
  if (!apply_impl_) return;

  try {
    gsl::not_null<std::shared_ptr<wal_file>> wf = std::const_pointer_cast<wal_file>(this->shared_from_this());
    start_detached(
        on(strand_, // XXX background scheduler
            just(wf)
            | repeat(
                []([[maybe_unused]] std::size_t idx, gsl::not_null<std::shared_ptr<wal_file>> wf) {
                  assert(wf->strand_.running_in_this_thread());

                  auto do_apply = [&]() {
                    assert(!wf->entries.empty());

                    wf->logger->debug("starting apply of {}", wf->entries.front()->name.native());
                    return not_on_strand(wf->strand_, wf->apply_impl_(wf->entries.front()))
                    | observe_value(
                        [wf]() -> void {
                          wf->logger->debug("done with apply of {}", wf->entries.front()->name.native());

                          // XXX increase a metric for successful apply
                          // XXX set a timestamp metric to current time for most-recent-apply

                          wf->old_entries.push_back(wf->entries.front());
                          wf->entries.erase(wf->entries.begin());
                        });
                  };
                  using opt_type = std::optional<decltype(do_apply())>;

                  if (wf->entries.empty() || wf->entries.front()->wal_file_data.locks != 0 || !wf->apply_impl_)
                    return opt_type(std::nullopt);
                  else
                    return opt_type(do_apply());
                })
            | observe<set_value_t, set_error_t, set_done_t>(
                [wf]([[maybe_unused]] auto tag, [[maybe_unused]] const auto&... args) {
                  assert(wf->strand_.running_in_this_thread());
                  wf->apply_running_ = false;
                  wf->logger->debug("apply loop completed");
                }))
        | upon_error( // We swallow errors, since we can simply retry the apply later.
            [wf]<typename Error>(Error err) {
              // XXX increase some metric
              if constexpr(std::same_as<std::exception_ptr, Error>) {
                try {
                  std::rethrow_exception(std::move(err));
                } catch (const std::exception& ex) {
                  wf->logger->error("apply loop failed due to exception: {}", ex.what());
                } catch (...) {
                  wf->logger->error("apply loop failed due to exception"); // We don't know what...
                }
              } else {
                static_assert(std::same_as<std::error_code, Error>);
                wf->logger->error("apply loop failed: {}", err.message());
              }
            })
        | observe_done([wf]() { wf->logger->debug("apply loop stopped"); }));

    const_cast<wal_file&>(*this).apply_running_ = true;
    logger->debug("starting apply loop");
  } catch (const std::exception& ex) {
    logger->error("unable to start apply loop due to exception: {}", ex.what());
  } catch (...) {
    logger->error("unable to start apply loop due to exception"); // We don't know what...
  }
}

auto wal_file::rollover_stage_1_(gsl::not_null<std::shared_ptr<wal_file>> wf, gsl::not_null<std::shared_ptr<wal_file_entry>> active)
-> execution::sender_of<
    gsl::not_null<std::shared_ptr<wal_file>> /*wf*/,
    gsl::not_null<std::shared_ptr<wal_file_entry>> /*old_active*/,
    gsl::not_null<std::shared_ptr<wal_file_entry>> /*new_active*/
> auto {
  using namespace execution;

  return just(wf, active)
  | observe_value(
      [](gsl::not_null<std::shared_ptr<wal_file>> wf, gsl::not_null<std::shared_ptr<wal_file_entry>> active) {
        assert(wf->strand_.running_in_this_thread());
        assert(active->wal_file_data.rollover_running); // Should have been set by caller.
        wf->entries.reserve(wf->entries.size() + 1u); // Reserve space.
      })
  | lazy_let_value(
      // We don't use strand while doing the writes etc.
      [](gsl::not_null<std::shared_ptr<wal_file>>& wf, gsl::not_null<std::shared_ptr<wal_file_entry>>& active) -> typed_sender auto {
        return not_on_strand(wf->strand_,
            just(wf, active)
            | then(
                [](gsl::not_null<std::shared_ptr<wal_file>> wf, gsl::not_null<std::shared_ptr<wal_file_entry>> active) {
                  std::uint64_t new_sequence = active->sequence + 1u;
                  if (new_sequence == 0) throw std::runtime_error("WAL-sequence overflow"); // I don't think this will happen in my lifetime. :P
                  std::filesystem::path new_filename = fmt::format(std::locale::classic(), "earnest-wal-{:016}{}", new_sequence, wal_file_extension);
                  return std::make_tuple(wf, active, new_sequence, new_filename);
                })
            | explode_tuple()
            | lazy_let_value(
                [](gsl::not_null<std::shared_ptr<wal_file>> wf, gsl::not_null<std::shared_ptr<wal_file_entry>> active, std::uint64_t new_sequence, std::filesystem::path new_filename) {
                  write_records_buffer wrb;
                  wrb.push_back(wal_record_rollover_intent{ .filename=new_filename.generic_string() });
                  return when_all(
                      just(wf, active, new_sequence, new_filename),
                      active->append(std::move(wrb))
                      | earnest::execution::lazy_let_value([](auto& append_op) { return std::invoke(std::move(append_op)); }));
                })
            | lazy_let_value(
                [](gsl::not_null<std::shared_ptr<wal_file>> wf, gsl::not_null<std::shared_ptr<wal_file_entry>> active, std::uint64_t new_sequence, std::filesystem::path new_filename) {
                  return when_all(
                      just(wf, active),
                      wal_file_entry::create(wf->dir_, new_filename, active->sequence + 1u));
                })
            | lazy_let_value(
                [](gsl::not_null<std::shared_ptr<wal_file>> wf, gsl::not_null<std::shared_ptr<wal_file_entry>> old_active, gsl::not_null<std::shared_ptr<wal_file_entry>> new_active, auto& link_acceptor) {
                  write_records_buffer wrb;
                  wrb.push_back(wal_record_rollover_ready{ .filename=new_active->name.generic_string() });
                  return when_all(
                      just(wf, old_active, new_active),
                      old_active->append(std::move(wrb))
                      | earnest::execution::lazy_let_value([](auto& append_op) { return std::invoke(std::move(append_op)); }))
                  | observe<set_error_t, set_done_t>(
                      [wf, new_active, &link_acceptor]<typename Tag>(Tag tag, const auto&... error) {
                        // If the `wal_record_rollover_ready` append fails (or is canceled), we must remove the file,
                        // so that a subsequent rollover can try creating the file again.
                        //
                        // Note that once this record has been written, we are commited to leave the file in place, so this is the only place
                        // where the file is erased.
                        wf->logger->debug("rollover: cleaning up not-installed rollover file {}", new_active->name.native());
                        try {
                          std::move(link_acceptor).assign(just_error(make_error_code(wal_errc::unrecoverable)));
                        } catch (...) {
                          std::move(link_acceptor).assign_error(std::current_exception()); // Only throws if `link_acceptor` isn't valid.
                        }
                        wf->dir_.erase(new_active->name);
                      })
                  | observe_value(
                      [](gsl::not_null<std::shared_ptr<wal_file>> wf, [[maybe_unused]] gsl::not_null<std::shared_ptr<wal_file_entry>> old_active, gsl::not_null<std::shared_ptr<wal_file_entry>> new_active) -> void {
                        wf->logger->info("rollover: created new file {}", new_active->name.native());
                      })
                  | observe_value(
                      [&link_acceptor](gsl::not_null<std::shared_ptr<wal_file>> wf, gsl::not_null<std::shared_ptr<wal_file_entry>> old_active, gsl::not_null<std::shared_ptr<wal_file_entry>> new_active) -> void {
                        // If the `wal_record_rollover_ready` succeeds, scribble the successor into the old-active.
                        // This means we can make the rollover call restartable upon failure.
                        //
                        // (Note: we don't need to acquire a lock on a strand, because each file can only have 1 rollover-task running in parallel.)
                        assert(!old_active->wal_file_data.successor.has_value());
                        old_active->wal_file_data.successor.emplace(new_active, std::move(link_acceptor));
                      });
                }));
      });
}

auto wal_file::rollover_sentinel(gsl::not_null<std::shared_ptr<wal_file>> wf, gsl::not_null<std::shared_ptr<wal_file_entry>> old_active) {
  using namespace execution;

  return observe<set_value_t, set_error_t, set_done_t>(
      // At the end of the operation, we clear the rollover-running flag.
      [wf, old_active]<typename Tag>([[maybe_unused]] Tag tag, [[maybe_unused]] const auto&... args) {
        std::string_view extra_message;
        if constexpr(std::same_as<set_error_t, Tag>)
          extra_message = " (failed)";
        else if constexpr(std::same_as<set_done_t, Tag>)
          extra_message = " (canceled)";
        wf->logger->debug("rollover: done with rollover of {}{}", old_active->name.native(), extra_message);
        old_active->wal_file_data.rollover_running = false;
      });
}

auto wal_file::rollover_sender_(gsl::not_null<std::shared_ptr<wal_file_entry>> active) -> execution::sender_of<> auto {
  // The rollover operation consists of two stages:
  // 1. the file creation stage.
  // 2. the swap-over stage.
  using namespace execution;

  gsl::not_null<std::shared_ptr<wal_file>> wf = this->shared_from_this();
  return just(wf, active)
  | let_variant(
      [](gsl::not_null<std::shared_ptr<wal_file>> wf, gsl::not_null<std::shared_ptr<wal_file_entry>> active) {
        auto skip_file_creation = [&]() {
          return just(wf, active, std::get<0>(*active->wal_file_data.successor));
        };

        static_assert(typed_sender<decltype(skip_file_creation())>);
        static_assert(typed_sender<decltype(rollover_stage_1_(wf, active))>);
        using variant_type = std::variant<decltype(skip_file_creation()), decltype(rollover_stage_1_(wf, active))>;

        if (active->wal_file_data.successor.has_value())
          return variant_type(skip_file_creation());
        else
          return variant_type(rollover_stage_1_(wf, active));
      })
  // Stage 2 start here.
  | observe_value(
      // Start the seal operation.
      [](gsl::not_null<std::shared_ptr<wal_file>> wf, gsl::not_null<std::shared_ptr<wal_file_entry>> old_active, gsl::not_null<std::shared_ptr<wal_file_entry>> new_active) -> void {
        assert(wf->strand_.running_in_this_thread());
        assert(old_active->wal_file_data.rollover_running); // Should have been set by caller.
        // Confirm successor has been set up previously.
        assert(old_active->wal_file_data.successor.has_value());
        assert(std::get<0>(*old_active->wal_file_data.successor) == new_active);

        // This operation can throw.
        // If it throws, we can retry later.
        //
        // However, if the seal operation completes with a failure, we'll be in an unrecoverable position.
        // (Because we have to install the next file _now_, or appends will fail with a state-error.)
        std::get<1>(std::move(*old_active->wal_file_data.successor)).assign(old_active->seal()); // May throw.
                                                                                                 // If it throws, we can resume the rollover at a later stage.
      })
  | observe_value(
      // Update the active entry.
      [](gsl::not_null<std::shared_ptr<wal_file>> wf, gsl::not_null<std::shared_ptr<wal_file_entry>> old_active, gsl::not_null<std::shared_ptr<wal_file_entry>> new_active) noexcept -> void {
        assert(wf->active == old_active);
        assert(wf->entries.capacity() > wf->entries.size()); // We reserved capacity in advance.

        wf->entries.push_back(old_active); // Never throws, because we reserved capacity in advance.
        wf->active = new_active; // Never throws.
        old_active->wal_file_data.successor.reset(); // No longer need sentinel.
      })
  | rollover_sentinel(wf, active)
  | discard_value();
}

auto wal_file::maybe_auto_rollover_(gsl::not_null<std::shared_ptr<wal_file_entry>> active, execution::io::offset_type active_file_size) noexcept -> void {
  using namespace execution;

  assert(strand_.running_in_this_thread());
  if (active->wal_file_data.rollover_running) return;
  if (this->active != active) return; // Rollover already happened.
  if (active_file_size < auto_rollover_size_) return; // File isn't large enough to rollover yet.

  try {
    start_detached(on(strand_, rollover_sender_(active))); // XXX use a background scheduler
  } catch (const std::exception& ex) {
    // If we can't start the rollover, we emit a warning.
    // Next write will probably try to start it again.
    // And in the meantime writes will keep going to the current file.
    // So it's allo harmless.
    this->logger->warn("failed to start auto-rollover: {}", ex.what());
    return;
  } catch (...) {
    this->logger->warn("failed to start auto-rollover: (unknown exception)"); // We don't expect this to ever happen.
    return;
  }
  active->wal_file_data.rollover_running = true;
}

auto wal_file::rollover() -> execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>> {
  using namespace execution;

  gsl::not_null<std::shared_ptr<wal_file>> wf = this->shared_from_this();
  return on(strand_,
      just(wf)
      | let_variant(
          [](gsl::not_null<std::shared_ptr<wal_file>> wf) {
            using variant_type = std::variant<decltype(just()), decltype(wf->rollover_sender_(wf->active))>;

            if (wf->active->wal_file_data.rollover_running) {
              return variant_type(just());
            } else {
              variant_type v = wf->rollover_sender_(wf->active);
              wf->active->wal_file_data.rollover_running = true;
              return v;
            }
          }));
}


} /* namespace earnest::detail */
