#pragma once

#include <algorithm>
#include <cstddef>
#include <exception>
#include <filesystem>
#include <functional>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <scoped_allocator>
#include <string_view>
#include <system_error>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <earnest/detail/file_recover_state.h>
#include <earnest/detail/logger.h>
#include <earnest/detail/wal_file_entry.h>
#include <earnest/dir.h>
#include <earnest/file_id.h>
#include <earnest/move_only_function.h>

namespace earnest::detail {


// Work around defect: std::hash<std::filesystem::path> isn't defined.
struct fs_path_hash {
  auto operator()(const std::filesystem::path& p) const noexcept -> std::size_t {
    return std::filesystem::hash_value(p);
  }
};

class wal_file
: public std::enable_shared_from_this<wal_file>,
  with_logger
{
  public:
  static inline constexpr std::string_view wal_file_extension = ".wal";

  using allocator_type = wal_allocator_type;
  using apply_callback = move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>(gsl::not_null<std::shared_ptr<wal_file_entry>>)>;

  private:
  template<typename T>
  using rebind_alloc = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;

  using entry_type = wal_file_entry;
  using nb_variant_type = wal_record_no_bookkeeping<typename entry_type::variant_type>;

  // We would like a deque, for the pop_front functionality.
  // But we can't, because we need to ensure space-availability during rollovers.
  // And deque lacks the `reserve()` function.
  // So we'll have to use a vector.
  using entries_list = std::vector<gsl::not_null<std::shared_ptr<wal_file_entry>>, rebind_alloc<gsl::not_null<std::shared_ptr<wal_file_entry>>>>;

  public:
  using variant_type = typename entry_type::variant_type;
  using write_variant_type = typename entry_type::write_variant_type;
  using record_type = nb_variant_type;
  using records_vector = std::vector<record_type>;
  using write_records_vector = typename entry_type::write_records_vector;
  using fd_type = typename entry_type::fd_type;

  using file_replacements = std::unordered_map<
      file_id,
      detail::file_recover_state<allocator_type>,
      std::hash<file_id>,
      std::equal_to<file_id>,
      std::scoped_allocator_adaptor<rebind_alloc<std::pair<const file_id, detail::file_recover_state<allocator_type>>>>>;

  class tx_lock {
    friend class ::earnest::detail::wal_file;

    private:
    struct state {
      state(
          gsl::not_null<std::shared_ptr<const wal_file>> wf,
          gsl::not_null<std::shared_ptr<wal_file_entry>> read_entry,
          gsl::not_null<std::shared_ptr<wal_file_entry>> write_entry) noexcept
      : write_entry(write_entry),
        read_entry(read_entry),
        wf(wf)
      {}

      friend auto swap(state& x, state& y) noexcept -> void {
        using std::swap;
        swap(x.write_entry, y.write_entry);
        swap(x.read_entry, y.read_entry);
        swap(x.wf, y.wf);
      }

      gsl::not_null<std::shared_ptr<wal_file_entry>> write_entry, read_entry;
      gsl::not_null<std::shared_ptr<const wal_file>> wf;
    };

    public:
    tx_lock() noexcept = default;

    private:
    tx_lock(state s) noexcept;

    public:
    tx_lock(const tx_lock&) = delete;

    tx_lock(tx_lock&& y) noexcept
    : s(std::exchange(y.s, std::nullopt))
    {}

    friend auto swap(tx_lock& x, tx_lock& y) noexcept -> void {
      x.s.swap(y.s);
    }

    auto operator=(tx_lock y) noexcept -> tx_lock& {
      swap(*this, y);
      return *this;
    }

    ~tx_lock() {
      reset();
    }

    auto reset() noexcept -> void {
      if (s.has_value()) {
        do_reset_(*s);
        s.reset();
      }
    }

    private:
    static auto do_reset_(const state& s) noexcept -> void;

    std::optional<state> s;
  };

  wal_file(dir d, gsl::not_null<std::shared_ptr<wal_file_entry>> active, gsl::not_null<std::shared_ptr<spdlog::logger>> logger, allocator_type alloc)
  : with_logger(logger),
    strand_(alloc),
    active(active),
    dir_(std::move(d)),
    very_old_entries(alloc),
    old_entries(alloc),
    entries(alloc),
    cached_file_replacements_(std::allocate_shared<file_replacements>(alloc, alloc))
  {}

  template<std::ranges::range VeryOldEntries, std::ranges::range Entries>
  wal_file(
      dir d, VeryOldEntries&& very_old_entries, Entries&& entries, gsl::not_null<std::shared_ptr<wal_file_entry>> active,
      gsl::not_null<std::shared_ptr<const file_replacements>> cached_file_replacements,
      gsl::not_null<std::shared_ptr<spdlog::logger>> logger, allocator_type alloc)
  : with_logger(logger),
    strand_(alloc),
    active(active),
    dir_(std::move(d)),
    very_old_entries(alloc),
    old_entries(alloc),
    entries(alloc),
    cached_file_replacements_(cached_file_replacements)
  {
    std::ranges::copy(std::forward<VeryOldEntries>(very_old_entries), std::back_inserter(this->very_old_entries));
    std::ranges::copy(std::forward<Entries>(entries), std::back_inserter(this->entries));
  }

  wal_file(const wal_file&) = delete;
  wal_file(wal_file&&) = delete;
  wal_file& operator=(const wal_file&) = delete;
  wal_file& operator=(wal_file&&) = delete;

  auto get_allocator() const -> allocator_type { return entries.get_allocator(); }
  auto get_dir() const -> const dir& { return dir_; }

  [[nodiscard]]
  auto append(
      write_records_buffer records,
      typename wal_file_entry_file::callback transaction_validation = nullptr,
      bool delay_flush = false)
  -> execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>;

  [[nodiscard]]
  static auto open(dir d, allocator_type alloc)
  -> execution::type_erased_sender<
      std::variant<std::tuple<gsl::not_null<std::shared_ptr<wal_file>>>>,
      std::variant<std::exception_ptr, std::error_code>,
      false>;

  [[nodiscard]]
  static auto open(dir d)
  -> execution::type_erased_sender<
      std::variant<std::tuple<gsl::not_null<std::shared_ptr<wal_file>>>>,
      std::variant<std::exception_ptr, std::error_code>,
      false> {
    return create(std::move(d), allocator_type(nullptr, "", prometheus::Labels{}));
  }

  [[nodiscard]]
  static auto create(dir d, allocator_type alloc)
  -> execution::type_erased_sender<
      std::variant<std::tuple<gsl::not_null<std::shared_ptr<wal_file>>>>,
      std::variant<std::exception_ptr, std::error_code>,
      false>;

  [[nodiscard]]
  static auto create(dir d)
  -> execution::type_erased_sender<
      std::variant<std::tuple<gsl::not_null<std::shared_ptr<wal_file>>>>,
      std::variant<std::exception_ptr, std::error_code>,
      false> {
    return create(std::move(d), allocator_type(nullptr, "", prometheus::Labels{}));
  }

  [[nodiscard]]
  auto tx() const -> execution::sender_of<tx_lock, gsl::not_null<std::shared_ptr<const file_replacements>>> auto {
    using namespace execution;

    gsl::not_null<std::shared_ptr<const wal_file>> wf = this->shared_from_this();
    return on(strand_,
        just(wf)
        | then(
            [](gsl::not_null<std::shared_ptr<const wal_file>> wf) -> std::tuple<tx_lock, gsl::not_null<std::shared_ptr<const file_replacements>>> {
              assert(wf->strand_.running_in_this_thread());
              auto read_entry = (wf->entries.empty() ? wf->active : wf->entries.front());
              return {
                tx_lock(tx_lock::state(wf, read_entry, wf->active)),
                wf->cached_file_replacements_
              };
            }))
    | explode_tuple();
  }

  [[nodiscard]]
  auto records(move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>(record_type)> acceptor) const
  -> execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>;

  [[nodiscard]]
  auto records() const
  -> execution::type_erased_sender<std::variant<std::tuple<records_vector>>, std::variant<std::exception_ptr, std::error_code>>;

  [[nodiscard]]
  auto raw_records(move_only_function<execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>(wal_file_entry::variant_type)> acceptor) const
  -> execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>;

  // Assign an apply callback.
  // You can assign nullptr to clear the apply callback.
  //
  // Note that this function won't wait for an active apply to complete.
  auto set_apply_callback(apply_callback callback) -> apply_callback {
    std::lock_guard lck{strand_};
    auto old_callback = std::exchange(apply_impl_, std::move(callback));
    maybe_run_apply_();
    return old_callback;
  }

  auto auto_rollover_size() const -> execution::io::offset_type {
    std::lock_guard lck{strand_};
    return auto_rollover_size_;
  }

  auto auto_rollover_size(std::size_t new_size) -> execution::io::offset_type {
    std::lock_guard lck{strand_};
    return std::exchange(auto_rollover_size_, new_size);
  }

  auto entries_empty() const -> bool {
    std::lock_guard lck{strand_};
    return entries.empty();
  }

  auto entries_size() const -> entries_list::size_type {
    std::lock_guard lck{strand_};
    return entries.size();
  }

  auto old_entries_empty() const -> bool {
    std::lock_guard lck{strand_};
    return old_entries.empty();
  }

  auto old_entries_size() const -> entries_list::size_type {
    std::lock_guard lck{strand_};
    return old_entries.size();
  }

  auto very_old_entries_empty() const -> bool {
    std::lock_guard lck{strand_};
    return very_old_entries.empty();
  }

  auto very_old_entries_size() const -> entries_list::size_type {
    std::lock_guard lck{strand_};
    return very_old_entries.size();
  }

  auto active_filename() const -> std::filesystem::path {
    std::lock_guard lck{strand_};
    return active->name;
  }

  auto active_state() const -> wal_file_entry_state {
    std::lock_guard lck{strand_};
    return active->state();
  }

  auto active_sequence() const -> std::uint_fast64_t {
    std::lock_guard lck{strand_};
    return active->sequence;
  }

  private:
  auto maybe_run_apply_() const noexcept -> void;
  auto maybe_release_entries_() const noexcept -> void;

  template<std::ranges::range FileRange>
  static auto compute_cached_file_replacements_(FileRange&& file_range, allocator_type alloc)
  -> execution::sender_of<gsl::not_null<std::shared_ptr<file_replacements>>> auto {
    using namespace execution;
    using wal_file_entry_ptr = gsl::not_null<std::shared_ptr<wal_file_entry>>;
    using file_vector = std::vector<wal_file_entry_ptr, rebind_alloc<wal_file_entry_ptr>>;

    struct state {
      state(FileRange&& file_range, allocator_type alloc)
      : files(alloc),
        cached_file_replacements(std::allocate_shared<file_replacements>(alloc, alloc))
      {
        std::ranges::copy(std::forward<FileRange>(file_range), std::back_inserter(files));
      }

      file_vector files;
      gsl::not_null<std::shared_ptr<file_replacements>> cached_file_replacements;
    };

    return just(state(std::forward<FileRange>(file_range), alloc))
    | repeat(
        [](std::size_t idx, state& s) {
          auto do_the_thing = [&]() {
            return s.files[idx]->records(
                [&s](wal_file_entry::variant_type r) {
                  if (!wal_record_is_bookkeeping(r)) {
                    std::visit(
                        [&s](auto r) {
                          (*s.cached_file_replacements)[r.file].apply(r);
                        },
                        make_wal_record_no_bookkeeping(r));
                  }
                  return just();
                });
          };
          using optional_type = std::optional<decltype(do_the_thing())>;

          if (idx == s.files.size())
            return optional_type(std::nullopt);
          else
            return optional_type(do_the_thing());
        })
    | then([](state s) { return s.cached_file_replacements; });
  }

  template<std::ranges::range Range>
  auto update_cached_file_replacements_(Range&& range) -> void {
    assert(strand_.running_in_this_thread());

    if (std::ranges::empty(range)) return;

    gsl::not_null<std::shared_ptr<file_replacements>> cfr = std::allocate_shared<file_replacements>(get_allocator(), *cached_file_replacements_, get_allocator());
    std::ranges::for_each(
        std::forward<Range>(range),
        [cfr=cfr.get().get(), this]<typename Record>(Record&& r) {
          std::error_code ec = std::visit(
              [&cfr, this]<typename R>(R&& r) -> std::error_code {
                return (*cfr)[r.file].apply(r);
              },
              std::forward<Record>(r));
          if (ec) throw std::system_error(ec, "error updating cached-file-replacements");
        });
    cached_file_replacements_ = cfr;
  }

  // The first stage: file-creation.
  // This records the intent, then creates the new file, then marks the file as successfully created.
  // The file is installed in the active->wal_file_data.successor.
  static auto rollover_stage_1_(gsl::not_null<std::shared_ptr<wal_file>> wf, gsl::not_null<std::shared_ptr<wal_file_entry>> active)
  -> execution::sender_of<
      gsl::not_null<std::shared_ptr<wal_file>> /*wf*/,
      gsl::not_null<std::shared_ptr<wal_file_entry>> /*old_active*/,
      gsl::not_null<std::shared_ptr<wal_file_entry>> /*new_active*/
  > auto;

  // At the end of a roll-over operation, we must clear the rollover-running flag.
  static auto rollover_sentinel(gsl::not_null<std::shared_ptr<wal_file>> wf, gsl::not_null<std::shared_ptr<wal_file_entry>> old_active);

  auto rollover_sender_(gsl::not_null<std::shared_ptr<wal_file_entry>> active) -> execution::sender_of<> auto; // We place this in the .cc file, because that's the only place it's used.
  auto maybe_auto_rollover_(gsl::not_null<std::shared_ptr<wal_file_entry>> active, execution::io::offset_type file_size) noexcept -> void;

  public:
  // Execute a roll-over.
  // Note: if a roll-over is already in progress, this call completes immediately, without waiting for the roll-over to complete.
  [[nodiscard]]
  auto rollover() -> execution::type_erased_sender<std::variant<std::tuple<>>, std::variant<std::exception_ptr, std::error_code>>;

#if 0 // OLD STUFF
  public:
  // `entries` holds all entries that lead up to `active`,
  // and has contiguous sequence.
  // `old` holds all entries that are not contiguous,
  // or that are no longer relevant.
  entries_list entries;
  old_list old;
  std::shared_ptr<entry> active;

  private:
  asio::strand<executor_type> strand_;
  dir dir_;

  // We use a fanout-barrier to make sure the rollovers update the 'active' pointer
  // in sequence of invocation.
  fanout_barrier<executor_type, allocator_type> rollover_barrier_;

  mutable bool apply_running_ = false;
  bool auto_rollover_running_ = false;
  std::size_t auto_rollover_size_ = 256 * 1024 * 1024;
  std::function<void()> apply_impl_;

  mutable std::shared_mutex cache_file_replacements_mtx_;
  std::shared_ptr<const file_replacements> cached_file_replacements_;

  const std::shared_ptr<spdlog::logger> logger = get_wal_logger();
#endif

  private:
  mutable execution::strand<allocator_type> strand_;
  gsl::not_null<std::shared_ptr<wal_file_entry>> active;
  dir dir_;

  // Very old entries, that are no longer in use.
  // These have been applied, but haven't yet been cleaned up.
  // Once they're processed, they should be deleted.
  // These may have gaps.
  // These are usually closed.
  entries_list very_old_entries;
  // Old entries.
  // These entries have been applied, but might still have readers using them.
  entries_list old_entries;
  // Entries preceding active, that are still in use by transactions.
  // All files in this list are yet to be applied.
  entries_list entries;

  bool auto_rollover_running_ = false;
  bool apply_running_ = false;
  execution::io::offset_type auto_rollover_size_ = 256 * 1024 * 1024;

  gsl::not_null<std::shared_ptr<const file_replacements>> cached_file_replacements_;
  apply_callback apply_impl_;
};

inline wal_file::tx_lock::tx_lock(state s) noexcept
: s(s)
{
  assert(this->s->wf->strand_.running_in_this_thread());
  ++this->s->write_entry->wal_file_data.locks;
  ++this->s->read_entry->wal_file_data.read_locks;
}


} /* namespace earnest::detail */
