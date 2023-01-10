#pragma once

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <ios>
#include <iostream>
#include <iterator>
#include <list>
#include <memory>
#include <ranges>
#include <scoped_allocator>
#include <sstream>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <asio/strand.hpp>
#include <asio/associated_allocator.hpp>
#include <asio/associated_executor.hpp>
#include <asio/bind_allocator.hpp>
#include <asio/bind_executor.hpp>
#include <asio/executor_work_guard.hpp>

#include <earnest/detail/wal_file_entry.h>
#include <earnest/detail/completion_barrier.h>
#include <earnest/dir.h>

namespace earnest::detail {


// Work around defect: std::hash<std::filesystem::path> isn't defined.
struct fs_path_hash {
  auto operator()(const std::filesystem::path& p) const noexcept -> std::size_t {
    return std::filesystem::hash_value(p);
  }
};

template<typename Executor, typename Allocator = std::allocator<std::byte>>
class wal_file {
  public:
  static inline constexpr std::string_view wal_file_extension = ".wal";

  using executor_type = Executor;
  using allocator_type = Allocator;

  private:
  template<typename T>
  using rebind_alloc = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;

  using entry_type = wal_file_entry<executor_type, allocator_type>;
  using entries_list = std::list<entry_type, rebind_alloc<entry_type>>;

  public:
  wal_file(executor_type ex, allocator_type alloc = allocator_type())
  : unarchived_wal_files(alloc),
    strand_(std::move(ex)),
    entries_(alloc)
  {}

  wal_file(const wal_file&) = delete;
  wal_file(wal_file&&) = delete;
  wal_file& operator=(const wal_file&) = delete;
  wal_file& operator=(wal_file&&) = delete;

  auto get_executor() const -> executor_type { return strand_.get_inner_executor(); }
  auto get_allocator() const -> allocator_type { return entries_.get_allocator(); }

  template<typename CompletionToken>
  auto async_open(dir d, CompletionToken&& token) {
    using namespace std::literals;

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [this](auto completion_handler, dir d) {
          auto ex = asio::make_work_guard(completion_handler, get_executor());
          auto wrapped_handler = asio::bind_executor(
              strand_,
              asio::bind_allocator(
                  get_allocator(),
                  [ex=std::move(ex), completion_handler=std::move(completion_handler)](std::error_code ec) {
                    auto alloc = asio::get_associated_allocator(completion_handler);
                    ex.get_executor().dispatch(
                        [completion_handler=std::move(completion_handler), ec]() mutable {
                          std::invoke(completion_handler, ec);
                        },
                        alloc);
                  }));

          strand_.post(
              completion_wrapper<void()>(
                  std::move(wrapped_handler),
                  [this, d=std::move(d)](auto handler) {
                    if (this->dir_.is_open()) {
                      std::invoke(handler, make_error_code(wal_errc::bad_state));
                      return;
                    }

                    auto records_map = std::allocate_shared<std::unordered_map<std::filesystem::path, typename entry_type::records_vector, fs_path_hash, std::equal_to<std::filesystem::path>, std::scoped_allocator_adaptor<rebind_alloc<std::pair<const std::filesystem::path, typename entry_type::records_vector>>>>>(get_allocator(), get_allocator());
                    auto barrier = make_completion_barrier(
                        completion_wrapper<void(std::error_code)>(
                            std::move(handler),
                            [this, records_map](auto handler, std::error_code ec) {
                              // We want to do some initial verification:
                              // - the entries should be ordered by their sequence
                              // - the entries should have unique sequence numbers
                              if (!ec) [[likely]] {
                                this->entries_.sort([](const entry_type& x, const entry_type& y) { return x.sequence < y.sequence; });
                                const auto same_sequence_iter = std::adjacent_find(
                                    this->entries_.begin(), this->entries_.end(),
                                    [](const entry_type& x, const entry_type& y) -> bool {
                                      return x.sequence == y.sequence;
                                    });
                                if (same_sequence_iter != this->entries_.end()) [[unlikely]] {
                                  std::clog << "WAL unrecoverable error: files " << same_sequence_iter->name << " and " << std::next(same_sequence_iter)->name << " have the same sequence number " << same_sequence_iter->sequence << std::endl;
                                  ec = make_error_code(wal_errc::unrecoverable);
                                }
                              }

                              if (ec) {
                                std::invoke(handler, ec);
                                return;
                              }

                              recover_(std::move(*records_map), std::move(handler));
                            }),
                            strand_);

                    this->dir_ = std::move(d);
                    for (const auto& dir_entry : dir_) {
                      // Skip non-files and files with the wrong name.
                      if (!dir_entry.is_regular_file() || dir_entry.path().extension() != wal_file_extension)
                        continue;
                      std::filesystem::path filename = dir_entry.path();

                      entries_.emplace_back(get_executor(), get_allocator());
                      entries_.back().async_open(this->dir_, filename,
                          completion_wrapper<void(std::error_code, typename entry_type::records_vector records)>(
                              ++barrier,
                              [filename, records_map](auto handler, std::error_code ec, typename entry_type::records_vector records) {
                                if (ec) [[unlikely]]
                                  std::clog << "error while opening WAL file '"sv << filename << "': "sv << ec << "\n";
                                else
                                  records_map->emplace(filename, std::move(records));
                                std::invoke(handler, ec);
                              }));
                    }
                    std::invoke(barrier, std::error_code());
                  }),
              get_allocator());
        },
        token, std::move(d));
  }

  template<typename CompletionToken>
  auto async_create(dir d, CompletionToken&& token) {
    using namespace std::literals;

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [this](auto completion_handler, dir d) {
          auto ex = asio::make_work_guard(completion_handler, get_executor());
          auto wrapped_handler = asio::bind_executor(
              strand_,
              asio::bind_allocator(
                  get_allocator(),
                  [ex=std::move(ex), completion_handler=std::move(completion_handler)](std::error_code ec) {
                    auto alloc = asio::get_associated_allocator(completion_handler);
                    ex.get_executor().dispatch(
                        [completion_handler=std::move(completion_handler), ec]() mutable {
                          std::invoke(completion_handler, ec);
                        },
                        alloc);
                  }));

          strand_.post(
              completion_wrapper<void()>(
                  std::move(wrapped_handler),
                  [this, d=std::move(d)](auto handler) {
                    if (this->dir_.is_open()) {
                      std::invoke(handler, make_error_code(wal_errc::bad_state));
                      return;
                    }

                    this->dir_ = std::move(d);
                    entries_.emplace_back(get_executor(), get_allocator());
                    entries_.back().async_create(this->dir_, filename_for_wal_(0), 0,
                        completion_wrapper<void(std::error_code, typename entry_type::link_done_event_type link_event)>(
                            std::move(handler),
                            [](auto handler, std::error_code ec, typename entry_type::link_done_event_type link_event) {
                              std::invoke(link_event, ec);
                              std::invoke(handler, ec);
                            }));;
                  }),
              get_allocator());
        },
        token, std::move(d));
  }

  private:
  static auto filename_for_wal_(std::uint64_t sequence) -> std::filesystem::path {
    std::ostringstream s;
    s << std::hex << std::setfill('0') << std::setw(16) << sequence << wal_file_extension;
    return std::move(s).str();
  }

  template<typename RecordsMap, typename CompletionHandler>
  auto recover_(RecordsMap&& records, CompletionHandler&& handler) {
    if (this->entries_.empty()) [[unlikely]] {
      std::invoke(handler, make_error_code(wal_errc::no_data_files));
      return;
    }

    auto records_ptr = std::allocate_shared<std::remove_cvref_t<RecordsMap>>(get_allocator(), std::forward<RecordsMap>(records));
    auto recover_barrier = make_completion_barrier(
        completion_wrapper<void(std::error_code)>(
            std::forward<CompletionHandler>(handler),
            [this, records_ptr](auto handler, std::error_code ec) mutable {
              if (ec) [[unlikely]]
                std::invoke(handler, ec);
              else
                this->update_bookkeeping_(std::move(*records_ptr), std::move(handler));
            }),
        strand_);

    const typename entries_list::iterator unsealed_entry = find_first_unsealed_();
    if (unsealed_entry == this->entries_.end()) {
      std::clog << "WAL up to date (last file is sealed)\n";
      create_initial_unsealed_(++recover_barrier); // Start a new file, so we have an unsealed file.
    } else if (std::next(unsealed_entry) == this->entries_.end()) {
      std::clog << "WAL up to date (last file is accepting writes)\n";
    } else {
      std::clog << "WAL has multiple unsealed files, recovery is required\n";

      // Once we've discarded all trailing files, seal the current file.
      // Then create a new empty file.
      auto discard_barrier = make_completion_barrier(
          completion_wrapper<void(std::error_code)>(
              completion_wrapper<void(std::error_code)>(
                  ++recover_barrier,
                  [this, unsealed_entry](auto handler, std::error_code ec) {
                    if (ec) [[unlikely]] {
                      std::invoke(handler, ec);
                    } else {
                      unsealed_entry->async_seal(
                          completion_wrapper<void(std::error_code)>(
                              std::move(handler),
                              [this](auto handler, std::error_code ec) {
                                if (ec) [[unlikely]]
                                  std::invoke(handler, ec);
                                else
                                  this->create_initial_unsealed_(std::move(handler));
                              }));
                    }
                  }),
              [this, unsealed_entry](auto handler, std::error_code ec) {
                if (ec) [[unlikely]]
                  std::invoke(handler, ec);
                else
                  this->ensure_no_gaps_after_(unsealed_entry, std::move(handler));
              }),
          strand_);

      // Files past the unsealed file never had their writes confirmed,
      // and we cannot know if all preceding writes made it into the unsealed-entry.
      // So we must discard those files.
      std::for_each(std::next(unsealed_entry), this->entries_.end(),
          [&discard_barrier, records_ptr](entry_type& e) {
            std::clog << "WAL: discarding never-confirmed entries in " << e.name << "\n";
            records_ptr->erase(e.name);
            e.async_discard_all(++discard_barrier);
          });
      std::invoke(discard_barrier, std::error_code());
    }

    std::invoke(recover_barrier, std::error_code());
  }

  template<typename CompletionHandler>
  auto create_initial_unsealed_(CompletionHandler&& handler) -> void {
    const auto new_sequence = entries_.back().sequence + 1u;
    entries_.emplace_back(get_executor(), get_allocator());
    entries_.back().async_create(dir_, filename_for_wal_(new_sequence), new_sequence,
        completion_wrapper<void(std::error_code, typename entry_type::link_done_event_type)>(
            std::forward<CompletionHandler>(handler),
            [](auto handler, std::error_code ec, typename entry_type::link_done_event_type link_done) {
              std::invoke(link_done, std::error_code());
              std::invoke(handler, ec);
            }
        ));
  }

  template<typename RecordsMap, typename CompletionHandler>
  auto update_bookkeeping_(RecordsMap&& records, CompletionHandler&& handler) {
    assert(!entries_.empty());
    assert(std::all_of(entries_.begin(), std::prev(entries_.end()),
            [](const entry_type& e) {
              return e.state() == wal_file_entry_state::sealed;
            }));
    assert(entries_.back().state() == wal_file_entry_state::ready);

    auto bookkeeping_barrier = make_completion_barrier(
        std::forward<CompletionHandler>(handler),
        strand_);

    unarchived_wal_files = compute_unarchived_wal_files_(records);
    std::invoke(bookkeeping_barrier, std::error_code());
  }

  auto find_first_unsealed_() -> typename entries_list::iterator {
    return std::find_if(
        this->entries_.begin(), this->entries_.end(),
        [](const entry_type& e) {
          return e.state() != wal_file_entry_state::sealed;
        });
  }

  template<typename RecordsMap>
  auto compute_unarchived_wal_files_(const RecordsMap& records) const -> std::unordered_set<std::filesystem::path, fs_path_hash, std::equal_to<std::filesystem::path>, rebind_alloc<std::filesystem::path>> {
    std::unordered_set<std::uint64_t, std::hash<std::uint64_t>, std::equal_to<std::uint64_t>, rebind_alloc<std::uint64_t>> archived_sequences(get_allocator());
    std::for_each(records.begin(), records.end(),
        [&archived_sequences](const auto& rp) {
          std::for_each(
              rp.second.begin(), rp.second.end(),
              [&archived_sequences](const auto& record_variant) {
                if (std::holds_alternative<wal_record_wal_archived>(record_variant))
                  archived_sequences.insert(std::get<wal_record_wal_archived>(record_variant).sequence);
              });
        });

    std::unordered_set<std::filesystem::path, fs_path_hash, std::equal_to<std::filesystem::path>, rebind_alloc<std::filesystem::path>> unarchived(get_allocator());
    if (!this->entries_.empty()) {
      std::for_each(this->entries_.begin(), std::prev(this->entries_.end()),
          [&unarchived, &archived_sequences](const entry_type& e) {
            if (!archived_sequences.contains(e.sequence))
              unarchived.insert(e.name);
          });
    }

    return unarchived;
  }

  template<typename CompletionHandler>
  auto ensure_no_gaps_after_(typename entries_list::iterator iter, CompletionHandler&& handler) -> void {
    if (iter == entries_.end()) {
      std::invoke(handler, std::error_code());
      return;
    }

    auto barrier = make_completion_barrier(
        std::forward<CompletionHandler>(handler),
        strand_);

    for (typename entries_list::iterator next_iter = std::next(iter);
         next_iter != entries_.end();
         iter = std::exchange(next_iter, std::next(next_iter))) {
      for (auto missing_sequence = iter->sequence + 1u; missing_sequence < next_iter->sequence; ++missing_sequence) {
        const auto new_elem_iter = entries_.emplace(next_iter, get_executor(), get_allocator());
        new_elem_iter->async_create(dir_, filename_for_wal_(missing_sequence), missing_sequence,
            completion_wrapper<void(std::error_code, typename entry_type::link_done_event_type link_event)>(
                ++barrier,
                [](auto handler, std::error_code ec, typename entry_type::link_done_event_type link_event) {
                  std::invoke(link_event, ec);
                  std::invoke(handler, ec);
                }));
      }
    }

    std::invoke(barrier, std::error_code());
  }

  public:
  std::unordered_set<std::filesystem::path, fs_path_hash, std::equal_to<std::filesystem::path>, rebind_alloc<std::filesystem::path>> unarchived_wal_files;

  private:
  asio::strand<executor_type> strand_;
  entries_list entries_;
  dir dir_;
};


} /* namespace earnest::detail */
