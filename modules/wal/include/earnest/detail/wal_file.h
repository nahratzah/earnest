#pragma once

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <deque>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <ios>
#include <iostream>
#include <iterator>
#include <memory>
#include <queue>
#include <ranges>
#include <scoped_allocator>
#include <sstream>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <asio/associated_allocator.hpp>
#include <asio/associated_executor.hpp>
#include <asio/bind_allocator.hpp>
#include <asio/bind_executor.hpp>
#include <asio/executor_work_guard.hpp>
#include <asio/strand.hpp>

#include <earnest/detail/adjecent_find_last.h>
#include <earnest/detail/completion_barrier.h>
#include <earnest/detail/completion_handler_fun.h>
#include <earnest/detail/wal_file_entry.h>
#include <earnest/dir.h>

namespace earnest::detail {


// Work around defect: std::hash<std::filesystem::path> isn't defined.
struct fs_path_hash {
  auto operator()(const std::filesystem::path& p) const noexcept -> std::size_t {
    return std::filesystem::hash_value(p);
  }
};

template<typename Executor, typename Allocator = std::allocator<std::byte>>
class wal_file
: public std::enable_shared_from_this<wal_file<Executor, Allocator>>
{
  public:
  static inline constexpr std::string_view wal_file_extension = ".wal";

  using executor_type = Executor;
  using allocator_type = Allocator;

  private:
  template<typename T>
  using rebind_alloc = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;

  using entry_type = wal_file_entry<executor_type, allocator_type>;
  using entries_list = std::vector<std::shared_ptr<entry_type>, rebind_alloc<std::shared_ptr<entry_type>>>;

  public:
  using variant_type = typename entry_type::variant_type;
  using write_variant_type = typename entry_type::write_variant_type;
  using records_vector = typename entry_type::records_vector;
  using write_records_vector = typename entry_type::write_records_vector;

  explicit wal_file(executor_type ex, allocator_type alloc = allocator_type())
  : entries(alloc),
    old(alloc),
    strand_(ex),
    rollover_barrier_(ex, alloc)
  {
    std::invoke(rollover_barrier_, std::error_code());
  }

  wal_file(const wal_file&) = delete;
  wal_file(wal_file&&) = delete;
  wal_file& operator=(const wal_file&) = delete;
  wal_file& operator=(wal_file&&) = delete;

  auto get_executor() const -> executor_type { return strand_.get_inner_executor(); }
  auto get_allocator() const -> allocator_type { return entries.get_allocator(); }
  auto get_dir() const -> const dir& { return dir_; }

  template<typename Range, typename CompletionToken>
#if __cpp_concepts >= 201907L
  requires std::ranges::input_range<std::remove_reference_t<Range>>
#endif
  auto async_append(Range&& records, CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, std::shared_ptr<wal_file> wf, auto records) {

        asio::dispatch(
            completion_wrapper<void()>(
                completion_handler_fun(std::move(handler), wf->get_executor(), wf->strand_, wf->get_allocator()),
                [wf, records=std::move(records)](auto handler) mutable {
                  if (wf->active == nullptr) [[unlikely]] {
                    handler.dispatch(make_error_code(wal_errc::bad_state));
                    return;
                  }

                  wf->active->async_append(std::move(records), std::move(handler).inner_handler());
                }));
        },
        token, this->shared_from_this(), write_records_vector(std::ranges::begin(records), std::ranges::end(records), get_allocator()));
  }

  template<typename CompletionToken>
  auto async_rollover(CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, std::shared_ptr<wal_file> wf) {
          auto ex = asio::make_work_guard(handler, wf->get_executor());

          asio::dispatch(
              completion_wrapper<void()>(
                  completion_handler_fun(std::move(handler), wf->get_executor(), wf->strand_, wf->get_allocator()),
                  [wf](auto handler) {
                    if (wf->active == nullptr) {
                      handler.post(make_error_code(wal_errc::bad_state));
                      return;
                    }

                    fanout_barrier<executor_type, allocator_type> new_barrier(wf->get_executor(), wf->get_allocator());
                    wf->rollover_barrier_.async_on_ready(
                        completion_wrapper<void(std::error_code)>(
                            std::move(handler),
                            [wf, new_barrier](auto wrapped_handler, std::error_code ec) mutable {
                              if (ec) [[unlikely]] {
                                std::invoke(new_barrier, ec);
                                std::invoke(wrapped_handler, ec);
                                return;
                              }

                              const auto new_sequence = wf->active->sequence + 1u;
                              auto new_active = allocate_shared<entry_type>(wf->get_allocator(), wf->get_executor(), wf->get_allocator());
                              new_active->async_create(wf->dir_, filename_for_wal_(new_sequence), new_sequence,
                                  completion_wrapper<void(std::error_code, typename entry_type::link_done_event_type)>(
                                      std::move(wrapped_handler),
                                      [ wf, new_barrier=std::move(new_barrier),
                                        new_active=std::move(new_active)
                                      ](auto handler, std::error_code ec, typename entry_type::link_done_event_type link_event) mutable {
                                        if (!ec) [[likely]] { // Success!
                                          assert(wf->active->sequence + 1u == new_active->sequence); // guaranteed by rollover-barrier.
                                          const auto old_active = std::exchange(wf->active, new_active);
                                          wf->entries.push_back(old_active);
                                          std::invoke(new_barrier, std::error_code());

                                          link_event.async_on_ready(std::move(handler));
                                          old_active->async_seal(std::move(link_event));
                                        } else { // Fail, remove the new file.
                                          auto name = new_active->name;
                                          std::error_code undo_ec;
                                          wf->dir_.erase(name, undo_ec);
                                          if (undo_ec)
                                            std::clog << "WAL: rollover failed, and during recovery, the file erase failed (filename: " << name << ", error: " << undo_ec << "); further rollovers will be impossible\n";
                                          std::invoke(new_barrier, undo_ec);

                                          std::invoke(handler, ec);
                                          return;
                                        }
                                      }));
                            }));
                    wf->rollover_barrier_ = std::move(new_barrier);
                  }));
        },
        token, this->shared_from_this());
  }

  template<typename CompletionToken>
  auto async_open(dir d, CompletionToken&& token) {
    using namespace std::literals;

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto completion_handler, std::shared_ptr<wal_file> wf, dir d) {
          asio::dispatch(
              completion_wrapper<void()>(
                  completion_handler_fun(std::move(completion_handler), wf->get_executor(), wf->strand_, wf->get_allocator()),
                  [wf, d=std::move(d)](auto handler) {
                    if (wf->dir_.is_open()) {
                      std::invoke(handler, make_error_code(wal_errc::bad_state));
                      return;
                    }

                    auto barrier = make_completion_barrier(
                        completion_wrapper<void(std::error_code)>(
                            std::move(handler),
                            [wf](auto handler, std::error_code ec) {
                              // We want to do some initial verification:
                              // - the entries should be ordered by their sequence
                              // - the entries should have unique sequence numbers
                              if (!ec) [[likely]] {
                                std::sort(
                                    wf->entries.begin(), wf->entries.end(),
                                    [](const std::shared_ptr<entry_type>& x, const std::shared_ptr<entry_type>& y) {
                                      return x->sequence < y->sequence;
                                    });
                                const auto same_sequence_iter = std::adjacent_find(
                                    wf->entries.begin(), wf->entries.end(),
                                    [](const std::shared_ptr<entry_type>& x, const std::shared_ptr<entry_type>& y) -> bool {
                                      return x->sequence == y->sequence;
                                    });
                                if (same_sequence_iter != wf->entries.end()) [[unlikely]] {
                                  std::clog << "WAL: unrecoverable error: files " << (*same_sequence_iter)->name << " and " << (*std::next(same_sequence_iter))->name << " have the same sequence number " << (*same_sequence_iter)->sequence << std::endl;
                                  ec = make_error_code(wal_errc::unrecoverable);
                                }
                              }

                              if (ec) {
                                std::invoke(handler, ec);
                                return;
                              }

                              wf->recover_(std::move(handler));
                            }),
                            wf->strand_);

                    wf->dir_ = std::move(d);
                    for (const auto& dir_entry : wf->dir_) {
                      // Skip non-files and files with the wrong name.
                      if (!dir_entry.is_regular_file() || dir_entry.path().extension() != wal_file_extension)
                        continue;
                      std::filesystem::path filename = dir_entry.path();

                      wf->entries.emplace_back(std::allocate_shared<entry_type>(wf->get_allocator(), wf->get_executor(), wf->get_allocator()));
                      wf->entries.back()->async_open(wf->dir_, filename,
                          completion_wrapper<void(std::error_code)>(
                              ++barrier,
                              [filename](auto handler, std::error_code ec) {
                                if (ec) [[unlikely]]
                                  std::clog << "WAL: error while opening WAL file '"sv << filename << "': "sv << ec << "\n";
                                std::invoke(handler, ec);
                              }));
                    }
                    std::invoke(barrier, std::error_code());
                  }));
        },
        token, this->shared_from_this(), std::move(d));
  }

  template<typename CompletionToken>
  auto async_create(dir d, CompletionToken&& token) {
    using namespace std::literals;

    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto completion_handler, std::shared_ptr<wal_file> wf, dir d) {
          asio::dispatch(
              completion_wrapper<void()>(
                  completion_handler_fun(std::move(completion_handler), wf->get_executor(), wf->strand_, wf->get_allocator()),
                  [wf, d=std::move(d)](auto handler) {
                    if (wf->dir_.is_open()) {
                      std::invoke(handler, make_error_code(wal_errc::bad_state));
                      return;
                    }

                    wf->dir_ = std::move(d);
                    auto new_active = std::allocate_shared<entry_type>(wf->get_allocator(), wf->get_executor(), wf->get_allocator());
                    new_active->async_create(wf->dir_, filename_for_wal_(0), 0,
                        completion_wrapper<void(std::error_code, typename entry_type::link_done_event_type link_event)>(
                            std::move(handler),
                            [wf, new_active](auto handler, std::error_code ec, typename entry_type::link_done_event_type link_event) {
                              wf->active = new_active;
                              std::invoke(link_event, ec);
                              std::invoke(handler, ec);
                            }));;
                  }));
        },
        token, this->shared_from_this(), std::move(d));
  }

  template<typename CompletionToken>
  auto async_records(CompletionToken&& token) const {
    return asio::async_initiate<CompletionToken, void(std::error_code, records_vector)>(
        [](auto handler, std::shared_ptr<const wal_file> wf) {
          asio::dispatch(
              completion_wrapper<void()>(
                  completion_handler_fun(std::move(handler), wf->get_executor(), wf->strand_, wf->get_allocator()),
                  [wf](auto handler) {
                    auto queue = std::queue<std::shared_ptr<const entry_type>, std::deque<std::shared_ptr<const entry_type>, rebind_alloc<std::shared_ptr<const entry_type>>>>(
                        std::deque<std::shared_ptr<const entry_type>, rebind_alloc<std::shared_ptr<const entry_type>>>(
                            wf->entries.begin(), wf->entries.end(), wf->get_allocator()));
                    queue.push(wf->active);

                    wf->async_records_iter_(
                        records_vector(wf->get_allocator()),
                        std::move(queue),
                        std::move(handler).as_unbound_handler());
                  }));
        },
        token, this->shared_from_this());
  }

  private:
  template<typename CompletionHandler>
  static auto async_records_iter_(records_vector&& result, std::queue<std::shared_ptr<const entry_type>, std::deque<std::shared_ptr<const entry_type>, rebind_alloc<std::shared_ptr<const entry_type>>>>&& files, CompletionHandler&& handler) -> void {
    if (files.empty()) {
      std::invoke(handler, std::error_code(), std::move(result));
      return;
    }

    auto current = std::move(files.front());
    files.pop();
    current->async_records(
        completion_wrapper<void(std::error_code, records_vector)>(
            std::move(handler),
            [files=std::move(files), result=std::move(result)](auto handler, std::error_code ec, records_vector records) mutable {
              if (ec) [[unlikely]] {
                std::invoke(handler, ec, std::move(result));
                return;
              }

              result.insert(result.end(), std::make_move_iterator(records.begin()), std::make_move_iterator(records.end()));
              async_records_iter_(std::move(result), std::move(files), std::move(handler));
            }));
  }

  static auto filename_for_wal_(std::uint64_t sequence) -> std::filesystem::path {
    std::ostringstream s;
    s << std::hex << std::setfill('0') << std::setw(16) << sequence << wal_file_extension;
    return std::move(s).str();
  }

  template<typename CompletionHandler>
  auto recover_(CompletionHandler&& handler) {
    if (this->entries.empty()) [[unlikely]] {
      std::invoke(handler, make_error_code(wal_errc::no_data_files));
      return;
    }

    auto recover_barrier = make_completion_barrier(
        completion_wrapper<void(std::error_code)>(
            std::forward<CompletionHandler>(handler),
            [wf=this->shared_from_this()](auto handler, std::error_code ec) mutable {
              if (ec) [[unlikely]]
                std::invoke(handler, ec);
              else
                wf->update_bookkeeping_(std::move(handler));
            }),
        strand_);

    const typename entries_list::iterator unsealed_entry = find_first_unsealed_();
    if (unsealed_entry == this->entries.end()) {
      std::clog << "WAL: up to date (last file is sealed)\n";
      create_initial_unsealed_(++recover_barrier); // Start a new file, so we have an unsealed file.
    } else if (std::next(unsealed_entry) == this->entries.end()) {
      std::clog << "WAL: up to date (last file is accepting writes)\n";
    } else {
      std::clog << "WAL: has multiple unsealed files, recovery is required\n";

      // Once we've discarded all trailing files, seal the current file.
      // Then create a new empty file.
      auto discard_barrier = make_completion_barrier(
          completion_wrapper<void(std::error_code)>(
              completion_wrapper<void(std::error_code)>(
                  ++recover_barrier,
                  [wf=this->shared_from_this(), unsealed_entry=*unsealed_entry](auto handler, std::error_code ec) {
                    if (ec) [[unlikely]] {
                      std::invoke(handler, ec);
                    } else {
                      unsealed_entry->async_seal(
                          completion_wrapper<void(std::error_code)>(
                              std::move(handler),
                              [wf](auto handler, std::error_code ec) {
                                if (ec) [[unlikely]]
                                  std::invoke(handler, ec);
                                else
                                  wf->create_initial_unsealed_(std::move(handler));
                              }));
                    }
                  }),
              [wf=this->shared_from_this(), unsealed_entry](auto handler, std::error_code ec) {
                if (ec) [[unlikely]]
                  std::invoke(handler, ec);
                else
                  wf->ensure_no_gaps_after_(unsealed_entry, std::move(handler));
              }),
          strand_);

      // Files past the unsealed file never had their writes confirmed,
      // and we cannot know if all preceding writes made it into the unsealed-entry.
      // So we must discard those files.
      std::for_each(std::next(unsealed_entry), this->entries.end(),
          [&discard_barrier](const std::shared_ptr<entry_type>& e) {
            std::clog << "WAL: discarding never-confirmed entries in " << e->name << "\n";
            e->async_discard_all(++discard_barrier);
          });
      std::invoke(discard_barrier, std::error_code());
    }

    std::invoke(recover_barrier, std::error_code());
  }

  template<typename CompletionHandler>
  auto create_initial_unsealed_(CompletionHandler&& handler) -> void {
    const auto new_sequence = entries.back()->sequence + 1u;
    entries.emplace_back(std::allocate_shared<entry_type>(get_allocator(), get_executor(), get_allocator()));
    entries.back()->async_create(dir_, filename_for_wal_(new_sequence), new_sequence,
        completion_wrapper<void(std::error_code, typename entry_type::link_done_event_type)>(
            std::forward<CompletionHandler>(handler),
            [](auto handler, std::error_code ec, typename entry_type::link_done_event_type link_done) {
              std::invoke(link_done, std::error_code());
              std::invoke(handler, ec);
            }
        ));
  }

  template<typename CompletionHandler>
  auto update_bookkeeping_(CompletionHandler&& handler) {
    assert(!entries.empty());
    assert(std::all_of(entries.begin(), std::prev(entries.end()),
            [](const std::shared_ptr<entry_type>& e) {
              return e->state() == wal_file_entry_state::sealed;
            }));
    assert(entries.back()->state() == wal_file_entry_state::ready);

    // Find the most recent point where the sequence breaks.
    auto sequence_break_iter = adjecent_find_last(entries.begin(), entries.end(),
        [](const std::shared_ptr<entry_type>& x_ptr, const std::shared_ptr<entry_type>& y_ptr) -> bool {
          return x_ptr->sequence + 1u != y_ptr->sequence;
        });
    if (sequence_break_iter != entries.end()) {
      std::copy(std::make_move_iterator(entries.begin()), std::make_move_iterator(std::next(sequence_break_iter)), std::back_inserter(old));
      entries.erase(entries.begin(), std::next(sequence_break_iter));
    }

    this->active = entries.back();
    entries.pop_back();

    std::invoke(handler, std::error_code());
  }

  auto find_first_unsealed_() -> typename entries_list::iterator {
    return std::find_if(
        this->entries.begin(), this->entries.end(),
        [](const std::shared_ptr<entry_type>& e) {
          return e->state() != wal_file_entry_state::sealed;
        });
  }

  template<typename CompletionHandler>
  auto ensure_no_gaps_after_(typename entries_list::iterator iter, CompletionHandler&& handler) -> void {
    if (iter == entries.end()) {
      std::invoke(handler, std::error_code());
      return;
    }

    auto barrier = make_completion_barrier(
        std::forward<CompletionHandler>(handler),
        strand_);

    for (typename entries_list::iterator next_iter = std::next(iter);
         next_iter != entries.end();
         iter = std::exchange(next_iter, std::next(next_iter))) {
      for (auto missing_sequence = (*iter)->sequence + 1u; missing_sequence < (*next_iter)->sequence; ++missing_sequence) {
        const auto new_elem = std::allocate_shared<entry_type>(get_allocator(), get_executor(), get_allocator());
        new_elem->async_create(dir_, filename_for_wal_(missing_sequence), missing_sequence,
            completion_wrapper<void(std::error_code, typename entry_type::link_done_event_type link_event)>(
                ++barrier,
                [new_elem](auto handler, std::error_code ec, typename entry_type::link_done_event_type link_event) {
                  std::invoke(link_event, ec);
                  if (ec)
                    std::invoke(handler, ec);
                  else
                    new_elem->async_seal(std::move(handler));
                }));
        auto inserted_iter = entries.insert(next_iter, std::move(new_elem)); // Invalidates iter, next_iter.
        next_iter = std::next(inserted_iter);
      }
    }

    std::invoke(barrier, std::error_code());
  }

  public:
  // `entries` holds all entries that lead up to `active`,
  // and has contiguous sequence.
  // `old` holds all entries that are not contiguous,
  // or that are no longer relevant.
  entries_list entries, old;
  std::shared_ptr<entry_type> active;

  private:
  asio::strand<executor_type> strand_;
  dir dir_;

  // We use a fanout-barrier to make sure the rollovers update the 'active' pointer
  // in sequence of invocation.
  fanout_barrier<executor_type, allocator_type> rollover_barrier_;
};


} /* namespace earnest::detail */
