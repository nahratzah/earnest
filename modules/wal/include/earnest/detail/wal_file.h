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
#include <mutex>
#include <queue>
#include <ranges>
#include <scoped_allocator>
#include <sstream>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <asio/append.hpp>
#include <asio/associated_allocator.hpp>
#include <asio/associated_executor.hpp>
#include <asio/async_result.hpp>
#include <asio/bind_allocator.hpp>
#include <asio/bind_executor.hpp>
#include <asio/deferred.hpp>
#include <asio/executor_work_guard.hpp>
#include <asio/strand.hpp>
#include <asio/defer.hpp>

#include <earnest/detail/adjecent_find_last.h>
#include <earnest/detail/completion_barrier.h>
#include <earnest/detail/completion_handler_fun.h>
#include <earnest/detail/deferred_on_executor.h>
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

  public:
  class entry
  : public std::enable_shared_from_this<entry>
  {
    public:
    using nb_variant_type = wal_record_no_bookkeeping<typename entry_type::variant_type>;

    private:
    using cached_records = std::vector<nb_variant_type, typename std::allocator_traits<typename entry_type::allocator_type>::template rebind_alloc<nb_variant_type>>;
    using cache_impl = fanout<typename entry_type::executor_type, void(std::error_code, std::shared_ptr<const cached_records>), typename entry_type::allocator_type>;

    public:
    explicit entry(std::shared_ptr<entry_type> file)
    : file(std::move(file))
    {}

    template<typename Range, typename CompletionToken>
    auto async_append(Range&& records, CompletionToken&& token) {
      return asio::async_initiate<CompletionToken, void(std::error_code)>(
          [](auto handler, std::shared_ptr<entry> self, typename entry_type::write_records_vector records) {
            self->file->async_append(
                std::move(records),
                completion_wrapper<void(std::error_code)>(
                    std::move(handler),
                    [self](auto handler, std::error_code ec) {
                      if (!ec) self->on_change_();
                      std::invoke(handler, ec);
                    }));
          },
          token, this->shared_from_this(), typename entry_type::write_records_vector(std::ranges::begin(records), std::ranges::end(records), file->get_allocator()));
    }

    template<typename Acceptor, typename CompletionToken>
    auto async_records(Acceptor&& acceptor, CompletionToken&& token) const {
      return asio::async_initiate<CompletionToken, void(std::error_code)>(
          [](auto handler, std::shared_ptr<const entry> self, auto acceptor) {
            self->completion_event_op_()
            | asio::deferred(
                [acceptor=std::move(acceptor)](std::error_code ec, std::shared_ptr<const cached_records> records) mutable {
                  for (auto iter = records->begin(), end = records->end(); !ec && iter != end; ++iter)
                    ec = std::invoke(acceptor, *iter);
                  return asio::deferred.values(ec);
                })
            | completion_handler_fun(std::move(handler), self->file->get_executor());
          },
          token, this->shared_from_this(), std::forward<Acceptor>(acceptor));
    }

    template<typename CompletionToken>
    auto async_seal(CompletionToken&& token) {
      return file->async_seal(std::forward<CompletionToken>(token));
    }

    private:
    auto on_change_() -> void {
      std::scoped_lock lck{ mtx_ };
      cache_.reset();
    }

    auto completion_event_op_() const {
      std::scoped_lock lck{ mtx_ };
      if (cache_ == nullptr) [[unlikely]] {
        auto new_cache = std::allocate_shared<cache_impl>(file->get_allocator(), file->get_executor(), file->get_allocator());
        auto new_records = std::allocate_shared<cached_records>(file->get_allocator(), file->get_allocator());
        file->async_records(
            [new_records](typename entry_type::variant_type v) -> std::error_code {
              if (wal_record_is_bookkeeping(v)) return {};
              new_records->push_back(make_wal_record_no_bookkeeping(std::move(v)));
              return {};
            },
            detail::completion_wrapper<void(std::error_code)>(
                *new_cache,
                [new_records](auto new_cache, std::error_code ec) mutable {
                  std::invoke(new_cache, ec, std::move(new_records));
                }));

        cache_ = std::move(new_cache);
      }
      return cache_->async_on_ready(asio::deferred);
    }

    public:
    std::shared_ptr<entry_type> file;
    std::size_t locks = 0;

    private:
    mutable std::mutex mtx_;
    mutable std::shared_ptr<cache_impl> cache_;
  };

  private:
  using entries_list = std::vector<std::shared_ptr<entry>, rebind_alloc<std::shared_ptr<entry>>>;
  using old_list = std::vector<std::shared_ptr<entry_type>, rebind_alloc<std::shared_ptr<entry_type>>>;

  public:
  using variant_type = typename entry_type::variant_type;
  using write_variant_type = typename entry_type::write_variant_type;
  using record_type = typename entry::nb_variant_type;
  using records_vector = typename entry_type::records_vector;
  using write_records_vector = typename entry_type::write_records_vector;
  using fd_type = typename entry_type::fd_type;

  class tx_lock {
    public:
    tx_lock() = default;

    tx_lock(std::shared_ptr<const wal_file> wf, std::shared_ptr<entry> entry)
    : entry_(std::move(entry)),
      wf_(std::move(wf))
    {
      assert(wf_->strand_.running_in_this_thread());
      ++entry_->locks;
    }

    tx_lock(const tx_lock&) = delete;

    tx_lock(tx_lock&& y) noexcept
    : entry_(std::move(y.entry_)),
      wf_(std::move(y.wf_))
    {}

    auto operator=(const tx_lock&) = delete;

    auto operator=(tx_lock&& y) -> tx_lock& {
      if (this != &y) [[likely]] {
        if (entry_ != nullptr) {
          wf_->strand_.dispatch(
              [entry=this->entry_]() {
                assert(entry->locks > 0);
                --entry->locks;
              },
              wf_->get_allocator());
        }

        entry_ = std::move(y.entry_);
        wf_ = std::move(y.wf_);
      }
      return *this;
    }

    ~tx_lock() {
      if (entry_ != nullptr) {
        wf_->strand_.dispatch(
            [wf=this->wf_, entry=this->entry_]() {
              --entry->locks;
              wf->maybe_run_apply_();
            },
            wf_->get_allocator());
      }
    }

    private:
    std::shared_ptr<entry> entry_;
    std::shared_ptr<const wal_file> wf_;
  };

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
  requires std::ranges::input_range<std::remove_reference_t<Range>>
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
    return rollover_op_()
    | std::forward<CompletionToken>(token);
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

                    auto bad_wal_files = std::allocate_shared<std::unordered_set<std::filesystem::path, fs_path_hash>>(wf->get_allocator());
                    auto barrier = make_completion_barrier(
                        completion_wrapper<void(std::error_code)>(
                            std::move(handler),
                            [wf, bad_wal_files](auto handler, std::error_code ec) {
                              // We want to do some initial verification:
                              // - the entries should be ordered by their sequence
                              // - the entries should have unique sequence numbers
                              if (!ec) [[likely]] {
                                std::sort(
                                    wf->entries.begin(), wf->entries.end(),
                                    [](const std::shared_ptr<entry>& x, const std::shared_ptr<entry>& y) {
                                      return x->file->sequence < y->file->sequence;
                                    });
                                const auto same_sequence_iter = std::adjacent_find(
                                    wf->entries.begin(), wf->entries.end(),
                                    [](const std::shared_ptr<entry>& x, const std::shared_ptr<entry>& y) -> bool {
                                      return x->file->sequence == y->file->sequence;
                                    });
                                if (same_sequence_iter != wf->entries.end()) [[unlikely]] {
                                  std::clog << "WAL: unrecoverable error: files " << (*same_sequence_iter)->file->name << " and " << (*std::next(same_sequence_iter))->file->name << " have the same sequence number " << (*same_sequence_iter)->file->sequence << std::endl;
                                  ec = make_error_code(wal_errc::unrecoverable);
                                }
                              }

                              if (ec) {
                                std::invoke(handler, ec);
                                return;
                              }

                              wf->deal_with_bad_wal_files_(std::move(handler), std::move(*bad_wal_files));
                            }),
                            wf->strand_);

                    wf->dir_ = std::move(d);
                    for (const auto& dir_entry : wf->dir_) {
                      // Skip non-files and files with the wrong name.
                      if (!dir_entry.is_regular_file() || dir_entry.path().extension() != wal_file_extension)
                        continue;
                      std::filesystem::path filename = dir_entry.path();

                      auto new_entry = std::allocate_shared<entry_type>(wf->get_allocator(), wf->get_executor(), wf->get_allocator());
                      new_entry->async_open(wf->dir_, filename,
                          completion_wrapper<void(std::error_code)>(
                              ++barrier,
                              [wf, new_entry, filename, bad_wal_files](auto handler, std::error_code ec) {
                                assert(wf->strand_.running_in_this_thread());
                                if (ec) [[unlikely]] {
                                  std::clog << "WAL: error while opening WAL file '"sv << filename << "': "sv << ec << "\n";
                                  bad_wal_files->insert(filename);
                                } else {
                                  wf->entries.emplace_back(std::allocate_shared<entry>(wf->get_allocator(), new_entry));
                                }
                                std::invoke(handler, std::error_code());
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
                              wf->active = std::allocate_shared<entry>(wf->get_allocator(), new_active);
                              std::invoke(link_event, ec);
                              std::invoke(handler, ec);
                            }));;
                  }));
        },
        token, this->shared_from_this(), std::move(d));
  }

  template<typename Acceptor, typename CompletionToken>
  requires std::invocable<Acceptor, record_type>
  auto async_tx(Acceptor&& acceptor, CompletionToken&& token) const {
    return asio::async_initiate<CompletionToken, void(std::error_code, tx_lock)>(
        [](auto handler, std::shared_ptr<const wal_file> wf, auto acceptor) {
          asio::dispatch(
              completion_wrapper<void()>(
                  completion_handler_fun(std::move(handler), wf->get_executor(), wf->strand_, wf->get_allocator()),
                  [wf, acceptor=std::move(acceptor)](auto handler) mutable {
                    auto queue = std::queue<std::shared_ptr<const entry>, std::deque<std::shared_ptr<const entry>, rebind_alloc<std::shared_ptr<const entry>>>>(
                        std::deque<std::shared_ptr<const entry>, rebind_alloc<std::shared_ptr<const entry>>>(
                            wf->entries.begin(), wf->entries.end(), wf->get_allocator()));
                    queue.push(wf->active);

                    wf->async_records_iter_(
                        std::move(acceptor),
                        std::move(queue),
                        completion_wrapper<void(std::error_code)>(
                            std::move(handler).as_unbound_handler(),
                            [lck=tx_lock(wf, wf->active)](auto handler, std::error_code ec) mutable {
                              std::invoke(handler, ec, std::move(lck));
                            }));
                  }));
        },
        token, this->shared_from_this(), std::forward<Acceptor>(acceptor));
  }

  template<typename Acceptor, typename CompletionToken>
  requires std::invocable<Acceptor, record_type>
  auto async_records(Acceptor&& acceptor, CompletionToken&& token) const {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, std::shared_ptr<const wal_file> wf, auto acceptor) {
          asio::dispatch(
              completion_wrapper<void()>(
                  completion_handler_fun(std::move(handler), wf->get_executor(), wf->strand_, wf->get_allocator()),
                  [wf, acceptor=std::move(acceptor)](auto handler) mutable {
                    auto queue = std::queue<std::shared_ptr<const entry>, std::deque<std::shared_ptr<const entry>, rebind_alloc<std::shared_ptr<const entry>>>>(
                        std::deque<std::shared_ptr<const entry>, rebind_alloc<std::shared_ptr<const entry>>>(
                            wf->entries.begin(), wf->entries.end(), wf->get_allocator()));
                    queue.push(wf->active);

                    wf->async_records_iter_(
                        std::move(acceptor),
                        std::move(queue),
                        std::move(handler).as_unbound_handler());
                  }));
        },
        token, this->shared_from_this(), std::forward<Acceptor>(acceptor));
  }

  template<typename Acceptor, typename CompletionToken>
  auto async_records_raw(Acceptor&& acceptor, CompletionToken&& token) const {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [](auto handler, std::shared_ptr<const wal_file> wf, auto acceptor) {
          asio::dispatch(
              completion_wrapper<void()>(
                  completion_handler_fun(std::move(handler), wf->get_executor(), wf->strand_, wf->get_allocator()),
                  [wf, acceptor=std::move(acceptor)](auto handler) mutable {
                    auto queue = std::queue<std::shared_ptr<const entry>, std::deque<std::shared_ptr<const entry>, rebind_alloc<std::shared_ptr<const entry>>>>(
                        std::deque<std::shared_ptr<const entry>, rebind_alloc<std::shared_ptr<const entry>>>(
                            wf->entries.begin(), wf->entries.end(), wf->get_allocator()));
                    queue.push(wf->active);

                    wf->async_records_raw_iter_(
                        std::move(acceptor),
                        std::move(queue),
                        std::move(handler).as_unbound_handler());
                  }));
        },
        token, this->shared_from_this(), std::forward<Acceptor>(acceptor));
  }

  template<typename Callback>
  auto set_apply_callback(Callback&& callback) -> void {
    strand_.dispatch(
        [wf=this->shared_from_this(), callback=std::forward<Callback>(callback)]() mutable {
          wf->apply_impl_ = [weak_wf=std::weak_ptr<wal_file>(wf), callback=std::move(callback)]() mutable {
            std::shared_ptr<wal_file> wf;
            try {
              wf = std::shared_ptr<wal_file>(weak_wf);
            } catch (const std::bad_weak_ptr&) {
              return;
            }

            wf->apply_(callback);
          };
          wf->maybe_run_apply_();
        },
        get_allocator());
  }

  auto clear_apply_callback() -> void {
    strand_.dispatch(
        [wf=this->shared_from_this()]() {
          wf->apply_impl_ = nullptr;
        });
  }

  private:
  template<typename Acceptor, typename CompletionHandler>
  static auto async_records_iter_(Acceptor&& acceptor, std::queue<std::shared_ptr<const entry>, std::deque<std::shared_ptr<const entry>, rebind_alloc<std::shared_ptr<const entry>>>>&& files, CompletionHandler&& handler) -> void {
    if (files.empty()) {
      std::invoke(handler, std::error_code());
      return;
    }

    auto current = std::move(files.front());
    files.pop();
    current->async_records(
        acceptor,
        completion_wrapper<void(std::error_code)>(
            std::move(handler),
            [files=std::move(files), acceptor](auto handler, std::error_code ec) mutable {
              if (ec) [[unlikely]] {
                std::invoke(handler, ec);
                return;
              }

              async_records_iter_(std::move(acceptor), std::move(files), std::move(handler));
            }));
  }

  template<typename Acceptor, typename CompletionHandler>
  static auto async_records_raw_iter_(Acceptor&& acceptor, std::queue<std::shared_ptr<const entry>, std::deque<std::shared_ptr<const entry>, rebind_alloc<std::shared_ptr<const entry>>>>&& files, CompletionHandler&& handler) -> void {
    if (files.empty()) {
      std::invoke(handler, std::error_code());
      return;
    }

    auto current = std::move(files.front());
    files.pop();
    current->file->async_records(
        acceptor,
        completion_wrapper<void(std::error_code)>(
            std::move(handler),
            [files=std::move(files), acceptor](auto handler, std::error_code ec) mutable {
              if (ec) [[unlikely]] {
                std::invoke(handler, ec);
                return;
              }

              async_records_raw_iter_(std::move(acceptor), std::move(files), std::move(handler));
            }));
  }

  static auto filename_for_wal_(std::uint64_t sequence) -> std::filesystem::path {
    std::ostringstream s;
    s << std::hex << std::setfill('0') << std::setw(16) << sequence << wal_file_extension;
    return std::move(s).str();
  }

  template<typename CompletionHandler>
  auto deal_with_bad_wal_files_(CompletionHandler&& handler, std::unordered_set<std::filesystem::path, fs_path_hash> bad_wal_files) -> void {
    struct update_t {
      bool intent_declared = false;
      bool ready = false;
    };
    using update_map = std::unordered_map<std::filesystem::path, update_t, fs_path_hash>;

    if (bad_wal_files.empty()) {
      recover_(std::forward<CompletionHandler>(handler));
      return;
    }

    const auto wal_data = std::allocate_shared<update_map>(get_allocator());
    auto barrier = make_completion_barrier(
        completion_wrapper<void(std::error_code)>(
            std::forward<CompletionHandler>(handler),
            [wal_data, wf=this->shared_from_this(), bad_wal_files=std::move(bad_wal_files)](auto handler, std::error_code ec) mutable {
              if (ec) {
                std::invoke(handler, ec);
                return;
              }

              bool fail = false;
              bool erase_fail = false;
              for (const auto& pair : *wal_data) {
                auto iter = bad_wal_files.find(pair.first);
                if (iter == bad_wal_files.end()) continue;

                if (pair.second.ready) {
                  std::clog << "WAL: unrecoverable error: " << pair.first << " should be ready, but failed at reading it\n";
                  fail = true;
                } else if (pair.second.intent_declared) {
                  std::clog << "WAL: erasing unused invalid WAL file " << pair.first << "\n";
                  try {
                    wf->dir_.erase(pair.first);
                  } catch (const std::system_error& ex) {
                    std::clog << "WAL: unable to erase invalid WAL file " << pair.first << ": " << ex.what() << "\n";
                    erase_fail = true;
                    continue;
                  }
                  bad_wal_files.erase(iter);
                }
              }
              for (const auto& entry : bad_wal_files) { // Log any files not accounted for.
                std::clog << "WAL: no record of creation for bad WAL file " << entry << "\n";
                fail = true;
              }
              if (fail) {
                std::invoke(handler, make_error_code(wal_errc::unrecoverable));
                return;
              }
              if (erase_fail) { // erase errors are recoverable (with a little help from a human).
                std::invoke(handler, make_error_code(wal_errc::recovery_failure));
                return;
              }

              wf->recover_(std::move(handler));
            }),
        strand_);

    assert(active == nullptr); // Won't be filled in until recover_() has run.
    assert(old.empty()); // Won't be filled in yet.
    for (const std::shared_ptr<entry>& entry : entries) {
      auto local_wal_data = std::allocate_shared<update_map>(get_allocator());
      entry->file->async_records(
          [local_wal_data](const auto& record) -> std::error_code {
            if (std::holds_alternative<wal_record_rollover_intent>(record)) {
              (*local_wal_data)[std::get<wal_record_rollover_intent>(record).filename].intent_declared = true;
            } else if (std::holds_alternative<wal_record_rollover_ready>(record)) {
              (*local_wal_data)[std::get<wal_record_rollover_ready>(record).filename].ready = true;
            }
            return {};
          },
          completion_wrapper<void(std::error_code)>(
              ++barrier,
              [wal_data, local_wal_data](auto handler, std::error_code ec) {
                std::for_each(local_wal_data->begin(), local_wal_data->end(),
                    [&wal_data](const auto& pair) {
                      auto& dst = (*wal_data)[pair.first];
                      if (pair.second.intent_declared)
                        dst.intent_declared = true;
                      if (pair.second.ready)
                        dst.ready = true;
                    });
                std::invoke(handler, ec);
              }));
    }

    std::invoke(barrier, std::error_code());
  }

  template<typename CompletionHandler>
  auto recover_(CompletionHandler&& handler) -> void {
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
          [&discard_barrier](const std::shared_ptr<entry>& e) {
            std::clog << "WAL: discarding never-confirmed entries in " << e->file->name << "\n";
            e->file->async_discard_all(++discard_barrier);
          });
      std::invoke(discard_barrier, std::error_code());
    }

    std::invoke(recover_barrier, std::error_code());
  }

  template<typename CompletionHandler>
  auto create_initial_unsealed_(CompletionHandler&& handler) -> void {
    const auto new_sequence = entries.back()->file->sequence + 1u;
    entries.emplace_back(
        std::allocate_shared<entry>(get_allocator(),
            std::allocate_shared<entry_type>(get_allocator(), get_executor(), get_allocator())));
    entries.back()->file->async_create(dir_, filename_for_wal_(new_sequence), new_sequence,
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
            [](const std::shared_ptr<entry>& e) {
              return e->file->state() == wal_file_entry_state::sealed;
            }));
    assert(entries.back()->file->state() == wal_file_entry_state::ready);

    // Find the most recent point where the sequence breaks.
    auto sequence_break_iter = adjecent_find_last(entries.begin(), entries.end(),
        [](const std::shared_ptr<entry>& x_ptr, const std::shared_ptr<entry>& y_ptr) -> bool {
          return x_ptr->file->sequence + 1u != y_ptr->file->sequence;
        });
    if (sequence_break_iter != entries.end()) {
      std::transform(entries.begin(), std::next(sequence_break_iter), std::back_inserter(old),
          [](const std::shared_ptr<entry>& e) -> std::shared_ptr<entry_type> {
            return e->file;
          });
      entries.erase(entries.begin(), std::next(sequence_break_iter));
    }

    this->active = entries.back();
    entries.pop_back();

    std::invoke(handler, std::error_code());
  }

  auto find_first_unsealed_() -> typename entries_list::iterator {
    return std::find_if(
        this->entries.begin(), this->entries.end(),
        [](const std::shared_ptr<entry>& e) {
          return e->file->state() != wal_file_entry_state::sealed;
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
      for (auto missing_sequence = (*iter)->file->sequence + 1u; missing_sequence < (*next_iter)->file->sequence; ++missing_sequence) {
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
        auto inserted_iter = entries.insert(next_iter,
            std::allocate_shared<entry>(get_allocator(), std::move(new_elem))); // Invalidates iter, next_iter.
        next_iter = std::next(inserted_iter);
      }
    }

    std::invoke(barrier, std::error_code());
  }

  auto rollover_op_() {
    return deferred_on_executor<void(std::error_code)>(
        [](std::shared_ptr<wal_file> wf) {
          assert(wf->strand_.running_in_this_thread());

          return asio::deferred.when(wf->active == nullptr)
              .then(asio::deferred.values(make_error_code(wal_errc::bad_state)))
              .otherwise(
                  wf->rollover_create_new_entry_op_()
                  | asio::deferred(
                      [wf](std::error_code ec, typename entry_type::link_done_event_type link_event, std::shared_ptr<entry_type> new_active, fanout_barrier<executor_type, allocator_type> new_barrier) {
                        if (ec) wf->rollover_recover_(new_active->name, std::move(new_barrier));
                        return asio::deferred.when(!ec)
                            .then(wf->rollover_install_new_entry_op_(std::move(link_event), new_active, new_barrier))
                            .otherwise(asio::deferred.values(ec));
                      }));
        },
        strand_, this->shared_from_this());
  }

  auto rollover_create_new_entry_op_() {
    return deferred_on_executor<void(std::error_code, std::shared_ptr<wal_file>, fanout_barrier<executor_type, allocator_type>)>(
        [](std::shared_ptr<wal_file> wf) {
          assert(wf->strand_.running_in_this_thread());

          fanout_barrier<executor_type, allocator_type> new_barrier(wf->get_executor(), wf->get_allocator());
          auto op = wf->rollover_barrier_.async_on_ready(asio::append(asio::deferred, wf, new_barrier));
          wf->rollover_barrier_ = std::move(new_barrier);
          return op;
        },
        strand_, this->shared_from_this())
    | asio::bind_executor(
        strand_,
        asio::deferred(
            [](std::error_code ec, std::shared_ptr<wal_file> wf, fanout_barrier<executor_type, allocator_type> new_barrier) {
              assert(wf->strand_.running_in_this_thread());

              auto new_filename = filename_for_wal_(wf->active->file->sequence + 1u);
              return asio::deferred.when(!ec)
                  .then(
                      wf->async_append(
                          std::initializer_list<write_variant_type>{
                            wal_record_rollover_intent{ .filename=new_filename.generic_string() },
                          },
                          asio::append(asio::deferred, wf, new_barrier, new_filename)))
                  .otherwise(asio::deferred.values(ec, wf, new_barrier, new_filename));
            }))
    | asio::deferred(
        [](std::error_code ec, std::shared_ptr<wal_file> wf, fanout_barrier<executor_type, allocator_type> new_barrier, std::filesystem::path new_filename) {
          auto new_active = allocate_shared<entry_type>(wf->get_allocator(), wf->get_executor(), wf->get_allocator());
          return asio::deferred.when(!ec)
              .then(
                  deferred_on_executor<void(std::error_code, typename entry_type::link_done_event_type, std::shared_ptr<wal_file> wf, std::shared_ptr<entry_type>, fanout_barrier<executor_type, allocator_type>, std::filesystem::path)>(
                      [](std::shared_ptr<wal_file> wf, std::shared_ptr<entry_type> new_active, fanout_barrier<executor_type, allocator_type> new_barrier, std::filesystem::path new_filename) {
                        assert(wf->strand_.running_in_this_thread());

                        const auto new_sequence = wf->active->file->sequence + 1u;
                        return new_active->async_create(wf->dir_, new_filename, new_sequence, asio::append(asio::deferred, wf, new_active, new_barrier, new_filename));
                      },
                      wf->strand_, wf, new_active, new_barrier, new_filename))
              .otherwise(asio::deferred.values(ec, typename entry_type::link_done_event_type(new_active->get_executor(), new_active->get_allocator()), wf, new_active, new_barrier, new_filename));
        })
    | asio::deferred(
        [](std::error_code ec, typename entry_type::link_done_event_type link_event, std::shared_ptr<wal_file> wf, std::shared_ptr<entry_type> new_active, fanout_barrier<executor_type, allocator_type> new_barrier, std::filesystem::path new_filename) {
          return asio::deferred.when(!ec)
              .then(
                  wf->async_append(
                      std::initializer_list<write_variant_type>{
                        wal_record_rollover_ready{ .filename=new_filename.generic_string() },
                      },
                      asio::append(asio::deferred, link_event, new_active, new_barrier)))
              .otherwise(asio::deferred.values(ec, link_event, new_active, new_barrier));
        });
  }

  auto rollover_install_new_entry_op_(typename entry_type::link_done_event_type link_event, std::shared_ptr<entry_type> new_active, fanout_barrier<executor_type, allocator_type> new_barrier) {
    return deferred_on_executor<void(std::error_code)>(
        [](std::shared_ptr<wal_file> wf, typename entry_type::link_done_event_type link_event, std::shared_ptr<entry_type> new_active, fanout_barrier<executor_type, allocator_type> new_barrier) {
          assert(wf->strand_.running_in_this_thread());
          assert(wf->active->file->sequence + 1u == new_active->sequence); // guaranteed by rollover-barrier.

          const auto old_active = std::exchange(wf->active,
              std::allocate_shared<entry>(wf->get_allocator(), new_active));
          wf->entries.push_back(old_active);
          std::invoke(new_barrier, std::error_code());

          auto completion = link_event.async_on_ready(asio::deferred);
          old_active->async_seal(std::move(link_event));
          return completion;
        },
        strand_, this->shared_from_this(), std::move(link_event), std::move(new_active), std::move(new_barrier));
  }

  auto rollover_recover_(std::filesystem::path name, fanout_barrier<executor_type, allocator_type> new_barrier) -> void {
    using namespace std::string_view_literals;

    asio::defer(
        completion_handler_fun(
            [wf=this->shared_from_this(), name=std::move(name), new_barrier=std::move(new_barrier)]() mutable {
              std::error_code undo_ec;
              wf->dir_.erase(name, undo_ec);
              if (undo_ec == make_error_code(std::errc::no_such_file_or_directory))
                undo_ec.clear();
              else if (undo_ec)
                std::clog << "WAL: rollover failed, and during recovery, the file erase failed (filename: "sv << name << ", error: "sv << undo_ec << "); further rollovers will be impossible\n"sv;
              std::invoke(new_barrier, undo_ec);
            },
            strand_));
  }

  auto maybe_run_apply_() const -> void {
    assert(strand_.running_in_this_thread());

    if (!apply_impl_) return;

    if (!apply_running_ && !entries.empty() && entries.front()->locks == 0) {
      apply_running_ = true;
      apply_impl_();
    }
  }

  template<typename Callback>
  auto apply_(Callback& callback) -> void {
    assert(strand_.running_in_this_thread());
    assert(apply_running_);

    if (entries.empty() || entries.front()->locks != 0) {
      apply_running_ = false;
      return;
    }

    std::invoke(
        callback, entries.front(),
        asio::bind_executor(
            strand_,
            [wf=this->shared_from_this()](std::error_code ec) {
              assert(wf->strand_.running_in_this_thread());

              if (ec) {
                std::clog << "WAL: unable to apply WAL-file, " << ec.message() << "\n";
                wf->apply_running_ = false;
                return;
              }

              wf->old.push_back(wf->entries.front()->file);
              wf->entries.erase(wf->entries.begin());
              if (wf->apply_impl_)
                wf->apply_impl_();
              else
                wf->apply_running_ = false;
            }));
  }

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
  std::function<void()> apply_impl_;
};


} /* namespace earnest::detail */
