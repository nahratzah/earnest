#pragma once

#include <cstddef>
#include <scoped_allocator>
#include <string_view>
#include <sstream>
#include <ios>
#include <iomanip>
#include <list>
#include <memory>
#include <ranges>
#include <iostream>
#include <utility>
#include <system_error>
#include <functional>
#include <unordered_map>
#include <filesystem>

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


template<typename Executor, typename Allocator = std::allocator<std::byte>>
class wal_file {
  using entry_type = wal_file_entry<Executor, Allocator>;

  public:
  static inline constexpr std::string_view wal_file_extension = ".wal";

  using executor_type = Executor;
  using allocator_type = Allocator;

  private:
  template<typename T>
  using rebind_alloc = typename std::allocator_traits<allocator_type>::template rebind_alloc<T>;

  public:
  wal_file(executor_type ex, allocator_type alloc = allocator_type())
  : strand_(std::move(ex)),
    entries_(std::move(alloc))
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
              completion_wrapper(
                  std::move(wrapped_handler),
                  [this, d=std::move(d)](auto handler, std::error_code ec) {
                    if (this->dir_.is_open()) {
                      std::invoke(handler, make_error_code(wal_errc::bad_state));
                      return;
                    }

                    auto records_map = std::allocate_shared<std::unordered_map<std::filesystem::path, typename entry_type::records_vector, std::hash<std::filesystem::path>, std::equal_to<std::filesystem::path>, std::scoped_allocator_adaptor<rebind_alloc<typename entry_type::records_vector>>>>(get_allocator(), get_allocator());
                    auto barrier = make_completion_barrier(
                        completion_wrapper<void(std::error_code)>(
                            std::move(handler),
                            [this, records_map](auto handler, std::error_code ec) {
                              if (ec) {
                                std::invoke(handler, ec);
                                return;
                              }

                              this->entries_.sort([](const entry_type& x, const entry_type& y) { return x.sequence < y.sequence; });
                              // XXX
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
                                  records_map.emplace(filename, std::move(records));
                                std::invoke(handler, ec);
                              }));
                    }
                    std::invoke(barrier);
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

  asio::strand<executor_type> strand_;
  std::list<entry_type, rebind_alloc<entry_type>> entries_;
  dir dir_;
};


} /* namespace earnest::detail */
