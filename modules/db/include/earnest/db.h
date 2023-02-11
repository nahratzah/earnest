#pragma once

#include <cstddef>
#include <memory>
#include <utility>

#include <cycle_ptr.h>

#include <earnest/db_cache.h>
#include <earnest/dir.h>
#include <earnest/file_db.h>

namespace earnest {


template<typename Executor, typename Allocator = std::allocator<std::byte>>
class db
: private cycle_ptr::cycle_base
{
  public:
  using executor_type = Executor;
  using allocator_type = Allocator;

  private:
  using file_db_type = file_db<executor_type, allocator_type>;
  using cache_type = db_cache<executor_type, allocator_type>;

  public:
  explicit db(executor_type ex, allocator_type alloc = allocator_type())
  : fdb_(std::allocate_shared<file_db_type>(ex, alloc)),
    cache_(*this, ex, alloc)
  {}

  template<typename CompletionToken>
  auto async_create(dir d, CompletionToken&& token) {
    return fdb_->async_create(std::move(d), std::forward<CompletionToken>(token));
  }

  template<typename CompletionToken>
  auto async_open(dir d, CompletionToken&& token) {
    return fdb_->async_open(std::move(d), std::forward<CompletionToken>(token));
  }

  auto get_executor() const -> executor_type {
    return cache_->get_executor();
  }

  auto get_allocator() const -> allocator_type {
    return cache_->get_allocator();
  }

  private:
  std::shared_ptr<file_db_type> fdb_;
  cache_type cache_;
};


} /* namespace earnest */
