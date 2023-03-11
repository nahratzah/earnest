#pragma once

#include <memory>
#include <utility>

#include <earnest/raw_db.h>

namespace earnest {


template<typename Executor, typename Allocator = std::allocator<std::byte>>
class db
: private raw_db<Executor, Allocator>
{
  public:
  using executor_type = typename raw_db<Executor, Allocator>::executor_type;
  using allocator_type = typename raw_db<Executor, Allocator>::allocator_type;

  using raw_db<Executor, Allocator>::raw_db; // inherit constructor

  using raw_db<Executor, Allocator>::async_create;
  using raw_db<Executor, Allocator>::async_open;
  using raw_db<Executor, Allocator>::get_executor;
  using raw_db<Executor, Allocator>::get_allocator;
};


} /* namespace earnest */
