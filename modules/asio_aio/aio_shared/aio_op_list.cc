#include <earnest/detail/aio/aio_op_list.h>

#include <cassert>
#include <functional>

#include <asio/error.hpp>

namespace earnest::detail::aio {


auto aio_op_list::operation::is_map_linked() const -> bool {
  return this->map_hook_type::is_linked();
}

auto aio_op_list::operation::is_queue_linked() const -> bool {
  return this->queue_hook_type::is_linked();
}

void aio_op_list::operation::map_link_locked() {
  list_.map_.insert(*this);
}

void aio_op_list::operation::map_unlink_locked() {
  list_.map_.erase(list_.map_.iterator_to(*this));
}

void aio_op_list::operation::queue_link_locked() {
  list_.queue_.push_back(*this);
}

void aio_op_list::operation::queue_unlink_locked() {
  list_.queue_.erase(list_.queue_.iterator_to(*this));
}

auto aio_op_list::operation::dequeue(std::unique_lock<std::mutex>& lck) && -> operation_callback_result {
  // Ensure worker thread and cancel operations don't overlap.
  assert(lck.owns_lock() && lck.mutex() == &list_.mtx_);
  assert(is_map_linked() && is_queue_linked());

  std::error_code ec = invoke();
  if (!ec) [[likely]] {
    queue_unlink_locked();
    return operation_callback_result::in_progress;
  }

  if (retry_count_ < max_retries) {
    ++retry_count_;
    return operation_callback_result::enqueued;
  }

  map_unlink_locked();
  queue_unlink_locked();

  lck.unlock();
  std::move(*this).invoke_handler(std::move(ec));
  return operation_callback_result::destroyed;
}

auto aio_op_list::operation::on_some_work_done() && -> operation_callback_result {
  std::error_code ec;
  std::optional<std::size_t> done;

  {
    // Ensure worker thread and cancel operations don't overlap.
    std::lock_guard<std::mutex> lck{ list_.mtx_ };
    assert(is_map_linked() && !is_queue_linked());

    // Update state.
    std::tie(ec, done) = update();

    // Test for cancelation.
    if (!ec && canceled_) ec = asio::error::operation_aborted;

    if (!ec && !done) { // There is more work to be done.
      ec = invoke();
      if (!ec) [[likely]] {
        return operation_callback_result::in_progress;
      } else if (max_retries > 0 && ec == std::errc::resource_unavailable_try_again) { // EAGAIN
        retry_count_ = 1u;
        queue_link_locked();
        return operation_callback_result::enqueued;
      }
    }

    // If we haven't exited, then we either failed, or completed successfully.
    // We have to enqueue the callback and destroy this.
    map_unlink_locked();
  }

  std::move(*this).invoke_handler(std::move(ec), std::move(done));
  return operation_callback_result::destroyed;
}


void aio_op_list::operation_deleter::operator()(operation* op) const {
  std::move(*op).destructor();
}


aio_op_list::aio_op_list()
: map_(bucket_traits(buckets_))
{}

aio_op_list::~aio_op_list() {
  assert(map_.empty());
  assert(queue_.empty());
}

void aio_op_list::shutdown() {
  queue_.clear();
  map_.clear_and_dispose(operation_deleter());
}

void aio_op_list::close_fd(int fd) {
  struct cancelation_queue_t : queue_type {
    ~cancelation_queue_t() {
      this->clear_and_dispose(
          [](operation* op) {
            std::move(*op).invoke_handler(asio::error::operation_aborted);
          });
    }
  };

  // Track requests that were queued, but not started.
  // Must be kept outside the lock, otherwise assertions may deadlock.
  cancelation_queue_t cancelation_queue;

  std::lock_guard<std::mutex> lck{ mtx_ };
  auto map_range = map_.equal_range(fd);
  while (map_range.first != map_range.second) {
    const auto current = map_range.first++;

    if (current->is_queue_linked()) {
      queue_.erase(queue_.iterator_to(*current));
      cancelation_queue.push_back(*current);
      map_.erase(current);
    } else {
      current->cancel();
    }
  }
}

void aio_op_list::notify(operation* op) {
  switch (std::move(*op).on_some_work_done()) {
    case operation_callback_result::destroyed:
      [[fallthrough]];
    case operation_callback_result::enqueued:
      {
        std::lock_guard<std::mutex> lck{ mtx_ };
        assert(in_progress_count_ > 0);
        --in_progress_count_;
      }
      break;
    case operation_callback_result::in_progress:
      break;
  }
}

void aio_op_list::process_queue() {
  std::unique_lock<std::mutex> lck{ mtx_ };

  while (!queue_.empty()) {
    operation& op = queue_.front();

    switch (std::move(op).dequeue(lck)) {
      case operation_callback_result::destroyed:
        lck.lock(); // Relock.
        break;
      case operation_callback_result::enqueued: // Can't schedule any more AIO operations.
        assert(lck.owns_lock());

        // If we can make some progress, then we accept the current outstanding set as active.
        if (in_progress_count_ > 0)
          return;

        // If we can't make progress, we report the error immediately.
        if (in_progress_count_ == 0) {
          op.queue_unlink_locked();
          op.map_unlink_locked();

          lck.unlock();
          std::move(op).invoke_handler(std::make_error_code(std::errc::resource_unavailable_try_again));
          lck.lock();
        }
        break;
      case operation_callback_result::in_progress:
        assert(lck.owns_lock());
        ++in_progress_count_;
        break;
    }
  }
}

void aio_op_list::on_fork_child() {
  std::lock_guard<std::mutex> lck{ mtx_ };

  std::for_each(
      map_.begin(), map_.end(),
      [](operation& op) {
        if (!op.is_queue_linked()) op.queue_link_locked();
      });
}

void aio_op_list::add(operation_ptr<> op) {
  std::lock_guard<std::mutex> lck{ mtx_ };

  // If there are queued tasks, just add this onto the queue.
  if (!queue_.empty()) {
    op->map_link_locked();
    op->queue_link_locked();
    op.release();
    return;
  }

  std::error_code ec = op->invoke();
  if (!ec) { // Successfully started.
    ++in_progress_count_;
    op->map_link_locked();
    op.release();
  } else if (in_progress_count_ > 0 && operation::max_retries > 0 && ec == std::errc::resource_unavailable_try_again) { // EAGAIN
    op->retry_count_ = 1u;
    op->map_link_locked();
    op->queue_link_locked();
    op.release();
  } else { // Error.
    std::move(*op.release()).invoke_handler(ec, std::nullopt, true);
  }
}


} /* namespace earnest::detail::aio */
