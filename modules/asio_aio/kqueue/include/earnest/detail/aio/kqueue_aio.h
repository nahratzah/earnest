#ifndef EARNEST_DETAIL_AIO_KQUEUE_AIO_H
#define EARNEST_DETAIL_AIO_KQUEUE_AIO_H

#include <sys/event.h>
#include <sys/signal.h>

#include <thread>

#include <asio/execution_context.hpp>

#include <earnest/detail/aio/aio_op_list.h>
#include <earnest/detail/aio/kqueue_fd.h>
#include <earnest/detail/aio/operation_impl.h>

namespace earnest::detail::aio {


class kqueue_aio
: public asio::execution_context::service
{
  public:
  using key_type = kqueue_aio;
  static const asio::execution_context::id id;

  explicit kqueue_aio(asio::execution_context& ctx);
  ~kqueue_aio() override;

  auto close_fd(int fd) { ops_.close_fd(fd); }

  template<typename Buffers, typename CompletionHandler, typename Executor>
  void read_some(int fd, std::uint64_t offset, Buffers&& buffers, CompletionHandler&& handler, const Executor& executor) {
    add(new_read_operation(ops_, fd, offset, std::forward<Buffers>(buffers), std::forward<CompletionHandler>(handler), executor));
  }

  template<typename Buffers, typename CompletionHandler, typename Executor>
  void write_some(int fd, std::uint64_t offset, Buffers&& buffers, CompletionHandler&& handler, const Executor& executor) {
    add(new_write_operation(ops_, fd, offset, std::forward<Buffers>(buffers), std::forward<CompletionHandler>(handler), executor));
  }

  private:
  template<typename T>
  void add(aio_op_list::operation_ptr<T> op) {
    aio_op_list::operation*const ev_udata = op.get();

    auto& s = op->sigevent();
    s.sigev_notify = SIGEV_KEVENT;
    s.sigev_notify_kevent_flags = EV_DISPATCH;
    s.sigev_notify_kqueue = kq_.fd();
    s.sigev_value.sigval_ptr = ev_udata;

    ops_.add(std::move(op));
  }

  void shutdown() override;
  void notify_fork(asio::execution_context::fork_event event) override;
  void thread_fn_();
  void start_thread_();
  void stop_thread_();

  kqueue_fd kq_;
  aio_op_list ops_;
  std::thread thr_;
};


} /* namespace earnest::detail::aio */

#endif /* EARNEST_DETAIL_AIO_KQUEUE_AIO_H */
