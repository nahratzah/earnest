#include <earnest/detail/aio/kqueue_aio.h>

#include <sys/event.h>

#include <array>

namespace earnest::detail::aio {


kqueue_aio::kqueue_aio(asio::execution_context& ctx)
: asio::execution_context::service(ctx),
  kq_(kqueue_fd::create())
{}

kqueue_aio::~kqueue_aio() {
  if (thr_.joinable()) stop_thread_();
}

void kqueue_aio::shutdown() {
  stop_thread_();
  ops_.shutdown();
}

void kqueue_aio::notify_fork(asio::execution_context::fork_event event) {
  switch (event) {
    case asio::execution_context::fork_event::fork_prepare:
      stop_thread_();
      break;
    case asio::execution_context::fork_event::fork_parent:
      start_thread_();
      break;
    case asio::execution_context::fork_event::fork_child:
      kq_.release(); // kqueue gets closed by fork automatically

#if 0 // Commented out because this would duplicate all in-flight requests.
      ops_.on_fork_child(); // all active requests are moved to queued

      kq_ = kqueue_fd::create();
      ops_.process_queue();
      start_thread_();
#endif
      break;
  }
}

void kqueue_aio::thread_fn_() {
  std::array<struct ::kevent, 4096> evlist;

  for (bool stop_signal = false; !stop_signal;) {
    const int nev = ::kevent(kq_.fd(), nullptr, 0, evlist.data(), evlist.size(), nullptr);
    if (errno == EINTR) continue;
    if (nev == -1) throw std::system_error(errno, std::system_category(), "kqueue");

    std::for_each_n(
        evlist.begin(), nev,
        [this, &stop_signal](struct ::kevent& ev) {
          switch (ev.filter) {
            case EVFILT_USER:
              assert(this == ev.udata);
              stop_signal = true;
              break;
            case EVFILT_LIO: [[fallthrough]];
            case EVFILT_AIO:
              ops_.notify(static_cast<aio_op_list::operation*>(ev.udata));
              break;
          }
        });

    ops_.process_queue();
  }
}

void kqueue_aio::start_thread_() {
  thr_ = std::thread(&kqueue_aio::thread_fn_, this);
}

void kqueue_aio::stop_thread_() {
  struct ::kevent stop_signal;
  EV_SET(&stop_signal, 0, EVFILT_USER, EV_ONESHOT, NOTE_FFNOP, 0, this);
  if (::kevent(kq_.fd(), &stop_signal, 1, nullptr, 0, nullptr) == -1)
    throw std::system_error(errno, std::system_category(), "failed to enqueue kqueue stop-signal");

  thr_.join();
}


} /* namespace earnest::detail::aio */
