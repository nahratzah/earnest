#include <earnest/detail/aio/kqueue_fd.h>

#include <sys/event.h>
#include <unistd.h>

#include <system_error>

namespace earnest::detail::aio {


auto kqueue_fd::create() -> kqueue_fd {
  kqueue_fd kqfd;
  kqfd.fd_ = ::kqueue();
  if (kqfd.fd_ == -1)
    throw std::system_error(errno, std::system_category(), "kqueue");
  return kqfd;
}

void kqueue_fd::close() {
  if (fd_ != -1) {
    if (::close(fd_) != 0)
      throw std::system_error(errno, std::system_category(), "kqueue close");
    fd_ = -1;
  }
}


} /* namespace earnest::detail::aio */
