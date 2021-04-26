#ifndef EARNEST_DETAIL_AIO_KQUEUE_FD_H
#define EARNEST_DETAIL_AIO_KQUEUE_FD_H

#include <utility>

namespace earnest::detail::aio {


class kqueue_fd {
  public:
  constexpr kqueue_fd() noexcept = default;

  kqueue_fd(const kqueue_fd&) = delete;
  kqueue_fd& operator=(const kqueue_fd&) = delete;

  kqueue_fd(kqueue_fd&& other) noexcept
  : fd_(std::exchange(other.fd_, -1))
  {}

  auto operator=(kqueue_fd&& other) noexcept -> kqueue_fd& {
    fd_ = std::exchange(other.fd_, -1);
    return *this;
  }

  ~kqueue_fd() {
    if (fd_ != -1) close();
  }

  static auto create() -> kqueue_fd;
  void close();

  constexpr auto fd() const noexcept -> int { return fd_; }
  constexpr explicit operator bool() const noexcept { return fd_ != -1; }
  constexpr auto operator!() const noexcept -> bool { return fd_ == -1; }
  auto release() -> int { return std::exchange(fd_, -1); }

  private:
  int fd_{ -1 };
};


} /* namespace earnest::detail::aio */

#endif /* EARNEST_DETAIL_AIO_KQUEUE_FD_H */
