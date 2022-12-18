#include <earnest/detail/aio/flush_op.h>

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>

namespace earnest::detail::aio {


auto flush_op::invoke() -> std::error_code {
  assert(!in_progress());

  int op = O_SYNC;
#if _POSIX_SYNCHRONIZED_IO >= 200112L
  if (data_only_) op = O_DSYNC;
#endif

restart:
  if (::aio_fsync(op, &iocb_) == 0) return {};
  const int e = errno;

  assert(!in_progress());
  switch (e) {
    case EINTR:
      goto restart;
    case EAGAIN:
      // Not enough resources.
      // Caller should queue the request.
      [[fallthrough]];
    default:
      return std::error_code(e, std::system_category());
  }
}

auto flush_op::update() -> std::tuple<std::error_code, std::optional<std::size_t>> {
  const int e = ::aio_error(&iocb_);
  if (e == -1)
    throw std::system_error(errno, std::system_category(), "aio_error");
  assert(e != EINPROGRESS);

  switch (e) {
    case EINPROGRESS:
      // Operation should not be in progress during call to update.
      throw std::system_error(EINPROGRESS, std::system_category(), "flush_op::update in progress");
    case 0:
      break;
    default:
      {
        [[maybe_unused]] const auto rv = ::aio_return(&iocb_);
        assert(rv == -1);
      }
      return std::make_tuple(std::error_code(e, std::system_category()), std::nullopt);
  }

  [[maybe_unused]] const auto rv = ::aio_return(&iocb_);
  assert(rv == 0);
  return std::make_tuple(std::error_code(), std::optional<std::size_t>(0));
}

auto flush_op::in_progress() const -> bool {
  switch (::aio_error(&iocb_)) {
    default:
      return false;
    case -1:
      if (errno == EINVAL) return false; // iocb is not an outstanding event.
      throw std::system_error(errno, std::system_category(), "aio_error");
    case EINPROGRESS:
      return true;
  }
}


} /* namespace earnest::detail::aio */
