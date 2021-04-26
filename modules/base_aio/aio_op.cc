#include <earnest/detail/aio/aio_op.h>

#include <cerrno>

#include <asio/error.hpp>

namespace earnest::detail::aio {


auto aio_op::invoke() -> std::error_code {
  assert(!in_progress());

restart:
  switch (op_) {
    case op::read:
      if (::aio_read(&iocb_) == 0) return {};
      break;
    case op::write:
      if (::aio_write(&iocb_) == 0) return {};
      break;
  }
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

void aio_op::cancel() {
  switch (::aio_cancel(iocb_.aio_fildes, &iocb_)) {
    case -1:
      throw std::system_error(errno, std::system_category(), "aio_cancel");
    case AIO_CANCELED: [[fallthrough]];
    case AIO_ALLDONE:
      break;
    case AIO_NOTCANCELED:
      switch (::aio_error(&iocb_)) {
        case -1:
          if (errno == EINVAL) break; // Not a queued operation.
          throw std::system_error(errno, std::system_category(), "aio_error");
        case EINPROGRESS:
          throw std::system_error(EINPROGRESS, std::system_category(), "aio_cancel");
        default:
          break;
      }
      break;
  }
}

auto aio_op::update() -> std::tuple<std::error_code, std::optional<std::size_t>> {
  const int e = ::aio_error(&iocb_);
  if (e == -1)
    throw std::system_error(errno, std::system_category(), "aio_error");
  assert(e != EINPROGRESS);

  switch (e) {
    case ECANCELED:
      return std::make_tuple(asio::error::operation_aborted, std::nullopt);
    case EINPROGRESS:
      // Operation should not be in progress during call to update.
      throw std::system_error(EINPROGRESS, std::system_category(), "aio_op::update in progress");
    case 0:
      break;
    default:
      {
        [[maybe_unused]] const auto rv = ::aio_return(&iocb_);
        assert(rv == -1);
      }
      return std::make_tuple(std::error_code(e, std::system_category()), std::nullopt);
  }

  auto nbytes = ::aio_return(&iocb_);
  assert(nbytes >= 0);
  nbytes_ += nbytes;
  iocb_.aio_offset += nbytes;
  iocb_.aio_buf = reinterpret_cast<void*>(reinterpret_cast<std::uintptr_t>(iocb_.aio_buf) + std::size_t(nbytes));
  iocb_.aio_nbytes -= nbytes;

  // Handle EOF.
  if (nbytes_ == 0 && nbytes == 0)
    return std::make_tuple(asio::stream_errc::eof, std::nullopt);

  return std::make_tuple(
      std::error_code(),
      nbytes == 0 || iocb_.aio_nbytes == 0 ? std::optional<std::size_t>(nbytes_) : std::nullopt);
}

auto aio_op::in_progress() const -> bool {
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
