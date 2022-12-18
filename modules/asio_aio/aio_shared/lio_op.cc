#include <earnest/detail/aio/lio_op.h>

#include <unistd.h>

#include <cerrno>

#include <asio/error.hpp>

namespace earnest::detail::aio {


auto basic_lio_op::listio_max() noexcept -> std::size_t {
#ifdef _SC_AIO_LISTIO_MAX
  const auto sc_listio_max = ::sysconf(_SC_AIO_LISTIO_MAX);
  if (sc_listio_max > 0) return sc_listio_max;
#endif
  return AIO_LISTIO_MAX;
}

auto basic_lio_op::invoke_(struct ::aiocb* iocb_ptr_vec_data[], std::size_t iocb_ptr_vec_size) -> std::error_code {
  assert(!in_progress());

restart:
  if (::lio_listio(
          LIO_NOWAIT, iocb_ptr_vec_data, iocb_ptr_vec_size,
          sigevent_.sigev_notify == SIGEV_NONE ? nullptr : &sigevent_) == 0)
    return {};
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

void basic_lio_op::cancel_(struct ::aiocb* iocb_ptr_vec_data[], std::size_t iocb_ptr_vec_size) {
  std::for_each_n(
      iocb_ptr_vec_data, iocb_ptr_vec_size,
      [](struct ::aiocb* iocb_ptr) {
        switch (::aio_cancel(iocb_ptr->aio_fildes, iocb_ptr)) {
          case -1:
            throw std::system_error(errno, std::system_category(), "aio_cancel");
          case AIO_CANCELED: [[fallthrough]];
          case AIO_ALLDONE:
            break;
          case AIO_NOTCANCELED:
            switch (::aio_error(iocb_ptr)) {
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
      });
}

auto basic_lio_op::update_(struct ::aiocb* iocb_ptr_vec_data[], std::size_t iocb_ptr_vec_size) -> std::tuple<std::error_code, std::optional<std::size_t>> {
  std::error_code ec;
  bool all_done = true;

  std::for_each_n(
      iocb_ptr_vec_data, iocb_ptr_vec_size,
      [this, &ec, &all_done](struct ::aiocb*& iocb_ptr) {
        if (iocb_ptr == nullptr) return;

        const int e = ::aio_error(iocb_ptr);
        if (e == -1)
          throw std::system_error(errno, std::system_category(), "aio_error");
        assert(e != EINPROGRESS);

        switch (e) {
          case ECANCELED:
            {
              [[maybe_unused]] const auto rv = ::aio_return(iocb_ptr);
              assert(rv == -1);
            }
            ec = asio::error::operation_aborted;
            return;
          case EINPROGRESS:
            // Operation should not be in progress during call to update.
            throw std::system_error(EINPROGRESS, std::system_category(), "aio_op::update in progress");
          case 0:
            break;
          default:
            {
              [[maybe_unused]] const auto rv = ::aio_return(iocb_ptr);
              assert(rv == -1);
            }
            if (!ec) ec = std::error_code(e, std::system_category());
            return;
        }

        auto nbytes = ::aio_return(iocb_ptr);
        assert(nbytes >= 0);
        nbytes_ += nbytes;
        iocb_ptr->aio_offset += nbytes;
        iocb_ptr->aio_buf = reinterpret_cast<void*>(reinterpret_cast<std::uintptr_t>(iocb_ptr->aio_buf) + std::size_t(nbytes));
        iocb_ptr->aio_nbytes -= nbytes;

        if (iocb_ptr->aio_nbytes == 0)
          iocb_ptr = nullptr;
        else if (nbytes > 0)
          all_done = false;
      });

  if (!ec && all_done && nbytes_ == 0)
    return std::make_tuple(asio::stream_errc::eof, std::nullopt);

  return std::make_tuple(
      std::move(ec),
      all_done ? std::optional<std::size_t>(nbytes_) : std::nullopt);
}

auto basic_lio_op::in_progress_test_(const struct ::aiocb& iocb) -> bool {
  switch (::aio_error(&iocb)) {
    default:
      return false;
    case -1:
      if (errno == EINVAL) return false; // iocb is not an outstanding event.
      throw std::system_error(errno, std::system_category(), "aio_error");
    case EINPROGRESS:
      return true;
  }
}


template class lio_op<std::allocator<void>>;


} /* namespace earnest::detail::aio */
