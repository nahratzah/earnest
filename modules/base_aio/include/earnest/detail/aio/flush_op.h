#ifndef EARNEST_DETAIL_AIO_FLUSH_OP_H
#define EARNEST_DETAIL_AIO_FLUSH_OP_H

#include <aio.h>
#include <signal.h>

#include <cassert>
#include <cstddef>
#include <cstring>
#include <optional>
#include <system_error>
#include <tuple>

#include <earnest/detail/aio/aio_op_list.h>

namespace earnest::detail::aio {


class flush_op
: public aio_op_list::operation
{
  protected:
  flush_op(aio_op_list& list, int fd, bool data_only = false) noexcept
  : aio_op_list::operation(list, fd),
    data_only_(data_only)
  {
    std::memset(&iocb_, 0, sizeof(iocb_));
    iocb_.aio_fildes = fd;
    iocb_.aio_sigevent.sigev_notify = SIGEV_NONE;
  }

  ~flush_op() = default;

  public:
  [[nodiscard]] auto invoke() -> std::error_code override final;
  [[nodiscard]] auto update() -> std::tuple<std::error_code, std::optional<std::size_t>> override final;
  auto sigevent() noexcept -> struct ::sigevent& { return iocb_.aio_sigevent; }
  auto sigevent() const noexcept -> const struct ::sigevent& { return iocb_.aio_sigevent; }

  protected:
  auto in_progress() const -> bool;

  private:
  struct ::aiocb iocb_;
  const bool data_only_;
};


} /* namespace earnest::detail::aio */

#endif /* EARNEST_DETAIL_AIO_FLUSH_OP_H */
