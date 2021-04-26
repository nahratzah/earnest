#ifndef EARNEST_DETAIL_AIO_AIO_OP_H
#define EARNEST_DETAIL_AIO_AIO_OP_H

#include <aio.h>
#include <signal.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <system_error>
#include <tuple>
#include <utility>

#include <asio/buffer.hpp>

#include <earnest/detail/aio/aio_op_list.h>
#include <earnest/detail/aio/buffer_holder.h>

namespace earnest::detail::aio {


class aio_op
: public aio_op_list::operation
{
  private:
  enum class op { read, write };

  public:
  struct read_op_t {};
  struct write_op_t {};

  static inline constexpr read_op_t read_op{};
  static inline constexpr write_op_t write_op{};

  private:
  aio_op(aio_op_list& list, op o, int fd, std::uint64_t offset, void* buf, std::size_t buflen) noexcept
  : aio_op_list::operation(list, fd),
    op_(o)
  {
    std::memset(&iocb_, 0, sizeof(iocb_));
    iocb_.aio_fildes = fd;
    iocb_.aio_offset = offset;
    iocb_.aio_buf = buf;
    iocb_.aio_nbytes = buflen;
    iocb_.aio_sigevent.sigev_notify = SIGEV_NONE;
  }

  protected:
  aio_op(aio_op_list& list, int fd, std::uint64_t offset, asio::mutable_buffer buffer, [[maybe_unused]] read_op_t read_op) noexcept
  : aio_op(list, op::read, fd, offset, buffer.data(), buffer.size())
  {}

  aio_op(aio_op_list& list, int fd, std::uint64_t offset, asio::const_buffer buffer, [[maybe_unused]] write_op_t write_op) noexcept
  : aio_op(list, op::write, fd, offset, const_cast<void*>(buffer.data()), buffer.size())
  {}

  ~aio_op() = default;

  public:
  [[nodiscard]] auto invoke() -> std::error_code override final;
  void cancel() override final;
  [[nodiscard]] auto update() -> std::tuple<std::error_code, std::optional<std::size_t>> override final;
  auto sigevent() noexcept -> struct ::sigevent& { return iocb_.aio_sigevent; }
  auto sigevent() const noexcept -> const struct ::sigevent& { return iocb_.aio_sigevent; }

  protected:
  auto in_progress() const -> bool;

  private:
  struct ::aiocb iocb_;
  const op op_;
  std::size_t nbytes_ = 0;
};


template<typename Buffer>
class aio_op_with_buffer_ownership
: private buffer_holder<Buffer>,
  public aio_op
{
  protected:
  template<typename ReadWriteOp>
  aio_op_with_buffer_ownership(aio_op_list& list, int fd, std::uint64_t offset, Buffer&& buffer, ReadWriteOp op_type)
  : buffer_holder<Buffer>(std::move(buffer)),
    aio_op(list, fd, offset, this->buffer_holder<Buffer>::buffers, op_type)
  {}

  template<typename ReadWriteOp>
  aio_op_with_buffer_ownership(aio_op_list& list, int fd, std::uint64_t offset, const Buffer& buffer, ReadWriteOp op_type)
  : buffer_holder<Buffer>(buffer),
    aio_op(list, fd, offset, this->buffer_holder<Buffer>::buffers, op_type)
  {}

  ~aio_op_with_buffer_ownership() = default;
};


} /* namespace namespace earnest::detail::aio */

#endif /* EARNEST_DETAIL_AIO_AIO_OP_H */
