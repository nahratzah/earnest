#ifndef EARNEST_DETAIL_AIO_LIO_OP_H
#define EARNEST_DETAIL_AIO_LIO_OP_H

#include <aio.h>
#include <signal.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <optional>
#include <system_error>
#include <tuple>
#include <utility>
#include <vector>

#include <asio/buffer.hpp>

#include <earnest/detail/aio/aio_op_list.h>
#include <earnest/detail/aio/buffer_holder.h>

namespace earnest::detail::aio {


class basic_lio_op {
  public:
  struct read_op_t {};
  struct write_op_t {};

  static inline constexpr read_op_t read_op{};
  static inline constexpr write_op_t write_op{};

  protected:
  basic_lio_op() {
    std::memset(&sigevent_, 0, sizeof(sigevent_));
    sigevent_.sigev_notify = SIGEV_NONE;
  }

  ~basic_lio_op() = default;

  public:
  static auto listio_max() noexcept -> std::size_t;

  auto sigevent() noexcept -> struct ::sigevent& { return sigevent_; }
  auto sigevent() const noexcept -> const struct ::sigevent& { return sigevent_; }

  protected:
  [[nodiscard]] auto invoke_(struct ::aiocb* iocb_ptr_vec_data[], std::size_t iocb_ptr_vec_size) -> std::error_code;
  void cancel_(struct ::aiocb* iocb_ptr_vec_data[], std::size_t iocb_ptr_vec_size);
  [[nodiscard]] auto update_(struct ::aiocb* iocb_ptr_vec_data[], std::size_t iocb_ptr_vec_size) -> std::tuple<std::error_code, std::optional<std::size_t>>;
  [[nodiscard]] static auto in_progress_test_(const struct ::aiocb& iocb) -> bool;

  virtual auto in_progress() const -> bool = 0;

  private:
  struct ::sigevent sigevent_;
  std::size_t nbytes_ = 0;
};


template<typename VoidAlloc>
class lio_op
: public aio_op_list::operation,
  public basic_lio_op
{
public:
  using allocator_type = VoidAlloc;

private:
  template<typename T>
  using vector_template = std::vector<T, typename std::allocator_traits<allocator_type>::template rebind_alloc<T>>;

protected:
  template<typename Buffers, typename ReadWriteOp>
  lio_op(aio_op_list& list, int fd, std::uint64_t offset, const Buffers& buffers, [[maybe_unused]] ReadWriteOp op_type, allocator_type alloc)
  : aio_op_list::operation(list, fd),
    iocb_vec_(lio_op::build_vector_(fd, offset, buffers, op_type, alloc)),
    iocb_ptr_vec_(lio_op::build_ptr_vector_(iocb_vec_, alloc))
  {}

  lio_op(const lio_op&) = delete;
  lio_op(lio_op&&) = delete;

  ~lio_op() = default;

public:
  [[nodiscard]] auto invoke() -> std::error_code override final {
    return invoke_(iocb_ptr_vec_.data(), iocb_ptr_vec_.size());
  }

  void cancel() override final {
    return cancel_(iocb_ptr_vec_.data(), iocb_ptr_vec_.size());
  }

  [[nodiscard]] auto update() -> std::tuple<std::error_code, std::optional<std::size_t>> override final {
    return update_(iocb_ptr_vec_.data(), iocb_ptr_vec_.size());
  }

protected:
  auto in_progress() const -> bool override final {
    return std::any_of(
        iocb_vec_.cbegin(), iocb_vec_.cend(),
        [](const struct ::aiocb& iocb) { return in_progress_test_(iocb); });
  }

private:
  template<typename Buffers>
  static auto build_vector_(int fd, std::uint64_t offset, const Buffers& buffers, [[maybe_unused]] read_op_t op, allocator_type alloc) -> vector_template<struct ::aiocb> {
    auto iocb_vec = vector_template<struct ::aiocb>(std::move(alloc));
    std::transform(
        asio::buffer_sequence_begin(buffers), asio::buffer_sequence_end(buffers),
        std::back_inserter(iocb_vec),
        [fd, &offset](asio::mutable_buffer buffer) {
          struct ::aiocb iocb;
          std::memset(&iocb, 0, sizeof(iocb));

          iocb.aio_fildes = fd;
          iocb.aio_offset = offset;
          iocb.aio_buf = buffer.data();
          iocb.aio_nbytes = buffer.size();
          iocb.aio_lio_opcode = LIO_READ;
          iocb.aio_sigevent.sigev_notify = SIGEV_NONE;

          offset += buffer.size();
          return iocb;
        });

    const auto max_size = listio_max();
    if (iocb_vec.size() > max_size) iocb_vec.resize(max_size);
    return iocb_vec;
  }

  template<typename Buffers>
  static auto build_vector_(int fd, std::uint64_t offset, const Buffers& buffers, [[maybe_unused]] write_op_t op, allocator_type alloc) -> vector_template<struct ::aiocb> {
    auto iocb_vec = vector_template<struct ::aiocb>(std::move(alloc));
    std::transform(
        asio::buffer_sequence_begin(buffers), asio::buffer_sequence_end(buffers),
        std::back_inserter(iocb_vec),
        [fd, &offset](asio::const_buffer buffer) {
          struct ::aiocb iocb;
          std::memset(&iocb, 0, sizeof(iocb));

          iocb.aio_fildes = fd;
          iocb.aio_offset = offset;
          iocb.aio_buf = const_cast<void*>(buffer.data());
          iocb.aio_nbytes = buffer.size();
          iocb.aio_lio_opcode = LIO_WRITE;
          iocb.aio_sigevent.sigev_notify = SIGEV_NONE;

          offset += buffer.size();
          return iocb;
        });

    const auto max_size = listio_max();
    if (iocb_vec.size() > max_size) iocb_vec.resize(max_size);
    return iocb_vec;
  }

  static auto build_ptr_vector_(vector_template<struct ::aiocb>& iocb_vec, allocator_type alloc) -> vector_template<struct ::aiocb*> {
    auto iocb_ptr_vec = vector_template<struct ::aiocb*>(iocb_vec.size(), alloc);

    std::transform(
        iocb_vec.begin(), iocb_vec.end(),
        iocb_ptr_vec.begin(),
        [](struct ::aiocb& iocb) -> struct ::aiocb* {
          return (iocb.aio_nbytes == 0 ? nullptr : &iocb);
        });

    return iocb_ptr_vec;
  }

  vector_template<struct ::aiocb> iocb_vec_;
  vector_template<struct ::aiocb*> iocb_ptr_vec_;
};


template<typename Buffers, typename VoidAlloc>
class lio_op_with_buffer_ownership
: private buffer_holder<Buffers>,
  public lio_op<VoidAlloc>
{
  public:
  using allocator_type = typename lio_op<VoidAlloc>::allocator_type;

  template<typename ReadWriteOp>
  lio_op_with_buffer_ownership(aio_op_list& list, int fd, std::uint64_t offset, Buffers&& buffers, ReadWriteOp op_type, allocator_type alloc)
  : buffer_holder<Buffers>(std::move(buffers)),
    lio_op<VoidAlloc>(list, fd, offset, this->buffer_holder<Buffers>::buffers, op_type, alloc)
  {}

  template<typename ReadWriteOp>
  lio_op_with_buffer_ownership(aio_op_list& list, int fd, std::uint64_t offset, const Buffers& buffers, ReadWriteOp op_type, allocator_type alloc)
  : buffer_holder<Buffers>(buffers),
    lio_op<VoidAlloc>(list, fd, offset, this->buffer_holder<Buffers>::buffers, op_type, alloc)
  {}

  ~lio_op_with_buffer_ownership() = default;
};


extern template class lio_op<std::allocator<void>>;


} /* namespace earnest::detail::aio */

#endif /* EARNEST_DETAIL_AIO_LIO_OP_H */
