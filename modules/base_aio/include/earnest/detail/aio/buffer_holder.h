#ifndef EARNEST_DETAIL_AIO_BUFFER_HOLDER_H
#define EARNEST_DETAIL_AIO_BUFFER_HOLDER_H

#include <type_traits>
#include <utility>

namespace earnest::detail::aio {


// Helper type, that ensures buffers are owned and remain valid.
template<typename Buffers>
class buffer_holder {
  protected:
  explicit buffer_holder(Buffers&& buffers)
      noexcept(std::is_nothrow_move_constructible_v<Buffers>)
  : buffers(std::move(buffers))
  {}

  explicit buffer_holder(const Buffers& buffers)
      noexcept(std::is_nothrow_copy_constructible_v<Buffers>)
  : buffers(buffers)
  {}

  ~buffer_holder() = default;

  Buffers buffers;
};


} /* namespace namespace earnest::detail::aio */

#endif /* EARNEST_DETAIL_AIO_BUFFER_HOLDER_H */
