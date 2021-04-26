#ifndef EARNEST_DETAIL_ALLOCATED_BUFFER_H
#define EARNEST_DETAIL_ALLOCATED_BUFFER_H

#include <cstring>
#include <memory>
#include <type_traits>
#include <utility>

#include <asio/buffer.hpp>

namespace earnest::detail {


template<typename Alloc = std::allocator<void>>
class allocated_buffer
: private std::allocator_traits<Alloc>::template rebind_alloc<unsigned char>
{
  public:
  using allocator_type = typename std::allocator_traits<Alloc>::template rebind_alloc<unsigned char>;
  using size_type = typename std::allocator_traits<allocator_type>::size_type;

  explicit constexpr allocated_buffer(allocator_type alloc = allocator_type())
      noexcept(std::is_nothrow_copy_constructible_v<allocator_type>)
  : allocator_type(alloc)
  {}

  explicit constexpr allocated_buffer(size_type sz, allocator_type alloc = allocator_type())
  : allocator_type(alloc),
    buf_(nullptr),
    sz_(sz)
  {
    if (sz != 0) buf_ = std::allocator_traits<allocator_type>::allocate(*this, sz);
  }

  allocated_buffer(allocated_buffer&& x) noexcept(std::is_nothrow_copy_constructible_v<allocator_type>)
  : allocator_type(std::move(x)),
    buf_(std::exchange(x.buf_, nullptr)),
    sz_(std::exchange(x.sz_, 0))
  {}

  auto operator=(allocated_buffer&& x) noexcept(std::allocator_traits<allocator_type>::propagate_on_container_move_assignment::value) -> allocated_buffer& {
    allocator_type& this_alloc = *this;
    allocator_type& x_alloc = x;

    if constexpr(std::allocator_traits<allocator_type>::propagate_on_container_move_assignment::value) {
      if (buf_ != nullptr) std::allocator_traits<allocator_type>::deallocate(*this, buf_, sz_);

      this_alloc = std::move(x_alloc); // never throws, because propagate_on_container_move_assignment
      buf_ = std::exchange(x.move_, nullptr);
      sz_ = std::exchange(x.sz_, 0);
    } else {
      if (this_alloc == x_alloc) {
        if (buf_ != nullptr) std::allocator_traits<allocator_type>::deallocate(*this, buf_, sz_);

        buf_ = std::exchange(x.buf_, nullptr);
        sz_ = std::exchange(x.sz_, 0);
      } else {
        resize(x.sz_);
        std::memcpy(buf_, x.buf_, sz_);
      }
    }

    return *this;
  }

  ~allocated_buffer() {
    if (buf_ != nullptr) std::allocator_traits<allocator_type>::deallocate(*this, buf_, sz_);
  }

  allocated_buffer(const allocated_buffer&) = delete;
  auto operator=(const allocated_buffer&) -> allocated_buffer& = delete;

  auto get_allocator() const noexcept(std::is_nothrow_copy_constructible_v<allocator_type>) -> allocator_type {
    return *this;
  }

  operator asio::mutable_buffer() const {
    return asio::buffer(data(), sz_);
  }

  auto data() const -> void* {
    return buf_;
  }

  auto size() const -> size_type {
    return sz_;
  }

  /**
   * \brief Resize the buffer.
   * \details
   * Re-allocates the buffer to be \p new_sz bytes.
   * All data is lost in the process.
   *
   * Strong exception guarantee.
   *
   * \param[in] new_sz The new size of the buffer.
   */
  void resize(size_type new_sz) {
    if (new_sz == 0) {
      if (buf_ != nullptr) std::allocator_traits<allocator_type>::deallocate(*this, buf_, sz_);
      buf_ = nullptr;
      return;
    }

    if (new_sz == sz_ && buf_ != nullptr) {
      return;
    }

    unsigned char*const new_buf = std::allocator_traits<allocator_type>::allocate(*this, new_sz);
    try {
      if (buf_ != nullptr) std::allocator_traits<allocator_type>::deallocate(*this, buf_, sz_);
    } catch (...) {
      std::allocator_traits<allocator_type>::deallocate(*this, new_buf, new_sz);
      throw;
    }

    buf_ = new_buf;
    sz_ = new_sz;
  }

  void swap(allocated_buffer& x) noexcept(std::allocator_traits<allocator_type>::propagate_on_container_swap::value) {
    using std::swap;

    allocator_type& this_alloc = *this;
    allocator_type& x_alloc = x;

    if constexpr(std::allocator_traits<allocator_type>::propagate_on_container_swap::value) {
      swap(this_alloc, x_alloc);
      swap(buf_, x.buf_);
      swap(sz_, x.sz_);
    } else {
      assert(this_alloc == x_alloc); // Undefined behaviour if allocators are not-equal.

      swap(buf_, x.buf_);
      swap(sz_, x.sz_);
    }
  }

  private:
  unsigned char* buf_ = nullptr;
  size_type sz_ = 0;
};


template<typename Alloc>
void swap(allocated_buffer<Alloc>& x, allocated_buffer<Alloc>& y)
    noexcept(noexcept(std::declval<allocated_buffer<Alloc>&>().swap(std::declval<allocated_buffer<Alloc>&>()))) {
  x.swap(y);
}


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_ALLOCATED_BUFFER_H */
