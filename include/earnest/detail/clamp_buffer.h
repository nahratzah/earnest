#ifndef EARNEST_DETAIL_CLAMP_BUFFER_H
#define EARNEST_DETAIL_CLAMP_BUFFER_H

#include <cstddef>
#include <iterator>
#include <type_traits>

#include <asio/buffer.hpp>

namespace earnest::detail {


template<typename MB>
class clamp_buffer_t {
  private:
  template<typename T>
  using is_buffer_sequence_of =
      std::is_convertible<
          std::remove_reference_t<decltype(*asio::buffer_sequence_begin(std::declval<MB&>()))>,
          T>;
  template<typename T>
  static constexpr bool is_buffer_sequence_of_v = is_buffer_sequence_of<T>::value;

  using buffer_type = std::conditional_t<
      is_buffer_sequence_of_v<asio::mutable_buffer>,
      asio::mutable_buffer,
      asio::const_buffer>;

  public:
  class iterator;
  using const_iterator = iterator;

  clamp_buffer_t(MB&& mb, std::size_t max_bytes)
  : mb_(std::move(mb)),
    buf_(nullptr, 0),
    end_idx_(0)
  {
    std::size_t total_bytes = 0;
    const auto end_iter = asio::buffer_sequence_end(mb_);
    for (auto iter = asio::buffer_sequence_begin(mb_);
        iter != end_iter;
        ++iter, ++end_idx_) {
      if (max_bytes == total_bytes) break;

      buf_ = *iter;
      if (buf_.size() > max_bytes - total_bytes) {
        buf_ = asio::buffer(buf_, max_bytes - total_bytes);
        ++end_idx_;
      }

      total_bytes += buf_.size();
    }
  }

  auto begin() const -> iterator {
    return iterator(*this);
  }

  auto end() const -> iterator {
    auto i = begin();
    std::advance(i, end_idx_);
    return i;
  }

  private:
  MB mb_;
  buffer_type buf_;
  std::size_t end_idx_;
};


template<typename MB>
class clamp_buffer_t<MB>::iterator {
  private:
  using underlying_iter = std::decay_t<decltype(asio::buffer_sequence_begin(std::declval<const MB&>()))>;

  public:
  using iterator_category = std::bidirectional_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using value_type = buffer_type;
  using reference = buffer_type;
  using pointer = void;

  iterator() = default;

  explicit iterator(const clamp_buffer_t& cb)
  : iter_(asio::buffer_sequence_begin(cb.mb_)),
    cb_(&cb)
  {}

  auto operator++() -> iterator& {
    assert(idx_ < cb_->end_idx_);

    ++iter_;
    ++idx_;
    return *this;
  }

  auto operator--() -> iterator& {
    assert(idx_ > 0);

    --iter_;
    --idx_;
    return *this;
  }

  auto operator++(int) -> iterator {
    iterator copy = *this;
    ++*this;
    return copy;
  }

  auto operator--(int) -> iterator {
    iterator copy = *this;
    --*this;
    return copy;
  }

  auto operator*() const -> buffer_type {
    if (idx_ + 1u == cb_->end_idx_) return cb_->buf_;
    return *iter_;
  }

  auto operator==(const iterator& y) const
      noexcept(noexcept(std::declval<const underlying_iter&>() == std::declval<const underlying_iter&>()))
  -> bool {
    return iter_ == y.iter_ && idx_ == y.idx_ && cb_ == y.cb_;
  }

  auto operator!=(const iterator& y) const
      noexcept(noexcept(std::declval<const underlying_iter&>() == std::declval<const underlying_iter&>()))
  -> bool {
    return !(*this == y);
  }

  private:
  underlying_iter iter_;
  std::size_t idx_ = 0;
  const clamp_buffer_t* cb_ = nullptr;
};


template<typename MB>
auto clamp_buffer(MB&& mb, std::size_t bytes) -> clamp_buffer_t<std::decay_t<MB>> {
  return clamp_buffer_t<std::decay_t<MB>>(std::forward<MB>(mb), bytes);
}


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_CLAMP_BUFFER_H */
