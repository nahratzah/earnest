#ifndef EARNEST_TX_MODE_H
#define EARNEST_TX_MODE_H

#include <type_traits>

namespace earnest {


/**
 * \brief Transaction read/write mode.
 * \details
 * Controls if a transaction is read-only, write-only, or read-write.
 */
enum class tx_mode {
  /**
   * \brief Decline transaction mode.
   * \details
   * Reads will fail.
   * Writes will fail.
   */
  decline = 0,

  /**
   * \brief Read-only transaction mode.
   * \details
   * Reads are permitted.
   * Writes will fail.
   */
  read_only = 1,

  /**
   * \brief Write-only transaction mode.
   * \details
   * Reads will fail.
   * Writes are permitted.
   */
  write_only = 2,

  /**
   * \brief Write-only transaction mode.
   * \details
   * Reads are permitted.
   * Writes are permitted.
   */
  read_write = 3
};


///\brief Test if \p m allows for reading.
constexpr auto read_permitted(tx_mode m) noexcept -> bool {
  return m == tx_mode::read_only || m == tx_mode::read_write;
}

///\brief Test if \p m allows for writing.
constexpr auto write_permitted(tx_mode m) noexcept -> bool {
  return m == tx_mode::write_only || m == tx_mode::read_write;
}

constexpr auto operator&(tx_mode x, tx_mode y) noexcept -> tx_mode {
  std::underlying_type_t<tx_mode> x_val = static_cast<std::underlying_type_t<tx_mode>>(x);
  std::underlying_type_t<tx_mode> y_val = static_cast<std::underlying_type_t<tx_mode>>(y);
  return static_cast<tx_mode>(x_val & y_val);
}

constexpr auto operator|(tx_mode x, tx_mode y) noexcept -> tx_mode {
  std::underlying_type_t<tx_mode> x_val = static_cast<std::underlying_type_t<tx_mode>>(x);
  std::underlying_type_t<tx_mode> y_val = static_cast<std::underlying_type_t<tx_mode>>(y);
  return static_cast<tx_mode>(x_val | y_val);
}

constexpr auto operator~(tx_mode x) noexcept -> tx_mode {
  std::underlying_type_t<tx_mode> x_val = static_cast<std::underlying_type_t<tx_mode>>(x);
  std::underlying_type_t<tx_mode> mask = 3;
  return static_cast<tx_mode>(mask & ~x_val);
}


} /* namespace earnest */

#endif /* EARNEST_TX_MODE_H */
