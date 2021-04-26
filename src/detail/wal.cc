#include <earnest/detail/wal.h>

namespace earnest::detail {


auto wal_error_category() -> const std::error_category& {
  class impl_type
  : public std::error_category
  {
    public:
    constexpr impl_type() noexcept = default;
    ~impl_type() override = default;

    const char* name() const noexcept override { return "earnest::wal"; }
    std::string message(int condition) const override {
      switch (static_cast<wal_errc>(condition)) {
        case wal_errc::already_open:
          return "already open";
        case wal_errc::invalid:
          return "invalid wal";
        case wal_errc::bad_opcode:
          return "invalid wal record opcode";
      }

      return "wal error code " + std::to_string(condition);
    }
  };

  static const impl_type impl;
  return impl;
}

auto wal_tx_error_category() -> const std::error_category& {
  class impl_type
  : public std::error_category
  {
    public:
    constexpr impl_type() noexcept = default;
    ~impl_type() override = default;

    const char* name() const noexcept override { return "earnest::wal::tx"; }
    std::string message(int condition) const override {
      switch (static_cast<wal_tx_errc>(condition)) {
        case wal_tx_errc::bad_tx:
          return "bad transaction";
        case wal_tx_errc::write_past_eof:
          return "write past end-of-file";
      }

      return "wal tx error code " + std::to_string(condition);
    }
  };

  static const impl_type impl;
  return impl;
}


} /* namespace earnest::detail */
