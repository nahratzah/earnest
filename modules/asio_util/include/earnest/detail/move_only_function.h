#pragma once

#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

namespace earnest::detail {


template<typename Signature> class move_only_function;

template<typename R, typename... Args>
class move_only_function<R(Args...)> {
  private:
  class intf {
    public:
    virtual ~intf() = default;
    virtual auto operator()(std::add_rvalue_reference_t<Args>... args) -> R = 0;
  };

  template<typename Fn>
  class impl
  : public intf
  {
    public:
    impl(Fn&& fn)
    : fn_(std::move(fn))
    {}

    impl(const Fn& fn)
    : fn_(fn)
    {}

    ~impl() override = default;

    auto operator()(std::add_rvalue_reference_t<Args>... args) -> R override {
      return std::invoke(fn_, std::forward<Args>(args)...);
    }

    Fn fn_;
  };

  public:
  template<typename Fn>
  move_only_function(Fn&& fn)
  : fn_(std::make_unique<impl<std::remove_cvref_t<Fn>>>(std::forward<Fn>(fn)))
  {}

  auto operator()(Args... args) const -> R {
    return std::invoke(*fn_, std::forward<Args>(args)...);
  }

  private:
  std::unique_ptr<intf> fn_;
};


} /* namespace earnest::detail */
