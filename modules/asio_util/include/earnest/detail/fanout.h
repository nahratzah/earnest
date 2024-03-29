#ifndef EARNEST_DETAIL_FANOUT_H
#define EARNEST_DETAIL_FANOUT_H

#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>

#include <asio/associated_allocator.hpp>
#include <asio/associated_executor.hpp>
#include <asio/async_result.hpp>
#include <asio/executor_work_guard.hpp>

#include <earnest/detail/move_only_function.h>

namespace earnest::detail {


///\brief Completion task that fans out to multiple completion tasks.
template<typename Executor, typename Signature, typename Alloc = std::allocator<std::byte>> class fanout;

/**
 * \brief Completion task that fans out to multiple completion tasks.
 * \details
 * A fanout is a completion handler, that copies its completion arguments
 * to multiple completion handlers.
 *
 * Fanout is copy constructible, with each of the copies sharing their state.
 */
template<typename Executor, typename... Args, typename Alloc>
class fanout<Executor, void(Args...), Alloc> {
  static_assert(std::conjunction_v<std::is_same<std::decay_t<Args>, Args>...>,
      "arguments may not be references, and not be const/volatile");

  public:
  using executor_type = Executor;
  using allocator_type = typename std::allocator_traits<Alloc>::template rebind_alloc<std::byte>;

  private:
  ///\brief Implementation.
  ///\details This is a separate type, so we can use pointer-to-impl approach.
  class impl
  : public std::enable_shared_from_this<impl>
  {
    private:
    using function_type = move_only_function<void(std::shared_ptr<const std::tuple<Args...>>)>;

    public:
    explicit impl(executor_type ex, allocator_type alloc)
    : queued_(std::move(alloc)),
      ex_(std::move(ex))
    {}

    impl(const impl&) = delete;
    impl(impl&&) noexcept = default;
    impl& operator=(const impl&) = delete;
    impl& operator=(impl&&) noexcept = default;

    auto get_allocator() const -> allocator_type {
      return queued_.get_allocator();
    }

    auto get_executor() const -> executor_type {
      return ex_;
    }

    auto ready() const noexcept -> bool {
      std::lock_guard<std::mutex> lck(mtx_);
      return args_ != nullptr;
    }

    auto values() const -> std::optional<std::tuple<Args...>> {
      std::lock_guard<std::mutex> lck(mtx_);
      if (args_) return *args_;
      return std::nullopt;
    }

    template<typename CompletionToken>
    auto async_on_ready(CompletionToken&& token) {
      return asio::async_initiate<CompletionToken, void(Args...)>(
          [](auto completion_handler, std::shared_ptr<impl> self) {
            std::lock_guard<std::mutex> lck(self->mtx_);
            auto ex = asio::get_associated_executor(completion_handler, self->get_executor());
            if (self->args_) {
              auto alloc = asio::get_associated_allocator(completion_handler);
              ex.post(
                  [h=std::move(completion_handler), args=self->args_]() mutable {
                    std::apply(h, *args);
                  },
                  alloc);
            } else {
              self->queued_.emplace_back(
                  [h=std::move(completion_handler), ex=asio::make_work_guard(ex)](std::shared_ptr<const std::tuple<Args...>> args) mutable {
                    auto alloc = asio::get_associated_allocator(h);
                    ex.get_executor().post(
                        [h=std::move(h), args=std::move(args)]() mutable {
                          std::apply(h, *args);
                        },
                        alloc);
                  });
            }
          },
          token, this->shared_from_this());
    }

    auto operator()(Args... args) {
      std::lock_guard<std::mutex> lck(mtx_);
      if (args_) throw std::logic_error("fanout completion handler may only be called once");
      args_ = std::allocate_shared<std::tuple<Args...>>(get_allocator(), std::forward<Args>(args)...);

      std::for_each(queued_.begin(), queued_.end(),
          [this](const function_type& fn) {
            fn(args_);
          });
      queued_.clear();
    }

    private:
    mutable std::mutex mtx_;
    std::vector<function_type, typename std::allocator_traits<Alloc>::template rebind_alloc<function_type>> queued_;
    std::shared_ptr<const std::tuple<Args...>> args_;
    executor_type ex_;
  };

  public:
  ///\brief Construct a new fanout.
  ///\param alloc Use the supplied allocator for allocations.
  explicit fanout(executor_type ex, allocator_type alloc = allocator_type())
  : impl_(std::allocate_shared<impl>(alloc, std::move(ex), alloc))
  {}

  ///\brief Retrieve the fanout allocator.
  auto get_allocator() const -> allocator_type {
    return impl_->get_allocator();
  }

  ///\brief Retrieve the fanout executor.
  auto get_executor() const -> executor_type {
    return impl_->get_executor();
  }

  ///\brief Test if the fanout is ready.
  auto ready() const noexcept -> bool {
    return impl_->ready();
  }

  ///\brief Retrieve values, if ready.
  ///\details
  ///Retrieves the values of the completion.
  ///If the fanout isn't ready, an empty optional is returned.
  ///\attention Uses a copy construction on the values.
  auto values() const -> std::optional<std::tuple<Args...>> {
    return impl_->values();
  }

  ///\brief Add a completion token to the fanout.
  ///\details
  ///The completion token will be completed when the fanout completes.
  ///A fanout can have multiple completion tokens.
  template<typename CompletionToken>
  auto async_on_ready(CompletionToken&& token) {
    return impl_->async_on_ready(std::forward<CompletionToken>(token));
  }

  ///\brief Complete the fanout event.
  ///\details Notifies all queued completion handlers with the arguments.
  auto operator()(Args... args) {
    return std::invoke(*impl_, std::forward<Args>(args)...);
  }

  private:
  std::shared_ptr<impl> impl_;
};


} /* namespace earnest::detail */

#endif /* EARNEST_DETAIL_FANOUT_H */
