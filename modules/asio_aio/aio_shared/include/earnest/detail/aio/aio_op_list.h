#ifndef EARNEST_DETAIL_AIO_AIO_OP_LIST_H
#define EARNEST_DETAIL_AIO_AIO_OP_LIST_H

#include <array>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <utility>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/polymorphic_cast.hpp>

namespace earnest::detail::aio {


class aio_op_list {
  private:
  struct map_tag {};
  struct queue_tag {};

  enum class operation_callback_result {
    // Callback took ownership of the operation and destroyed it.
    destroyed,
    // Callback put the operation on the queue.
    enqueued,
    // Callback started an aio operaton.
    in_progress
  };

  public:
  using map_hook_type = boost::intrusive::unordered_set_base_hook<
      boost::intrusive::tag<map_tag>,
      boost::intrusive::link_mode<boost::intrusive::safe_link>,
      boost::intrusive::optimize_multikey<true>>;
  using queue_hook_type = boost::intrusive::list_base_hook<
      boost::intrusive::tag<queue_tag>,
      boost::intrusive::link_mode<boost::intrusive::safe_link>>;

  private:
  static inline constexpr std::size_t bucket_count = 16384;

  public:
  struct operation_deleter;

  class operation
  : public map_hook_type,
    public queue_hook_type
  {
    friend struct aio_op_list::operation_deleter;
    friend aio_op_list;

    public:
    static constexpr std::size_t max_retries = 3;

    protected:
    explicit operation(aio_op_list& list, int fd) noexcept
    : fd(fd),
      list_(list)
    {}

    ~operation() = default;

    private:
    auto is_map_linked() const -> bool;
    auto is_queue_linked() const -> bool;
    void map_link_locked();
    void map_unlink_locked();
    void queue_link_locked();
    void queue_unlink_locked();

    [[nodiscard]] auto dequeue(std::unique_lock<std::mutex>& lck) && -> operation_callback_result;
    [[nodiscard]] auto on_some_work_done() && -> operation_callback_result;

    virtual void destructor() && = 0; // Destroys this.
    virtual void invoke_handler(std::error_code ec, std::optional<std::size_t> nbytes = std::nullopt, bool use_post = false) && = 0;
    [[nodiscard]] virtual auto invoke() -> std::error_code = 0;
    virtual void cancel() = 0; // Note: always called with list_.mtx_ held.
    [[nodiscard]] virtual auto update() -> std::tuple<std::error_code, std::optional<std::size_t>> = 0;

    public:
    const int fd;

    private:
    std::size_t retry_count_ = 0;
    bool canceled_ = false;
    aio_op_list& list_;
  };

  struct operation_deleter {
    constexpr operation_deleter() noexcept = default;
    void operator()(operation* op) const;
  };

  private:
  template<typename T, typename = std::enable_if_t<std::is_convertible_v<T*, operation*>>>
  using operation_ptr_ = std::unique_ptr<T, operation_deleter>;

  public:
  template<typename T = operation>
  using operation_ptr = operation_ptr_<T>;

  template<typename T, typename Alloc, typename... Args>
  static auto allocate_operation(Alloc alloc, Args&&... args) -> operation_ptr<T> {
    using allocator_traits = typename std::allocator_traits<Alloc>::template rebind_traits<T>;

    // Deletes an allocation.
    struct deallocator {
      explicit deallocator(typename allocator_traits::allocator_type alloc)
      : alloc(alloc)
      {}

      auto operator()(T* ptr) {
        if (destroy) allocator_traits::destroy(alloc, ptr);
        allocator_traits::deallocate(alloc, ptr, 1);
      }

      typename allocator_traits::allocator_type alloc;
      bool destroy = false;
    };

    // Intialize the unique-ptr in two phases, so that if the deallocator
    // construction throws an exception, we won't risk a memory leak.
    auto ptr = std::unique_ptr<T, deallocator>(nullptr, deallocator(alloc));
    ptr.reset(allocator_traits::allocate(ptr.get_deleter().alloc, 1));

    // Initialize pointed-to element.
    if constexpr(std::uses_allocator_v<T, Alloc>) {
      allocator_traits::construct(ptr.get_deleter().alloc, ptr.get(), std::forward<Args>(args)..., alloc);
    } else {
      allocator_traits::construct(ptr.get_deleter().alloc, ptr.get(), std::forward<Args>(args)...);
    }
    ptr.get_deleter().destroy = true;

    // Transfer to operation pointer.
    operation_ptr<T> op_ptr;
    op_ptr.reset(ptr.release()); // Never throws.
    return op_ptr;
  }

  private:
  struct operation_fd {
    using type = int;

    auto operator()(const operation& op) const noexcept -> const type& {
      return op.fd;
    }
  };

  using bucket_array = std::array<boost::intrusive::unordered_bucket<boost::intrusive::base_hook<map_hook_type>>::type, bucket_count>;

  class bucket_traits {
    public:
    explicit bucket_traits(bucket_array& buckets) : buckets_(buckets) {}

    auto bucket_begin() const noexcept -> boost::intrusive::unordered_bucket_ptr<boost::intrusive::base_hook<map_hook_type>>::type {
      return buckets_.data();
    }

    constexpr auto bucket_count() const noexcept -> std::size_t { return buckets_.size(); }

    private:
    bucket_array& buckets_;
  };

  using map_type = boost::intrusive::unordered_multiset<
      operation,
      boost::intrusive::base_hook<map_hook_type>,
      boost::intrusive::constant_time_size<false>,
      boost::intrusive::bucket_traits<bucket_traits>,
      boost::intrusive::power_2_buckets<true>,
      boost::intrusive::key_of_value<operation_fd>>;
  using queue_type = boost::intrusive::list<
      operation,
      boost::intrusive::base_hook<queue_hook_type>,
      boost::intrusive::constant_time_size<false>>;

  public:
  aio_op_list();
  ~aio_op_list();
  aio_op_list(const aio_op_list&) = delete;

  void shutdown(); // Must be called in single-thread context.
  void close_fd(int fd);
  void notify(operation* op);
  void process_queue();
  void on_fork_child(); // Moves everything from active to queued.

  template<typename T>
  auto add(operation_ptr<T> typed_op) -> std::enable_if_t<!std::is_same_v<operation, std::remove_cv_t<T>>> {
    operation_ptr<> op;
    op.reset(typed_op.release()); // Never throws.
    add(std::move(op));
  }

  void add(operation_ptr<> op);

  private:
  std::mutex mtx_;
  bucket_array buckets_;
  map_type map_;
  queue_type queue_;
  std::size_t in_progress_count_ = 0;
};


} /* namespace earnest::detail::aio */

#endif /* EARNEST_DETAIL_AIO_AIO_OP_LIST_H */
