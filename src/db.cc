#include <earnest/db.h>
#include <earnest/db_errc.h>
#include <earnest/tx_aware_data.h>
#include <earnest/detail/commit_manager.h>
#include <earnest/detail/commit_manager_impl.h>
#include <earnest/detail/buffered_read_stream_at.h>
#include <earnest/detail/buffered_write_stream_at.h>
#include <objpipe/of.h>
#include <boost/endian/conversion.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/read_at.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/write_at.hpp>
#include <algorithm>
#include <cassert>
#include <iterator>
#include <limits>
#include <set>

namespace earnest {
namespace {


/**
 * \brief The header in front of the WAL of the database.
 * \details
 * Validates the database mime-magic, and maintains information on the size of the WAL.
 *
 * Note: this datatype can never be changed, because it is unversioned.
 * When making changes, instead change the types inside the database, which are
 * transactional (and thus can be upgraded atomically).
 */
struct front_header {
  static constexpr std::size_t SIZE = 24;

  static constexpr std::array<std::uint8_t, 16> MAGIC = {{
    17u, 19u,  7u, 11u,
    'M', 'O', 'N', '-',
    's', 'o', 'o', 'n',
    '-', 'd', 'b', '\0'
  }};

  std::uint64_t wal_bytes;

  void read(fd& f, fd::offset_type off);
  template<typename SyncReadStream> void read(SyncReadStream& i);
  void write(fd& f, fd::offset_type off);
  template<typename SyncWriteStream> void write(SyncWriteStream& o);
};

void front_header::read(fd& f, fd::offset_type off) {
  auto i = earnest::detail::buffered_read_stream_at<fd>(f, off, SIZE);
  read(i);
  assert(i.offset() == off + SIZE);
}

template<typename SyncReadStream>
void front_header::read(SyncReadStream& i) {
  std::array<std::uint8_t, MAGIC.size()> magic;
  boost::asio::read(
      i,
      boost::asio::buffer(magic),
      boost::asio::transfer_all());
  if (magic != MAGIC) throw db_invalid_error("magic mismatch");

  boost::asio::read(
      i,
      boost::asio::buffer(&wal_bytes, sizeof(wal_bytes)),
      boost::asio::transfer_all());
  boost::endian::big_to_native_inplace(wal_bytes);
}

void front_header::write(fd& f, fd::offset_type off) {
  auto o = earnest::detail::buffered_write_stream_at<fd>(f, off, SIZE);
  write(o);
  assert(o.offset() == off + SIZE);
}

template<typename SyncWriteStream>
void front_header::write(SyncWriteStream& o) {
  auto enc_wal_bytes = wal_bytes;
  boost::endian::native_to_big_inplace(enc_wal_bytes);

  boost::asio::write(
      o,
      std::array<boost::asio::const_buffer, 2>{
        boost::asio::buffer(MAGIC),
        boost::asio::buffer(&enc_wal_bytes, sizeof(enc_wal_bytes))
      },
      boost::asio::transfer_all());
}


} /* namespace earnest::<unnamed> */


db_invalid_error::~db_invalid_error() noexcept = default;


db::db(std::string name, fd&& file, fd::offset_type off, const db_options& options)
: db(name, validate_header_and_load_wal_(name, std::move(file), off), options)
{}

db::db(std::string name, txfile&& f, const db_options& options)
: f_(std::move(f)),
  obj_cache_(cycle_ptr::allocate_cycle<detail::db_cache>(options.allocator, name, options.max_memory, options.allocator))
{
  std::uint32_t version;

  // Validate version.
  auto t = f_.begin(true);
  boost::asio::read_at(t, DB_OFF_VERSION_, boost::asio::buffer(&version, sizeof(version)), boost::asio::transfer_all());
  boost::endian::big_to_native_inplace(version);
  if (version > VERSION) throw db_invalid_error("unsupported database version");
  t.commit();

  // Version 1: has a commit manager.
  assert(version >= 0);
  if (version == 0) {
    auto tx = f_.begin(false);
    // Update version.
    const std::uint32_t new_version = boost::endian::native_to_big(std::uint32_t(1));
    boost::asio::write_at(tx, DB_OFF_VERSION_, boost::asio::buffer(&new_version, sizeof(new_version)), boost::asio::transfer_all());
    // Initialize transaction sequence.
    detail::commit_manager_impl::init(tx, DB_OFF_TX_ID_SEQ_);
    // Commit the upgrade.
    tx.commit();
    version = 1;
  }
  cm_ = detail::commit_manager::allocate(f_, DB_OFF_TX_ID_SEQ_);
}

auto db::validate_header_and_load_wal_(const std::string& name, fd&& file, fd::offset_type off) -> txfile {
  front_header fh;
  fh.read(file, off);

  return txfile(name, std::move(file), off + front_header::SIZE, fh.wal_bytes);
}

auto db::create(std::string name, fd&& file, fd::offset_type off, fd::size_type wal_len, const db_options& options) -> std::shared_ptr<db> {
  if (wal_len > 0xffff'ffff'ffff'ffffULL)
    throw std::invalid_argument("wal size must be a 64-bit integer");

  front_header fh;
  fh.wal_bytes = wal_len;
  fh.write(file, off);

  auto f = txfile::create(name, std::move(file), off + front_header::SIZE, wal_len);

  // Build up static database elements.
  auto init_tx = f.begin(false);
  init_tx.resize(DB_HEADER_SIZE);
  // Write the version.
  const std::uint32_t version = boost::endian::big_to_native(std::uint32_t(0));
  boost::asio::write_at(init_tx, DB_OFF_VERSION_, boost::asio::buffer(&version, sizeof(version)), boost::asio::transfer_all());
  // Commit all written data.
  init_tx.commit();

  return std::make_shared<db>(std::move(name), std::move(f), options);
}

auto db::begin(bool read_only) -> transaction {
  return transaction(cm_->get_tx_commit_id(), read_only, *this);
}

auto db::begin() const -> transaction {
  return transaction(cm_->get_tx_commit_id(), true, const_cast<db&>(*this));
}


db::transaction_obj::~transaction_obj() noexcept = default;

void db::transaction_obj::do_commit_phase1(detail::commit_manager::write_id& tx) {}
void db::transaction_obj::do_commit_phase2(const detail::commit_manager::commit_id& write_id) noexcept {}
auto db::transaction_obj::do_validate(const detail::commit_manager::commit_id& write_id) -> std::error_code { return {}; }
void db::transaction_obj::do_rollback() noexcept {}

void db::transaction_obj::commit_phase1(detail::commit_manager::write_id& tx) {
  do_commit_phase1(tx);
}

void db::transaction_obj::commit_phase2(const detail::commit_manager::commit_id& write_id) noexcept {
  do_commit_phase2(write_id);
}

auto db::transaction_obj::validate(const detail::commit_manager::commit_id& write_id) -> std::error_code {
  return do_validate(write_id);
}

void db::transaction_obj::rollback() noexcept {
  do_rollback();
}


auto db::transaction::visible(const cycle_ptr::cycle_gptr<const tx_aware_data>& datum) const noexcept -> bool {
  assert(datum != nullptr);
  if (deleted_set_.count(datum) > 0) return false;
  if (created_set_.count(datum) > 0) return true;
  return datum->visible_in_tx(seq_);
}

void db::transaction::commit() {
  if (!active_) throw std::logic_error("commit called on inactive transaction");
  assert(self_.lock() != nullptr);

  if (read_only_) {
    rollback_();
  } else {
    const std::shared_ptr<db> self = std::shared_ptr<db>(self_);
    auto tx = self->cm_->prepare_commit(self->f_);
    const auto layout_locks = lock_all_layouts_();

    commit_phase1_(tx);
    tx.apply(
        [this, &tx]() -> std::error_code {
          return validate_(tx.seq());
        },
        [this, &tx]() noexcept {
          commit_phase2_(tx.seq());
          ops_.commit();
        });
  }

  // Must be the last statement, so that exceptions will not mark this TX as done.
  active_ = false;
}

void db::transaction::rollback() noexcept {
  if (!active_) return;
  assert(self_.lock() != nullptr);

  rollback_();
  ops_.rollback();
  active_ = false;
}

auto db::transaction::lock_all_layouts_() const -> std::unordered_map<cycle_ptr::cycle_gptr<const detail::layout_obj>, detail::layout_lock> {
  using layout_map_element = std::pair<const detail::layout_domain*, cycle_ptr::cycle_gptr<const detail::layout_obj>>;
  struct layout_map_compare {
    auto operator()(const layout_map_element& x, const layout_map_element& y) const -> bool {
      if (x.first == y.first) return x.first->less_compare(*x.second, *y.second);
      return x.first < y.first;
    }
  };
  using layout_map = std::set<layout_map_element, layout_map_compare>;
  using layout_locks_map = std::unordered_map<cycle_ptr::cycle_gptr<const detail::layout_obj>, detail::layout_lock>;

  // Gather all the layouts.
  layout_map layouts = objpipe::of(&deleted_set_, &created_set_, &require_set_)
      .deref()
      .iterate()
      .transform(
          [](const auto& tx_aware_datum) {
            return tx_aware_datum->get_container_for_layout();
          })
      .reduce(
          layout_map(),
          [](auto&& map, auto&& lobj) -> decltype(map) {
            map.emplace(&lobj->get_layout_domain(), std::forward<decltype(lobj)>(lobj));
            return std::forward<decltype(map)>(map);
          });

  // Lock all the layouts.
  bool everything_is_locked;
  layout_locks_map layout_locks;
  do {
    everything_is_locked = true;
    layout_locks = objpipe::of(std::cref(layouts))
        .iterate()
        .select<1>()
        .reduce(
            layout_locks_map(),
            [](auto&& map, const cycle_ptr::cycle_gptr<const detail::layout_obj>& lobj) -> decltype(map) {
              assert(map.count(lobj) == 0);
              map.emplace(lobj, detail::layout_lock(*lobj));
              return std::forward<decltype(map)>(map);
            });

    // Recheck the layouts: until we have the locked, they can shift about
    // and thus change their layout association.
    // In which case we may not have the proper layout locked.
    objpipe::of(&deleted_set_, &created_set_, &require_set_)
        .deref()
        .iterate()
        .transform(
            [](const auto& tx_aware_datum) {
              return tx_aware_datum->get_container_for_layout();
            })
        .for_each(
            [&layout_locks, &layouts, &everything_is_locked](cycle_ptr::cycle_gptr<const detail::layout_obj> lobj) {
              if (layout_locks.count(lobj) == 0) {
                // Update the layouts.
                layouts.emplace(&lobj->get_layout_domain(), std::forward<decltype(lobj)>(lobj));
                // Try to lock the layout. (We use try-lock because we are not respecting lock ordering.)
                detail::layout_lock lck{ *lobj, std::try_to_lock };
                if (lck.owns_lock())
                  layout_locks.emplace(lobj, std::move(lck));
                else // If the lock attempt failed, we'll need to restart the locking process.
                  everything_is_locked = false;
              }
            });
  } while (!everything_is_locked);

  return layout_locks;
}

void db::transaction::commit_phase1_(detail::commit_manager::write_id& tx) {
  // Write all creation records.
  if (!created_set_.empty()) {
    const auto buf = tx_aware_data::make_creation_buffer(tx.seq().val());
    auto offs = objpipe::of(std::cref(created_set_))
        .iterate()
        .transform([](const auto& datum_ptr) { return datum_ptr->offset(); })
        .transform([](const auto& off) { return off + tx_aware_data::CREATION_OFFSET; })
        .to_vector();
    tx.write_at_many(std::move(offs), buf.data(), buf.size());
  }

  // Write all deletion records.
  if (!deleted_set_.empty()) {
    const auto buf = tx_aware_data::make_deletion_buffer(tx.seq().val());
    auto offs = objpipe::of(std::cref(deleted_set_))
        .iterate()
        .transform([](const auto& datum_ptr) { return datum_ptr->offset(); })
        .transform([](const auto& off) { return off + tx_aware_data::DELETION_OFFSET; })
        .to_vector();
    tx.write_at_many(std::move(offs), buf.data(), buf.size());
  }

  // Run phase1 on each transaction object.
  std::for_each(
      callbacks_.begin(), callbacks_.end(),
      [&tx](auto& cb) {
        cb.second->commit_phase1(tx);
      });
}

void db::transaction::commit_phase2_(const detail::commit_manager::commit_id& write_id) noexcept {
  // In-memory: mark all created objects.
  std::for_each(
      created_set_.begin(), created_set_.end(),
      [&write_id](const auto& datum_ptr) {
        const_cast<tx_aware_data&>(*datum_ptr).set_created(write_id.val());
      });

  // In-memory: mark all deleted objects.
  std::for_each(
      deleted_set_.begin(), deleted_set_.end(),
      [&write_id](const auto& datum_ptr) {
        const_cast<tx_aware_data&>(*datum_ptr).set_deleted(write_id.val());
      });

  // Run phase2 on each transaction object.
  std::for_each(
      callbacks_.begin(), callbacks_.end(),
      [&write_id](auto& cb) {
        cb.second->commit_phase2(write_id);
      });
}

auto db::transaction::validate_(const detail::commit_manager::commit_id& write_id) -> std::error_code {
  // Validate all required objects are still here.
  for (const auto& datum : require_set_) {
    if (deleted_set_.count(datum) > 0)
      return db_errc::deleted_required_object_in_tx;
    if (created_set_.count(datum) == 0
        && !datum->visible_in_tx(write_id))
      return db_errc::deleted_required_object;
  }

  // Validate deleted objects are present (avoid double deletes).
  for (const auto& datum : deleted_set_) {
    if (created_set_.count(datum) == 0
        && !datum->visible_in_tx(write_id))
      return db_errc::double_delete;
  }

  // Run callback validations.
  for (auto& cb : callbacks_) {
    std::error_code ec = cb.second->validate(write_id);
    if (ec) return ec;
  }
  return {};
}

void db::transaction::rollback_() noexcept {
  std::for_each(
      callbacks_.begin(), callbacks_.end(),
      [](auto& cb) {
        cb.second->rollback();
      });
}


db::db_obj::~db_obj() noexcept = default;

auto db::db_obj::obj_cache() const -> cycle_ptr::cycle_gptr<detail::db_cache> {
  return db()->obj_cache_;
}

auto db::db_obj::db() const -> std::shared_ptr<class db> {
  return std::shared_ptr<class db>(db_);
}

auto db::db_obj::txfile_begin() const -> txfile::transaction {
  return db()->f_.begin();
}

auto db::db_obj::txfile_begin(bool read_only) const -> txfile::transaction {
  return db()->f_.begin(read_only);
}


} /* namespace earnest */
