#include <earnest/detail/tree/page.h>

#include <earnest/detail/tree/tree.h>
#include <earnest/detail/tree/leaf.h>
#include <earnest/detail/tree/branch.h>
#include <earnest/detail/tree/loader.h>
#include <boost/endian/conversion.hpp>
#include <boost/asio/error.hpp>
#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>
#include <cassert>

namespace earnest::detail::tree {


abstract_page::abstract_page(std::shared_ptr<const struct cfg> tree_config, allocator_type alloc)
: alloc(std::move(alloc)),
  cfg(std::move(tree_config))
{
  assert(tree_config != nullptr);
}

abstract_page::~abstract_page() noexcept = default;

void abstract_page::init() {}

auto abstract_page::decode(
    const loader& loader,
    std::shared_ptr<const struct cfg> tree_config,
    const txfile::transaction& tx,
    offset_type off,
    allocator_type alloc,
    boost::system::error_code& ec)
-> cycle_ptr::cycle_gptr<abstract_page> {
  std::uint32_t magic;
  boost::asio::read_at(tx, off, boost::asio::buffer(&magic, sizeof(magic)), boost::asio::transfer_all());
  boost::endian::big_to_native_inplace(magic);

  cycle_ptr::cycle_gptr<abstract_page> page;
  switch (magic) {
    default:
      ec = boost::asio::error::operation_not_supported;
      return page;
    case leaf::magic:
      page = allocate_page<leaf>(std::move(tree_config), std::move(alloc));
      break;
    case branch::magic:
      page = allocate_page<branch>(std::move(tree_config), std::move(alloc));
      break;
  }

  page->decode_(loader, tx, off, ec);
  return page;
}

auto abstract_page::load_from_disk(
    offset_type off,
    const loader& loader)
-> cycle_ptr::cycle_gptr<abstract_page> {
  return loader.load_from_disk<abstract_page>(
      off,
      [&loader](db_cache::allocator_type alloc, std::shared_ptr<const struct cfg> tree_config, const txfile::transaction& tx, txfile::transaction::offset_type off) -> cycle_ptr::cycle_gptr<abstract_page> {
        boost::system::error_code ec;
        auto page_ptr = decode(loader, std::move(tree_config), tx, off, std::move(alloc), ec);
        if (ec) throw boost::system::error_code(ec);
        return page_ptr;
      });
}


} /* namespace earnest::detail::tree */
