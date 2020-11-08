#include <earnest/detail/tree/page.h>

#include <earnest/detail/tree/tree.h>
#include <earnest/detail/tree/leaf.h>
#include <earnest/detail/tree/branch.h>
#include <earnest/detail/tree/loader.h>
#include <boost/endian/conversion.hpp>
#include <boost/asio/error.hpp>
#include <boost/system/error_code.hpp>
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


} /* namespace earnest::detail::tree */
