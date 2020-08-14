#ifndef EARNEST_DETAIL_TREE_TREE_H
#define EARNEST_DETAIL_TREE_TREE_H

#include <memory>

#include <earnest/detail/export_.h>

#include <earnest/detail/tree/fwd.h>

namespace earnest::detail::tree {


class earnest_export_ abstract_tree {
  public:
  explicit abstract_tree(std::shared_ptr<const cfg> cfg) : cfg(cfg) {}
  virtual ~abstract_tree() noexcept = 0;

  const std::shared_ptr<const cfg> cfg;
};


} /* namespace earnest::detail::tree */

#endif /* EARNEST_DETAIL_TREE_TREE_H */
