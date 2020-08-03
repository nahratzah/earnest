#ifndef EARNEST_TREE_ERROR_H
#define EARNEST_TREE_ERROR_H

#include <earnest/detail/export_.h>
#include <stdexcept>

namespace earnest {


class earnest_export_ tree_error
: public std::runtime_error {
  public:
  using std::runtime_error::runtime_error;
  ~tree_error() noexcept override;
};


} /* namespace earnest */

#endif /* EARNEST_TREE_ERROR_H */
