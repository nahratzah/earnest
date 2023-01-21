#pragma once

#include <functional>
#include <iterator>
#include <utility>

namespace earnest::detail {


template<typename BidirectionalIt, typename BinaryPredicate>
inline auto adjecent_find_last(BidirectionalIt first, BidirectionalIt last, BinaryPredicate p) -> BidirectionalIt {
  if (last == first) return last;
  BidirectionalIt iter = std::prev(last);
  if (iter == first) return last;

  for (;;) {
    BidirectionalIt successor = iter;
    --iter;
    if (p(*iter, *successor)) return iter;
    if (iter == first) return last;
  }
}


template<typename BidirectionalIt>
inline auto adjecent_find_last(BidirectionalIt first, BidirectionalIt last) -> BidirectionalIt {
  return adjecent_find_last(std::move(first), std::move(last), std::equal_to<void>());
}


} /* namespace earnest::detail */
