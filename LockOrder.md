Lock Ordering
----

We have a lot of locks.
We should make sure we don't get deadlocks.
And that we track their usage.
For that reason, we have created this document, that declares a single lock order.

1. `monsoon::tx::db::db_obj::layout_mtx_` (in ascending order of pointer)
2. `monsoon::tx::detail::commit_manager`:
    1. `monsoon::tx::detail::commit_manager_impl::commit_mtx_` (only one at a time)
    2. `monsoon::tx::detail::commit_manager_impl::mtx_` (only one at a time)
3. `monsoon::tx::detail::abstract_tree_page::mtx_` (root to leaf, when at the same level in forward order of the tree)
4. `monsoon::tx::tx_aware_data::mtx_` (only one at a time)


`txfile_allocator_log`
----

The locks in `txfile_allocator_log` are:
1. `monsoon::tx::txfile_allocator_log::mtx_` (only one)
2. `monsoon::tx::txfile_allocator_log::page::mtx_` (in order of the pages list)

The `monsoon::tx::txfile_allocator_log::mtx_` will be held while the `page_allocator` callback is invoked.
