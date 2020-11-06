cmake_minimum_required(VERSION 3.8)
project (earnest)

option(BUILD_SHARED_LIBS "Build shared libraries" ON)

set(EARNEST_VERSION_MAJOR 0 CACHE STRING "major version" FORCE)
set(EARNEST_VERSION_MINOR 0 CACHE STRING "minor version" FORCE)
set(EARNEST_VERSION ${EARNEST_VERSION_MAJOR}.${EARNEST_VERSION_MINOR} CACHE STRING "version" FORCE)

include (CheckFunctionExists)

enable_testing()
find_package (Boost COMPONENTS container filesystem system REQUIRED)
find_package (instrumentation 1.0 REQUIRED)
find_package (monsoon_cache 0.5 REQUIRED)
find_package (objpipe 0.3 REQUIRED)
find_package (cycle_ptr 0.4 REQUIRED)

# If using ninja and clang, enable colours.
if (UNIX AND
    CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND
    CMAKE_GENERATOR STREQUAL "Ninja")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcolor-diagnostics")
endif()

add_library (earnest
  src/detail/tree/cfg.cc
  src/detail/tree/page.cc
  src/detail/tree/leaf.cc
  src/detail/tree/branch.cc
  src/detail/tree/tree.cc
  src/detail/tree/key_type.cc
  src/detail/tree/value_type.cc
  src/detail/tree/tx_aware_value_type.cc
  src/detail/tree/loader.cc
  src/detail/tree/leaf_iterator.cc
  src/detail/tree/augmented_page_ref.cc
  src/detail/tree/ops.cc
  src/detail/tree/loader_impl.cc
  src/db.cc
  src/db_errc.cc
  src/fd.cc
  src/sequence.cc
  src/tree.cc
  src/tree_error.cc
  src/tx_aware_data.cc
  src/txfile.cc
  src/detail/commit_manager.cc
  src/detail/commit_manager_impl.cc
  src/detail/db_cache.cc
  src/detail/layout_domain.cc
  src/detail/replacement_map.cc
  src/detail/tree_cfg.cc
  src/detail/tree_page.cc
  src/detail/tree_page_leaf_elems.cc
  src/detail/tx_op.cc
  src/detail/tx_sequencer.cc
  src/detail/txfile_allocator.cc
  src/detail/txfile_allocator_log.cc
  src/detail/wal.cc
)
target_include_directories (earnest PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>)
target_include_directories (earnest PUBLIC ${Boost_INCLUDE_DIR})
target_link_libraries (earnest PUBLIC
  instrumentation
  monsoon_cache
  cycle_ptr
  ${Boost_CONTAINER_LIBRARY}
  ${Boost_SYSTEM_LIBRARY}
)
target_link_libraries (earnest PRIVATE
  objpipe
  ${Boost_FILESYSTEM_LIBRARY}
)
set_property (TARGET earnest PROPERTY VERSION ${EARNEST_VERSION})
target_compile_features (earnest PUBLIC cxx_std_17)
set_target_properties (earnest PROPERTIES CXX_EXTENSIONS OFF)

find_path(AsioIncludes asio.hpp)
if(NOT AsioIncludes)
  message(FATAL_ERROR "Can't find folder containing asio.hpp")
endif()
target_include_directories(earnest PUBLIC ${AsioIncludes})

find_package (Threads REQUIRED)
if(CMAKE_USE_PTHREADS_INIT)
  target_link_libraries(earnest PUBLIC ${CMAKE_THREAD_LIBS_INIT})
endif()

if (NOT WIN32)
  CHECK_FUNCTION_EXISTS (mkstemp HAS_MKSTEMP)
  mark_as_advanced (HAS_MKSTEMP)
  if (HAS_MKSTEMP)
    target_compile_definitions(earnest PRIVATE HAS_MKSTEMP)
  else ()
    message (WARNING "no mkstemp() function found")
  endif ()
endif ()

install (TARGETS earnest DESTINATION lib)
install (FILES
  include/earnest/db-inl.h
  include/earnest/db.h
  include/earnest/db_errc.h
  include/earnest/fd.h
  include/earnest/sequence.h
  include/earnest/shared_resource_allocator.h
  include/earnest/tree-inl.h
  include/earnest/tree.h
  include/earnest/tree_error.h
  include/earnest/tx_aware_data.h
  include/earnest/txfile-inl.h
  include/earnest/txfile.h
  DESTINATION include/earnest)
install (FILES
  include/earnest/detail/buffered_read_stream_at.h
  include/earnest/detail/buffered_write_stream_at.h
  include/earnest/detail/cheap_fn_ref.h
  include/earnest/detail/commit_manager-inl.h
  include/earnest/detail/commit_manager.h
  include/earnest/detail/commit_manager_impl-inl.h
  include/earnest/detail/commit_manager_impl.h
  include/earnest/detail/db_cache.h
  include/earnest/detail/export_.h
  include/earnest/detail/layout_domain.h
  include/earnest/detail/replacement_map.h
  include/earnest/detail/stop_continue.h
  include/earnest/detail/tree_cfg.h
  include/earnest/detail/tree_page-inl.h
  include/earnest/detail/tree_page.h
  include/earnest/detail/tree_page_leaf_elems-inl.h
  include/earnest/detail/tree_page_leaf_elems.h
  include/earnest/detail/tree_spec.h
  include/earnest/detail/tx_op-inl.h
  include/earnest/detail/tx_op.h
  include/earnest/detail/tx_sequencer.h
  include/earnest/detail/txfile_allocator-inl.h
  include/earnest/detail/txfile_allocator.h
  include/earnest/detail/txfile_allocator_log.h
  include/earnest/detail/unique_alloc_ptr.h
  include/earnest/detail/wal.h
  include/earnest/detail/wal-inl.h
  DESTINATION include/earnest/detail)
install (FILES
  include/earnest/detail/tree/fwd.h
  include/earnest/detail/tree/loader.h
  include/earnest/detail/tree/cfg.h
  include/earnest/detail/tree/page.h
  include/earnest/detail/tree/leaf.h
  include/earnest/detail/tree/leaf-inl.h
  include/earnest/detail/tree/branch.h
  include/earnest/detail/tree/branch-inl.h
  include/earnest/detail/tree/key_type.h
  include/earnest/detail/tree/value_type.h
  include/earnest/detail/tree/tx_aware_value_type.h
  include/earnest/detail/tree/leaf_iterator.h
  include/earnest/detail/tree/leaf_iterator-inl.h
  include/earnest/detail/tree/augmented_page_ref.h
  include/earnest/detail/tree/ops.h
  include/earnest/detail/tree/ops-inl.h
  include/earnest/detail/tree/loader_impl.h
  include/earnest/detail/tree/loader_impl-inl.h
  DESTINATION include/earnest/detail/tree)

add_subdirectory (tests)

find_package(Doxygen COMPONENTS mscgen OPTIONAL_COMPONENTS dot)

if(DOXYGEN_FOUND)
  doxygen_add_docs(earnest-doc include)
endif()
