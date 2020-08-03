cmake_minimum_required(VERSION 3.3)
project (earnest)

set(EARNEST_VERSION_MAJOR 0 CACHE STRING "major version" FORCE)
set(EARNEST_VERSION_MINOR 5 CACHE STRING "minor version" FORCE)
set(EARNEST_VERSION ${EARNEST_VERSION_MAJOR}.${EARNEST_VERSION_MINOR} CACHE STRING "version" FORCE)

include (CheckFunctionExists)

enable_testing()
find_package (Boost COMPONENTS system REQUIRED)
find_package (instrumentation 1.0 REQUIRED)
find_package (monsoon_cache 0.5 REQUIRED)
find_package (cycle_ptr 0.4 REQUIRED)

include (CheckCXXCompilerFlag)
check_cxx_compiler_flag ("-std=c++17" STD_CXX17)
check_cxx_compiler_flag ("-std=c++1z" STD_CXX1Z)
if(STD_CXX17)
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
elseif(STD_CXX1Z)
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z")
else()
  message(WARNING "Cannot figure out how to enable C++17 on compiler.")
endif()

# If using ninja and clang, enable colours.
if (UNIX AND
    CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND
    CMAKE_GENERATOR STREQUAL "Ninja")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcolor-diagnostics")
endif()

add_library (earnest
  src/fd.cc
  src/txfile.cc
  src/sequence.cc
  src/detail/replacement_map.cc
  src/detail/tx_sequencer.cc
  src/detail/wal.cc
  src/db_errc.cc
  src/db.cc
  src/tree.cc
  src/tree_error.cc
  src/detail/tree_cfg.cc
  src/tx_aware_data.cc
  src/detail/commit_manager.cc
  src/detail/commit_manager_impl.cc
  src/detail/txfile_allocator.cc
  src/detail/txfile_allocator_log.cc
  src/detail/db_cache.cc
  src/detail/layout_domain.cc
  src/detail/tx_op.cc
  src/detail/tree_page_leaf_elems.cc
  src/detail/tree_page.cc
)
target_include_directories (earnest PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>)
target_include_directories (earnest PUBLIC ${Boost_INCLUDE_DIR})
target_link_libraries (earnest PUBLIC
  instrumentation
  monsoon_cache
  cycle_ptr
  ${Boost_SYSTEM_LIBRARY}
)
target_link_libraries (earnest PRIVATE
  objpipe
)
set_property (TARGET earnest PROPERTY VERSION ${EARNEST_VERSION})
set_property (TARGET earnest PROPERTY SOVERSION ${EARNEST_VERSION})

find_package (Threads REQUIRED)
if(CMAKE_USE_PTHREADS_INIT)
  target_link_libraries(monsoon_cache INTERFACE ${CMAKE_THREAD_LIBS_INIT})
endif()

install (TARGETS earnest DESTINATION lib)
install (FILES
  include/earnest/txfile.h
  include/earnest/txfile-inl.h
  include/earnest/sequence.h
  include/earnest/db_errc.h
  include/earnest/db.h
  include/earnest/db-inl.h
  include/earnest/tree.h
  include/earnest/tree-inl.h
  include/earnest/tree_error.h
  include/earnest/tx_aware_data.h
  include/earnest/shared_resource_allocator.h
  include/earnest/fd.h
  DESTINATION include/earnest)
install (FILES
  include/earnest/detail/export_.h
  include/earnest/detail/buffered_read_stream_at.h
  include/earnest/detail/buffered_write_stream_at.h
  include/earnest/detail/cheap_fn_ref.h
  include/earnest/detail/replacement_map.h
  include/earnest/detail/tx_sequencer.h
  include/earnest/detail/wal.h
  include/earnest/detail/tree_spec.h
  include/earnest/detail/tree_page.h
  include/earnest/detail/tree_page-inl.h
  include/earnest/detail/tree_cfg.h
  include/earnest/detail/commit_manager.h
  include/earnest/detail/commit_manager-inl.h
  include/earnest/detail/commit_manager_impl.h
  include/earnest/detail/commit_manager_impl-inl.h
  include/earnest/detail/txfile_allocator.h
  include/earnest/detail/txfile_allocator-inl.h
  include/earnest/detail/txfile_allocator_log.h
  include/earnest/detail/db_cache.h
  include/earnest/detail/layout_domain.h
  include/earnest/detail/tx_op.h
  include/earnest/detail/tx_op-inl.h
  include/earnest/detail/stop_continue.h
  include/earnest/detail/tree_page_leaf_elems.h
  include/earnest/detail/tree_page_leaf_elems-inl.h
  DESTINATION include/earnest/detail)

add_subdirectory (tests)

find_package(Doxygen COMPONENTS mscgen OPTIONAL_COMPONENTS dot)

if(DOXYGEN_FOUND)
  doxygen_add_docs(earnest-doc include)
endif()
