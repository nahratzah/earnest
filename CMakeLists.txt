cmake_minimum_required(VERSION 3.8)
project (earnest)
enable_testing()

# If using ninja and clang, enable colours.
if (UNIX AND
    CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND
    CMAKE_GENERATOR STREQUAL "Ninja")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcolor-diagnostics")
endif()

# find_program(CLANG_TIDY_BINARY clang-tidy14)
# if (CLANG_TIDY_BINARY)
#   set(CMAKE_CXX_CLANG_TIDY
#     ${CLANG_TIDY_BINARY};
#     --format-style='file';
#     --header-filter=${CMAKE_CURRENT_SOURCE_DIR};
#     --use-color;
#   )
# endif ()

option(BUILD_SHARED_LIBS "Build shared libraries" ON)

add_subdirectory(modules)

set(EARNEST_VERSION_MAJOR 0 CACHE STRING "major version" FORCE)
set(EARNEST_VERSION_MINOR 0 CACHE STRING "minor version" FORCE)
set(EARNEST_VERSION ${EARNEST_VERSION_MAJOR}.${EARNEST_VERSION_MINOR} CACHE STRING "version" FORCE)

find_package (Boost COMPONENTS container filesystem system REQUIRED)
find_package (instrumentation 1.0 REQUIRED)
find_package (monsoon_cache 0.5 REQUIRED)
find_package (objpipe 0.3 REQUIRED)

add_library (earnest
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
target_compile_features (earnest PUBLIC cxx_std_20)
set_target_properties (earnest PROPERTIES CXX_EXTENSIONS OFF)

target_include_directories(earnest PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/modules/asio/asio/include)
target_link_libraries(earnest PUBLIC asio asio_util io wal)

find_package (Threads REQUIRED)
if(CMAKE_USE_PTHREADS_INIT)
  target_link_libraries(earnest PUBLIC ${CMAKE_THREAD_LIBS_INIT})
endif()

install (TARGETS earnest DESTINATION lib)
install (FILES
  include/earnest/shared_resource_allocator.h
  include/earnest/isolation.h
  include/earnest/tx_mode.h
  DESTINATION include/earnest)
install (FILES
  include/earnest/detail/allocated_buffer.h
  include/earnest/detail/allocated_object.h
  include/earnest/detail/buffered_read_stream_at.h
  include/earnest/detail/cheap_fn_ref.h
  include/earnest/detail/clamp_buffer.h
  include/earnest/detail/closeable_transaction.h
  include/earnest/detail/completion_barrier.h
  include/earnest/detail/completion_wrapper.h
  include/earnest/detail/concat_buffer.h
  include/earnest/detail/export_.h
  include/earnest/detail/fanout.h
  include/earnest/detail/locked_ptr.h
  include/earnest/detail/stop_continue.h
  include/earnest/detail/unique_alloc_ptr.h
  include/earnest/detail/wal-inl.h
  include/earnest/detail/wal.h
  DESTINATION include/earnest/detail)
install (FILES
  $<$<IN_LIST:${IO_BACKEND},aio_kqueue;aio>:modules/base_aio/include/earnest/detail/aio/aio_op.h>
  $<$<IN_LIST:${IO_BACKEND},aio_kqueue;aio>:modules/base_aio/include/earnest/detail/aio/flush_op.h>
  $<$<IN_LIST:${IO_BACKEND},aio_kqueue;aio>:modules/base_aio/include/earnest/detail/aio/lio_op.h>
  $<$<IN_LIST:${IO_BACKEND},aio_kqueue;aio>:modules/base_aio/include/earnest/detail/aio/operation_impl.h>
  $<$<IN_LIST:${IO_BACKEND},aio_kqueue;aio>:modules/base_aio/include/earnest/detail/aio/buffer_holder.h>
  $<$<IN_LIST:${IO_BACKEND},aio_kqueue;aio>:modules/base_aio/include/earnest/detail/aio/aio_op_list.h>
  $<$<STREQUAL:${IO_BACKEND},aio_kqueue>:modules/aio_kqueue/include/earnest/detail/aio/kqueue_fd.h>
  $<$<STREQUAL:${IO_BACKEND},aio_kqueue>:modules/aio_kqueue/include/earnest/detail/aio/kqueue_aio.h>
  $<$<STREQUAL:${IO_BACKEND},aio_kqueue>:modules/aio_kqueue/include/earnest/detail/aio/asio.h>
  DESTINATION include/earnest/detail/aio)

add_subdirectory (tests)

find_package(Doxygen COMPONENTS mscgen OPTIONAL_COMPONENTS dot)

if(DOXYGEN_FOUND)
  doxygen_add_docs(earnest-doc include)
endif()
