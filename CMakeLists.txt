cmake_minimum_required(VERSION 3.8)
project (earnest)

option(BUILD_SHARED_LIBS "Build shared libraries" ON)

set(EARNEST_VERSION_MAJOR 0 CACHE STRING "major version" FORCE)
set(EARNEST_VERSION_MINOR 0 CACHE STRING "minor version" FORCE)
set(EARNEST_VERSION ${EARNEST_VERSION_MAJOR}.${EARNEST_VERSION_MINOR} CACHE STRING "version" FORCE)

include (CheckFunctionExists)
include (CheckIncludeFile)

check_include_file (aio.h HAS_AIO_H)
mark_as_advanced (HAS_AIO_H)
check_include_file (sys/event.h HAS_SYS_EVENT_H)
mark_as_advanced (HAS_SYS_EVENT_H)

if (WIN32)
  set (IO_BACKEND "overlapped" CACHE STRING
      "select backend to use for asynchronous IO (one of 'overlapped', 'aio_kqueue', 'aio', 'synchronous')")
elseif (HAS_AIO_H AND HAS_SYS_EVENT_H)
  set (IO_BACKEND "aio_kqueue" CACHE STRING
      "select backend to use for asynchronous IO (one of 'overlapped', 'aio_kqueue', 'aio', 'synchronous')")
elseif (HAS_AIO_H)
  set (IO_BACKEND "aio" CACHE STRING
      "select backend to use for asynchronous IO (one of 'overlapped', 'aio_kqueue', 'aio', 'synchronous')")
else ()
  set (IO_BACKEND "synchronous" CACHE STRING
      "select backend to use for asynchronous IO (one of 'overlapped', 'aio_kqueue', 'aio', 'synchronous')")
endif ()
set_property (CACHE IO_BACKEND PROPERTY STRINGS overlapped aio_kqueue aio synchronous)

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
  src/fd.cc
  src/detail/wal.cc
  src/detail/replacement_map.cc
  $<$<IN_LIST:${IO_BACKEND},aio_kqueue;aio>:modules/base_aio/lio_op.cc>
  $<$<IN_LIST:${IO_BACKEND},aio_kqueue;aio>:modules/base_aio/aio_op.cc>
  $<$<IN_LIST:${IO_BACKEND},aio_kqueue;aio>:modules/base_aio/flush_op.cc>
  $<$<IN_LIST:${IO_BACKEND},aio_kqueue;aio>:modules/base_aio/aio_op_list.cc>
  $<$<STREQUAL:${IO_BACKEND},aio_kqueue>:modules/aio_kqueue/kqueue_fd.cc>
  $<$<STREQUAL:${IO_BACKEND},aio_kqueue>:modules/aio_kqueue/kqueue_aio.cc>
)
target_include_directories (earnest PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>)
target_include_directories (earnest PUBLIC
  $<BUILD_INTERFACE:$<$<STREQUAL:${IO_BACKEND},aio_kqueue>:${CMAKE_CURRENT_SOURCE_DIR}/modules/aio_kqueue/include>>
  $<BUILD_INTERFACE:$<$<STREQUAL:${IO_BACKEND},aio>:${CMAKE_CURRENT_SOURCE_DIR}/modules/aio/include>>
  $<BUILD_INTERFACE:$<$<STREQUAL:${IO_BACKEND},overlapped>:${CMAKE_CURRENT_SOURCE_DIR}/modules/overlapped/include>>
  $<BUILD_INTERFACE:$<$<STREQUAL:${IO_BACKEND},synchronous>:${CMAKE_CURRENT_SOURCE_DIR}/modules/sync_io/include>>
  $<BUILD_INTERFACE:$<$<STREQUAL:${IO_BACKEND},aio_kqueue>:${CMAKE_CURRENT_SOURCE_DIR}/modules/base_aio/include>>
  $<BUILD_INTERFACE:$<$<STREQUAL:${IO_BACKEND},aio>:${CMAKE_CURRENT_SOURCE_DIR}/modules/base_aio/include>>)
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
  include/earnest/fd.h
  include/earnest/shared_resource_allocator.h
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
  include/earnest/detail/replacement_map.h
  include/earnest/detail/replacement_map-inl.h
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
