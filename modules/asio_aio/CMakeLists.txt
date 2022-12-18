cmake_minimum_required(VERSION 3.23)

include(CheckIncludeFile)
check_include_file(aio.h HAS_AIO_H)
mark_as_advanced(HAS_AIO_H)
check_include_file(sys/event.h HAS_SYS_EVENT_H)
mark_as_advanced(HAS_SYS_EVENT_H)

if (WIN32)
  set(IO_BACKEND "overlapped" CACHE STRING
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

add_library(asio_aio SHARED)
target_compile_features(asio_aio PUBLIC cxx_std_17)
target_link_libraries(asio_aio PUBLIC asio)

find_package(Boost REQUIRED)
target_include_directories(asio_aio PUBLIC ${Boost_INCLUDE_DIR})

if (IO_BACKEND STREQUAL "overlapped")
  message(SEND_ERROR "overlapped-IO is not yet implemented")
endif ()

if (IO_BACKEND STREQUAL "aio_kqueue" OR IO_BACKEND STREQUAL "aio")
  target_sources(asio_aio PUBLIC
      FILE_SET HEADERS
      BASE_DIRS aio_shared/include
      FILES
      aio_shared/include/earnest/detail/aio/aio_op.h
      aio_shared/include/earnest/detail/aio/flush_op.h
      aio_shared/include/earnest/detail/aio/lio_op.h
      aio_shared/include/earnest/detail/aio/operation_impl.h
      aio_shared/include/earnest/detail/aio/buffer_holder.h
      aio_shared/include/earnest/detail/aio/aio_op_list.h)
  target_sources(asio_aio PRIVATE
      aio_shared/aio_op.cc
      aio_shared/aio_op_list.cc
      aio_shared/flush_op.cc
      aio_shared/lio_op.cc)
endif ()

if (IO_BACKEND STREQUAL "aio_kqueue")
  target_sources(asio_aio PUBLIC
      FILE_SET HEADERS
      BASE_DIRS kqueue/include
      FILES
      kqueue/include/earnest/detail/aio/asio.h
      kqueue/include/earnest/detail/aio/kqueue_aio.h
      kqueue/include/earnest/detail/aio/kqueue_fd.h)
  target_sources(asio_aio PRIVATE kqueue/kqueue_aio.cc kqueue/kqueue_fd.cc)
endif ()

if (IO_BACKEND STREQUAL "aio")
  message(SEND_ERROR "aio is not fully implemented")
endif ()

if (IO_BACKEND STREQUAL "synchronous")
  message(SEND_ERROR "sync-io is not fully implemented")
endif ()
