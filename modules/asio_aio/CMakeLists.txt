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

add_library(asio_aio INTERFACE)
target_compile_features(asio_aio INTERFACE cxx_std_17)
target_link_libraries(asio_aio INTERFACE asio)

if (IO_BACKEND STREQUAL "overlapped")
  message(SEND_ERROR "overlapped-IO is not yet implemented")
endif ()

if (IO_BACKEND STREQUAL "aio_kqueue")
  message(SEND_ERROR "aio-kqueue is not yet implemented")
endif ()

if (IO_BACKEND STREQUAL "aio")
  message(SEND_ERROR "aio is not fully implemented")
endif ()

if (IO_BACKEND STREQUAL "synchronous" AND WIN32)
  message(SEND_ERROR "sync-io is not implemented for windows")
endif ()

file(CONFIGURE OUTPUT include/earnest/detail/aio/reactor.h
    CONTENT "#pragma once\n\n#include <earnest/detail/aio/@IO_BACKEND@_reactor.h>\n\nnamespace earnest::detail::aio {\nusing reactor = @IO_BACKEND@_reactor;\n}\n"
    @ONLY)
target_sources(asio_aio
    INTERFACE
    FILE_SET HEADERS
    BASE_DIRS ${CMAKE_CURRENT_BINARY_DIR}/include
    FILES ${CMAKE_CURRENT_BINARY_DIR}/include/earnest/detail/aio/reactor.h)

if (NOT WIN32)
  add_subdirectory(synchronous)
  target_link_libraries(asio_aio INTERFACE asio_aio_synchronous)
endif ()

if (HAS_AIO_H)
  add_subdirectory(aio)
  target_link_libraries(asio_aio INTERFACE asio_aio_aiothread)
endif ()
