cmake_minimum_required(VERSION 3.23)

set(IO_VERSION_MAJOR 0 CACHE STRING "io major version" FORCE)
set(IO_VERSION_MINOR 0 CACHE STRING "io minor version" FORCE)
set(IO_VERSION ${IO_VERSION_MAJOR}.${IO_VERSION_MINOR} CACHE STRING "io version" FORCE)
mark_as_advanced(IO_VERSION_MAJOR)
mark_as_advanced(IO_VERSION_MINOR)
mark_as_advanced(IO_VERSION)

add_library(io INTERFACE)
set_property(TARGET io PROPERTY VERSION ${IO_VERSION})
target_compile_features(io INTERFACE cxx_std_17)
target_sources(io
    PUBLIC
    FILE_SET HEADERS
    BASE_DIRS include
    FILES include/earnest/fd.h include/earnest/dir.h)

find_package(Boost REQUIRED)
target_include_directories(io INTERFACE ${Boost_INCLUDE_DIR})
target_link_libraries(io INTERFACE asio asio_util asio_aio execution)

include (CheckFunctionExists)
if (NOT WIN32)
  CHECK_FUNCTION_EXISTS (mkstemp HAS_MKSTEMP)
  mark_as_advanced (HAS_MKSTEMP)
  if (HAS_MKSTEMP)
    target_compile_definitions(io INTERFACE HAS_MKSTEMP)
  else ()
    message (WARNING "no mkstemp() function found")
  endif ()
endif ()

# add_executable(io_test)
# target_link_libraries(io_test PUBLIC io UnitTest++)
# target_sources(io_test PRIVATE tests/io.cc)
# add_test(NAME io COMMAND io_test)
