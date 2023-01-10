cmake_minimum_required(VERSION 3.23)

set(WAL_VERSION_MAJOR 0 CACHE STRING "wal major version" FORCE)
set(WAL_VERSION_MINOR 0 CACHE STRING "wal minor version" FORCE)
set(WAL_VERSION ${WAL_VERSION_MAJOR}.${WAL_VERSION_MINOR} CACHE STRING "wal version" FORCE)
mark_as_advanced(WAL_VERSION_MAJOR)
mark_as_advanced(WAL_VERSION_MINOR)
mark_as_advanced(WAL_VERSION)

add_library(wal SHARED)
set_property(TARGET wal PROPERTY VERSION ${WAL_VERSION})
target_compile_features(wal PUBLIC cxx_std_20)
target_sources(wal
    PUBLIC
    FILE_SET HEADERS
    BASE_DIRS include
    FILES
    include/earnest/detail/replacement_map.h
    include/earnest/detail/replacement_map.ii
    include/earnest/detail/wal_file_entry.h
    include/earnest/detail/wal_file_entry.ii
    include/earnest/detail/wal_flusher.h
    include/earnest/detail/wal_records.h
    include/earnest/file_id.h
    include/earnest/wal_error.h)
target_sources(wal PRIVATE
    src/replacement_map.cc)

find_package(Boost REQUIRED)
target_include_directories(wal PUBLIC ${Boost_INCLUDE_DIR})
target_link_libraries(wal PUBLIC asio asio_util xdr io)

find_package(UnitTest++)
if (UnitTest++_FOUND)
  file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/wal_file_entry_test_tmpdir")
  file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/wal_file_test_tmpdir")

  add_executable(wal_file_entry_test)
  target_include_directories(wal_file_entry_test PRIVATE ${UTPP_INCLUDE_DIRS})
  target_link_libraries(wal_file_entry_test PUBLIC wal io UnitTest++)
  target_sources(wal_file_entry_test PRIVATE tests/wal_file_entry.cc)
  add_test(NAME wal_file_entry COMMAND wal_file_entry_test "${CMAKE_CURRENT_SOURCE_DIR}/tests/wal_file_entries" "${CMAKE_CURRENT_BINARY_DIR}/wal_file_entry_test_tmpdir")

  add_executable(wal_file_test)
  target_include_directories(wal_file_test PRIVATE ${UTPP_INCLUDE_DIRS})
  target_link_libraries(wal_file_test PUBLIC wal io UnitTest++)
  target_sources(wal_file_test PRIVATE tests/wal_file.cc)
  add_test(NAME wal_file COMMAND wal_file_test "${CMAKE_CURRENT_BINARY_DIR}/wal_file_test_tmpdir")
endif ()
