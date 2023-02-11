cmake_minimum_required(VERSION 3.23)

set(DB_VERSION_MAJOR 0 CACHE STRING "db major version" FORCE)
set(DB_VERSION_MINOR 0 CACHE STRING "db minor version" FORCE)
set(DB_VERSION ${DB_VERSION_MAJOR}.${DB_VERSION_MINOR} CACHE STRING "db version" FORCE)
mark_as_advanced(DB_VERSION_MAJOR)
mark_as_advanced(DB_VERSION_MINOR)
mark_as_advanced(DB_VERSION)

add_library(db INTERFACE)
set_property(TARGET db PROPERTY VERSION ${DB_VERSION})
target_compile_features(db INTERFACE cxx_std_20)
target_sources(db
    INTERFACE
    FILE_SET HEADERS
    BASE_DIRS include
    FILES
    include/earnest/db.h
    include/earnest/db_cache.h)

find_package(Boost REQUIRED)
target_include_directories(db INTERFACE ${Boost_INCLUDE_DIR})
target_link_libraries(db INTERFACE util io asio asio_util wal cycle_ptr)

find_package(UnitTest++)
if (UnitTest++_FOUND)
  add_executable(db_cache_test)
  target_include_directories(db_cache_test PRIVATE ${UTPP_INCLUDE_DIRS})
  target_link_libraries(db_cache_test PUBLIC db UnitTest++)
  target_sources(db_cache_test PRIVATE
      tests/db_cache.cc)
  add_test(NAME db_cache COMMAND db_cache_test)
endif ()
