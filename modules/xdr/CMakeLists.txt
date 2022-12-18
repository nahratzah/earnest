cmake_minimum_required(VERSION 3.23)

set(XDR_VERSION_MAJOR 0 CACHE STRING "xdr major version" FORCE)
set(XDR_VERSION_MINOR 0 CACHE STRING "xdr minor version" FORCE)
set(XDR_VERSION ${XDR_VERSION_MAJOR}.${XDR_VERSION_MINOR} CACHE STRING "xdr version" FORCE)
mark_as_advanced(XDR_VERSION_MAJOR)
mark_as_advanced(XDR_VERSION_MINOR)
mark_as_advanced(XDR_VERSION)

add_library(xdr INTERFACE)
set_property(TARGET xdr PROPERTY VERSION ${XDR_VERSION})
target_compile_features(xdr INTERFACE cxx_std_20)
target_sources(xdr
    INTERFACE
    FILE_SET HEADERS
    BASE_DIRS include
    FILES include/earnest/xdr.h include/earnest/xdr.ii)

find_package(Boost REQUIRED)
target_include_directories(xdr INTERFACE ${Boost_INCLUDE_DIR})
target_link_libraries(xdr INTERFACE asio util)

find_package(UnitTest++)
if (UnitTest++_FOUND)
  add_executable(xdr_test)
  target_include_directories(xdr_test PRIVATE ${UTPP_INCLUDE_DIRS})
  target_link_libraries(xdr_test PUBLIC xdr UnitTest++)
  target_sources(xdr_test PRIVATE tests/xdr.cc)
  add_test(NAME xdr COMMAND xdr_test)
endif ()
