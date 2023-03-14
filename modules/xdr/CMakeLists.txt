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

add_executable(xdr_test)
target_link_libraries(xdr_test PUBLIC xdr asio_util UnitTest++)
target_sources(xdr_test PRIVATE
    tests/xdr_bool.cc
    tests/xdr_bytes.cc
    tests/xdr_collection.cc
    tests/xdr_constant.cc
    tests/xdr_numeric.cc
    tests/xdr_optional.cc
    tests/xdr_padding.cc
    tests/xdr_processor.cc
    tests/xdr_tuple.cc
    tests/xdr_variant.cc
    tests/xdr.cc)
add_test(NAME xdr COMMAND xdr_test)
