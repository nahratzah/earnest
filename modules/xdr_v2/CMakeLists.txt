cmake_minimum_required(VERSION 3.23)

set(XDR_VERSION_MAJOR 0 CACHE STRING "xdr major version" FORCE)
set(XDR_VERSION_MINOR 0 CACHE STRING "xdr minor version" FORCE)
set(XDR_VERSION ${XDR_VERSION_MAJOR}.${XDR_VERSION_MINOR} CACHE STRING "xdr version" FORCE)
mark_as_advanced(XDR_VERSION_MAJOR)
mark_as_advanced(XDR_VERSION_MINOR)
mark_as_advanced(XDR_VERSION)

add_library(xdr_v2 INTERFACE)
set_property(TARGET xdr_v2 PROPERTY VERSION ${XDR_VERSION})
target_compile_features(xdr_v2 INTERFACE cxx_std_20)
target_sources(xdr_v2
    INTERFACE
    FILE_SET HEADERS
    BASE_DIRS include
    FILES include/earnest/xdr.h)

find_package(Boost REQUIRED)
target_include_directories(xdr_v2 INTERFACE ${Boost_INCLUDE_DIR})
target_link_libraries(xdr_v2 INTERFACE execution)

add_executable(xdr_v2_test)
target_link_libraries(xdr_v2_test PUBLIC xdr_v2 UnitTest++)
target_sources(xdr_v2_test PRIVATE
    tests/xdr.cc
    tests/uint8.cc
    tests/uint16.cc
    tests/uint32.cc
    tests/uint64.cc
    tests/manual.cc
    tests/constant.cc
    tests/ascii_string.cc
    tests/collection.cc
    tests/optional.cc
    tests/buffer.cc
    tests/main.cc)
add_test(NAME xdr_v2 COMMAND xdr_v2_test)
