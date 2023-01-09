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
  macro(xdr_test name)
    add_executable(${name})
    target_include_directories(${name} PRIVATE ${UTPP_INCLUDE_DIRS})
    target_link_libraries(${name} PUBLIC xdr asio_util UnitTest++)
    target_sources(${name} PRIVATE tests/${name}.cc)
    add_test(NAME ${name} COMMAND ${name})
  endmacro(xdr_test)

  xdr_test(xdr_bool)
  xdr_test(xdr_bytes)
  xdr_test(xdr_collection)
  xdr_test(xdr_constant)
  xdr_test(xdr_numeric)
  xdr_test(xdr_optional)
  xdr_test(xdr_padding)
  xdr_test(xdr_processor)
  xdr_test(xdr_tuple)
  xdr_test(xdr_variant)
endif ()
