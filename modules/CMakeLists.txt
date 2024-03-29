add_library(asio INTERFACE)
target_include_directories(asio INTERFACE asio/asio/include)
find_package (Threads REQUIRED)
if (CMAKE_USE_PTHREADS_INIT)
  target_link_libraries(asio INTERFACE ${CMAKE_THREAD_LIBS_INIT})
endif ()

set(UTPP_INCLUDE_TESTS_IN_BUILD "NO" CACHE STRING "")
set(UTPP_AMPLIFY_WARNINGS "NO" CACHE STRING "")
add_subdirectory(unittest-cpp)

add_subdirectory(prometheus-cpp)
add_subdirectory(spdlog)
add_subdirectory(opentelemetry-cpp)

add_subdirectory(execution)
add_subdirectory(xdr_v2)
add_subdirectory(cycle_ptr)
add_subdirectory(libhoard)
add_subdirectory(util)
add_subdirectory(asio_util)
add_subdirectory(asio_aio)
add_subdirectory(io)
add_subdirectory(xdr)
add_subdirectory(wal)
add_subdirectory(db)
