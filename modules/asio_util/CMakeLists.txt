cmake_minimum_required(VERSION 3.23)

add_library(asio_util INTERFACE)
target_compile_features(asio_util INTERFACE cxx_std_20)
target_sources(asio_util
    INTERFACE
    FILE_SET HEADERS
    BASE_DIRS include
    FILES
    include/earnest/detail/completion_barrier.h
    include/earnest/detail/completion_wrapper.h
    include/earnest/detail/fanout.h
    include/earnest/detail/positional_stream_adapter.h)
target_compile_features(asio_util INTERFACE cxx_std_17)
target_link_libraries(asio_util INTERFACE asio util cycle_ptr spdlog opentelemetry_api)

find_package(Boost REQUIRED)
target_include_directories(asio_util INTERFACE ${Boost_INCLUDE_DIR})

add_executable(completion_handler_fun_test)
target_link_libraries(completion_handler_fun_test PUBLIC asio_util UnitTest++)
target_sources(completion_handler_fun_test PRIVATE tests/completion_handler_fun.cc)
add_test(NAME completion_handler_fun COMMAND completion_handler_fun_test)

add_executable(completion_wrapper_test)
target_link_libraries(completion_wrapper_test PUBLIC asio_util UnitTest++)
target_sources(completion_wrapper_test PRIVATE tests/completion_wrapper.cc)
add_test(NAME completion_wrapper COMMAND completion_wrapper_test)

add_executable(monitor_test)
target_link_libraries(monitor_test PUBLIC asio_util UnitTest++)
target_sources(monitor_test PRIVATE tests/monitor.cc)
add_test(NAME monitor COMMAND monitor_test)
