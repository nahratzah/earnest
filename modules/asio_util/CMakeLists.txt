cmake_minimum_required(VERSION 3.23)

add_library(asio_util INTERFACE)
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
target_link_libraries(asio_util INTERFACE asio)
