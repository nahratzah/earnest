cmake_minimum_required(VERSION 3.23)

add_library(asio_aio_aiothread INTERFACE)
target_compile_features(asio_aio_aiothread INTERFACE cxx_std_20)
target_sources(asio_aio INTERFACE
    FILE_SET HEADERS
    BASE_DIRS include
    FILES include/earnest/detail/aio/aio_reactor.h)

target_link_libraries(asio_aio_aiothread INTERFACE asio)
