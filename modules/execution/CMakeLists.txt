cmake_minimum_required(VERSION 3.23)

add_library(execution INTERFACE)
target_compile_features(execution INTERFACE cxx_std_20)
target_sources(execution
    INTERFACE
    FILE_SET HEADERS
    BASE_DIRS include
    FILES
    include/earnest/execution.h)
target_compile_features(execution INTERFACE cxx_std_17)

add_executable(execution_test)
target_link_libraries(execution_test PUBLIC execution UnitTest++)
target_sources(execution_test PRIVATE
	tests/execution.cc
	tests/execution_util.cc
	tests/execution_io.cc
	tests/strand.cc
	tests/temporary_file.cc
	tests/main.cc)
add_test(NAME execution COMMAND execution_test)
