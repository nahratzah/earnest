cmake_minimum_required(VERSION 3.23)

add_library(util INTERFACE)
target_sources(util
    INTERFACE
    FILE_SET HEADERS
    BASE_DIRS include
    FILES include/earnest/detail/array_cat.h include/earnest/detail/missing_stl.h)
target_compile_features(util INTERFACE cxx_std_17)
target_link_libraries(util INTERFACE prometheus-cpp::core)

add_executable(array_cat_test)
target_link_libraries(array_cat_test PUBLIC util UnitTest++)
target_sources(array_cat_test PRIVATE tests/array_cat.cc)
add_test(NAME array_cat COMMAND array_cat_test)

add_executable(constexpr_sum_test)
target_link_libraries(constexpr_sum_test PUBLIC util UnitTest++)
target_sources(constexpr_sum_test PRIVATE tests/constexpr_sum.cc)
add_test(NAME constexpr_sum COMMAND constexpr_sum_test)

add_executable(adjecent_find_last_test)
target_link_libraries(adjecent_find_last_test PUBLIC util UnitTest++)
target_sources(adjecent_find_last_test PRIVATE tests/adjecent_find_last.cc)
add_test(NAME adjecent_find_last COMMAND adjecent_find_last_test)
