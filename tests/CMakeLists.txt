find_package(UnitTest++)

macro (do_test binary)
  add_executable (test_${binary} ${binary}.cc)
  target_link_libraries (test_${binary} PRIVATE earnest UnitTest++)
  add_test (${binary} test_${binary})
endmacro (do_test)


if (UnitTest++_FOUND)
  include_directories(${UTPP_INCLUDE_DIRS})

  # do_test (replacement_map)
  # do_test (wal_record)
  # do_test (wal_region)
  # do_test (txfile)
  # do_test (sequence)
  # do_test (commit_manager_impl)
  # do_test (tree)
  # do_test (tx_mode)
endif ()
