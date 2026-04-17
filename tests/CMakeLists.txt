find_package(GTest REQUIRED)

# Phase 0 smoke test: just links common and proto_gen, verifies the
# build system produces a working test binary.
add_executable(test_smoke smoke_test.cpp)
target_link_libraries(test_smoke PRIVATE
  common
  proto_gen
  GTest::GTest
  GTest::Main
)
add_test(NAME smoke COMMAND test_smoke)
