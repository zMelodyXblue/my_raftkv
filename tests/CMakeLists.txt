find_package(GTest REQUIRED)

# Phase 0 smoke test: just links common and proto_msg, verifies the
# build system produces a working test binary.
add_executable(test_smoke smoke_test.cpp)
target_link_libraries(test_smoke PRIVATE
  common
  proto_msg
  GTest::GTest
  GTest::Main
)
add_test(NAME smoke COMMAND test_smoke)

# Phase 1: Persister unit tests
add_executable(test_persister persister_test.cpp)
target_link_libraries(test_persister PRIVATE
  raft
  GTest::GTest
  GTest::Main
)
add_test(NAME persister COMMAND test_persister)

# Phase 2: Leader election integration tests
# Uses real gRPC on loopback; nodes run in-process.
add_executable(test_election election_test.cpp)
target_link_libraries(test_election PRIVATE
  ${TRANSPORT_LIB}
  GTest::GTest
  GTest::Main
)
add_test(NAME election COMMAND test_election)

# Phase 3: Log replication integration tests
add_executable(test_replication replication_test.cpp)
target_link_libraries(test_replication PRIVATE
  ${TRANSPORT_LIB}
  GTest::GTest
  GTest::Main
)
add_test(NAME replication COMMAND test_replication)

# Phase 4: KV state machine integration tests
# Full cluster (Raft + KvStore + KvService) exercised via KvClient.
add_executable(test_kv kv_test.cpp)
target_link_libraries(test_kv PRIVATE
  ${TRANSPORT_LIB}
  kv_client
  GTest::GTest
  GTest::Main
)
add_test(NAME kv COMMAND test_kv)

# Phase 5: Snapshot & crash recovery integration tests
add_executable(test_snapshot snapshot_test.cpp)
target_link_libraries(test_snapshot PRIVATE
  ${TRANSPORT_LIB}
  kv_client
  GTest::GTest
  GTest::Main
)
add_test(NAME snapshot COMMAND test_snapshot)

# Phase 6A: Stress tests (500 ops, concurrent clients, leader switch)
add_executable(test_stress stress_test.cpp)
target_link_libraries(test_stress PRIVATE
  ${TRANSPORT_LIB}
  kv_client
  GTest::GTest
  GTest::Main
)
add_test(NAME stress COMMAND test_stress)

# Phase 6A: Deduplication unit tests
add_executable(test_dedup dedup_test.cpp)
target_link_libraries(test_dedup PRIVATE
  ${TRANSPORT_LIB}
  kv_client
  GTest::GTest
  GTest::Main
)
add_test(NAME dedup COMMAND test_dedup)

# Phase 6B: SkipList unit tests (header-only, no runtime deps)
add_executable(test_skiplist skiplist_test.cpp)
target_link_libraries(test_skiplist PRIVATE
  common
  GTest::GTest
  GTest::Main
)
add_test(NAME skiplist COMMAND test_skiplist)
