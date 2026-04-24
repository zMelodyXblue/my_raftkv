# ── Global include paths for all source targets ───────────────────
# src/  → allows #include "common/foo.h", "raft/bar.h", etc.
# proto/ → allows #include "raft.pb.h", "raft.grpc.pb.h", etc.
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/proto)

# ── Modules (uncomment as each phase is implemented) ─────────────
add_subdirectory(common)
add_subdirectory(raft)         # Phase 1+
add_subdirectory(kv_store)     # Phase 4
add_subdirectory(rpc)          # Phase 2+
add_subdirectory(client)       # Phase 4

# ── Server executable ─────────────────────────────────────────────
add_executable(raftkv_server main.cpp)
target_link_libraries(raftkv_server PRIVATE
  rpc
  spdlog::spdlog
  Threads::Threads
)

# ── CLI client executable ─────────────────────────────────────────
add_executable(raftkv_cli cli.cpp)
target_link_libraries(raftkv_cli PRIVATE
  kv_client
  spdlog::spdlog
  Threads::Threads
)

# ── Benchmark tool ───────────────────────────────────────────────
add_executable(raftkv_bench benchmark.cpp)
target_link_libraries(raftkv_bench PRIVATE
  kv_client
  spdlog::spdlog
  Threads::Threads
)
