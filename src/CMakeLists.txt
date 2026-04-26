# ── Global include paths for all source targets ───────────────────
# src/  → allows #include "common/foo.h", "raft/bar.h", etc.
# proto/ → allows #include "raft.pb.h", "raft.grpc.pb.h", etc.
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/proto)

# ── Modules (uncomment as each phase is implemented) ─────────────
add_subdirectory(common)
add_subdirectory(raft)         # Phase 1+
add_subdirectory(kv_store)     # Phase 4
add_subdirectory(rpc)          # RPC backend(s)
add_subdirectory(client)       # Phase 4

# ── Server executable ─────────────────────────────────────────────
add_executable(raftkv_server main.cpp)
target_link_libraries(raftkv_server PRIVATE
  ${TRANSPORT_LIB}
  spdlog::spdlog
  Threads::Threads
)

# ── CLI client executable ─────────────────────────────────────────
add_executable(raftkv_cli cli.cpp)
target_link_libraries(raftkv_cli PRIVATE
  kv_client
  ${TRANSPORT_LIB}
  spdlog::spdlog
  Threads::Threads
)

# ── Benchmark tool ───────────────────────────────────────────────
add_executable(raftkv_bench benchmark.cpp)
target_link_libraries(raftkv_bench PRIVATE
  kv_client
  ${TRANSPORT_LIB}
  spdlog::spdlog
  Threads::Threads
)

# ── Redis Protocol Proxy ──────────────────────────────────────────
add_executable(raftkv_redis_proxy redis_proxy/redis_proxy.cpp)
target_link_libraries(raftkv_redis_proxy PRIVATE
  kv_client
  ${TRANSPORT_LIB}
  spdlog::spdlog
  Threads::Threads
)

# ── REST Gateway ─────────────────────────────────────────────────
add_executable(raftkv_gateway gateway/gateway.cpp)
target_link_libraries(raftkv_gateway PRIVATE
  kv_client
  ${TRANSPORT_LIB}
  proto_grpc
  spdlog::spdlog
  Threads::Threads
)
