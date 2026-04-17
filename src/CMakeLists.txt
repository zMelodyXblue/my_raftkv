# ── Global include paths for all source targets ───────────────────
# Every target in src/ can #include "common/foo.h", "raft/bar.h", etc.
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# ── Modules (uncomment as each phase is implemented) ─────────────
add_subdirectory(common)
add_subdirectory(raft)         # Phase 1+
# add_subdirectory(kv_store)   # Phase 4
# add_subdirectory(rpc)        # Phase 2
# add_subdirectory(client)     # Phase 4

# ── Server executable ─────────────────────────────────────────────
add_executable(raftkv_server main.cpp)
target_link_libraries(raftkv_server PRIVATE
  raft
  proto_gen
  spdlog::spdlog
  Threads::Threads
)

# ── CLI client executable ─────────────────────────────────────────
add_executable(raftkv_cli cli.cpp)
target_link_libraries(raftkv_cli PRIVATE
  common
  proto_gen
  spdlog::spdlog
  Threads::Threads
)
