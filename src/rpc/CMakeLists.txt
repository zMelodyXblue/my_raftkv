add_library(rpc STATIC
  raft_service.cpp
  # kv_service.cpp   # Phase 4
)

target_link_libraries(rpc PUBLIC
  raft
  proto_gen
  spdlog::spdlog
)
