add_library(rpc STATIC
  raft_service.cpp
  kv_service.cpp
)

target_link_libraries(rpc PUBLIC
  raft
  kv_store
  proto_gen
  spdlog::spdlog
)
