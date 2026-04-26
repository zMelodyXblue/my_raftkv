add_library(rpc_grpc STATIC
  grpc_raft_peer.cpp
  grpc_raft_service.cpp
  grpc_kv_service.cpp
  grpc_kv_service_client.cpp
  grpc_node_server.cpp
)

target_link_libraries(rpc_grpc PUBLIC
  raft
  kv_store
  proto_grpc
  spdlog::spdlog
)
