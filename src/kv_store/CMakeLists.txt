add_library(kv_store STATIC kv_store.cpp)
target_link_libraries(kv_store PUBLIC raft proto_gen spdlog::spdlog)
