add_library(kv_client STATIC kv_client.cpp)
target_link_libraries(kv_client PUBLIC proto_gen spdlog::spdlog)
