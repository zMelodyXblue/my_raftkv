add_library(raft STATIC
  persister.cpp
  raft.cpp
)

target_link_libraries(raft PUBLIC
  common
  proto_msg
  spdlog::spdlog
  Threads::Threads
)