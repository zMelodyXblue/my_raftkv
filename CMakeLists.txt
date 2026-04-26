cmake_minimum_required(VERSION 3.16)
project(my_raftkv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(BUILD_TESTS "Build unit tests" ON)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/lib)

# ── RPC backend selection ────────────────────────────────────────
set(RAFTKV_RPC_BACKEND "grpc" CACHE STRING "RPC backend: grpc")
if(RAFTKV_RPC_BACKEND STREQUAL "grpc")
  set(TRANSPORT_LIB rpc_grpc)
endif()

# ── Dependencies ──────────────────────────────────────────────────
find_package(Threads REQUIRED)
find_package(spdlog REQUIRED)

# Protobuf is always needed (for persistence serialization).
option(PIN_SYSTEM_PROTOBUF "Force use of system apt protobuf (/usr/bin, /usr/lib)" OFF)
if(PIN_SYSTEM_PROTOBUF)
  set(Protobuf_PROTOC_EXECUTABLE /usr/bin/protoc CACHE FILEPATH "" FORCE)
  set(Protobuf_LIBRARY /usr/lib/x86_64-linux-gnu/libprotobuf.so CACHE FILEPATH "" FORCE)
  set(Protobuf_LITE_LIBRARY /usr/lib/x86_64-linux-gnu/libprotobuf-lite.so CACHE FILEPATH "" FORCE)
endif()
find_package(Protobuf REQUIRED)

# gRPC is only needed when RAFTKV_RPC_BACKEND=grpc.
if(RAFTKV_RPC_BACKEND STREQUAL "grpc")
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(GRPC REQUIRED grpc++)
  pkg_check_modules(GRPC_CORE REQUIRED grpc)
endif()

# ── proto_msg: protobuf message serialization (no gRPC) ──────────
# To regenerate after editing a .proto file, run from my_raftkv/:
#   /usr/bin/protoc --proto_path=proto --cpp_out=src/proto proto/raft.proto
#   /usr/bin/protoc --proto_path=proto --cpp_out=src/proto proto/kv.proto
set(PROTO_GEN_DIR ${CMAKE_SOURCE_DIR}/src/proto)

add_library(proto_msg STATIC
  ${PROTO_GEN_DIR}/raft.pb.cc
  ${PROTO_GEN_DIR}/kv.pb.cc
)
target_include_directories(proto_msg PUBLIC
  ${PROTO_GEN_DIR}
  ${Protobuf_INCLUDE_DIRS}
)
target_link_libraries(proto_msg PUBLIC
  ${Protobuf_LIBRARIES}
  Threads::Threads
)
target_compile_options(proto_msg PRIVATE -w)

# ── proto_grpc: gRPC service stubs (only for grpc backend) ───────
if(RAFTKV_RPC_BACKEND STREQUAL "grpc")
  # To regenerate gRPC stubs:
  #   /usr/bin/protoc --proto_path=proto --grpc_out=src/proto \
  #     --plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin proto/raft.proto
  #   /usr/bin/protoc --proto_path=proto --grpc_out=src/proto \
  #     --plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin proto/kv.proto
  add_library(proto_grpc STATIC
    ${PROTO_GEN_DIR}/raft.grpc.pb.cc
    ${PROTO_GEN_DIR}/kv.grpc.pb.cc
  )
  target_link_libraries(proto_grpc PUBLIC
    proto_msg
    ${GRPC_LIBRARIES}
    ${GRPC_CORE_LIBRARIES}
  )
  target_compile_options(proto_grpc PRIVATE -w)
endif()

# ── Source modules ────────────────────────────────────────────────
add_subdirectory(src)

# ── Tests ─────────────────────────────────────────────────────────
if(BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
