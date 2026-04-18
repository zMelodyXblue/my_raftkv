cmake_minimum_required(VERSION 3.16)
project(my_raftkv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(BUILD_TESTS "Build unit tests" ON)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# ── Dependencies ──────────────────────────────────────────────────
find_package(Threads REQUIRED)

# Protobuf: pin all three to the apt (3.6.1) versions so we don't accidentally
# pick up a manually-compiled protoc/library at a different version.
# The library pin matters because /usr/local/lib may contain an old source build.
set(Protobuf_PROTOC_EXECUTABLE /usr/bin/protoc CACHE FILEPATH "" FORCE)
set(Protobuf_LIBRARY /usr/lib/x86_64-linux-gnu/libprotobuf.so CACHE FILEPATH "" FORCE)
set(Protobuf_LITE_LIBRARY /usr/lib/x86_64-linux-gnu/libprotobuf-lite.so CACHE FILEPATH "" FORCE)
find_package(Protobuf REQUIRED)

# gRPC 1.16.1 from apt; no cmake config file, use pkg-config for link flags.
find_package(PkgConfig REQUIRED)
pkg_check_modules(GRPC REQUIRED grpc++)
pkg_check_modules(GRPC_CORE REQUIRED grpc)
find_package(spdlog REQUIRED)

# ── proto_gen: static library of pre-generated stubs ─────────────
# Files are generated once with protoc and committed to src/proto/.
# To regenerate after editing a .proto file, run from my_raftkv/:
#   /usr/bin/protoc --proto_path=proto --cpp_out=src/proto --grpc_out=src/proto \
#     --plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin proto/raft.proto
#   /usr/bin/protoc --proto_path=proto --cpp_out=src/proto --grpc_out=src/proto \
#     --plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin proto/kv.proto
set(PROTO_GEN_DIR ${CMAKE_SOURCE_DIR}/src/proto)

add_library(proto_gen STATIC
  ${PROTO_GEN_DIR}/raft.pb.cc
  ${PROTO_GEN_DIR}/raft.grpc.pb.cc
  ${PROTO_GEN_DIR}/kv.pb.cc
  ${PROTO_GEN_DIR}/kv.grpc.pb.cc
)
target_include_directories(proto_gen PUBLIC
  ${PROTO_GEN_DIR}
  ${Protobuf_INCLUDE_DIRS}
)
target_link_libraries(proto_gen PUBLIC
  ${Protobuf_LIBRARIES}
  ${GRPC_LIBRARIES}
  ${GRPC_CORE_LIBRARIES}
  Threads::Threads
)
target_compile_options(proto_gen PRIVATE -w)  # silence warnings in generated code

# ── Source modules ────────────────────────────────────────────────
add_subdirectory(src)

# ── Tests ─────────────────────────────────────────────────────────
if(BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
