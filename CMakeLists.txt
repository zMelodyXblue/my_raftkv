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

# Protobuf: pin to /usr/bin/protoc (apt 3.6.1) so we don't accidentally
# pick up a manually-compiled protoc at a different version.
set(Protobuf_PROTOC_EXECUTABLE /usr/bin/protoc CACHE FILEPATH "" FORCE)
find_package(Protobuf REQUIRED)

# gRPC 1.16.1 from apt; no cmake config file, use pkg-config for link flags.
find_package(PkgConfig REQUIRED)
pkg_check_modules(GRPC REQUIRED grpc++)
pkg_check_modules(GRPC_CORE REQUIRED grpc)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

find_package(spdlog REQUIRED)

# ── Proto code generation ─────────────────────────────────────────
set(PROTO_DIR     ${CMAKE_SOURCE_DIR}/proto)
set(PROTO_GEN_DIR ${CMAKE_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

function(generate_grpc_proto PROTO_FILE)
  get_filename_component(STEM ${PROTO_FILE} NAME_WE)
  set(PB_CC   ${PROTO_GEN_DIR}/${STEM}.pb.cc)
  set(PB_H    ${PROTO_GEN_DIR}/${STEM}.pb.h)
  set(GRPC_CC ${PROTO_GEN_DIR}/${STEM}.grpc.pb.cc)
  set(GRPC_H  ${PROTO_GEN_DIR}/${STEM}.grpc.pb.h)
  add_custom_command(
    OUTPUT  ${PB_CC} ${PB_H} ${GRPC_CC} ${GRPC_H}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
            --proto_path=${PROTO_DIR}
            --cpp_out=${PROTO_GEN_DIR}
            --grpc_out=${PROTO_GEN_DIR}
            --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
            ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
    COMMENT "Generating C++ for ${STEM}.proto"
    VERBATIM
  )
  set(${STEM}_SRCS ${PB_CC} ${GRPC_CC} PARENT_SCOPE)
  set(${STEM}_HDRS ${PB_H}  ${GRPC_H}  PARENT_SCOPE)
endfunction()

generate_grpc_proto(${PROTO_DIR}/raft.proto)
generate_grpc_proto(${PROTO_DIR}/kv.proto)

# ── proto_gen: static library of all generated stubs ─────────────
add_library(proto_gen STATIC ${raft_SRCS} ${kv_SRCS})
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
