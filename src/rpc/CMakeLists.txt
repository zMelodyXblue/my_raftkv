# ── RPC backend selection ────────────────────────────────────────
if(RAFTKV_RPC_BACKEND STREQUAL "grpc")
  add_subdirectory(grpc)
endif()
