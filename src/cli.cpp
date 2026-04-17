#include <iostream>
#include <spdlog/spdlog.h>

// Phase 0: skeleton CLI entry point.
// Phase 4 will replace this with KvClient logic.
int main(int argc, char* argv[]) {
  spdlog::info("raftkv_cli starting (Phase 0 skeleton)");
  if (argc < 2) {
    std::cerr << "Usage: raftkv_cli <put|get|append> [key] [value]\n";
    return 1;
  }
  spdlog::info("Command: {}", argv[1]);
  return 0;
}
