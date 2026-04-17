#include <iostream>
#include <spdlog/spdlog.h>

// Phase 0: skeleton entry point.
// Subsequent phases will replace this with actual server startup logic.
int main(int argc, char* argv[]) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::info("raftkv_server starting (Phase 0 skeleton)");

  // Smoke-test: verify proto_gen library linked correctly by
  // referencing a generated type.  Remove once real code is in place.
  // (Uncomment after proto generation succeeds)
  // raftkv::raft::LogEntry entry;
  // entry.set_term(1);
  // spdlog::info("proto_gen linked: LogEntry term={}", entry.term());

  spdlog::info("Nothing to do yet. Exiting.");
  return 0;
}
