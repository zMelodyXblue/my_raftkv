#pragma once

#include <string>

#include "common/types.h"

namespace raftkv {

// ═════════════════════════════════════════════════════════════════
// RaftPeerClient — Abstract interface for Raft peer communication
// ═════════════════════════════════════════════════════════════════
// Raft core uses this interface to send RPCs to peers.
// The concrete implementation (GrpcRaftPeerClient) lives in rpc/.
//
// This separation allows:
//   - Raft core to have zero gRPC dependency
//   - Mock implementations for unit testing
//   - Future swap to async gRPC without touching Raft code
//
class RaftPeerClient {
 public:
  virtual ~RaftPeerClient() = default;

  // Returns true if the RPC succeeded at the network level.
  // A false reply (vote denied / log mismatch) is still a success here.
  virtual bool append_entries(const AppendEntriesArgs& args,
                              AppendEntriesReply*      reply,
                              int timeout_ms) = 0;

  virtual bool request_vote(const RequestVoteArgs& args,
                            RequestVoteReply*      reply,
                            int timeout_ms) = 0;

  virtual bool install_snapshot(const InstallSnapshotArgs& args,
                                InstallSnapshotReply*      reply,
                                int timeout_ms) = 0;
};

}  // namespace raftkv
