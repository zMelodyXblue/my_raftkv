#pragma once

#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include "proto/raft.grpc.pb.h"
#include "common/raft_peer.h"

namespace raftkv {

// ═════════════════════════════════════════════════════════════════
// GrpcRaftPeerClient — gRPC implementation of RaftPeerClient
// ═════════════════════════════════════════════════════════════════
class GrpcRaftPeerClient : public RaftPeerClient {
 public:
  explicit GrpcRaftPeerClient(const std::string& addr);

  bool append_entries(const AppendEntriesArgs& args,
                      AppendEntriesReply*      reply,
                      int timeout_ms) override;

  bool request_vote(const RequestVoteArgs& args,
                    RequestVoteReply*      reply,
                    int timeout_ms) override;

  bool install_snapshot(const InstallSnapshotArgs& args,
                        InstallSnapshotReply*      reply,
                        int timeout_ms) override;

 private:
  std::unique_ptr<raft::RaftService::Stub> stub_;
};

}  // namespace raftkv
