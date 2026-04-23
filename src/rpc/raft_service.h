#pragma once

#include <memory>

#include <grpcpp/grpcpp.h>
#include "raft.grpc.pb.h"
#include "common/raft_peer.h"
#include "common/types.h"

namespace raftkv {

class Raft;  // Forward declaration

// ═════════════════════════════════════════════════════════════════
// RaftServiceImpl — gRPC server-side handler for Raft RPCs
// ═════════════════════════════════════════════════════════════════
// Translates incoming gRPC calls → internal DTOs → Raft::handle_*().
//
class RaftServiceImpl final : public raft::RaftService::Service {
 public:
  explicit RaftServiceImpl(std::shared_ptr<Raft> raft_node);

  grpc::Status AppendEntries(
      grpc::ServerContext* ctx,
      const raft::AppendEntriesRequest* request,
      raft::AppendEntriesReply* reply) override;

  grpc::Status RequestVote(
      grpc::ServerContext* ctx,
      const raft::RequestVoteRequest* request,
      raft::RequestVoteReply* reply) override;

  grpc::Status InstallSnapshot(
      grpc::ServerContext* ctx,
      const raft::InstallSnapshotRequest* request,
      raft::InstallSnapshotReply* reply) override;

 private:
  std::shared_ptr<Raft> raft_node_;
};

// ═════════════════════════════════════════════════════════════════
// GrpcRaftPeerClient — gRPC implementation of RaftPeerClient
// ═════════════════════════════════════════════════════════════════
// Translates internal DTOs → proto → gRPC call → DTO reply.
//
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
