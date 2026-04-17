#pragma once

#include <memory>

#include <grpcpp/grpcpp.h>
#include "raft.grpc.pb.h"

namespace raftkv {

class Raft;  // Forward declaration

// ═════════════════════════════════════════════════════════════════
// RaftServiceImpl — gRPC server-side handler for Raft RPCs
// ═════════════════════════════════════════════════════════════════
// Translates incoming gRPC calls to Raft::handle_*() methods.
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
// RaftPeerClient — gRPC client stub for communicating with one peer
// ═════════════════════════════════════════════════════════════════
class RaftPeerClient {
 public:
  explicit RaftPeerClient(const std::string& addr);

  bool append_entries(const raft::AppendEntriesRequest& req,
                      raft::AppendEntriesReply* reply,
                      int timeout_ms);

  bool request_vote(const raft::RequestVoteRequest& req,
                    raft::RequestVoteReply* reply,
                    int timeout_ms);

  bool install_snapshot(const raft::InstallSnapshotRequest& req,
                        raft::InstallSnapshotReply* reply,
                        int timeout_ms);

 private:
  std::unique_ptr<raft::RaftService::Stub> stub_;
};

}  // namespace raftkv
