#pragma once

#include <memory>

#include <grpcpp/grpcpp.h>
#include "proto/raft.grpc.pb.h"

namespace raftkv {

class Raft;

// ═════════════════════════════════════════════════════════════════
// RaftServiceImpl — gRPC server-side handler for Raft RPCs
// ═════════════════════════════════════════════════════════════════
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

  grpc::Status GetStatus(
      grpc::ServerContext* ctx,
      const raft::GetStatusRequest* request,
      raft::GetStatusReply* reply) override;

 private:
  std::shared_ptr<Raft> raft_node_;
};

}  // namespace raftkv
