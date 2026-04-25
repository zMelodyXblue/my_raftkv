#include "rpc/grpc/grpc_raft_peer.h"

#include <chrono>

namespace raftkv {

GrpcRaftPeerClient::GrpcRaftPeerClient(const std::string& addr)
    : stub_(raft::RaftService::NewStub(
          grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()))) {}

bool GrpcRaftPeerClient::append_entries(const AppendEntriesArgs& args,
                                        AppendEntriesReply*      reply,
                                        int timeout_ms) {
  raft::AppendEntriesRequest req;
  req.set_term(args.term);
  req.set_leader_id(args.leader_id);
  req.set_prev_log_index(args.prev_log_index);
  req.set_prev_log_term(args.prev_log_term);
  req.set_leader_commit(args.leader_commit);
  for (size_t i = 0; i < args.entries.size(); ++i) {
    raft::LogEntry* pe = req.add_entries();
    pe->set_command(args.entries[i].command);
    pe->set_term(args.entries[i].term);
    pe->set_index(args.entries[i].index);
  }

  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() +
                   std::chrono::milliseconds(timeout_ms));

  raft::AppendEntriesReply proto_reply;
  grpc::Status status = stub_->AppendEntries(&ctx, req, &proto_reply);
  if (!status.ok()) return false;

  reply->term           = proto_reply.term();
  reply->success        = proto_reply.success();
  reply->conflict_index = proto_reply.conflict_index();
  reply->conflict_term  = proto_reply.conflict_term();
  return true;
}

bool GrpcRaftPeerClient::request_vote(const RequestVoteArgs& args,
                                      RequestVoteReply*      reply,
                                      int timeout_ms) {
  raft::RequestVoteRequest req;
  req.set_term(args.term);
  req.set_candidate_id(args.candidate_id);
  req.set_last_log_index(args.last_log_index);
  req.set_last_log_term(args.last_log_term);

  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() +
                   std::chrono::milliseconds(timeout_ms));

  raft::RequestVoteReply proto_reply;
  grpc::Status status = stub_->RequestVote(&ctx, req, &proto_reply);
  if (!status.ok()) return false;

  reply->term         = proto_reply.term();
  reply->vote_granted = proto_reply.vote_granted();
  return true;
}

bool GrpcRaftPeerClient::install_snapshot(const InstallSnapshotArgs& args,
                                          InstallSnapshotReply*      reply,
                                          int timeout_ms) {
  raft::InstallSnapshotRequest req;
  req.set_term(args.term);
  req.set_leader_id(args.leader_id);
  req.set_last_included_index(args.last_included_index);
  req.set_last_included_term(args.last_included_term);
  req.set_data(args.data);

  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() +
                   std::chrono::milliseconds(timeout_ms));

  raft::InstallSnapshotReply proto_reply;
  grpc::Status status = stub_->InstallSnapshot(&ctx, req, &proto_reply);
  if (!status.ok()) return false;

  reply->term = proto_reply.term();
  return true;
}

}  // namespace raftkv
