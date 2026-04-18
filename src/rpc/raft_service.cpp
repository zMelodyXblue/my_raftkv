#include "rpc/raft_service.h"

#include <chrono>
#include <spdlog/spdlog.h>

#include "raft/raft.h"

namespace raftkv {

// ── RaftServiceImpl ───────────────────────────────────────────────

RaftServiceImpl::RaftServiceImpl(std::shared_ptr<Raft> raft_node)
    : raft_node_(std::move(raft_node)) {}

grpc::Status RaftServiceImpl::AppendEntries(
    grpc::ServerContext* /*ctx*/,
    const raft::AppendEntriesRequest* req,
    raft::AppendEntriesReply* reply) {
  // Convert proto → internal DTO
  AppendEntriesArgs args;
  args.term           = req->term();
  args.leader_id      = req->leader_id();
  args.prev_log_index = req->prev_log_index();
  args.prev_log_term  = req->prev_log_term();
  args.leader_commit  = req->leader_commit();
  for (int i = 0; i < req->entries_size(); ++i) {
    LogEntry e;
    e.command = req->entries(i).command();
    e.term    = req->entries(i).term();
    e.index   = req->entries(i).index();
    args.entries.push_back(e);
  }

  AppendEntriesReply dto;
  raft_node_->handle_append_entries(args, &dto);

  // Convert internal DTO → proto
  reply->set_term(dto.term);
  reply->set_success(dto.success);
  reply->set_conflict_index(dto.conflict_index);
  reply->set_conflict_term(dto.conflict_term);
  return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::RequestVote(
    grpc::ServerContext* /*ctx*/,
    const raft::RequestVoteRequest* req,
    raft::RequestVoteReply* reply) {
  RequestVoteArgs args;
  args.term           = req->term();
  args.candidate_id   = req->candidate_id();
  args.last_log_index = req->last_log_index();
  args.last_log_term  = req->last_log_term();

  RequestVoteReply dto;
  raft_node_->handle_request_vote(args, &dto);

  reply->set_term(dto.term);
  reply->set_vote_granted(dto.vote_granted);
  return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::InstallSnapshot(
    grpc::ServerContext* /*ctx*/,
    const raft::InstallSnapshotRequest* req,
    raft::InstallSnapshotReply* reply) {
  // Phase 5
  InstallSnapshotArgs args;
  args.term                = req->term();
  args.leader_id           = req->leader_id();
  args.last_included_index = req->last_included_index();
  args.last_included_term  = req->last_included_term();
  args.data                = req->data();

  InstallSnapshotReply dto;
  raft_node_->handle_install_snapshot(args, &dto);

  reply->set_term(dto.term);
  return grpc::Status::OK;
}

// ── RaftPeerClient ────────────────────────────────────────────────

RaftPeerClient::RaftPeerClient(const std::string& addr)
    : stub_(raft::RaftService::NewStub(
          grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()))) {}

bool RaftPeerClient::append_entries(const AppendEntriesArgs& args,
                                    AppendEntriesReply*      reply,
                                    int timeout_ms) {
  // Convert DTO → proto request
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

  // Convert proto reply → DTO
  reply->term           = proto_reply.term();
  reply->success        = proto_reply.success();
  reply->conflict_index = proto_reply.conflict_index();
  reply->conflict_term  = proto_reply.conflict_term();
  return true;
}

bool RaftPeerClient::request_vote(const RequestVoteArgs& args,
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

bool RaftPeerClient::install_snapshot(const InstallSnapshotArgs& args,
                                      InstallSnapshotReply*      reply,
                                      int timeout_ms) {
  // Phase 5
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
