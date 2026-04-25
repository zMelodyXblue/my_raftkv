#include "rpc/grpc/grpc_raft_service.h"

#include <spdlog/spdlog.h>

#include "common/types.h"
#include "raft/raft.h"

namespace raftkv {

RaftServiceImpl::RaftServiceImpl(std::shared_ptr<Raft> raft_node)
    : raft_node_(std::move(raft_node)) {}

grpc::Status RaftServiceImpl::AppendEntries(
    grpc::ServerContext* /*ctx*/,
    const raft::AppendEntriesRequest* req,
    raft::AppendEntriesReply* reply) {
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

grpc::Status RaftServiceImpl::GetStatus(
    grpc::ServerContext* /*ctx*/,
    const raft::GetStatusRequest* /*req*/,
    raft::GetStatusReply* reply) {
  Raft::RaftStatus s = raft_node_->get_status();
  reply->set_node_id(s.node_id);
  reply->set_term(s.term);
  reply->set_role(to_string(s.role));
  reply->set_last_log_index(s.last_log_index);
  reply->set_commit_index(s.commit_index);
  reply->set_last_applied(s.last_applied);
  reply->set_last_snapshot_index(s.last_snapshot_index);
  return grpc::Status::OK;
}

}  // namespace raftkv
