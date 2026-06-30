package raft

import (
	"context"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	"github.com/raftimpl/mini/internal/raft/pb"
)

// raftGRPCImpl 是为 Raft 内部 RPC 提供的 gRPC 服务实现。
type raftGRPCImpl struct {
	pb.UnimplementedInternalRaftServer
	rf *Raft
}

// RequestVote 处理候选人的投票请求（gRPC 入口），转换为内部类型后委托给 Raft 节点。
func (s *raftGRPCImpl) RequestVote(ctx context.Context, req *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	args := request_vote_from_pb(req)
	var reply RequestVoteReply
	s.rf.handle_request_vote(&args, &reply)
	return request_vote_to_pb(&reply), nil
}

// AppendEntries 处理 Leader 的日志复制/心跳请求（gRPC 入口），转换为内部类型后委托给 Raft 节点。
func (s *raftGRPCImpl) AppendEntries(ctx context.Context, req *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	args := append_entries_from_pb(req)
	var reply AppendEntriesReply
	s.rf.handle_append_entries(&args, &reply)
	return append_entries_to_pb(&reply), nil
}

// request_vote_from_pb 将 protobuf 投票请求转换为内部 RequestVoteArgs。
func request_vote_from_pb(p *pb.RequestVoteArgs) RequestVoteArgs {
	return RequestVoteArgs{
		term:          int(p.Term),
		candidate_id:  int(p.CandidateId),
		last_log_idx:  int(p.LastLogIndex),
		last_log_term: int(p.LastLogTerm),
	}
}

// request_vote_to_pb 将内部投票回复转换为 protobuf 消息。
func request_vote_to_pb(r *RequestVoteReply) *pb.RequestVoteReply {
	return &pb.RequestVoteReply{
		Term:        int32(r.term),
		VoteGranted: r.vote_granted,
	}
}

// append_entries_from_pb 将 protobuf 日志复制请求转换为内部 AppendEntriesArgs。
func append_entries_from_pb(p *pb.AppendEntriesArgs) AppendEntriesArgs {
	ents := make([]LogEntry, len(p.Entries))
	for i, e := range p.Entries {
		ents[i] = LogEntry{Term: int(e.Term), Command: e.Command}
	}
	return AppendEntriesArgs{
		term:               int(p.Term),
		leader_id:          int(p.LeaderId),
		follower_last_idx:  int(p.PrevLogIndex),
		follower_last_term: int(p.PrevLogTerm),
		entries:            ents,
		leader_commit:      int(p.LeaderCommit),
	}
}

// append_entries_to_pb 将内部日志复制回复转换为 protobuf 消息。
func append_entries_to_pb(r *AppendEntriesReply) *pb.AppendEntriesReply {
	return &pb.AppendEntriesReply{
		Term:          int32(r.term),
		Success:       r.success,
		ConflictTerm:  int32(r.conflict_term),
		ConflictIndex: int32(r.conflict_index),
	}
}

// append_entries_args_to_pb 将内部 AppendEntriesArgs 转换为 protobuf 消息，用于发送 RPC。
func append_entries_args_to_pb(a *AppendEntriesArgs) *pb.AppendEntriesArgs {
	ents := make([]*pb.LogEntryMsg, len(a.entries))
	for i, e := range a.entries {
		ents[i] = &pb.LogEntryMsg{Term: int32(e.Term), Command: e.Command}
	}
	return &pb.AppendEntriesArgs{
		Term:         int32(a.term),
		LeaderId:     int32(a.leader_id),
		PrevLogIndex: int32(a.follower_last_idx),
		PrevLogTerm:  int32(a.follower_last_term),
		Entries:      ents,
		LeaderCommit: int32(a.leader_commit),
	}
}

// request_vote_args_to_pb 将内部投票请求转换为 protobuf 消息，用于发送 RPC。
func request_vote_args_to_pb(a *RequestVoteArgs) *pb.RequestVoteArgs {
	return &pb.RequestVoteArgs{
		Term:         int32(a.term),
		CandidateId:  int32(a.candidate_id),
		LastLogIndex: int32(a.last_log_idx),
		LastLogTerm:  int32(a.last_log_term),
	}
}

// append_entries_reply_from_pb 将 protobuf 日志复制回复填充到内部 AppendEntriesReply。
func append_entries_reply_from_pb(p *pb.AppendEntriesReply, r *AppendEntriesReply) {
	r.term = int(p.Term)
	r.success = p.Success
	r.conflict_term = int(p.ConflictTerm)
	r.conflict_index = int(p.ConflictIndex)
}

// request_vote_reply_from_pb 将 protobuf 投票回复填充到内部 RequestVoteReply。
func request_vote_reply_from_pb(p *pb.RequestVoteReply, r *RequestVoteReply) {
	r.term = int(p.Term)
	r.vote_granted = p.VoteGranted
}

// start_raft_grpc 启动 Raft 内部 gRPC 服务，监听指定地址并拨通所有已知 Peer。
func (rf *Raft) start_raft_grpc(bind string) error {
	lis, err := net.Listen("tcp", bind)
	if err != nil {
		return err
	}
	rf.grpc_srv = grpc.NewServer()
	pb.RegisterInternalRaftServer(rf.grpc_srv, &raftGRPCImpl{rf: rf})
	reflection.Register(rf.grpc_srv)
	go func() {
		_ = rf.grpc_srv.Serve(lis)
	}()

	rf.mu.Lock()
	for nodeID, addr := range rf.peers {
		if nodeID == rf.id {
			continue
		}
		go rf.ensure_peer_client(nodeID, addr)
	}
	rf.mu.Unlock()
	return nil
}

// send_request_vote 向指定 Peer 发送投票请求 RPC，结果写入 reply。
// 返回 true 表示 RPC 调用成功，false 表示网络错误或 Peer 不可达。
func (rf *Raft) send_request_vote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	c := rf.get_peer_client(server)
	if c == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := c.RequestVote(ctx, request_vote_args_to_pb(args))
	if err != nil {
		return false
	}
	request_vote_reply_from_pb(out, reply)
	return true
}

// send_append_entries 向指定 Peer 发送日志复制/心跳 RPC，结果写入 reply。
// 返回 true 表示 RPC 调用成功。
func (rf *Raft) send_append_entries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	c := rf.get_peer_client(server)
	if c == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := c.AppendEntries(ctx, append_entries_args_to_pb(args))
	if err != nil {
		return false
	}
	append_entries_reply_from_pb(out, reply)
	return true
}

// stop_grpc 优雅关闭 gRPC 服务端并断开所有 Peer 连接。
func (rf *Raft) stop_grpc() {
	if rf.grpc_srv != nil {
		rf.grpc_srv.Stop()
		rf.grpc_srv = nil
	}
	for _, c := range rf.peer_conns {
		if c != nil {
			_ = c.Close()
		}
	}
	rf.peer_conns = map[int]*grpc.ClientConn{}
	rf.peer_clients = map[int]pb.InternalRaftClient{}
}

// get_peer_client 获取指定 Peer 的 gRPC 客户端，若尚未建立连接则惰性拨号。
func (rf *Raft) get_peer_client(nodeID int) pb.InternalRaftClient {
	rf.mu.Lock()
	client := rf.peer_clients[nodeID]
	addr := rf.peers[nodeID]
	rf.mu.Unlock()
	if client != nil {
		return client
	}
	if addr == "" {
		return nil
	}
	if err := rf.ensure_peer_client(nodeID, addr); err != nil {
		return nil
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.peer_clients[nodeID]
}

// ensure_peer_client 为指定 Peer 建立 gRPC 连接（若已存在旧连接则替换），并记录地址到 peers 映射。
func (rf *Raft) ensure_peer_client(nodeID int, addr string) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial peer %d: %w", nodeID, err)
	}
	client := pb.NewInternalRaftClient(conn)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if old := rf.peer_conns[nodeID]; old != nil {
		_ = old.Close()
	}
	rf.peer_conns[nodeID] = conn
	rf.peer_clients[nodeID] = client
	rf.peers[nodeID] = addr
	return nil
}
