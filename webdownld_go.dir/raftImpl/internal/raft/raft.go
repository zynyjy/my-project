package raft

import (
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/raftimpl/mini/internal/raft/pb"
)

// ApplyMsg 已提交并应用到状态机的一条日志（供 HTTP 层更新本地 KV）。
type ApplyMsg struct {
	Index int
	Op    Op
	Done  chan struct{}
}

// Raft 手写 Raft 节点。
type Raft struct {
	mu sync.Mutex

	id        int
	bind_addr string
	peers     map[int]string // nodeID -> raftAddr

	current_term int
	voted_for    int // -1 表示未投票
	log          []LogEntry

	commit_index int
	last_applied int

	state ServerState

	// leader_last_idx：Leader 视角，下一次要向该 Follower 发送的日志起始下标（原论文 nextIndex）。
	leader_last_idx map[int]int
	// follower_matched_idx：该 Follower 上已确认复制到的最后日志下标（原论文 matchIndex；0 表示尚未对齐确认）。
	follower_matched_idx map[int]int

	election_deadline time.Time
	heartbeat_due     time.Time

	apply_cond *sync.Cond
	apply_ch   chan ApplyMsg

	pending map[int][]chan struct{}

	grpc_srv     *grpc.Server
	peer_clients map[int]pb.InternalRaftClient
	peer_conns   map[int]*grpc.ClientConn

	persist_path string

	leader_hint int

	stop_ch chan struct{}
	dead    int32
}

// NewRaft 创建并启动 Raft 节点：初始化状态、读取持久化快照、启动 gRPC 服务，
// 并启动后台 apply 协程和定时器协程。
func NewRaft(id int, bind, persist_path string, apply_ch chan ApplyMsg) (*Raft, error) {
	rf := &Raft{
		id:                   id,
		bind_addr:            bind,
		peers:                map[int]string{id: bind},
		current_term:         0,
		voted_for:            -1,
		log:                  []LogEntry{{Term: 0, Command: nil}},
		apply_ch:             apply_ch,
		pending:              make(map[int][]chan struct{}),
		persist_path:         persist_path,
		stop_ch:              make(chan struct{}),
		leader_last_idx:      make(map[int]int),
		follower_matched_idx: make(map[int]int),
		peer_clients:         make(map[int]pb.InternalRaftClient),
		peer_conns:           make(map[int]*grpc.ClientConn),
		leader_hint:          -1,
	}
	rf.apply_cond = sync.NewCond(&rf.mu)
	if persist_path != "" {
		rf.read_persist(persist_path)
	}
	if _, ok := rf.peers[rf.id]; !ok {
		rf.peers[rf.id] = bind
	}
	rf.reset_election_timer()
	rf.heartbeat_due = time.Now()
	if err := rf.start_raft_grpc(bind); err != nil {
		return nil, err
	}
	go rf.apply_committed_entries_loop()
	go rf.ticker_loop()
	return rf, nil
}

// Stop 标记节点已停止，关闭 gRPC 服务，并唤醒所有等待中的协程。
func (rf *Raft) Stop() {
	if !atomic.CompareAndSwapInt32(&rf.dead, 0, 1) {
		return
	}
	close(rf.stop_ch)
	rf.stop_grpc()
	rf.mu.Lock()
	rf.apply_cond.Broadcast()
	rf.mu.Unlock()
}

// ticker_loop 定时器主循环，每 30ms 调用一次 tick()，驱动选举或心跳发送。
func (rf *Raft) ticker_loop() {
	t := time.NewTicker(30 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-rf.stop_ch:
			return
		case <-t.C:
			if atomic.LoadInt32(&rf.dead) != 0 {
				return
			}
			rf.tick()
		}
	}
}

// tick 单次时钟滴答：Leader 发送心跳，Follower/Candidate 检测选举超时。
func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch rf.state {
	case Leader:
		if time.Now().After(rf.heartbeat_due) {
			rf.heartbeat_due = time.Now().Add(90 * time.Millisecond)
			go rf.broadcast_append_to_followers()
		}
	default:
		if time.Now().After(rf.election_deadline) {
			rf.start_election()
		}
	}
}

// reset_election_timer 重置选举超时计时器（300-550ms 随机），避免选举分裂。
func (rf *Raft) reset_election_timer() {
	rf.election_deadline = time.Now().Add(time.Duration(300+rand.Intn(250)) * time.Millisecond)
}

// last_log_idx 返回最后一条日志的下标（含哨兵索引 0）。
func (rf *Raft) last_log_idx() int {
	return len(rf.log) - 1
}

// last_log_term 返回最后一条日志的任期。
func (rf *Raft) last_log_term() int {
	return rf.log[rf.last_log_idx()].Term
}

// become_follower 将当前节点转为 Follower，若 term 更高则更新任期并清除投票记录。
func (rf *Raft) become_follower(term int) {
	if term > rf.current_term {
		rf.current_term = term
		rf.voted_for = -1
		rf.persist_locked()
	}
	rf.state = Follower
	rf.reset_election_timer()
}

// start_election 启动选举：自增任期、投票给自己、并发向所有 Peer 请求投票。
func (rf *Raft) start_election() {
	rf.state = Candidate
	rf.current_term++
	rf.voted_for = rf.id
	rf.leader_hint = -1
	rf.persist_locked()
	rf.reset_election_timer()

	term := rf.current_term
	args := RequestVoteArgs{
		term:          term,
		candidate_id:  rf.id,
		last_log_idx:  rf.last_log_idx(),
		last_log_term: rf.last_log_term(),
	}
	votes := 1
	peerIDs := rf.peer_ids_locked()
	majority := len(peerIDs)/2 + 1
	if votes >= majority {
		rf.become_leader()
		return
	}

	for _, i := range peerIDs {
		if i == rf.id {
			continue
		}
		go func(peer int) {
			var reply RequestVoteReply
			ok := rf.send_request_vote(peer, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != Candidate || rf.current_term != term {
				return
			}
			if !ok {
				return
			}
			if reply.term > rf.current_term {
				rf.become_follower(reply.term)
				rf.persist_locked()
				return
			}
			if reply.vote_granted {
				if rf.state != Candidate || rf.current_term != term {
					return
				}
				votes++
				if votes >= majority && rf.state == Candidate && rf.current_term == term {
					rf.become_leader()
				}
			}
		}(i)
	}
}

// become_leader 转为 Leader 角色，初始化每个 Peer 的 nextIndex 和 matchIndex，并广播心跳。
func (rf *Raft) become_leader() {
	rf.state = Leader
	rf.leader_hint = rf.id
	rf.heartbeat_due = time.Now()
	n := len(rf.log)
	for _, i := range rf.peer_ids_locked() {
		rf.leader_last_idx[i] = n
		rf.follower_matched_idx[i] = 0
	}
	rf.follower_matched_idx[rf.id] = rf.last_log_idx()
	go rf.broadcast_append_to_followers()
}

// broadcast_append_to_followers 作为 Leader 向所有 Follower 并发发送 AppendEntries 复制数据。
func (rf *Raft) broadcast_append_to_followers() {
	peerIDs, ok := rf.snapshot_follower_ids_if_leader()
	if !ok {
		return
	}
	for _, i := range peerIDs {
		if i == rf.id {
			continue
		}
		go rf.run_append_entries_for_follower(i)
	}
}

// run_append_entries_for_follower 向单个 Follower 循环发送 AppendEntries，直到本轮成功或不再是 Leader。
func (rf *Raft) run_append_entries_for_follower(peer int) {
	for atomic.LoadInt32(&rf.dead) == 0 {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		term := rf.current_term
		send_from := rf.leader_last_idx[peer]
		// 链式检查：新条目接在「prefixIdx」之后，Follower 在 prefixIdx 处的任期须为 prefixTerm。
		prefixIdx := send_from - 1
		if prefixIdx < 0 {
			prefixIdx = 0
		}
		prefixTerm := rf.log[prefixIdx].Term
		entries := append([]LogEntry(nil), rf.log[send_from:]...)
		args := AppendEntriesArgs{
			term:               term,
			leader_id:          rf.id,
			follower_last_idx:  prefixIdx,
			follower_last_term: prefixTerm,
			entries:            entries,
			leader_commit:      rf.commit_index,
		}
		rf.mu.Unlock()

		var reply AppendEntriesReply
		ok := rf.send_append_entries(peer, &args, &reply)
		if !ok {
			return
		}

		rf.mu.Lock()
		if reply.term > rf.current_term {
			rf.become_follower(reply.term)
			rf.persist_locked()
			rf.mu.Unlock()
			return
		}
		if rf.state != Leader || rf.current_term != term {
			rf.mu.Unlock()
			return
		}
		if reply.success {
			if len(entries) > 0 {
				rf.follower_matched_idx[peer] = prefixIdx + len(entries)
			} else {
				rf.follower_matched_idx[peer] = prefixIdx
			}
			rf.leader_last_idx[peer] = rf.follower_matched_idx[peer] + 1
			rf.try_commit()
			rf.apply_cond.Broadcast()
			rf.mu.Unlock()
			return
		}
		rf.adjust_send_idx_after_append_rejected_locked(peer, &reply)
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// adjust_send_idx_after_append_rejected_locked：Follower 拒绝复制时，根据冲突提示回退 leader_last_idx（须已持锁）。
func (rf *Raft) adjust_send_idx_after_append_rejected_locked(peer int, reply *AppendEntriesReply) {
	if reply.conflict_term == 0 {
		if reply.conflict_index < 1 {
			rf.leader_last_idx[peer] = 1
		} else {
			rf.leader_last_idx[peer] = reply.conflict_index
		}
		return
	}
	last := 0
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == reply.conflict_term {
			last = i
			break
		}
	}
	if last > 0 {
		rf.leader_last_idx[peer] = last + 1
	} else {
		rf.leader_last_idx[peer] = reply.conflict_index
	}
	if rf.leader_last_idx[peer] < 1 {
		rf.leader_last_idx[peer] = 1
	}
}

// try_commit 检查是否有新日志条目在多数节点复制成功，若满足则推进 commit_index。
func (rf *Raft) try_commit() {
	last := len(rf.log) - 1
	rf.follower_matched_idx[rf.id] = last
	majority := len(rf.peers)/2 + 1
	for n := last; n > rf.commit_index; n-- {
		if rf.log[n].Term != rf.current_term {
			continue
		}
		cnt := 0
		for j := range rf.peers {
			if rf.follower_matched_idx[j] >= n {
				cnt++
			}
		}
		if cnt >= majority {
			rf.commit_index = n
			break
		}
	}
}

// handle_request_vote 处理投票请求：检查候选人的日志是否最新，决定是否授予投票。
func (rf *Raft) handle_request_vote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.term = rf.current_term
	reply.vote_granted = false

	if args.term < rf.current_term {
		return
	}
	if args.term > rf.current_term {
		rf.current_term = args.term
		rf.voted_for = -1
		rf.persist_locked()
	}
	reply.term = rf.current_term
	rf.state = Follower
	rf.leader_hint = -1
	rf.reset_election_timer()

	lastIdx := rf.last_log_idx()
	lastTerm := rf.log[lastIdx].Term
	upToDate := args.last_log_term > lastTerm ||
		(args.last_log_term == lastTerm && args.last_log_idx >= lastIdx)

	if (rf.voted_for == -1 || rf.voted_for == args.candidate_id) && upToDate {
		rf.voted_for = args.candidate_id
		reply.vote_granted = true
		rf.persist_locked()
	}
}

// handle_append_entries 处理日志复制/心跳请求：进行一致性检查，追加或截断日志，更新 commit_index。
func (rf *Raft) handle_append_entries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.term = rf.current_term
	reply.success = false

	if args.term < rf.current_term {
		return
	}
	if args.term > rf.current_term {
		rf.current_term = args.term
		rf.voted_for = -1
		rf.persist_locked()
	}
	rf.state = Follower
	rf.leader_hint = args.leader_id
	rf.reset_election_timer()
	reply.term = rf.current_term

	if args.follower_last_idx >= len(rf.log) {
		reply.conflict_term = 0
		reply.conflict_index = len(rf.log)
		return
	}
	if rf.log[args.follower_last_idx].Term != args.follower_last_term {
		t := rf.log[args.follower_last_idx].Term
		if t == 0 && args.follower_last_idx == 0 {
			reply.conflict_term = 0
			reply.conflict_index = 1
			return
		}
		idx := args.follower_last_idx
		for idx > 0 && rf.log[idx-1].Term == t {
			idx--
		}
		reply.conflict_term = t
		reply.conflict_index = idx
		return
	}

	base := args.follower_last_idx + 1
	for i, e := range args.entries {
		idx := base + i
		if idx < len(rf.log) {
			if rf.log[idx].Term != e.Term {
				rf.log = append(rf.log[:idx], args.entries[i:]...)
				break
			}
		} else {
			rf.log = append(rf.log, args.entries[i:]...)
			break
		}
	}
	rf.persist_locked()

	if args.leader_commit > rf.commit_index {
		lastNew := len(rf.log) - 1
		if args.leader_commit < lastNew {
			rf.commit_index = args.leader_commit
		} else {
			rf.commit_index = lastNew
		}
		rf.apply_cond.Broadcast()
	}
	reply.success = true
}

// apply_committed_entries_loop：顺序将已提交日志应用到状态机，并在状态机确认应用后唤醒 submit_and_wait_applied。
func (rf *Raft) apply_committed_entries_loop() {
	for {
		rf.mu.Lock()
		if atomic.LoadInt32(&rf.dead) != 0 {
			rf.mu.Unlock()
			return
		}
		for rf.last_applied >= rf.commit_index {
			rf.apply_cond.Wait()
			if atomic.LoadInt32(&rf.dead) != 0 {
				rf.mu.Unlock()
				return
			}
		}
		for rf.last_applied < rf.commit_index {
			rf.last_applied++
			idx := rf.last_applied
			op, _ := DecodeOp(rf.log[idx].Command)
			if op.Type == "join" && op.NodeID > 0 && op.NodeAddr != "" {
				rf.apply_join_locked(op.NodeID, op.NodeAddr)
				rf.signal_pending_locked(idx)
				rf.persist_locked()
				continue
			}
			done := make(chan struct{})
			rf.mu.Unlock()
			select {
			case rf.apply_ch <- ApplyMsg{Index: idx, Op: op, Done: done}:
			case <-rf.stop_ch:
				return
			}
			select {
			case <-done:
			case <-rf.stop_ch:
				return
			}
			rf.mu.Lock()
			rf.signal_pending_locked(idx)
		}
		rf.mu.Unlock()
	}
}

// signal_pending_locked 通知所有等待指定日志索引已应用的协程（须持锁）。
func (rf *Raft) signal_pending_locked(idx int) {
	for _, ch := range rf.pending[idx] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	delete(rf.pending, idx)
}

// submit_and_wait_applied：Leader 追加日志后阻塞，直到该索引已提交并应用到状态机（或超时）。
func (rf *Raft) submit_and_wait_applied(b []byte) (int, error) {
	ch := make(chan struct{}, 1)

	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return 0, ErrNotLeader
	}
	rf.log = append(rf.log, LogEntry{Term: rf.current_term, Command: b})
	idx := rf.last_log_idx()
	rf.pending[idx] = append(rf.pending[idx], ch)
	rf.follower_matched_idx[rf.id] = idx
	rf.persist_locked()
	rf.try_commit()
	rf.apply_cond.Broadcast()
	rf.mu.Unlock()

	go rf.broadcast_append_to_followers()

	select {
	case <-ch:
		return idx, nil
	case <-time.After(8 * time.Second):
		return 0, ErrTimeout
	}
}

// Submit 将操作提交到 Raft 日志并等待法定人数提交及状态机应用。
// 返回日志索引和错误；若当前节点不是 Leader 则返回 ErrNotLeader。
func (rf *Raft) Submit(op Op) (int, error) {
	b, err := EncodeOp(op)
	if err != nil {
		return 0, err
	}
	return rf.submit_and_wait_applied(b)
}

// JoinNode 将新节点加入集群配置（通过日志复制保证集群一致性）。
// nodeID 为新节点 ID，addr 为其 Raft RPC 地址。
func (rf *Raft) JoinNode(nodeID int, addr string) error {
	if nodeID <= 0 || addr == "" {
		return ErrInvalidJoin
	}
	_, err := rf.Submit(Op{
		Type:     "join",
		NodeID:   nodeID,
		NodeAddr: addr,
	})
	return err
}

// IsLeader 返回当前节点是否为 Leader。
func (rf *Raft) IsLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Leader
}

// CurrentTerm 返回当前节点的 term 值。
func (rf *Raft) CurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.current_term
}

// LeaderHint 返回当前已知或上一次通信的 Leader 节点 ID，-1 表示未知。
func (rf *Raft) LeaderHint() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leader_hint
}

// PeerAddr 返回指定节点的 Raft RPC 地址和是否存在。
func (rf *Raft) PeerAddr(nodeID int) (string, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	addr, ok := rf.peers[nodeID]
	return addr, ok
}

// apply_join_locked 将 JoinNode 操作应用于 in-memory 状态：更新 peers 映射并建立 gRPC 连接（须持锁）。
func (rf *Raft) apply_join_locked(nodeID int, addr string) {
	if nodeID == rf.id {
		rf.peers[nodeID] = rf.bind_addr
		return
	}
	if old, ok := rf.peers[nodeID]; ok && old == addr {
		return
	}
	rf.peers[nodeID] = addr
	if _, ok := rf.leader_last_idx[nodeID]; !ok {
		rf.leader_last_idx[nodeID] = len(rf.log)
	}
	if _, ok := rf.follower_matched_idx[nodeID]; !ok {
		rf.follower_matched_idx[nodeID] = 0
	}
	go rf.ensure_peer_client(nodeID, addr)
}

// peer_ids_locked 返回所有 Peer ID 的有序列表（须持锁）。
func (rf *Raft) peer_ids_locked() []int {
	ids := make([]int, 0, len(rf.peers))
	for id := range rf.peers {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

// snapshot_follower_ids_if_leader 返回所有 Follower ID 的有序列表；若非 Leader 则返回 nil, false。
func (rf *Raft) snapshot_follower_ids_if_leader() ([]int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return nil, false
	}
	ids := make([]int, 0, len(rf.peers))
	for id := range rf.peers {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids, true
}
