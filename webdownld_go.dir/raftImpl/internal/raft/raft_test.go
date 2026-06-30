package raft

import "testing"

// TestTryCommitSinglePeer 测试单节点集群多数提交：Leader 自己是唯一节点，应能直接提交日志。
func TestTryCommitSinglePeer(t *testing.T) {
	rf := &Raft{
		peers:        map[int]string{1: "x"},
		id:           1,
		current_term: 2,
		log: []LogEntry{
			{Term: 0, Command: nil},
			{Term: 2, Command: []byte(`{"type":"put","key":"k","value":"v"}`)},
		},
		commit_index:         0,
		state:                Leader,
		follower_matched_idx: map[int]int{1: 0},
		leader_last_idx:      map[int]int{1: 2},
	}
	rf.try_commit()
	if rf.commit_index != 1 {
		t.Fatalf("commit_index=%d want 1", rf.commit_index)
	}
}

// TestTryCommitWaitsForMajority 测试三节点集群必须收到多数确认后才提交。
func TestTryCommitWaitsForMajority(t *testing.T) {
	rf := &Raft{
		peers:        map[int]string{1: "a", 2: "b", 3: "c"},
		id:           1,
		current_term: 1,
		log: []LogEntry{
			{Term: 0, Command: nil},
			{Term: 1, Command: []byte(`{}`)},
		},
		commit_index:         0,
		state:                Leader,
		follower_matched_idx: map[int]int{1: 0, 2: 0, 3: 0},
		leader_last_idx:      map[int]int{1: 2, 2: 2, 3: 2},
	}
	rf.try_commit()
	if rf.commit_index != 0 {
		t.Fatalf("should not commit without follower ack, got %d", rf.commit_index)
	}
	rf.follower_matched_idx[2] = 1
	rf.follower_matched_idx[3] = 1
	rf.try_commit()
	if rf.commit_index != 1 {
		t.Fatalf("commit_index=%d want 1", rf.commit_index)
	}
}

// TestApplyAppendEntriesFailure_logShorter 测试 Follower 日志更短时冲突提示为日志长度。
func TestApplyAppendEntriesFailure_logShorter(t *testing.T) {
	rf := &Raft{
		log:             []LogEntry{{Term: 0}, {Term: 1}, {Term: 1}},
		leader_last_idx: map[int]int{1: 99},
	}
	rf.adjust_send_idx_after_append_rejected_locked(1, &AppendEntriesReply{conflict_term: 0, conflict_index: len(rf.log)})
	if rf.leader_last_idx[1] != 3 {
		t.Fatalf("leader_last_idx=%d want 3", rf.leader_last_idx[1])
	}
}

// TestApplyAppendEntriesFailure_leaderHasConflictTerm 测试 Leader 本地有相同任期时回退到该任期末尾+1。
func TestApplyAppendEntriesFailure_leaderHasConflictTerm(t *testing.T) {
	rf := &Raft{
		log: []LogEntry{
			{Term: 0}, {Term: 2}, {Term: 2}, {Term: 3},
		},
		leader_last_idx: map[int]int{1: 10},
	}
	rf.adjust_send_idx_after_append_rejected_locked(1, &AppendEntriesReply{conflict_term: 2, conflict_index: 1})
	if rf.leader_last_idx[1] != 3 {
		t.Fatalf("leader_last_idx=%d want 3", rf.leader_last_idx[1])
	}
}

// TestApplyAppendEntriesFailure_leaderLacksConflictTerm 测试 Leader 本地无冲突任期时直接使用 Follower 的 conflict_index。
func TestApplyAppendEntriesFailure_leaderLacksConflictTerm(t *testing.T) {
	rf := &Raft{
		log:             []LogEntry{{Term: 0}, {Term: 5}},
		leader_last_idx: map[int]int{1: 10},
	}
	rf.adjust_send_idx_after_append_rejected_locked(1, &AppendEntriesReply{conflict_term: 2, conflict_index: 7})
	if rf.leader_last_idx[1] != 7 {
		t.Fatalf("leader_last_idx=%d want 7", rf.leader_last_idx[1])
	}
}

// TestHandleAppendEntries_conflictHintLogShorter 测试 Follower 日志比 prevLogIndex 短时返回 conflict_index=len(log)。
func TestHandleAppendEntries_conflictHintLogShorter(t *testing.T) {
	rf := &Raft{
		current_term: 1,
		log:          []LogEntry{{Term: 0}, {Term: 1}},
	}
	var reply AppendEntriesReply
	rf.handle_append_entries(&AppendEntriesArgs{
		term: 1, follower_last_idx: 5, follower_last_term: 1, leader_commit: 0,
	}, &reply)
	if reply.success {
		t.Fatal("expected failure")
	}
	if reply.conflict_term != 0 || reply.conflict_index != 2 {
		t.Fatalf("hint (%d,%d) want (0,2)", reply.conflict_term, reply.conflict_index)
	}
}

// TestHandleAppendEntries_conflictHintTermMismatch 测试 Follower 在 prevLogIndex 处任期不匹配时返回正确冲突提示。
func TestHandleAppendEntries_conflictHintTermMismatch(t *testing.T) {
	rf := &Raft{
		current_term: 1,
		log: []LogEntry{
			{Term: 0}, {Term: 1}, {Term: 1}, {Term: 2},
		},
	}
	var reply AppendEntriesReply
	rf.handle_append_entries(&AppendEntriesArgs{
		term: 2, follower_last_idx: 3, follower_last_term: 1, leader_commit: 0,
	}, &reply)
	if reply.success {
		t.Fatal("expected failure")
	}
	if reply.conflict_term != 2 || reply.conflict_index != 3 {
		t.Fatalf("hint (%d,%d) want (2,3)", reply.conflict_term, reply.conflict_index)
	}
}
