package raft

import (
	"encoding/json"
	"os"
)

// persisted 持久化快照结构体，包含当前任期、投票信息、日志及集群拓扑。
type persisted struct {
	CurrentTerm int              // CurrentTerm 持久化的当前任期。
	VotedFor    int              // VotedFor 持久化的投票目标，-1 表示未投票。
	Log         []LogEntry       // Log 持久化的日志条目。
	Peers       map[int]string   // Peers 持久化的集群节点映射。
}

// persist_locked 将 Raft 关键状态（任期、投票、日志、Peers）写入磁盘 JSON 文件。
// 须在持有锁的情况下调用。
func (rf *Raft) persist_locked() {
	if rf.persist_path == "" {
		return
	}
	p := persisted{
		CurrentTerm: rf.current_term,
		VotedFor:    rf.voted_for,
		Log:         rf.log,
		Peers:       rf.peers,
	}
	b, err := json.Marshal(p)
	if err != nil {
		return
	}
	_ = os.WriteFile(rf.persist_path, b, 0o600)
}

// read_persist 从磁盘 JSON 文件恢复 Raft 状态（任期、投票、日志、Peers）。
// 若文件不存在、为空或格式无效则静默跳过，以初始状态启动。
func (rf *Raft) read_persist(path string) {
	b, err := os.ReadFile(path)
	if err != nil || len(b) == 0 {
		return
	}
	var p persisted
	if json.Unmarshal(b, &p) != nil {
		return
	}
	if len(p.Log) < 1 || p.Log[0].Term != 0 {
		return
	}
	rf.current_term = p.CurrentTerm
	rf.voted_for = p.VotedFor
	rf.log = p.Log
	if len(p.Peers) > 0 {
		rf.peers = p.Peers
	}
}
