// Package raft 为手写 Raft 最小实现（选举 + 日志复制 + 提交），用于学习。
package raft

import "encoding/json"

// ServerState 节点角色。
type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

// LogEntry 日志项；索引 0 为占位哨兵（Term=0），真实数据从索引 1 开始。
type LogEntry struct {
	Term    int
	Command []byte
}

// RequestVoteArgs / Reply — 论文 Fig 2。
type RequestVoteArgs struct {
	term          int
	candidate_id  int
	last_log_idx  int
	last_log_term int
}

type RequestVoteReply struct {
	term         int
	vote_granted bool
}

// AppendEntriesArgs 携带 0 或多条日志（心跳时 entries 为空）。
// follower_last_idx / follower_last_term：追加新条目前，Follower 必须在「该下标」处与 Leader 日志任期一致（链式一致性检查）。
type AppendEntriesArgs struct {
	term               int
	leader_id          int
	follower_last_idx  int
	follower_last_term int
	entries            []LogEntry
	leader_commit      int
}

type AppendEntriesReply struct {
	term    int
	success bool
	// 仅当 success==false 时有意义：见 api/raft.proto 注释（加速回滚）。
	conflict_term  int
	conflict_index int
}

// Op 业务命令（JSON 存入日志）。
type Op struct {
	Type     string `json:"type"` // put | del | join
	Key      string `json:"key,omitempty"`
	Value    string `json:"value,omitempty"`
	NodeID   int    `json:"node_id,omitempty"`
	NodeAddr string `json:"node_addr,omitempty"`
}

// EncodeOp 将操作序列化为 JSON 字节，用于写入 Raft 日志。
func EncodeOp(o Op) ([]byte, error) {
	return json.Marshal(o)
}

// DecodeOp 将 Raft 日志中的 JSON 字节反序列化为 Op 命令。
func DecodeOp(b []byte) (Op, error) {
	var o Op
	err := json.Unmarshal(b, &o)
	return o, err
}
