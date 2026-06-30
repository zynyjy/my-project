package raft

import "errors"

var (
	// ErrNotLeader 当前节点不是 Leader，无法处理写请求。
	ErrNotLeader = errors.New("not leader")
	// ErrTimeout 操作超时，Leader 在等待法定人数确认时超时。
	ErrTimeout = errors.New("operation timeout")
	// ErrInvalidJoin 集群加入请求参数无效。
	ErrInvalidJoin = errors.New("invalid join request")
)
