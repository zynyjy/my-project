package monitor

import (
	"sync"
)

// TimeseriesRing 固定容量环形缓冲区，存储 Snapshot 时间序列数据，
// 供前端折线图 API 使用，线程安全。
type TimeseriesRing struct {
	buf      []Snapshot // buf 环形缓冲区存储空间。
	head     int        // head 下一次写入的位置索引。
	size     int        // size 当前已存储的条目数。
	capacity int        // capacity 最大可存储条目数。
	mu       sync.RWMutex // mu 保护并发读写。
}

// NewTimeseriesRing 创建容量为 capacity 的环形缓冲区实例。
func NewTimeseriesRing(capacity int) *TimeseriesRing {
	r := new(TimeseriesRing)
	r.capacity = capacity
	r.buf = make([]Snapshot, capacity)
	return r
}

// Push 向环形缓冲区追加一条快照，缓冲区满时覆盖最旧的条目。
func (r *TimeseriesRing) Push(s Snapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buf[r.head] = s
	r.head = (r.head + 1) % r.capacity
	if r.size < r.capacity {
		r.size++
	}
}

// Snapshot 返回按时间顺序排列的快照副本（最旧→最新）。
func (r *TimeseriesRing) Snapshot() []Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.size == 0 {
		return nil
	}
	out := make([]Snapshot, r.size)
	start := (r.head - r.size + r.capacity) % r.capacity
	for i := 0; i < r.size; i++ {
		out[i] = r.buf[(start+i)%r.capacity]
	}
	return out
}

// Latest 返回最新的快照条目，缓冲区为空时返回 nil。
func (r *TimeseriesRing) Latest() *Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.size == 0 {
		return nil
	}
	idx := (r.head - 1 + r.capacity) % r.capacity
	cp := r.buf[idx]
	return &cp
}

// Len 返回当前已存储的条目数。
func (r *TimeseriesRing) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.size
}
