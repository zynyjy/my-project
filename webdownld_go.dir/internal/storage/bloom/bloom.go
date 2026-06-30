// Package bloom 提供一个简单的内存 Bloom Filter，用于分片去重预判。
// 目标：大幅减少 os.Stat 磁盘查询次数。
package bloom

import (
	"encoding/binary"
	"hash/fnv"
	"sync"
)

// Filter 是线程安全的内存 Bloom Filter。
// 假阳性率约 1%，位数组大小和哈希次数可根据预期元素量调整。
type Filter struct {
	bits []uint64
	m    uint64 // 位数组长度（以 uint64 计）。
	k    int    // 哈希函数次数。
	mu   sync.RWMutex
}

const (
	defaultM = 1 << 20 // 1M 个 uint64 = 64M bits，约 8MB 内存。
	defaultK = 7        // 7 次哈希，约 1% 假阳性率。
)

// New 创建一个新的 Bloom Filter。
func New() *Filter {
	f := new(Filter)
	f.bits = make([]uint64, defaultM)
	f.m = defaultM
	f.k = defaultK
	return f
}

// Add 将元素加入 Bloom Filter。
func (f *Filter) Add(data []byte) {
	h1, h2 := hash(data)
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := 0; i < f.k; i++ {
		idx := (h1 + uint64(i)*h2) % (f.m * 64)
		f.bits[idx/64] |= 1 << (idx % 64)
	}
}

// Contains 检查元素是否可能存在（可能假阳性，不会假阴性）。
func (f *Filter) Contains(data []byte) bool {
	h1, h2 := hash(data)
	f.mu.RLock()
	defer f.mu.RUnlock()
	for i := 0; i < f.k; i++ {
		idx := (h1 + uint64(i)*h2) % (f.m * 64)
		if f.bits[idx/64]&(1<<(idx%64)) == 0 {
			return false
		}
	}
	return true
}

// hash 计算数据的 FNV-128a 哈希并返回两个 uint64 分量，用于 Bloom Filter 双哈希。
func hash(data []byte) (uint64, uint64) {
	h := fnv.New128a()
	h.Write(data)
	sum := h.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8]), binary.BigEndian.Uint64(sum[8:])
}
