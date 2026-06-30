package agent

import (
	"runtime"
	"sync"
	"time"
)

type Manager struct {
	hub            *Hub                   // hub 用于对外广播智能体事件。
	mu             sync.RWMutex           // mu 保护 state 的并发访问。
	state          map[string]interface{} // state 保存各智能体最新快照状态。
	emitQ          chan Event             // emitQ 异步事件投递队列。
	closeOnce      sync.Once              // closeOnce 保证 Stop 只执行一次。
	wg             sync.WaitGroup         // wg 等待所有 emitter worker 退出。
	monitorHistory interface{}            // monitorHistory 进程监控时间序列环形缓冲区。
}

// NewManager 创建管理器并初始化事件总线与状态存储。
func NewManager() *Manager {
	m := new(Manager)
	m.hub = NewHub()
	m.state = make(map[string]interface{})
	m.emitQ = make(chan Event, 256)
	// 启动多个 emitter worker 以提升吞吐，worker 数 = min(4, GOMAXPROCS)
	workerN := runtime.GOMAXPROCS(0)
	if workerN > 4 {
		workerN = 4
	}
	if workerN < 1 {
		workerN = 1
	}
	m.wg.Add(workerN)
	for i := 0; i < workerN; i++ {
		go m.runEmitter()
	}
	return m
}

// Stop 关闭事件队列，等待所有 emitter worker 退出。
func (m *Manager) Stop() {
	m.closeOnce.Do(func() {
		close(m.emitQ)
	})
	m.wg.Wait()
}

// Hub 返回底层事件总线，供 Web 层进行订阅。
func (m *Manager) Hub() *Hub {
	return m.hub
}

// Emit 发送一条智能体事件。
// agent 为智能体名称，status 为状态，detail 为附加详情。
func (m *Manager) Emit(agent, status string, detail interface{}) {
	ev := Event{
		Agent:     agent,
		Status:    status,
		Detail:    detail,
		Timestamp: time.Now(),
	}
	select {
	case m.emitQ <- ev:
	default:
		// 队列满时直接同步发布，避免业务主链路阻塞的同时防止 goroutine 泄漏。
		m.hub.Publish(ev)
	}
}

// SetState 更新某个状态键的最新快照值。
// key 为状态键，detail 为最新状态详情。
func (m *Manager) SetState(key string, detail interface{}) {
	m.mu.Lock()
	m.state[key] = detail
	m.mu.Unlock()
}

// Snapshot 返回当前状态快照的副本，避免外部修改内部状态。
func (m *Manager) Snapshot() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]interface{}, len(m.state))
	for k, v := range m.state {
		out[k] = v
	}
	return out
}

// SetMonitorHistory 注册进程监控时间序列环形缓冲区，供 Web 层获取历史数据。
func (m *Manager) SetMonitorHistory(h interface{}) { m.monitorHistory = h }

// MonitorHistory 返回进程监控时间序列环形缓冲区，供 API 端点使用。
func (m *Manager) MonitorHistory() interface{} { return m.monitorHistory }

// runEmitter 后台消费异步事件队列并统一发布到 Hub。
func (m *Manager) runEmitter() {
	defer m.wg.Done()
	for ev := range m.emitQ {
		m.hub.Publish(ev)
	}
}
