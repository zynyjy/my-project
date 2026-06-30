package agent

import (
	"encoding/json"
	"sync"
	"time"
)

type Event struct {
	Agent     string      `json:"agent"`            // Agent 产生事件的智能体名称。
	Status    string      `json:"status"`           // Status 智能体当前状态，如 running/completed/error。
	Detail    interface{} `json:"detail,omitempty"` // Detail 状态附加信息，可为文本或结构化数据。
	Timestamp time.Time   `json:"timestamp"`        // Timestamp 事件产生时间。
}

type Hub struct {
	mu          sync.RWMutex                  // mu 保护订阅者集合的并发读写。
	subscribers map[chan Event]*hubSubscriber // subscribers 保存订阅输出通道到订阅者对象的映射。
}

type hubSubscriber struct {
	in   chan Event    // in 发布侧写入队列。
	out  chan Event    // out 消费侧读取队列。
	done chan struct{} // done 订阅者退出信号。
}

// NewHub 创建一个事件总线实例。
func NewHub() *Hub {
	h := new(Hub)
	h.subscribers = make(map[chan Event]*hubSubscriber)
	return h
}

// Subscribe 注册一个订阅通道，用于接收后续广播事件。
func (h *Hub) Subscribe() chan Event {
	sub := new(hubSubscriber)
	sub.in = make(chan Event, 64)
	sub.out = make(chan Event, 64)
	sub.done = make(chan struct{})
	go sub.forward()
	h.mu.Lock()
	h.subscribers[sub.out] = sub
	h.mu.Unlock()
	return sub.out
}

// Unsubscribe 注销订阅通道并关闭它，避免资源泄漏。
func (h *Hub) Unsubscribe(ch chan Event) {
	h.mu.Lock()
	sub, ok := h.subscribers[ch]
	if ok {
		delete(h.subscribers, ch)
		close(sub.done)
	}
	h.mu.Unlock()
}

// Publish 向所有订阅者广播事件，慢消费者会被非阻塞跳过当前消息。
func (h *Hub) Publish(ev Event) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, sub := range h.subscribers {
		select {
		case sub.in <- ev:
		default:
		}
	}
}

// forward 将订阅者内部输入队列转发到对外输出队列。
// 若输出拥塞则丢弃当前事件，避免慢消费者反压发布链路。
func (s *hubSubscriber) forward() {
	defer close(s.out)
	for {
		select {
		case <-s.done:
			return
		case ev := <-s.in:
			select {
			case s.out <- ev:
			default:
			}
		}
	}
}

// JSON 将事件序列化为 JSON 字符串，便于日志或调试输出。
func (e Event) JSON() string {
	b, _ := json.Marshal(e)
	return string(b)
}
