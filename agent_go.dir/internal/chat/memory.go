package chat

import (
	"strings"
	"sync"
	"time"
)

// Message 表示对话历史中的一条消息。
type Message struct {
	Role    string    // Role 消息角色（user 或 assistant）。
	Content string    // Content 消息文本内容。
	Time    time.Time // Time 消息时间戳。
}

// ConversationBuffer 按会话 ID 管理固定大小的对话历史窗口。
type ConversationBuffer struct {
	mu       sync.Mutex              // mu 保护 sessions 的并发访问。
	sessions map[string]*ringBuffer  // sessions 按会话 ID 存储对话环形缓冲区。
	capacity int                     // capacity 每个会话最多保存的消息条数（窗口大小）。
}

// ringBuffer 固定容量的消息环形缓冲区。
type ringBuffer struct {
	buf  []Message // buf 消息存储空间。
	head int       // head 下一次写入的位置索引。
	size int       // size 当前已存储的消息数。
}

// NewConversationBuffer 创建指定容量的对话缓冲区。
// capacity 为每个会话最多保存的消息数。
func NewConversationBuffer(capacity int) *ConversationBuffer {
	b := new(ConversationBuffer)
	b.capacity = capacity
	b.sessions = make(map[string]*ringBuffer)
	return b
}

// Append 向指定会话追加一轮用户-助手对话。
// sessionID 为会话标识，userMsg 为用户消息，assistantMsg 为助手回复。
func (b *ConversationBuffer) Append(sessionID, userMsg, assistantMsg string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	rb := b.getOrCreateLocked(sessionID)
	now := time.Now()
	rb.push(Message{Role: "user", Content: userMsg, Time: now})
	rb.push(Message{Role: "assistant", Content: assistantMsg, Time: now})
}

// History 返回指定会话的历史对话文本，格式化为可注入 LLM 上下文的字符串。
func (b *ConversationBuffer) History(sessionID string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	rb, ok := b.sessions[sessionID]
	if !ok || rb.size == 0 {
		return ""
	}
	msgs := rb.ordered()
	var sb strings.Builder
	for _, m := range msgs {
		sb.WriteString(m.Role)
		sb.WriteString(": ")
		sb.WriteString(m.Content)
		sb.WriteString("\n")
	}
	return strings.TrimSpace(sb.String())
}

// Messages 返回指定会话的原始消息列表（按时间顺序）。
func (b *ConversationBuffer) Messages(sessionID string) []Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	rb, ok := b.sessions[sessionID]
	if !ok {
		return nil
	}
	return rb.ordered()
}

// getOrCreateLocked 在持有锁的情况下获取或创建会话环形缓冲区。
func (b *ConversationBuffer) getOrCreateLocked(sessionID string) *ringBuffer {
	if rb, ok := b.sessions[sessionID]; ok {
		return rb
	}
	rb := new(ringBuffer)
	rb.buf = make([]Message, b.capacity)
	b.sessions[sessionID] = rb
	return rb
}

// push 向环形缓冲区追加一条消息，满时覆盖最旧的条目。
func (rb *ringBuffer) push(m Message) {
	rb.buf[rb.head] = m
	rb.head = (rb.head + 1) % len(rb.buf)
	if rb.size < len(rb.buf) {
		rb.size++
	}
}

// ordered 返回按时间顺序排列的消息副本（最旧→最新）。
func (rb *ringBuffer) ordered() []Message {
	if rb.size == 0 {
		return nil
	}
	cap := len(rb.buf)
	start := (rb.head - rb.size + cap) % cap
	out := make([]Message, rb.size)
	for i := 0; i < rb.size; i++ {
		out[i] = rb.buf[(start+i)%cap]
	}
	return out
}
