package chat

import (
	"testing"
)

// TestConversationBufferEmpty 测试空缓冲区行为。
func TestConversationBufferEmpty(t *testing.T) {
	b := NewConversationBuffer(10)
	if h := b.History("test"); h != "" {
		t.Errorf("expected empty history, got %s", h)
	}
	if msgs := b.Messages("test"); msgs != nil {
		t.Errorf("expected nil messages, got %v", msgs)
	}
}

// TestConversationBufferAppendAndHistory 测试追加和读取。
func TestConversationBufferAppendAndHistory(t *testing.T) {
	b := NewConversationBuffer(10)
	b.Append("s1", "你好", "你好！有什么可以帮助你的？")
	h := b.History("s1")
	if h == "" {
		t.Error("expected non-empty history")
	}
}

// TestConversationBufferOverflow 测试窗口溢出行为。
func TestConversationBufferOverflow(t *testing.T) {
	b := NewConversationBuffer(2) // 容量 2，即最多保留 2 条消息。
	b.Append("s1", "q1", "a1")
	b.Append("s1", "q2", "a2")
	b.Append("s1", "q3", "a3")
	msgs := b.Messages("s1")
	// 容量 2，写入 6 条（3 轮×2），应保留最新 2 条消息(q3, a3)。
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	if msgs[0].Content != "q3" || msgs[1].Content != "a3" {
		t.Errorf("expected latest messages (q3, a3), got (%s, %s)", msgs[0].Content, msgs[1].Content)
	}
}

// TestConversationBufferMultipleSessions 测试多会话隔离。
func TestConversationBufferMultipleSessions(t *testing.T) {
	b := NewConversationBuffer(10)
	b.Append("s1", "hello", "hi")
	b.Append("s2", "bonjour", "salut")
	if h := b.History("s1"); h == "" {
		t.Error("s1 should have history")
	}
	if h := b.History("s2"); h == "" {
		t.Error("s2 should have history")
	}
}
