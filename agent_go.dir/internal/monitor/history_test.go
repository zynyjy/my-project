package monitor

import (
	"testing"
	"time"
)

// TestTimeseriesRingEmpty 测试空环形缓冲区的行为。
func TestTimeseriesRingEmpty(t *testing.T) {
	r := NewTimeseriesRing(10)
	if r.Len() != 0 {
		t.Errorf("expected len 0, got %d", r.Len())
	}
	if r.Latest() != nil {
		t.Error("expected nil Latest on empty ring")
	}
	if snap := r.Snapshot(); snap != nil {
		t.Errorf("expected nil snapshot, got %d entries", len(snap))
	}
}

// TestTimeseriesRingPush 测试追加和读取。
func TestTimeseriesRingPush(t *testing.T) {
	r := NewTimeseriesRing(5)
	for i := 0; i < 3; i++ {
		r.Push(Snapshot{UpdatedAt: time.Now()})
	}
	if r.Len() != 3 {
		t.Errorf("expected len 3, got %d", r.Len())
	}
	snap := r.Snapshot()
	if len(snap) != 3 {
		t.Errorf("expected 3 entries, got %d", len(snap))
	}
}

// TestTimeseriesRingOverflow 测试缓冲区溢出覆盖行为。
func TestTimeseriesRingOverflow(t *testing.T) {
	r := NewTimeseriesRing(3)
	// 写入 5 条，容量仅 3，最旧的 2 条应被覆盖。
	for i := 0; i < 5; i++ {
		r.Push(Snapshot{ProcessCount: i + 1})
	}
	if r.Len() != 3 {
		t.Errorf("expected len 3, got %d", r.Len())
	}
	snap := r.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(snap))
	}
	// 应保留最新的 3 条（ProcessCount: 3, 4, 5）。
	if snap[0].ProcessCount != 3 {
		t.Errorf("expected first entry ProcessCount=3, got %d", snap[0].ProcessCount)
	}
	if snap[2].ProcessCount != 5 {
		t.Errorf("expected last entry ProcessCount=5, got %d", snap[2].ProcessCount)
	}
}

// TestTimeseriesRingLatest 测试 Latest 方法。
func TestTimeseriesRingLatest(t *testing.T) {
	r := NewTimeseriesRing(5)
	r.Push(Snapshot{ProcessCount: 1})
	r.Push(Snapshot{ProcessCount: 2})
	latest := r.Latest()
	if latest == nil {
		t.Fatal("expected non-nil Latest")
	}
	if latest.ProcessCount != 2 {
		t.Errorf("expected ProcessCount=2, got %d", latest.ProcessCount)
	}
}
