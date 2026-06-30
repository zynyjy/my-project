package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type RaftClient struct {
	mu    sync.Mutex
	nodes []string     // nodes Raft HTTP 节点地址列表。
	http  *http.Client // http 复用的 HTTP 客户端。
}

type KVItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

const (
	maxRetries       = 10
	baseBackoff      = 50 * time.Millisecond
	maxBackoff       = 2 * time.Second
)

// backoff 计算指数退避时间，带 ±25% 随机抖动。
func backoff(attempt int) time.Duration {
	d := float64(baseBackoff) * math.Pow(2, float64(attempt))
	if d > float64(maxBackoff) {
		d = float64(maxBackoff)
	}
	jitter := d * 0.25 * (2*rand.Float64() - 1)
	return time.Duration(d + jitter)
}

// isTemporary 判断错误是否为临时性错误（可重试）。
func isTemporary(err error, statusCode int) bool {
	if err != nil {
		return true
	}
	// 409 (not leader) 和 503 可重试，404 不可重试。
	return statusCode == http.StatusConflict || statusCode >= 500
}

// NewRaftClient 创建 Raft 元数据客户端。
// nodes 为可访问的 Raft HTTP 节点列表。
func NewRaftClient(nodes []string) *RaftClient {
	rc := new(RaftClient)
	rc.nodes = nodes
	rc.http = new(http.Client)
	rc.http.Timeout = 4 * time.Second
	return rc
}

// Put 将键值写入 Raft 集群，使用指数退避重试直到成功或耗尽。
// key 为元数据键，value 为序列化值。
func (r *RaftClient) Put(ctx context.Context, key, value string) error {
	return r.doWithRetry(ctx, func(node string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, r.kvURL(node, key), strings.NewReader(value))
		if err != nil {
			return nil, err
		}
		return r.http.Do(req)
	}, "put")
}

// Get 从 Raft 集群读取键值，使用指数退避重试。
// key 为元数据键，返回对应值字符串。
func (r *RaftClient) Get(ctx context.Context, key string) (string, error) {
	return r.getWithBackoff(ctx, key)
}

// getWithBackoff 是 Get 的带退避实现。
func (r *RaftClient) getWithBackoff(ctx context.Context, key string) (string, error) {
	lastErr := fmt.Errorf("key not found")
	nodes := r.snapshotNodes()
	if len(nodes) == 0 {
		return "", fmt.Errorf("no raft node available")
	}
	for attempt := 0; attempt < maxRetries; attempt++ {
		node := nodes[attempt%len(nodes)]
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, r.kvURL(node, key), nil)
		resp, err := r.http.Do(req)
		if err != nil {
			lastErr = err
			if !isTemporary(err, 0) {
				break
			}
		} else {
			if leader := leaderAddr(resp); resp.StatusCode == http.StatusConflict && leader != "" {
				_ = resp.Body.Close()
				r.promote(leader)
				nodes = r.snapshotNodes()
				lastErr = fmt.Errorf("raft get redirected to leader=%s", leader)
			} else if resp.StatusCode == http.StatusNotFound {
				_ = resp.Body.Close()
				return "", fmt.Errorf("key not found")
			} else if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				b, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				return string(bytes.TrimSpace(b)), nil
			} else {
				_ = resp.Body.Close()
				lastErr = fmt.Errorf("raft get failed status=%d node=%s", resp.StatusCode, node)
				if !isTemporary(nil, resp.StatusCode) {
					break
				}
			}
		}
		// 指数退避。
		select {
		case <-time.After(backoff(attempt)):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return "", lastErr
}

// Delete 从 Raft 集群删除键值，使用指数退避重试。
func (r *RaftClient) Delete(ctx context.Context, key string) error {
	return r.doWithRetry(ctx, func(node string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, r.kvURL(node, key), nil)
		if err != nil {
			return nil, err
		}
		return r.http.Do(req)
	}, "delete")
}

// ListPrefix 从 Raft leader 读取指定前缀下的所有 KV 项。
func (r *RaftClient) ListPrefix(ctx context.Context, prefix string) ([]KVItem, error) {
	lastErr := fmt.Errorf("no raft node available")
	nodes := r.snapshotNodes()
	if len(nodes) == 0 {
		return nil, lastErr
	}
	for attempt := 0; attempt < maxRetries; attempt++ {
		var out struct {
			Items []KVItem `json:"items"`
		}
		node := strings.TrimSpace(nodes[attempt%len(nodes)])
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+node+"/kv-prefix/"+url.PathEscape(prefix), nil)
		resp, err := r.http.Do(req)
		if err != nil {
			lastErr = err
			if !isTemporary(err, 0) {
				break
			}
		} else {
			if leader := leaderAddr(resp); resp.StatusCode == http.StatusConflict && leader != "" {
				_ = resp.Body.Close()
				r.promote(leader)
				nodes = r.snapshotNodes()
				lastErr = fmt.Errorf("raft prefix redirected to leader=%s", leader)
			} else if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				err := json.NewDecoder(resp.Body).Decode(&out)
				_ = resp.Body.Close()
				return out.Items, err
			} else {
				_ = resp.Body.Close()
				lastErr = fmt.Errorf("raft prefix failed status=%d node=%s", resp.StatusCode, node)
				if !isTemporary(nil, resp.StatusCode) {
					break
				}
			}
		}
		select {
		case <-time.After(backoff(attempt)):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, lastErr
}

// doWithRetry 是 Put 等操作的通用带退避重试框架。
func (r *RaftClient) doWithRetry(ctx context.Context, doReq func(node string) (*http.Response, error), opName string) error {
	lastErr := fmt.Errorf("no raft node available")
	nodes := r.snapshotNodes()
	if len(nodes) == 0 {
		return lastErr
	}
	for attempt := 0; attempt < maxRetries; attempt++ {
		node := strings.TrimSpace(nodes[attempt%len(nodes)])
		resp, err := doReq(node)
		if err != nil {
			lastErr = err
			if !isTemporary(err, 0) {
				break
			}
		} else {
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				_ = resp.Body.Close()
				return nil
			}
			if leader := leaderAddr(resp); resp.StatusCode == http.StatusConflict && leader != "" {
				_ = resp.Body.Close()
				r.promote(leader)
				nodes = r.snapshotNodes()
				lastErr = fmt.Errorf("raft %s redirected to leader=%s", opName, leader)
			} else {
				_ = resp.Body.Close()
				lastErr = fmt.Errorf("raft %s failed status=%d node=%s", opName, resp.StatusCode, node)
				if !isTemporary(nil, resp.StatusCode) {
					break
				}
			}
		}
		select {
		case <-time.After(backoff(attempt)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return lastErr
}

// kvURL 构造 Raft 节点上指定 key 的 HTTP KV 访问地址。
func (r *RaftClient) kvURL(node, key string) string {
	return "http://" + strings.TrimSpace(node) + "/kv/" + url.PathEscape(key)
}

// snapshotNodes 返回当前可用 Raft 节点的线程安全快照列表。
func (r *RaftClient) snapshotNodes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	nodes := make([]string, 0, len(r.nodes))
	for _, node := range r.nodes {
		node = strings.TrimSpace(node)
		if node != "" {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// promote 将指定 node 提升到客户端节点列表首位，加速后续 Leader 路由。
func (r *RaftClient) promote(node string) {
	node = strings.TrimSpace(node)
	if node == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	next := []string{node}
	for _, cur := range r.nodes {
		cur = strings.TrimSpace(cur)
		if cur != "" && cur != node {
			next = append(next, cur)
		}
	}
	r.nodes = next
}

// leaderAddr 从 HTTP 409 响应体中解析 Raft Leader 地址。
func leaderAddr(resp *http.Response) string {
	if resp == nil || resp.StatusCode != http.StatusConflict {
		return ""
	}
	var body struct {
		LeaderAddr string `json:"leader_addr"`
		LeaderHint struct {
			Addr string `json:"addr"`
		} `json:"leader_hint"`
	}
	b, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(b, &body)
	if body.LeaderAddr != "" {
		return body.LeaderAddr
	}
	return body.LeaderHint.Addr
}
