package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"webdownld_go/internal/storage/bloom"
	pb "webdownld_go/internal/storage/pb"
)

// conn 包装一个 gRPC 连接及其存储节点客户端。
type conn struct {
	cc     *grpc.ClientConn       // cc gRPC 底层连接。
	client pb.StorageNodeClient   // client 存储节点 gRPC 客户端。
}

// Service 是分片存储服务，负责将分片路由到对应的存储节点。
type Service struct {
	nodes   []string          // nodes 存储节点 gRPC 地址列表。
	clients map[string]conn   // clients 按地址缓存的 gRPC 连接。
	mu      sync.RWMutex      // mu 保护 clients 并发访问。
	bf      *bloom.Filter     // bf 内存 Bloom Filter，加速去重预判。
}

// NewService 创建存储服务并预拨所有节点。
// nodes 为存储节点 gRPC 地址列表。
func NewService(nodes []string) (*Service, error) {
	s := new(Service)
	s.nodes = nodes
	s.clients = make(map[string]conn)
	s.bf = bloom.New()
	for _, addr := range nodes {
		if _, err := s.getOrDial(addr); err != nil {
			return nil, fmt.Errorf("dial %s: %w", addr, err)
		}
	}
	return s, nil
}

// getOrDial 获取指定地址的 gRPC 连接，若不存在则拨号建立（锁外拨号避免阻塞其他查询）。
// addr 为存储节点 gRPC 地址。
func (s *Service) getOrDial(addr string) (conn, error) {
	s.mu.RLock()
	c, ok := s.clients[addr]
	s.mu.RUnlock()
	if ok {
		return c, nil
	}
	// 锁外拨号，避免持写锁时阻塞其他连接查询。
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return conn{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok = s.clients[addr]; ok {
		cc.Close() // 另一个 goroutine 已先拨号成功，关闭多余连接。
		return c, nil
	}
	c = conn{cc: cc, client: pb.NewStorageNodeClient(cc)}
	s.clients[addr] = c
	return c, nil
}

// getClient 根据 chunkHash 路由到对应存储节点并返回其客户端。
// chunkHash 为分片内容哈希，用于一致性路由。
func (s *Service) getClient(chunkHash string) (pb.StorageNodeClient, string, error) {
	if len(s.nodes) == 0 {
		return nil, "", fmt.Errorf("no nodes")
	}
	addr := s.pickNode(chunkHash)
	c, err := s.getOrDial(addr)
	if err != nil {
		return nil, "", err
	}
	return c.client, addr, nil
}

// pickNode 根据 chunkHash 选择目标存储节点。
func (s *Service) pickNode(chunkHash string) string { return hashRoute(chunkHash, s.nodes) }

// hashRoute 使用 FNV-1a 哈希将键路由到节点列表中的某个节点。
func hashRoute(k string, nodes []string) string {
	if len(nodes) == 0 {
		return ""
	}
	h := fnv1a(k)
	return nodes[int(h%uint64(len(nodes)))]
}

// fnv1a 计算 64 位 FNV-1a 哈希。
func fnv1a(s string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// SaveChunk 保存单个分片到对应存储节点，返回分片哈希、大小、目标节点和是否去重命中。
// ctx 为请求上下文，uploadID 为上传会话 ID，index 为分片序号，data 为分片二进制数据（调用方负责 buffer 生命周期，本函数不持有引用）。
func (s *Service) SaveChunk(ctx context.Context, uploadID string, index int, data []byte) (chunkHash string, size int64, storageID string, reused bool, err error) {
	sum := sha256.Sum256(data)
	chunkHash = hex.EncodeToString(sum[:])
	size = int64(len(data))

	// Bloom Filter 预判 + 远程确认去重。
	if s.bf.Contains([]byte(chunkHash)) {
		client, addr, cerr := s.getClient(chunkHash)
		if cerr == nil {
			ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			resp, rerr := client.ChunkExists(ctx2, &pb.ChunkRequest{ChunkHash: chunkHash})
			if rerr == nil && resp.Exists {
				return chunkHash, size, addr, true, nil
			}
		}
	}

	client, _, err := s.getClient(chunkHash)
	if err != nil {
		return
	}
	ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	stream, err := client.SaveChunk(ctx2)
	if err != nil {
		return
	}
	block := 64 * 1024
	for i := 0; i < len(data); i += block {
		end := i + block
		if end > len(data) {
			end = len(data)
		}
		if err = stream.Send(&pb.ChunkData{UploadId: uploadID, Index: int32(index), Data: data[i:end]}); err != nil {
			return
		}
	}
	result, err := stream.CloseAndRecv()
	if err != nil {
		return
	}
	if !result.Reused {
		s.bf.Add([]byte(result.ChunkHash))
	}
	return result.ChunkHash, result.Size, result.StorageId, result.Reused, nil
}

// ReadChunk 从指定存储节点流式读取分片内容。
// ctx 为请求上下文，storageID 为目标节点标识，chunkHash 为分片内容哈希，chunkSize 为分片预期大小（用于 Content-Length）。
func (s *Service) ReadChunk(ctx context.Context, storageID, chunkHash string, chunkSize int64) (io.ReadCloser, int64, error) {
	client, _, err := s.getClient(chunkHash)
	if err != nil {
		return nil, 0, err
	}
	ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
	stream, err := client.ReadChunk(ctx2, &pb.ChunkRequest{StorageId: storageID, ChunkHash: chunkHash})
	if err != nil {
		cancel()
		return nil, 0, err
	}
	return &grpcStreamReader{stream: stream, cancel: cancel}, chunkSize, nil
}

// grpcStreamReader 将 gRPC 服务端流式响应适配为 io.ReadCloser，支持流式下载。
type grpcStreamReader struct {
	stream pb.StorageNode_ReadChunkClient // stream gRPC 读取流。
	cancel context.CancelFunc             // cancel 取消关联的上下文。
	buf    []byte                         // buf 当前消息缓冲区。
	offset int                            // offset 当前缓冲区读取偏移。
	eof    bool                           // eof 是否已到达流末尾。
	closed bool                           // closed 标记是否已关闭。
}

func (r *grpcStreamReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if r.eof && r.offset >= len(r.buf) {
		return 0, io.EOF
	}
	if r.offset >= len(r.buf) {
		msg, err := r.stream.Recv()
		if err == io.EOF {
			r.eof = true
			return 0, io.EOF
		}
		if err != nil {
			return 0, err
		}
		r.buf = msg.Data
		r.offset = 0
	}
	n := copy(p, r.buf[r.offset:])
	r.offset += n
	return n, nil
}

// Close 关闭 gRPC 流并取消上下文，同时标记已关闭防止后续读取。
func (r *grpcStreamReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.cancel()
	return nil
}

// OpenChunk 打开分片用于流式下载，返回 io.ReadCloser 和分片大小。
// ctx 为请求上下文，storageID 为目标节点，chunkHash 为分片哈希，chunkSize 为已知分片大小。
func (s *Service) OpenChunk(ctx context.Context, storageID, chunkHash string, chunkSize int64) (io.ReadCloser, int64, error) {
	return s.ReadChunk(ctx, storageID, chunkHash, chunkSize)
}

// DeleteChunk 从指定存储节点删除分片。
// ctx 为请求上下文，storageID 为目标节点，chunkHash 为待删除分片哈希。
func (s *Service) DeleteChunk(ctx context.Context, storageID, chunkHash string) error {
	client, _, err := s.getClient(chunkHash)
	if err != nil {
		return err
	}
	ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = client.DeleteChunk(ctx2, &pb.ChunkRequest{StorageId: storageID, ChunkHash: chunkHash})
	return err
}

// Close 关闭所有存储节点 gRPC 连接。
func (s *Service) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.clients {
		c.cc.Close()
	}
	s.clients = make(map[string]conn)
}

