package meta

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"webdownld_go/internal/model"
)

// jsonBufPool 复用 JSON 序列化的 bytes.Buffer，降低 Raft 写入路径上的 GC 分配。
var jsonBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

type Service struct {
	raft *RaftClient // raft Raft 元数据客户端。
}

// NewService 创建元数据服务。
// raft 为底层一致性存储客户端。
func NewService(raft *RaftClient) *Service {
	s := new(Service)
	s.raft = raft
	return s
}

// NameHash 计算文件名哈希，用于名称级去重索引。
func NameHash(name string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(name))))
	return hex.EncodeToString(sum[:])
}

// FileIDByName 基于文件名哈希与大小生成稳定文件 ID。
func FileIDByName(name string, size int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", NameHash(name), size)))
	return hex.EncodeToString(sum[:])
}

// UploadID 基于文件 ID 和当前时间生成上传会话 ID。
func UploadID(fileID string) string {
	sum := sha256.Sum256([]byte(fileID + ":" + time.Now().Format(time.RFC3339Nano)))
	return hex.EncodeToString(sum[:16])
}

// SaveUploadSession 保存上传会话元数据到 Raft。
// ss 为完整上传会话对象。
func (s *Service) SaveUploadSession(ctx context.Context, ss model.UploadSession) error {
	ss.UpdatedAt = time.Now()
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer jsonBufPool.Put(buf)
	json.NewEncoder(buf).Encode(ss)
	return s.raft.Put(ctx, "upload:"+ss.UploadID, buf.String())
}

// GetUploadSession 按 uploadID 获取上传会话。
func (s *Service) GetUploadSession(ctx context.Context, uploadID string) (*model.UploadSession, error) {
	raw, err := s.raft.Get(ctx, "upload:"+uploadID)
	if err != nil {
		return nil, err
	}
	var ss model.UploadSession
	if err := json.Unmarshal([]byte(raw), &ss); err != nil {
		return nil, err
	}
	if ss.Received == nil {
		ss.Received = map[int]bool{}
	}
	if ss.ChunkMap == nil {
		ss.ChunkMap = map[int]string{}
	}
	items, err := s.raft.ListPrefix(ctx, uploadChunkPrefix(uploadID))
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		var ch model.UploadChunkState
		if json.Unmarshal([]byte(item.Value), &ch) == nil {
			ss.Received[ch.Index] = true
			ss.ChunkMap[ch.Index] = ch.ChunkHash + "|" + ch.StorageID + "|" + fmt.Sprintf("%d", ch.Size)
		}
	}
	return &ss, nil
}

// SaveUploadChunk 保存单个分片完成状态，避免并发上传时覆盖整个 UploadSession。
func (s *Service) SaveUploadChunk(ctx context.Context, uploadID string, ch model.UploadChunkState) error {
	ch.UpdatedAt = time.Now()
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer jsonBufPool.Put(buf)
	json.NewEncoder(buf).Encode(ch)
	return s.raft.Put(ctx, uploadChunkKey(uploadID, ch.Index), buf.String())
}

// SaveFile 保存文件元数据并更新名称索引与文件目录。
// fm 为完整文件元数据对象。
func (s *Service) SaveFile(ctx context.Context, fm model.FileMeta) error {
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer jsonBufPool.Put(buf)
	json.NewEncoder(buf).Encode(fm)
	data := buf.String()
	if err := s.raft.Put(ctx, "file:"+fm.FileID, data); err != nil {
		return err
	}
	if err := s.raft.Put(ctx, nameSizeIndexKey(fm.NameHash, fm.Size), fm.FileID); err != nil {
		return err
	}
	return s.raft.Put(ctx, "catalog:file:"+fm.FileID, fm.FileID)
}

// GetFileByID 按文件 ID 查询文件元数据。
func (s *Service) GetFileByID(ctx context.Context, fileID string) (*model.FileMeta, error) {
	raw, err := s.raft.Get(ctx, "file:"+fileID)
	if err != nil {
		return nil, err
	}
	var fm model.FileMeta
	if err := json.Unmarshal([]byte(raw), &fm); err != nil {
		return nil, err
	}
	return &fm, nil
}

// GetFileByNameHash 按名称哈希查询文件元数据。
func (s *Service) GetFileByNameHashAndSize(ctx context.Context, nameHash string, size int64) (*model.FileMeta, error) {
	fileID, err := s.raft.Get(ctx, nameSizeIndexKey(nameHash, size))
	if err != nil {
		return nil, err
	}
	return s.GetFileByID(ctx, strings.TrimSpace(fileID))
}

// ListFiles 列出目录中的全部文件元数据。
func (s *Service) ListFiles(ctx context.Context) ([]model.FileMeta, error) {
	items, err := s.raft.ListPrefix(ctx, "catalog:file:")
	if err != nil {
		return nil, err
	}
	out := make([]model.FileMeta, 0, len(items))
	for _, item := range items {
		id := strings.TrimSpace(item.Value)
		if id == "" {
			continue
		}
		fm, err := s.GetFileByID(ctx, id)
		if err == nil && fm != nil {
			out = append(out, *fm)
		}
	}
	return out, nil
}

// DeleteFile 删除文件元数据、秒传索引和目录项（存储分片由调用方异步清理）。
func (s *Service) DeleteFile(ctx context.Context, fm *model.FileMeta) error {
	// 顺序不重要，尽力删除。
	var lastErr error
	if err := s.raft.Delete(ctx, "file:"+fm.FileID); err != nil {
		lastErr = err
	}
	if err := s.raft.Delete(ctx, nameSizeIndexKey(fm.NameHash, fm.Size)); err != nil {
		lastErr = err
	}
	if err := s.raft.Delete(ctx, "catalog:file:"+fm.FileID); err != nil {
		lastErr = err
	}
	return lastErr
}

// nameSizeIndexKey 构造名称+大小全局索引键，用于秒传判定。
func nameSizeIndexKey(nameHash string, size int64) string {
	return fmt.Sprintf("idx:namehash:%s:%d", nameHash, size)
}

// uploadChunkPrefix 构造上传分片键前缀，用于前缀扫描。
func uploadChunkPrefix(uploadID string) string {
	return "uploadchunk:" + uploadID + ":"
}

// uploadChunkKey 构造单个上传分片的完整键。
func uploadChunkKey(uploadID string, index int) string {
	return fmt.Sprintf("%s%06d", uploadChunkPrefix(uploadID), index)
}
