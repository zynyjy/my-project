package nodeserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"

	pb "webdownld_go/internal/storage/pb"
)

// chunkHashPattern 校验 chunkHash 是否为 64 位 hex 编码的 SHA256（防路径穿越）。
var chunkHashPattern = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)

// Server 存储节点 gRPC 服务，处理分片的保存、读取、删除和存在性检查。
type Server struct {
	pb.UnimplementedStorageNodeServer
	nodeID    string // nodeID 当前存储节点的标识符。
	chunkDir  string // chunkDir 持久化分片文件的存储目录。
	uploadDir string // uploadDir 上传临时文件的存放目录。
}

// New 创建存储节点服务，初始化 chunks 和 uploads 目录。
func New(nodeID, dataRoot string) (*Server, error) {
	s := new(Server)
	s.nodeID = nodeID
	s.chunkDir = filepath.Join(dataRoot, "chunks")
	s.uploadDir = filepath.Join(dataRoot, "uploads")
	if err := os.MkdirAll(s.chunkDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(s.uploadDir, 0o755); err != nil {
		return nil, err
	}
	return s, nil
}

// SaveChunk 接收客户端流式上传的分片数据，计算 SHA256 哈希，通过硬链接去重后持久化到 chunks 目录。
func (s *Server) SaveChunk(stream pb.StorageNode_SaveChunkServer) error {
	var uploadID string
	var index int32
	first := true
	var tmpPath string
	var tmpFile *os.File
	hasher := sha256.New()
	var written int64

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if first {
			uploadID = chunk.UploadId
			index = chunk.Index
			first = false
			tmpDir := filepath.Join(s.uploadDir, uploadID)
			if err := os.MkdirAll(tmpDir, 0o755); err != nil {
				return err
			}
			tmpFile, err = os.CreateTemp(tmpDir, fmt.Sprintf("%06d-*.part", index))
			if err != nil {
				return err
			}
			tmpPath = tmpFile.Name()
			defer os.Remove(tmpPath)
		}
		n, err := io.MultiWriter(tmpFile, hasher).Write(chunk.Data)
		if err != nil {
			tmpFile.Close()
			return err
		}
		written += int64(n)
	}

	if tmpFile == nil {
		return errors.New("empty chunk stream")
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}

	chunkHash := hex.EncodeToString(hasher.Sum(nil))

	if s.chunkExists(chunkHash) {
		return stream.SendAndClose(&pb.ChunkResult{
			ChunkHash: chunkHash, Size: written, StorageId: s.nodeID, Reused: true,
		})
	}

	finalPath := filepath.Join(s.chunkDir, chunkHash+".chunk")
	if err := os.Link(tmpPath, finalPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return stream.SendAndClose(&pb.ChunkResult{
				ChunkHash: chunkHash, Size: written, StorageId: s.nodeID, Reused: true,
			})
		}
		return err
	}

	slog.Info("chunk saved", "hash", chunkHash, "size", written)
	return stream.SendAndClose(&pb.ChunkResult{
		ChunkHash: chunkHash, Size: written, StorageId: s.nodeID, Reused: false,
	})
}

// validChunkHash 校验 chunkHash 仅为 64 位 hex 字符串，防止路径穿越攻击。
func validChunkHash(hash string) bool {
	return chunkHashPattern.MatchString(hash)
}

// ReadChunk 以流式方式读取指定分片文件，分块发送给客户端（每次最多 64KB）。
func (s *Server) ReadChunk(req *pb.ChunkRequest, stream pb.StorageNode_ReadChunkServer) error {
	if !validChunkHash(req.ChunkHash) {
		return fmt.Errorf("invalid chunk hash")
	}
	p := filepath.Join(s.chunkDir, req.ChunkHash+".chunk")
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, 64*1024)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&pb.ChunkDataMsg{Data: buf[:n]}); sendErr != nil {
				return sendErr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// DeleteChunk 从磁盘删除指定分片文件，若文件不存在视为成功。
func (s *Server) DeleteChunk(ctx context.Context, req *pb.ChunkRequest) (*pb.DeleteResult, error) {
	if !validChunkHash(req.ChunkHash) {
		return nil, fmt.Errorf("invalid chunk hash")
	}
	p := filepath.Join(s.chunkDir, req.ChunkHash+".chunk")
	if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return &pb.DeleteResult{Ok: false}, err
	}
	return &pb.DeleteResult{Ok: true}, nil
}

// ChunkExists 检查指定分片是否已存在于磁盘上，用于去重判定。
func (s *Server) ChunkExists(ctx context.Context, req *pb.ChunkRequest) (*pb.ExistResult, error) {
	if !validChunkHash(req.ChunkHash) {
		return &pb.ExistResult{Exists: false}, nil
	}
	return &pb.ExistResult{Exists: s.chunkExists(req.ChunkHash)}, nil
}

// chunkExists 通过 os.Stat 检查指定哈希的分片文件是否已存在于磁盘。
func (s *Server) chunkExists(chunkHash string) bool {
	_, err := os.Stat(filepath.Join(s.chunkDir, chunkHash+".chunk"))
	return err == nil
}

// NodeID 返回当前存储节点的标识符。
func (s *Server) NodeID() string { return s.nodeID }
