package model

import "time"

type ChunkMeta struct {
	Index      int      `json:"index"`       // Index 分片序号（从 0 开始）。
	ChunkHash  string   `json:"chunk_hash"`  // ChunkHash 分片内容哈希，用于去重与定位。
	Size       int64    `json:"size"`        // Size 分片字节大小。
	StorageIDs []string `json:"storage_ids"` // StorageIDs 分片所在存储节点列表。
}

type FileMeta struct {
	FileID      string      `json:"file_id"`      // FileID 文件唯一 ID（由名称哈希+大小派生）。
	Name        string      `json:"name"`         // Name 原始文件名。
	NameHash    string      `json:"name_hash"`    // NameHash 文件名哈希，用于秒传索引。
	Size        int64       `json:"size"`         // Size 文件总字节数。
	CreatedAt   time.Time   `json:"created_at"`   // CreatedAt 元数据创建时间。
	Owner       string      `json:"owner"`        // Owner 文件所有者。
	Permission  string      `json:"permission"`   // Permission 访问权限描述。
	ChunkSize   int64       `json:"chunk_size"`   // ChunkSize 分片大小（字节）。
	TotalChunks int         `json:"total_chunks"` // TotalChunks 分片总数。
	Chunks      []ChunkMeta `json:"chunks"`       // Chunks 分片元数据列表。
	Complete    bool        `json:"complete"`     // Complete 文件是否已完整上传并可下载。
}

type UploadSession struct {
	UploadID    string         `json:"upload_id"`    // UploadID 上传会话唯一 ID。
	FileID      string         `json:"file_id"`      // FileID 会话关联的目标文件 ID。
	Name        string         `json:"name"`         // Name 上传文件名。
	NameHash    string         `json:"name_hash"`    // NameHash 文件名哈希。
	Size        int64          `json:"size"`         // Size 文件总大小。
	Owner       string         `json:"owner"`        // Owner 上传者标识。
	Permission  string         `json:"permission"`   // Permission 文件权限设置。
	ChunkSize   int64          `json:"chunk_size"`   // ChunkSize 本次上传采用的分片大小。
	TotalChunks int            `json:"total_chunks"` // TotalChunks 本次上传总分片数。
	Received    map[int]bool   `json:"received"`     // Received 已上传分片索引集合。
	ChunkMap    map[int]string `json:"chunk_map"`    // ChunkMap 分片索引到“hash|storage|size”的映射。
	CreatedAt   time.Time      `json:"created_at"`   // CreatedAt 会话创建时间。
	UpdatedAt   time.Time      `json:"updated_at"`   // UpdatedAt 会话最后更新时间。
}

type UploadChunkState struct {
	UploadID  string    `json:"upload_id"`  // UploadID 上传会话 ID。
	Index     int       `json:"index"`      // Index 分片序号。
	ChunkHash string    `json:"chunk_hash"` // ChunkHash 分片内容哈希。
	StorageID string    `json:"storage_id"` // StorageID 实际落盘节点。
	Size      int64     `json:"size"`       // Size 分片字节大小。
	UpdatedAt time.Time `json:"updated_at"` // UpdatedAt 分片状态更新时间。
}
