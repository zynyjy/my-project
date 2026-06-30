package api

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"webdownld_go/internal/config"
	"webdownld_go/internal/meta"
	"webdownld_go/internal/model"
	"webdownld_go/internal/storage"

	"github.com/gin-gonic/gin"
)

// uploadLockVal 包装 sync.Mutex 并记录最后一次使用时间，用于定期清理。
type uploadLockVal struct {
	mu     sync.Mutex
	lastAt time.Time
}

type Handler struct {
	meta        *meta.Service    // meta 元数据服务，负责 Raft 一致性读写。
	storage     *storage.Service // storage 分片存储服务。
	chunkSize   int64            // chunkSize 当前服务使用的分片大小（字节）。
	chunkPool   *ChunkWorkerPool // chunkPool 固定 worker 分片写入池。
	uploadLocks sync.Map         // uploadLocks 按 uploadID 管理会话级互斥锁（含惰性清理）。
}

// cleanUploadLocks 启动后台协程，每 5 分钟清理超过 10 分钟未使用的上传锁，防止内存泄漏。
func (h *Handler) cleanUploadLocks() {
	go func() {
		for {
			time.Sleep(5 * time.Minute)
			now := time.Now()
			h.uploadLocks.Range(func(key, value any) bool {
				v := value.(*uploadLockVal)
				v.mu.Lock()
				if now.Sub(v.lastAt) > 10*time.Minute {
					v.mu.Unlock()
					h.uploadLocks.Delete(key)
					return true
				}
				v.mu.Unlock()
				return true
			})
		}
	}()
}

// New 创建 API 处理器实例。
// metaSvc 为元数据服务，storageSvc 为存储服务，maxConcurrentChunkWrites 为并发写入上限。
func New(metaSvc *meta.Service, storageSvc *storage.Service, maxConcurrentChunkWrites int) *Handler {
	if maxConcurrentChunkWrites <= 0 {
		maxConcurrentChunkWrites = 64
	}
	queueSize := maxConcurrentChunkWrites * 4
	h := new(Handler)
	h.meta = metaSvc
	h.storage = storageSvc
	h.chunkSize = config.ChunkSize()
	h.chunkPool = NewChunkWorkerPool(storageSvc, maxConcurrentChunkWrites, queueSize)
	h.cleanUploadLocks()
	return h
}

// Register 注册网盘相关 API 路由。
func (h *Handler) Register(r *gin.Engine) {
	api := r.Group("/api")
	api.Use(AuthMiddleware())
	{
		api.GET("/files", h.listFiles)
		api.DELETE("/files/:fileID", h.deleteFile)
		api.POST("/uploads/init", h.initUpload)
		api.GET("/uploads/:uploadID/status", h.uploadStatus)
		api.POST("/uploads/:uploadID/chunks/:index", h.uploadChunk)
		api.POST("/uploads/:uploadID/complete", h.completeUpload)
		api.GET("/files/:fileID/manifest", h.fileManifest)
		api.GET("/files/:fileID/chunks/:index", h.downloadChunk)
	}

	// 观测端点（无需鉴权）。
	r.GET("/metrics", MetricsHandler)
}

// initUpload 初始化上传会话并返回上传计划，支持秒传判定。
func (h *Handler) initUpload(c *gin.Context) {
	var req struct {
		Name       string `json:"name"`       // Name 上传文件名。
		Size       int64  `json:"size"`       // Size 文件总大小（字节）。
		Owner      string `json:"owner"`      // Owner 上传者标识。
		Permission string `json:"permission"` // Permission 文件权限设置。
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Name == "" || req.Size <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid request"})
		return
	}

	nameHash := meta.NameHash(req.Name)
	if existing, err := h.meta.GetFileByNameHashAndSize(c.Request.Context(), nameHash, req.Size); err == nil && existing != nil && existing.Complete {
		c.JSON(http.StatusOK, gin.H{"ok": true, "instant_upload": true, "file": existing})
		return
	}

	fileID := meta.FileIDByName(req.Name, req.Size)
	totalChunks := int((req.Size + h.chunkSize - 1) / h.chunkSize)
	ss := model.UploadSession{
		UploadID:    meta.UploadID(fileID),
		FileID:      fileID,
		Name:        req.Name,
		NameHash:    nameHash,
		Size:        req.Size,
		Owner:       fallback(req.Owner, "guest"),
		Permission:  fallback(req.Permission, "rw"),
		ChunkSize:   h.chunkSize,
		TotalChunks: totalChunks,
		Received:    map[int]bool{},
		ChunkMap:    map[int]string{},
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	if err := h.meta.SaveUploadSession(c.Request.Context(), ss); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"ok":             true,
		"upload_id":      ss.UploadID,
		"file_id":        ss.FileID,
		"chunk_size":     ss.ChunkSize,
		"total_chunks":   ss.TotalChunks,
		"received":       []int{},
		"instant_upload": false,
	})
}

// uploadStatus 返回上传会话的已收分片进度，用于断点续传。
func (h *Handler) uploadStatus(c *gin.Context) {
	ss, err := h.meta.GetUploadSession(c.Request.Context(), c.Param("uploadID"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "upload not found"})
		return
	}
	received := make([]int, 0, len(ss.Received))
	for idx, ok := range ss.Received {
		if ok {
			received = append(received, idx)
		}
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "received": received, "total_chunks": ss.TotalChunks})
}

// uploadChunk 接收并保存单个分片，同时更新上传会话状态。
func (h *Handler) uploadChunk(c *gin.Context) {
	uploadID := c.Param("uploadID")
	lock := h.getUploadLock(uploadID)
	lock.Lock()
	defer lock.Unlock()

	ss, err := h.meta.GetUploadSession(c.Request.Context(), uploadID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "upload not found"})
		return
	}
	index, err := strconv.Atoi(c.Param("index"))
	if err != nil || index < 0 || index >= ss.TotalChunks {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid chunk index"})
		return
	}
	if ss.Received[index] {
		c.JSON(http.StatusOK, gin.H{"ok": true, "already_uploaded": true})
		return
	}

	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, h.chunkSize+1)

	expectedSize := h.expectedChunkSize(ss, index)

	// 使用 sync.Pool 复用 buffer，避免高并发下频繁分配大块内存触发 GC 压力。
	buf := GetChunkBuf()[:0]
	if cap(buf) < int(expectedSize) {
		buf = make([]byte, expectedSize)
	}
	buf = buf[:expectedSize]
	if _, err := io.ReadFull(c.Request.Body, buf); err != nil {
		PutChunkBuf(buf)
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "failed to read chunk body"})
		return
	}
	result, err := h.chunkPool.Submit(c.Request.Context(), chunkWriteTask{
		uploadID: ss.UploadID,
		index:    index,
		data:     buf,
	})
	if err != nil {
		c.JSON(http.StatusRequestTimeout, gin.H{"ok": false, "error": err.Error()})
		return
	}

	if result.err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": result.err.Error()})
		return
	}

	chunkHash := result.chunkHash
	size := result.size
	storageID := result.storageID
	reused := result.reused

	ss.Received[index] = true
	ss.ChunkMap[index] = chunkHash + "|" + storageID + "|" + strconv.FormatInt(size, 10)
	if err := h.meta.SaveUploadChunk(c.Request.Context(), ss.UploadID, model.UploadChunkState{
		UploadID:  ss.UploadID,
		Index:     index,
		ChunkHash: chunkHash,
		StorageID: storageID,
		Size:      size,
	}); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"ok":           true,
		"chunk_hash":   chunkHash,
		"storage_id":   storageID,
		"size":         size,
		"deduplicated": reused,
	})
}

// completeUpload 在所有分片上传完成后组装并固化文件元数据。
func (h *Handler) completeUpload(c *gin.Context) {
	uploadID := c.Param("uploadID")
	lock := h.getUploadLock(uploadID)
	lock.Lock()
	defer lock.Unlock()

	ss, err := h.meta.GetUploadSession(c.Request.Context(), uploadID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "upload not found"})
		return
	}
	if len(ss.Received) != ss.TotalChunks {
		c.JSON(http.StatusConflict, gin.H{"ok": false, "error": "not all chunks uploaded", "uploaded": len(ss.Received), "total": ss.TotalChunks})
		return
	}

	chunks := make([]model.ChunkMeta, 0, ss.TotalChunks)
	var totalSize int64
	for i := 0; i < ss.TotalChunks; i++ {
		raw := ss.ChunkMap[i]
		part := strings.SplitN(raw, "|", 3)
		if len(part) != 3 {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "invalid chunk map"})
			return
		}
		size, err := strconv.ParseInt(part[2], 10, 64)
		if err != nil || size != h.expectedChunkSize(ss, i) {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "invalid chunk size in metadata"})
			return
		}
		totalSize += size
		chunks = append(chunks, model.ChunkMeta{
			Index:      i,
			ChunkHash:  part[0],
			StorageIDs: []string{part[1]},
			Size:       size,
		})
	}
	if totalSize != ss.Size {
		c.JSON(http.StatusConflict, gin.H{"ok": false, "error": "uploaded size mismatch", "uploaded": totalSize, "expected": ss.Size})
		return
	}

	metaFile := model.FileMeta{
		FileID:      ss.FileID,
		Name:        ss.Name,
		NameHash:    ss.NameHash,
		Size:        ss.Size,
		CreatedAt:   time.Now(),
		Owner:       ss.Owner,
		Permission:  ss.Permission,
		ChunkSize:   ss.ChunkSize,
		TotalChunks: ss.TotalChunks,
		Chunks:      chunks,
		Complete:    true,
	}
	if err := h.meta.SaveFile(c.Request.Context(), metaFile); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "file": metaFile})
}

// getUploadLock 获取指定上传会话的互斥锁（惰性创建，后台协程定期清理过期锁）。
func (h *Handler) getUploadLock(uploadID string) *sync.Mutex {
	v, _ := h.uploadLocks.LoadOrStore(uploadID, &uploadLockVal{})
	lv := v.(*uploadLockVal)
	lv.mu.Lock()
	lv.lastAt = time.Now()
	lv.mu.Unlock()
	return &lv.mu
}

// expectedChunkSize 计算指定分片索引的期望大小（尾部分片可能不足 chunkSize）。
func (h *Handler) expectedChunkSize(ss *model.UploadSession, index int) int64 {
	if index == ss.TotalChunks-1 {
		used := int64(index) * ss.ChunkSize
		return ss.Size - used
	}
	return ss.ChunkSize
}

// listFiles 返回文件目录列表。
func (h *Handler) listFiles(c *gin.Context) {
	files, err := h.meta.ListFiles(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "data": files})
}

// fileManifest 返回指定文件的下载清单（分片映射）。
func (h *Handler) fileManifest(c *gin.Context) {
	file, err := h.meta.GetFileByID(c.Request.Context(), c.Param("fileID"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "file not found"})
		return
	}
	manifest := gin.H{
		"file_id":      file.FileID,
		"name":         file.Name,
		"size":         file.Size,
		"total_chunks": file.TotalChunks,
		"chunk_size":   file.ChunkSize,
		"chunks":       file.Chunks,
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "manifest": manifest})
}

// downloadChunk 下载指定文件的单个分片内容（流式传输，避免全量读入内存）。
func (h *Handler) downloadChunk(c *gin.Context) {
	file, err := h.meta.GetFileByID(c.Request.Context(), c.Param("fileID"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "file not found"})
		return
	}
	index, err := strconv.Atoi(c.Param("index"))
	if err != nil || index < 0 || index >= len(file.Chunks) {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid chunk index"})
		return
	}
	ch := file.Chunks[index]
	if len(ch.StorageIDs) == 0 {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "no storage location"})
		return
	}

	rc, contentLength, err := h.storage.OpenChunk(c.Request.Context(), ch.StorageIDs[0], ch.ChunkHash, ch.Size)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}
	defer rc.Close()

	c.Header("X-Chunk-Index", strconv.Itoa(index))
	c.Header("X-Chunk-Hash", ch.ChunkHash)
	c.Header("X-Source-Storage", ch.StorageIDs[0])
	c.Header("Content-Length", strconv.FormatInt(contentLength, 10))
	c.DataFromReader(http.StatusOK, contentLength, "application/octet-stream", rc, nil)
}

// deleteFile 删除文件元数据、秒传索引、目录项和存储分片。
func (h *Handler) deleteFile(c *gin.Context) {
	fileID := c.Param("fileID")
	fm, err := h.meta.GetFileByID(c.Request.Context(), fileID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "file not found"})
		return
	}
	if err := h.meta.DeleteFile(c.Request.Context(), fm); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": err.Error()})
		return
	}
	// 异步清理存储分片，不阻塞响应。
	go func() {
		for _, ch := range fm.Chunks {
			for _, sid := range ch.StorageIDs {
				_ = h.storage.DeleteChunk(context.Background(), sid, ch.ChunkHash)
			}
		}
	}()
	c.JSON(http.StatusOK, gin.H{"ok": true, "deleted": fileID})
}

// fallback 当 v 为空时返回 def，否则返回 v。
func fallback(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
