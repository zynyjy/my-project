package api

import (
	"bytes"
	"context"
	"sync"

	"webdownld_go/internal/config"
	"webdownld_go/internal/storage"
)

// chunkBufPool 复用分片上传的临时 []byte buffer，避免高并发下频繁分配大块内存导致 GC 压力。
var chunkBufPool = sync.Pool{
	New: func() any {
		// pool 首次创建时按当前 chunkSize 分配；后续由 Put 归还的 buffer 大小可能变化，
		// Get 的调用方负责按本次分片实际大小进行截取/扩容。
		buf := make([]byte, config.ChunkSize())
		return &buf
	},
}

// GetChunkBuf 从池中获取一个分片上传 buffer，注意调用方需负责 PutChunkBuf 归还。
func GetChunkBuf() []byte {
	bp := chunkBufPool.Get().(*[]byte)
	return *bp
}

// PutChunkBuf 归还分片上传 buffer 到池中。
func PutChunkBuf(buf []byte) {
	// 仅归还容量足够大的 buffer，避免池中堆积小 buffer。
	if cap(buf) >= int(config.ChunkSize()) {
		chunkBufPool.Put(&buf)
	}
}

// chunkWriteTask 描述一次分片写入任务。
type chunkWriteTask struct {
	uploadID string // uploadID 上传会话 ID。
	index    int    // index 分片序号。
	data     []byte // data 分片二进制内容（可能来自 chunkBufPool）。
}

// chunkWriteResult 是分片写入任务执行结果。
type chunkWriteResult struct {
	chunkHash string // chunkHash 分片内容哈希。
	size      int64  // size 分片大小。
	storageID string // storageID 实际落盘存储节点 ID。
	reused    bool   // reused 是否命中去重复用。
	err       error  // err 执行错误。
}

type chunkJob struct {
	task     chunkWriteTask        // task 待执行的分片写入任务。
	resultCh chan chunkWriteResult // resultCh 用于回传任务执行结果。
	ctx      context.Context       // ctx 调用方的上下文，用于检测取消。
}

// ChunkWorkerPool 是固定 worker + 有界队列的分片写入池。
type ChunkWorkerPool struct {
	jobs chan chunkJob
	wg   sync.WaitGroup
}

// NewChunkWorkerPool 创建并启动分片写入池。
// workers 为固定 worker 数量，queueSize 为任务队列长度。
func NewChunkWorkerPool(storageSvc *storage.Service, workers, queueSize int) *ChunkWorkerPool {
	if workers <= 0 {
		workers = 32
	}
	if queueSize <= 0 {
		queueSize = workers * 4
	}
	pool := new(ChunkWorkerPool)
	pool.jobs = make(chan chunkJob, queueSize)
	for i := 0; i < workers; i++ {
		pool.wg.Add(1)
		go func() {
			defer pool.wg.Done()
			for job := range pool.jobs {
				// 提交方已取消，跳过任务避免 goroutine 阻塞在 resultCh 发送上。
				if job.ctx.Err() != nil {
					PutChunkBuf(job.task.data)
					continue
				}
				chunkHash, size, storageID, reused, err := storageSvc.SaveChunk(
					job.ctx,
					job.task.uploadID,
					job.task.index,
					bytes.NewReader(job.task.data),
				)
				// 任务完成后归还 buffer 到池中，降低 GC 压力。
				PutChunkBuf(job.task.data)
				select {
				case job.resultCh <- chunkWriteResult{
					chunkHash: chunkHash,
					size:      size,
					storageID: storageID,
					reused:    reused,
					err:       err,
				}:
				default:
					// 提交方已超时/取消且未在读取，丢弃结果避免阻塞 worker。
				}
			}
		}()
	}
	return pool
}

// Submit 提交分片写入任务并等待执行结果。
func (p *ChunkWorkerPool) Submit(ctx context.Context, task chunkWriteTask) (chunkWriteResult, error) {
	resultCh := make(chan chunkWriteResult, 1)
	job := chunkJob{task: task, resultCh: resultCh, ctx: ctx}

	select {
	case p.jobs <- job:
	case <-ctx.Done():
		return chunkWriteResult{}, ctx.Err()
	}

	select {
	case res := <-resultCh:
		return res, nil
	case <-ctx.Done():
		return chunkWriteResult{}, ctx.Err()
	}
}

// Shutdown 优雅关闭 worker 池，等待所有进行中的任务完成。
func (p *ChunkWorkerPool) Shutdown() {
	close(p.jobs)
	p.wg.Wait()
}
