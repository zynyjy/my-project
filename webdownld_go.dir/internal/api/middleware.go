package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"webdownld_go/internal/auth"

	"github.com/gin-gonic/gin"
)

// ---------------- 中间件 ----------------

// RequestIDMiddleware 为每个请求注入唯一 requestID，并写入响应头。
func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		reqID := c.GetHeader("X-Request-ID")
		if reqID == "" {
			var b [8]byte
			_, _ = rand.Read(b[:])
			reqID = hex.EncodeToString(b[:])
		}
		c.Set("request_id", reqID)
		c.Header("X-Request-ID", reqID)
		c.Next()
	}
}

// AccessLogMiddleware 记录结构化访问日志，包含 requestID、方法、路径、状态码、耗时。
// 即使请求 panic（被 Recovery 捕获），也会记录日志。
func AccessLogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		defer func() {
			reqID, _ := c.Get("request_id")
			status := c.Writer.Status()
			// 若 Recovery 捕获了 panic，状态码可能为 200（未写入），以 500 兜底。
			if status == http.StatusOK && c.Writer.Size() == 0 && len(c.Errors) > 0 {
				status = http.StatusInternalServerError
			}
			INFO("access",
				"req_id", reqID,
				"method", c.Request.Method,
				"path", c.Request.URL.Path,
				"status", status,
				"latency_ms", time.Since(start).Milliseconds(),
				"client_ip", c.ClientIP(),
			)
		}()
		c.Next()
	}
}

// ---------------- Prometheus Metrics ----------------

var (
	uploadRequests   uint64
	downloadRequests uint64
	chunkUploads     uint64
	chunkDownloads   uint64
	deleteRequests   uint64
	activeUploads    int64
)

// MetricsMiddleware 收集请求级指标。
func MetricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Request.URL.Path
		method := c.Request.Method

		// 匹配 /api/files/<id>/manifest 等带参数路径。
		if strings.HasSuffix(path, "/init") && strings.HasPrefix(path, "/api/uploads/") {
			atomic.AddUint64(&uploadRequests, 1)
		} else if strings.Contains(path, "/manifest") && strings.HasPrefix(path, "/api/files/") {
			atomic.AddUint64(&downloadRequests, 1)
		} else if strings.HasPrefix(path, "/api/files/") && method == http.MethodDelete {
			atomic.AddUint64(&deleteRequests, 1)
		}

		if len(c.Param("index")) > 0 && method == http.MethodPost {
			atomic.AddUint64(&chunkUploads, 1)
		}
		if len(c.Param("index")) > 0 && method == http.MethodGet {
			atomic.AddUint64(&chunkDownloads, 1)
		}
		c.Next()
	}
}

// MetricsHandler 返回当前累积指标。
func MetricsHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"upload_requests":   atomic.LoadUint64(&uploadRequests),
		"download_requests": atomic.LoadUint64(&downloadRequests),
		"delete_requests":   atomic.LoadUint64(&deleteRequests),
		"chunk_uploads":     atomic.LoadUint64(&chunkUploads),
		"chunk_downloads":   atomic.LoadUint64(&chunkDownloads),
	})
}

// ---------------- JWT 鉴权中间件 ----------------

// JWTAuthMiddleware 从 Authorization header 提取并校验 JWT Bearer 令牌，
// 通过后将用户信息写入 Gin Context，后续处理器可通过 c.Get("user_id") 等获取。
func JWTAuthMiddleware(jwtService *auth.JWTService) gin.HandlerFunc {
	return func(c *gin.Context) {
		token := c.GetHeader("Authorization")
		if len(token) > 7 && token[:7] == "Bearer " {
			token = token[7:]
		}
		if token == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"ok": false, "error": "未提供认证令牌"})
			return
		}

		claims, err := jwtService.ValidateToken(token)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"ok": false, "error": "令牌无效或已过期"})
			return
		}

		c.Set("user_id", claims.UserID)
		c.Set("username", claims.Username)
		c.Set("is_member", claims.IsMember)
		c.Next()
	}
}

// AuthMiddleware 旧版简易鉴权中间件（已废弃，由 JWTAuthMiddleware 替代）。
// 保留空实现以兼容旧调用，实际鉴权由 JWTAuthMiddleware 完成。
func AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
	}
}

// ---------------- 健康检查处理器 ----------------

// ReadyHandler 就绪检查：验证 Raft 元数据服务是否可达。
func (h *Handler) ReadyzHandler(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	_, err := h.meta.ListFiles(ctx)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready", "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

// Shutdown 优雅关闭处理器内部资源。
func (h *Handler) Shutdown() {
	INFO("shutting down chunk worker pool...")
	h.chunkPool.Shutdown()
	INFO("chunk worker pool stopped")
}