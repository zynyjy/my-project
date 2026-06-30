package web

import (
	"encoding/json"
	"net/http"

	"agent_go/internal/agent"
	"agent_go/internal/chat"
	"agent_go/internal/monitor"
	"agent_go/internal/sensitive"

	"github.com/gin-gonic/gin"
)

// Server HTTP 服务器，管理路由注册、静态资源和 SSE 流。
type Server struct {
	manager *agent.Manager     // manager 提供智能体状态快照与事件流。
	chatSvc *chat.Service      // chatSvc 提供流式聊天能力。
	filter  *sensitive.Filter  // filter 敏感词过滤器。
}

// NewServer 创建 HTTP 服务器对象。
// manager 为状态管理器，chatSvc 为聊天服务，filter 为敏感词过滤器。
func NewServer(manager *agent.Manager, chatSvc *chat.Service, filter *sensitive.Filter) *Server {
	srv := new(Server)
	srv.manager = manager
	srv.chatSvc = chatSvc
	srv.filter = filter
	return srv
}

// Router 构建 Gin 路由并注册静态资源与 API。
func (s *Server) Router() *gin.Engine {
	r := gin.Default()
	r.Static("/assets", "./web")
	r.GET("/", func(c *gin.Context) {
		c.File("./web/index.html")
	})

	api := r.Group("/api")
	{
		api.GET("/agents/snapshot", func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{"ok": true, "data": s.manager.Snapshot()})
		})

		api.GET("/agents/stream", s.agentsStream)
		api.GET("/mcp/tools", s.mcpTools)
		api.GET("/monitor/history", s.monitorHistory)
		api.POST("/chat/stream", s.chatStream)
	}

	return r
}

// agentsStream 建立 SSE 连接，持续推送 agent 事件与初始快照。
func (s *Server) agentsStream(c *gin.Context) {
	stream := s.manager.Hub().Subscribe()
	defer s.manager.Hub().Unsubscribe(stream)

	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "stream unsupported"})
		return
	}

	c.SSEvent("snapshot", s.manager.Snapshot())
	flusher.Flush()

	for {
		select {
		case <-c.Request.Context().Done():
			return
		case ev := <-stream:
			c.SSEvent("agent", ev)
			flusher.Flush()
		}
	}
}

// monitorHistory 返回进程监控时间序列历史数据，供前端折线图使用。
func (s *Server) monitorHistory(c *gin.Context) {
	raw := s.manager.MonitorHistory()
	if raw == nil {
		c.JSON(http.StatusOK, gin.H{"ok": true, "data": gin.H{"snapshots": []monitor.Snapshot{}}})
		return
	}
	ring, ok := raw.(*monitor.TimeseriesRing)
	if !ok {
		c.JSON(http.StatusOK, gin.H{"ok": true, "data": gin.H{"snapshots": []monitor.Snapshot{}}})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "data": gin.H{"snapshots": ring.Snapshot()}})
}

// chatStream 接收聊天请求并通过 SSE 流式返回 token 与状态事件。
func (s *Server) chatStream(c *gin.Context) {
	var req struct {
		Message   string   `json:"message"`    // Message 用户输入的消息文本。
		SessionID string   `json:"session_id"` // SessionID 会话标识。
		UserID    string   `json:"user_id"`    // UserID 用户唯一标识。
		Roles     []string `json:"roles"`      // Roles 用户角色列表。
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Message == "" {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "message is required"})
		return
	}

	// 敏感词过滤。
	if err := s.filter.IsSafe(req.Message); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "stream unsupported"})
		return
	}

	send := func(event, data string) error {
		raw, _ := json.Marshal(gin.H{"value": data})
		if _, err := c.Writer.Write([]byte("event: " + event + "\n")); err != nil {
			return err
		}
		if _, err := c.Writer.Write([]byte("data: " + string(raw) + "\n\n")); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	if err := s.chatSvc.StreamChat(c.Request.Context(), req.SessionID, req.UserID, req.Roles, req.Message, send); err != nil {
		raw, _ := json.Marshal(gin.H{"value": err.Error()})
		c.Writer.Write([]byte("event: error\n"))
		c.Writer.Write([]byte("data: " + string(raw) + "\n\n"))
		flusher.Flush()
	}
}

// mcpTools 返回当前配置 MCP 服务的工具清单，便于前端展示可调用能力。
func (s *Server) mcpTools(c *gin.Context) {
	state, err := s.chatSvc.MCPState(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"ok": false, "data": state, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "data": state})
}
