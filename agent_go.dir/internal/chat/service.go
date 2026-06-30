package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	internalagent "agent_go/internal/agent"
	"agent_go/internal/env"
	"agent_go/internal/llm"
	"agent_go/internal/mcp"
	"agent_go/internal/rag"

	"github.com/cloudwego/eino/schema"
)

// Service 编排聊天流水线：RAG 检索、ReAct 推理和流式输出。
type Service struct {
	llm                 *llm.Client                // llm Eino LLM 客户端实例。
	retriever           *rag.CombinedRetriever     // retriever 多通道 RAG 检索引擎。
	mcpClient           *mcp.Client                // mcpClient MCP 工具调用客户端。
	manager             *internalagent.Manager     // manager 智能体事件管理器。
	memory              *ConversationBuffer        // memory 按会话 ID 存储对话历史。
	maxContextMessages  int                        // maxContextMessages 对话窗口最大消息数。
	reactMaxSteps       int                        // reactMaxSteps ReAct 推理最大步数。
}

// reactRunInput 封装单次 ReAct 推理所需的全部输入参数。
type reactRunInput struct {
	SessionID   string       // SessionID 会话标识。
	UserID      string       // UserID 用户唯一标识。
	Roles       []string     // Roles 用户角色列表。
	Query       string       // Query 用户当前提问。
	HistoryText string       // HistoryText 历史对话文本。
	Docs        []rag.Document // Docs RAG 检索到的相关文档。
	Tools       []mcp.Tool   // Tools 可用的 MCP 工具列表。
	SystemState any          // SystemState 当前系统状态快照。
}

// reactDecision 表示 LLM 在单步 ReAct 推理中返回的决策。
type reactDecision struct {
	Step      int            `json:"step"`                 // Step 当前推理步数。
	Thought   string         `json:"thought,omitempty"`   // Thought 推理摘要。
	Action    string         `json:"action"`              // Action 决策动作（mcp_tool 或 final）。
	Tool      string         `json:"tool,omitempty"`      // Tool 当 action=mcp_tool 时使用的工具名。
	ToolInput map[string]any `json:"tool_input,omitempty"` // ToolInput 工具调用参数。
	Answer    string         `json:"answer,omitempty"`    // Answer 最终回答（action=final 时）。
	Citations []string       `json:"citations,omitempty"` // Citations 引用来源列表。
}

// reactObservation 表示 ReAct 推理过程中收集到的观察信息。
type reactObservation struct {
	Type    string `json:"type"`              // Type 观察类型（retrieval/mcp_tool_result）。
	Source  string `json:"source,omitempty"`  // Source 观察来源。
	Content string `json:"content,omitempty"` // Content 观察内容摘要。
	Data    any    `json:"data,omitempty"`    // Data 观察原始数据。
	Error   string `json:"error,omitempty"`   // Error 观察中的错误信息。
}

// NewService 创建聊天服务实例，初始化 ES+Milvus 双通道 RAG、Eino LLM 客户端和 MCP 支持。
func NewService(manager *internalagent.Manager, es *rag.ElasticsearchRetriever, milvus *rag.MilvusRetriever) (*Service, error) {
	llmClient, err := llm.NewClient()
	if err != nil {
		return nil, fmt.Errorf("llm init: %w", err)
	}

	maxContextMessages := env.Int("MAX_CONTEXT_MESSAGES", 12)
	reactMaxSteps := env.Int("REACT_MAX_STEPS", 4)

	svc := new(Service)
	svc.llm = llmClient
	svc.retriever = new(rag.CombinedRetriever)
	svc.retriever.Items = []rag.Retriever{es, milvus}
	svc.mcpClient = mcp.NewClientFromEnv()
	svc.manager = manager
	svc.memory = NewConversationBuffer(maxContextMessages / 2)
	svc.maxContextMessages = maxContextMessages
	svc.reactMaxSteps = reactMaxSteps
	return svc, nil
}

// StreamChat 执行完整的 RAG + LLM 流水线，包括检索、MCP 工具发现、ReAct 推理和流式输出。
func (s *Service) StreamChat(
	ctx context.Context,
	sessionID string,
	userID string,
	roles []string,
	query string,
	send func(event, data string) error,
) error {
	query = strings.TrimSpace(query)
	if query == "" {
		return fmt.Errorf("query is required")
	}

	if strings.TrimSpace(sessionID) == "" {
		sessionID = "default"
	}
	scope := rag.AccessScope{
		UserID: strings.TrimSpace(userID),
		Roles:  normalizeRoles(roles),
	}

	// 步骤 1：RAG 检索上下文。
	s.manager.Emit("rag_retriever_agent", "running", "embedded dual-recall retrieval")
	docs, _ := s.retriever.Retrieve(ctx, query, 6, scope)
	s.manager.Emit("rag_retriever_agent", "completed", fmt.Sprintf("retrieved %d docs via RRF", len(docs)))

	// 步骤 2：加载历史对话。
	historyText := s.loadHistoryText(ctx, sessionID)
	if historyText == "" {
		historyText = "（无历史对话）"
	}
	systemState := s.currentSystemState()

	// 步骤 3：发现 MCP 工具。
	tools, _ := s.discoverMCPTools(ctx, send)
	if err := sendJSON(send, "status", map[string]any{
		"message": "ReAct planning started",
		"mode":    "react_json",
	}); err != nil {
		return err
	}

	// 步骤 4：生成回答（ReAct 循环或降级模式）。
	s.manager.Emit("llm_agent", "running", "generate answer")

	var answer string
	var fallback bool

	if s.llm == nil {
		s.manager.Emit("llm_agent", "warning", "OPENAI_API_KEY not set, using fallback")
		answer = fmt.Sprintf("当前未配置 OPENAI_API_KEY。已完成ES + Milvus 双通道 RAG 检索（RRF 融合），检索到 %d 条文档，发现 %d 个 MCP 工具。配置 API Key 后将启用 ReAct 推理与 LLM 生成。", len(docs), len(tools))
		fallback = true
		_ = sendJSON(send, "react_step", reactDecision{
			Step:     1,
			Thought:  "模型未配置，返回演示结果",
			Action:   "final",
			Answer:   answer,
		})
	} else {
		var err error
		answer, err = s.runReAct(ctx, reactRunInput{
			SessionID:   sessionID,
			UserID:      scope.UserID,
			Roles:       scope.Roles,
			Query:       query,
			HistoryText: historyText,
			Docs:        docs,
			Tools:       tools,
			SystemState: systemState,
		}, send)
		if err != nil {
			s.manager.Emit("llm_agent", "error", err.Error())
			return err
		}
	}

	// 步骤 5：流式输出回答 token。
	if err := streamText(send, answer); err != nil {
		return err
	}

	// 步骤 6：保存到对话记忆。
	s.appendHistory(ctx, sessionID, query, answer)
	s.manager.Emit("chat_agent", "completed", "response finished")
	s.manager.Emit("llm_agent", "completed", "streaming done")
	return sendJSON(send, "done", map[string]any{"ok": true, "fallback": fallback, "mode": "react_json"})
}

// discoverMCPTools 向所有已配置的 MCP 服务器查询工具列表，并将结果推送给前端。
func (s *Service) discoverMCPTools(ctx context.Context, send func(event, data string) error) ([]mcp.Tool, error) {
	if s.mcpClient == nil || !s.mcpClient.Configured() {
		state := map[string]any{
			"configured": false,
			"tools":      []mcp.Tool{},
			"servers":    []mcp.Server{},
		}
		s.manager.SetState("mcp_agent", state)
		s.manager.Emit("mcp_agent", "idle", "MCP_HTTP_ENDPOINTS not configured")
		_ = sendJSON(send, "mcp_tools", state)
		return []mcp.Tool{}, nil
	}

	s.manager.Emit("mcp_agent", "running", "listing MCP tools")
	tools, err := s.mcpClient.ListTools(ctx)
	state := map[string]any{
		"configured": true,
		"tools":      tools,
		"servers":    s.mcpClient.Servers(),
	}
	if err != nil {
		state["error"] = err.Error()
		s.manager.SetState("mcp_agent", state)
		s.manager.Emit("mcp_agent", "warning", err.Error())
		_ = sendJSON(send, "mcp_tools", state)
		return tools, err
	}
	s.manager.SetState("mcp_agent", state)
	s.manager.Emit("mcp_agent", "completed", fmt.Sprintf("discovered %d MCP tools", len(tools)))
	_ = sendJSON(send, "mcp_tools", state)
	return tools, nil
}

// MCPState 返回当前 MCP 客户端状态，包括配置状态、工具列表和服务器信息。
func (s *Service) MCPState(ctx context.Context) (map[string]any, error) {
	if s.mcpClient == nil || !s.mcpClient.Configured() {
		return map[string]any{
			"configured": false,
			"tools":      []mcp.Tool{},
			"servers":    []mcp.Server{},
		}, nil
	}
	tools, err := s.mcpClient.ListTools(ctx)
	state := map[string]any{
		"configured": true,
		"tools":      tools,
		"servers":    s.mcpClient.Servers(),
	}
	if err != nil {
		state["error"] = err.Error()
	}
	return state, err
}

// runReAct 执行 ReAct 推理循环，最多运行 maxSteps 步，每步调用 LLM 决策并执行 MCP 工具或返回最终答案。
func (s *Service) runReAct(ctx context.Context, input reactRunInput, send func(event, data string) error) (string, error) {
	observations := initialObservations(input.Docs)
	maxSteps := s.reactMaxSteps
	if maxSteps <= 0 {
		maxSteps = 4
	}

	for step := 1; step <= maxSteps; step++ {
		forceFinal := step == maxSteps
		payload := s.buildReactPayload(input, observations, step, forceFinal)
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return "", err
		}
		s.manager.Emit("react_agent", "running", map[string]any{
			"step": step, "json_bytes": len(payloadBytes), "force_final": forceFinal,
		})
		if err := sendJSON(send, "react_payload", map[string]any{
			"step": step, "bytes": len(payloadBytes), "force_final": forceFinal,
			"preview": truncateText(string(payloadBytes), 2600),
		}); err != nil {
			return "", err
		}

		raw, err := s.llm.GenerateJSON(ctx, []*schema.Message{
			schema.UserMessage(string(payloadBytes)),
		})
		if err != nil {
			return "", err
		}
		decision, err := parseReactDecision(raw)
		if err != nil {
			return "", err
		}
		decision.Step = step
		decision.Action = normalizeAction(decision.Action)
		if decision.ToolInput == nil {
			decision.ToolInput = map[string]any{}
		}
		if err := sendJSON(send, "react_step", decision); err != nil {
			return "", err
		}

		switch decision.Action {
		case "mcp_tool":
			if forceFinal {
				if strings.TrimSpace(decision.Answer) != "" {
					return decision.Answer, nil
				}
				return "", fmt.Errorf("ReAct reached max steps but model requested MCP tool %q", decision.Tool)
			}
			result, callErr := s.callMCPTool(ctx, decision)
			if callErr != nil {
				observations = append(observations, reactObservation{
					Type: "mcp_tool_result", Source: decision.Tool, Error: callErr.Error(),
				})
				_ = sendJSON(send, "mcp_result", reactObservation{
					Type: "mcp_tool_result", Source: decision.Tool, Error: callErr.Error(),
				})
				continue
			}
			observation := reactObservation{
				Type: "mcp_tool_result", Source: result.Qualified, Data: result,
			}
			if result.IsError {
				observation.Error = "MCP tool returned isError=true"
			}
			observations = append(observations, observation)
			_ = sendJSON(send, "mcp_result", observation)
		case "final":
			answer := strings.TrimSpace(decision.Answer)
			if answer == "" {
				return "", fmt.Errorf("ReAct final answer is empty")
			}
			s.manager.Emit("react_agent", "completed", map[string]any{"steps": step})
			return answer, nil
		default:
			return "", fmt.Errorf("unsupported ReAct action %q", decision.Action)
		}
	}
	return "", fmt.Errorf("ReAct finished without final answer")
}

// callMCPTool 根据 ReAct 决策调用指定的 MCP 工具，返回工具执行结果。
func (s *Service) callMCPTool(ctx context.Context, decision reactDecision) (mcp.ToolResult, error) {
	if s.mcpClient == nil || !s.mcpClient.Configured() {
		return mcp.ToolResult{}, fmt.Errorf("MCP tool requested but MCP_HTTP_ENDPOINTS is not configured")
	}
	toolName := strings.TrimSpace(decision.Tool)
	if toolName == "" {
		return mcp.ToolResult{}, fmt.Errorf("MCP tool requested without tool name")
	}
	s.manager.Emit("mcp_agent", "running", map[string]any{"tool": toolName, "arguments": decision.ToolInput})
	result, err := s.mcpClient.CallTool(ctx, toolName, decision.ToolInput)
	if err != nil {
		s.manager.Emit("mcp_agent", "error", err.Error())
		return mcp.ToolResult{}, err
	}
	s.manager.Emit("mcp_agent", "completed", map[string]any{"tool": result.Qualified, "is_error": result.IsError})
	return result, nil
}

// buildReactPayload 构建发送给 LLM 的 ReAct JSON 负载，包含系统提示、运行时信息、工具定义和观察历史。
func (s *Service) buildReactPayload(input reactRunInput, observations []reactObservation, step int, forceFinal bool) map[string]any {
	actions := []string{"final"}
	if len(input.Tools) > 0 && !forceFinal {
		actions = []string{"mcp_tool", "final"}
	}
	return map[string]any{
		"mode": "react_json",
		"system_instruction": strings.Join([]string{
			"你是企业智能客服助手。",
			"你正在执行 ReAct：基于观察判断是否需要工具；需要外部实时数据或系统能力时调用 MCP 工具；信息足够时给出 final。",
			"整个输入是 JSON，输出也必须是一个 JSON object，不要输出 Markdown。",
			"thought 只写一句可展示的简短推理摘要，不要输出隐藏思维链。",
			"回答使用中文；如检索或工具结果不足，请明确不确定性。",
		}, " "),
		"runtime":              map[string]any{"step": step, "max_steps": s.reactMaxSteps, "force_final": forceFinal},
		"session":             map[string]any{"session_id": input.SessionID, "history": input.HistoryText},
		"user":                map[string]any{"user_id": input.UserID, "roles": input.Roles},
		"task":                map[string]any{"question": input.Query},
		"current_system_state": input.SystemState,
		"available_actions":   actions,
		"mcp_tools":           toolsForPrompt(input.Tools),
		"retrieved_context":   docsForPrompt(input.Docs),
		"observations":        observations,
		"output_schema": map[string]any{
			"thought": "string: 一句可展示的简短推理摘要",
			"action":  "string enum: final 或 mcp_tool",
			"tool":    "string: 当 action=mcp_tool 时填写 mcp_tools[].name",
			"tool_input": "object: 当 action=mcp_tool 时填写工具参数",
			"answer":  "string: 当 action=final 时填写最终回答",
			"citations": "array<string>: 可选，引用 retrieved_context 的 source/id",
		},
	}
}

// currentSystemState 从管理器中获取当前系统状态快照，提取进程监控和修复相关状态。
func (s *Service) currentSystemState() map[string]any {
	snapshot := s.manager.Snapshot()
	out := map[string]any{}
	if monitorState, ok := snapshot["process_monitor_agent"]; ok {
		out["process_monitor_agent"] = compactValue(monitorState, 3600)
	}
	if repairState, ok := snapshot["service_repair_agent"]; ok {
		out["service_repair_agent"] = compactValue(repairState, 1800)
	}
	return out
}

// compactValue 将任意值序列化为 JSON 后截断到指定长度，用于控制注入给 LLM 的上下文大小。
func compactValue(value any, limit int) any {
	b, err := json.Marshal(value)
	if err != nil {
		return value
	}
	text := truncateText(string(b), limit)
	var out any
	if err := json.Unmarshal([]byte(text), &out); err == nil {
		return out
	}
	return text
}

// initialObservations 将 RAG 检索结果转换为 ReAct 初始观察列表，供 LLM 参考。
func initialObservations(docs []rag.Document) []reactObservation {
	if len(docs) == 0 {
		return []reactObservation{{Type: "retrieval", Source: "rag", Content: "未检索到相关知识，必要时基于通用能力回答并明确不确定性。"}}
	}
	out := make([]reactObservation, 0, len(docs))
	for i, doc := range docs {
		out = append(out, reactObservation{
			Type:    "retrieval",
			Source:  fmt.Sprintf("%s:%s", doc.Source, doc.ID),
			Content: fmt.Sprintf("[%d] %s", i+1, truncateText(doc.Content, 1200)),
			Data:    map[string]any{"score": doc.Score},
		})
	}
	return out
}

// toolsForPrompt 将 MCP 工具列表转换为可供 LLM prompt 使用的精简格式。
func toolsForPrompt(tools []mcp.Tool) []map[string]any {
	out := make([]map[string]any, 0, len(tools))
	for _, tool := range tools {
		out = append(out, map[string]any{
			"name": tool.Qualified, "description": tool.Description, "input_schema": tool.InputSchema,
		})
	}
	return out
}

// docsForPrompt 将 RAG 检索文档转换为可供 LLM prompt 使用的精简格式，内容截断以控制 token 消耗。
func docsForPrompt(docs []rag.Document) []map[string]any {
	out := make([]map[string]any, 0, len(docs))
	for _, doc := range docs {
		out = append(out, map[string]any{
			"id": doc.ID, "source": doc.Source, "score": doc.Score,
			"content": truncateText(doc.Content, 1600),
		})
	}
	return out
}

// parseReactDecision 解析 LLM 返回的原始文本为 ReAct 决策结构体，自动处理 Markdown 包裹和隐式 final 动作。
func parseReactDecision(raw string) (reactDecision, error) {
	var decision reactDecision
	cleaned := extractJSONObject(raw)
	if cleaned == "" {
		return decision, fmt.Errorf("model did not return a JSON object")
	}
	if err := json.Unmarshal([]byte(cleaned), &decision); err != nil {
		return decision, fmt.Errorf("decode ReAct JSON failed: %w; raw=%s", err, truncateText(raw, 500))
	}
	if strings.TrimSpace(decision.Action) == "" && strings.TrimSpace(decision.Answer) != "" {
		decision.Action = "final"
	}
	return decision, nil
}

// extractJSONObject 从原始文本中提取 JSON 对象，自动去除 Markdown 代码块标记。
func extractJSONObject(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)
	start := strings.Index(raw, "{")
	end := strings.LastIndex(raw, "}")
	if start < 0 || end < start {
		return ""
	}
	return raw[start : end+1]
}

// normalizeAction 将 LLM 可能输出的各种动作别名统一为标准动作名（mcp_tool 或 final）。
func normalizeAction(action string) string {
	switch strings.TrimSpace(strings.ToLower(action)) {
	case "tool", "call_tool", "mcp", "mcp_tool":
		return "mcp_tool"
	case "answer", "final_answer", "finish", "final":
		return "final"
	default:
		return action
	}
}

// sendJSON 将负载序列化为 JSON 字符串并通过 send 回调发送指定事件。
func sendJSON(send func(event, data string) error, event string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return send(event, string(b))
}

// streamText 将文本按批次（默认 16 个字符）通过 send 回调流式发送 token 事件。
func streamText(send func(event, data string) error, text string) error {
	if text == "" {
		return nil
	}
	const batchSize = 16
	runes := []rune(text)
	for i := 0; i < len(runes); i += batchSize {
		end := i + batchSize
		if end > len(runes) {
			end = len(runes)
		}
		if err := send("token", string(runes[i:end])); err != nil {
			return err
		}
	}
	return nil
}

// truncateText 按字符数截断文本，超过限制时追加省略号，用于控制 prompt 大小。
func truncateText(text string, limit int) string {
	text = strings.TrimSpace(text)
	if limit <= 0 || len([]rune(text)) <= limit {
		return text
	}
	runes := []rune(text)
	return string(runes[:limit]) + "..."
}

// loadHistoryText 从对话缓冲区中加载历史对话文本，供 LLM 上下文使用。
func (s *Service) loadHistoryText(ctx context.Context, sessionID string) string {
	_ = ctx // 保留 context 参数以兼容未来超时控制。
	return s.memory.History(sessionID)
}

// appendHistory 将一轮用户提问和助手回答追加到对话缓冲区。
func (s *Service) appendHistory(ctx context.Context, sessionID, userQuery, assistantReply string) {
	_ = ctx // 保留 context 参数以兼容未来超时控制。
	s.memory.Append(sessionID, userQuery, assistantReply)
}

// normalizeRoles 去除角色列表中的空白和重复项，返回去重后的角色列表。
func normalizeRoles(roles []string) []string {
	out := make([]string, 0, len(roles))
	seen := map[string]bool{}
	for _, r := range roles {
		r = strings.TrimSpace(r)
		if r == "" || seen[r] {
			continue
		}
		seen[r] = true
		out = append(out, r)
	}
	return out
}



