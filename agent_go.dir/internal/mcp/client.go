package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Server 表示一个 MCP HTTP JSON-RPC 服务端连接配置。
type Server struct {
	Name        string `json:"name"` // Name 服务器名称标识。
	URL         string `json:"url"`  // URL 服务器 JSON-RPC 端点地址。
	BearerToken string `json:"-"`    // BearerToken 认证令牌（不序列化到 JSON）。
}

// Tool 表示 MCP 服务器提供的一个可调用工具。
type Tool struct {
	Server      string         `json:"server"`                // Server 所属服务器名称。
	Name        string         `json:"name"`                  // Name 工具名称。
	Qualified   string         `json:"qualified"`             // Qualified 全限定名（server.name）。
	Description string         `json:"description,omitempty"` // Description 工具功能描述。
	InputSchema map[string]any `json:"input_schema,omitempty"` // InputSchema 工具输入参数的 JSON Schema。
}

// ToolResult 表示 MCP 工具调用的返回结果。
type ToolResult struct {
	Server    string `json:"server"`              // Server 来源服务器名称。
	Name      string `json:"name"`                // Name 工具名称。
	Qualified string `json:"qualified"`           // Qualified 全限定名。
	Content   any    `json:"content,omitempty"`   // Content 工具返回内容。
	IsError   bool   `json:"is_error"`            // IsError 工具是否返回错误。
}

// Client 是 HTTP JSON-RPC MCP 客户端，管理多服务器连接与协议握手。
type Client struct {
	servers         []Server          // servers 已配置的 MCP 服务器列表。
	http            *http.Client      // http HTTP 客户端实例。
	protocolVersion string            // protocolVersion 默认 MCP 协议版本。
	mu              sync.Mutex        // mu 保护 sessions、protocols 和 initialized 的并发访问。
	sessions        map[string]string // sessions 保存各服务器的会话 ID。
	protocols       map[string]string // protocols 保存各服务器协商后的协议版本。
	initialized     map[string]bool   // initialized 标记各服务器是否已完成初始化握手。
}

// rpcRequest 表示 JSON-RPC 2.0 请求结构。
type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`        // JSONRPC 协议版本，固定为 "2.0"。
	ID      any    `json:"id,omitempty"`   // ID 请求标识（通知类请求可省略）。
	Method  string `json:"method"`         // Method 调用的 RPC 方法名。
	Params  any    `json:"params,omitempty"` // Params 方法参数。
}

// rpcResponse 表示 JSON-RPC 2.0 响应结构。
type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`       // JSONRPC 协议版本。
	ID      string          `json:"id"`            // ID 对应的请求标识。
	Result  json.RawMessage `json:"result,omitempty"` // Result 成功时的方法返回值。
	Error   *rpcError       `json:"error,omitempty"`  // Error 失败时的错误详情。
}

// rpcError 表示 JSON-RPC 错误信息。
type rpcError struct {
	Code    int    `json:"code"`    // Code 错误码。
	Message string `json:"message"` // Message 错误描述信息。
}

// NewClientFromEnv 从环境变量 MCP_HTTP_ENDPOINTS 构建 HTTP JSON-RPC MCP 客户端。
// MCP_HTTP_ENDPOINTS 接受逗号分隔的条目，格式为 "name=http://host/mcp" 或纯 URL。
func NewClientFromEnv() *Client {
	timeout := 8 * time.Second
	if raw := strings.TrimSpace(os.Getenv("MCP_TIMEOUT_SECONDS")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			timeout = time.Duration(parsed) * time.Second
		}
	}
	token := strings.TrimSpace(os.Getenv("MCP_BEARER_TOKEN"))
	protocolVersion := strings.TrimSpace(os.Getenv("MCP_PROTOCOL_VERSION"))
	if protocolVersion == "" {
		protocolVersion = "2025-11-25"
	}
	c := new(Client)
	c.servers = parseServers(os.Getenv("MCP_HTTP_ENDPOINTS"), token)
	c.http = new(http.Client)
	c.http.Timeout = timeout
	c.protocolVersion = protocolVersion
	c.sessions = make(map[string]string)
	c.protocols = make(map[string]string)
	c.initialized = make(map[string]bool)
	return c
}

// parseServers 解析逗号分隔的服务器配置字符串，返回 Server 列表。
func parseServers(raw, token string) []Server {
	parts := strings.Split(raw, ",")
	servers := make([]Server, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name := fmt.Sprintf("mcp%d", len(servers)+1)
		url := part
		if strings.Contains(part, "=") {
			pair := strings.SplitN(part, "=", 2)
			if strings.TrimSpace(pair[0]) != "" {
				name = normalizeName(pair[0])
			}
			url = strings.TrimSpace(pair[1])
		}
		if url == "" {
			continue
		}
		servers = append(servers, Server{Name: name, URL: url, BearerToken: token})
	}
	return servers
}

// normalizeName 将服务器名称标准化为小写字母、数字和下划线的组合。
func normalizeName(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_' || r == '-':
			b.WriteRune('_')
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "mcp"
	}
	return out
}

// Configured 返回客户端是否已配置有效的 MCP 服务器。
func (c *Client) Configured() bool {
	return c != nil && len(c.servers) > 0
}

// Servers 返回已配置服务器列表的副本，去除 BearerToken 等敏感信息。
func (c *Client) Servers() []Server {
	if c == nil {
		return nil
	}
	out := make([]Server, len(c.servers))
	copy(out, c.servers)
	for i := range out {
		out[i].BearerToken = ""
	}
	return out
}

// ListTools 并发向所有已配置的 MCP 服务器查询工具列表，合并返回。
func (c *Client) ListTools(ctx context.Context) ([]Tool, error) {
	if c == nil || len(c.servers) == 0 {
		return []Tool{}, nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	all := make([]Tool, 0)
	var firstErr error
	for _, server := range c.servers {
		srv := server
		wg.Add(1)
		go func() {
			defer wg.Done()
			tools, err := c.listServerTools(ctx, srv)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			all = append(all, tools...)
		}()
	}
	wg.Wait()
	return all, firstErr
}

// listServerTools 向单个 MCP 服务器查询所有工具，支持分页遍历。
func (c *Client) listServerTools(ctx context.Context, server Server) ([]Tool, error) {
	if err := c.ensureInitialized(ctx, server); err != nil {
		return nil, err
	}
	var tools []Tool
	cursor := ""
	for {
		params := map[string]any{}
		if cursor != "" {
			params["cursor"] = cursor
		}
		raw, err := c.rpc(ctx, server, "tools/list", params)
		if err != nil {
			return nil, err
		}

		var parsed struct {
			NextCursor string `json:"nextCursor"`
			Tools      []struct {
				Name             string          `json:"name"`
				Description      string          `json:"description"`
				InputSchema      json.RawMessage `json:"inputSchema"`
				InputSchemaSnake json.RawMessage `json:"input_schema"`
			} `json:"tools"`
		}
		if err := json.Unmarshal(raw, &parsed); err != nil {
			return nil, fmt.Errorf("%s tools/list decode failed: %w", server.Name, err)
		}

		for _, item := range parsed.Tools {
			name := strings.TrimSpace(item.Name)
			if name == "" {
				continue
			}
			schema := map[string]any{}
			schemaRaw := item.InputSchema
			if len(schemaRaw) == 0 {
				schemaRaw = item.InputSchemaSnake
			}
			if len(schemaRaw) > 0 {
				_ = json.Unmarshal(schemaRaw, &schema)
			}
			tools = append(tools, Tool{
				Server:      server.Name,
				Name:        name,
				Qualified:   server.Name + "." + name,
				Description: item.Description,
				InputSchema: schema,
			})
		}
		cursor = strings.TrimSpace(parsed.NextCursor)
		if cursor == "" {
			break
		}
	}
	return tools, nil
}

// CallTool 调用指定 MCP 工具，qualified 为全限定工具名（server.tool），arguments 为工具参数。
func (c *Client) CallTool(ctx context.Context, qualified string, arguments map[string]any) (ToolResult, error) {
	server, toolName, err := c.resolveTool(ctx, qualified)
	if err != nil {
		return ToolResult{}, err
	}
	if err := c.ensureInitialized(ctx, server); err != nil {
		return ToolResult{}, err
	}
	extraHeaders := c.toolParameterHeaders(ctx, server, toolName, arguments)
	raw, err := c.rpcWithHeaders(ctx, server, "tools/call", map[string]any{
		"name":      toolName,
		"arguments": arguments,
	}, extraHeaders)
	if err != nil {
		return ToolResult{}, err
	}

	var parsed struct {
		Content any  `json:"content"`
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return ToolResult{}, fmt.Errorf("%s.%s tools/call decode failed: %w", server.Name, toolName, err)
	}
	return ToolResult{
		Server:    server.Name,
		Name:      toolName,
		Qualified: server.Name + "." + toolName,
		Content:   parsed.Content,
		IsError:   parsed.IsError,
	}, nil
}

// resolveTool 根据全限定名（server.tool）或短名解析出对应的服务器和工具名。
func (c *Client) resolveTool(ctx context.Context, qualified string) (Server, string, error) {
	if c == nil || len(c.servers) == 0 {
		return Server{}, "", fmt.Errorf("no MCP server configured")
	}
	qualified = strings.TrimSpace(qualified)
	if qualified == "" {
		return Server{}, "", fmt.Errorf("tool name is empty")
	}

	if strings.Contains(qualified, ".") {
		parts := strings.SplitN(qualified, ".", 2)
		serverName := strings.TrimSpace(parts[0])
		toolName := strings.TrimSpace(parts[1])
		for _, server := range c.servers {
			if server.Name == serverName {
				return server, toolName, nil
			}
		}
		return Server{}, "", fmt.Errorf("MCP server %q not configured", serverName)
	}

	tools, err := c.ListTools(ctx)
	if err != nil {
		return Server{}, "", err
	}
	var matched *Tool
	for i := range tools {
		if tools[i].Name != qualified {
			continue
		}
		if matched != nil {
			return Server{}, "", fmt.Errorf("tool %q is ambiguous, use server.tool", qualified)
		}
		matched = &tools[i]
	}
	if matched == nil {
		return Server{}, "", fmt.Errorf("tool %q not found", qualified)
	}
	for _, server := range c.servers {
		if server.Name == matched.Server {
			return server, matched.Name, nil
		}
	}
	return Server{}, "", fmt.Errorf("MCP server %q not configured", matched.Server)
}

// toolParameterHeaders 从工具输入 schema 中提取 x-mcp-header 标注的参数，生成对应的 HTTP 请求头。
func (c *Client) toolParameterHeaders(ctx context.Context, server Server, toolName string, arguments map[string]any) map[string]string {
	if len(arguments) == 0 {
		return nil
	}
	tools, err := c.listServerTools(ctx, server)
	if err != nil {
		return nil
	}
	var selected *Tool
	for i := range tools {
		if tools[i].Name == toolName {
			selected = &tools[i]
			break
		}
	}
	if selected == nil || len(selected.InputSchema) == 0 {
		return nil
	}

	properties, ok := selected.InputSchema["properties"].(map[string]any)
	if !ok {
		return nil
	}
	headers := map[string]string{}
	for argName, rawProperty := range properties {
		property, ok := rawProperty.(map[string]any)
		if !ok {
			continue
		}
		headerName, ok := property["x-mcp-header"].(string)
		if !ok || !validHeaderName(headerName) {
			continue
		}
		value, ok := arguments[argName]
		if !ok {
			continue
		}
		headers["Mcp-Param-"+headerName] = encodeHeaderValue(fmt.Sprint(value))
	}
	if len(headers) == 0 {
		return nil
	}
	return headers
}

// ensureInitialized 确保与指定 MCP 服务器完成初始化握手（initialize + notifications/initialized）。
func (c *Client) ensureInitialized(ctx context.Context, server Server) error {
	if c.isInitialized(server.Name) {
		return nil
	}

	raw, headers, err := c.doJSONRPC(ctx, server, "initialize", map[string]any{
		"protocolVersion": c.protocolVersion,
		"capabilities":    map[string]any{},
		"clientInfo": map[string]any{
			"name":    "agent_go",
			"version": "0.1.0",
		},
	}, true, nil)
	if err != nil {
		return err
	}

	var initResult struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	_ = json.Unmarshal(raw, &initResult)

	c.mu.Lock()
	if sessionID := headers.Get("Mcp-Session-Id"); sessionID != "" {
		c.sessions[server.Name] = sessionID
	}
	if initResult.ProtocolVersion != "" {
		c.protocols[server.Name] = initResult.ProtocolVersion
	}
	c.mu.Unlock()

	if err := c.notify(ctx, server, "notifications/initialized", nil); err != nil {
		return err
	}

	c.mu.Lock()
	c.initialized[server.Name] = true
	c.mu.Unlock()
	return nil
}

// isInitialized 检查指定服务器是否已完成初始化握手。
func (c *Client) isInitialized(serverName string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.initialized[serverName]
}

// rpc 执行单次 JSON-RPC 调用并返回原始结果，不附加额外请求头。
func (c *Client) rpc(ctx context.Context, server Server, method string, params any) (json.RawMessage, error) {
	return c.rpcWithHeaders(ctx, server, method, params, nil)
}

// rpcWithHeaders 执行带额外请求头的 JSON-RPC 调用并返回原始结果。
func (c *Client) rpcWithHeaders(ctx context.Context, server Server, method string, params any, extraHeaders map[string]string) (json.RawMessage, error) {
	raw, _, err := c.doJSONRPC(ctx, server, method, params, true, extraHeaders)
	return raw, err
}

// notify 发送 JSON-RPC 通知（不期待响应），用于 notifications/initialized 等。
func (c *Client) notify(ctx context.Context, server Server, method string, params any) error {
	_, _, err := c.doJSONRPC(ctx, server, method, params, false, nil)
	return err
}

// doJSONRPC 执行完整的 HTTP JSON-RPC 调用，包含请求构建、头部注入、响应解码和错误处理。
func (c *Client) doJSONRPC(
	ctx context.Context,
	server Server,
	method string,
	params any,
	expectResponse bool,
	extraHeaders map[string]string,
) (json.RawMessage, http.Header, error) {
	request := rpcRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
	}
	if expectResponse {
		request.ID = fmt.Sprintf("agent-go-%d", time.Now().UnixNano())
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewReader(payload))
	if err != nil {
		return nil, nil, err
	}
	c.applyHeaders(req, server, method, params, extraHeaders)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("%s %s failed: %w", server.Name, method, err)
	}
	defer resp.Body.Close()
	if !expectResponse && resp.StatusCode == http.StatusAccepted {
		return nil, resp.Header, nil
	}
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return nil, resp.Header, fmt.Errorf("%s %s returned %d: %s", server.Name, method, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if !expectResponse {
		return nil, resp.Header, nil
	}

	out, err := decodeRPCResponse(resp)
	if err != nil {
		return nil, resp.Header, fmt.Errorf("%s %s response decode failed: %w", server.Name, method, err)
	}
	if out.Error != nil {
		return nil, resp.Header, fmt.Errorf("%s %s rpc error %d: %s", server.Name, method, out.Error.Code, out.Error.Message)
	}
	return out.Result, resp.Header, nil
}

// applyHeaders 设置 MCP JSON-RPC 请求所需的标准 HTTP 头及额外自定义头。
func (c *Client) applyHeaders(req *http.Request, server Server, method string, params any, extraHeaders map[string]string) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Mcp-Method", method)
	if name := requestName(method, params); name != "" {
		req.Header.Set("Mcp-Name", encodeHeaderValue(name))
	}
	if method != "initialize" {
		req.Header.Set("MCP-Protocol-Version", c.protocolFor(server.Name))
		if sessionID := c.sessionFor(server.Name); sessionID != "" {
			req.Header.Set("Mcp-Session-Id", sessionID)
		}
	}
	for key, value := range extraHeaders {
		req.Header.Set(key, value)
	}
	if server.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+server.BearerToken)
	}
}

// sessionFor 返回指定服务器的当前会话 ID，线程安全。
func (c *Client) sessionFor(serverName string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessions[serverName]
}

// protocolFor 返回指定服务器协商后的协议版本，未协商时返回默认版本。
func (c *Client) protocolFor(serverName string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if protocol := c.protocols[serverName]; protocol != "" {
		return protocol
	}
	return c.protocolVersion
}

// requestName 从 RPC 请求参数中提取资源名称（tools/call 的 name 或 resources/read 的 uri）。
func requestName(method string, params any) string {
	if method != "tools/call" && method != "resources/read" && method != "prompts/get" {
		return ""
	}
	paramMap, ok := params.(map[string]any)
	if !ok {
		return ""
	}
	for _, key := range []string{"name", "uri"} {
		if value, ok := paramMap[key].(string); ok {
			return value
		}
	}
	return ""
}

// decodeRPCResponse 从 HTTP 响应中解码 JSON-RPC 响应，自动处理 SSE text/event-stream 格式。
func decodeRPCResponse(resp *http.Response) (rpcResponse, error) {
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return rpcResponse{}, err
	}
	if strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
		raw = firstSSEData(raw)
	}
	var out rpcResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return rpcResponse{}, err
	}
	return out, nil
}

// firstSSEData 从 SSE 事件流中提取首个 data 段的内容。
func firstSSEData(raw []byte) []byte {
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	var data []string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" && len(data) > 0 {
			break
		}
		if strings.HasPrefix(line, "data:") {
			data = append(data, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	return []byte(strings.Join(data, "\n"))
}

// validHeaderName 验证 HTTP 头名称是否仅包含合法可见 ASCII 字符且不含冒号。
func validHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if r < 33 || r > 126 || r == ':' {
			return false
		}
	}
	return true
}

// encodeHeaderValue 对 HTTP 头值进行编码，若含不合法字符则使用 Base64 编码。
func encodeHeaderValue(value string) string {
	if safeHeaderValue(value) {
		return value
	}
	return "=?base64?" + base64.StdEncoding.EncodeToString([]byte(value)) + "?="
}

// safeHeaderValue 检查字符串是否可作为 HTTP 头值直接使用（仅含可见 ASCII 且无首尾空白）。
func safeHeaderValue(value string) bool {
	if strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if r == '\t' || r == ' ' {
			continue
		}
		if r < 33 || r > 126 {
			return false
		}
	}
	return true
}
