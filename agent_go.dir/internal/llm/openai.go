package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
)

// openAIChatModel 基于 net/http 实现的 OpenAI 兼容 ChatModel，满足 Eino BaseChatModel 接口。
type openAIChatModel struct {
	apiKey  string       // apiKey API 密钥。
	baseURL string       // baseURL API 网关地址。
	model   string       // model 模型名称。
	http    *http.Client // http HTTP 客户端。
}

// openAIChatRequest OpenAI Chat Completions API 请求体。
type openAIChatRequest struct {
	Model          string              `json:"model"`
	Messages       []openAIMessage     `json:"messages"`
	Temperature    float32             `json:"temperature,omitempty"`
	MaxTokens      int                 `json:"max_tokens,omitempty"`
	ResponseFormat *openAIResponseFmt  `json:"response_format,omitempty"`
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIResponseFmt struct {
	Type string `json:"type"`
}

// openAIChatResponse OpenAI Chat Completions API 响应体。
type openAIChatResponse struct {
	Choices []struct {
		Message openAIMessage `json:"message"`
	} `json:"choices"`
	Error *openAIError `json:"error,omitempty"`
}

type openAIError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

// newOpenAIChatModel 创建 OpenAI 兼容的 ChatModel 实例。
func newOpenAIChatModel(apiKey, baseURL, modelName string) *openAIChatModel {
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	m := new(openAIChatModel)
	m.apiKey = apiKey
	m.baseURL = baseURL
	m.model = modelName
	m.http = new(http.Client)
	m.http.Timeout = 60 * time.Second
	return m
}

// Generate 调用 OpenAI Chat Completions API 生成一条回复消息。
// 实现 model.BaseChatModel 接口。
func (m *openAIChatModel) Generate(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	options := model.GetCommonOptions(&model.Options{}, opts...)

	reqURL := m.baseURL + "/chat/completions"

	messages := make([]openAIMessage, len(input))
	for i, msg := range input {
		messages[i] = openAIMessage{Role: string(msg.Role), Content: msg.Content}
	}

	body := new(openAIChatRequest)
	body.Model = m.model
	body.Messages = messages

	if options.Temperature != nil {
		body.Temperature = *options.Temperature
	}
	if options.MaxTokens != nil {
		body.MaxTokens = *options.MaxTokens
	}

	reqBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("openai marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("openai request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+m.apiKey)

	resp, err := m.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openai call: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("openai read: %w", err)
	}

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("openai %d: %s", resp.StatusCode, string(respBytes))
	}

	var chatResp openAIChatResponse
	if err := json.Unmarshal(respBytes, &chatResp); err != nil {
		return nil, fmt.Errorf("openai decode: %w", err)
	}
	if chatResp.Error != nil {
		return nil, fmt.Errorf("openai error: %s", chatResp.Error.Message)
	}
	if len(chatResp.Choices) == 0 {
		return nil, fmt.Errorf("openai returned no choices")
	}

	return schema.AssistantMessage(chatResp.Choices[0].Message.Content, nil), nil
}

// Stream 返回一个流式读取器，逐块产生消息内容。
// 实现 model.BaseChatModel 接口。
func (m *openAIChatModel) Stream(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, fmt.Errorf("openai stream not yet implemented")
}
