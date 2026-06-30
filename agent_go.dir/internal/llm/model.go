// Package llm 提供基于字节 Eino 框架的 LLM 客户端抽象层。
package llm

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
)

// Client 包装 Eino BaseChatModel，提供统一的 LLM 调用接口。
type Client struct {
	chatModel model.BaseChatModel // chatModel Eino 聊天模型实例。
}

// NewClient 从环境变量创建 LLM 客户端。
// OPENAI_API_KEY 未配置时返回 nil（demo 回退模式）。
func NewClient() (*Client, error) {
	apiKey := strings.TrimSpace(os.Getenv("OPENAI_API_KEY"))
	if apiKey == "" {
		return nil, nil
	}

	baseURL := strings.TrimSpace(os.Getenv("OPENAI_BASE_URL"))
	modelName := strings.TrimSpace(os.Getenv("OPENAI_MODEL"))
	if modelName == "" {
		modelName = "gpt-4o-mini"
	}

	// 使用自研 OpenAI 兼容 ChatModel 实现。
	chatModel := newOpenAIChatModel(apiKey, baseURL, modelName)

	c := new(Client)
	c.chatModel = chatModel
	return c, nil
}

// Generate 调用 LLM 生成回复，返回响应文本。
func (c *Client) Generate(ctx context.Context, messages []*schema.Message, opts ...model.Option) (string, error) {
	if c == nil || c.chatModel == nil {
		return "", fmt.Errorf("llm client: model not configured")
	}
	msg, err := c.chatModel.Generate(ctx, messages, opts...)
	if err != nil {
		return "", err
	}
	return msg.Content, nil
}

// GenerateJSON 以 JSON 模式调用 LLM，返回原始 JSON 响应文本。
// 通过 system prompt 指示模型返回纯 JSON 实现。
func (c *Client) GenerateJSON(ctx context.Context, messages []*schema.Message) (string, error) {
	if c == nil || c.chatModel == nil {
		return "", fmt.Errorf("llm client: model not configured")
	}
	msg, err := c.chatModel.Generate(ctx, messages)
	if err != nil {
		return "", err
	}
	return msg.Content, nil
}
