// Package sensitive 提供可配置的敏感词检测与过滤功能。
package sensitive

import (
	"fmt"
	"os"
	"strings"
	"sync"
)

// Filter 存储敏感词集合与短语列表，提供检测、替换和安全性校验方法。
type Filter struct {
	words       map[string]bool // words 精确匹配的敏感词集合（已小写化）。
	phrases     []string        // phrases 多词短语，通过子串匹配检测。
	replacement string          // replacement 替换字符，默认 "***"。
	enabled     bool            // enabled 是否启用过滤。
	mu          sync.RWMutex    // mu 保护 words 和 phrases 的并发访问。
}

// NewFilter 从环境变量加载敏感词配置并创建过滤器实例。
// SENSITIVE_WORDS 接受逗号分隔的词列表；SENSITIVE_WORDS_FILE 接受文件路径（每行一个词）。
// SENSITIVE_FILTER_ENABLED 控制是否启用过滤，默认为 true。
func NewFilter() *Filter {
	f := new(Filter)
	f.enabled = strings.ToLower(strings.TrimSpace(os.Getenv("SENSITIVE_FILTER_ENABLED"))) != "false"
	f.replacement = "***"
	f.words = make(map[string]bool)

	// 从环境变量加载逗号分隔的敏感词列表。
	if raw := strings.TrimSpace(os.Getenv("SENSITIVE_WORDS")); raw != "" {
		for _, w := range strings.Split(raw, ",") {
			w = strings.TrimSpace(strings.ToLower(w))
			if w == "" {
				continue
			}
			if strings.Contains(w, " ") {
				f.phrases = append(f.phrases, w)
			} else {
				f.words[w] = true
			}
		}
	}

	// 从文件加载敏感词列表（每行一个词，# 开头为注释）。
	if filePath := strings.TrimSpace(os.Getenv("SENSITIVE_WORDS_FILE")); filePath != "" {
		data, err := os.ReadFile(filePath)
		if err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				w := strings.TrimSpace(strings.ToLower(line))
				if w == "" || strings.HasPrefix(w, "#") {
					continue
				}
				if strings.Contains(w, " ") {
					f.phrases = append(f.phrases, w)
				} else {
					f.words[w] = true
				}
			}
		}
	}
	return f
}

// IsEnabled 返回过滤器是否处于启用状态。
func (f *Filter) IsEnabled() bool {
	return f.enabled
}

// Match 检查文本是否包含任何敏感词或短语。
// 返回是否命中以及命中的第一个敏感词。
func (f *Filter) Match(text string) (bool, string) {
	if !f.enabled {
		return false, ""
	}
	lower := strings.ToLower(text)

	// 检查短语（更长的优先匹配）。
	for _, phrase := range f.phrases {
		if strings.Contains(lower, phrase) {
			return true, phrase
		}
	}

	// 检查单个词（按词边界分割后精确匹配）。
	words := tokenizeText(lower)
	for _, word := range words {
		if f.words[word] {
			return true, word
		}
	}
	return false, ""
}

// Replace 将文本中的所有敏感词替换为 ***。
func (f *Filter) Replace(text string) string {
	if !f.enabled {
		return text
	}
	result := text
	lower := strings.ToLower(text)

	// 先替换短语。
	for _, phrase := range f.phrases {
		for {
			idx := strings.Index(strings.ToLower(result), phrase)
			if idx < 0 {
				break
			}
			result = result[:idx] + strings.Repeat("*", len(phrase)) + result[idx+len(phrase):]
		}
	}

	// 再替换单个词。
	words := tokenizeText(lower)
	for _, word := range words {
		if f.words[word] {
			result = strings.ReplaceAll(result, word, f.replacement)
		}
	}
	return result
}

// IsSafe 检查文本是否安全，安全时返回 nil，否则返回包含命中词的错误。
func (f *Filter) IsSafe(text string) error {
	hit, word := f.Match(text)
	if hit {
		return fmt.Errorf("输入包含敏感内容: %s", word)
	}
	return nil
}

// tokenizeText 将文本按非字母数字边界分割为词元列表，用于匹配单个敏感词。
func tokenizeText(text string) []string {
	var tokens []string
	var current strings.Builder
	for _, r := range text {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			current.WriteRune(r)
		} else {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		}
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}
