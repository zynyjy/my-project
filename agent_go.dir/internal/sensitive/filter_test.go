package sensitive

import (
	"os"
	"testing"
)

// TestNewFilterEmpty 测试空配置时创建过滤器。
func TestNewFilterEmpty(t *testing.T) {
	os.Unsetenv("SENSITIVE_WORDS")
	os.Unsetenv("SENSITIVE_WORDS_FILE")
	os.Unsetenv("SENSITIVE_FILTER_ENABLED")
	f := NewFilter()
	if !f.IsEnabled() {
		t.Error("expected filter enabled by default")
	}
	if hit, _ := f.Match("hello world"); hit {
		t.Error("empty filter should not match anything")
	}
}

// TestFilterMatchSingleWord 测试单词语配。
func TestFilterMatchSingleWord(t *testing.T) {
	t.Setenv("SENSITIVE_WORDS", "badword,evil")
	f := NewFilter()
	if hit, word := f.Match("this contains badword here"); !hit {
		t.Error("expected match for badword")
	} else if word != "badword" {
		t.Errorf("expected badword, got %s", word)
	}
}

// TestFilterMatchPhrase 测试多词短语匹配。
func TestFilterMatchPhrase(t *testing.T) {
	t.Setenv("SENSITIVE_WORDS", "hello world")
	f := NewFilter()
	if hit, phrase := f.Match("say hello world today"); !hit {
		t.Error("expected match for phrase")
	} else if phrase != "hello world" {
		t.Errorf("expected 'hello world', got %s", phrase)
	}
}

// TestFilterNoMatch 测试正常文本不触发匹配。
func TestFilterNoMatch(t *testing.T) {
	t.Setenv("SENSITIVE_WORDS", "badword")
	f := NewFilter()
	if hit, _ := f.Match("this is clean text"); hit {
		t.Error("expected no match for clean text")
	}
}

// TestFilterCaseInsensitive 测试大小写不敏感匹配。
func TestFilterCaseInsensitive(t *testing.T) {
	t.Setenv("SENSITIVE_WORDS", "BadWord")
	f := NewFilter()
	if hit, _ := f.Match("this has BADWORD inside"); !hit {
		t.Error("expected case-insensitive match")
	}
}

// TestFilterReplace 测试敏感词替换功能。
func TestFilterReplace(t *testing.T) {
	t.Setenv("SENSITIVE_WORDS", "badword")
	f := NewFilter()
	result := f.Replace("this badword is here")
	if result == "this badword is here" {
		t.Error("expected replacement to occur")
	}
}

// TestFilterIsSafe 测试 IsSafe 校验方法。
func TestFilterIsSafe(t *testing.T) {
	t.Setenv("SENSITIVE_WORDS", "badword")
	f := NewFilter()
	if err := f.IsSafe("clean text"); err != nil {
		t.Errorf("clean text should be safe: %v", err)
	}
	if err := f.IsSafe("contains badword!"); err == nil {
		t.Error("expected error for sensitive text")
	}
}

// TestFilterDisabled 测试禁用过滤器。
func TestFilterDisabled(t *testing.T) {
	t.Setenv("SENSITIVE_FILTER_ENABLED", "false")
	t.Setenv("SENSITIVE_WORDS", "badword")
	f := NewFilter()
	if f.IsEnabled() {
		t.Error("expected filter disabled")
	}
	if hit, _ := f.Match("badword"); hit {
		t.Error("disabled filter should not match")
	}
}
