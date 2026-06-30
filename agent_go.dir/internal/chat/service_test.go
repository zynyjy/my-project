package chat

import (
	"testing"
)

func TestParseReactDecisionFinal(t *testing.T) {
	raw := `{"thought": "info is sufficient", "action": "final", "answer": "Hello world"}`
	dec, err := parseReactDecision(raw)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if dec.Action != "final" {
		t.Errorf("expected final, got %s", dec.Action)
	}
	if dec.Answer != "Hello world" {
		t.Errorf("expected answer, got %s", dec.Answer)
	}
}

func TestParseReactDecisionTool(t *testing.T) {
	raw := `{"thought": "need data", "action": "mcp_tool", "tool": "server.get_metrics", "tool_input": {"host": "prod-1"}}`
	dec, err := parseReactDecision(raw)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if dec.Action != "mcp_tool" {
		t.Errorf("expected mcp_tool, got %s", dec.Action)
	}
	if dec.Tool != "server.get_metrics" {
		t.Errorf("expected tool name, got %s", dec.Tool)
	}
}

func TestParseReactDecisionMarkdown(t *testing.T) {
	raw := "```json\n{\"action\": \"final\", \"answer\": \"test\"}\n```"
	dec, err := parseReactDecision(raw)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if dec.Action != "final" {
		t.Errorf("expected final, got %s", dec.Action)
	}
}

func TestParseReactDecisionImplicitFinal(t *testing.T) {
	raw := `{"answer": "some answer without explicit action"}`
	dec, err := parseReactDecision(raw)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if dec.Action != "final" {
		t.Errorf("expected implicit final, got %s", dec.Action)
	}
}

func TestParseReactDecisionInvalid(t *testing.T) {
	_, err := parseReactDecision("not json at all")
	if err == nil {
		t.Error("expected error for invalid input")
	}
}

func TestNormalizeAction(t *testing.T) {
	tests := map[string]string{
		"mcp_tool":     "mcp_tool",
		"tool":         "mcp_tool",
		"call_tool":    "mcp_tool",
		"mcp":          "mcp_tool",
		"final":        "final",
		"answer":       "final",
		"final_answer": "final",
		"finish":       "final",
	}
	for input, expected := range tests {
		if got := normalizeAction(input); got != expected {
			t.Errorf("normalizeAction(%q) = %q, want %q", input, got, expected)
		}
	}
}

func TestExtractJSONObject(t *testing.T) {
	if got := extractJSONObject("```json\n{\"a\":1}\n```"); got != "{\"a\":1}" {
		t.Errorf("expected stripped markdown, got %s", got)
	}
	if got := extractJSONObject("no json here"); got != "" {
		t.Errorf("expected empty, got %s", got)
	}
}

func TestNormalizeRoles(t *testing.T) {
	roles := normalizeRoles([]string{"admin", "", "admin", " viewer "})
	if len(roles) != 2 {
		t.Fatalf("expected 2, got %d: %v", len(roles), roles)
	}
}
