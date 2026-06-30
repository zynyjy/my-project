package rag

import (
	"testing"
)

func TestRRFEmpty(t *testing.T) {
	result := RRF(rank, nil, 10)
	if len(result) != 0 {
		t.Fatalf("expected empty, got %d", len(result))
	}
}

func TestRRFSingleList(t *testing.T) {
	docs := []Document{
		{ID: "a", Source: "elasticsearch", Content: "a", Score: 1.0},
		{ID: "b", Source: "elasticsearch", Content: "b", Score: 0.8},
	}
	result := RRF(rank, [][]Document{docs}, 10)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0].ID != "a" {
		t.Errorf("expected a first, got %s", result[0].ID)
	}
}

func TestRRFMerge(t *testing.T) {
	docsV := []Document{
		{ID: "a", Source: "milvus", Content: "a"},
		{ID: "b", Source: "milvus", Content: "b"},
	}
	docsT := []Document{
		{ID: "c", Source: "elasticsearch", Content: "c"},
		{ID: "d", Source: "elasticsearch", Content: "d"},
	}
	result := RRF(rank, [][]Document{docsV, docsT}, 10)
	if len(result) != 4 {
		t.Fatalf("expected 4, got %d", len(result))
	}
	if result[0].ID != "a" {
		t.Errorf("expected a first, got %s", result[0].ID)
	}
}

func TestRRFTopN(t *testing.T) {
	docs := []Document{
		{ID: "a", Source: "milvus", Content: "a"},
		{ID: "b", Source: "milvus", Content: "b"},
		{ID: "c", Source: "milvus", Content: "c"},
	}
	result := RRF(rank, [][]Document{docs}, 2)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
}

func TestTokenizeQuery(t *testing.T) {
	tokens := tokenizeQuery("Kubernetes 高可用部署 Helm Chart")
	if len(tokens) < 3 {
		t.Errorf("expected >= 3 tokens, got %d: %v", len(tokens), tokens)
	}
}
