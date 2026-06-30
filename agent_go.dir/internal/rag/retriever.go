package rag

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Document 表示一条从知识库中检索到的文档。
type Document struct {
	ID      string  `json:"id"`      // ID 文档唯一标识。
	Source  string  `json:"source"`  // Source 文档来源（如 elasticsearch/milvus）。
	Content string  `json:"content"` // Content 文档文本内容。
	Score   float64 `json:"score"`   // Score 检索相关性得分。
}

// AccessScope 携带用户身份信息，用于权限过滤。
type AccessScope struct {
	UserID string   // UserID 用户唯一标识。
	Roles  []string // Roles 用户角色列表。
}

// Retriever 定义统一的检索接口。
type Retriever interface {
	Name() string                                                                          // Name 返回检索器名称。
	Retrieve(ctx context.Context, query string, limit int, scope AccessScope) ([]Document, error) // Retrieve 执行检索并返回文档列表。
}

// ElasticsearchRetriever 通过 Elasticsearch 执行词汇搜索，支持权限过滤。
type ElasticsearchRetriever struct {
	Addr   string // Addr ES 服务地址。
	Index  string // Index ES 索引名称。
	APIKey string // APIKey ES API 密钥。
}

// Name 返回检索器名称为 "elasticsearch"。
func (e *ElasticsearchRetriever) Name() string { return "elasticsearch" }

// Retrieve 对 ES 执行 multi_match 查询，结合分词与权限过滤，返回排序后的文档列表。
func (e *ElasticsearchRetriever) Retrieve(ctx context.Context, query string, limit int, scope AccessScope) ([]Document, error) {
	if e.Addr == "" || e.Index == "" {
		return nil, nil
	}
	tokens := tokenizeQuery(query)
	shouldClauses := make([]map[string]interface{}, 0, len(tokens)+1)
	for _, tk := range tokens {
		shouldClauses = append(shouldClauses, map[string]interface{}{
			"multi_match": map[string]interface{}{
				"query": tk, "fields": []string{"title^3", "content"},
				"type": "best_fields", "operator": "or", "fuzziness": "AUTO",
			},
		})
	}
	shouldClauses = append(shouldClauses, map[string]interface{}{
		"multi_match": map[string]interface{}{
			"query": query, "fields": []string{"title^2", "content"},
		},
	})

	shouldFilter := make([]map[string]interface{}, 0, 2)
	if strings.TrimSpace(scope.UserID) != "" {
		shouldFilter = append(shouldFilter, map[string]interface{}{
			"term": map[string]interface{}{"allowed_users.keyword": scope.UserID},
		})
	}
	if len(scope.Roles) > 0 {
		shouldFilter = append(shouldFilter, map[string]interface{}{
			"terms": map[string]interface{}{"allowed_roles.keyword": scope.Roles},
		})
	}

	boolQuery := map[string]interface{}{
		"should": shouldClauses, "minimum_should_match": 1,
	}
	if len(shouldFilter) > 0 {
		boolQuery["filter"] = []map[string]interface{}{{
			"bool": map[string]interface{}{
				"should": shouldFilter, "minimum_should_match": 1,
			},
		}}
	}

	body := map[string]interface{}{
		"size":  limit,
		"query": map[string]interface{}{"bool": boolQuery},
	}
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/%s/_search", strings.TrimRight(e.Addr, "/"), e.Index), bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if e.APIKey != "" {
		req.Header.Set("Authorization", "ApiKey "+e.APIKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("es search failed: %s", string(raw))
	}

	var parsed struct {
		Hits struct {
			Hits []struct {
				ID     string  `json:"_id"`
				Score  float64 `json:"_score"`
				Source struct {
					Title   string `json:"title"`
					Content string `json:"content"`
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, err
	}

	out := make([]Document, 0, len(parsed.Hits.Hits))
	for _, h := range parsed.Hits.Hits {
		txt := strings.TrimSpace(h.Source.Title + "\n" + h.Source.Content)
		out = append(out, Document{ID: h.ID, Source: "elasticsearch", Content: txt, Score: h.Score})
	}
	return out, nil
}

// MilvusRetriever 通过 Milvus HTTP 适配器执行向量搜索。
type MilvusRetriever struct {
	Endpoint string // Endpoint Milvus HTTP 服务端点地址。
}

// Name 返回检索器名称为 "milvus"。
func (m *MilvusRetriever) Name() string { return "milvus" }

// Retrieve 向 Milvus HTTP 适配器发送查询请求，获取向量相似度最高的文档列表。
func (m *MilvusRetriever) Retrieve(ctx context.Context, query string, limit int, scope AccessScope) ([]Document, error) {
	if m.Endpoint == "" {
		return nil, nil
	}
	payload := map[string]interface{}{
		"query": query, "top_k": limit, "user_id": scope.UserID, "roles": scope.Roles,
	}
	b, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.Endpoint, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := new(http.Client)
	client.Timeout = 8 * time.Second
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("milvus adapter failed: %s", string(raw))
	}

	var docs []Document
	if err := json.NewDecoder(resp.Body).Decode(&docs); err != nil {
		return nil, err
	}
	for i := range docs {
		docs[i].Source = "milvus"
	}
	return docs, nil
}

// CombinedRetriever 并发调用多个检索器，并通过 RRF 算法融合排序结果。
type CombinedRetriever struct {
	Items []Retriever // Items 包含的检索器列表。
}

// Retrieve 并发调用所有检索器，使用 RRF 算法合并并排序结果。
func (c *CombinedRetriever) Retrieve(ctx context.Context, query string, limit int, scope AccessScope) ([]Document, error) {
	rankedLists := make([][]Document, 0, len(c.Items))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, r := range c.Items {
		retriever := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			docs, err := retriever.Retrieve(ctx, query, limit, scope)
			if err != nil || len(docs) == 0 {
				return
			}
			mu.Lock()
			rankedLists = append(rankedLists, docs)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return RRF(rank, rankedLists, limit), nil
}

// rank 计算 RRF 排序中的单条位置得分，参数 i 为在列表中的排名索引。
func rank(i int) float64 {
	return 1.0 / float64(60+i+1)
}

// RRF 使用 Reciprocal Rank Fusion 算法合并多个排序结果列表。
// scoring 为位置得分函数，lists 为多个排序列表，limit 为返回的最大文档数。
func RRF(scoring func(int) float64, lists [][]Document, limit int) []Document {
	type scoreDoc struct {
		doc   Document
		score float64
	}
	acc := map[string]*scoreDoc{}
	for _, list := range lists {
		for i, doc := range list {
			key := doc.Source + ":" + doc.ID
			if _, ok := acc[key]; !ok {
				sd := new(scoreDoc)
			sd.doc = doc
			acc[key] = sd
			}
			acc[key].score += scoring(i)
		}
	}
	final := make([]scoreDoc, 0, len(acc))
	for _, item := range acc {
		item.doc.Score = item.score
		final = append(final, *item)
	}
	sort.Slice(final, func(i, j int) bool { return final[i].score > final[j].score })
	if len(final) > limit {
		final = final[:limit]
	}
	out := make([]Document, 0, len(final))
	for _, f := range final {
		out = append(out, f.doc)
	}
	return out
}

// tokenizeQuery 对查询字符串进行分词，过滤短词和重复词，返回词元列表。
func tokenizeQuery(query string) []string {
	query = strings.TrimSpace(strings.ToLower(query))
	if query == "" {
		return nil
	}
	var tokens []string
	seen := map[string]bool{}
	for _, part := range strings.FieldsFunc(query, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r >= 0x4e00 && r <= 0x9fff || r == '_')
	}) {
		part = strings.TrimSpace(part)
		if len([]rune(part)) < 2 {
			continue
		}
		if seen[part] {
			continue
		}
		seen[part] = true
		tokens = append(tokens, part)
	}
	return tokens
}
