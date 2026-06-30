// raftnode 启动手写 Raft 节点 + Pebble KV + HTTP 调试接口。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/gin-gonic/gin"
	"github.com/raftimpl/mini/internal/raft"
)

// joinReq 集群加入请求体，包含新节点的 ID 和 Raft RPC 地址。
type joinReq struct {
	NodeID   int    `json:"node_id"`   // NodeID 新加入节点的唯一 ID。
	NodeAddr string `json:"node_addr"` // NodeAddr 新节点的 Raft RPC 监听地址。
}

// kvItem KV 键值对，用于 HTTP API 序列化。
type kvItem struct {
	Key   string `json:"key"`   // Key 键名。
	Value string `json:"value"` // Value 键值。
}

func main() {
	nodeID := flag.Int("id", 1, "节点 ID（必须唯一）")
	raftAddr := flag.String("raft", "127.0.0.1:9000", "本机 Raft RPC 监听地址")
	httpAddr := flag.String("http", "127.0.0.1:8080", "HTTP 调试地址")
	persist := flag.String("persist", "", "持久化文件路径，空则仅内存")
	bootstrap := flag.Bool("bootstrap", false, "是否自举为首个节点")
	joinLeader := flag.String("join", "", "leader HTTP 地址，例如 127.0.0.1:8080")
	dbPath := flag.String("db", "", "Pebble 数据目录，默认 ./data-<id>")
	flag.Parse()

	if *nodeID <= 0 {
		log.Fatal("-id 必须 > 0")
	}
	if *bootstrap && strings.TrimSpace(*joinLeader) != "" {
		log.Fatal("-bootstrap 与 -join 互斥")
	}
	if !*bootstrap && strings.TrimSpace(*joinLeader) == "" {
		log.Fatal("非 bootstrap 节点必须提供 -join")
	}
	if *dbPath == "" {
		*dbPath = "./data-" + strconv.Itoa(*nodeID)
	}

	applyCh := make(chan raft.ApplyMsg, 256)

	db, err := pebble.Open(*dbPath, &pebble.Options{})
	if err != nil {
		log.Fatalf("open pebble failed: %v", err)
	}
	defer db.Close()

	go func() {
		for msg := range applyCh {
			deferApply := func() {
				if msg.Done != nil {
					close(msg.Done)
				}
			}
			switch msg.Op.Type {
			case "put":
				if err := db.Set([]byte(msg.Op.Key), []byte(msg.Op.Value), pebble.Sync); err != nil {
					log.Printf("pebble set failed: %v", err)
				}
			case "del":
				if err := db.Delete([]byte(msg.Op.Key), pebble.Sync); err != nil {
					log.Printf("pebble delete failed: %v", err)
				}
			default:
			}
			deferApply()
		}
	}()

	rf, err := raft.NewRaft(*nodeID, *raftAddr, *persist, applyCh)
	if err != nil {
		log.Fatal(err)
	}

	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery())
	router.GET("/status", func(c *gin.Context) {
		leaderID := rf.LeaderHint()
		leaderAddr, _ := rf.PeerAddr(leaderID)
		c.JSON(http.StatusOK, gin.H{
			"leader":      rf.IsLeader(),
			"term":        rf.CurrentTerm(),
			"id":          *nodeID,
			"raft":        *raftAddr,
			"leader_hint": gin.H{"id": leaderID, "addr": leaderAddr},
		})
	})
	router.GET("/kv/:key", func(c *gin.Context) {
		if !rf.IsLeader() {
			writeLeaderConflict(c, rf)
			return
		}
		key := c.Param("key")
		val, closer, err := db.Get([]byte(key))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				c.Status(http.StatusNotFound)
				return
			}
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
		defer closer.Close()
		c.Data(http.StatusOK, "text/plain", append([]byte(nil), val...))
	})
	router.GET("/kv-prefix/*prefix", func(c *gin.Context) {
		if !rf.IsLeader() {
			writeLeaderConflict(c, rf)
			return
		}
		prefix := strings.TrimPrefix(c.Param("prefix"), "/")
		items, err := scanPrefix(db, prefix)
		if err != nil {
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
		c.JSON(http.StatusOK, gin.H{"items": items})
	})
	router.PUT("/kv/:key", func(c *gin.Context) {
		if !rf.IsLeader() {
			writeLeaderConflict(c, rf)
			return
		}
		key := c.Param("key")
		b, _ := io.ReadAll(c.Request.Body)
		op := raft.Op{Type: "put", Key: key, Value: string(b)}
		if _, err := rf.Submit(op); err != nil {
			c.String(http.StatusServiceUnavailable, "submit failed: %v", err)
			return
		}
		c.Status(http.StatusNoContent)
	})
	router.DELETE("/kv/:key", func(c *gin.Context) {
		if !rf.IsLeader() {
			writeLeaderConflict(c, rf)
			return
		}
		key := c.Param("key")
		op := raft.Op{Type: "del", Key: key}
		if _, err := rf.Submit(op); err != nil {
			c.String(http.StatusServiceUnavailable, "submit failed: %v", err)
			return
		}
		c.Status(http.StatusNoContent)
	})
	router.POST("/cluster/join", func(c *gin.Context) {
		var req joinReq
		if err := c.ShouldBindJSON(&req); err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		if rf.IsLeader() {
			if err := rf.JoinNode(req.NodeID, req.NodeAddr); err != nil {
				c.String(http.StatusServiceUnavailable, err.Error())
				return
			}
			c.Status(http.StatusNoContent)
			return
		}
		leaderID := rf.LeaderHint()
		c.JSON(http.StatusConflict, gin.H{
			"error":     "not leader",
			"leader_id": leaderID,
		})
	})

	srv := &http.Server{Addr: *httpAddr, Handler: router}
	go func() {
		log.Printf("HTTP %s (Raft RPC %s, id=%d)", *httpAddr, *raftAddr, *nodeID)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	if !*bootstrap {
		registerSelf(*joinLeader, *nodeID, *raftAddr)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutdown...")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	rf.Stop()
}

// registerSelf 向 Leader 的 HTTP 接口发送集群加入请求，做指数重试直到成功或 25 秒超时。
func registerSelf(leaderHTTP string, nodeID int, raftAddr string) {
	req := joinReq{NodeID: nodeID, NodeAddr: raftAddr}
	b, _ := json.Marshal(req)
	url := "http://" + strings.TrimSpace(leaderHTTP) + "/cluster/join"
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := http.Post(url, "application/json", strings.NewReader(string(b)))
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				log.Printf("join cluster success via %s", url)
				return
			}
		}
		if time.Now().After(deadline) {
			log.Fatalf("join cluster timeout via %s", url)
		}
		time.Sleep(700 * time.Millisecond)
	}
}

// writeLeaderConflict 当收到非 Leader 请求时，返回 409 冲突响应并附带 Leader 提示信息。
func writeLeaderConflict(c *gin.Context, rf *raft.Raft) {
	leaderID := rf.LeaderHint()
	leaderRaftAddr, _ := rf.PeerAddr(leaderID)
	c.JSON(http.StatusConflict, gin.H{
		"error":            "not leader",
		"leader_id":        leaderID,
		"leader_raft_addr": leaderRaftAddr,
	})
}

// scanPrefix 按前缀扫描 Pebble 中的 KV 对，用于实现 ListPrefix 接口。
func scanPrefix(db *pebble.DB, prefix string) ([]kvItem, error) {
	lower := []byte(prefix)
	upper := prefixUpperBound(lower)
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	items := []kvItem{}
	for ok := iter.First(); ok; ok = iter.Next() {
		items = append(items, kvItem{
			Key:   string(append([]byte(nil), iter.Key()...)),
			Value: string(append([]byte(nil), iter.Value()...)),
		})
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return items, nil
}

// prefixUpperBound 计算给定前缀在字典序上的上界（最后一个字节 +1），用于 Pebble 范围扫描。
func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	upper := append([]byte(nil), prefix...)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 0xff {
			upper[i]++
			return upper[:i+1]
		}
	}
	return nil
}
