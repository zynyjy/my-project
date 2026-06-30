package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"agent_go/internal/agent"
	"agent_go/internal/chat"
	"agent_go/internal/monitor"
	"agent_go/internal/rag"
	"agent_go/internal/repair"
	"agent_go/internal/sensitive"
	"agent_go/internal/web"
)

// main 是程序入口，初始化智能体管理器、RAG 检索引擎、聊天服务、
// 修复智能体、进程监控智能体及 Web 服务器，并监听终止信号实现优雅退出。
func main() {
	manager := agent.NewManager()

	es := new(rag.ElasticsearchRetriever)
	es.Addr = os.Getenv("ES_ADDR")
	es.Index = os.Getenv("ES_INDEX")
	es.APIKey = os.Getenv("ES_API_KEY")
	milvus := new(rag.MilvusRetriever)
	milvus.Endpoint = os.Getenv("MILVUS_HTTP_ENDPOINT")

	chatSvc, err := chat.NewService(manager, es, milvus)
	if err != nil {
		log.Fatalf("init chat service failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repairAgent := repair.NewAgent(manager)
	go repairAgent.Start(ctx)

	processAgent := monitor.NewProcessAgent(manager, 5*time.Second)
	go processAgent.Start(ctx)

	// 注册监控历史环形缓冲区，供折线图 API 使用。
	manager.SetMonitorHistory(processAgent.History())

	// 初始化敏感词过滤器。
	sensitiveFilter := sensitive.NewFilter()

	server := web.NewServer(manager, chatSvc, sensitiveFilter).Router()
	addr := ":8090"
	if v := os.Getenv("APP_ADDR"); v != "" {
		addr = v
	}

	go func() {
		log.Printf("server listening on %s", addr)
		if err := server.Run(addr); err != nil {
			log.Fatalf("server run failed: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	cancel()
	manager.Stop()
	log.Println("server stopped")
}
