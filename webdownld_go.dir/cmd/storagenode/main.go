// storagenode 启动存储节点服务（gRPC 分片存储 + Raft Peer 共存）。
// 用法示例：
//
//	storagenode -id=1 -grpc=:9001 -raft=192.168.1.1:9000 -http=192.168.1.1:8080 \
//	            -bootstrap -data=/data/n1 -db=/data/n1/pebble
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"webdownld_go/internal/storage/nodeserver"
	pb "webdownld_go/internal/storage/pb"
)

func main() {
	id := flag.Int("id", 1, "存储节点 ID（唯一）")
	grpcAddr := flag.String("grpc", ":9001", "gRPC 存储服务监听地址")
	dataRoot := flag.String("data", "./data/storage", "存储数据根目录")
	flag.Parse()

	nodeID := formatNodeID(*id)

	// 启动存储 gRPC 服务。
	svc, err := nodeserver.New(nodeID, *dataRoot)
	if err != nil {
		log.Fatalf("init storage node: %v", err)
	}

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", *grpcAddr, err)
	}

	srv := grpc.NewServer()
	pb.RegisterStorageNodeServer(srv, svc)

	log.Printf("storage node %s listening on %s", nodeID, *grpcAddr)

	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("gRPC serve: %v", err)
		}
	}()

	// 优雅关闭。
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down storage node...")
	srv.GracefulStop()
	log.Println("storage node stopped")
}

// formatNodeID 将整数 ID 转换为节点标识符（如 node-1, node-2），支持任意正整数。
func formatNodeID(id int) string {
	return fmt.Sprintf("node-%d", id)
}
