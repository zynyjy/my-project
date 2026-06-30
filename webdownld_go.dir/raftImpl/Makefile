.PHONY: build test proto

build:
	go build -o bin/raftnode ./cmd/raftnode

test:
	go test ./... -count=1

# 需已安装 protoc、protoc-gen-go、protoc-gen-go-grpc（且在 PATH）
proto:
	protoc --proto_path=. \
		--go_out=. --go_opt=module=github.com/raftimpl/mini \
		--go-grpc_out=. --go-grpc_opt=module=github.com/raftimpl/mini \
		api/raft.proto
