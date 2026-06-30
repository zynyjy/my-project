package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config 网盘服务全部配置项。
type Config struct {
	AppAddr                  string   // AppAddr HTTP 服务监听地址。
	DataRoot                 string   // DataRoot 本地数据根目录。
	StorageNodes             []string // StorageNodes 可用存储节点 ID 列表（兼容旧配置）。
	StorageNodeAddrs         []string // StorageNodeAddrs 存储节点 gRPC 地址列表。
	RaftHTTPNodes            []string // RaftHTTPNodes Raft HTTP 节点地址列表。
	MaxConcurrentChunkWrites int      // MaxConcurrentChunkWrites 服务端最大并发分片写入数。

	// 用户系统与安全配置。
	MySQLDSN            string        // MySQLDSN MySQL 数据库连接字符串。
	JWTSecret            string        // JWTSecret JWT 签名密钥。
	JWTAccessTokenTTL    time.Duration // JWTAccessTokenTTL 访问令牌有效期。
	JWTRefreshTokenTTL   time.Duration // JWTRefreshTokenTTL 刷新令牌有效期。

	// 消息队列配置。
	RabbitMQURL string // RabbitMQURL RabbitMQ 连接地址。

	// 支付宝支付配置。
	AlipayAppID        string // AlipayAppID 支付宝应用 ID。
	AlipayPrivateKey    string // AlipayPrivateKey 支付宝应用私钥。
	AlipayPublicKey     string // AlipayPublicKey 支付宝公钥。
	AlipayNotifyURL     string // AlipayNotifyURL 支付宝异步通知回调地址。
	AlipayReturnURL     string // AlipayReturnURL 支付宝支付完成同步跳转地址。
	AlipayIsProduction  bool   // AlipayIsProduction 是否为生产环境（false 为沙箱模式）。
}

// Load 从环境变量加载配置并填充默认值。
func Load() Config {
	cfg := Config{
		AppAddr:  env("APP_ADDR", ":8188"),
		DataRoot: env("DATA_ROOT", "./data"),
	}
	cfg.MaxConcurrentChunkWrites = int(envInt64("MAX_CONCURRENT_CHUNK_WRITES", 64))

	nodes := strings.Split(env("STORAGE_NODE_IDS", "node-a,node-b,node-c"), ",")
	for _, n := range nodes {
		n = strings.TrimSpace(n)
		if n != "" {
			cfg.StorageNodes = append(cfg.StorageNodes, n)
		}
	}

	storageAddrs := strings.Split(env("STORAGE_NODE_ADDRS", "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003"), ",")
	for _, addr := range storageAddrs {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			cfg.StorageNodeAddrs = append(cfg.StorageNodeAddrs, addr)
		}
	}

	raftNodes := strings.Split(env("RAFT_HTTP_NODES", "127.0.0.1:8080,127.0.0.1:8081,127.0.0.1:8082"), ",")
	for _, n := range raftNodes {
		n = strings.TrimSpace(n)
		if n != "" {
			cfg.RaftHTTPNodes = append(cfg.RaftHTTPNodes, n)
		}
	}

	// 用户系统配置。
	cfg.MySQLDSN = env("MYSQL_DSN", "root:root@tcp(127.0.0.1:3306)/cloudraft?parseTime=true&charset=utf8mb4")
	cfg.JWTSecret = env("JWT_SECRET", "cloudraft-jwt-secret-change-me-in-production")
	cfg.JWTAccessTokenTTL = time.Duration(envInt64("JWT_ACCESS_TTL_HOURS", 2)) * time.Hour
	cfg.JWTRefreshTokenTTL = time.Duration(envInt64("JWT_REFRESH_TTL_DAYS", 7)) * 24 * time.Hour

	// 消息队列配置。
	cfg.RabbitMQURL = env("RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/")

	// 支付宝配置。
	cfg.AlipayAppID = env("ALIPAY_APP_ID", "")
	cfg.AlipayPrivateKey = env("ALIPAY_PRIVATE_KEY", "")
	cfg.AlipayPublicKey = env("ALIPAY_PUBLIC_KEY", "")
	cfg.AlipayNotifyURL = env("ALIPAY_NOTIFY_URL", "https://your-domain.com/api/payment/notify")
	cfg.AlipayReturnURL = env("ALIPAY_RETURN_URL", "https://your-domain.com/payment/return")
	cfg.AlipayIsProduction = env("ALIPAY_IS_PRODUCTION", "false") == "true"

	return cfg
}

// env 读取环境变量，若为空则返回默认值。
func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// ChunkSize 读取分片大小配置（MB）并转换为字节。
func ChunkSize() int64 {
	v := env("CHUNK_SIZE_MB", "4")
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 4 * 1024 * 1024
	}
	return int64(n) * 1024 * 1024
}

// envInt64 读取 int64 环境变量，若无效则返回默认值。
func envInt64(key string, def int64) int64 {
	v := env(key, strconv.FormatInt(def, 10))
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
