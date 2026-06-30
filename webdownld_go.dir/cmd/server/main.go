package main

import (
	"context"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"webdownld_go/internal/api"
	"webdownld_go/internal/auth"
	"webdownld_go/internal/config"
	"webdownld_go/internal/meta"
	"webdownld_go/internal/mq"
	"webdownld_go/internal/payment"
	"webdownld_go/internal/storage"
	"webdownld_go/internal/store"

	"github.com/gin-gonic/gin"
)

// main 启动网盘服务：加载配置、初始化存储层、用户系统、支付服务与事件总线后监听 HTTP。
func main() {
	// 初始化结构化日志。
	opts := new(slog.HandlerOptions)
	opts.Level = slog.LevelInfo
	logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
	slog.SetDefault(logger)

	cfg := config.Load()
	api.INFO("config loaded", "addr", cfg.AppAddr, "data_root", cfg.DataRoot)

	if err := os.MkdirAll(cfg.DataRoot, 0o755); err != nil {
		slog.Error("create data dir failed", "error", err)
		os.Exit(1)
	}

	// 初始化 MySQL 数据库连接（可选，不可用时跳过用户/支付功能）。
	var mysqlStore *store.MySQLStore
	var err error
	mysqlStore, err = store.NewMySQLStore(cfg.MySQLDSN)
	if err != nil {
		api.INFO("MySQL 不可用，跳过用户认证与支付功能", "error", err)
		mysqlStore = nil
	} else {
		defer mysqlStore.Close()
	}

	// 初始化 JWT 服务（MySQL 不可用时跳过）。
	var jwtService *auth.JWTService
	if mysqlStore != nil {
		jwtService = auth.NewJWTService(cfg.JWTSecret, cfg.JWTAccessTokenTTL, cfg.JWTRefreshTokenTTL)
	}

	// 初始化支付宝支付服务（若未配置则跳过）。
	var alipaySvc *payment.AlipayService
	if cfg.AlipayAppID != "" {
		alipaySvc, err = payment.NewAlipayService(
			cfg.AlipayAppID, cfg.AlipayPrivateKey, cfg.AlipayPublicKey,
			cfg.AlipayNotifyURL, cfg.AlipayReturnURL, cfg.AlipayIsProduction,
		)
		if err != nil {
			slog.Error("init alipay failed", "error", err)
			os.Exit(1)
		}
	}

	// 初始化 Topic 事件总线（内存版 RabbitMQ）。
	eventBus := mq.NewTopicExchange()
	eventBus.InitSubscriptions(func(event []byte) error {
		api.INFO("会员处理回调", "event", string(event))
		return nil
	})

	// 初始化元数据与存储层。
	metaSvc := meta.NewService(meta.NewRaftClient(cfg.RaftHTTPNodes))
	storageSvc, err := storage.NewService(cfg.StorageNodeAddrs)
	if err != nil {
		slog.Error("init storage failed", "error", err)
		os.Exit(1)
	}

	h := api.New(metaSvc, storageSvc, cfg.MaxConcurrentChunkWrites)

	// 初始化用户认证处理器（仅 MySQL 可用时）。
	var authHandler *api.AuthHandler
	if mysqlStore != nil && jwtService != nil {
		authHandler = api.NewAuthHandler(mysqlStore.DB, jwtService)
	}

	// 初始化支付处理器（仅 MySQL 可用时）。
	var paymentHandler *api.PaymentHandler
	if mysqlStore != nil && alipaySvc != nil {
		paymentHandler = api.NewPaymentHandler(mysqlStore.DB, alipaySvc, eventBus)
	}

	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(api.RequestIDMiddleware())
	r.Use(api.AccessLogMiddleware())
	r.Use(api.MetricsMiddleware())

	r.Static("/assets", "./web")
	r.GET("/", func(c *gin.Context) {
		c.File("./web/index.html")
	})

	// 健康检查与观测端点。
	r.GET("/healthz", healthzHandler)
	r.GET("/readyz", h.ReadyzHandler)

	// 注册各模块路由。
	if authHandler != nil {
		authHandler.Register(r)
	}
	if paymentHandler != nil {
		paymentHandler.Register(r)
	}
	h.Register(r)

	srv := new(http.Server)
	srv.Addr = cfg.AppAddr
	srv.Handler = r

	// 优雅关闭。
	go func() {
		api.INFO("webdownld server listening", "addr", cfg.AppAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	api.INFO("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("server shutdown error", "error", err)
	}
	h.Shutdown()
	storageSvc.Close()
	eventBus.Shutdown()
	api.INFO("server stopped gracefully")
}

// healthzHandler 探活端点，供 Kubernetes 等编排平台使用。
func healthzHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
