package api

import (
	"database/sql"
	"net/http"
	"strings"
	"time"

	"webdownld_go/internal/auth"
	"webdownld_go/internal/model"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

// AuthHandler 用户认证处理器，处理注册、登录、令牌刷新和用户信息查询。
type AuthHandler struct {
	db  *sql.DB         // db MySQL 数据库连接。
	jwt *auth.JWTService // jwt JWT 令牌服务。
}

// NewAuthHandler 创建认证处理器实例。
// database 为 MySQL 连接，jwtService 为 JWT 服务。
func NewAuthHandler(database *sql.DB, jwtService *auth.JWTService) *AuthHandler {
	h := new(AuthHandler)
	h.db = database
	h.jwt = jwtService
	return h
}

// Register 注册认证相关路由。
func (h *AuthHandler) Register(r *gin.Engine) {
	auth := r.Group("/api/auth")
	{
		auth.POST("/register", h.registerUser)
		auth.POST("/login", h.loginUser)
		auth.GET("/me", JWTAuthMiddleware(h.jwt), h.currentUser)
		auth.POST("/refresh", h.refreshToken)
	}
}

// registerUser 创建新用户账号，用户名唯一，密码经 bcrypt 哈希后存储。
func (h *AuthHandler) registerUser(c *gin.Context) {
	var req struct {
		Username string `json:"username"` // Username 登录用户名。
		Password string `json:"password"` // Password 明文密码。
	}
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Username) == "" || len(req.Password) < 6 {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "用户名不能为空且密码不少于6位"})
		return
	}
	req.Username = strings.TrimSpace(req.Username)

	// 检查用户名是否已存在。
	var existID int64
	err := h.db.QueryRow("SELECT id FROM users WHERE username = ?", req.Username).Scan(&existID)
	if err == nil {
		c.JSON(http.StatusConflict, gin.H{"ok": false, "error": "用户名已存在"})
		return
	}
	if err != sql.ErrNoRows {
		INFO("查询用户失败", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "服务内部错误"})
		return
	}

	// bcrypt 哈希密码。
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		INFO("密码哈希失败", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "服务内部错误"})
		return
	}

	now := time.Now()
	result, err := h.db.Exec(
		"INSERT INTO users (username, password_hash, is_member, created_at, updated_at) VALUES (?, ?, 0, ?, ?)",
		req.Username, string(hash), now, now,
	)
	if err != nil {
		INFO("创建用户失败", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "注册失败"})
		return
	}

	userID, _ := result.LastInsertId()
	c.JSON(http.StatusOK, gin.H{"ok": true, "user_id": userID, "username": req.Username})
}

// loginUser 验证用户凭证，成功则返回 JWT 访问令牌和刷新令牌。
func (h *AuthHandler) loginUser(c *gin.Context) {
	var req struct {
		Username string `json:"username"` // Username 登录用户名。
		Password string `json:"password"` // Password 明文密码。
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "请求参数无效"})
		return
	}

	var user model.User
	err := h.db.QueryRow(
		"SELECT id, username, password_hash, is_member, member_expire FROM users WHERE username = ?",
		strings.TrimSpace(req.Username),
	).Scan(&user.ID, &user.Username, &user.PasswordHash, &user.IsMember, &user.MemberExpire)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusUnauthorized, gin.H{"ok": false, "error": "用户名或密码错误"})
		return
	}
	if err != nil {
		INFO("查询用户失败", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "服务内部错误"})
		return
	}

	// 校验密码。
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"ok": false, "error": "用户名或密码错误"})
		return
	}

	// 检查会员是否过期。
	isMember := user.IsMember
	if isMember && user.MemberExpire != nil && time.Now().After(*user.MemberExpire) {
		isMember = false
	}

	accessToken, err := h.jwt.GenerateAccessToken(user.ID, user.Username, isMember)
	if err != nil {
		INFO("生成访问令牌失败", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "令牌生成失败"})
		return
	}

	refreshToken, err := h.jwt.GenerateRefreshToken(user.ID, user.Username)
	if err != nil {
		INFO("生成刷新令牌失败", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "令牌生成失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":           true,
		"access_token": accessToken,
		"refresh_token": refreshToken,
		"user_id":      user.ID,
		"username":     user.Username,
		"is_member":    isMember,
	})
}

// currentUser 返回当前登录用户的详细信息（需 JWT 鉴权）。
func (h *AuthHandler) currentUser(c *gin.Context) {
	userID := c.GetInt64("user_id")
	var user model.User
	err := h.db.QueryRow(
		"SELECT id, username, is_member, member_expire, created_at FROM users WHERE id = ?",
		userID,
	).Scan(&user.ID, &user.Username, &user.IsMember, &user.MemberExpire, &user.CreatedAt)

	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "用户不存在"})
		return
	}

	// 检查会员是否过期。
	isMember := user.IsMember
	if isMember && user.MemberExpire != nil && time.Now().After(*user.MemberExpire) {
		isMember = false
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":         true,
		"user_id":    user.ID,
		"username":   user.Username,
		"is_member":  isMember,
		"created_at": user.CreatedAt,
	})
}

// refreshToken 使用有效的刷新令牌换取新的访问令牌。
func (h *AuthHandler) refreshToken(c *gin.Context) {
	var req struct {
		RefreshToken string `json:"refresh_token"` // RefreshToken 长期刷新令牌。
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.RefreshToken == "" {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "刷新令牌不能为空"})
		return
	}

	claims, err := h.jwt.ValidateToken(req.RefreshToken)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"ok": false, "error": "刷新令牌无效或已过期"})
		return
	}

	// 查询最新用户状态。
	var user model.User
	err = h.db.QueryRow(
		"SELECT is_member, member_expire FROM users WHERE id = ?",
		claims.UserID,
	).Scan(&user.IsMember, &user.MemberExpire)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"ok": false, "error": "用户不存在"})
		return
	}

	isMember := user.IsMember
	if isMember && user.MemberExpire != nil && time.Now().After(*user.MemberExpire) {
		isMember = false
	}

	accessToken, err := h.jwt.GenerateAccessToken(claims.UserID, claims.Username, isMember)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "令牌生成失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"ok": true, "access_token": accessToken})
}
