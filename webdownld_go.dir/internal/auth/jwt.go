package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// JWTClaims 自定义 JWT 载荷，包含用户身份与会员状态。
type JWTClaims struct {
	UserID   int64  `json:"user_id"`   // UserID 登录用户的唯一标识。
	Username string `json:"username"`  // Username 登录账号名。
	IsMember bool   `json:"is_member"` // IsMember 当前是否为有效会员。
}

// JWTService JWT 令牌服务，负责生成和校验访问令牌与刷新令牌。
type JWTService struct {
	secret      []byte        // secret 用于签名 JWT 的对称密钥。
	accessTTL   time.Duration // accessTTL Access Token 的生存时间。
	refreshTTL  time.Duration // refreshTTL Refresh Token 的生存时间。
}

// NewJWTService 创建 JWT 服务实例。
// secret 为签名密钥，accessTTL 为访问令牌有效期，refreshTTL 为刷新令牌有效期。
func NewJWTService(secret string, accessTTL, refreshTTL time.Duration) *JWTService {
	s := new(JWTService)
	s.secret = []byte(secret)
	s.accessTTL = accessTTL
	s.refreshTTL = refreshTTL
	return s
}

// GenerateAccessToken 为用户生成短期访问令牌。
func (s *JWTService) GenerateAccessToken(userID int64, username string, isMember bool) (string, error) {
	return s.signToken(userID, username, isMember, s.accessTTL)
}

// GenerateRefreshToken 为用户生成长期刷新令牌，用于续期访问令牌。
func (s *JWTService) GenerateRefreshToken(userID int64, username string) (string, error) {
	return s.signToken(userID, username, false, s.refreshTTL)
}

// signToken 构造 JWT 三段式令牌并返回。
func (s *JWTService) signToken(userID int64, username string, isMember bool, ttl time.Duration) (string, error) {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))

	now := time.Now()
	claims := map[string]any{
		"user_id":   userID,
		"username":  username,
		"is_member": isMember,
		"iat":       now.Unix(),
		"exp":       now.Add(ttl).Unix(),
		"iss":       "cloudraft-drive",
	}
	payloadBytes, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("序列化 JWT 载荷失败: %w", err)
	}
	payload := base64.RawURLEncoding.EncodeToString(payloadBytes)

	signingInput := header + "." + payload
	signature := s.computeSignature([]byte(signingInput))

	return signingInput + "." + signature, nil
}

// computeSignature 使用 HMAC-SHA256 计算签名并 Base64 编码。
func (s *JWTService) computeSignature(data []byte) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write(data)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// ValidateToken 解析并验证 JWT 令牌字符串，返回其中的载荷信息。
func (s *JWTService) ValidateToken(tokenString string) (*JWTClaims, error) {
	parts := strings.Split(tokenString, ".")
	if len(parts) != 3 {
		return nil, errors.New("无效的令牌格式")
	}

	signingInput := parts[0] + "." + parts[1]
	expectedSig := s.computeSignature([]byte(signingInput))
	if !hmac.Equal([]byte(expectedSig), []byte(parts[2])) {
		return nil, errors.New("令牌签名校验失败")
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("解码令牌载荷失败: %w", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(payloadBytes, &raw); err != nil {
		return nil, fmt.Errorf("解析令牌载荷失败: %w", err)
	}

	exp, ok := raw["exp"].(float64)
	if !ok || time.Now().Unix() > int64(exp) {
		return nil, errors.New("令牌已过期")
	}

	claims := new(JWTClaims)
	if v, ok := raw["user_id"].(float64); ok {
		claims.UserID = int64(v)
	}
	if v, ok := raw["username"].(string); ok {
		claims.Username = v
	}
	if v, ok := raw["is_member"].(bool); ok {
		claims.IsMember = v
	}
	return claims, nil
}
