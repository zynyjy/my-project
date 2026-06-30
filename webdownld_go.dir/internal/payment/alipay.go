package payment

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"webdownld_go/internal/model"
)

// AlipayService 支付宝支付服务，封装下单、签名、验签等核心逻辑。
// 对接支付宝开放平台电脑/手机网站支付接口。
type AlipayService struct {
	appID          string          // appID 支付宝开放平台应用 ID。
	appPrivateKey  *rsa.PrivateKey // appPrivateKey 用于请求签名。
	alipayPublicKey *rsa.PublicKey // alipayPublicKey 用于回调验签。
	notifyURL      string          // notifyURL 支付宝异步通知回调 URL。
	returnURL      string          // returnURL 支付完成同步跳转 URL。
	isProduction   bool            // isProduction 沙箱模式/生产模式。
	httpClient     *http.Client    // httpClient 复用的 HTTP 客户端。
}

// NewAlipayService 创建支付宝服务实例。
func NewAlipayService(appID, privateKeyPEM, alipayPublicKeyPEM, notifyURL, returnURL string, isProduction bool) (*AlipayService, error) {
	svc := new(AlipayService)
	svc.appID = appID
	svc.notifyURL = notifyURL
	svc.returnURL = returnURL
	svc.isProduction = isProduction
	svc.httpClient = new(http.Client)
	svc.httpClient.Timeout = 15 * time.Second

	// 解析应用私钥。
	privKey, err := parsePrivateKey(privateKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("解析支付宝应用私钥失败: %w", err)
	}
	svc.appPrivateKey = privKey

	// 解析支付宝公钥。
	pubKey, err := parsePublicKey(alipayPublicKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("解析支付宝公钥失败: %w", err)
	}
	svc.alipayPublicKey = pubKey

	return svc, nil
}

// gatewayURL 返回支付宝网关地址（沙箱或生产）。
func (s *AlipayService) gatewayURL() string {
	if s.isProduction {
		return "https://openapi.alipay.com/gateway.do"
	}
	return "https://openapi-sandbox.dl.alipaydev.com/gateway.do"
}

// CreatePaymentOrder 生成支付宝支付请求参数并返回支付页面 URL。
func (s *AlipayService) CreatePaymentOrder(orderID int64, plan model.MemberPlan) (string, error) {
	bizContent := map[string]any{
		"out_trade_no": fmt.Sprintf("%d", orderID),
		"total_amount": fmt.Sprintf("%.2f", float64(plan.PriceCent)/100.0),
		"subject":      plan.PlanName + "会员充值",
		"product_code": "FAST_INSTANT_TRADE_PAY",
	}

	bizJSON, _ := json.Marshal(bizContent)

	params := map[string]string{
		"app_id":      s.appID,
		"method":      "alipay.trade.page.pay",
		"format":      "JSON",
		"charset":     "utf-8",
		"sign_type":   "RSA2",
		"timestamp":   time.Now().Format("2006-01-02 15:04:05"),
		"version":     "1.0",
		"notify_url":  s.notifyURL,
		"return_url":  s.returnURL,
		"biz_content": string(bizJSON),
	}

	// 构造待签名字符串。
	signStr := s.buildSignString(params)
	sig, err := s.sign([]byte(signStr))
	if err != nil {
		return "", fmt.Errorf("签名失败: %w", err)
	}
	params["sign"] = sig

	// 拼接完整 URL。
	query := url.Values{}
	for k, v := range params {
		query.Set(k, v)
	}
	return s.gatewayURL() + "?" + query.Encode(), nil
}

// buildSignString 按支付宝规范将参数排序并拼接为 key=value&key=value 格式。
func (s *AlipayService) buildSignString(params map[string]string) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		if k != "sign" && k != "sign_type" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	var parts []string
	for _, k := range keys {
		parts = append(parts, k+"="+params[k])
	}
	return strings.Join(parts, "&")
}

// sign 使用应用私钥对数据进行 RSA-SHA256 签名并 Base64 编码。
func (s *AlipayService) sign(data []byte) (string, error) {
	hash := sha256.Sum256(data)
	sig, err := rsa.SignPKCS1v15(rand.Reader, s.appPrivateKey, crypto.SHA256, hash[:])
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(sig), nil
}

// VerifyCallbackSign 校验支付宝异步通知的 RSA 签名是否合法。
func (s *AlipayService) VerifyCallbackSign(params map[string]string) bool {
	sign, ok := params["sign"]
	if !ok {
		return false
	}
	signBytes, err := base64.StdEncoding.DecodeString(sign)
	if err != nil {
		return false
	}

	signStr := s.buildSignString(params)
	hash := sha256.Sum256([]byte(signStr))
	err = rsa.VerifyPKCS1v15(s.alipayPublicKey, crypto.SHA256, hash[:], signBytes)
	return err == nil
}

// parsePrivateKey 从 PEM 编码字符串解析 RSA 私钥。
func parsePrivateKey(pemStr string) (*rsa.PrivateKey, error) {
	// 简化处理：直接解析 PKCS1 格式的 base64 私钥。
	block, _ := decodePEM(pemStr)
	if block == nil {
		return nil, fmt.Errorf("无法解析 PEM 私钥")
	}
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		// 尝试 PKCS8 格式。
		k, e2 := x509.ParsePKCS8PrivateKey(block.Bytes)
		if e2 != nil {
			return nil, fmt.Errorf("解析私钥失败: PKCS1=%v, PKCS8=%v", err, e2)
		}
		rsaKey, ok := k.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("密钥不是 RSA 私钥")
		}
		return rsaKey, nil
	}
	return key, nil
}

// parsePublicKey 从 PEM 编码字符串解析 RSA 公钥。
func parsePublicKey(pemStr string) (*rsa.PublicKey, error) {
	block, _ := decodePEM(pemStr)
	if block == nil {
		return nil, fmt.Errorf("无法解析 PEM 公钥")
	}
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	rsaKey, ok := key.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("密钥不是 RSA 公钥")
	}
	return rsaKey, nil
}

// decodePEM 解码 PEM 格式数据，支持带或不带首尾标记的 Base64 原始密钥。
func decodePEM(raw string) (*pemBlock, []byte) {
	// 尝试标准 PEM 解码。
	if strings.Contains(raw, "-----BEGIN") {
		block, _ := decodePEMStandard([]byte(raw))
		if block != nil {
			b := new(pemBlock)
			b.Bytes = block.Bytes
			return b, nil
		}
	}
	// 直接按 Base64 解码原始密钥。
	clean := strings.ReplaceAll(strings.ReplaceAll(raw, "\n", ""), " ", "")
	data, err := base64.StdEncoding.DecodeString(clean)
	if err != nil {
		return nil, nil
	}
	b := new(pemBlock)
	b.Bytes = data
	return b, nil
}

// pemBlock 简化版 PEM 块，避免引入 encoding/pem 复杂接口。
type pemBlock struct {
	Bytes []byte
}

// decodePEMStandard 使用 encoding/pem 解码标准 PEM 格式。
func decodePEMStandard(data []byte) (*pemBlock, []byte) {
	rest := data
	for len(rest) > 0 {
		var block *pemLine
		block, rest = nextPEMBlock(rest)
		if block != nil && block.Type == "PRIVATE KEY" || block != nil && block.Type == "RSA PRIVATE KEY" || block != nil && block.Type == "PUBLIC KEY" || block != nil && block.Type == "RSA PUBLIC KEY" {
			b := new(pemBlock)
			b.Bytes = block.Bytes
			return b, rest
		}
	}
	return nil, data
}

// pemLine 单行 PEM 结构。
type pemLine struct {
	Type  string
	Bytes []byte
}

// nextPEMBlock 从数据中解码下一个 PEM 块。
func nextPEMBlock(data []byte) (*pemLine, []byte) {
	text := string(data)
	if !strings.HasPrefix(text, "-----BEGIN ") {
		return nil, data
	}
	endHdr := strings.Index(text, "-----\n")
	if endHdr < 0 {
		return nil, data
	}
	hdr := text[11:endHdr] // "-----BEGIN " is 11 bytes
	footer := "-----END " + hdr + "-----"
	endFooter := strings.Index(text, footer)
	if endFooter < 0 {
		return nil, data
	}
	b64 := text[endHdr+6 : endFooter]
	decoded, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(b64, "\n", ""))
	if err != nil {
		return nil, data
	}
	l := new(pemLine)
	l.Type = strings.TrimSpace(hdr)
	l.Bytes = decoded
	return l, data[endFooter+len(footer):]
}

// HTTPPostForm 发送表单 POST 请求（支付宝异步通知验证时使用）。
func (s *AlipayService) HTTPPostForm(rawURL string, data url.Values) ([]byte, error) {
	resp, err := s.httpClient.PostForm(rawURL, data)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}
