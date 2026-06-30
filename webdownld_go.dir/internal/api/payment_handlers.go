package api

import (
	"database/sql"
	"net/http"
	"strconv"
	"time"

	"webdownld_go/internal/model"
	"webdownld_go/internal/mq"
	"webdownld_go/internal/payment"

	"github.com/gin-gonic/gin"
)

// PaymentHandler 支付处理器，处理会员套餐查询、订单创建和支付宝回调。
type PaymentHandler struct {
	db       *sql.DB                // db MySQL 数据库连接。
	alipay   *payment.AlipayService // alipay 支付宝支付服务。
	eventBus *mq.TopicExchange      // eventBus 订单事件总线。
}

// NewPaymentHandler 创建支付处理器实例。
func NewPaymentHandler(database *sql.DB, alipaySvc *payment.AlipayService, bus *mq.TopicExchange) *PaymentHandler {
	h := new(PaymentHandler)
	h.db = database
	h.alipay = alipaySvc
	h.eventBus = bus
	return h
}

// Register 注册支付相关路由。
func (h *PaymentHandler) Register(r *gin.Engine) {
	pay := r.Group("/api/payment")
	pay.Use(JWTAuthMiddleware(nil)) // 由外部注入，此处占位，实际由 main 统一注册。
	{
		pay.GET("/plans", h.listPlans)
		pay.POST("/order", h.createOrder)
	}

	// 支付宝回调不需要 JWT 鉴权。
	r.POST("/api/payment/notify", h.paymentNotify)
	r.GET("/api/payment/return", h.paymentReturn)
}

// listPlans 返回所有可购买的会员套餐选项。
func (h *PaymentHandler) listPlans(c *gin.Context) {
	plans := model.PresetPlans()
	c.JSON(http.StatusOK, gin.H{"ok": true, "plans": plans})
}

// createOrder 为用户创建充值订单并返回支付宝支付链接。
func (h *PaymentHandler) createOrder(c *gin.Context) {
	userID := c.GetInt64("user_id")

	var req struct {
		PlanID int64 `json:"plan_id"` // PlanID 套餐 ID。
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "请求参数无效"})
		return
	}

	// 查找对应套餐。
	plans := model.PresetPlans()
	var selectedPlan *model.MemberPlan
	for i := range plans {
		if int64(i) == req.PlanID {
			selectedPlan = &plans[i]
			break
		}
	}
	if selectedPlan == nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "无效的套餐"})
		return
	}

	// 创建订单记录。
	now := time.Now()
	result, err := h.db.Exec(
		"INSERT INTO orders (user_id, plan_id, amount_cent, status, created_at) VALUES (?, ?, ?, ?, ?)",
		userID, req.PlanID, selectedPlan.PriceCent, model.StatusPending, now,
	)
	if err != nil {
		INFO("创建订单失败", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "创建订单失败"})
		return
	}

	orderID, _ := result.LastInsertId()

	// 发布订单创建事件。
	h.eventBus.Publish(mq.RoutingKeyOrderCreated, mq.OrderEvent{
		OrderID:    orderID,
		UserID:     userID,
		PlanID:     req.PlanID,
		AmountCent: selectedPlan.PriceCent,
	})

	// 生成支付宝支付链接。
	payURL, err := h.alipay.CreatePaymentOrder(orderID, *selectedPlan)
	if err != nil {
		INFO("生成支付链接失败", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "生成支付链接失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":         true,
		"order_id":   orderID,
		"pay_url":    payURL,
		"amount_cent": selectedPlan.PriceCent,
	})
}

// paymentNotify 接收支付宝异步通知，验证签名后更新订单状态并开通会员。
func (h *PaymentHandler) paymentNotify(c *gin.Context) {
	params := make(map[string]string)
	c.Request.ParseForm()
	for k, v := range c.Request.PostForm {
		if len(v) > 0 {
			params[k] = v[0]
		}
	}

	// 验证支付宝签名。
	if !h.alipay.VerifyCallbackSign(params) {
		c.String(http.StatusBadRequest, "fail")
		return
	}

	tradeStatus := params["trade_status"]
	if tradeStatus != "TRADE_SUCCESS" && tradeStatus != "TRADE_FINISHED" {
		c.String(http.StatusOK, "success")
		return
	}

	outTradeNo := params["out_trade_no"]
	tradeNo := params["trade_no"]
	orderID, err := strconv.ParseInt(outTradeNo, 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, "fail")
		return
	}

	// 查询订单信息。
	var order model.Order
	err = h.db.QueryRow(
		"SELECT id, user_id, plan_id, amount_cent, status FROM orders WHERE id = ? AND status = ?",
		orderID, model.StatusPending,
	).Scan(&order.ID, &order.UserID, &order.PlanID, &order.AmountCent, &order.Status)

	if err == sql.ErrNoRows {
		// 订单已处理或不存在。
		c.String(http.StatusOK, "success")
		return
	}
	if err != nil {
		INFO("查询订单失败", "error", err)
		c.String(http.StatusInternalServerError, "fail")
		return
	}

	// 更新订单状态。
	now := time.Now()
	_, err = h.db.Exec(
		"UPDATE orders SET status = ?, alipay_trade_no = ?, paid_at = ? WHERE id = ?",
		model.StatusPaid, tradeNo, now, orderID,
	)
	if err != nil {
		INFO("更新订单状态失败", "error", err)
		c.String(http.StatusInternalServerError, "fail")
		return
	}

	// 查找套餐信息。
	plans := model.PresetPlans()
	var plan *model.MemberPlan
	for i := range plans {
		if int64(i) == order.PlanID {
			plan = &plans[i]
			break
		}
	}
	if plan == nil {
		c.String(http.StatusOK, "success")
		return
	}

	// 开通/续费会员。
	memberExpire := now.Add(time.Duration(plan.DurationDays) * 24 * time.Hour)
	_, err = h.db.Exec(
		"UPDATE users SET is_member = 1, member_expire = ?, updated_at = ? WHERE id = ?",
		memberExpire, now, order.UserID,
	)
	if err != nil {
		INFO("开通会员失败", "error", err)
	}

	// 发布支付成功和会员升级事件。
	h.eventBus.Publish(mq.RoutingKeyOrderPaid, mq.OrderEvent{
		OrderID:    orderID,
		UserID:     order.UserID,
		PlanID:     order.PlanID,
		AmountCent: order.AmountCent,
		TradeNo:    tradeNo,
	})
	h.eventBus.Publish(mq.RoutingKeyMemberUpgrade, mq.OrderEvent{
		OrderID: orderID,
		UserID:  order.UserID,
		PlanID:  order.PlanID,
	})

	c.String(http.StatusOK, "success")
}

// paymentReturn 处理支付完成后的同步跳转，重定向到前端页面。
func (h *PaymentHandler) paymentReturn(c *gin.Context) {
	c.Redirect(http.StatusFound, "/")
}
