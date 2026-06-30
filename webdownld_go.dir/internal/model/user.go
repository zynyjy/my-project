package model

import "time"

// User 系统用户模型，包含认证信息与会员状态。
type User struct {
	ID           int64      `json:"id"`            // ID 用户唯一标识。
	Username     string     `json:"username"`      // Username 登录账号名。
	PasswordHash string     `json:"-"`             // PasswordHash bcrypt 加密后的密码，JSON 序列化时隐藏。
	IsMember     bool       `json:"is_member"`     // IsMember 当前是否为有效会员。
	MemberExpire *time.Time `json:"member_expire"` // MemberExpire 会员有效期截止时间，nil 表示从未开通。
	CreatedAt    time.Time  `json:"created_at"`    // CreatedAt 用户注册时间。
	UpdatedAt    time.Time  `json:"updated_at"`    // UpdatedAt 用户信息最后修改时间。
}

// MemberPlan 会员订阅套餐定义。
type MemberPlan struct {
	ID           int64  `json:"id"`            // ID 套餐唯一标识。
	PlanName     string `json:"plan_name"`     // PlanName 如"月卡"、"季卡"、"年卡"。
	PriceCent    int64  `json:"price_cent"`    // PriceCent 价格以分为单位，避免浮点精度问题。
	DurationDays int    `json:"duration_days"` // DurationDays 套餐包含的会员天数。
}

// Order 充值订单模型，记录用户购买会员的完整交易信息。
type Order struct {
	ID           int64      `json:"id"`              // ID 订单唯一标识。
	UserID       int64      `json:"user_id"`         // UserID 下单用户 ID。
	PlanID       int64      `json:"plan_id"`         // PlanID 购买的会员套餐 ID。
	AmountCent   int64      `json:"amount_cent"`     // AmountCent 订单金额，单位为分。
	Status       string     `json:"status"`          // Status pending/paid/expired/refunded。
	TradeNo      string     `json:"alipay_trade_no"` // TradeNo 支付宝返回的交易流水号。
	CreatedAt    time.Time  `json:"created_at"`      // CreatedAt 订单创建时间。
	PaidAt       *time.Time `json:"paid_at"`         // PaidAt 订单支付完成时间，nil 表示未支付。
}
