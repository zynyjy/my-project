package model

// 订单状态常量，定义订单生命周期中的各阶段。
const (
	StatusPending  = "pending"  // StatusPending 订单已创建但尚未付款。
	StatusPaid     = "paid"     // StatusPaid 支付宝回调确认收款。
	StatusExpired  = "expired"  // StatusExpired 超时未支付自动关闭。
	StatusRefunded = "refunded" // StatusRefunded 已退款给用户。
)

// PresetPlans 提供默认的三档会员套餐（月卡/季卡/年卡）。
func PresetPlans() []MemberPlan {
	return []MemberPlan{
		{PlanName: "月卡", PriceCent: 1990, DurationDays: 30},    // 19.90 元
		{PlanName: "季卡", PriceCent: 4990, DurationDays: 90},    // 49.90 元
		{PlanName: "年卡", PriceCent: 15990, DurationDays: 365},  // 159.90 元
	}
}
