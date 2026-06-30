package mq

import (
	"encoding/json"
	"log/slog"
	"sync"
)

// OrderEvent 描述一次订单状态变更的消息体。
type OrderEvent struct {
	OrderID    int64  `json:"order_id"`       // OrderID 订单唯一标识。
	UserID     int64  `json:"user_id"`        // UserID 下单用户 ID。
	PlanID     int64  `json:"plan_id"`        // PlanID 购买的套餐 ID。
	AmountCent int64  `json:"amount_cent"`    // AmountCent 订单金额（分）。
	TradeNo    string `json:"alipay_trade_no"` // TradeNo 支付宝流水号。
}

// 路由键常量，定义 Topic Exchange 中的消息路由规则。
const (
	RoutingKeyOrderCreated  = "order.created"   // RoutingKeyOrderCreated 订单新建事件。
	RoutingKeyOrderPaid     = "order.paid"      // RoutingKeyOrderPaid 支付成功事件。
	RoutingKeyOrderExpired  = "order.expired"   // RoutingKeyOrderExpired 订单超时事件。
	RoutingKeyMemberUpgrade = "member.upgraded" // RoutingKeyMemberUpgrade 会员开通成功事件。
)

// Subscriber 消息处理函数签名，接收事件字节并返回可能的错误。
type Subscriber func(event []byte) error

// TopicExchange 基于 Topic 模式的内存事件总线，模拟 RabbitMQ Topic Exchange。
// 使用 goroutine + channel 实现异步消息分发，支持通配符路由键匹配。
type TopicExchange struct {
	mu      sync.RWMutex
	subs    map[string][]Subscriber // subs routingKey → 订阅者列表。
	closeCh chan struct{}           // closeCh 通知所有消费者停止。
}

// NewTopicExchange 创建 Topic 事件总线实例。
func NewTopicExchange() *TopicExchange {
	e := new(TopicExchange)
	e.subs = make(map[string][]Subscriber)
	e.closeCh = make(chan struct{})
	return e
}

// Publish 向指定路由键发布事件消息，异步分发给所有匹配的订阅者。
func (e *TopicExchange) Publish(routingKey string, event OrderEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		slog.Error("序列化事件失败", "routingKey", routingKey, "error", err)
		return
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	for key, subs := range e.subs {
		if e.matchRoute(key, routingKey) {
			for _, sub := range subs {
				go func(s Subscriber, d []byte) {
					if err := s(d); err != nil {
						slog.Error("事件处理失败", "routingKey", routingKey, "error", err)
					}
				}(sub, data)
			}
		}
	}
	slog.Info("事件已发布", "routingKey", routingKey)
}

// Subscribe 向指定路由键注册订阅者。
// bindingKey 支持通配符 *（匹配单段）和 #（匹配多段）。
func (e *TopicExchange) Subscribe(bindingKey string, sub Subscriber) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.subs[bindingKey] = append(e.subs[bindingKey], sub)
}

// matchRoute 判断路由键是否匹配绑定键（支持 * 和 # 通配符）。
func (e *TopicExchange) matchRoute(bindingKey, routingKey string) bool {
	if bindingKey == "#" {
		return true
	}
	if bindingKey == routingKey {
		return true
	}
	if bindingKey == "order.#" {
		return len(routingKey) >= 6 && routingKey[:6] == "order."
	}
	if bindingKey == "member.#" {
		return len(routingKey) >= 7 && routingKey[:7] == "member."
	}
	return false
}

// Shutdown 停止事件总线，通知所有消费者退出。
func (e *TopicExchange) Shutdown() {
	close(e.closeCh)
}

// InitSubscriptions 注册默认的事件订阅者（日志记录、会员处理）。
func (e *TopicExchange) InitSubscriptions(membershipCallback Subscriber) {
	// 订单日志订阅：监听所有 order.* 事件。
	e.Subscribe("order.#", func(event []byte) error {
		var evt OrderEvent
		if err := json.Unmarshal(event, &evt); err != nil {
			return err
		}
		slog.Info("订单日志", "order_id", evt.OrderID, "user_id", evt.UserID, "amount_cent", evt.AmountCent)
		return nil
	})

	// 支付成功 → 开通会员。
	e.Subscribe("order.paid", membershipCallback)

	// 会员升级日志。
	e.Subscribe("member.upgraded", func(event []byte) error {
		var evt OrderEvent
		if err := json.Unmarshal(event, &evt); err != nil {
			return err
		}
		slog.Info("会员升级", "user_id", evt.UserID, "plan_id", evt.PlanID)
		return nil
	})
}
