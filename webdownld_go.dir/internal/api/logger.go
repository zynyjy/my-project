package api

import "log/slog"

// INFO 统一信息级别日志宏，所有 info 等级日志均通过此函数输出，
// 确保日志格式与输出目标一致，便于后续集中配置。
func INFO(msg string, args ...any) {
	slog.Info(msg, args...)
}
