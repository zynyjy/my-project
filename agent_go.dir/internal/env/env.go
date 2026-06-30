// Package env 提供集中式的环境变量帮助函数，供所有智能体包共享使用。
package env

import (
	"os"
	"strconv"
	"strings"
)

// Int returns the int value of the environment variable key,
// or fallback if the variable is empty or not a valid positive integer.
func Int(key string, fallback int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}

// Float32 returns the float32 value of the environment variable key,
// or fallback if the variable is empty or not a valid positive number.
func Float32(key string, fallback float32) float32 {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if parsed, err := strconv.ParseFloat(v, 32); err == nil && parsed > 0 {
			return float32(parsed)
		}
	}
	return fallback
}

// String returns the environment variable value, or fallback if empty.
func String(key string, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// Duration returns the duration value (in seconds) from environment variable,
// or fallback if the variable is empty or not a valid positive integer.
func Duration(key string, fallbackSec int) int {
	return Int(key, fallbackSec)
}
