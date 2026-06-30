package repair

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	internalagent "agent_go/internal/agent"
	"agent_go/internal/monitor"

	"github.com/shirou/gopsutil/v3/process"
)

// Agent 是服务修复智能体，订阅进程告警事件并执行自动修复。
type Agent struct {
	manager *internalagent.Manager // manager 用于发布修复事件与状态快照。
	mu      sync.Mutex             // mu 保护 repairs 的并发访问。
	repairs []Record               // repairs 保存修复记录历史。
}

// Record 表示一条修复记录，记录从告警到修复完成的完整生命周期。
type Record struct {
	AlertID     string    `json:"alert_id"`               // AlertID 关联的告警唯一标识。
	Service     string    `json:"service,omitempty"`      // Service 匹配到的业务服务名称。
	ProcessName string    `json:"process_name"`           // ProcessName 进程名称。
	Type        string    `json:"type"`                   // Type 告警类型。
	Command     string    `json:"command,omitempty"`      // Command 执行的重启命令。
	Status      string    `json:"status"`                 // Status 修复状态（running/skipped/failed/completed/warning）。
	Message     string    `json:"message"`                // Message 修复状态描述信息。
	Verified    bool      `json:"verified"`               // Verified 进程恢复验证是否通过。
	StartedAt   time.Time `json:"started_at"`             // StartedAt 修复开始时间。
	FinishedAt  time.Time `json:"finished_at,omitempty"`  // FinishedAt 修复完成时间。
}

// NewAgent 创建服务修复智能体实例。
func NewAgent(manager *internalagent.Manager) *Agent {
	a := new(Agent)
	a.manager = manager
	a.repairs = make([]Record, 0, 32)
	return a
}

// Start 启动修复智能体事件循环，订阅进程告警并异步处理。
// ctx 取消时退出循环并发出停止事件。
func (a *Agent) Start(ctx context.Context) {
	stream := a.manager.Hub().Subscribe()
	defer a.manager.Hub().Unsubscribe(stream)

	a.manager.SetState("service_repair_agent", map[string]any{
		"status":  "listening",
		"repairs": []Record{},
	})
	a.manager.Emit("service_repair_agent", "running", "listening process alerts")

	for {
		select {
		case <-ctx.Done():
			a.manager.Emit("service_repair_agent", "stopped", "repair loop stopped")
			return
		case ev := <-stream:
			if ev.Agent != "process_alert_agent" || ev.Status != "alert" {
				continue
			}
			alert, ok := ev.Detail.(monitor.Alert)
			if !ok {
				continue
			}
			go a.handleAlert(ctx, alert)
		}
	}
}

// handleAlert 处理单条进程告警，依次执行：记录开始、重启服务、验证进程恢复。
func (a *Agent) handleAlert(ctx context.Context, alert monitor.Alert) {
	// 步骤 1：记录修复开始。
	record := Record{
		AlertID:     alert.ID,
		Service:     alert.Service,
		ProcessName: alert.Name,
		Type:        alert.Type,
		Command:     alert.RestartCommand,
		Status:      "running",
		Message:     "repair started",
		StartedAt:   time.Now(),
	}
	a.saveAndEmit(record)

	// 步骤 2：重启服务。
	if !alert.Restartable || strings.TrimSpace(alert.RestartCommand) == "" {
		record.Status = "skipped"
		record.Message = "alert is not restartable; configure MONITORED_SERVICES restart_command"
		record.FinishedAt = time.Now()
		a.saveAndEmit(record)
		return
	}

	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(runCtx, "sh", "-c", alert.RestartCommand)
	output, err := cmd.CombinedOutput()
	if err != nil {
		record.Status = "failed"
		record.Message = fmt.Sprintf("restart command failed: %v %s", err, strings.TrimSpace(string(output)))
		record.FinishedAt = time.Now()
		a.saveAndEmit(record)
		return
	}

	record.Status = "verifying"
	record.Message = "restart command completed; verifying process"
	a.saveAndEmit(record)

	// 步骤 3：验证服务恢复。
	select {
	case <-ctx.Done():
		record.Status = "cancelled"
		record.Message = "verify cancelled during shutdown"
		record.FinishedAt = time.Now()
		a.saveAndEmit(record)
		return
	case <-time.After(2 * time.Second):
	}

	record.Verified = verifyProcess(alert.MatchName, alert.Name)
	record.FinishedAt = time.Now()
	if record.Verified {
		record.Status = "completed"
		record.Message = "restart completed and process is running"
	} else {
		record.Status = "warning"
		record.Message = "restart completed, but process verification did not find a matching process"
	}
	a.saveAndEmit(record)
}

// saveAndEmit 保存修复记录到历史列表并发布状态更新事件。
func (a *Agent) saveAndEmit(record Record) {
	a.mu.Lock()
	replaced := false
	for i := range a.repairs {
		if a.repairs[i].AlertID == record.AlertID {
			a.repairs[i] = record
			replaced = true
			break
		}
	}
	if !replaced {
		a.repairs = append([]Record{record}, a.repairs...)
	}
	if len(a.repairs) > 50 {
		a.repairs = a.repairs[:50]
	}
	repairs := append([]Record(nil), a.repairs...)
	a.mu.Unlock()

	a.manager.SetState("service_repair_agent", map[string]any{
		"status":  record.Status,
		"latest":  record,
		"repairs": repairs,
	})
	a.manager.Emit("service_repair_agent", record.Status, record)
}

// verifyProcess 验证指定名称的进程是否仍在运行，返回 true 表示进程存活。
func verifyProcess(matchName, fallback string) bool {
	match := strings.ToLower(strings.TrimSpace(matchName))
	if match == "" {
		match = strings.ToLower(strings.TrimSpace(fallback))
	}
	if match == "" {
		return true
	}

	pids, err := process.Pids()
	if err != nil {
		return false
	}
	for _, pid := range pids {
		proc, err := process.NewProcess(pid)
		if err != nil {
			continue
		}
		name, err := proc.Name()
		if err != nil {
			continue
		}
		if strings.Contains(strings.ToLower(name), match) {
			running, _ := proc.IsRunning()
			return running
		}
	}
	return false
}
