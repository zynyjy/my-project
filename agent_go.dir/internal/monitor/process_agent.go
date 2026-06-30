package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"agent_go/internal/agent"
	"agent_go/internal/env"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/process"
)

type ProcessInfo struct {
	PID     int32   `json:"pid"`            // PID 进程 ID。
	Name    string  `json:"name"`           // Name 进程名称。
	Status  string  `json:"status"`         // Status 进程状态码。
	CPU     float64 `json:"cpu_percent"`    // CPU 进程 CPU 使用率百分比。
	Memory  float32 `json:"memory_percent"` // Memory 进程内存使用率百分比。
	Running bool    `json:"running"`        // Running 进程是否仍在运行。
}

type Snapshot struct {
	HostCPUPercent    float64       `json:"host_cpu_percent"`    // HostCPUPercent 主机总体 CPU 使用率。
	HostMemoryPercent float64       `json:"host_memory_percent"` // HostMemoryPercent 主机总体内存使用率。
	ProcessCount      int           `json:"process_count"`       // ProcessCount 当前进程总数。
	Processes         []ProcessInfo `json:"processes"`           // Processes 按 CPU 排序的进程采样列表。
	TopProcesses      []ProcessInfo `json:"top_processes"`       // TopProcesses 按 CPU 排序的热点进程列表。
	Alerts            []Alert       `json:"alerts"`              // Alerts 最近告警列表。
	UpdatedAt         time.Time     `json:"updated_at"`          // UpdatedAt 本次快照更新时间。
}

type Alert struct {
	ID             string    `json:"id"`                        // ID 告警唯一标识。
	PID            int32     `json:"pid"`                       // PID 告警对应进程 ID。
	Name           string    `json:"name"`                      // Name 进程名称。
	Service        string    `json:"service,omitempty"`         // Service 匹配到的业务服务名称。
	MatchName      string    `json:"match_name,omitempty"`      // MatchName 修复校验使用的进程名关键字。
	Type           string    `json:"type"`                      // Type 告警类型标识。
	Severity       string    `json:"severity"`                  // Severity 告警级别。
	Message        string    `json:"message"`                   // Message 告警描述信息。
	Restartable    bool      `json:"restartable"`               // Restartable 是否允许自动修复。
	RestartCommand string    `json:"restart_command,omitempty"` // RestartCommand 受控服务配置中的重启命令。
	CreatedAt      time.Time `json:"created_at"`                // CreatedAt 告警触发时间。
}

type ProcessAgent struct {
	manager            *agent.Manager    // manager 用于发布监控事件与快照。
	interval           time.Duration     // interval 采样周期。
	mu                 sync.Mutex        // mu 保护 lastCPU、alerts 与 alertCooldown。
	lastCPU            map[int32]float64 // lastCPU 保存上一轮采样的进程 CPU。
	alerts             []Alert           // alerts 保存告警历史（新到旧）。
	alertCooldown      map[string]time.Time
	workers            int              // workers 进程采样并发 worker 数。
	services           []ServiceSpec    // services 可自动修复的业务服务白名单。
	memoryLimitPercent float32          // memoryLimitPercent 进程内存告警阈值。
	processListLimit   int              // processListLimit 快照中最多返回的进程数。
	history            *TimeseriesRing  // history 时间序列环形缓冲区，存储历史快照供图表 API 使用。
}

type ServiceSpec struct {
	Name           string  `json:"name"`
	MatchName      string  `json:"match_name"`
	RestartCommand string  `json:"restart_command"`
	MemoryLimit    float32 `json:"memory_limit_percent"`
}

// NewProcessAgent 创建进程监控智能体。
// manager 为事件管理器，interval 为采样间隔。
func NewProcessAgent(manager *agent.Manager, interval time.Duration) *ProcessAgent {
	a := new(ProcessAgent)
	a.manager = manager
	a.interval = interval
	a.lastCPU = make(map[int32]float64)
	a.alerts = make([]Alert, 0, 32)
	a.alertCooldown = make(map[string]time.Time)
	a.workers = env.Int("PROCESS_SAMPLE_WORKERS", 24)
	a.services = loadServiceSpecs()
	a.memoryLimitPercent = env.Float32("OOM_MEMORY_PERCENT", 85)
	a.processListLimit = env.Int("PROCESS_LIST_LIMIT", 250)
	a.history = NewTimeseriesRing(env.Int("MONITOR_HISTORY_CAPACITY", 120))
	return a
}

// History 返回时间序列环形缓冲区，供 Web 层获取历史快照数据。
func (p *ProcessAgent) History() *TimeseriesRing { return p.history }

// Start 启动监控循环，定时采集进程与主机指标并发布。
// ctx 取消时退出监控并发出停止事件。
func (p *ProcessAgent) Start(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	p.collectAndPublish()
	for {
		select {
		case <-ctx.Done():
			p.manager.Emit("process_monitor_agent", "stopped", "monitor loop stopped")
			return
		case <-ticker.C:
			p.collectAndPublish()
		}
	}
}

// collectAndPublish 采集一次系统状态，构建快照并向外发布。
func (p *ProcessAgent) collectAndPublish() {
	ids, _ := process.Pids()
	infos := make([]ProcessInfo, 0, len(ids))
	currentCPU := make(map[int32]float64, len(ids))

	type sample struct {
		info ProcessInfo
		ok   bool
	}
	jobs := make(chan int32, len(ids))
	results := make(chan sample, len(ids))
	workerN := p.workers
	if workerN <= 0 {
		workerN = 8
	}
	if workerN > len(ids) && len(ids) > 0 {
		workerN = len(ids)
	}
	var wg sync.WaitGroup
	for i := 0; i < workerN; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pid := range jobs {
				info, ok := p.sampleProcess(pid)
				results <- sample{info: info, ok: ok}
			}
		}()
	}
	for _, pid := range ids {
		jobs <- pid
	}
	close(jobs)
	wg.Wait()
	close(results)
	for item := range results {
		if !item.ok {
			continue
		}
		currentCPU[item.info.PID] = item.info.CPU
		infos = append(infos, item.info)
		p.tryCPUZeroAlert(item.info.PID, item.info.Name, item.info.CPU, item.info.Running)
		p.tryMemoryAlert(item.info)
	}

	sort.Slice(infos, func(i, j int) bool {
		return infos[i].CPU > infos[j].CPU
	})
	allProcesses := infos
	if p.processListLimit > 0 && len(allProcesses) > p.processListLimit {
		allProcesses = allProcesses[:p.processListLimit]
	}
	topProcesses := infos
	if len(topProcesses) > 25 {
		topProcesses = topProcesses[:25]
	}

	hostCPU := 0.0
	if v, err := cpu.Percent(300*time.Millisecond, false); err == nil && len(v) > 0 {
		hostCPU = v[0]
	}

	hostMem := 0.0
	if m, err := mem.VirtualMemory(); err == nil {
		hostMem = m.UsedPercent
	}

	snapshot := Snapshot{
		HostCPUPercent:    hostCPU,
		HostMemoryPercent: hostMem,
		ProcessCount:      len(ids),
		Processes:         allProcesses,
		TopProcesses:      topProcesses,
		Alerts:            p.recentAlerts(),
		UpdatedAt:         time.Now(),
	}

	p.mu.Lock()
	p.lastCPU = currentCPU
	p.mu.Unlock()

	p.manager.SetState("process_monitor_agent", snapshot)
	p.manager.Emit("process_monitor_agent", "running", snapshot)
	p.history.Push(snapshot)
}

// sampleProcess 采样单个进程指标并转换为 ProcessInfo。
// pid 为进程 ID，返回进程信息和是否采样成功。
func (p *ProcessAgent) sampleProcess(pid int32) (ProcessInfo, bool) {
	proc, err := process.NewProcess(pid)
	if err != nil {
		return ProcessInfo{}, false
	}
	name, _ := proc.Name()
	statusList, _ := proc.Status()
	status := "unknown"
	if len(statusList) > 0 {
		status = statusList[0]
	}
	cpuPercent, _ := proc.CPUPercent()
	memPercent, _ := proc.MemoryPercent()
	running, _ := proc.IsRunning()
	return ProcessInfo{
		PID:     pid,
		Name:    name,
		Status:  status,
		CPU:     cpuPercent,
		Memory:  memPercent,
		Running: running,
	}, true
}

// tryCPUZeroAlert 检测“CPU 从非零跌至 0 且进程仍存活”的告警条件。
// pid/name/current/running 分别表示进程标识、名称、当前 CPU 和存活状态。
func (p *ProcessAgent) tryCPUZeroAlert(pid int32, name string, current float64, running bool) {
	if !running {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	prev, ok := p.lastCPU[pid]
	if !ok {
		return
	}
	if prev <= 0.1 || current > 0.0 {
		return
	}

	spec := p.matchService(name)
	alert := p.newAlert(pid, name, "cpu_drop_to_zero", "critical", fmt.Sprintf("process %s(%d) CPU dropped from %.2f%% to 0%%", name, pid, prev), spec)
	p.publishAlertLocked(alert)
}

// tryMemoryAlert 检测进程内存是否超过阈值并触发 OOM 风险告警。
func (p *ProcessAgent) tryMemoryAlert(info ProcessInfo) {
	spec := p.matchService(info.Name)
	limit := p.memoryLimitPercent
	if spec != nil && spec.MemoryLimit > 0 {
		limit = spec.MemoryLimit
	}
	if limit <= 0 || info.Memory < limit {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	alert := p.newAlert(info.PID, info.Name, "memory_oom_risk", "critical", fmt.Sprintf("process %s(%d) memory %.2f%% exceeded %.2f%%", info.Name, info.PID, info.Memory, limit), spec)
	p.publishAlertLocked(alert)
}

// newAlert 构造一条告警对象，若有匹配的业务服务则填充重启相关字段。
func (p *ProcessAgent) newAlert(pid int32, name, typ, severity, message string, spec *ServiceSpec) Alert {
	alert := Alert{
		ID:        fmt.Sprintf("%s-%d-%d", typ, pid, time.Now().UnixNano()),
		PID:       pid,
		Name:      name,
		Type:      typ,
		Severity:  severity,
		Message:   message,
		CreatedAt: time.Now(),
	}
	if spec != nil {
		alert.Service = spec.Name
		alert.MatchName = spec.MatchName
		alert.RestartCommand = spec.RestartCommand
		alert.Restartable = strings.TrimSpace(spec.RestartCommand) != ""
	}
	return alert
}

// publishAlertLocked 在持有锁的情况下发布告警，支持冷却期内抑制重复告警。
func (p *ProcessAgent) publishAlertLocked(alert Alert) {
	key := alert.Type + ":" + alert.Name
	if alert.Service != "" {
		key = alert.Type + ":" + alert.Service
	}
	if until, ok := p.alertCooldown[key]; ok && time.Now().Before(until) {
		return
	}
	p.alertCooldown[key] = time.Now().Add(45 * time.Second)

	p.alerts = append([]Alert{alert}, p.alerts...)
	if len(p.alerts) > 80 {
		p.alerts = p.alerts[:80]
	}
	p.manager.Emit("process_alert_agent", "alert", alert)
}

// matchService 根据进程名匹配已配置的可自动修复业务服务，返回匹配到的服务配置或 nil。
func (p *ProcessAgent) matchService(processName string) *ServiceSpec {
	processName = strings.ToLower(strings.TrimSpace(processName))
	if processName == "" {
		return nil
	}
	for i := range p.services {
		spec := &p.services[i]
		match := strings.ToLower(strings.TrimSpace(spec.MatchName))
		if match == "" {
			match = strings.ToLower(strings.TrimSpace(spec.Name))
		}
		if match != "" && strings.Contains(processName, match) {
			return spec
		}
	}
	return nil
}

// recentAlerts 返回最近告警副本，默认最多 20 条。
func (p *ProcessAgent) recentAlerts() []Alert {
	p.mu.Lock()
	defer p.mu.Unlock()
	limit := len(p.alerts)
	if limit > 20 {
		limit = 20
	}
	out := make([]Alert, 0, limit)
	out = append(out, p.alerts[:limit]...)
	return out
}

// loadServiceSpecs 从环境变量 MONITORED_SERVICES 加载可自动修复的业务服务白名单。
func loadServiceSpecs() []ServiceSpec {
	raw := strings.TrimSpace(os.Getenv("MONITORED_SERVICES"))
	if raw == "" {
		return nil
	}

	var specs []ServiceSpec
	if strings.HasPrefix(raw, "[") {
		if err := json.Unmarshal([]byte(raw), &specs); err == nil {
			return normalizeServiceSpecs(specs)
		}
	}

	for _, part := range strings.Split(raw, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		fields := strings.Split(part, "|")
		spec := ServiceSpec{Name: strings.TrimSpace(fields[0])}
		if len(fields) > 1 {
			spec.MatchName = strings.TrimSpace(fields[1])
		}
		if len(fields) > 2 {
			spec.RestartCommand = strings.TrimSpace(fields[2])
		}
		if len(fields) > 3 {
			if v, err := strconv.ParseFloat(strings.TrimSpace(fields[3]), 32); err == nil {
				spec.MemoryLimit = float32(v)
			}
		}
		specs = append(specs, spec)
	}
	return normalizeServiceSpecs(specs)
}

// normalizeServiceSpecs 规范化服务配置列表，去除空白并补齐缺省字段。
func normalizeServiceSpecs(specs []ServiceSpec) []ServiceSpec {
	out := make([]ServiceSpec, 0, len(specs))
	for _, spec := range specs {
		spec.Name = strings.TrimSpace(spec.Name)
		spec.MatchName = strings.TrimSpace(spec.MatchName)
		spec.RestartCommand = strings.TrimSpace(spec.RestartCommand)
		if spec.Name == "" {
			continue
		}
		if spec.MatchName == "" {
			spec.MatchName = spec.Name
		}
		out = append(out, spec)
	}
	return out
}


