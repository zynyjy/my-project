// ===== DOM 元素 =====
var cpuChartEl = document.getElementById('cpu-chart');
var memChartEl = document.getElementById('memory-chart');
var alertsList = document.getElementById('alerts-list');
var alertCount = document.getElementById('alert-count');
var mcpStatus = document.getElementById('mcp-status');
var animeWidget = document.getElementById('anime-widget');
var chatOverlay = document.getElementById('chat-overlay');
var overlayClose = document.getElementById('overlay-close');
var overlayMessages = document.getElementById('overlay-messages');
var overlayInput = document.getElementById('overlay-input');
var overlaySend = document.getElementById('overlay-send');
var overlayComposer = document.getElementById('overlay-composer');

var SESSION_KEY = 'agent_go_session_id';
var sessionId = getOrCreateSessionId();
var userId = 'demo-user-001';
var roles = ['employee'];

// ===== Chart.js 配置（无网格线） =====
var PROCESS_COLORS = [
  '#4A90D9', '#E74C3C', '#2ECC71', '#F39C12', '#9B59B6',
  '#1ABC9C', '#E67E22', '#3498DB', '#D35400', '#27AE60'
];

var cpuChart = null;
var memChart = null;
var historyCache = [];

function makeChartConfig() {
  return {
    type: 'line',
    data: { labels: [], datasets: [] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 400 },
      interaction: { mode: 'index', intersect: false },
      plugins: {
        tooltip: {
          mode: 'index',
          intersect: false,
          backgroundColor: '#fff',
          titleColor: '#2c3e50',
          bodyColor: '#2c3e50',
          borderColor: '#e8ecf1',
          borderWidth: 1,
          padding: 10,
          callbacks: {
            label: function(ctx) {
              return ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(1) + '%';
            }
          }
        },
        legend: {
          position: 'bottom',
          labels: {
            boxWidth: 10,
            boxHeight: 10,
            padding: 14,
            font: { size: 11 },
            usePointStyle: true,
            pointStyleWidth: 8
          }
        }
      },
      scales: {
        x: {
          display: true,
          grid: { display: false },
          ticks: { maxTicksLimit: 6, font: { size: 10 }, color: '#95a5a6' }
        },
        y: {
          display: true,
          beginAtZero: true,
          max: 100,
          grid: { display: false },
          ticks: {
            callback: function(v) { return v + '%'; },
            font: { size: 10 },
            color: '#95a5a6'
          }
        }
      }
    }
  };
}

// ===== 折线图初始化 =====
function initCharts() {
  if (cpuChartEl) cpuChart = new Chart(cpuChartEl, makeChartConfig());
  if (memChartEl) memChart = new Chart(memChartEl, makeChartConfig());
}

// ===== 获取历史数据 =====
async function fetchHistory() {
  try {
    var resp = await fetch('/api/monitor/history');
    var json = await resp.json();
    if (!json.ok || !json.data) return;
    var snaps = json.data.snapshots || [];
    if (snaps.length < 1) return;
    historyCache = snaps;
    updateCharts();
  } catch (_) {}
}

// ===== 构建 Chart.js 数据集 =====
function buildDatasets(snaps, field) {
  var labels = snaps.map(function(s) {
    return s.updated_at ? new Date(s.updated_at).toLocaleTimeString() : '--';
  });

  // 统计所有进程的平均使用率取 Top 8
  var procs = {};
  snaps.forEach(function(s) {
    (s.processes || []).forEach(function(p) {
      if (!p.name) return;
      if (!procs[p.name]) procs[p.name] = { sum: 0, count: 0 };
      procs[p.name].sum += (p[field] || 0);
      procs[p.name].count += 1;
    });
  });

  var ranked = Object.entries(procs)
    .filter(function(e) { return e[1].sum > 0; })
    .sort(function(a, b) { return (b[1].sum / b[1].count) - (a[1].sum / a[1].count); })
    .slice(0, 8)
    .map(function(e) { return e[0]; });

  if (ranked.length === 0) return { labels: labels, datasets: [] };

  var datasets = ranked.map(function(name, idx) {
    var data = snaps.map(function(s) {
      var proc = (s.processes || []).find(function(p) { return p.name === name; });
      return proc ? Number(proc[field]) || 0 : null;
    });
    return {
      label: name,
      data: data,
      borderColor: PROCESS_COLORS[idx % PROCESS_COLORS.length],
      backgroundColor: 'transparent',
      tension: 0.35,
      pointRadius: 0,
      pointHoverRadius: 4,
      pointHoverBackgroundColor: PROCESS_COLORS[idx % PROCESS_COLORS.length],
      borderWidth: 1.8,
      spanGaps: false
    };
  });

  return { labels: labels, datasets: datasets };
}

// ===== 更新折线图 =====
function updateCharts() {
  if (!historyCache.length) return;

  if (cpuChart && cpuChartEl) {
    var cpuData = buildDatasets(historyCache, 'cpu_percent');
    cpuChart.data.labels = cpuData.labels;
    cpuChart.data.datasets = cpuData.datasets;
    cpuChart.update('none');
  }

  if (memChart && memChartEl) {
    var memData = buildDatasets(historyCache, 'memory_percent');
    memChart.data.labels = memData.labels;
    memChart.data.datasets = memData.datasets;
    memChart.update('none');
  }
}

// ===== 告警 SSE =====
var alertEvSource = null;

function connectAlertSSE() {
  if (alertEvSource) { alertEvSource.close(); alertEvSource = null; }
  alertEvSource = new EventSource('/api/agents/stream');

  alertEvSource.addEventListener('agent', function(e) {
    try {
      var item = JSON.parse(e.data);
      if (item.agent === 'process_alert_agent' && item.status === 'alert') {
        prependAlert(item.detail);
      }
      if (item.agent === 'mcp_agent' && item.detail && typeof item.detail === 'object') {
        renderMcpState(item.detail);
      }
    } catch (_) {}
  });

  alertEvSource.addEventListener('snapshot', function(e) {
    try {
      var data = JSON.parse(e.data);
      if (data.process_monitor_agent && data.process_monitor_agent.alerts) {
        renderAlerts(data.process_monitor_agent.alerts);
      }
      if (data.mcp_agent) renderMcpState(data.mcp_agent);
    } catch (_) {}
  });

  alertEvSource.onerror = function() {
    alertEvSource.close();
    setTimeout(connectAlertSSE, 4000);
  };
}

var alertCache = [];

function prependAlert(alert) {
  alertCache.unshift(alert);
  if (alertCache.length > 50) alertCache = alertCache.slice(0, 50);
  renderAlerts(alertCache);
}

function renderAlerts(alerts) {
  if (!alertsList) return;
  var list = alerts || alertCache;
  alertsList.innerHTML = '';
  alertCount.textContent = String(list.length);

  if (!list.length) {
    alertsList.innerHTML = '<div class="empty-state">暂无告警</div>';
    return;
  }

  list.slice(0, 20).forEach(function(a) {
    var node = document.createElement('div');
    node.className = 'alert-item';
    node.innerHTML =
      '<strong>' + escapeHtml(a.type || 'alert') + (a.service ? ' / ' + escapeHtml(a.service) : '') + '</strong>' +
      '<div>' + escapeHtml(a.message || '') + '</div>' +
      '<span>' + (a.restartable ? '可自动修复' : '') + ' &middot; ' +
      (a.created_at ? new Date(a.created_at).toLocaleTimeString() : '--') + '</span>';
    alertsList.appendChild(node);
  });
}

// ===== MCP 状态 =====
function renderMcpState(state) {
  if (!mcpStatus) return;
  var tools = Array.isArray(state.tools) ? state.tools : [];
  if (state.configured) {
    mcpStatus.textContent = 'MCP ' + tools.length;
    mcpStatus.className = 'status-chip ok-chip';
  } else {
    mcpStatus.textContent = 'MCP';
    mcpStatus.className = 'status-chip muted-chip';
  }
}

// ===== 动漫客服 + 聊天弹窗 =====
if (animeWidget) {
  animeWidget.addEventListener('click', function() {
    chatOverlay.classList.remove('hidden');
    if (overlayInput) overlayInput.focus();
  });
}

if (overlayClose) {
  overlayClose.addEventListener('click', function() {
    chatOverlay.classList.add('hidden');
  });
}

if (chatOverlay) {
  chatOverlay.addEventListener('click', function(e) {
    if (e.target === chatOverlay) chatOverlay.classList.add('hidden');
  });
}

function addOverlayBubble(role, text) {
  var div = document.createElement('div');
  div.className = 'bubble ' + role;
  div.textContent = text;
  overlayMessages.appendChild(div);
  overlayMessages.scrollTop = overlayMessages.scrollHeight;
  return div;
}

if (overlayComposer) {
  overlayComposer.addEventListener('submit', function(e) {
    e.preventDefault();
    var text = overlayInput.value.trim();
    if (!text || (overlaySend && overlaySend.disabled)) return;
    overlayInput.value = '';

    var ph = overlayMessages.querySelector('.overlay-placeholder');
    if (ph) ph.remove();

    addOverlayBubble('user', text);
    var asstBubble = addOverlayBubble('assistant', '');
    setOverlayBusy(true);

    fetch('/api/chat/stream', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: text,
        session_id: sessionId,
        user_id: userId,
        roles: roles
      })
    }).then(function(resp) {
      if (!resp.ok || !resp.body) throw new Error('HTTP ' + resp.status);
      var reader = resp.body.getReader();
      var decoder = new TextDecoder();
      var buf = '';
      function pump() {
        reader.read().then(function(r) {
          if (r.done) { setOverlayBusy(false); return; }
          buf += decoder.decode(r.value, { stream: true });
          var idx;
          while ((idx = buf.indexOf('\n\n')) >= 0) {
            handleOverlayPacket(buf.slice(0, idx), asstBubble);
            buf = buf.slice(idx + 2);
          }
          pump();
        }).catch(function() { setOverlayBusy(false); });
      }
      pump();
    }).catch(function(err) {
      asstBubble.textContent = '请求失败: ' + (err.message || 'unknown');
      setOverlayBusy(false);
    });
  });
}

function handleOverlayPacket(packet, bubble) {
  var lines = packet.split('\n');
  var event = '', data = '';
  lines.forEach(function(line) {
    if (line.startsWith('event:')) event = line.slice(6).trim();
    if (line.startsWith('data:')) data += line.slice(5).trim();
  });
  if (!data) return;
  try {
    var wrapped = JSON.parse(data);
    var value = wrapped.value;
    if (event === 'token') {
      bubble.textContent += value;
      overlayMessages.scrollTop = overlayMessages.scrollHeight;
    }
    if (event === 'done') setOverlayBusy(false);
    if (event === 'error') {
      bubble.textContent = '请求失败: ' + value;
      setOverlayBusy(false);
    }
  } catch (_) {}
}

function setOverlayBusy(b) {
  if (overlaySend) overlaySend.disabled = b;
  if (overlayInput) overlayInput.disabled = b;
}

// ===== 滚动动画 =====
function initScrollAnimations() {
  if (!('IntersectionObserver' in window)) return;
  var obs = new IntersectionObserver(function(entries) {
    entries.forEach(function(entry) {
      if (entry.isIntersecting) {
        entry.target.classList.add('is-visible');
        obs.unobserve(entry.target);
      }
    });
  }, { threshold: 0.1, rootMargin: '0px 0px -30px 0px' });

  document.querySelectorAll('.fade-card').forEach(function(el) {
    obs.observe(el);
  });
}

// ===== 工具函数 =====
function getOrCreateSessionId() {
  var id = localStorage.getItem(SESSION_KEY);
  if (id) return id;
  id = 'sess_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
  localStorage.setItem(SESSION_KEY, id);
  return id;
}

function escapeHtml(str) {
  var div = document.createElement('div');
  div.textContent = str || '';
  return div.innerHTML;
}

// ===== 启动 =====
document.addEventListener('DOMContentLoaded', function() {
  initCharts();
  fetchHistory();
  connectAlertSSE();
  initScrollAnimations();
});

// 每 5 秒拉取历史数据
setInterval(fetchHistory, 5000);
