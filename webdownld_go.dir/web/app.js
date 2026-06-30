// ====== JWT 令牌管理 ======
function getToken() {
  return localStorage.getItem('accessToken') || '';
}

function getAuthHeaders() {
  const token = getToken();
  if (!token) return {};
  return { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' };
}

function isLoggedIn() {
  return !!getToken();
}

function showToast(msg, isError) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'toast' + (isError ? ' error' : '') + ' show';
  setTimeout(() => t.classList.remove('show'), 3000);
}

// 未登录跳转登录页。
if (!isLoggedIn()) {
  window.location.href = '/assets/login.html';
}

// ====== 顶部导航 ======
const userDisplay = document.getElementById('userDisplay');
const memberBadge = document.getElementById('memberBadge');
const rechargeBtn = document.getElementById('rechargeBtn');
const logoutBtn = document.getElementById('logoutBtn');

userDisplay.textContent = '用户: ' + (localStorage.getItem('username') || '—');
if (localStorage.getItem('isMember') === '1') {
  memberBadge.style.display = 'inline';
}

logoutBtn.addEventListener('click', () => {
  localStorage.clear();
  window.location.href = '/assets/login.html';
});

// ====== 充值弹窗 ======
let selectedPlanIndex = 0;
let plans = [];

rechargeBtn.addEventListener('click', async () => {
  try {
    const resp = await fetch('/api/payment/plans', { headers: getAuthHeaders() }).then(r => r.json());
    if (!resp.ok) { showToast('获取套餐失败', true); return; }
    plans = resp['plans'] || [];
    renderPlans();
    document.getElementById('rechargeModal').style.display = 'flex';
  } catch (e) { showToast('网络错误', true); }
});

function renderPlans() {
  const list = document.getElementById('planList');
  list.innerHTML = '';
  plans.forEach((p, i) => {
    const item = document.createElement('div');
    item.className = 'plan-item' + (i === selectedPlanIndex ? ' selected' : '');
    item.innerHTML = '<span class="plan-name">' + p['plan_name'] + '（' + p['duration_days'] + '天）</span>' +
                     '<span class="plan-price">¥' + (p['price_cent'] / 100).toFixed(2) + '</span>';
    item.addEventListener('click', () => { selectedPlanIndex = i; renderPlans(); });
    list.appendChild(item);
  });
}

document.getElementById('modalCancel').addEventListener('click', () => {
  document.getElementById('rechargeModal').style.display = 'none';
});

document.getElementById('modalConfirm').addEventListener('click', async () => {
  try {
    const resp = await fetch('/api/payment/order', {
      method: 'POST',
      headers: getAuthHeaders(),
      body: JSON.stringify({ 'plan_id': selectedPlanIndex }),
    }).then(r => r.json());
    if (!resp.ok) { showToast(resp.error || '创建订单失败', true); return; }
    // 跳转到支付宝支付页面。
    if (resp['pay_url']) {
      window.open(resp['pay_url'], '_blank');
    }
    document.getElementById('rechargeModal').style.display = 'none';
    showToast('订单已创建，请完成支付');
  } catch (e) { showToast('网络错误', true); }
});

// ====== 文件上传 ======
const dropZone = document.getElementById('dropZone');
const fileInput = document.getElementById('fileInput');
const progressPanel = document.getElementById('progressPanel');
const fileGrid = document.getElementById('fileGrid');
const refreshBtn = document.getElementById('refreshBtn');
const selected = new Set();

['dragenter', 'dragover'].forEach(ev => dropZone.addEventListener(ev, e => {
  e.preventDefault();
  dropZone.classList.add('dragover');
}));
['dragleave', 'drop'].forEach(ev => dropZone.addEventListener(ev, e => {
  e.preventDefault();
  dropZone.classList.remove('dragover');
}));
dropZone.addEventListener('drop', e => {
  handleFiles([...(e.dataTransfer?.files || [])]);
});
fileInput.addEventListener('change', e => handleFiles([...(e.target.files || [])]));
refreshBtn.addEventListener('click', loadFiles);

function createProgressItem(name) {
  const item = document.createElement('div');
  item.className = 'progress-item';
  item.innerHTML = '<strong>' + name + '</strong><div class="bar"><i></i></div><small>0%</small>';
  progressPanel.prepend(item);
  return item;
}

function setProgress(item, percent, text) {
  item.querySelector('i').style.width = Math.min(100, Math.round(percent)) + '%';
  item.querySelector('small').textContent = text || (Math.round(percent) + '%');
}

async function handleFiles(files) {
  for (const file of files) {
    const pItem = createProgressItem(file.name);
    try {
      await uploadFile(file, pItem);
      setProgress(pItem, 100, '完成');
    } catch (e) {
      setProgress(pItem, 100, '失败: ' + e.message);
    }
  }
  await loadFiles();
}

async function uploadFile(file, progressItem) {
  const initResp = await fetch('/api/uploads/init', {
    method: 'POST',
    headers: getAuthHeaders(),
    body: JSON.stringify({ name: file.name, size: file.size, owner: localStorage.getItem('username') || 'web-user', permission: 'rw' }),
  }).then(r => r.json());
  if (!initResp.ok) throw new Error(initResp.error || 'init failed');
  if (initResp.instant_upload) {
    setProgress(progressItem, 100, '秒传完成');
    return;
  }

  const uploadID = initResp.upload_id;
  const totalChunks = initResp.total_chunks;
  const chunkSize = initResp.chunk_size;
  const statusResp = await fetch('/api/uploads/' + uploadID + '/status', { headers: getAuthHeaders() }).then(r => r.json());
  const uploaded = new Set(statusResp.received || []);
  const queue = [];
  for (let i = 0; i < totalChunks; i++) { if (!uploaded.has(i)) queue.push(i); }

  let done = uploaded.size;
  setProgress(progressItem, (done / totalChunks) * 100, '续传: ' + done + '/' + totalChunks);

  const concurrency = 4;
  async function worker() {
    while (queue.length) {
      const idx = queue.shift();
      const start = idx * chunkSize;
      const end = Math.min(file.size, start + chunkSize);
      const blob = file.slice(start, end);
      const chunkResp = await fetch('/api/uploads/' + uploadID + '/chunks/' + idx, {
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + getToken() },
        body: blob,
      }).then(r => r.json());
      if (!chunkResp.ok) throw new Error(chunkResp.error || 'chunk ' + idx + ' failed');
      done++;
      setProgress(progressItem, (done / totalChunks) * 100, '上传中: ' + done + '/' + totalChunks);
    }
  }

  await Promise.all(new Array(concurrency).fill(0).map(() => worker()));
  const doneResp = await fetch('/api/uploads/' + uploadID + '/complete', {
    method: 'POST',
    headers: getAuthHeaders(),
  }).then(r => r.json());
  if (!doneResp.ok) throw new Error(doneResp.error || 'complete failed');
}

function formatSize(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  if (bytes < 1024 * 1024 * 1024) return (bytes / 1024 / 1024).toFixed(1) + ' MB';
  return (bytes / 1024 / 1024 / 1024).toFixed(1) + ' GB';
}

// ====== 文件列表 ======
async function loadFiles() {
  try {
    const resp = await fetch('/api/files', { headers: getAuthHeaders() }).then(r => r.json());
    if (!resp.ok) {
      // token 过期，跳转登录。
      if (resp.error === '令牌无效或已过期') { localStorage.clear(); window.location.href = '/assets/login.html'; }
      return;
    }
    fileGrid.innerHTML = '';
    (resp.data || []).forEach(f => {
      const card = document.createElement('article');
      card.className = 'file-card card-enter';
      card.dataset.id = f.file_id;
      card.innerHTML =
        '<h3>' + f.name + '</h3>' +
        '<p>' + formatSize(f.size) + '</p>' +
        '<p>' + new Date(f.created_at).toLocaleString() + '</p>' +
        '<p>分片: ' + f.total_chunks + '</p>' +
        '<div class="card-actions">' +
          '<button data-action="download" class="btn btn-primary btn-sm">下载</button>' +
          '<button data-action="delete" class="btn btn-danger btn-sm">删除</button>' +
        '</div>';
      card.addEventListener('click', e => {
        if (e.target.dataset.action === 'download' || e.target.dataset.action === 'delete') return;
        toggleSelect(f.file_id, card, e.metaKey || e.ctrlKey);
      });
      card.querySelector('[data-action="download"]').addEventListener('click', async e => {
        e.stopPropagation();
        await downloadFile(f.file_id, f.name, card);
      });
      card.querySelector('[data-action="delete"]').addEventListener('click', async e => {
        e.stopPropagation();
        if (confirm('确定要删除 "' + f.name + '" 吗？')) {
          await deleteFile(f.file_id);
          await loadFiles();
        }
      });
      fileGrid.appendChild(card);
    });
  } catch (e) { console.error('loadFiles error', e); }
}

async function deleteFile(fileID) {
  const resp = await fetch('/api/files/' + fileID, {
    method: 'DELETE',
    headers: getAuthHeaders(),
  }).then(r => r.json());
  if (!resp.ok) showToast(resp.error || '删除失败', true);
}

function toggleSelect(fileID, card, additive) {
  if (!additive) {
    selected.clear();
    document.querySelectorAll('.file-card.selected').forEach(n => n.classList.remove('selected'));
  }
  if (selected.has(fileID)) {
    selected.delete(fileID);
    card.classList.remove('selected');
  } else {
    selected.add(fileID);
    card.classList.add('selected');
  }
}

// ====== 文件下载 ======
async function downloadFile(fileID, name, card) {
  let progressEl = null;
  if (card) {
    progressEl = document.createElement('div');
    progressEl.className = 'download-progress';
    progressEl.innerHTML = '<div class="bar"><i></i></div><small>准备下载...</small>';
    card.appendChild(progressEl);
  }

  try {
    const manifestResp = await fetch('/api/files/' + fileID + '/manifest', { headers: getAuthHeaders() }).then(r => r.json());
    if (!manifestResp.ok) throw new Error('manifest failed');
    const m = manifestResp.manifest;
    const chunks = m.chunks || [];
    const totalChunks = chunks.length;
    const results = new Array(totalChunks);
    let completed = 0;

    const queue = chunks.map(c => c.index);
    const concurrency = 4;

    function updateProgress() {
      if (progressEl) {
        const pct = (completed / totalChunks) * 100;
        progressEl.querySelector('i').style.width = Math.round(pct) + '%';
        progressEl.querySelector('small').textContent = '下载中 ' + completed + '/' + totalChunks;
      }
    }

    async function worker() {
      while (queue.length) {
        const idx = queue.shift();
        const bytes = await fetch('/api/files/' + fileID + '/chunks/' + idx, {
          headers: { 'Authorization': 'Bearer ' + getToken() },
        }).then(r => r.arrayBuffer());
        results[idx] = new Uint8Array(bytes);
        completed++;
        updateProgress();
      }
    }
    updateProgress();
    await Promise.all(new Array(concurrency).fill(0).map(() => worker()));

    const blob = new Blob(results, { type: 'application/octet-stream' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = name;
    a.click();
    URL.revokeObjectURL(a.href);

    if (progressEl) {
      progressEl.querySelector('small').textContent = '下载完成';
      setTimeout(() => progressEl.remove(), 2000);
    }
  } catch (e) {
    if (progressEl) {
      progressEl.querySelector('small').textContent = '下载失败: ' + e.message;
    }
  }
}

// ====== 初始加载 ======
loadFiles();
