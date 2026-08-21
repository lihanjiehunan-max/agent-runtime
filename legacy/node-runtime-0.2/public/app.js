const $ = (selector) => document.querySelector(selector);
const state = { selectedRunId: null, leases: new Map(), runs: [] };

function token() { return $('#access-token').value.trim(); }

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { 'content-type': 'application/json', authorization: `Bearer ${token()}`, ...(options.headers ?? {}) },
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  });
  const data = await response.json();
  if (!response.ok) throw new Error(`${data.error?.code ?? response.status}: ${data.error?.message ?? 'Request failed'}`);
  return data;
}

function notify(message) {
  const toast = $('#toast');
  toast.textContent = message;
  toast.classList.add('show');
  setTimeout(() => toast.classList.remove('show'), 2800);
}

async function refreshRuns() {
  const data = await api('/api/runs');
  state.runs = data.items;
  $('#metric-total').textContent = state.runs.length;
  $('#metric-running').textContent = state.runs.filter((run) => run.status === 'RUNNING').length;
  $('#metric-attention').textContent = state.runs.filter((run) => ['WAITING_MANUAL', 'FAILED', 'RECOVERING'].includes(run.status)).length;
  const container = $('#runs');
  container.innerHTML = state.runs.length ? state.runs.map((run) => `
    <article class="run-card ${run.runId === state.selectedRunId ? 'selected' : ''}" data-run-id="${run.runId}">
      <header><strong>${escapeHtml(run.goal)}</strong><span class="badge ${run.status}">${run.status}</span></header>
      <p>Epoch ${run.executionEpoch} · ${run.lastEventSeq} events · ${formatTime(run.updatedAt)}</p>
    </article>`).join('') : '<p class="empty">暂无任务，先创建一个运行。</p>';
  container.querySelectorAll('.run-card').forEach((card) => card.addEventListener('click', () => selectRun(card.dataset.runId)));
  if (state.selectedRunId) await renderTimeline();
}

async function selectRun(runId) {
  state.selectedRunId = runId;
  $('#actions').classList.remove('hidden');
  await refreshRuns();
}

async function renderTimeline() {
  const run = state.runs.find((item) => item.runId === state.selectedRunId);
  if (!run) return;
  $('#detail-title').textContent = run.goal;
  const data = await api(`/api/runs/${run.runId}/events`);
  $('#timeline').innerHTML = data.items.length ? data.items.slice().reverse().map((event) => `
    <article class="event">
      <header><strong>${event.eventType}</strong><time>${formatTime(event.createdAt)}</time></header>
      <p>seq ${event.eventSeq} · epoch ${event.executionEpoch} · ${escapeHtml(JSON.stringify(event.payload))}</p>
    </article>`).join('') : '<p class="empty">尚无事件。</p>';
}

async function createAndStart() {
  const run = await api('/api/runs', { method: 'POST', body: { goal: $('#goal').value } });
  const lease = await api(`/api/runs/${run.runId}/lease`, { method: 'POST', body: {} });
  state.leases.set(run.runId, lease);
  await api(`/api/runs/${run.runId}/start`, { method: 'POST', body: { lease } });
  state.selectedRunId = run.runId;
  $('#actions').classList.remove('hidden');
  await refreshRuns();
  notify('任务已创建并进入 RUNNING');
}

async function saveContext() {
  const saved = await api('/api/contexts', { method: 'POST', body: { content: $('#context-content').value, classification: 'internal' } });
  $('#context-ref').textContent = saved.contextRefId;
  notify('上下文已保存为不可变引用');
}

async function runAction(action) {
  const runId = state.selectedRunId;
  if (!runId) return;
  if (action === 'recover') {
    const result = await api(`/api/runs/${runId}/recover`, { method: 'POST', body: { reason: 'operator requested recovery from the local console' } });
    state.leases.set(runId, result);
    notify(`已由新 Epoch ${result.executionEpoch} 接管`);
  } else {
    let lease = state.leases.get(runId);
    if (!lease) {
      lease = await api(`/api/runs/${runId}/lease`, { method: 'POST', body: { force: true, reason: 'operator requested takeover from the local console' } });
      state.leases.set(runId, lease);
    }
    if (action === 'checkpoint') await api(`/api/runs/${runId}/checkpoint`, { method: 'POST', body: { lease, kind: 'FULL', state: { phase: 'operator-checkpoint', savedAt: new Date().toISOString() } } });
    if (action === 'complete') await api(`/api/runs/${runId}/complete`, { method: 'POST', body: { lease, result: { summary: 'operator completed' } } });
    notify(action === 'checkpoint' ? 'Checkpoint 已原子提交' : '任务已完成');
  }
  await refreshRuns();
}

function escapeHtml(value) {
  return value.replace(/[&<>'"]/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' })[char]);
}
function formatTime(value) { return new Intl.DateTimeFormat('zh-CN', { hour: '2-digit', minute: '2-digit', second: '2-digit' }).format(new Date(value)); }
function guard(work) { return () => work().catch((error) => notify(error.message)); }

$('#create').addEventListener('click', guard(createAndStart));
$('#save-context').addEventListener('click', guard(saveContext));
$('#refresh').addEventListener('click', guard(refreshRuns));
$('#access-token').addEventListener('change', guard(refreshRuns));
$('#actions').addEventListener('click', (event) => { if (event.target.dataset.action) guard(() => runAction(event.target.dataset.action))(); });
refreshRuns().catch((error) => notify(error.message));
setInterval(() => refreshRuns().catch(() => {}), 5000);
