/*
 * main.js — 主线程 UI 胶水
 * 只做：编辑器 / 按钮 / 历史列表 / 日志面板 / 把 Worker 与 BroadcastChannel 的快照喂给 Canvas。
 * 所有调度与计算都在 Worker 中，主线程保持轻量不卡顿。
 */
'use strict';

/* ---------- 示例 DAG：覆盖全部验收点 ---------- */
const SAMPLE_DAG = {
  name: '订单处理流水线',
  nodes: [
    { id: 'load_order',   type: 'sleep',   params: { ms: 600 }, timeoutMs: 3000 },
    { id: 'check_stock',  type: 'emit',    params: { value: 42 } },
    { id: 'flaky_pay',    type: 'flaky',   params: { failTimes: 2, ms: 400 },
      retry: { maxAttempts: 4, baseMs: 400, factor: 2, jitter: 0.2 }, timeoutMs: 5000 },
    { id: 'slow_report',  type: 'compute', params: { ms: 6000 }, timeoutMs: 1500,
      retry: { maxAttempts: 2, baseMs: 300 } },
    { id: 'big_order',    type: 'sleep',   params: { ms: 800 } },
    { id: 'small_order',  type: 'sleep',   params: { ms: 500 } },
    { id: 'pay_failed',   type: 'sleep',   params: { ms: 300 } },
    { id: 'notify',       type: 'maybe',   params: { p: 0.3, ms: 700 },
      retry: { maxAttempts: 3, baseMs: 500, factor: 2 } },
    { id: 'archive',      type: 'sleep',   params: { ms: 400 }, join: 'any' },
  ],
  edges: [
    { from: 'load_order', to: 'check_stock' },
    { from: 'check_stock', to: 'flaky_pay' },
    { from: 'check_stock', to: 'big_order',  when: 'expr', expr: 'result.value > 10' },
    { from: 'check_stock', to: 'small_order', when: 'expr', expr: 'result.value <= 10' },
    { from: 'big_order',  to: 'notify' },
    { from: 'small_order', to: 'notify' },
    { from: 'flaky_pay',  to: 'notify',     when: 'onSuccess' },
    { from: 'flaky_pay',  to: 'pay_failed', when: 'onFailure' },
    { from: 'notify',     to: 'archive' },
    { from: 'pay_failed', to: 'archive' },
  ],
};

const $ = (sel) => document.querySelector(sel);
const worker = new Worker('js/worker.js');
const bc = new BroadcastChannel('workflow-engine');

let currentRunId = null;   // 画布当前展示的运行
let canvasView = null;
const logLines = [];
const MAX_LOG = 300;

/* ---------- 初始化 ---------- */
window.addEventListener('DOMContentLoaded', () => {
  $('#dag-input').value = JSON.stringify(SAMPLE_DAG, null, 2);
  canvasView = new WorkflowCanvas($('#dag-canvas'));

  $('#btn-validate').addEventListener('click', onValidate);
  $('#btn-run').addEventListener('click', onRun);
  $('#btn-stop').addEventListener('click', () => {
    if (currentRunId) worker.postMessage({ type: 'stop', runId: currentRunId });
  });
  $('#btn-refresh-runs').addEventListener('click', () => worker.postMessage({ type: 'list-runs' }));
  $('#btn-clear-runs').addEventListener('click', () => worker.postMessage({ type: 'clear-runs' }));
  $('#btn-clear-log').addEventListener('click', () => { logLines.length = 0; renderLog(); });

  worker.postMessage({ type: 'init' });
});

/* ---------- 操作 ---------- */
function readDef() {
  try {
    const def = JSON.parse($('#dag-input').value);
    $('#parse-error').textContent = '';
    return def;
  } catch (e) {
    $('#parse-error').textContent = 'JSON 解析失败: ' + e.message;
    return null;
  }
}

function onValidate() {
  const def = readDef();
  if (!def) return;
  worker.postMessage({ type: 'validate', def });
}

function onRun() {
  const def = readDef();
  if (!def) return;
  const maxConcurrent = Math.max(1, parseInt($('#concurrency').value, 10) || 4);
  worker.postMessage({ type: 'start', def, options: { maxConcurrent } });
}

/* ---------- Worker 消息 ---------- */
worker.addEventListener('message', (e) => {
  const msg = e.data;
  switch (msg.type) {
    case 'ready':
      if (msg.recovered && msg.recovered.length)
        pushLog('system', `检测到 ${msg.recovered.length} 个上次未完成的运行，已标记为 interrupted（崩溃恢复）`);
      break;
    case 'validated':
      renderValidation(msg);
      break;
    case 'start-rejected':
      pushLog('error', '运行被拒绝：DAG 校验未通过（' + msg.errors.map((x) => x.message).join('；') + '）');
      break;
    case 'snapshot':
      applySnapshot(msg.run);
      break;
    case 'event':
      pushLog(msg.event.kind, `[${msg.event.nodeId || 'run'}] ${msg.event.message}`, msg.event.ts);
      break;
    case 'runs':
      renderRuns(msg.runs);
      break;
    case 'run-detail':
      renderRunDetail(msg);
      break;
    case 'run-finished':
      worker.postMessage({ type: 'list-runs' });
      break;
    case 'engine-error':
      pushLog('error', '引擎错误: ' + msg.message);
      break;
  }
});

/* ---------- BroadcastChannel：其他标签页启动的运行也能实时看到 ---------- */
bc.addEventListener('message', (e) => {
  const msg = e.data;
  if (msg.type === 'snapshot' && msg.run.runId !== currentRunId && $('#follow-remote').checked) {
    applySnapshot(msg.run);
  } else if (msg.type === 'event' && $('#follow-remote').checked) {
    pushLog(msg.event.kind, `(远程) [${msg.event.nodeId || 'run'}] ${msg.event.message}`, msg.event.ts);
  }
});

function applySnapshot(run) {
  currentRunId = run.runId;
  canvasView.setRun(run);
  $('#btn-stop').disabled = !['running'].includes(run.status);
  $('#run-status').textContent =
    `运行 ${run.name} · 状态 ${run.status} · 并发 ${run.runningCount}/${run.maxConcurrent}`;
  $('#run-status').dataset.status = run.status;
}

/* ---------- 渲染 ---------- */
function renderValidation(msg) {
  const box = $('#validation-result');
  const lines = [];
  if (msg.ok) lines.push('✅ DAG 校验通过');
  for (const w of msg.warnings || []) lines.push('⚠️ ' + w.message);
  for (const er of msg.errors || []) lines.push('❌ ' + er.message);
  box.textContent = lines.join('\n');
  box.dataset.ok = msg.ok;
}

function renderRuns(runs) {
  const ul = $('#run-list');
  ul.innerHTML = '';
  for (const r of runs.slice(0, 30)) {
    const li = document.createElement('li');
    const time = new Date(r.startedAt).toLocaleTimeString();
    li.innerHTML =
      `<span class="run-name">${escapeHtml(r.name)}</span>` +
      `<span class="run-status" data-status="${r.status}">${r.status}</span>` +
      `<span class="run-time">${time}</span>` +
      `<button data-del="${r.runId}" title="删除">✕</button>`;
    li.addEventListener('click', (e) => {
      if (e.target.dataset.del) {
        worker.postMessage({ type: 'delete-run', runId: e.target.dataset.del });
      } else {
        worker.postMessage({ type: 'get-run', runId: r.runId });
      }
    });
    ul.appendChild(li);
  }
  if (!runs.length) ul.innerHTML = '<li class="empty">暂无历史运行</li>';
}

function renderRunDetail(msg) {
  // 从历史记录恢复一次运行的可视化（持久化验证）
  if (!msg.run) return;
  applySnapshot({
    runId: msg.run.runId, name: msg.run.name, status: msg.run.status,
    startedAt: msg.run.startedAt, endedAt: msg.run.endedAt,
    maxConcurrent: 0, runningCount: 0,
    def: msg.run.def,
    nodes: msg.nodes,
  });
  pushLog('system', `已从 IndexedDB 恢复运行 ${msg.run.runId}（${msg.events.length} 条事件）`);
}

function pushLog(kind, text, ts) {
  logLines.push({ kind, text, ts: ts || Date.now() });
  if (logLines.length > MAX_LOG) logLines.splice(0, logLines.length - MAX_LOG);
  renderLog();
}

function renderLog() {
  const el = $('#event-log');
  el.innerHTML = logLines.map((l) =>
    `<div class="log-line" data-kind="${l.kind}">` +
    `<span class="log-ts">${new Date(l.ts).toLocaleTimeString()}.${String(l.ts % 1000).padStart(3, '0')}</span> ` +
    escapeHtml(l.text) + '</div>'
  ).join('');
  el.scrollTop = el.scrollHeight;
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}
