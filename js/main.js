/*
 * main.js — 主线程: UI  wiring / Worker 通信 / BroadcastChannel 跨标签页同步 / Canvas 渲染。
 * 引擎全部在 Worker 内执行, 主线程只做绘制, 保证不卡。
 */
'use strict';

const DEMO_DEF = {
  id: 'demo-dag',
  nodes: [
    { id: 'fetch', task: 'fetchData', timeout: 3000, retries: 1, backoff: 300, params: { source: 'orders-db' } },
    { id: 'clean', task: 'process', timeout: 5000 },
    { id: 'analyze', task: 'process', timeout: 5000 },
    { id: 'flaky', task: 'flaky', retries: 3, backoff: 400, backoffFactor: 2, params: { failTimes: 2 } },
    { id: 'slowReport', task: 'slow', timeout: 1500, retries: 1, backoff: 200, params: { duration: 8000 } },
    { id: 'reportA', task: 'process' },
    { id: 'reportB', task: 'process' },
    { id: 'aggregate', task: 'aggregate' },
    { id: 'alert', task: 'alert' },
  ],
  edges: [
    { from: 'fetch', to: 'clean' },
    { from: 'clean', to: 'analyze' },
    { from: 'fetch', to: 'flaky' },
    { from: 'fetch', to: 'slowReport' },
    { from: 'analyze', to: 'reportA', when: 'result.score >= 50' },
    { from: 'analyze', to: 'reportB', when: 'result.score < 50' },
    { from: 'reportA', to: 'aggregate' },
    { from: 'reportB', to: 'aggregate' },
    { from: 'flaky', to: 'alert', when: 'onFailure' },
  ],
};

const canvas = document.getElementById('canvas');
const viz = WFViz.createViz(canvas);
viz.setGraph(DEMO_DEF);

const worker = new Worker('js/worker.js');
const channel = new BroadcastChannel('workflow-engine');

const runStatusEl = document.getElementById('run-status');
const eventLogEl = document.getElementById('event-log');
const runListEl = document.getElementById('run-list');
const nodeDetailEl = document.getElementById('node-detail');
const fpsEl = document.getElementById('fps');

let currentRunId = null;

function setRunStatus(text, cls) {
  runStatusEl.textContent = text;
  runStatusEl.className = 'status ' + (cls || '');
}

function appendEvent(ev) {
  const div = document.createElement('div');
  div.className = 'ev ev-' + (ev.level || 'info');
  const time = new Date(ev.ts).toLocaleTimeString('zh-CN', { hour12: false });
  div.textContent = '[' + time + '] ' + ev.message;
  eventLogEl.appendChild(div);
  eventLogEl.scrollTop = eventLogEl.scrollHeight;
  while (eventLogEl.children.length > 300) eventLogEl.removeChild(eventLogEl.firstChild);
}

function renderRunList(runs) {
  runListEl.innerHTML = '';
  for (const run of runs.slice(0, 20)) {
    const li = document.createElement('li');
    const time = run.updatedAt ? new Date(run.updatedAt).toLocaleTimeString('zh-CN', { hour12: false }) : '';
    li.textContent = run.runId + ' · ' + run.status + ' · ' + time;
    li.onclick = () => worker.postMessage({ type: 'getRunDetail', runId: run.runId });
    runListEl.appendChild(li);
  }
}

function handleMessage(msg) {
  const { type, payload } = msg;
  switch (type) {
    case 'ready':
      setRunStatus('Worker 就绪', 'idle');
      worker.postMessage({ type: 'listRuns' });
      break;
    case 'started':
      currentRunId = payload.runId;
      setRunStatus('运行中: ' + payload.runId, 'running');
      break;
    case 'node':
      if (currentRunId && payload.runId !== currentRunId) break;
      viz.updateNode(payload.node);
      break;
    case 'event':
      if (currentRunId && payload.runId !== currentRunId) break;
      appendEvent(payload);
      break;
    case 'run':
      if (payload.status === 'running') {
        setRunStatus('运行中: ' + payload.runId, 'running');
      } else {
        setRunStatus('运行结束: ' + payload.status + (payload.stuckNodes ? ' (死锁: ' + payload.stuckNodes.join(',') + ')' : ''), payload.status);
        worker.postMessage({ type: 'listRuns' });
      }
      break;
    case 'finished':
      worker.postMessage({ type: 'listRuns' });
      break;
    case 'runs':
      renderRunList(payload.runs);
      break;
    case 'runDetail': {
      const { run, nodes, events } = payload;
      if (run && run.def) {
        viz.setGraph(run.def);
        viz.setNodes(nodes);
        currentRunId = run.runId;
        setRunStatus('查看历史: ' + run.runId + ' · ' + run.status, run.status);
        eventLogEl.innerHTML = '';
        events.sort((a, b) => a.ts - b.ts).forEach(appendEvent);
      }
      break;
    }
    case 'cleared':
      runListEl.innerHTML = '';
      eventLogEl.innerHTML = '';
      break;
    case 'error':
      setRunStatus('错误: ' + payload.errors.join('; '), 'failed');
      appendEvent({ level: 'error', message: payload.errors.join('; '), ts: Date.now() });
      break;
  }
}

worker.onmessage = (e) => handleMessage(e.data);
channel.onmessage = (e) => handleMessage(e.data);

document.getElementById('btn-start').onclick = () => {
  viz.setGraph(DEMO_DEF);
  eventLogEl.innerHTML = '';
  const maxConcurrent = parseInt(document.getElementById('concurrency').value, 10) || 4;
  worker.postMessage({ type: 'start', def: DEMO_DEF, maxConcurrent });
};
document.getElementById('btn-cancel').onclick = () => worker.postMessage({ type: 'cancel' });
document.getElementById('btn-clear').onclick = () => worker.postMessage({ type: 'clear' });
document.getElementById('btn-refresh').onclick = () => worker.postMessage({ type: 'listRuns' });

viz.setOnNodeClick((nodeId, state) => {
  const def = DEMO_DEF.nodes.find(n => n.id === nodeId);
  nodeDetailEl.textContent = JSON.stringify({ def, state }, null, 2);
});

// 主线程流畅度指示: 持续显示 rAF FPS, 证明引擎运行时不阻塞主线程
let frames = 0;
let lastFpsTs = performance.now();
(function fpsLoop(now) {
  frames += 1;
  if (now - lastFpsTs >= 1000) {
    fpsEl.textContent = '主线程 FPS: ' + frames;
    frames = 0;
    lastFpsTs = now;
  }
  requestAnimationFrame(fpsLoop);
})(performance.now());
