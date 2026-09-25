/*
 * worker.js — 引擎运行的 Web Worker。
 * 主线程通过 postMessage 控制; 状态通过 postMessage + BroadcastChannel 双通道广播,
 * 因此多个标签页都能实时看到同一运行状态。状态同时持久化到 IndexedDB。
 */
'use strict';

importScripts('dag.js', 'store.js', 'tasks.js', 'engine.js');

const channel = new BroadcastChannel('workflow-engine');
let storePromise = WFStore.createStore();
let currentEngine = null;

function broadcast(type, payload) {
  const msg = { type, payload, ts: Date.now() };
  channel.postMessage(msg);
  self.postMessage(msg);
}

async function persistEmitFactory() {
  const store = await storePromise;
  return (kind, data) => broadcast(kind === 'node' ? 'node' : kind === 'run' ? 'run' : 'event', data);
}

async function handleStart(msg) {
  const store = await storePromise;
  const def = msg.def;

  const validation = DAG.validateDAG(def);
  if (!validation.ok) {
    broadcast('error', { errors: validation.errors });
    return;
  }

  if (currentEngine) {
    await currentEngine.cancel();
  }

  const emit = await persistEmitFactory();
  const engine = WFEngine.createEngine({
    def,
    tasks: WFTasks.tasks,
    store,
    emit,
    maxConcurrent: msg.maxConcurrent || 4,
  });
  currentEngine = engine;
  const { runId, done } = await engine.start();
  broadcast('started', { runId });
  await done;
  broadcast('finished', { runId });
}

async function handleCancel() {
  if (currentEngine) await currentEngine.cancel();
}

async function handleListRuns() {
  const store = await storePromise;
  const runs = await store.listRuns();
  runs.sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));
  broadcast('runs', { runs });
}

async function handleGetRunDetail(msg) {
  const store = await storePromise;
  const [run, nodes, events] = await Promise.all([
    store.getRun(msg.runId),
    store.getNodes(msg.runId),
    store.getEvents(msg.runId),
  ]);
  broadcast('runDetail', { run, nodes, events });
}

async function handleClear() {
  const store = await storePromise;
  await store.clear();
  broadcast('cleared', {});
}

const handlers = {
  start: handleStart,
  cancel: handleCancel,
  listRuns: handleListRuns,
  getRunDetail: handleGetRunDetail,
  clear: handleClear,
};

self.onmessage = (e) => {
  const msg = e.data || {};
  const handler = handlers[msg.type];
  if (handler) {
    Promise.resolve(handler(msg)).catch(err => {
      broadcast('error', { errors: [String(err && err.stack || err)] });
    });
  }
};

broadcast('ready', {});
