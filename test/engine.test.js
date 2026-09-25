'use strict';
/*
 * 引擎集成测试：在 Node vm 中模拟 Web Worker 环境，加载真实 js/worker.js 驱动运行。
 * 覆盖：条件分支 / 重试退避 / 超时中断 / 并发调度 / 环拒绝 / 持久化 / 手动停止。
 */
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');

let passed = 0;
function t(name, fn) {
  return Promise.resolve()
    .then(fn)
    .then(() => { passed++; console.log('  ok -', name); })
    .catch((e) => { console.error('  FAIL -', name); console.error(e); process.exitCode = 1; });
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/* ---- 内存版 WorkflowDB（替代 IndexedDB 验证持久化调用） ---- */
class FakeDB {
  static async open() { FakeDB.instance = new FakeDB(); return FakeDB.instance; }
  constructor() { this.runs = new Map(); this.nodes = new Map(); this.events = []; }
  async putRun(r) { this.runs.set(r.runId, structuredClone(r)); }
  async getRun(id) { return this.runs.get(id) || null; }
  async listRuns() { return [...this.runs.values()]; }
  async putNodeState(s) { this.nodes.set(s.runId + ':' + s.nodeId, structuredClone(s)); }
  async putNodeStates(ss) { for (const s of ss) await this.putNodeState(s); }
  async getNodeStates(id) { return [...this.nodes.values()].filter((s) => s.runId === id); }
  async appendEvent(e) { this.events.push(structuredClone(e)); }
  async listEvents(id) { return this.events.filter((e) => e.runId === id); }
  async deleteRun(id) { this.runs.delete(id); }
  async recoverInterrupted() { return []; }
}

/* ---- 在 vm 中加载真实 worker.js ---- */
function createEngine() {
  const outbox = [];
  const sandbox = {
    console, performance, AbortController, structuredClone,
    setTimeout, clearTimeout, setInterval, clearInterval,
    Promise, Date, Math, JSON, Object, Array, Map, Set, Error, Number, String, Boolean,
    BroadcastChannel: class { postMessage() {} },
  };
  sandbox.self = sandbox;
  sandbox.postMessage = (msg) => outbox.push(msg);
  sandbox.importScripts = (...files) => {
    for (const f of files) {
      const code = fs.readFileSync(path.join(__dirname, '..', 'js', f), 'utf8');
      vm.runInContext(code, sandbox, { filename: f });
    }
  };
  vm.createContext(sandbox);
  vm.runInContext(fs.readFileSync(path.join(__dirname, '..', 'js', 'worker.js'), 'utf8'), sandbox, { filename: 'worker.js' });
  sandbox.WorkflowDB = FakeDB; // 换掉真实 IndexedDB 实现
  return {
    outbox,
    send: (msg) => sandbox.self.onmessage({ data: msg }),
    db: () => FakeDB.instance,
  };
}

async function waitFor(outbox, pred, timeoutMs = 8000, what = '') {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const hit = outbox.find(pred);
    if (hit) return hit;
    await sleep(10);
  }
  throw new Error('waitFor 超时: ' + what);
}

async function startRun(engine, def, options = {}) {
  await engine.send({ type: 'init' });
  await waitFor(engine.outbox, (m) => m.type === 'ready', 2000, 'ready');
  await engine.send({ type: 'start', def, options });
  const snap = await waitFor(engine.outbox, (m) => m.type === 'snapshot', 2000, 'first snapshot');
  return snap.run.runId;
}

async function waitFinish(engine, runId, timeoutMs = 10000) {
  return waitFor(engine.outbox, (m) => m.type === 'run-finished' && m.runId === runId, timeoutMs, 'run-finished');
}

const nodeOf = (snap, id) => snap.run.nodes.find((n) => n.nodeId === id);
const lastSnap = (outbox, runId) =>
  outbox.filter((m) => m.type === 'snapshot' && m.run.runId === runId).pop();

(async () => {
  /* ---- 1. 条件分支（expr 双向） ---- */
  await t('条件分支：expr 走通一侧、另一侧跳过', async () => {
    const engine = createEngine();
    const def = {
      name: 'branch',
      nodes: [
        { id: 'src', type: 'emit', params: { value: 42 } },
        { id: 'big', type: 'sleep', params: { ms: 20 } },
        { id: 'small', type: 'sleep', params: { ms: 20 } },
      ],
      edges: [
        { from: 'src', to: 'big', when: 'expr', expr: 'result.value > 10' },
        { from: 'src', to: 'small', when: 'expr', expr: 'result.value <= 10' },
      ],
    };
    const runId = await startRun(engine, def);
    const fin = await waitFinish(engine, runId);
    assert.strictEqual(fin.status, 'success');
    const snap = lastSnap(engine.outbox, runId);
    assert.strictEqual(nodeOf(snap, 'big').status, 'success');
    assert.strictEqual(nodeOf(snap, 'small').status, 'skipped');

    // 反向：value=5 时 small 走通
    const engine2 = createEngine();
    def.nodes[0].params.value = 5;
    const runId2 = await startRun(engine2, def);
    await waitFinish(engine2, runId2);
    const snap2 = lastSnap(engine2.outbox, runId2);
    assert.strictEqual(nodeOf(snap2, 'big').status, 'skipped');
    assert.strictEqual(nodeOf(snap2, 'small').status, 'success');
  });

  /* ---- 2. onFailure 分支 ---- */
  await t('条件分支：onFailure 路由到补偿节点', async () => {
    const engine = createEngine();
    const def = {
      name: 'fail-route',
      nodes: [
        { id: 'a', type: 'flaky', params: { failTimes: 99, ms: 10 }, retry: { maxAttempts: 2, baseMs: 10, jitter: 0 } },
        { id: 'compensate', type: 'sleep', params: { ms: 10 } },
        { id: 'normal', type: 'sleep', params: { ms: 10 } },
      ],
      edges: [
        { from: 'a', to: 'compensate', when: 'onFailure' },
        { from: 'a', to: 'normal', when: 'onSuccess' },
      ],
    };
    const runId = await startRun(engine, def);
    const fin = await waitFinish(engine, runId);
    assert.strictEqual(fin.status, 'failed'); // a 失败 => run failed
    const snap = lastSnap(engine.outbox, runId);
    assert.strictEqual(nodeOf(snap, 'a').status, 'failed');
    assert.strictEqual(nodeOf(snap, 'compensate').status, 'success');
    assert.strictEqual(nodeOf(snap, 'normal').status, 'skipped');
  });

  /* ---- 3. 重试退避 ---- */
  await t('重试退避：flaky 第 3 次成功，退避间隔递增', async () => {
    const engine = createEngine();
    const def = {
      name: 'retry',
      nodes: [{ id: 'f', type: 'flaky', params: { failTimes: 2, ms: 10 },
        retry: { maxAttempts: 4, baseMs: 50, factor: 2, jitter: 0 } }],
      edges: [],
    };
    const t0 = Date.now();
    const runId = await startRun(engine, def);
    const fin = await waitFinish(engine, runId);
    const total = Date.now() - t0;
    assert.strictEqual(fin.status, 'success');
    const snap = lastSnap(engine.outbox, runId);
    assert.strictEqual(nodeOf(snap, 'f').attempt, 3);
    // 退避：attempt2 等 100ms，attempt3 等 200ms => 总耗时 >= 300ms
    assert.ok(total >= 290, `总耗时 ${total}ms 应 >= 290ms（含退避等待）`);
    const retryEvents = engine.db().events.filter((e) => e.kind === 'node-retry');
    assert.strictEqual(retryEvents.length, 2);
  });

  /* ---- 4. 超时中断 ---- */
  await t('超时中断：5000ms 任务在 200ms 被中止', async () => {
    const engine = createEngine();
    const def = {
      name: 'timeout',
      nodes: [{ id: 'slow', type: 'sleep', params: { ms: 5000 }, timeoutMs: 200 }],
      edges: [],
    };
    const t0 = Date.now();
    const runId = await startRun(engine, def);
    const fin = await waitFinish(engine, runId);
    const total = Date.now() - t0;
    assert.strictEqual(fin.status, 'failed');
    assert.ok(total < 2000, `应在 200ms 左右中断，实际 ${total}ms`);
    const snap = lastSnap(engine.outbox, runId);
    assert.match(nodeOf(snap, 'slow').error, /timeout/);
    const evts = engine.db().events.filter((e) => e.kind === 'node-timeout');
    assert.strictEqual(evts.length, 1);
  });

  /* ---- 5. 并发调度 ---- */
  await t('并发调度：maxConcurrent=2 时 4 个并行任务分两批', async () => {
    const engine = createEngine();
    const def = {
      name: 'concurrency',
      nodes: [0, 1, 2, 3].map((i) => ({ id: 'n' + i, type: 'sleep', params: { ms: 200 } })),
      edges: [],
    };
    const t0 = Date.now();
    const runId = await startRun(engine, def, { maxConcurrent: 2 });
    await waitFinish(engine, runId);
    const total = Date.now() - t0;
    assert.ok(total >= 390, `两批执行应 >= 390ms，实际 ${total}ms`);
    const maxRunning = Math.max(...engine.outbox
      .filter((m) => m.type === 'snapshot' && m.run.runId === runId)
      .map((m) => m.run.runningCount));
    assert.strictEqual(maxRunning, 2, `峰值并发应为 2，实际 ${maxRunning}`);
  });

  /* ---- 6. 环（死锁图）被拒绝 ---- */
  await t('DAG 校验：有环的运行被拒绝', async () => {
    const engine = createEngine();
    await engine.send({ type: 'init' });
    await waitFor(engine.outbox, (m) => m.type === 'ready', 2000);
    const def = {
      name: 'cyclic',
      nodes: [{ id: 'a' }, { id: 'b' }],
      edges: [{ from: 'a', to: 'b' }, { from: 'b', to: 'a' }],
    };
    await engine.send({ type: 'start', def, options: {} });
    const rej = await waitFor(engine.outbox, (m) => m.type === 'start-rejected', 2000, 'start-rejected');
    assert.ok(rej.errors.some((e) => e.code === 'CYCLE'));
  });

  /* ---- 7. 状态持久化 ---- */
  await t('持久化：运行与节点状态全部写入存储', async () => {
    const engine = createEngine();
    const def = {
      name: 'persist',
      nodes: [
        { id: 'a', type: 'sleep', params: { ms: 30 } },
        { id: 'b', type: 'sleep', params: { ms: 30 } },
      ],
      edges: [{ from: 'a', to: 'b' }],
    };
    const runId = await startRun(engine, def);
    await waitFinish(engine, runId);
    const run = await engine.db().getRun(runId);
    assert.strictEqual(run.status, 'success');
    const nodes = await engine.db().getNodeStates(runId);
    assert.strictEqual(nodes.length, 2);
    assert.ok(nodes.every((n) => n.status === 'success'));
    assert.ok((await engine.db().listEvents(runId)).length >= 4);
  });

  /* ---- 8. 手动停止 ---- */
  await t('停止：运行中断所有任务并标记 blocked', async () => {
    const engine = createEngine();
    const def = {
      name: 'stop',
      nodes: [
        { id: 'a', type: 'sleep', params: { ms: 5000 } },
        { id: 'b', type: 'sleep', params: { ms: 10 } },
      ],
      edges: [{ from: 'a', to: 'b' }],
    };
    const runId = await startRun(engine, def);
    await waitFor(engine.outbox, (m) =>
      m.type === 'snapshot' && m.run.runId === runId &&
      nodeOf(m, 'a').status === 'running', 2000, 'a running');
    await engine.send({ type: 'stop', runId });
    const fin = await waitFinish(engine, runId);
    assert.strictEqual(fin.status, 'stopped');
    const snap = lastSnap(engine.outbox, runId);
    assert.strictEqual(nodeOf(snap, 'a').status, 'blocked');
    assert.strictEqual(nodeOf(snap, 'b').status, 'blocked');
  });

  /* ---- 9. join=any 聚合 ---- */
  await t('join=any：任一上游走通即触发', async () => {
    const engine = createEngine();
    const def = {
      name: 'join-any',
      nodes: [
        { id: 'ok', type: 'sleep', params: { ms: 10 } },
        { id: 'bad', type: 'flaky', params: { failTimes: 9, ms: 10 } },
        { id: 'sink', type: 'sleep', params: { ms: 10 }, join: 'any' },
      ],
      edges: [
        { from: 'ok', to: 'sink', when: 'onSuccess' },
        { from: 'bad', to: 'sink', when: 'onSuccess' },
      ],
    };
    const runId = await startRun(engine, def);
    await waitFinish(engine, runId);
    const snap = lastSnap(engine.outbox, runId);
    assert.strictEqual(nodeOf(snap, 'sink').status, 'success'); // ok 走通即可触发
  });

  console.log(`\n${passed} integration tests passed${process.exitCode ? ' (with failures)' : ''}`);
  process.exit(process.exitCode || 0);
})();
