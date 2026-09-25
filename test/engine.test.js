/*
 * 验收测试: node test/engine.test.js
 * 覆盖: DAG 校验 / 条件分支 / 重试退避 / 超时中断 / 并发调度 / 死锁检测 / 状态持久化
 */
'use strict';

const DAG = require('../js/dag.js');
const { createStore, createMemoryStore } = require('../js/store.js');
const { createEngine, backoffDelay, evalCondition } = require('../js/engine.js');

let passed = 0;
let failed = 0;
function assert(cond, name, extra) {
  if (cond) { passed += 1; console.log('  ✓ ' + name); }
  else { failed += 1; console.error('  ✗ ' + name + (extra ? ' — ' + extra : '')); }
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function runEngine(def, tasks, opts) {
  const store = createMemoryStore();
  const events = [];
  const engine = createEngine({
    def, tasks, store,
    emit: (kind, data) => events.push({ kind, data }),
    maxConcurrent: (opts && opts.maxConcurrent) || 4,
  });
  const { runId, done } = await engine.start();
  const run = await done;
  return { engine, store, events, runId, run };
}

async function testValidate() {
  console.log('\n[1] DAG 校验');
  const ok = DAG.validateDAG({
    id: 't', nodes: [{ id: 'a', task: 'x' }, { id: 'b', task: 'x' }],
    edges: [{ from: 'a', to: 'b' }],
  });
  assert(ok.ok && ok.order.join(',') === 'a,b', '合法 DAG 通过校验并给出拓扑序');

  const cyc = DAG.validateDAG({
    id: 't', nodes: [{ id: 'a', task: 'x' }, { id: 'b', task: 'x' }, { id: 'c', task: 'x' }],
    edges: [{ from: 'a', to: 'b' }, { from: 'b', to: 'c' }, { from: 'c', to: 'a' }],
  });
  assert(!cyc.ok && cyc.errors.some(e => e.includes('环')), '环被检测为死锁风险', JSON.stringify(cyc.errors));

  const dup = DAG.validateDAG({ id: 't', nodes: [{ id: 'a', task: 'x' }, { id: 'a', task: 'x' }], edges: [] });
  assert(!dup.ok && dup.errors.some(e => e.includes('重复')), '重复节点 id 被拒绝');

  const badEdge = DAG.validateDAG({ id: 't', nodes: [{ id: 'a', task: 'x' }], edges: [{ from: 'a', to: 'ghost' }] });
  assert(!badEdge.ok && badEdge.errors.some(e => e.includes('不存在')), '悬空边被拒绝');

  const selfLoop = DAG.validateDAG({ id: 't', nodes: [{ id: 'a', task: 'x' }], edges: [{ from: 'a', to: 'a' }] });
  assert(!selfLoop.ok, '自环被拒绝');
}

async function testConditional() {
  console.log('\n[2] 条件分支');
  const def = {
    id: 'cond',
    nodes: [
      { id: 'gate', task: 'gate' },
      { id: 'high', task: 'noop' },
      { id: 'low', task: 'noop' },
      { id: 'join', task: 'noop' },
    ],
    edges: [
      { from: 'gate', to: 'high', when: 'result.score >= 50' },
      { from: 'gate', to: 'low', when: 'result.score < 50' },
      { from: 'high', to: 'join' },
      { from: 'low', to: 'join' },
    ],
  };
  for (const score of [80, 20]) {
    const tasks = {
      gate: async () => ({ score }),
      noop: async () => 'ok',
    };
    const { store, runId, run } = await runEngine(def, tasks);
    const nodes = await store.getNodes(runId);
    const byId = Object.fromEntries(nodes.map(n => [n.nodeId, n]));
    const taken = score >= 50 ? 'high' : 'low';
    const skipped = score >= 50 ? 'low' : 'high';
    assert(byId[taken].status === 'success', `score=${score} 时 ${taken} 分支执行`);
    assert(byId[skipped].status === 'skipped', `score=${score} 时 ${skipped} 分支跳过`);
    assert(byId.join.status === 'success', `score=${score} 时汇聚节点执行`);
    assert(run.status === 'success', `score=${score} 时运行成功`);
  }

  // onFailure 异常链路
  const def2 = {
    id: 'err-chain',
    nodes: [{ id: 'boom', task: 'boom' }, { id: 'handler', task: 'noop' }, { id: 'after', task: 'noop' }],
    edges: [{ from: 'boom', to: 'handler', when: 'onFailure' }, { from: 'boom', to: 'after' }],
  };
  const { store: s2, runId: r2 } = await runEngine(def2, {
    boom: async () => { throw new Error('炸了'); },
    noop: async () => 'ok',
  });
  const n2 = Object.fromEntries((await s2.getNodes(r2)).map(n => [n.nodeId, n]));
  assert(n2.boom.status === 'failed', '失败节点终态为 failed');
  assert(n2.handler.status === 'success', 'onFailure 异常处理分支被激活');
  assert(n2.after.status === 'skipped', '默认成功边不触发, 下游跳过');
  assert(n2.after.causedBy === 'boom', '跳过节点记录异常来源 (异常链路)');
}

async function testRetryBackoff() {
  console.log('\n[3] 重试退避');
  // 退避公式: base * factor^(attempt-1), 递增
  const node = { backoff: 100, backoffFactor: 2 };
  const d1 = backoffDelay(node, 1), d2 = backoffDelay(node, 2), d3 = backoffDelay(node, 3);
  assert(d1 >= 100 && d1 < 160 && d2 >= 200 && d2 < 310 && d3 >= 400 && d3 < 610,
    '指数退避递增且带抖动', `${d1},${d2},${d3}`);

  const attempts = [];
  const timestamps = [];
  const def = {
    id: 'retry',
    nodes: [{ id: 'flaky', task: 'flaky', retries: 2, backoff: 60, backoffFactor: 2 }],
    edges: [],
  };
  const { store, runId, run } = await runEngine(def, {
    flaky: async (ctx) => {
      attempts.push(ctx.attempt);
      timestamps.push(Date.now());
      if (ctx.attempt < 3) throw new Error('fail-' + ctx.attempt);
      return 'recovered';
    },
  });
  const st = (await store.getNodes(runId))[0];
  assert(attempts.join(',') === '1,2,3', '重试到第 3 次成功', attempts.join(','));
  assert(st.status === 'success' && st.attempt === 3, '节点最终成功且记录尝试次数');
  const gap1 = timestamps[1] - timestamps[0];
  const gap2 = timestamps[2] - timestamps[1];
  assert(gap1 >= 55 && gap2 >= 110 && gap2 > gap1, '重试间隔按指数增长', `${gap1}ms,${gap2}ms`);

  // 重试耗尽 -> failed
  const def2 = {
    id: 'retry-exhaust',
    nodes: [{ id: 'bad', task: 'bad', retries: 1, backoff: 20 }],
    edges: [],
  };
  const { store: s3, runId: r3, run: run3 } = await runEngine(def2, {
    bad: async () => { throw new Error('永远失败'); },
  });
  const st3 = (await s3.getNodes(r3))[0];
  assert(st3.status === 'failed' && st3.attempt === 2, '重试耗尽后失败 (1+1 次尝试)');
  assert(run3.status === 'failed', '运行终态为 failed');
}

async function testTimeout() {
  console.log('\n[4] 超时中断');
  let aborted = false;
  const def = {
    id: 'timeout',
    nodes: [{ id: 'slow', task: 'slow', timeout: 80, retries: 0 }],
    edges: [],
  };
  const t0 = Date.now();
  const { store, runId } = await runEngine(def, {
    slow: async (ctx) => {
      await new Promise((resolve, reject) => {
        const timer = setTimeout(resolve, 5000);
        ctx.signal.addEventListener('abort', () => { clearTimeout(timer); reject(new Error('aborted')); });
      });
      return 'never';
    },
  });
  const elapsed = Date.now() - t0;
  const st = (await store.getNodes(runId))[0];
  assert(st.status === 'timeout', '超时节点终态为 timeout');
  assert(st.error && st.error.type === 'timeout', '记录 timeout 错误类型');
  assert(elapsed < 2000, '任务被及时中断, 未等待 5s', elapsed + 'ms');
  assert(st.finishedAt - st.startedAt < 2000, '中断信号已送达任务');
}

async function testConcurrency() {
  console.log('\n[5] 并发调度');
  let running = 0;
  let peak = 0;
  const def = {
    id: 'conc',
    nodes: Array.from({ length: 6 }, (_, i) => ({ id: 'n' + i, task: 'work' })),
    edges: [],
  };
  const { run } = await runEngine(def, {
    work: async () => {
      running += 1;
      peak = Math.max(peak, running);
      await sleep(60);
      running -= 1;
      return 'ok';
    },
  }, { maxConcurrent: 2 });
  assert(peak === 2, '并发峰值等于上限 2', 'peak=' + peak);
  assert(run.status === 'success', '全部节点完成');

  // 有依赖时下游不会提前执行
  const order = [];
  const def2 = {
    id: 'order',
    nodes: [{ id: 'a', task: 't' }, { id: 'b', task: 't' }],
    edges: [{ from: 'a', to: 'b' }],
  };
  await runEngine(def2, { t: async (ctx) => { order.push(ctx); return 1; } });
  const { store: s4, runId: r4 } = await runEngine(def2, {
    t: async () => { return 1; },
  });
  const n4 = Object.fromEntries((await s4.getNodes(r4)).map(n => [n.nodeId, n]));
  assert(n4.a.finishedAt <= n4.b.startedAt, '依赖顺序: a 完成后 b 才开始');
}

async function testDeadlock() {
  console.log('\n[6] 死锁检测');
  // 静态: 环在校验期被拦截 (见 [1])
  const cyc = DAG.validateDAG({
    id: 't', nodes: [{ id: 'a', task: 'x' }, { id: 'b', task: 'x' }],
    edges: [{ from: 'a', to: 'b' }, { from: 'b', to: 'a' }],
  });
  assert(!cyc.ok, '静态死锁 (环) 在校验期拦截');

  // 运行时: detectRuntimeDeadlock 判定
  const dl = DAG.detectRuntimeDeadlock(
    { a: { nodeId: 'a', status: 'failed' }, b: { nodeId: 'b', status: 'pending' }, c: { nodeId: 'c', status: 'pending' } },
    { nodes: [], edges: [] }
  );
  assert(dl.deadlocked && dl.stuckNodes.join(',') === 'b,c', '运行时死锁: 无活跃节点且存在 pending');
  const noDl = DAG.detectRuntimeDeadlock(
    { a: { nodeId: 'a', status: 'running' }, b: { nodeId: 'b', status: 'pending' } },
    { nodes: [], edges: [] }
  );
  assert(!noDl.deadlocked, '有活跃节点时不误判死锁');
}

async function testPersistence() {
  console.log('\n[7] 状态持久化');
  const store = createMemoryStore();
  const def = {
    id: 'persist',
    nodes: [{ id: 'a', task: 't' }, { id: 'b', task: 't' }],
    edges: [{ from: 'a', to: 'b' }],
  };
  const events = [];
  const engine = createEngine({ def, tasks: { t: async () => 42 }, store, emit: (k, d) => events.push(d) });
  const { runId, done } = await engine.start();
  const run = await done;

  const savedRun = await store.getRun(runId);
  assert(savedRun && savedRun.status === 'success', '运行记录已持久化');
  assert(savedRun.def && savedRun.def.nodes.length === 2, 'DAG 定义随运行保存 (可恢复)');
  const nodes = await store.getNodes(runId);
  assert(nodes.length === 2 && nodes.every(n => n.status === 'success'), '全部节点状态已持久化');
  assert(nodes.find(n => n.nodeId === 'a').result === 42, '节点结果已持久化');
  const evts = await store.getEvents(runId);
  assert(evts.length >= 4 && evts.every(e => e.runId === runId), '事件日志已持久化');
  const runs = await store.listRuns();
  assert(runs.some(r => r.runId === runId), '运行列表可查询');
}

async function testCancel() {
  console.log('\n[8] 取消');
  const def = {
    id: 'cancel',
    nodes: [{ id: 'a', task: 'slow' }, { id: 'b', task: 'slow' }],
    edges: [{ from: 'a', to: 'b' }],
  };
  const store = createMemoryStore();
  const engine = createEngine({
    def, store, emit: () => {},
    tasks: { slow: async (ctx) => { await sleep(3000); return 1; } },
  });
  const { runId, done } = await engine.start();
  await sleep(100);
  await engine.cancel();
  const run = await done;
  assert(run.status === 'cancelled', '运行被取消');
  const nodes = await store.getNodes(runId);
  assert(nodes.find(n => n.nodeId === 'a').status === 'failed', '运行中节点被中止');
}

(async () => {
  console.log('工作流引擎验收测试');
  await testValidate();
  await testConditional();
  await testRetryBackoff();
  await testTimeout();
  await testConcurrency();
  await testDeadlock();
  await testPersistence();
  await testCancel();
  console.log(`\n结果: ${passed} 通过, ${failed} 失败`);
  process.exit(failed ? 1 : 0);
})().catch(err => { console.error(err); process.exit(1); });
