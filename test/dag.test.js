'use strict';
const assert = require('node:assert');
const DAG = require('../js/dag.js');

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log('  ok -', name); }
  catch (e) { console.error('  FAIL -', name); console.error(e); process.exitCode = 1; }
}

/* ---- 校验 ---- */
t('合法 DAG 通过校验', () => {
  const def = {
    nodes: [{ id: 'a' }, { id: 'b' }, { id: 'c' }],
    edges: [{ from: 'a', to: 'b' }, { from: 'a', to: 'c' }],
  };
  const r = DAG.validateDAG(def);
  assert.strictEqual(r.ok, true, JSON.stringify(r.errors));
});

t('重复节点 id 报错', () => {
  const r = DAG.validateDAG({ nodes: [{ id: 'a' }, { id: 'a' }], edges: [] });
  assert.ok(!r.ok && r.errors.some((e) => e.code === 'DUP_NODE'));
});

t('边端点不存在报错', () => {
  const r = DAG.validateDAG({ nodes: [{ id: 'a' }], edges: [{ from: 'a', to: 'ghost' }] });
  assert.ok(!r.ok && r.errors.some((e) => e.code === 'EDGE_TO_MISSING'));
});

t('自环报错', () => {
  const r = DAG.validateDAG({ nodes: [{ id: 'a' }], edges: [{ from: 'a', to: 'a' }] });
  assert.ok(!r.ok && r.errors.some((e) => e.code === 'SELF_LOOP'));
});

t('环检测（死锁图）', () => {
  const def = {
    nodes: [{ id: 'a' }, { id: 'b' }, { id: 'c' }],
    edges: [{ from: 'a', to: 'b' }, { from: 'b', to: 'c' }, { from: 'c', to: 'a' }],
  };
  const r = DAG.validateDAG(def);
  assert.ok(!r.ok && r.errors.some((e) => e.code === 'CYCLE'), JSON.stringify(r));
  const cycle = DAG.findCycle(def);
  assert.ok(cycle && cycle.length === 4 && cycle[0] === cycle[3]);
});

t('非法 when / 非法表达式报错', () => {
  const def = {
    nodes: [{ id: 'a' }, { id: 'b' }],
    edges: [{ from: 'a', to: 'b', when: 'nonsense' }],
  };
  assert.ok(DAG.validateDAG(def).errors.some((e) => e.code === 'BAD_WHEN'));
  def.edges[0] = { from: 'a', to: 'b', when: 'expr', expr: 'return 1' };
  assert.ok(DAG.validateDAG(def).errors.some((e) => e.code === 'EXPR_SYNTAX'));
});

t('非法 timeout / retry 报错', () => {
  const def = { nodes: [{ id: 'a', timeoutMs: -5 }, { id: 'b', retry: { maxAttempts: 0 } }], edges: [] };
  const r = DAG.validateDAG(def);
  assert.ok(r.errors.some((e) => e.code === 'BAD_TIMEOUT'));
  assert.ok(r.errors.some((e) => e.code === 'BAD_RETRY'));
});

/* ---- 拓扑 ---- */
t('拓扑排序与分层', () => {
  const def = {
    nodes: [{ id: 'a' }, { id: 'b' }, { id: 'c' }, { id: 'd' }],
    edges: [{ from: 'a', to: 'b' }, { from: 'a', to: 'c' }, { from: 'b', to: 'd' }, { from: 'c', to: 'd' }],
  };
  const { order } = DAG.topoSort(def);
  assert.strictEqual(order.length, 4);
  assert.ok(order.indexOf('a') < order.indexOf('b'));
  assert.ok(order.indexOf('b') < order.indexOf('d'));
  const { layers } = DAG.topoLayers(def);
  assert.deepStrictEqual(layers.map((l) => l.slice().sort()), [['a'], ['b', 'c'], ['d']]);
});

/* ---- 条件边 ---- */
t('always / onSuccess / onFailure', () => {
  assert.strictEqual(DAG.evalEdge({ when: 'always' }, 'success'), true);
  assert.strictEqual(DAG.evalEdge({ when: 'always' }, 'failed'), true);
  assert.strictEqual(DAG.evalEdge({ when: 'always' }, 'skipped'), false);
  assert.strictEqual(DAG.evalEdge({ when: 'onSuccess' }, 'success'), true);
  assert.strictEqual(DAG.evalEdge({ when: 'onSuccess' }, 'failed'), false);
  assert.strictEqual(DAG.evalEdge({ when: 'onFailure' }, 'failed'), true);
  assert.strictEqual(DAG.evalEdge({ when: 'onFailure' }, 'success'), false);
});

t('expr 条件表达式', () => {
  const e = { when: 'expr', expr: 'result.value > 10' };
  assert.strictEqual(DAG.evalEdge(e, 'success', { value: 42 }), true);
  assert.strictEqual(DAG.evalEdge(e, 'success', { value: 3 }), false);
  assert.strictEqual(DAG.evalEdge(e, 'failed', { value: 42 }), false); // 失败不求值
  assert.strictEqual(DAG.evalEdge({ when: 'expr', expr: 'boom(' }, 'success', {}), false); // 运行错误 -> false
});

/* ---- 退避 ---- */
t('指数退避 + 上限', () => {
  const retry = { baseMs: 100, factor: 2, maxMs: 500, jitter: 0 };
  assert.strictEqual(DAG.backoffDelay(1, retry), 0);
  assert.strictEqual(DAG.backoffDelay(2, retry), 200);
  assert.strictEqual(DAG.backoffDelay(3, retry), 400);
  assert.strictEqual(DAG.backoffDelay(4, retry), 500); // capped
  assert.strictEqual(DAG.backoffDelay(10, retry), 500);
});

t('退避抖动在范围内', () => {
  const retry = { baseMs: 1000, factor: 2, jitter: 0.5 };
  for (let i = 0; i < 200; i++) {
    const d = DAG.backoffDelay(2, retry);
    assert.ok(d >= 1000 && d <= 3000, `d=${d}`);
  }
});

/* ---- 运行时死锁 ---- */
t('运行时死锁判定', () => {
  assert.strictEqual(DAG.isRuntimeDeadlocked({
    nodes: [{ status: 'success' }, { status: 'pending' }],
  }), true);
  assert.strictEqual(DAG.isRuntimeDeadlocked({
    nodes: [{ status: 'running' }, { status: 'pending' }],
  }), false);
  assert.strictEqual(DAG.isRuntimeDeadlocked({
    nodes: [{ status: 'success' }, { status: 'skipped' }],
  }), false);
});

t('离线阻塞节点检测', () => {
  const def = {
    nodes: [{ id: 'a' }, { id: 'b' }, { id: 'c' }],
    edges: [{ from: 'a', to: 'b', when: 'onSuccess' }, { from: 'b', to: 'c' }],
  };
  // a 失败且没有 onFailure 边 -> b、c 都永久阻塞
  const blocked = DAG.blockedNodes(def, { a: 'failed' });
  assert.ok(blocked.has('b') && blocked.has('c'));
});

console.log(`\n${passed} tests passed${process.exitCode ? ' (with failures)' : ''}`);
