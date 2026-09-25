/*
 * dag.js — DAG 纯逻辑层（无 DOM/IDB 依赖）
 * 负责：结构校验、环检测、拓扑分层、条件边求值、重试退避计算、运行时死锁判定。
 * 同时可运行于 Web Worker(importScripts) 与 Node(单测)。
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.DAG = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  const NODE_STATUS = ['pending', 'ready', 'running', 'retrying', 'success', 'failed', 'skipped', 'blocked'];
  const RUN_STATUS = ['idle', 'running', 'paused', 'success', 'failed', 'deadlocked', 'stopped', 'interrupted'];
  const EDGE_WHEN = ['always', 'onSuccess', 'onFailure', 'expr'];

  /* ---------- 基础工具 ---------- */

  function indexNodes(def) {
    const map = Object.create(null);
    for (const n of def.nodes || []) map[n.id] = n;
    return map;
  }

  function buildAdjacency(def) {
    const out = Object.create(null);
    const inc = Object.create(null);
    for (const n of def.nodes || []) { out[n.id] = []; inc[n.id] = []; }
    for (const e of def.edges || []) {
      if (out[e.from] && inc[e.to]) out[e.from].push(e);
      if (inc[e.to] && out[e.from]) inc[e.to].push(e);
    }
    return { out, inc };
  }

  // DFS 三色标记法找环，返回第一个环的节点路径（如 ['a','b','c','a']），无环返回 null
  function findCycle(def) {
    const { out } = buildAdjacency(def);
    const color = Object.create(null); // 0=白 1=灰 2=黑
    const stack = [];
    let cycle = null;

    function dfs(u) {
      if (cycle) return;
      color[u] = 1;
      stack.push(u);
      for (const e of out[u]) {
        const v = e.to;
        if (color[v] === 1) {
          const start = stack.indexOf(v);
          cycle = stack.slice(start).concat(v);
          return;
        }
        if (!color[v]) dfs(v);
        if (cycle) return;
      }
      stack.pop();
      color[u] = 2;
    }

    for (const n of def.nodes || []) {
      if (!color[n.id]) dfs(n.id);
      if (cycle) break;
    }
    return cycle;
  }

  // Kahn 拓扑排序；返回 {order, indegreeLeft}（indegreeLeft 非 0 的节点即处于环中/被环阻塞）
  function topoSort(def) {
    const { out, inc } = buildAdjacency(def);
    const indeg = Object.create(null);
    for (const id of Object.keys(out)) indeg[id] = inc[id].length;
    const queue = Object.keys(indeg).filter((id) => indeg[id] === 0);
    const order = [];
    while (queue.length) {
      const u = queue.shift();
      order.push(u);
      for (const e of out[u]) {
        indeg[e.to]--;
        if (indeg[e.to] === 0) queue.push(e.to);
      }
    }
    return { order, indegreeLeft: indeg };
  }

  // 拓扑分层：同一层的节点互相无依赖，可并发执行，用于调度与画布布局
  function topoLayers(def) {
    const { out, inc } = buildAdjacency(def);
    const depth = Object.create(null);
    const remaining = Object.create(null);
    for (const id of Object.keys(out)) {
      depth[id] = inc[id].length === 0 ? 0 : -1;
      remaining[id] = inc[id].length;
    }
    let queue = Object.keys(out).filter((id) => remaining[id] === 0);
    const layers = [];
    while (queue.length) {
      const layer = queue.slice();
      layers.push(layer);
      const next = [];
      for (const u of layer) {
        for (const e of out[u]) {
          depth[e.to] = Math.max(depth[e.to], depth[u] + 1);
          remaining[e.to]--;
          if (remaining[e.to] === 0) next.push(e.to);
        }
      }
      queue = next;
    }
    return { layers, depth };
  }

  /* ---------- DAG 校验 ---------- */

  // 返回 {ok, errors:[{code,message}], warnings:[{code,message}]}
  function validateDAG(def) {
    const errors = [];
    const warnings = [];
    const fail = (code, message) => errors.push({ code, message });
    const warn = (code, message) => warnings.push({ code, message });

    if (!def || typeof def !== 'object') {
      fail('BAD_DEF', '定义必须是对象');
      return { ok: false, errors, warnings };
    }
    if (!Array.isArray(def.nodes) || def.nodes.length === 0) {
      fail('NO_NODES', 'nodes 必须是非空数组');
      return { ok: false, errors, warnings };
    }
    if (!Array.isArray(def.edges)) def.edges = [];

    const ids = new Set();
    for (const n of def.nodes) {
      if (!n || typeof n.id !== 'string' || !n.id) { fail('BAD_NODE_ID', `节点 id 非法: ${JSON.stringify(n)}`); continue; }
      if (ids.has(n.id)) fail('DUP_NODE', `节点 id 重复: ${n.id}`);
      ids.add(n.id);
      if (!n.type) warn('NO_TYPE', `节点 ${n.id} 未指定 type，按 sleep 处理`);
      if (n.join && n.join !== 'all' && n.join !== 'any') fail('BAD_JOIN', `节点 ${n.id} 的 join 只能是 all|any`);
      if (n.timeoutMs != null && (!(Number.isFinite(n.timeoutMs)) || n.timeoutMs <= 0))
        fail('BAD_TIMEOUT', `节点 ${n.id} 的 timeoutMs 必须是正数`);
      if (n.retry != null && (!Number.isInteger(n.retry.maxAttempts) || n.retry.maxAttempts < 1))
        fail('BAD_RETRY', `节点 ${n.id} 的 retry.maxAttempts 必须是 >=1 的整数`);
    }

    def.edges.forEach((e, i) => {
      const where = `第 ${i} 条边`;
      if (!e || typeof e.from !== 'string' || typeof e.to !== 'string')
        return fail('BAD_EDGE', `${where} 缺少 from/to`);
      if (!ids.has(e.from)) return fail('EDGE_FROM_MISSING', `${where} 的起点不存在: ${e.from}`);
      if (!ids.has(e.to)) return fail('EDGE_TO_MISSING', `${where} 的终点不存在: ${e.to}`);
      if (e.from === e.to) return fail('SELF_LOOP', `${where} 存在自环: ${e.from}`);
      if (e.when && !EDGE_WHEN.includes(e.when))
        fail('BAD_WHEN', `${where} 的 when 非法: ${e.when}（可选 ${EDGE_WHEN.join('/')}）`);
      if ((e.when === 'expr' || e.expr) && typeof e.expr !== 'string')
        fail('BAD_EXPR', `${where} 使用 expr 条件时 expr 必须是字符串`);
      if (e.expr) {
        try { new Function('result', 'ctx', 'return (' + e.expr + ');'); }
        catch (err) { fail('EXPR_SYNTAX', `${where} 表达式语法错误: ${err.message}`); }
      }
    });

    // 重复边（同 from/to）只警告，不影响正确性
    const seen = new Set();
    for (const e of def.edges) {
      const k = e.from + '->' + e.to;
      if (seen.has(k)) warn('DUP_EDGE', `存在重复边: ${k}（条件求值时会被视为多条入边）`);
      seen.add(k);
    }

    if (errors.length) return { ok: false, errors, warnings };

    const cycle = findCycle(def);
    if (cycle) {
      fail('CYCLE', `存在环（死锁图）: ${cycle.join(' -> ')}`);
      return { ok: false, errors, warnings };
    }

    // 孤立节点 / 不可达节点检查
    const { inc } = buildAdjacency(def);
    const roots = def.nodes.filter((n) => inc[n.id].length === 0 && buildAdjacency(def).out[n.id].length === 0);
    roots.forEach((n) => warn('ISOLATED_NODE', `孤立节点 ${n.id}（无任何边）将单独执行`));

    return { ok: true, errors, warnings };
  }

  /* ---------- 条件边求值 ---------- */

  // 返回 true=走这条边 / false=不走（供下游 join 判定）
  // status: 'success' | 'failed' | 'skipped'
  function evalEdge(edge, upstreamStatus, result, ctx) {
    const when = edge.when || 'always';
    if (upstreamStatus === 'skipped') return false; // 跳过的节点不触发任何下游
    if (when === 'always') return true;
    if (when === 'onSuccess') return upstreamStatus === 'success';
    if (when === 'onFailure') return upstreamStatus === 'failed';
    if (when === 'expr') {
      if (upstreamStatus !== 'success') return false;
      try {
        const fn = new Function('result', 'ctx', 'return (' + edge.expr + ');');
        return !!fn(result, ctx || {});
      } catch (err) {
        return false; // 表达式运行时错误按“不通过”处理
      }
    }
    return false;
  }

  /* ---------- 重试退避 ---------- */

  // attempt: 即将进行的第几次尝试（从 1 开始），第 1 次不等待
  // delay(attempt n) = min(maxMs, baseMs * factor^(n-1)) * (1 ± jitter)
  function backoffDelay(attempt, retry) {
    const r = retry || {};
    const baseMs = Number.isFinite(r.baseMs) ? r.baseMs : 500;
    const factor = Number.isFinite(r.factor) ? r.factor : 2;
    const maxMs = Number.isFinite(r.maxMs) ? r.maxMs : 30000;
    const jitter = Number.isFinite(r.jitter) ? Math.min(Math.abs(r.jitter), 1) : 0.2;
    if (attempt <= 1) return 0;
    let d = baseMs * Math.pow(factor, attempt - 1);
    d = Math.min(d, maxMs);
    if (jitter > 0) d = d * (1 + (Math.random() * 2 - 1) * jitter);
    return Math.max(0, Math.round(d));
  }

  /* ---------- 运行时死锁判定 ---------- */

  // 调度器在每次结算后调用：
  // 没有任何运行中、退避等待中、就绪节点，但仍有未终结节点 => 死锁
  // 正常的 skip 级联不会触发，因为级联是同步结算的；触发说明调度不变量被破坏（例如依赖无法满足）。
  function isRuntimeDeadlocked(snapshot) {
    const hasActive = snapshot.nodes.some((n) =>
      n.status === 'running' || n.status === 'retrying' || n.status === 'ready');
    const hasUnfinished = snapshot.nodes.some((n) =>
      ['pending', 'ready', 'running', 'retrying'].includes(n.status));
    return !hasActive && hasUnfinished;
  }

  // 离线检测：找出“永远不可能就绪”的节点集合
  // （依赖了失败且没有 onFailure/always 边的节点，或依赖链最终断裂）
  function blockedNodes(def, settledStatus) {
    const { inc } = buildAdjacency(def);
    const blocked = new Set();
    let changed = true;
    while (changed) {
      changed = false;
      for (const n of def.nodes) {
        if (settledStatus[n.id] && settledStatus[n.id] !== 'blocked') continue;
        if (blocked.has(n.id)) continue;
        // 入边的上游全部已终结，且没有任何一条边被“走通” => 永久阻塞
        const incoming = inc[n.id];
        if (incoming.length === 0) continue;
        const allSettled = incoming.every((e) => {
          if (blocked.has(e.from)) return true; // 上游已阻塞 => 级联阻塞
          const s = settledStatus[e.from];
          return s && !['pending', 'ready', 'running', 'retrying'].includes(s);
        });
        if (allSettled) { blocked.add(n.id); changed = true; }
      }
    }
    return blocked;
  }

  return {
    NODE_STATUS, RUN_STATUS, EDGE_WHEN,
    indexNodes, buildAdjacency, findCycle, topoSort, topoLayers,
    validateDAG, evalEdge, backoffDelay, isRuntimeDeadlocked, blockedNodes,
  };
});
