/*
 * dag.js — DAG 定义、校验、拓扑排序、运行时死锁检测。
 * 纯逻辑，无环境依赖：浏览器 / Web Worker / Node 均可加载。
 */
(function (global) {
  'use strict';

  /**
   * DAG 定义格式:
   * {
   *   id: string,
   *   nodes: [{ id, task, timeout?, retries?, backoff?, backoffFactor?, params? }],
   *   edges: [{ from, to, when? }]   // when: 条件表达式, 缺省表示上游成功时通过
   * }
   */

  function validateDAG(def) {
    const errors = [];
    if (!def || typeof def !== 'object') {
      return { ok: false, errors: ['定义必须是对象'], order: [] };
    }
    const nodes = Array.isArray(def.nodes) ? def.nodes : [];
    const edges = Array.isArray(def.edges) ? def.edges : [];
    if (nodes.length === 0) errors.push('nodes 不能为空');

    const ids = new Set();
    for (const n of nodes) {
      if (!n || typeof n.id !== 'string' || n.id === '') {
        errors.push('存在缺少 id 的节点');
        continue;
      }
      if (ids.has(n.id)) errors.push('节点 id 重复: ' + n.id);
      ids.add(n.id);
      if (typeof n.task !== 'string' || n.task === '') {
        errors.push('节点 ' + n.id + ' 缺少 task');
      }
      if (n.timeout != null && (!(n.timeout > 0))) {
        errors.push('节点 ' + n.id + ' 的 timeout 必须为正数');
      }
      if (n.retries != null && (!(n.retries >= 0) || !Number.isInteger(n.retries))) {
        errors.push('节点 ' + n.id + ' 的 retries 必须为非负整数');
      }
    }

    const edgeKeys = new Set();
    for (const e of edges) {
      if (!e || typeof e.from !== 'string' || typeof e.to !== 'string') {
        errors.push('存在非法边 (缺少 from/to)');
        continue;
      }
      if (!ids.has(e.from)) errors.push('边的起点不存在: ' + e.from);
      if (!ids.has(e.to)) errors.push('边的终点不存在: ' + e.to);
      if (e.from === e.to) errors.push('存在自环: ' + e.from);
      const key = e.from + '->' + e.to;
      if (edgeKeys.has(key)) errors.push('重复边: ' + key);
      edgeKeys.add(key);
    }

    const order = errors.length === 0 ? topoSort(nodes, edges) : [];
    if (errors.length === 0 && order.length !== nodes.length) {
      const cycleNodes = findCycleNodes(nodes, edges);
      errors.push('检测到环 (死锁风险): ' + cycleNodes.join(' -> '));
    }
    return { ok: errors.length === 0, errors, order };
  }

  /** Kahn 拓扑排序; 有环时返回部分结果 (长度 < 节点数)。 */
  function topoSort(nodes, edges) {
    const indeg = new Map();
    const adj = new Map();
    for (const n of nodes) { indeg.set(n.id, 0); adj.set(n.id, []); }
    for (const e of edges) {
      if (!indeg.has(e.from) || !indeg.has(e.to)) continue;
      indeg.set(e.to, indeg.get(e.to) + 1);
      adj.get(e.from).push(e.to);
    }
    const queue = [];
    for (const [id, d] of indeg) if (d === 0) queue.push(id);
    const order = [];
    while (queue.length) {
      const id = queue.shift();
      order.push(id);
      for (const next of adj.get(id)) {
        indeg.set(next, indeg.get(next) - 1);
        if (indeg.get(next) === 0) queue.push(next);
      }
    }
    return order;
  }

  /** 找出处于环上的节点 (入度未清零的节点), 用于错误提示。 */
  function findCycleNodes(nodes, edges) {
    const indeg = new Map();
    const adj = new Map();
    for (const n of nodes) { indeg.set(n.id, 0); adj.set(n.id, []); }
    for (const e of edges) {
      indeg.set(e.to, (indeg.get(e.to) || 0) + 1);
      (adj.get(e.from) || adj.set(e.from, []).get(e.from)).push(e.to);
    }
    const queue = [];
    for (const [id, d] of indeg) if (d === 0) queue.push(id);
    while (queue.length) {
      const id = queue.shift();
      for (const next of adj.get(id) || []) {
        indeg.set(next, indeg.get(next) - 1);
        if (indeg.get(next) === 0) queue.push(next);
      }
    }
    return [...indeg.entries()].filter(([, d]) => d > 0).map(([id]) => id);
  }

  /**
   * 运行时死锁检测:
   * 存在 pending 节点, 但没有 ready/running 节点, 且这些 pending 节点
   * 的剩余上游全部处于终态 —— 调度器无法再推进, 判定死锁。
   * 返回 { deadlocked, stuckNodes }。
   */
  function detectRuntimeDeadlock(nodeStates, def) {
    const states = Object.values(nodeStates);
    const hasActive = states.some(s => s.status === 'ready' || s.status === 'running' || s.status === 'retry');
    if (hasActive) return { deadlocked: false, stuckNodes: [] };
    const pending = states.filter(s => s.status === 'pending').map(s => s.nodeId);
    if (pending.length === 0) return { deadlocked: false, stuckNodes: [] };
    return { deadlocked: true, stuckNodes: pending };
  }

  /** 计算每个节点的入边 / 出边索引, 供引擎与布局使用。 */
  function indexEdges(def) {
    const incoming = new Map();
    const outgoing = new Map();
    for (const n of def.nodes) { incoming.set(n.id, []); outgoing.set(n.id, []); }
    for (const e of def.edges) {
      if (incoming.has(e.to)) incoming.get(e.to).push(e);
      if (outgoing.has(e.from)) outgoing.get(e.from).push(e);
    }
    return { incoming, outgoing };
  }

  const api = { validateDAG, topoSort, findCycleNodes, detectRuntimeDeadlock, indexEdges };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  global.DAG = api;
})(typeof self !== 'undefined' ? self : globalThis);
