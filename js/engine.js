/*
 * engine.js — 工作流引擎核心: 并发调度 / 条件分支 / 重试退避 / 超时中断 / 状态持久化 / 异常链路。
 * 环境无关: 不依赖 DOM, 可在 Web Worker 与 Node (测试) 中运行。
 */
(function (global) {
  'use strict';

  const DAG = global.DAG || (typeof require !== 'undefined' ? require('./dag.js') : null);

  const TERMINAL = new Set(['success', 'failed', 'timeout', 'skipped']);
  const ACTIVE = new Set(['ready', 'running', 'retry']);

  let runSeq = 0;
  function genRunId(defId) {
    runSeq += 1;
    return defId + '-' + Date.now().toString(36) + '-' + runSeq;
  }

  /** 计算指数退避延迟: backoff * factor^(attempt-1) + 抖动 */
  function backoffDelay(node, attempt) {
    const base = node.backoff != null ? node.backoff : 200;
    const factor = node.backoffFactor != null ? node.backoffFactor : 2;
    const jitter = Math.random() * base * 0.5;
    return Math.round(base * Math.pow(factor, attempt - 1) + jitter);
  }

  /** 求值边条件。context: { status, result, error, outputs } */
  function evalCondition(expr, context) {
    if (expr == null || expr === '' || expr === 'onSuccess') return context.status === 'success';
    if (expr === 'always') return context.status !== 'skipped';
    if (expr === 'onFailure') return context.status === 'failed' || context.status === 'timeout';
    const fn = new Function('status', 'result', 'error', 'outputs',
      '"use strict"; return !!(' + expr + ');');
    return !!fn(context.status, context.result, context.error, context.outputs);
  }

  function createEngine(options) {
    const def = options.def;
    const tasks = options.tasks;
    const store = options.store;
    const emit = options.emit || (() => {});
    const maxConcurrent = options.maxConcurrent || 4;
    const runId = options.runId || genRunId(def.id || 'run');

    const nodeDefs = new Map(def.nodes.map(n => [n.id, n]));
    const { incoming, outgoing } = DAG.indexEdges(def);
    const states = new Map(); // nodeId -> state
    const controllers = new Map(); // nodeId -> AbortController
    const timers = new Map(); // nodeId -> timeout timer
    const retryTimers = new Map();
    const outputs = {}; // nodeId -> result

    let runningCount = 0;
    let cancelled = false;
    let finished = false;
    let doneResolve;
    const done = new Promise(resolve => { doneResolve = resolve; });

    for (const n of def.nodes) {
      states.set(n.id, {
        runId, nodeId: n.id, task: n.task, status: 'pending',
        attempt: 0, result: undefined, error: null,
        startedAt: null, finishedAt: null, causedBy: null,
      });
    }

    async function persistNode(nodeId) {
      const s = states.get(nodeId);
      await store.putNode({ ...s });
    }

    async function persistRun(patch) {
      const prev = (await store.getRun(runId)) || { runId, defId: def.id, createdAt: Date.now() };
      await store.putRun({ ...prev, def, ...patch, updatedAt: Date.now() });
    }

    async function transition(nodeId, status, extra) {
      const s = states.get(nodeId);
      if (extra) Object.assign(s, extra);
      s.status = status;
      if (status === 'running') s.startedAt = Date.now();
      if (TERMINAL.has(status)) s.finishedAt = Date.now();
      await persistNode(nodeId);
      emit('node', { runId, node: { ...s } });
    }

    async function logEvent(level, message, data) {
      const event = { runId, level, message, data: data || null, ts: Date.now() };
      await store.appendEvent(event);
      emit('event', event);
    }

    /** 尝试激活 pending 节点: 任一入边条件满足 -> ready; 全部上游终态且无边满足 -> skipped */
    async function activate(nodeId) {
      const s = states.get(nodeId);
      if (s.status !== 'pending') return;
      const ins = incoming.get(nodeId) || [];
      if (ins.length === 0) { s.status = 'ready'; return; }

      let allTerminal = true;
      let fired = false;
      for (const e of ins) {
        const up = states.get(e.from);
        if (!TERMINAL.has(up.status)) { allTerminal = false; break; }
        if (up.status === 'skipped') continue;
        const ctx = { status: up.status, result: up.result, error: up.error, outputs };
        let pass = false;
        try { pass = evalCondition(e.when, ctx); }
        catch (err) {
          await logEvent('error', '条件表达式求值失败: ' + (e.when || '') + ' @ ' + e.from + '->' + e.to, { error: String(err) });
        }
        if (pass) { fired = true; break; }
      }
      if (fired) { s.status = 'ready'; return; }
      if (allTerminal) {
        const cause = ins.map(e => e.from).find(from => {
          const st = states.get(from).status;
          return st === 'failed' || st === 'timeout' || st === 'skipped';
        });
        await transition(nodeId, 'skipped', { causedBy: cause || null });
        await logEvent('warn', '节点 ' + nodeId + ' 被跳过 (异常链路传导)', { causedBy: cause || null });
      }
    }

    function collectInputs(nodeId) {
      const inputs = {};
      for (const e of incoming.get(nodeId) || []) {
        const up = states.get(e.from);
        if (up.status === 'success') inputs[e.from] = up.result;
      }
      return inputs;
    }

    async function runNode(nodeId) {
      const s = states.get(nodeId);
      const nodeDef = nodeDefs.get(nodeId);
      const taskFn = tasks[nodeDef.task];
      if (typeof taskFn !== 'function') {
        runningCount += 0;
        await transition(nodeId, 'failed', { error: { type: 'error', message: '未注册的任务: ' + nodeDef.task } });
        await propagate(nodeId);
        await pump();
        return;
      }
      s.attempt += 1;
      runningCount += 1;
      const controller = new AbortController();
      controllers.set(nodeId, controller);
      await transition(nodeId, 'running', { error: null });
      await logEvent('info', '节点 ' + nodeId + ' 开始执行 (第 ' + s.attempt + ' 次尝试)', { attempt: s.attempt });

      let timedOut = false;
      let settled = false;
      const timeoutMs = nodeDef.timeout != null ? nodeDef.timeout : 30000;
      const timer = setTimeout(() => {
        timedOut = true;
        controller.abort();
        if (!settled) {
          settled = true;
          finishNode(nodeId, null, { type: 'timeout', message: '执行超过 ' + timeoutMs + 'ms, 已中断' });
        }
      }, timeoutMs);
      timers.set(nodeId, timer);

      const ctx = {
        signal: controller.signal,
        params: nodeDef.params || {},
        inputs: collectInputs(nodeId),
        attempt: s.attempt,
        log: (msg) => logEvent('debug', '[' + nodeId + '] ' + msg),
      };

      try {
        const result = await taskFn(ctx);
        if (settled || cancelled) return;
        settled = true;
        clearTimeout(timer);
        timers.delete(nodeId);
        await finishNode(nodeId, result, null);
      } catch (err) {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        timers.delete(nodeId);
        const error = timedOut
          ? { type: 'timeout', message: '执行超过 ' + timeoutMs + 'ms, 已中断' }
          : { type: 'error', message: String((err && err.message) || err) };
        await finishNode(nodeId, null, error);
      }
    }

    async function finishNode(nodeId, result, error) {
      const s = states.get(nodeId);
      const nodeDef = nodeDefs.get(nodeId);
      controllers.delete(nodeId);
      runningCount -= 1;

      if (cancelled) return;

      if (error) {
        const maxRetries = nodeDef.retries != null ? nodeDef.retries : 0;
        if (s.attempt <= maxRetries) {
          const delay = backoffDelay(nodeDef, s.attempt);
          await transition(nodeId, 'retry', { error });
          await logEvent('warn', '节点 ' + nodeId + ' 失败 (' + error.message + '), ' + delay + 'ms 后重试 (第 ' + (s.attempt + 1) + ' 次)', { attempt: s.attempt, delay, error });
          const timer = setTimeout(() => {
            retryTimers.delete(nodeId);
            if (!cancelled) { states.get(nodeId).status = 'ready'; pump(); }
          }, delay);
          retryTimers.set(nodeId, timer);
        } else {
          const finalStatus = error.type === 'timeout' ? 'timeout' : 'failed';
          await transition(nodeId, finalStatus, { error });
          await logEvent('error', '节点 ' + nodeId + ' 最终失败: ' + error.message, { error, attempts: s.attempt });
          await propagate(nodeId);
        }
      } else {
        outputs[nodeId] = result;
        await transition(nodeId, 'success', { result, error: null });
        await logEvent('info', '节点 ' + nodeId + ' 执行成功', { result });
        await propagate(nodeId);
      }
      await pump();
    }

    /** 上游进入终态后, 重新评估下游激活条件 */
    async function propagate(fromId) {
      for (const e of outgoing.get(fromId) || []) {
        await activate(e.to);
      }
    }

    /** 调度泵: 填满并发槽; 无事可做时检查完成 / 死锁 */
    async function pump() {
      if (cancelled || finished) return;

      // 级联激活: 跳过会传导, 循环直到没有新激活
      let changed = true;
      while (changed) {
        changed = false;
        for (const [nodeId, s] of states) {
          if (s.status === 'pending') {
            const before = s.status;
            await activate(nodeId);
            if (states.get(nodeId).status !== before) changed = true;
          }
        }
      }

      while (runningCount < maxConcurrent) {
        let next = null;
        for (const id of DAG.topoSort(def.nodes, def.edges)) {
          if (states.get(id).status === 'ready') { next = id; break; }
        }
        if (!next) break;
        states.get(next).status = 'starting'; // 占位, 防止重复调度
        runNode(next); // 不 await, 并发执行
      }

      const snapshot = {};
      for (const [id, s] of states) snapshot[id] = { status: s.status };
      const anyActive = [...states.values()].some(s => ACTIVE.has(s.status) || s.status === 'starting');
      if (!anyActive && !finished) {
        const pending = [...states.values()].filter(s => s.status === 'pending').map(s => s.nodeId);
        if (pending.length > 0) {
          const dl = DAG.detectRuntimeDeadlock(snapshot, def);
          if (dl.deadlocked) {
            finished = true;
            await logEvent('error', '检测到运行时死锁, 停滞节点: ' + dl.stuckNodes.join(', '), { stuckNodes: dl.stuckNodes });
            await persistRun({ status: 'deadlocked', stuckNodes: dl.stuckNodes });
            emit('run', { runId, status: 'deadlocked', stuckNodes: dl.stuckNodes });
            doneResolve(await store.getRun(runId));
            return;
          }
        }
        const failedNodes = [...states.values()].filter(s => s.status === 'failed' || s.status === 'timeout');
        finished = true;
        const finalStatus = failedNodes.length > 0 ? 'failed' : 'success';
        await persistRun({ status: finalStatus });
        await logEvent(failedNodes.length ? 'error' : 'info', '运行结束: ' + finalStatus);
        emit('run', { runId, status: finalStatus });
        doneResolve(await store.getRun(runId));
      }
    }

    async function start() {
      await persistRun({ status: 'running', startedAt: Date.now() });
      await Promise.all([...states.keys()].map(persistNode));
      await logEvent('info', '运行开始, 并发上限 ' + maxConcurrent);
      emit('run', { runId, status: 'running' });
      await pump();
      return { runId, done };
    }

    async function cancel() {
      if (finished) return;
      cancelled = true;
      for (const t of timers.values()) clearTimeout(t);
      for (const t of retryTimers.values()) clearTimeout(t);
      for (const c of controllers.values()) c.abort();
      runningCount = 0;
      for (const [id, s] of states) {
        if (s.status === 'running' || s.status === 'ready' || s.status === 'retry' || s.status === 'starting') {
          await transition(id, 'failed', { error: { type: 'cancelled', message: '运行被取消' } });
        }
      }
      finished = true;
      await persistRun({ status: 'cancelled' });
      await logEvent('warn', '运行被取消');
      emit('run', { runId, status: 'cancelled' });
      doneResolve(await store.getRun(runId));
    }

    return {
      runId,
      done,
      start,
      cancel,
      getState: () => {
        const snapshot = {};
        for (const [id, s] of states) snapshot[id] = { ...s };
        return { runId, cancelled, finished, runningCount, nodes: snapshot };
      },
      // 测试辅助
      _internals: { states, backoffDelay },
    };
  }

  const api = { createEngine, backoffDelay, evalCondition, genRunId };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  global.WFEngine = api;
})(typeof self !== 'undefined' ? self : globalThis);
