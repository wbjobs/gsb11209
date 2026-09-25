/*
 * worker.js — 工作流调度引擎（运行在 Web Worker 中，主线程零阻塞）
 * 职责：并发调度 / 条件分支 / 重试退避 / 超时中断 / 死锁检测 / IndexedDB 持久化 / BroadcastChannel 广播
 */
'use strict';
importScripts('dag.js', 'db.js');

const DAG = self.DAG;
const TERMINAL = ['success', 'failed', 'skipped', 'blocked'];

/* ---------------- 可中断任务注册表 ---------------- */
// 每个任务接收 (params, {signal, attempt, nodeId})，返回 Promise<result>
// 任务必须响应 AbortSignal —— 这是“超时可中断”的关键
const Tasks = {
  // 模拟耗时 IO
  sleep({ ms = 1000 } = {}, { signal }) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => resolve({ slept: ms }), ms);
      signal.addEventListener('abort', () => { clearTimeout(timer); reject(abortErr()); });
    });
  },
  // CPU 密集计算（分片执行，片间检查中断信号）——证明重计算在 Worker 中不卡主线程
  compute({ ms = 2000 } = {}, { signal }) {
    return new Promise((resolve, reject) => {
      const deadline = performance.now() + ms;
      let iterations = 0;
      (function chunk() {
        if (signal.aborted) return reject(abortErr());
        const sliceEnd = performance.now() + 25; // 每片 25ms
        while (performance.now() < sliceEnd) {
          for (let i = 0; i < 1e4; i++) iterations += Math.sqrt(i);
        }
        if (performance.now() >= deadline) resolve({ iterations: Math.round(iterations) });
        else setTimeout(chunk, 0);
      })();
    });
  },
  // 前 failTimes 次必失败，用于演示重试退避
  flaky({ failTimes = 2, ms = 300 } = {}, { signal, attempt }) {
    return Tasks.sleep({ ms }, { signal }).then(() => {
      if (attempt <= failTimes) throw new Error(`flaky: 第 ${attempt} 次尝试失败（共需失败 ${failTimes} 次）`);
      return { attempt, ok: true };
    });
  },
  // 以概率 p 随机失败
  maybe({ p = 0.5, ms = 500 } = {}, { signal }) {
    return Tasks.sleep({ ms }, { signal }).then(() => {
      if (Math.random() < p) throw new Error(`maybe: 随机失败 (p=${p})`);
      return { ok: true };
    });
  },
  // 产生一个值，供下游 expr 条件边消费
  emit({ value = 1 } = {}) {
    return Promise.resolve({ value });
  },
};
function abortErr() { const e = new Error('aborted'); e.name = 'AbortError'; return e; }
function timeoutErr(ms) { const e = new Error(`timeout after ${ms}ms`); e.name = 'TimeoutError'; return e; }

/* ---------------- 广播 ---------------- */
const bc = ('BroadcastChannel' in self) ? new BroadcastChannel('workflow-engine') : null;
function broadcast(msg) { if (bc) bc.postMessage(msg); }
function emit(msg) { postMessage(msg); broadcast(msg); } // 主线程 + 其他标签页

/* ---------------- 运行实例 ---------------- */
class Run {
  constructor(def, options) {
    this.def = def;
    this.runId = options.runId || ('run-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 7));
    this.maxConcurrent = Math.max(1, options.maxConcurrent || 4);
    this.status = 'running';
    this.startedAt = Date.now();
    this.endedAt = null;
    this.nodes = new Map();
    const { inc } = DAG.buildAdjacency(def);
    for (const n of def.nodes) {
      this.nodes.set(n.id, {
        runId: this.runId,
        nodeId: n.id,
        def: n,
        status: 'pending',
        attempt: 0,
        result: null,
        error: null,
        startedAt: null,
        endedAt: null,
        durationMs: null,
        incoming: (inc[n.id] || []).map((edge) => ({ edge, resolved: false, taken: false })),
        controller: null,
        retryTimer: null,
      });
    }
    this.readyQueue = [];
    this.runningCount = 0;
    this.stopped = false;
  }

  snapshot() {
    return {
      type: 'snapshot',
      run: {
        runId: this.runId,
        name: this.def.name || this.runId,
        status: this.status,
        startedAt: this.startedAt,
        endedAt: this.endedAt,
        maxConcurrent: this.maxConcurrent,
        runningCount: this.runningCount,
        def: this.def,
        nodes: [...this.nodes.values()].map((n) => ({
          nodeId: n.nodeId, status: n.status, attempt: n.attempt,
          result: n.result, error: n.error,
          startedAt: n.startedAt, endedAt: n.endedAt, durationMs: n.durationMs,
        })),
      },
    };
  }

  persistNode(n) { return db.putNodeState(stripNode(n)).catch(logDbErr); }
  persistRun() {
    return db.putRun({
      runId: this.runId, name: this.def.name || this.runId,
      status: this.status, startedAt: this.startedAt, endedAt: this.endedAt,
      def: this.def,
    }).catch(logDbErr);
  }

  logEvent(kind, nodeId, message) {
    const ev = { runId: this.runId, ts: Date.now(), kind, nodeId: nodeId || null, message };
    db.appendEvent(ev).catch(logDbErr);
    emit({ type: 'event', event: ev });
  }

  /* ---- 调度主循环：填充并发槽位 ---- */
  pump() {
    if (this.stopped || this.status !== 'running') return;
    while (this.runningCount < this.maxConcurrent && this.readyQueue.length) {
      const nodeId = this.readyQueue.shift();
      const n = this.nodes.get(nodeId);
      if (n.status !== 'ready') continue;
      this.execNode(n);
    }
    this.checkSettled();
  }

  enqueue(nodeId) {
    const n = this.nodes.get(nodeId);
    if (n.status !== 'pending') return;
    n.status = 'ready';
    this.readyQueue.push(nodeId);
    this.persistNode(n);
    emit(this.snapshot());
    this.pump();
  }

  execNode(n) {
    n.status = 'running';
    n.attempt += 1;
    n.startedAt = Date.now();
    n.error = null;
    n.controller = new AbortController();
    this.runningCount++;
    this.persistNode(n);
    this.logEvent('node-start', n.nodeId, `开始执行（第 ${n.attempt} 次尝试）`);
    emit(this.snapshot());

    const task = Tasks[n.def.type] || Tasks.sleep;
    const params = n.def.params || {};
    const timeoutMs = n.def.timeoutMs || 0;
    const signal = n.controller.signal;

    let timeoutTimer = null;
    const taskPromise = Promise.resolve().then(() =>
      task(params, { signal, attempt: n.attempt, nodeId: n.nodeId, runId: this.runId }));

    const raced = timeoutMs > 0
      ? Promise.race([
          taskPromise,
          new Promise((_, reject) => {
            timeoutTimer = setTimeout(() => {
              n.controller.abort();          // 超时 -> 主动中断任务
              reject(timeoutErr(timeoutMs));
            }, timeoutMs);
          }),
        ])
      : taskPromise;

    raced.then(
      (result) => { if (timeoutTimer) clearTimeout(timeoutTimer); this.onNodeDone(n, null, result); },
      (err) => { if (timeoutTimer) clearTimeout(timeoutTimer); this.onNodeDone(n, err, null); }
    );
  }

  onNodeDone(n, err, result) {
    if (this.stopped) return;
    this.runningCount--;
    n.endedAt = Date.now();
    n.durationMs = n.endedAt - n.startedAt;
    n.controller = null;

    if (!err) {
      n.status = 'success';
      n.result = result === undefined ? null : result;
      this.persistNode(n);
      this.logEvent('node-success', n.nodeId, `成功，耗时 ${n.durationMs}ms`);
      emit(this.snapshot());
      this.settleOutgoing(n);
      this.pump();
      return;
    }

    if (err && err.name === 'AbortError' && this.stopped) return; // stop 路径统一处理

    n.error = err.message || String(err);
    const isTimeout = err.name === 'TimeoutError';
    const maxAttempts = (n.def.retry && n.def.retry.maxAttempts) || 1;

    if (n.attempt < maxAttempts) {
      // 进入退避等待
      const delay = DAG.backoffDelay(n.attempt + 1, n.def.retry);
      n.status = 'retrying';
      this.persistNode(n);
      this.logEvent(isTimeout ? 'node-timeout' : 'node-retry', n.nodeId,
        `${isTimeout ? '超时中断' : '失败'}：${n.error}；${delay}ms 后进行第 ${n.attempt + 1}/${maxAttempts} 次尝试`);
      emit(this.snapshot());
      n.retryTimer = setTimeout(() => {
        n.retryTimer = null;
        if (this.stopped) return;
        n.status = 'ready';
        this.readyQueue.push(n.nodeId);
        this.pump();
      }, delay);
      this.pump();
    } else {
      n.status = 'failed';
      this.persistNode(n);
      this.logEvent(isTimeout ? 'node-timeout' : 'node-failed', n.nodeId,
        `${isTimeout ? '超时中断' : '失败'}（已用尽 ${maxAttempts} 次尝试）：${n.error}`);
      emit(this.snapshot());
      this.settleOutgoing(n);
      this.pump();
    }
  }

  /* ---- 下游边结算与 skip 级联 ---- */
  settleOutgoing(n) {
    const upstreamStatus = n.status === 'success' ? 'success' : 'failed';
    for (const edge of this.def.edges) {
      if (edge.from !== n.nodeId) continue;
      const down = this.nodes.get(edge.to);
      if (!down) continue;
      const slot = down.incoming.find((s) => s.edge === edge && !s.resolved);
      if (!slot) continue;
      slot.resolved = true;
      slot.taken = DAG.evalEdge(edge, upstreamStatus, n.result, { runId: this.runId });
      this.logEvent('edge', edge.to,
        `边 ${edge.from} -> ${edge.to}（${edge.when || 'always'}）${slot.taken ? '走通' : '不通过'}`);
      this.resolveNode(down);
    }
  }

  resolveNode(n) {
    if (n.status !== 'pending') return;
    if (!n.incoming.every((s) => s.resolved)) return; // 还有上游未终结
    const join = n.def.join || 'all';
    const fire = join === 'any' ? n.incoming.some((s) => s.taken) : n.incoming.every((s) => s.taken);
    if (fire) {
      this.enqueue(n.nodeId);
    } else {
      n.status = 'skipped';
      n.endedAt = Date.now();
      this.persistNode(n);
      this.logEvent('node-skipped', n.nodeId, `条件不满足（join=${join}），跳过`);
      emit(this.snapshot());
      // skip 级联：skipped 上游不触发任何下游边
      for (const edge of this.def.edges) {
        if (edge.from !== n.nodeId) continue;
        const down = this.nodes.get(edge.to);
        if (!down) continue;
        const slot = down.incoming.find((s) => s.edge === edge && !s.resolved);
        if (slot) { slot.resolved = true; slot.taken = false; this.resolveNode(down); }
      }
    }
  }

  /* ---- 终结判定 + 运行时死锁检测 ---- */
  checkSettled() {
    if (this.stopped || this.status !== 'running') return;
    const states = [...this.nodes.values()];
    const allTerminal = states.every((n) => TERMINAL.includes(n.status));
    if (allTerminal) {
      const anyFailed = states.some((n) => n.status === 'failed' || n.status === 'blocked');
      this.finish(anyFailed ? 'failed' : 'success');
      return;
    }
    // 死锁：没有运行中/退避中/就绪节点，但仍有未终结节点
    if (DAG.isRuntimeDeadlocked({ nodes: states })) {
      for (const n of states) {
        if (!TERMINAL.includes(n.status)) {
          n.status = 'blocked';
          n.error = 'deadlock: 依赖永远无法满足';
          n.endedAt = Date.now();
          this.persistNode(n);
        }
      }
      this.logEvent('deadlock', null, '检测到运行时死锁：剩余节点依赖无法满足，已全部标记为 blocked');
      this.finish('deadlocked');
    }
  }

  finish(status) {
    this.status = status;
    this.endedAt = Date.now();
    this.persistRun();
    this.logEvent('run-end', null, `运行结束：${status}，总耗时 ${this.endedAt - this.startedAt}ms`);
    emit(this.snapshot());
    emit({ type: 'run-finished', runId: this.runId, status });
    activeRuns.delete(this.runId);
  }

  stop() {
    if (TERMINAL.includes(this.status) || this.status === 'stopped') return;
    this.stopped = true;
    this.status = 'stopped';
    this.endedAt = Date.now();
    for (const n of this.nodes.values()) {
      if (n.retryTimer) { clearTimeout(n.retryTimer); n.retryTimer = null; }
      if (n.controller) { n.controller.abort(); n.controller = null; }
      if (!TERMINAL.includes(n.status)) {
        n.status = 'blocked';
        n.error = 'stopped by user';
        n.endedAt = Date.now();
        this.persistNode(n);
      }
    }
    this.persistRun();
    this.logEvent('run-stopped', null, '用户手动停止');
    emit(this.snapshot());
    emit({ type: 'run-finished', runId: this.runId, status: 'stopped' });
    activeRuns.delete(this.runId);
  }
}

function stripNode(n) {
  return {
    runId: n.runId, nodeId: n.nodeId, status: n.status, attempt: n.attempt,
    result: n.result, error: n.error,
    startedAt: n.startedAt, endedAt: n.endedAt, durationMs: n.durationMs,
  };
}
function logDbErr(e) { console.error('[workflow-db]', e); }

/* ---------------- Worker 消息入口 ---------------- */
let db = null;
const activeRuns = new Map();

async function handleStart(msg) {
  const def = msg.def;
  const validation = DAG.validateDAG(def);
  if (!validation.ok) {
    emit({ type: 'validated', ok: false, errors: validation.errors, warnings: validation.warnings });
    emit({ type: 'start-rejected', errors: validation.errors });
    return;
  }
  const run = new Run(def, msg.options || {});
  activeRuns.set(run.runId, run);
  await run.persistRun();
  await db.putNodeStates([...run.nodes.values()].map(stripNode)).catch(logDbErr);
  run.logEvent('run-start', null, `运行开始，并发上限 ${run.maxConcurrent}，节点数 ${def.nodes.length}`);
  emit(run.snapshot());
  // 根节点（无入边）直接进入就绪队列
  for (const n of run.nodes.values()) if (n.incoming.length === 0) run.enqueue(n.nodeId);
  run.pump();
}

self.onmessage = async (e) => {
  const msg = e.data;
  try {
    switch (msg.type) {
      case 'init': {
        db = await WorkflowDB.open();
        const recovered = await db.recoverInterrupted();
        emit({ type: 'ready', recovered });
        emit({ type: 'runs', runs: await db.listRuns() });
        break;
      }
      case 'validate': {
        const r = DAG.validateDAG(msg.def);
        emit({ type: 'validated', ok: r.ok, errors: r.errors, warnings: r.warnings });
        break;
      }
      case 'start':
        await handleStart(msg);
        break;
      case 'stop': {
        const run = activeRuns.get(msg.runId);
        if (run) run.stop();
        break;
      }
      case 'list-runs':
        emit({ type: 'runs', runs: await db.listRuns() });
        break;
      case 'get-run': {
        const run = await db.getRun(msg.runId);
        const nodes = await db.getNodeStates(msg.runId);
        const events = await db.listEvents(msg.runId);
        emit({ type: 'run-detail', run, nodes, events });
        break;
      }
      case 'delete-run':
        await db.deleteRun(msg.runId);
        emit({ type: 'runs', runs: await db.listRuns() });
        break;
      case 'clear-runs': {
        const runs = await db.listRuns();
        for (const r of runs) await db.deleteRun(r.runId);
        emit({ type: 'runs', runs: [] });
        break;
      }
    }
  } catch (err) {
    emit({ type: 'engine-error', message: err.message || String(err) });
  }
};
