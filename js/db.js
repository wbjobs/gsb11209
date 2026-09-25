/*
 * db.js — IndexedDB 持久化封装（主线程与 Worker 通用）
 * stores:
 *   runs      : 每次运行的元信息 + 终态（keyPath: runId）
 *   nodeStates: 运行中每个节点的状态快照（keyPath: [runId, nodeId]）
 *   events    : 事件流水（keyPath: id 自增，索引 runId）
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.WorkflowDB = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  const DB_NAME = 'workflow-engine';
  const DB_VERSION = 1;

  function open() {
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, DB_VERSION);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains('runs'))
          db.createObjectStore('runs', { keyPath: 'runId' });
        if (!db.objectStoreNames.contains('nodeStates'))
          db.createObjectStore('nodeStates', { keyPath: ['runId', 'nodeId'] });
        if (!db.objectStoreNames.contains('events')) {
          const s = db.createObjectStore('events', { keyPath: 'id', autoIncrement: true });
          s.createIndex('runId', 'runId', { unique: false });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  function tx(db, store, mode, fn) {
    return new Promise((resolve, reject) => {
      const t = db.transaction(store, mode);
      const s = t.objectStore(store);
      const out = fn(s);
      t.oncomplete = () => resolve(out && out._result !== undefined ? out._result : out);
      t.onerror = () => reject(t.error);
      t.onabort = () => reject(t.error);
    });
  }

  function reqToPromise(req) {
    return new Promise((resolve, reject) => {
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  class WorkflowDB {
    constructor(db) { this.db = db; }
    static async open() { return new WorkflowDB(await open()); }

    putRun(run) { return tx(this.db, 'runs', 'readwrite', (s) => s.put(run)); }
    getRun(runId) { return reqToPromise(this.db.transaction('runs').objectStore('runs').get(runId)); }
    listRuns() {
      return reqToPromise(this.db.transaction('runs').objectStore('runs').getAll())
        .then((runs) => runs.sort((a, b) => b.startedAt - a.startedAt));
    }
    deleteRun(runId) {
      return Promise.all([
        tx(this.db, 'runs', 'readwrite', (s) => s.delete(runId)),
        tx(this.db, 'nodeStates', 'readwrite', (s) => {
          // 遍历删除该 run 的节点状态
          s.openCursor().onsuccess = (e) => {
            const c = e.target.result;
            if (c) { if (c.key[0] === runId) c.delete(); c.continue(); }
          };
        }),
        tx(this.db, 'events', 'readwrite', (s) => {
          s.index('runId').openCursor(IDBKeyRange.only(runId)).onsuccess = (e) => {
            const c = e.target.result;
            if (c) { c.delete(); c.continue(); }
          };
        }),
      ]);
    }

    putNodeState(state) { return tx(this.db, 'nodeStates', 'readwrite', (s) => s.put(state)); }
    putNodeStates(states) {
      return tx(this.db, 'nodeStates', 'readwrite', (s) => states.forEach((st) => s.put(st)));
    }
    getNodeStates(runId) {
      return reqToPromise(this.db.transaction('nodeStates').objectStore('nodeStates').getAll())
        .then((all) => all.filter((s) => s.runId === runId));
    }

    appendEvent(ev) { return tx(this.db, 'events', 'readwrite', (s) => s.add(ev)); }
    listEvents(runId) {
      return reqToPromise(
        this.db.transaction('events').objectStore('events').index('runId').getAll(IDBKeyRange.only(runId))
      );
    }

    // 崩溃恢复：把上次遗留的 running/paused 运行标记为 interrupted
    async recoverInterrupted() {
      const runs = await this.listRuns();
      const stale = runs.filter((r) => r.status === 'running' || r.status === 'paused');
      for (const r of stale) {
        r.status = 'interrupted';
        r.endedAt = Date.now();
        await this.putRun(r);
        const nodes = await this.getNodeStates(r.runId);
        for (const n of nodes) {
          if (['running', 'retrying', 'ready', 'pending'].includes(n.status)) {
            n.status = 'blocked';
            n.error = 'run interrupted (page closed)';
            await this.putNodeState(n);
          }
        }
      }
      return stale.map((r) => r.runId);
    }
  }

  return WorkflowDB;
});
