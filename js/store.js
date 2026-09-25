/*
 * store.js — 状态持久化层。
 * 浏览器/Worker 中使用 IndexedDB; Node (测试) 中自动降级为内存实现。
 * 三个对象仓库: runs(运行记录) / nodes(节点状态) / events(事件日志)。
 */
(function (global) {
  'use strict';

  const DB_NAME = 'workflow-engine';
  const DB_VERSION = 1;

  function openDB() {
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, DB_VERSION);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains('runs')) {
          db.createObjectStore('runs', { keyPath: 'runId' });
        }
        if (!db.objectStoreNames.contains('nodes')) {
          const s = db.createObjectStore('nodes', { keyPath: ['runId', 'nodeId'] });
          s.createIndex('byRun', 'runId', { unique: false });
        }
        if (!db.objectStoreNames.contains('events')) {
          const s = db.createObjectStore('events', { keyPath: 'seq', autoIncrement: true });
          s.createIndex('byRun', 'runId', { unique: false });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  function tx(db, store, mode, fn) {
    return new Promise((resolve, reject) => {
      const t = db.transaction(store, mode);
      const req = fn(t.objectStore(store));
      t.oncomplete = () => resolve(req ? req.result : undefined);
      t.onerror = () => reject(t.error);
      t.onabort = () => reject(t.error);
    });
  }

  function getAllByIndex(db, store, indexName, key) {
    return new Promise((resolve, reject) => {
      const t = db.transaction(store, 'readonly');
      const req = t.objectStore(store).index(indexName).getAll(key);
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  async function createIndexedStore() {
    const db = await openDB();
    return {
      kind: 'indexeddb',
      putRun: (run) => tx(db, 'runs', 'readwrite', s => s.put(run)),
      getRun: (runId) => tx(db, 'runs', 'readonly', s => s.get(runId)),
      listRuns: () => tx(db, 'runs', 'readonly', s => s.getAll()),
      putNode: (nodeState) => tx(db, 'nodes', 'readwrite', s => s.put(nodeState)),
      getNodes: (runId) => getAllByIndex(db, 'nodes', 'byRun', runId),
      appendEvent: (event) => tx(db, 'events', 'readwrite', s => s.add(event)),
      getEvents: (runId) => getAllByIndex(db, 'events', 'byRun', runId),
      clear: async () => {
        await tx(db, 'runs', 'readwrite', s => s.clear());
        await tx(db, 'nodes', 'readwrite', s => s.clear());
        await tx(db, 'events', 'readwrite', s => s.clear());
      },
    };
  }

  function createMemoryStore() {
    const runs = new Map();
    const nodes = new Map();
    const events = [];
    let seq = 0;
    return {
      kind: 'memory',
      putRun: async (run) => { runs.set(run.runId, { ...run }); },
      getRun: async (runId) => runs.get(runId) || null,
      listRuns: async () => [...runs.values()],
      putNode: async (ns) => { nodes.set(ns.runId + '/' + ns.nodeId, { ...ns }); },
      getNodes: async (runId) => [...nodes.values()].filter(n => n.runId === runId),
      appendEvent: async (event) => { events.push({ ...event, seq: ++seq }); },
      getEvents: async (runId) => events.filter(e => e.runId === runId),
      clear: async () => { runs.clear(); nodes.clear(); events.length = 0; },
    };
  }

  async function createStore() {
    if (typeof indexedDB !== 'undefined') return createIndexedStore();
    return createMemoryStore();
  }

  const api = { createStore, createMemoryStore };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  global.WFStore = api;
})(typeof self !== 'undefined' ? self : globalThis);
