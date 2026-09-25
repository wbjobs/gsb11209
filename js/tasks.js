/*
 * tasks.js — 任务注册表。任务签名: async (ctx) => result
 * ctx = { signal, params, inputs, attempt, log }
 * 所有耗时任务必须响应 signal 中止 (超时中断的前提)。
 */
(function (global) {
  'use strict';

  /** 可中止的 sleep */
  function sleep(ms, signal) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        cleanup();
        resolve();
      }, ms);
      const onAbort = () => {
        cleanup();
        reject(new Error('aborted'));
      };
      const cleanup = () => {
        clearTimeout(timer);
        if (signal) signal.removeEventListener('abort', onAbort);
      };
      if (signal) {
        if (signal.aborted) { cleanup(); return reject(new Error('aborted')); }
        signal.addEventListener('abort', onAbort);
      }
    });
  }

  function rand(min, max) {
    return Math.floor(min + Math.random() * (max - min));
  }

  const tasks = {
    /** 模拟数据拉取, 随机耗时 */
    async fetchData(ctx) {
      await sleep(rand(300, 900), ctx.signal);
      return { rows: rand(10, 100), source: ctx.params.source || 'default' };
    },

    /** 通用处理节点, 输出随机分数 (供条件分支判断) */
    async process(ctx) {
      await sleep(rand(200, 700), ctx.signal);
      const score = rand(0, 100);
      ctx.log('score=' + score);
      return { score };
    },

    /** 不稳定任务: 前 params.failTimes 次必失败, 用于演示重试退避 */
    async flaky(ctx) {
      await sleep(rand(100, 300), ctx.signal);
      const failTimes = ctx.params.failTimes != null ? ctx.params.failTimes : 2;
      if (ctx.attempt <= failTimes) {
        throw new Error('模拟失败 (第 ' + ctx.attempt + ' 次尝试)');
      }
      return { recovered: true, attempts: ctx.attempt };
    },

    /** 慢任务: 默认 sleep 很长, 用于演示超时中断 */
    async slow(ctx) {
      const duration = ctx.params.duration != null ? ctx.params.duration : 10000;
      await sleep(duration, ctx.signal);
      return { done: true };
    },

    /** 聚合上游全部成功结果 */
    async aggregate(ctx) {
      await sleep(rand(100, 300), ctx.signal);
      return { inputs: ctx.inputs, count: Object.keys(ctx.inputs).length };
    },

    /** 失败告警 (异常链路处理节点, 接 onFailure 边) */
    async alert(ctx) {
      await sleep(100, ctx.signal);
      return { alerted: true };
    },
  };

  const api = { tasks, sleep };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  global.WFTasks = api;
})(typeof self !== 'undefined' ? self : globalThis);
