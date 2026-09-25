# 工作流引擎 (DAG Workflow Engine)

纯前端工作流引擎: **Web Worker 调度 + Canvas 可视化 + IndexedDB 持久化 + BroadcastChannel 跨标签页同步**, 零依赖、零构建。

## 运行

```bash
# 任意静态服务器 (Worker 要求 http 协议)
python3 -m http.server 8080
# 打开 http://localhost:8080
```

## 测试

```bash
node test/engine.test.js   # 41 项验收断言
```

## 架构

```
主线程                    Web Worker
┌──────────────┐        ┌─────────────────────┐
│ Canvas 渲染   │◄──────│ 引擎 (调度/重试/超时) │
│ 事件日志/UI   │        │   ↓                  │
└──────▲───────┘        │ IndexedDB 持久化     │
       │                └─────────┬───────────┘
       └──── BroadcastChannel ────┘ (多标签页实时同步)
```

| 文件 | 职责 |
|---|---|
| `js/dag.js` | DAG 校验 (环/重复 id/悬空边/自环)、拓扑排序、运行时死锁检测 |
| `js/engine.js` | 并发调度泵、条件分支求值、指数退避重试、AbortController 超时中断、异常链路传导 |
| `js/store.js` | IndexedDB 持久化 (runs/nodes/events 三个仓库), Node 下降级内存实现 |
| `js/tasks.js` | 任务注册表, 任务通过 `ctx.signal` 响应中止 |
| `js/worker.js` | Worker 入口: 消息协议 + 双通道广播 (postMessage + BroadcastChannel) |
| `js/viz.js` | Canvas 分层布局、状态着色、脉冲动画、平移缩放 |
| `js/main.js` | UI 接线、历史运行回放、FPS 指示器 |

## DAG 定义

```js
{
  id: 'demo',
  nodes: [
    { id: 'fetch', task: 'fetchData', timeout: 3000, retries: 1, backoff: 300 },
    { id: 'reportA', task: 'process' },
  ],
  edges: [
    { from: 'analyze', to: 'reportA', when: 'result.score >= 50' },
    { from: 'flaky',  to: 'alert',   when: 'onFailure' },   // 异常链路处理
  ],
}
```

- `when` 缺省: 上游成功时触发; `always`: 任意终态触发; `onFailure`: 失败/超时触发;
  其他字符串按表达式求值, 上下文为 `(status, result, error, outputs)`。
- 节点属性: `timeout` (ms, 超时 AbortController 中断)、`retries`、`backoff` (基数 ms)、
  `backoffFactor` (默认 2, 延迟 = backoff × factor^(attempt-1) + 抖动)。

## 验收标准对照

| 标准 | 实现 | 测试 |
|---|---|---|
| DAG 校验 | `validateDAG`: 环/重复/悬空/自环 | `testValidate` |
| 条件分支 | 边表达式求值 + 未命中分支跳过并传导 | `testConditional` |
| 重试退避 | 指数退避 + 抖动, 耗尽后终态 failed | `testRetryBackoff` |
| 超时可中断 | 每节点定时器 + AbortSignal | `testTimeout` |
| 并发调度 | 调度泵按 maxConcurrent 填槽, 依赖有序 | `testConcurrency` |
| 死锁检测 | 静态: 环校验; 动态: `detectRuntimeDeadlock` | `testDeadlock` |
| 状态持久化 | 每次状态迁移写 IndexedDB, 可回放历史 | `testPersistence` |
| 可视化实时 | 状态变更即时广播, Canvas rAF 重绘 | 页面验证 |
| 主线程不卡 | 引擎全在 Worker, 页面右上角实时 FPS | 页面验证 |

## 演示 DAG 覆盖的场景

- `fetch` → `clean` → `analyze` → (`score>=50` ? `reportA` : `reportB`) → `aggregate`: 条件分支 + 汇聚
- `flaky` (前 2 次必失败, retries=3): 重试退避; 最终失败时 `onFailure` 边激活 `alert`
- `slowReport` (执行 8s, timeout 1.5s): 超时中断
- 9 节点并发上限可调 (1–16)
