# DAG 工作流引擎

纯前端工作流引擎：**Web Worker 调度 + Canvas 可视化 + IndexedDB 持久化 + BroadcastChannel 多标签页同步**，主线程零阻塞。

## 运行

```bash
cd B
python3 -m http.server 8000
# 打开 http://localhost:8000
```

> 必须通过 HTTP 访问（Web Worker 在 file:// 下被浏览器拦截）。

## 测试

```bash
node test/dag.test.js      # 纯逻辑单测：校验/拓扑/条件/退避/死锁（14 项）
node test/engine.test.js   # 引擎集成测试：在 vm 中模拟 Worker 驱动真实调度（9 项）
```

## 架构

```
index.html            页面骨架
css/style.css         深色主题样式
js/dag.js             纯逻辑层：DAG 校验 / 环检测 / 拓扑分层 / 条件边求值 / 退避计算 / 死锁判定
js/worker.js          调度引擎（Web Worker）：并发调度 / 重试退避 / 超时中断 / 持久化 / 广播
js/db.js              IndexedDB 封装（runs / nodeStates / events 三个 store）
js/canvas.js          Canvas 渲染器：分层布局 / 状态色 / 流动动画 / 重试角标
js/main.js            主线程 UI 胶水：编辑器 / 历史列表 / 日志 / 快照分发
test/                 Node 测试（dag.js 与 worker.js 均被直接复用测试）
```

## DAG 定义格式

```jsonc
{
  "name": "pipeline",
  "nodes": [
    {
      "id": "pay",
      "type": "flaky",                    // sleep | compute | flaky | maybe | emit
      "params": { "failTimes": 2 },
      "timeoutMs": 5000,                  // 超时自动中断（AbortController）
      "retry": { "maxAttempts": 4, "baseMs": 400, "factor": 2, "jitter": 0.2 },
      "join": "all"                       // all=全部入边走通 | any=任一入边走通
    }
  ],
  "edges": [
    { "from": "a", "to": "b", "when": "always" },     // 默认
    { "from": "a", "to": "c", "when": "onSuccess" },
    { "from": "a", "to": "d", "when": "onFailure" },
    { "from": "a", "to": "e", "when": "expr", "expr": "result.value > 10" }
  ]
}
```

## 验收标准对照

| 验收点 | 实现 |
| --- | --- |
| DAG 校验 | `dag.js validateDAG`：重复 id / 悬空边 / 自环 / 非法表达式 / 非法 retry·timeout |
| 条件分支 | 边条件 `always / onSuccess / onFailure / expr` + 节点 `join: all/any`，skip 级联 |
| 重试退避 | `backoffDelay`：指数退避 `base·factor^(n-1)` + 上限 + 抖动，Worker 定时器调度 |
| 超时可中断 | `Promise.race` + `AbortController`，任务（含 CPU 分片计算）实时响应中断 |
| 并发调度 | `maxConcurrent` 槽位泵（`Run.pump`），集成测试验证峰值并发与分批 |
| 死锁检测 | 静态：DFS 三色环检测拒绝运行；动态：`isRuntimeDeadlocked` 守卫 + 阻塞级联标记 |
| 状态持久化 | 每次状态迁移写入 IndexedDB；刷新后可从历史恢复视图；崩溃运行标记 `interrupted` |
| 可视化实时 | Worker 快照 → rAF Canvas 渲染；运行节点脉冲光晕、边上流动虚线、重试角标 |
| 主线程不卡 | 调度/计算/持久化全在 Worker；主线程仅绘制（CPU 密集 `compute` 任务也在 Worker 分片执行） |

## 多标签页

引擎通过 `BroadcastChannel('workflow-engine')` 广播快照与事件：在标签页 A 启动运行，标签页 B 勾选「跟随其他标签页」即可实时看到同一运行的画布与日志。
