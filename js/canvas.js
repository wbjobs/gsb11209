/*
 * canvas.js — Canvas 实时可视化渲染器
 * 布局：按拓扑分层分列；渲染：节点状态色 / 重试角标 / 边流动画 / 死锁高亮。
 * 渲染完全由 requestAnimationFrame 驱动，数据来自 Worker 快照（主线程只做绘制）。
 */
(function (root) {
  'use strict';

  const STATUS_COLOR = {
    pending:  { fill: '#2b3245', stroke: '#4a5578', text: '#9aa7c7' },
    ready:    { fill: '#31456b', stroke: '#5b8def', text: '#cfe0ff' },
    running:  { fill: '#1f4d3a', stroke: '#2fd27d', text: '#b8f5d6' },
    retrying: { fill: '#4d3a1f', stroke: '#f0a832', text: '#ffe1a8' },
    success:  { fill: '#173f2c', stroke: '#27ae60', text: '#9fe8c1' },
    failed:   { fill: '#4d1f1f', stroke: '#e05252', text: '#ffb3b3' },
    skipped:  { fill: '#26262e', stroke: '#55555f', text: '#7a7a88' },
    blocked:  { fill: '#3d1f3d', stroke: '#c050c0', text: '#e8a8e8' },
  };

  const NODE_W = 168;
  const NODE_H = 56;
  const GAP_X = 90;
  const GAP_Y = 26;
  const PAD = 24;

  class WorkflowCanvas {
    constructor(canvas) {
      this.canvas = canvas;
      this.ctx = canvas.getContext('2d');
      this.run = null;          // 最近一次快照
      this.layout = null;       // {positions: Map nodeId->{x,y}, width, height}
      this.dashOffset = 0;
      this.pulse = 0;
      this._raf = null;
      this._lastFrame = 0;
      this._loop = this._loop.bind(this);
      this._resizeObserver = new ResizeObserver(() => this._fitCanvas());
      this._resizeObserver.observe(canvas.parentElement);
      this._fitCanvas();
      this._raf = requestAnimationFrame(this._loop);
    }

    setRun(run) {
      this.run = run;
      this.layout = run ? computeLayout(run.def) : null;
    }

    _fitCanvas() {
      const parent = this.canvas.parentElement;
      const dpr = window.devicePixelRatio || 1;
      const w = parent.clientWidth;
      const h = Math.max(parent.clientHeight, this.layout ? this.layout.height + PAD * 2 : 320);
      this.canvas.width = Math.round(w * dpr);
      this.canvas.height = Math.round(h * dpr);
      this.canvas.style.width = w + 'px';
      this.canvas.style.height = h + 'px';
      this.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    }

    _loop(ts) {
      const dt = Math.min(64, ts - this._lastFrame || 16);
      this._lastFrame = ts;
      this.dashOffset = (this.dashOffset + dt * 0.02) % 24;
      this.pulse = (this.pulse + dt * 0.004) % (Math.PI * 2);
      this._draw();
      this._raf = requestAnimationFrame(this._loop);
    }

    _draw() {
      const ctx = this.ctx;
      const w = this.canvas.clientWidth;
      const h = this.canvas.clientHeight;
      ctx.clearRect(0, 0, w, h);
      if (!this.run || !this.layout) {
        ctx.fillStyle = '#5b6478';
        ctx.font = '14px system-ui, sans-serif';
        ctx.textAlign = 'center';
        ctx.fillText('点击「运行」后此处实时显示 DAG 执行状态', w / 2, h / 2);
        return;
      }
      const { positions } = this.layout;
      const nodeState = Object.create(null);
      for (const n of this.run.nodes) nodeState[n.nodeId] = n;

      // 先画边
      for (const e of this.run.def.edges) {
        const a = positions[e.from];
        const b = positions[e.to];
        if (!a || !b) continue;
        const from = nodeState[e.from];
        const active = from && (from.status === 'running' || from.status === 'retrying');
        const done = from && (from.status === 'success' || from.status === 'failed');
        ctx.beginPath();
        ctx.moveTo(a.x + NODE_W, a.y + NODE_H / 2);
        const mx = a.x + NODE_W + GAP_X / 2;
        ctx.bezierCurveTo(mx, a.y + NODE_H / 2, mx, b.y + NODE_H / 2, b.x, b.y + NODE_H / 2);
        if (active) {
          ctx.strokeStyle = '#2fd27d';
          ctx.lineWidth = 2;
          ctx.setLineDash([8, 6]);
          ctx.lineDashOffset = -this.dashOffset;
        } else if (done) {
          ctx.strokeStyle = from.status === 'success' ? '#27ae60' : '#e05252';
          ctx.lineWidth = 1.6;
          ctx.setLineDash([]);
        } else {
          ctx.strokeStyle = '#3a4358';
          ctx.lineWidth = 1.2;
          ctx.setLineDash([]);
        }
        ctx.stroke();
        ctx.setLineDash([]);
        // 条件标签
        if (e.when && e.when !== 'always') {
          ctx.fillStyle = '#7f8db3';
          ctx.font = '10px system-ui, sans-serif';
          ctx.textAlign = 'center';
          const label = e.when === 'expr' ? 'expr' : e.when;
          ctx.fillText(label, mx, (a.y + b.y + NODE_H) / 2 - 4);
        }
      }

      // 再画节点
      for (const n of this.run.nodes) {
        const p = positions[n.nodeId];
        if (!p) continue;
        const c = STATUS_COLOR[n.status] || STATUS_COLOR.pending;
        const glow = n.status === 'running' ? (Math.sin(this.pulse) + 1) / 2 : 0;

        ctx.save();
        if (n.status === 'running') {
          ctx.shadowColor = 'rgba(47, 210, 125, ' + (0.35 + glow * 0.4) + ')';
          ctx.shadowBlur = 14 + glow * 10;
        }
        roundRect(ctx, p.x, p.y, NODE_W, NODE_H, 8);
        ctx.fillStyle = c.fill;
        ctx.fill();
        ctx.shadowBlur = 0;
        ctx.lineWidth = n.status === 'running' ? 2 : 1.4;
        ctx.strokeStyle = c.stroke;
        ctx.stroke();
        ctx.restore();

        // 节点文本
        ctx.fillStyle = c.text;
        ctx.font = '600 13px system-ui, sans-serif';
        ctx.textAlign = 'left';
        ctx.fillText(n.nodeId, p.x + 10, p.y + 21, NODE_W - 20);
        ctx.font = '11px system-ui, sans-serif';
        ctx.fillStyle = c.stroke;
        const sub = n.status === 'retrying'
          ? `retrying · 第 ${n.attempt} 次后退避中`
          : n.status + (n.durationMs != null ? ` · ${n.durationMs}ms` : '');
        ctx.fillText(sub, p.x + 10, p.y + 39, NODE_W - 20);

        // 尝试次数角标
        if (n.attempt > 1) {
          ctx.beginPath();
          ctx.arc(p.x + NODE_W - 12, p.y + 12, 10, 0, Math.PI * 2);
          ctx.fillStyle = '#f0a832';
          ctx.fill();
          ctx.fillStyle = '#1a1a1a';
          ctx.font = '700 10px system-ui, sans-serif';
          ctx.textAlign = 'center';
          ctx.fillText('×' + n.attempt, p.x + NODE_W - 12, p.y + 15.5);
        }
      }

      // 运行状态横幅
      const statusText = `run: ${this.run.status} · 并发 ${this.run.runningCount}/${this.run.maxConcurrent}`;
      ctx.fillStyle = '#8b96b5';
      ctx.font = '12px system-ui, sans-serif';
      ctx.textAlign = 'right';
      ctx.fillText(statusText, w - 12, 18);
    }

    destroy() { cancelAnimationFrame(this._raf); this._resizeObserver.disconnect(); }
  }

  function computeLayout(def) {
    const { layers } = DAG.topoLayers(def);
    const positions = Object.create(null);
    let maxRows = 0;
    layers.forEach((layer, li) => {
      maxRows = Math.max(maxRows, layer.length);
      layer.forEach((id, ri) => {
        positions[id] = { x: PAD + li * (NODE_W + GAP_X), y: PAD + ri * (NODE_H + GAP_Y) };
      });
    });
    // 垂直居中：行数少的层向下偏移
    layers.forEach((layer) => {
      const offset = ((maxRows - layer.length) * (NODE_H + GAP_Y)) / 2;
      layer.forEach((id) => { positions[id].y += offset; });
    });
    return {
      positions,
      width: PAD * 2 + layers.length * (NODE_W + GAP_X),
      height: PAD * 2 + maxRows * (NODE_H + GAP_Y),
    };
  }

  function roundRect(ctx, x, y, w, h, r) {
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.arcTo(x + w, y, x + w, y + h, r);
    ctx.arcTo(x + w, y + h, x, y + h, r);
    ctx.arcTo(x, y + h, x, y, r);
    ctx.arcTo(x, y, x + w, y, r);
    ctx.closePath();
  }

  root.WorkflowCanvas = WorkflowCanvas;
})(typeof self !== 'undefined' ? self : this);
