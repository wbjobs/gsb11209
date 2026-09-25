/*
 * viz.js — Canvas 实时可视化: 分层布局 + 状态着色 + 动画 + 平移缩放。
 * 渲染全部走 requestAnimationFrame, 引擎在 Worker 中, 主线程保持流畅。
 */
(function (global) {
  'use strict';

  const COLORS = {
    pending: '#8a94a6',
    ready: '#4f8cff',
    running: '#f5a623',
    retry: '#b06ef5',
    success: '#2fbf71',
    failed: '#ff4d4f',
    timeout: '#ff7a45',
    skipped: '#c9d1dc',
  };

  const NODE_W = 150;
  const NODE_H = 52;
  const GAP_X = 210;
  const GAP_Y = 96;

  function computeLayout(def) {
    const order = DAG.topoSort(def.nodes, def.edges);
    const { incoming } = DAG.indexEdges(def);
    const depth = new Map();
    for (const id of order) {
      const ins = incoming.get(id) || [];
      let d = 0;
      for (const e of ins) d = Math.max(d, (depth.get(e.from) || 0) + 1);
      depth.set(id, d);
    }
    const layers = new Map();
    for (const [id, d] of depth) {
      if (!layers.has(d)) layers.set(d, []);
      layers.get(d).push(id);
    }
    const pos = new Map();
    let maxLayerSize = 0;
    for (const arr of layers.values()) maxLayerSize = Math.max(maxLayerSize, arr.length);
    for (const [d, ids] of layers) {
      ids.forEach((id, i) => {
        const offsetY = (maxLayerSize - ids.length) * GAP_Y * 0.5;
        pos.set(id, { x: 60 + d * GAP_X, y: 60 + offsetY + i * GAP_Y });
      });
    }
    return pos;
  }

  function createViz(canvas) {
    const ctx = canvas.getContext('2d');
    let def = null;
    let positions = new Map();
    let nodeStates = new Map(); // nodeId -> { status, attempt, error }
    let scale = 1;
    let offsetX = 0;
    let offsetY = 0;
    let dragging = false;
    let lastX = 0;
    let lastY = 0;
    let hoverNode = null;
    let onNodeClick = null;

    function resize() {
      const dpr = window.devicePixelRatio || 1;
      const rect = canvas.getBoundingClientRect();
      canvas.width = Math.round(rect.width * dpr);
      canvas.height = Math.round(rect.height * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    }
    resize();
    window.addEventListener('resize', resize);

    function toWorld(clientX, clientY) {
      const rect = canvas.getBoundingClientRect();
      return {
        x: (clientX - rect.left - offsetX) / scale,
        y: (clientY - rect.top - offsetY) / scale,
      };
    }

    function nodeAt(wx, wy) {
      if (!def) return null;
      for (const n of def.nodes) {
        const p = positions.get(n.id);
        if (!p) continue;
        if (wx >= p.x && wx <= p.x + NODE_W && wy >= p.y && wy <= p.y + NODE_H) return n.id;
      }
      return null;
    }

    canvas.addEventListener('mousedown', (e) => {
      dragging = true;
      lastX = e.clientX;
      lastY = e.clientY;
    });
    window.addEventListener('mouseup', () => { dragging = false; });
    window.addEventListener('mousemove', (e) => {
      if (dragging) {
        offsetX += e.clientX - lastX;
        offsetY += e.clientY - lastY;
        lastX = e.clientX;
        lastY = e.clientY;
      }
      const w = toWorld(e.clientX, e.clientY);
      hoverNode = nodeAt(w.x, w.y);
      canvas.style.cursor = hoverNode ? 'pointer' : (dragging ? 'grabbing' : 'grab');
    });
    canvas.addEventListener('wheel', (e) => {
      e.preventDefault();
      const rect = canvas.getBoundingClientRect();
      const mx = e.clientX - rect.left;
      const my = e.clientY - rect.top;
      const factor = e.deltaY < 0 ? 1.1 : 0.9;
      const newScale = Math.min(3, Math.max(0.3, scale * factor));
      offsetX = mx - (mx - offsetX) * (newScale / scale);
      offsetY = my - (my - offsetY) * (newScale / scale);
      scale = newScale;
    }, { passive: false });
    canvas.addEventListener('click', (e) => {
      const w = toWorld(e.clientX, e.clientY);
      const id = nodeAt(w.x, w.y);
      if (id && onNodeClick) onNodeClick(id, nodeStates.get(id));
    });

    function roundRect(x, y, w, h, r) {
      ctx.beginPath();
      ctx.moveTo(x + r, y);
      ctx.arcTo(x + w, y, x + w, y + h, r);
      ctx.arcTo(x + w, y + h, x, y + h, r);
      ctx.arcTo(x, y + h, x, y, r);
      ctx.arcTo(x, y, x + w, y, r);
      ctx.closePath();
    }

    function drawEdge(e, now) {
      const from = positions.get(e.from);
      const to = positions.get(e.to);
      if (!from || !to) return;
      const x1 = from.x + NODE_W;
      const y1 = from.y + NODE_H / 2;
      const x2 = to.x;
      const y2 = to.y + NODE_H / 2;
      const fromState = nodeStates.get(e.from);
      const fromStatus = fromState ? fromState.status : 'pending';

      let color = '#b8c0cc';
      let width = 1.5;
      if (fromStatus === 'success') { color = '#2fbf71'; width = 2; }
      if (fromStatus === 'failed' || fromStatus === 'timeout') { color = '#ff4d4f'; width = 2; }

      ctx.strokeStyle = color;
      ctx.lineWidth = width;
      if (fromStatus === 'running') ctx.setLineDash([6, 4]);
      ctx.beginPath();
      ctx.moveTo(x1, y1);
      ctx.bezierCurveTo(x1 + 60, y1, x2 - 60, y2, x2, y2);
      ctx.stroke();
      ctx.setLineDash([]);

      // 箭头
      ctx.fillStyle = color;
      ctx.beginPath();
      ctx.moveTo(x2, y2);
      ctx.lineTo(x2 - 8, y2 - 5);
      ctx.lineTo(x2 - 8, y2 + 5);
      ctx.closePath();
      ctx.fill();

      // 条件标签
      if (e.when) {
        const mx = (x1 + x2) / 2;
        const my = (y1 + y2) / 2 - 8;
        ctx.font = '11px monospace';
        ctx.fillStyle = '#667';
        const label = e.when.length > 22 ? e.when.slice(0, 22) + '…' : e.when;
        ctx.fillText(label, mx - ctx.measureText(label).width / 2, my);
      }
    }

    function drawNode(n, now) {
      const p = positions.get(n.id);
      if (!p) return;
      const st = nodeStates.get(n.id) || { status: 'pending' };
      const color = COLORS[st.status] || COLORS.pending;

      ctx.save();
      if (st.status === 'running') {
        const pulse = 0.55 + 0.45 * Math.sin(now / 300);
        ctx.shadowColor = color;
        ctx.shadowBlur = 18 * pulse;
      }
      roundRect(p.x, p.y, NODE_W, NODE_H, 10);
      ctx.fillStyle = '#fff';
      ctx.fill();
      ctx.lineWidth = st.status === 'running' ? 3 : 2;
      ctx.strokeStyle = color;
      ctx.stroke();
      ctx.restore();

      // 状态点
      ctx.beginPath();
      ctx.arc(p.x + 16, p.y + NODE_H / 2, 6, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();

      ctx.fillStyle = '#1f2733';
      ctx.font = 'bold 13px system-ui, sans-serif';
      ctx.fillText(n.id, p.x + 30, p.y + 22);
      ctx.font = '11px system-ui, sans-serif';
      ctx.fillStyle = '#667';
      let sub = n.task;
      if (st.status === 'retry') sub += ' · 退避重试中';
      else if (st.attempt > 1) sub += ' · 第' + st.attempt + '次';
      ctx.fillText(sub, p.x + 30, p.y + 40);

      if (hoverNode === n.id) {
        ctx.font = '11px system-ui, sans-serif';
        ctx.fillStyle = '#334';
        const err = st.error ? ' · ' + st.error.message : '';
        ctx.fillText(st.status + err, p.x, p.y - 8);
      }
    }

    function frame(now) {
      const rect = canvas.getBoundingClientRect();
      ctx.clearRect(0, 0, rect.width, rect.height);
      ctx.save();
      ctx.translate(offsetX, offsetY);
      ctx.scale(scale, scale);
      if (def) {
        for (const e of def.edges) drawEdge(e, now);
        for (const n of def.nodes) drawNode(n, now);
      }
      ctx.restore();
      requestAnimationFrame(frame);
    }
    requestAnimationFrame(frame);

    return {
      setGraph(newDef) {
        def = newDef;
        positions = computeLayout(newDef);
        nodeStates = new Map();
        offsetX = 0; offsetY = 0; scale = 1;
      },
      updateNode(node) {
        nodeStates.set(node.nodeId, node);
      },
      setNodes(nodes) {
        nodeStates = new Map();
        for (const n of nodes) nodeStates.set(n.nodeId, n);
      },
      reset() {
        nodeStates = new Map();
      },
      setOnNodeClick(fn) { onNodeClick = fn; },
    };
  }

  global.WFViz = { createViz, COLORS };
})(typeof self !== 'undefined' ? self : globalThis);
