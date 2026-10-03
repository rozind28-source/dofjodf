/* ==========================================================================
   CandleChart — свечной график на canvas.

   Почему не TradingView lightweight-charts: он тянется с CDN, а интерфейс
   должен работать офлайн и в песочнице предпросмотра без сети. Свой рендерер —
   ~450 строк, зато ноль зависимостей и полный контроль над overlays
   (плотности стакана и спайки поверх свечей — то, ради чего скринер и делается).

   Возможности: свечи + объём, перекрестье с тултипом, зум колесом,
   панорамирование drag'ом, линии плотностей, маркеры спайков,
   автообновление последней свечи, HiDPI.
   ========================================================================== */
"use strict";

class CandleChart {
  constructor(canvas, opts = {}) {
    this.cv = canvas;
    this.ctx = canvas.getContext("2d");
    this.o = Object.assign({
      upColor: "#16c784", dnColor: "#ea3943",
      gridColor: "rgba(255,255,255,.055)",
      axisColor: "#5d6b7d", textColor: "#8c99a9",
      volColor: "rgba(120,140,170,.32)",
      bidColor: "rgba(22,199,132,.75)", askColor: "rgba(234,57,67,.75)",
      crossColor: "rgba(180,200,225,.45)",
      padLeft: 6, padRight: 64, padTop: 10, padBottom: 20,
      volRatio: 0.2,          // доля высоты под объём
      minBars: 20, maxBars: 600,
    }, opts);

    this.data = { candles: [], densities: [], spikes: [], last: null };
    this.view = { from: 0, to: 0 };      // индексы видимого окна
    this.hover = null;                   // {x, y, i}
    this.tfSeconds = 900;
    this.dragging = false;
    this._dpr = 1;
    this._raf = 0;                       // отложенный render (см. _scheduleRender)

    this._bind();
    this.resize();
  }

  /* ----------------------------- данные ------------------------------ */
  setData(d) {
    this.data = Object.assign({ candles: [], densities: [], spikes: [], last: null }, d);
    const n = this.data.candles.length;
    if (!n) { this.view = { from: 0, to: 0 }; this.render(); return; }
    // при первой загрузке показываем хвост; при обновлении окно сохраняем
    const keep = this.view.to === n - 1 || this.view.to === 0;
    const span = keep ? Math.min(n, this.o.maxBars) : (this.view.to - this.view.from);
    this.view.to = n - 1;
    this.view.from = Math.max(0, n - span);
    this.render();
  }

  /** Дешёвое обновление последней свечи из live-потока, без перезапроса биржи. */
  updateLast(price, ts) {
    const c = this.data.candles;
    if (!c.length || price == null) return;
    const last = c[c.length - 1];
    const bucket = ts - (ts % (this.tfSeconds * 1000));
    if (bucket > last[0]) {                       // открылась новая свеча
      c.push([bucket, price, price, price, price, 0]);
      if (c.length > this.o.maxBars * 2) c.shift();
      this.view.to = c.length - 1;
      this.view.from = Math.max(0, this.view.to - Math.min(c.length, this.o.maxBars));
    } else {
      last[4] = price;                            // close
      last[2] = Math.max(last[2], price);         // high
      last[3] = Math.min(last[3], price);         // low
    }
    this.data.last = price;
    this._scheduleRender();
  }

  /**
   * Коалесцинг перерисовок: не более одного render() на кадр.
   *
   * Зачем: в сетке 9–16 графиков live-пуш приносит батч тиков раз в секунду;
   * раньше каждый тик рисовал canvas немедленно (до 16 полных перерисовок на
   * батч, часть из них — в один и тот же кадр, впустую). Данные мутируются
   * сразу, а отрисовка откладывается до requestAnimationFrame — максимум
   * одна на кадр.
   */
  _scheduleRender() {
    if (typeof requestAnimationFrame !== "function") { this.render(); return; }
    if (this._raf) return;
    this._raf = requestAnimationFrame(() => { this._raf = 0; this.render(); });
  }

  setTfSeconds(s) { this.tfSeconds = s || 900; }

  resize() {
    const rect = this.cv.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    this._dpr = dpr;
    const w = Math.max(80, rect.width || this.cv.clientWidth || 420);
    const h = Math.max(60, rect.height || this.cv.clientHeight || 240);
    this.cv.width = Math.round(w * dpr);
    this.cv.height = Math.round(h * dpr);
    this.w = w; this.h = h;
    this.render();
  }

  /* --------------------------- геометрия ----------------------------- */
  _geom() {
    const o = this.o;
    const plotW = this.w - o.padLeft - o.padRight;
    const plotH = this.h - o.padTop - o.padBottom;
    const volH = plotH * o.volRatio;
    return {
      x0: o.padLeft, x1: o.padLeft + plotW,
      y0: o.padTop, y1: o.padTop + plotH - volH - 6,   // низ ценовой области
      vy0: o.padTop + plotH - volH, vy1: o.padTop + plotH,
      plotW, plotH, volH,
    };
  }

  _visible() {
    const c = this.data.candles;
    const { from, to } = this.view;
    return c.slice(Math.max(0, from), Math.min(c.length, to + 1));
  }

  _scale() {
    const vis = this._visible();
    const g = this._geom();
    if (!vis.length) return null;
    let lo = Infinity, hi = -Infinity, vmax = 0;
    for (const c of vis) {
      if (c[3] < lo) lo = c[3];
      if (c[2] > hi) hi = c[2];
      if (c[5] > vmax) vmax = c[5];
    }
    // плотности внутри видимого диапазона тоже должны влезать в масштаб
    for (const d of this.data.densities || []) {
      if (d.p >= lo && d.p <= hi) continue;
    }
    if (this.data.last != null) { hi = Math.max(hi, this.data.last); lo = Math.min(lo, this.data.last); }
    if (!(hi > lo)) { hi = lo + 1; }
    const pad = (hi - lo) * 0.08;
    hi += pad; lo -= pad;
    const n = vis.length;
    const barW = g.plotW / Math.max(1, n);
    return {
      lo, hi, vmax: vmax || 1, n, barW,
      px: (i) => g.x0 + (i + 0.5) * barW,
      py: (p) => g.y1 - ((p - lo) / (hi - lo)) * (g.y1 - g.y0),
      vy: (v) => g.vy1 - (v / (vmax || 1)) * (g.vy1 - g.vy0),
      invY: (y) => lo + ((g.y1 - y) / (g.y1 - g.y0)) * (hi - lo),
      invX: (x) => Math.floor((x - g.x0) / barW),
      g, vis,
    };
  }

  /* ----------------------------- рендер ------------------------------ */
  render() {
    const ctx = this.ctx, dpr = this._dpr;
    ctx.save();
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, this.w, this.h);

    const sc = this._scale();
    if (!sc) {
      ctx.fillStyle = this.o.axisColor;
      ctx.font = "11px ui-monospace, monospace";
      ctx.textAlign = "center";
      ctx.fillText("нет данных графика", this.w / 2, this.h / 2);
      ctx.restore();
      return;
    }
    this._drawGrid(sc);
    this._drawVolumes(sc);
    this._drawDensities(sc);
    this._drawCandles(sc);
    this._drawSpikes(sc);
    this._drawLastPrice(sc);
    this._drawAxes(sc);
    if (this.hover) this._drawCrosshair(sc);
    ctx.restore();
  }

  _drawGrid(sc) {
    const ctx = this.ctx, g = sc.g;
    ctx.strokeStyle = this.o.gridColor;
    ctx.lineWidth = 1;
    ctx.beginPath();
    for (const p of this._ticks(sc.lo, sc.hi, 5)) {
      const y = Math.round(sc.py(p)) + 0.5;
      ctx.moveTo(g.x0, y); ctx.lineTo(g.x1, y);
    }
    const step = Math.max(1, Math.round(sc.n / 6));
    for (let i = 0; i < sc.n; i += step) {
      const x = Math.round(sc.px(i)) + 0.5;
      ctx.moveTo(x, g.y0); ctx.lineTo(x, g.vy1);
    }
    ctx.stroke();
  }

  _drawVolumes(sc) {
    const ctx = this.ctx;
    const bw = Math.max(1, sc.barW * 0.7);
    for (let i = 0; i < sc.n; i++) {
      const c = sc.vis[i];
      const y = sc.vy(c[5]);
      ctx.fillStyle = c[4] >= c[1] ? "rgba(22,199,132,.26)" : "rgba(234,57,67,.26)";
      ctx.fillRect(sc.px(i) - bw / 2, y, bw, sc.g.vy1 - y);
    }
  }

  _drawCandles(sc) {
    const ctx = this.ctx;
    const bw = Math.max(1, Math.min(sc.barW * 0.7, 18));
    const thin = bw < 2.5;
    for (let i = 0; i < sc.n; i++) {
      const [, o, h, l, cl] = sc.vis[i];
      const up = cl >= o;
      const col = up ? this.o.upColor : this.o.dnColor;
      const x = sc.px(i);
      ctx.strokeStyle = col;
      ctx.fillStyle = col;
      // фитиль
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(Math.round(x) + 0.5, sc.py(h));
      ctx.lineTo(Math.round(x) + 0.5, sc.py(l));
      ctx.stroke();
      // тело
      const yo = sc.py(o), yc = sc.py(cl);
      const top = Math.min(yo, yc);
      const hgt = Math.max(1, Math.abs(yc - yo));
      if (thin) ctx.fillRect(Math.round(x), top, 1, hgt);
      else ctx.fillRect(x - bw / 2, top, bw, hgt);
    }
  }

  _drawDensities(sc) {
    const ctx = this.ctx, g = sc.g;
    const dens = this.data.densities || [];
    if (!dens.length) return;
    const maxQ = Math.max(...dens.map((d) => d.q || 0)) || 1;
    ctx.save();
    ctx.beginPath();
    ctx.rect(g.x0, g.y0, g.plotW, g.y1 - g.y0);
    ctx.clip();
    ctx.font = "9px ui-monospace, monospace";
    ctx.textAlign = "left";
    for (const d of dens) {
      if (d.p < sc.lo || d.p > sc.hi) continue;
      const y = sc.py(d.p);
      const w = 0.4 + 2.6 * ((d.q || 0) / maxQ);   // толщина ∝ объёму заявки
      ctx.strokeStyle = d.sd === "bid" ? this.o.bidColor : this.o.askColor;
      ctx.globalAlpha = 0.55;
      ctx.lineWidth = w;
      ctx.setLineDash([5, 4]);
      ctx.beginPath();
      ctx.moveTo(g.x0, y); ctx.lineTo(g.x1, y);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.globalAlpha = 0.95;
      ctx.fillStyle = d.sd === "bid" ? this.o.upColor : this.o.dnColor;
      ctx.fillText(`${d.sd === "bid" ? "▲" : "▼"} ${this._money(d.q)}`, g.x0 + 3, y - 2);
    }
    ctx.restore();
  }

  _drawSpikes(sc) {
    const ctx = this.ctx, g = sc.g;
    const sp = this.data.spikes || [];
    if (!sp.length) return;
    const first = sc.vis[0] ? sc.vis[0][0] : 0;
    const last = sc.vis[sc.vis.length - 1] ? sc.vis[sc.vis.length - 1][0] : 0;
    ctx.font = "10px sans-serif";
    ctx.textAlign = "center";
    for (const s of sp) {
      if (s.ts * 1000 < first || s.ts * 1000 > last + this.tfSeconds * 1000) continue;
      const i = Math.round((s.ts * 1000 - first) / (this.tfSeconds * 1000));
      if (i < 0 || i >= sc.n) continue;
      const x = sc.px(i), y = g.y0 + 2;
      ctx.fillStyle = "#f0b90b";
      ctx.fillText("⚡", x, y + 9);
      ctx.strokeStyle = "rgba(240,185,11,.5)";
      ctx.lineWidth = 1;
      ctx.setLineDash([2, 3]);
      ctx.beginPath(); ctx.moveTo(x, y + 12); ctx.lineTo(x, g.y1); ctx.stroke();
      ctx.setLineDash([]);
    }
  }

  _drawLastPrice(sc) {
    if (this.data.last == null) return;
    const ctx = this.ctx, g = sc.g;
    const y = sc.py(this.data.last);
    if (y < g.y0 || y > g.y1) return;
    ctx.strokeStyle = "rgba(96,165,250,.85)";
    ctx.lineWidth = 1;
    ctx.setLineDash([4, 3]);
    ctx.beginPath(); ctx.moveTo(g.x0, y); ctx.lineTo(g.x1, y); ctx.stroke();
    ctx.setLineDash([]);
    this._priceTag(this.data.last, y, "#2563eb", "#fff");
  }

  _drawAxes(sc) {
    const ctx = this.ctx, g = sc.g;
    ctx.font = "10px ui-monospace, monospace";
    ctx.textAlign = "left";
    ctx.fillStyle = this.o.textColor;
    for (const p of this._ticks(sc.lo, sc.hi, 5)) {
      const y = sc.py(p);
      if (y < g.y0 - 2 || y > g.y1 + 2) continue;
      ctx.fillText(this._fmtPrice(p), g.x1 + 5, y + 3);
    }
    // время
    ctx.textAlign = "center";
    const step = Math.max(1, Math.round(sc.n / 6));
    for (let i = 0; i < sc.n; i += step) {
      ctx.fillText(this._fmtTime(sc.vis[i][0]), sc.px(i), g.vy1 + 13);
    }
    // разделитель оси
    ctx.strokeStyle = "rgba(255,255,255,.09)";
    ctx.beginPath();
    ctx.moveTo(g.x1 + 0.5, g.y0); ctx.lineTo(g.x1 + 0.5, g.vy1);
    ctx.stroke();
  }

  _drawCrosshair(sc) {
    const ctx = this.ctx, g = sc.g;
    const { x, y } = this.hover;
    if (x < g.x0 || x > g.x1 || y < g.y0 || y > g.vy1) return;
    const i = Math.max(0, Math.min(sc.n - 1, sc.invX(x)));
    const c = sc.vis[i];
    if (!c) return;
    const cx = sc.px(i);
    ctx.strokeStyle = this.o.crossColor;
    ctx.lineWidth = 1;
    ctx.setLineDash([3, 3]);
    ctx.beginPath();
    ctx.moveTo(cx, g.y0); ctx.lineTo(cx, g.vy1);
    ctx.moveTo(g.x0, y); ctx.lineTo(g.x1, y);
    ctx.stroke();
    ctx.setLineDash([]);

    if (y <= g.y1) this._priceTag(sc.invY(y), y, "#2a3441", "#dfe6ef");

    // тултип OHLCV
    const up = c[4] >= c[1];
    const lines = [
      [this._fmtTime(c[0], true), ""],
      ["O", this._fmtPrice(c[1])], ["H", this._fmtPrice(c[2])],
      ["L", this._fmtPrice(c[3])], ["C", this._fmtPrice(c[4])],
      ["Δ", ((c[4] / c[1] - 1) * 100).toFixed(2) + "%"],
      ["V", this._money(c[5])],
    ];
    const w = 116, lh = 13, h = lines.length * lh + 8;
    let tx = cx + 10; if (tx + w > g.x1) tx = cx - w - 10;
    let ty = g.y0 + 6;
    ctx.fillStyle = "rgba(12,16,22,.94)";
    ctx.strokeStyle = "rgba(255,255,255,.14)";
    this._roundRect(tx, ty, w, h, 5); ctx.fill(); ctx.stroke();
    ctx.font = "10px ui-monospace, monospace";
    ctx.textAlign = "left";
    lines.forEach(([k, v], idx) => {
      const yy = ty + 13 + idx * lh;
      ctx.fillStyle = idx === 0 ? "#8c99a9" : "#5d6b7d";
      ctx.fillText(k, tx + 7, yy);
      if (!v) return;
      ctx.textAlign = "right";
      ctx.fillStyle = k === "Δ" ? (up ? this.o.upColor : this.o.dnColor) : "#dfe6ef";
      ctx.fillText(v, tx + w - 7, yy);
      ctx.textAlign = "left";
    });
  }

  _priceTag(price, y, bg, fg) {
    const ctx = this.ctx, g = this._geom();
    const txt = this._fmtPrice(price);
    ctx.font = "10px ui-monospace, monospace";
    const w = Math.max(52, ctx.measureText(txt).width + 10);
    ctx.fillStyle = bg;
    this._roundRect(g.x1 + 2, y - 7, w, 14, 3); ctx.fill();
    ctx.fillStyle = fg;
    ctx.textAlign = "left";
    ctx.fillText(txt, g.x1 + 6, y + 3.5);
  }

  _roundRect(x, y, w, h, r) {
    const ctx = this.ctx;
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.arcTo(x + w, y, x + w, y + h, r);
    ctx.arcTo(x + w, y + h, x, y + h, r);
    ctx.arcTo(x, y + h, x, y, r);
    ctx.arcTo(x, y, x + w, y, r);
    ctx.closePath();
  }

  /* ---------------------------- утилиты ------------------------------ */
  _ticks(lo, hi, count) {
    const span = hi - lo;
    if (!(span > 0)) return [lo];
    const raw = span / count;
    const mag = Math.pow(10, Math.floor(Math.log10(raw)));
    const norm = raw / mag;
    const step = (norm >= 5 ? 5 : norm >= 2 ? 2 : 1) * mag;
    const out = [];
    for (let p = Math.ceil(lo / step) * step; p <= hi; p += step) out.push(p);
    return out;
  }

  _fmtPrice(p) {
    if (p == null || isNaN(p)) return "—";
    const a = Math.abs(p);
    if (a >= 1000) return p.toLocaleString("en-US", { maximumFractionDigits: 1 });
    if (a >= 1) return p.toFixed(a >= 100 ? 2 : 4);
    if (a >= 0.01) return p.toFixed(5);
    return p.toPrecision(4);
  }

  _fmtTime(ms, full) {
    const d = new Date(ms);
    const p = (n) => String(n).padStart(2, "0");
    if (this.tfSeconds >= 86400) return `${p(d.getDate())}.${p(d.getMonth() + 1)}`;
    if (full) return `${p(d.getDate())}.${p(d.getMonth() + 1)} ${p(d.getHours())}:${p(d.getMinutes())}`;
    return `${p(d.getHours())}:${p(d.getMinutes())}`;
  }

  _money(v) {
    if (v == null || isNaN(v)) return "—";
    const a = Math.abs(v);
    if (a >= 1e9) return (v / 1e9).toFixed(2) + "B";
    if (a >= 1e6) return (v / 1e6).toFixed(2) + "M";
    if (a >= 1e3) return (v / 1e3).toFixed(1) + "K";
    return v.toFixed(0);
  }

  /* ------------------------- интерактивность ------------------------- */
  _bind() {
    const pos = (e) => {
      const r = this.cv.getBoundingClientRect();
      return { x: e.clientX - r.left, y: e.clientY - r.top };
    };
    this.cv.addEventListener("mousemove", (e) => {
      const p = pos(e);
      if (this.dragging) {
        const sc = this._scale();
        if (sc) {
          const shift = Math.round((this._dragX - p.x) / sc.barW);
          if (shift !== 0) { this._pan(shift); this._dragX = p.x; }
        }
      }
      this.hover = p;
      this.render();
    });
    this.cv.addEventListener("mouseleave", () => { this.hover = null; this.dragging = false; this.render(); });
    this.cv.addEventListener("mousedown", (e) => { this.dragging = true; this._dragX = pos(e).x; this.cv.style.cursor = "grabbing"; });
    window.addEventListener("mouseup", () => { this.dragging = false; this.cv.style.cursor = "crosshair"; });

    this.cv.addEventListener("wheel", (e) => {
      e.preventDefault();
      const sc = this._scale(); if (!sc) return;
      const p = pos(e);
      // Якорь зума — свеча под курсором: она должна остаться на том же месте
      // экрана. Раньше при захвате за правым краем графика (самая частая
      // ситуация на live-данных) якорь выходил за пределы окна, а нижний
      // clamp «to» принудительно раздвигал окно от нулевого индекса — экран
      // привязывало к левому краю истории вместо точки под курсором.
      const n = this.data.candles.length;
      const last = n - 1;
      const span = this.view.to - this.view.from;
      const anchorIdx = Math.max(this.view.from,
                                 Math.min(last, this.view.from + sc.invX(p.x)));
      const frac = span > 0 ? (anchorIdx - this.view.from) / span : 1;
      const k = e.deltaY > 0 ? 1.18 : 1 / 1.18;
      let newSpan = Math.round(span * k);
      newSpan = Math.max(this.o.minBars, Math.min(last, newSpan));
      if (newSpan === span) return;
      let from = Math.round(anchorIdx - frac * newSpan);
      from = Math.max(0, Math.min(last - newSpan, from));
      this.view.from = from;
      this.view.to = from + newSpan;
      this.render();
    }, { passive: false });

    // двойной клик — сброс зума
    this.cv.addEventListener("dblclick", () => {
      const n = this.data.candles.length;
      this.view = { from: Math.max(0, n - Math.min(n, this.o.maxBars)), to: n - 1 };
      this.render();
    });

    let rt = null;
    window.addEventListener("resize", () => { clearTimeout(rt); rt = setTimeout(() => this.resize(), 90); });
    this.cv.style.cursor = "crosshair";
  }

  _pan(shift) {
    const n = this.data.candles.length;
    const span = this.view.to - this.view.from;
    let from = this.view.from + shift;
    from = Math.max(0, Math.min(n - 1 - span, from));
    this.view.from = from;
    this.view.to = from + span;
  }
}

// Явно публикуем класс в глобальной области.
// В браузере `class` на верхнем уровне классического <script> и так виден
// остальным скриптам, но объявление попадает в лексическое окружение, а не
// в свойства window — при загрузке через eval (тестовый стенд) или в модульной
// обёртке этого не хватает. Привязка к window делает поведение одинаковым везде.
if (typeof window !== "undefined") window.CandleChart = CandleChart;
if (typeof module !== "undefined" && module.exports) module.exports = { CandleChart };
