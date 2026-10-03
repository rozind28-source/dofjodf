/* ==========================================================================
   Crypto Screener — фронтенд.

   Архитектура:
     1. /api/meta      → декларативное описание фильтров (UI строится из него)
     2. WebSocket /ws  → push-стрим отфильтрованных строк раз в ~1 сек
     3. Рендер         → инкрементальный: строки таблицы не пересоздаются,
                          а обновляются по месту (иначе на 500 строках/сек
                          браузер захлёбывается и пропадает подсветка сделок)
     4. DEMO-fallback  → если бэкенд недоступен (например, файл открыли
                          напрямую или в песочнице без сети), данные
                          генерируются на клиенте, чтобы UI был живой.
   ========================================================================== */
"use strict";

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

const state = {
  view: "map",
  meta: null,
  rows: [],
  rowIndex: new Map(),     // key → row
  prevPrices: new Map(),   // key → last price (для flash-подсветки)
  _cellCache: new Map(),   // key → {html[], cls[]} — diff ячеек таблицы без чтения innerHTML
  _rowEls: new Map(),      // key → <tr> — переиспользование строк без querySelector
  mapEls: new Map(),       // key → {el, ссылки на детей, кэш значений} — плитки карты
  query: { sort: "vol", desc: "1", limit: "150" },
  filters: {},             // field → {min,max} | true
  exchanges: new Set(),
  mt: "swap",           // глобальный тумблер рынка: "swap" | "spot"
  grid: { ex: "", tf: "5m", n: 9, cells: [], charts: new Map(), timer: null, tfSeconds: 300 },
  overview: null,
  ws: null,
  wsOk: false,
  demo: false,
  activePreset: null,
  alerts: { rules: [], events: [] },
  densRows: [],
  sortCol: "vol",
  sortDesc: true,
  chart: null,          // экземпляр CandleChart открытой карточки
  chartSource: null,    // "exchange" | "resampled" | "demo"
  chartKey: null,
  chartTf: "15m",
  // фокус-режим: одна биржа + один рынок + top-N по текущему фильтру.
  // server=true, когда бэкенд реально перестроил подписки (live/replay);
  // в DEMO отбор делается на клиенте (server=false).
  focus: { enabled: false, ex: "", limit: 50, interval: 15, pauseOthers: true,
           count: 0, nextIn: null, note: "", error: "", virtual: false,
           server: false, serverActive: false, lastMs: 0, keys: [],
           dedupeBase: true, provisional: false },
  // Полная вселенная выбранной биржи для таблицы/карты в фокус-режиме.
  // WS-пуш в фокусе узкий (только top-N стримов) — иначе вся экономия
  // пропадает, — поэтому таблицу кормим REST-срезом, а тики подмешиваем.
  baseRows: [],
  baseIndex: new Map(),
  baseSig: "",
  baseSortSig: "",
  baseMerged: [],
};

const TFS = ["1m", "5m", "15m", "1h", "4h", "1d"];

/**
 * fetch с жёстким таймаутом.
 *
 * Без него висящий запрос (медленная биржа, забитый event loop сервера)
 * оставляет UI в состоянии «загрузка…» навсегда — пользователь видит пустую
 * сетку и бесконечный спиннер и не понимает, жив ли интерфейс. По таймауту
 * показываем понятное состояние и даём кнопку повтора.
 */
function fetchT(url, ms = 12000, opts) {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), ms);
  return fetch(url, Object.assign({}, opts, { signal: ctl.signal, cache: "no-store" }))
    .finally(() => clearTimeout(t));
}
const TF_SEC = { "1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400 };

/* ============================== ФОРМАТИРОВАНИЕ ========================== */
const fmt = {
  compact(v) {
    if (v == null || isNaN(v)) return "—";
    const a = Math.abs(v);
    const sign = v < 0 ? "-" : "";
    if (a >= 1e12) return sign + (a / 1e12).toFixed(2) + "T";
    if (a >= 1e9) return sign + (a / 1e9).toFixed(2) + "B";
    if (a >= 1e6) return sign + (a / 1e6).toFixed(2) + "M";
    if (a >= 1e3) return sign + (a / 1e3).toFixed(1) + "K";
    return sign + a.toFixed(a < 10 ? 2 : 0);
  },
  price(v) {
    if (v == null || isNaN(v) || v === 0) return "—";
    const a = Math.abs(v);
    if (a >= 1000) return v.toLocaleString("en-US", { maximumFractionDigits: 2 });
    if (a >= 1) return v.toFixed(a >= 100 ? 2 : 4);
    if (a >= 0.01) return v.toFixed(5);
    return v.toPrecision(4);
  },
  pct(v, digits = 2) {
    if (v == null || isNaN(v)) return "—";
    return (v > 0 ? "+" : "") + v.toFixed(digits) + "%";
  },
  num(v, d = 2) {
    if (v == null || isNaN(v)) return "—";
    return v.toFixed(d);
  },
  int(v) {
    if (v == null || isNaN(v)) return "—";
    return fmt.compact(v);
  },
  fund(v) {
    if (v == null || isNaN(v)) return "—";
    return (v * 100).toFixed(4) + "%";
  },
  spike(s) {
    if (!s) return "";
    return "⚡" + (s.ratio ? s.ratio.toFixed(1) + "x" : "");
  },
};

/* цвет плитки/ячейки по проценту изменения */
function chgColor(chg) {
  if (chg == null || isNaN(chg)) return "#3a4553";
  const t = Math.max(-1, Math.min(1, chg / 10));
  const flat = [58, 69, 83];
  const target = t >= 0 ? [14, 159, 110] : [185, 28, 28];
  const k = Math.pow(Math.abs(t), 0.62);
  const c = flat.map((f, i) => Math.round(f + (target[i] - f) * k));
  return `rgb(${c[0]},${c[1]},${c[2]})`;
}

function esc(s) {
  return String(s == null ? "" : s).replace(/[&<>"']/g, (m) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[m]));
}

/* ============================== КОЛОНКИ ТАБЛИЦЫ ========================= */
const COLUMNS = [
  { k: "s", label: "Монета", cls: "sym", sortable: true },
  { k: "exl", label: "Биржа", cls: "ex", sortable: true },
  { k: "mt", label: "Рынок", sortable: true },
  { k: "last", label: "Цена", r: 1, f: "price", flash: true },
  { k: "chg", label: "24ч", r: 1, f: "pct", color: true },
  { k: "r300", label: "5м", r: 1, f: "pct", color: true },
  { k: "r900", label: "15м", r: 1, f: "pct", color: true },
  { k: "r3600", label: "1ч", r: 1, f: "pct", color: true },
  { k: "r14400", label: "4ч", r: 1, f: "pct", color: true },
  { k: "rng", label: "Диап.", r: 1, f: "pct" },
  { k: "natr", label: "NATR", r: 1, f: (v) => fmt.num(v, 2) },
  { k: "vol", label: "Объём", r: 1, f: "compact" },
  { k: "tr", label: "Сделки", r: 1, f: "int" },
  { k: "d1m", label: "Δ1м", r: 1, f: "compact", color: true },
  { k: "cvd", label: "CVD", r: 1, f: "compact", color: true },
  { k: "imb", label: "Имбал.", r: 1, f: (v) => fmt.num(v, 2) },
  { k: "fund", label: "Funding", r: 1, f: "fund", color: true },
  { k: "oiusd", label: "OI", r: 1, f: "compact" },
  { k: "spike", label: "Спайк", r: 1, f: "spike", cls: "spk" },
];

function cellValue(col, row) {
  const v = row[col.k];
  if (col.f == null) {
    if (col.k === "s") return esc((row.b || v || "").toString());
    if (col.k === "mt") return `<span class="mt-tag">${esc(row.mt || "")}</span>`;
    if (col.k === "exl") return esc(v) + (row.dex ? ' <span class="dex-tag" title="Децентрализованная биржа">DEX</span>' : "");
    return esc(v);
  }
  if (typeof col.f === "function") return col.f(v, row);
  return fmt[col.f](v);
}

/* ============================== ИНИЦИАЛИЗАЦИЯ =========================== */
async function boot() {
  bindStatic();
  state.view = "grid";
  try {
    const r = await fetchT("/api/meta", 10000);
    if (!r.ok) throw new Error("meta " + r.status);
    state.meta = await r.json();
    setMode(state.meta.mode === "live" ? "live" : "replay");
  } catch (e) {
    console.warn("[screener] backend недоступен → DEMO:", e.message);
    state.meta = demoMeta();
    setMode("demo");
  }
  buildFiltersUI();
  buildPresets();
  buildChips();
  buildGridControls();
  buildFocusPanel();      // после buildChips: панель управляет одиночным выбором биржи
  buildAlertForm();
  buildGridHead();
  connect();
  if (state.demo) startDemo();
  await loadFocus();      // сервер мог остаться в фокус-режиме с прошлого раза
  if (state.view === "grid") loadGrid();   // стартовая вкладка — сетка графиков
  setInterval(() => { if (state.demo) demoTick(); }, 1000);
  // обратный отсчёт до перепроверки отбора — раз в секунду, без запросов
  setInterval(renderFocus, 1000);
}

function setMode(mode) {
  state.demo = mode === "demo";
  const pill = $("#mode-pill");
  pill.className = "pill " + mode;
  pill.textContent = mode === "live" ? "● LIVE" : mode === "replay" ? "● REPLAY" : "● DEMO";
  pill.title = mode === "demo"
    ? "Бэкенд не отвечает: показаны синтетические данные. Запустите `python run.py`."
    : "Источник данных: " + mode;
}

/* ============================== ФИЛЬТРЫ ================================= */
function buildFiltersUI() {
  const groups = {};
  for (const s of state.meta.filters) (groups[s.group] ||= []).push(s);
  const host = $("#filters");
  host.innerHTML = "";
  let first = true;
  for (const [g, specs] of Object.entries(groups)) {
    const det = document.createElement("details");
    det.className = "fgroup";
    if (first) { det.open = true; first = false; }
    det.innerHTML = `<summary>${esc(g)}</summary><div class="fbody"></div>`;
    const body = $(".fbody", det);
    for (const s of specs) body.appendChild(s.kind === "flag" ? flagControl(s) : rangeControl(s));
    host.appendChild(det);
  }
}

function rangeControl(s) {
  const wrap = document.createElement("div");
  wrap.className = "fitem";
  wrap.innerHTML = `
    <div class="flabel"><span>${esc(s.label)}</span><b data-out></b></div>
    <div class="frange">
      <input type="number" step="${s.step || "any"}" placeholder="от" data-min>
      <span class="unit">–</span>
      <input type="number" step="${s.step || "any"}" placeholder="до" data-max>
      ${s.unit ? `<span class="unit">${esc(s.unit)}</span>` : ""}
    </div>
    ${s.hint ? `<div class="fhint">${esc(s.hint)}</div>` : ""}`;
  const min = $("[data-min]", wrap), max = $("[data-max]", wrap), out = $("[data-out]", wrap);
  const apply = () => {
    const cur = (state.filters[s.field] ||= {});
    cur.min = min.value === "" ? null : parseFloat(min.value);
    cur.max = max.value === "" ? null : parseFloat(max.value);
    if (cur.min == null && cur.max == null) delete state.filters[s.field];
    out.textContent = [cur.min != null ? "≥" + cur.min : "", cur.max != null ? "≤" + cur.max : ""].filter(Boolean).join(" ");
    state.activePreset = null; syncPresetUI(); pushFilters();
  };
  min.addEventListener("change", apply);
  max.addEventListener("change", apply);
  min.addEventListener("input", debounce(apply, 500));
  max.addEventListener("input", debounce(apply, 500));
  wrap._set = (v) => { min.value = v.min ?? ""; max.value = v.max ?? ""; apply(); };
  wrap.dataset.field = s.field;
  return wrap;
}

function flagControl(s) {
  const wrap = document.createElement("label");
  wrap.className = "fcheck";
  wrap.innerHTML = `<input type="checkbox"><span>${esc(s.label)}</span>`;
  if (s.hint) wrap.title = s.hint;
  const cb = $("input", wrap);
  cb.addEventListener("change", () => {
    if (cb.checked) state.filters[s.field] = true; else delete state.filters[s.field];
    wrap.classList.toggle("on", cb.checked);
    state.activePreset = null; syncPresetUI(); pushFilters();
  });
  wrap._set = (v) => { cb.checked = !!v; wrap.classList.toggle("on", !!v); };
  wrap.dataset.field = s.field;
  return wrap;
}

function buildPresets() {
  const host = $("#presets");
  host.innerHTML = "";
  for (const p of state.meta.presets || []) {
    const b = document.createElement("button");
    b.className = "preset";
    b.textContent = p.label;
    b.title = p.hint || "";
    b.dataset.id = p.id;
    b.addEventListener("click", () => applyPreset(p));
    host.appendChild(b);
  }
}

function applyPreset(p) {
  resetFilters(false);
  const params = p.params || {};
  for (const [k, v] of Object.entries(params)) {
    if (k === "sort") { state.sortCol = v; state.query.sort = v; continue; }
    if (k === "desc") { state.sortDesc = !!+v; state.query.desc = v ? "1" : "0"; continue; }
    const m = k.match(/^(.+)_(min|max)$/);
    if (m) { (state.filters[m[1]] ||= {})[m[2]] = v; continue; }
    state.filters[k] = v;
  }
  syncFilterControls();
  buildGridHead();
  state.activePreset = p.id; syncPresetUI();
  toast(`Пресет: ${p.label}`, p.hint || "");
  pushFilters();
}

function syncPresetUI() {
  $$(".preset").forEach((b) => b.classList.toggle("active", b.dataset.id === state.activePreset));
}

function syncFilterControls() {
  for (const el of $$("#filters [data-field]")) {
    const f = el.dataset.field;
    const v = state.filters[f];
    if (!el._set) continue;
    el._set(v == null ? (el.querySelector("input[type=checkbox]") ? false : {}) : v);
  }
}

function buildChips() {
  const exHost = $("#exchips");
  exHost.innerHTML = "";
  const exInfo = state.meta.exchange_info || {};
  for (const e of state.meta.exchanges || []) {
    const info = exInfo[e] || {};
    // биржа без выбранного рынка данных не даст — гасим её чипс, иначе
    // пользователь тычет в кнопку и получает пустую таблицу
    const usable = info.configured === false ? (info.markets || []).length > 0
      : (info.markets || [state.mt]).includes(state.mt);
    const c = document.createElement("button");
    c.className = "chip";
    c.dataset.ex = e;      // имя биржи без «◆»: по нему работает одиночный выбор
    c.disabled = !usable;
    c.style.opacity = usable ? "" : ".35";
    const isDex = !!info.dex;
    c.textContent = e + (isDex ? " ◆" : "");
    c.title = isDex ? "Децентрализованная биржа (perp DEX)"
      : (usable ? "" : `нет рынка «${state.mt === "spot" ? "спот" : "фьючерсы"}»`);
    c.addEventListener("click", () => {
      if (state.focus.enabled) {
        // фокус-режим: ОДНА биржа. Мультивыбор здесь бессмыслен — стрим
        // физически держится на одном коллекторе, а «выключить биржу»
        // означает остаться без данных вовсе.
        if (state.exchanges.has(e) && state.exchanges.size === 1) {
          toast("Фокус-режим", "Выберите другую биржу: в фокусе всегда ровно одна");
          return;
        }
        state.exchanges.clear();
        state.exchanges.add(e);
        // сетка графиков обязана смотреть на ту же биржу, иначе чипс
        // переключили, а ячейки остались от прошлой (проверялось смоком)
        state.grid.ex = e;
        buildGridControls();
      } else {
        state.exchanges.has(e) ? state.exchanges.delete(e) : state.exchanges.add(e);
      }
      syncChipsUI();
      pushFilters();
      if (state.focus.enabled) syncFocus();
      if (state.view === "grid") loadGrid();
    });
    exHost.appendChild(c);
  }
  // тумблер рынка: одна активная сторона, как в оригинале («Фьючерсы | Спот»).
  // Применяется ко всему: скринер, карта, плотности и сетка графиков.
  $$("#mtseg .seg-btn").forEach((b) => {
    b.classList.toggle("on", b.dataset.mt === state.mt);
    b.addEventListener("click", () => {
      if (state.mt === b.dataset.mt) return;
      state.mt = b.dataset.mt;
      $$("#mtseg .seg-btn").forEach((x) => x.classList.toggle("on", x.dataset.mt === state.mt));
      state.grid.ex = "";           // рынок сменился — прежняя биржа могла не иметь нового
      buildChips();                 // у бирж разный набор рынков
      buildGridControls();
      pushFilters();
      if (state.focus.enabled) syncFocus();
      if (state.view === "grid") loadGrid();
    });
  });
}

/* ============================== ФОКУС-РЕЖИМ =============================
   Одна биржа + один рынок + top-N по текущему фильтру.

   Зачем: полный профиль стримит ~10 500 инструментов на 12 коллекторах, и
   графики заказывают свечи у биржи для монет, которых в горячем наборе нет.
   В фокусе подписок ровно N (по умолчанию 50), отбор перепроверяется раз в
   15 секунд, а /api/grid берёт ячейки из этого же отбора — свечи приходят
   для тех монет, которые реально стримятся.                                     */
function focusEx() {
  const first = state.exchanges.size ? Array.from(state.exchanges)[0] : "";
  return first || state.grid.ex || "";
}

function ensureGridEx(labels) {
  // в фокусе «Все биржи» недоступно: подставляем выбранную или первую подходящую
  const ok = labels || [];
  if (!state.grid.ex || !ok.includes(state.grid.ex)) {
    state.grid.ex = focusEx() || ok[0] || "";
  }
}

/** Одиночный выбор биржи в чипсах (фокус) против мультивыбора (полный режим). */
function syncChipsUI() {
  $$(".chip").forEach((c) => {
    const name = (c.dataset.ex || "").trim();
    c.classList.toggle("on", !!name && state.exchanges.has(name));
  });
}

function buildFocusPanel() {
  const on = $("#focus-on");
  if (!on) return;
  const f = state.focus;
  on.checked = f.enabled;
  $("#focus-limit").value = f.limit;
  $("#focus-interval").value = f.interval;
  $("#focus-pause").checked = f.pauseOthers;
  $("#focus-uniq").checked = f.dedupeBase;
  // биржи чипсов должны знать своё имя: по нему восстанавливаем выбор
  $$(".chip").forEach((c, i) => {
    if (!c.dataset.ex) c.dataset.ex = (c.textContent || "").replace("◆", "").trim();
    if (!c.dataset.ex) c.dataset.ex = (state.meta.exchanges || [])[i] || "";
  });
  on.addEventListener("change", () => toggleFocusMode(on.checked));
  $("#focus-limit").addEventListener("change", (e) => {
    f.limit = Math.max(5, Math.min(200, parseInt(e.target.value, 10) || 50));
    e.target.value = f.limit;
    syncFocus();
  });
  $("#focus-interval").addEventListener("change", (e) => {
    f.interval = Math.max(5, Math.min(300, parseInt(e.target.value, 10) || 15));
    e.target.value = f.interval;
    syncFocus();
  });
  $("#focus-pause").addEventListener("change", (e) => {
    f.pauseOthers = !!e.target.checked;
    syncFocus();
  });
  $("#focus-uniq").addEventListener("change", (e) => {
    // без этого в топ-50 Binance Futures попадают BTC/USDT и BTC/USDC разом:
    // slots тратятся на одну и ту же монету, а сетка показывает её дважды
    f.dedupeBase = !!e.target.checked;
    syncFocus();
  });
  renderFocus();
}

async function toggleFocusMode(on) {
  const f = state.focus;
  f.enabled = !!on;
  $("#focus-body").hidden = !f.enabled;
  $("#kv-focus").hidden = !f.enabled;
  if (f.enabled) {
    // одна биржа: если выбрано несколько — оставляем первую
    if (state.exchanges.size > 1) {
      state.exchanges = new Set([Array.from(state.exchanges)[0]]);
    }
    const labels = (state.meta.exchanges || []).filter((e) => {
      const info = (state.meta.exchange_info || {})[e] || {};
      return info.configured === false ? true : (info.markets || [state.mt]).includes(state.mt);
    });
    ensureGridEx(labels);
    if (!state.exchanges.size && state.grid.ex) state.exchanges.add(state.grid.ex);
    toast("Фокус-режим",
      `Стримим top-${f.limit} · ${state.grid.ex || "биржа"} · ${state.mt === "spot" ? "спот" : "фьючерсы"} · перепроверка раз в ${f.interval} с`);
  } else {
    toast("Полный режим", "Все биржи и горячий набор восстановлены");
  }
  syncChipsUI();
  buildGridControls();
  await syncFocus();
  await loadBaseRows();     // база нужна до pushFilters: тот шлёт флаг base
  pushFilters();
  if (!f.server) {
    // DEMO: серверного отбора нет — применяем top-N к текущим строкам сразу,
    // не дожидаясь следующего демо-тика
    applyLocalFocus();
    if (state.view === "screener") renderTable();
    if (state.view === "map") renderMap();
  }
  renderFocus();
  if (state.view === "grid") loadGrid();
}

/** Отправить параметры фокуса на бэкенд (отбор пересчитывается сразу). */
async function syncFocus() {
  const f = state.focus;
  if (!f.enabled) {
    // ВЫКЛЮЧЕНИЕ обязательно надо донести до сервера: иначе он продолжит
    // держать отбор и сужать WS-пуш до top-N, хотя тумблер уже выключен
    // (таблица схлопывалась до N строк — ровно тот баг, что поймал смок).
    if (f.serverActive && !state.demo) {
      try {
        const r = await fetchT("/api/focus", 10000, { method: "DELETE" });
        f.serverActive = r.ok;
        if (r.ok) { f.count = 0; f.keys = []; f.note = ""; f.error = ""; f.provisional = false; }
      } catch (e) { /* сервер недоступен — попробуем при следующем вызове */ }
    }
    state.baseRows = []; state.baseIndex = new Map(); state.baseSig = "";
    renderFocus();
    return;
  }
  if (state.demo) { f.server = false; renderFocus(); return; }
  const body = {
    ex: focusEx(), mt: state.mt, limit: f.limit, interval: f.interval,
    pause_others: f.pauseOthers, dedupe_base: f.dedupeBase,
    sort: state.sortCol, desc: state.sortDesc ? "1" : "0",
    query: collectQuery(),
  };
  try {
    const r = await fetchT("/api/focus", 12000, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const d = await r.json().catch(() => ({}));
    f.server = r.ok;
    if (!r.ok) { f.error = (d && d.error) || ("HTTP " + r.status); renderFocus(); return; }
    f.error = "";
    f.serverActive = true;
    // сервер мог выбрать биржу сам (если чипс ещё не нажат) — синхронизируем UI
    if (d.ex && d.ex !== focusEx()) {
      state.exchanges = new Set([d.ex]);
      state.grid.ex = d.ex;
      syncChipsUI();
      buildGridControls();
    }
    f.limit = d.limit || f.limit;
    f.interval = d.interval || f.interval;
    f.virtual = !!d.virtual;
    f.provisional = !!d.provisional;
    f.ex = d.ex || f.ex;
    f.keys = d.keys || [];
    f.note = d.note || "";
    f.lastMs = d.last_ms || 0;
    f.count = d.count || 0;
    f.nextIn = d.next_in;
  } catch (e) {
    f.server = false;
    f.error = "бэкенд не ответил: " + (e && e.message ? e.message : e);
  }
  await loadBaseRows();
  renderFocus();
}

const syncFocusSoon = debounce(() => { syncFocus(); }, 700);

/** Периодически забираем свежий отбор (сервер пересчитывает его сам). */
async function loadFocus() {
  if (state.demo) { renderFocus(); return; }
  try {
    const r = await fetchT("/api/focus", 8000);
    if (!r.ok) return;
    const d = await r.json();
    const f = state.focus;
    f.server = true;
    f.serverActive = !!d.enabled;
    f.virtual = !!d.virtual;
    f.provisional = !!d.provisional;
    f.ex = d.ex || f.ex;
    f.limit = d.limit || f.limit;
    f.interval = d.interval || f.interval;
    f.pauseOthers = d.pause_others !== false;
    f.dedupeBase = d.dedupe_by_base !== false;
    f.count = d.count || 0;
    f.note = d.note || "";
    f.error = d.error || "";
    f.keys = d.keys || [];
    f.nextIn = d.next_in;
    if (d.enabled && !f.enabled) {
      // сервер уже в фокусе (например, страницу перезагрузили) — включаем UI
      f.enabled = true;
      if (d.ex) { state.exchanges = new Set([d.ex]); state.grid.ex = d.ex; }
      const on = $("#focus-on"); if (on) on.checked = true;
      const fb = $("#focus-body"); if (fb) fb.hidden = false;
      const kf = $("#kv-focus"); if (kf) kf.hidden = false;
      syncChipsUI();
      buildGridControls();
      await loadBaseRows();
      pushFilters();
    }
    renderFocus();
  } catch (e) { /* нет бэкенда — останемся в DEMO-поведении */ }
}

/**
 * Клиентский вариант фокуса: тот же результат, но без бэкенда.
 * Нужен для DEMO и как страховка, если /api/grid не ответил.
 */
function applyLocalFocus(d) {
  const f = state.focus;
  const sortKey = state.sortCol;
  const keyOf = (x) => x.k;
  const cmp = (a, b) => {
    const av = a[sortKey], bv = b[sortKey];
    const an = av == null, bn = bv == null;
    if (an && bn) return 0;
    if (an) return 1;
    if (bn) return -1;
    return state.sortDesc ? bv - av : av - bv;
  };
  if (d && Array.isArray(d.cells)) {
    d.cells = d.cells.slice().sort(cmp).slice(0, Math.min(d.cells.length, f.limit));
    // статус «в стриме N монет» обязан жить и в DEMO, иначе панель показывает 0
    f.count = d.cells.length;
    f.keys = d.cells.map(keyOf);
    return d;
  }
  state.rows = state.rows.slice().sort(cmp).slice(0, f.limit);
  state.rowIndex = new Map(state.rows.map((x) => [x.k, x]));
  f.count = state.rows.length;
  f.keys = state.rows.map(keyOf);
  return d;
}

function renderFocus(extra) {
  const f = state.focus;
  if (extra && typeof extra === "object") {
    if (typeof extra.count === "number") f.count = extra.count;
    if (extra.note != null) f.note = extra.note;
    if (typeof extra.next_in === "number") f.nextIn = extra.next_in;
    if (typeof extra.limit === "number") f.limit = extra.limit;
  }
  const st = $("#focus-status");
  if (st) {
    let txt;
    if (!f.enabled) txt = "";
    else if (f.error) txt = "⚠ " + f.error;
    else {
      // «в стриме N монет» — всегда: это главная цифра режима. Уточнение
      // сзади объясняет, кто именно пересчитал отбор (сервер или клиент).
      const next = f.nextIn != null ? Math.max(0, Math.round(f.nextIn)) : null;
      txt = `в стриме ${f.count || 0} монет`;
      if (!f.server) txt += " · локальный отбор (DEMO)";
      else if (f.virtual) txt += " · виртуальный (потоки не перестраиваются)";
      else if (f.provisional) txt += " · предварительный отбор — свечи уточняют состав";
      else if (next != null) txt += ` · перепроверка через ${next} с`;
      if (f.server && f.lastMs) txt += ` · отбор ${f.lastMs} мс`;
    }
    if (f.note) txt += ` · ${f.note}`;
    st.textContent = txt;
    st.classList.toggle("warn", !!f.error || !!f.note);
  }
  const badge = $("#focus-badge");
  if (badge) { badge.hidden = !f.enabled; badge.textContent = f.count || f.limit; }
  const kv = $("#st-focus");
  if (kv) {
    kv.textContent = f.enabled
      ? `${f.count || 0}/${f.limit}${f.server && !f.virtual ? "" : " (локально)"}`
      : "—";
  }
  const hint = $("#focus-hint");
  if (hint) {
    hint.textContent = f.enabled
      ? `Стримятся только top-${f.limit} выбранной биржи; отбор по текущему фильтру пересчитывается раз в ${f.interval} с.`
      : "Включите, чтобы стримить только top-N монет одной биржи — графики грузятся заметно быстрее.";
  }
}

/**
 * Полная вселенная выбранной биржи (REST) — основа таблицы и карты в фокусе.
 *
 * Зачем: WS-пуш в фокус-режиме намеренно узкий (только top-N стримов), иначе
 * сервер продолжал бы сериализовать тысячи строк раз в секунду и вся экономия
 * пропадает. Но «сжать пуш» не значит «сжато всё UI»: тикеры биржи мы и так
 * получаем одним REST-запросом, поэтому таблица показывает всю биржу.
 */
async function loadBaseRows() {
  if (!state.focus.enabled || !state.focus.server) {
    state.baseRows = []; state.baseIndex = new Map(); return;
  }
  const q = collectQuery();
  q.limit = "5000";
  try {
    const r = await fetchT("/api/screener?" + new URLSearchParams(q).toString(), 12000);
    if (!r.ok) return;
    const d = await r.json();
    state.baseRows = d.rows || [];
    state.baseIndex = new Map(state.baseRows.map((x) => [x.k, x]));
    state.baseSig = "";          // форсируем пересборку merged
  } catch (e) { /* останемся на WS-строках */ }
}

/** WS-строки (живые тики) поверх REST-базы. Порядок не трогаем — иначе
 *  инкрементальный рендер таблицы пересоздавал бы все узлы каждую секунду. */
function mergeBase(live) {
  if (!state.baseRows.length) return live;
  const sig = state.baseRows.length + ":" + live.length + ":" +
    (live.length ? live[live.length - 1].u : 0);
  if (sig === state.baseSig) return state.baseMerged;
  const byKey = new Map(live.map((x) => [x.k, x]));
  const out = state.baseRows.map((x) => byKey.get(x.k) || x);
  for (const x of live) if (!state.baseIndex.has(x.k)) out.push(x);
  state.baseSig = sig;
  state.baseMerged = out;
  return out;
}

/** Что реально показывать в таблице/карте. */
function displayedRows() {
  if (!state.focus.enabled) return state.rows;
  if (!state.focus.server) return state.rows;    // DEMO: отбор уже клиентский
  return mergeBase(state.rows);
}

/** Фильтр/сортировка изменились: перечитать базу и оповестить сервер. */
function refilter() {
  loadBaseRows();
  pushFilters();
}

function resetFilters(push = true) {
  state.filters = {};
  // пресеты умеют менять сортировку («Максимальная волатильность» ставит
  // sort=natr), поэтому «сброс» обязан возвращать и её. Иначе кнопка
  // «сброс» сбрасывала фильтры, но таблица продолжала стоять в порядке
  // пресета — пользователь не понимал, почему объём не убывает.
  state.sortCol = "vol";
  state.sortDesc = true;
  state.exchanges.clear();
  $$(".chip").forEach((c) => c.classList.remove("on"));
  syncFilterControls();
  buildGridHead();          // снять стрелку сортировки с колонки пресета
  state.activePreset = null; syncPresetUI();
  $("#search").value = "";
  delete state.query.q;
  if (push) refilter();
}

/* собираем query-параметры из состояния UI */
function collectQuery() {
  const q = { sort: state.sortCol, desc: state.sortDesc ? "1" : "0", limit: $("#row-limit").value };
  for (const [f, v] of Object.entries(state.filters)) {
    if (v === true) q[f] = "1";
    else if (v && typeof v === "object") {
      if (v.min != null) q[f + "_min"] = v.min;
      if (v.max != null) q[f + "_max"] = v.max;
    }
  }
  if (state.exchanges.size) q.ex = Array.from(state.exchanges).join(",");
  if (state.mt) q.mt = state.mt;
  const s = $("#search").value.trim();
  if (s) q.q = s;
  if ($("#opt-dens") && $("#opt-dens").checked) q.dens = "1";
  return q;
}

function pushFilters() {
  state.query = collectQuery();
  // фильтр изменился → отбор фокус-режима надо пересчитать. Дебаунс:
  // иначе каждое нажатие клавиши в диапазоне фильтра дёргало бы биржу.
  if (state.focus.enabled) syncFocusSoon();
  if (state.wsOk && state.ws && state.ws.readyState === 1) {
    state.ws.send(JSON.stringify({
      type: "filters", query: state.query, densities: !!state.query.dens,
      // base=1: «таблицу кормлю сам из REST, пуш нужен только для тиков» —
      // сервер не сужает выборку до фокус-набора
      base: !!(state.focus.enabled && state.focus.server),
    }));
  }
  if (state.demo) demoApplyFilters();
  if (state.view === "densities") loadDensities();
  if (state.view === "map") renderMap();
  if (state.view === "screener") renderTable();
}

function debounce(fn, ms) {
  let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); };
}

/* ============================== WEBSOCKET =============================== */
function connect() {
  if (state.demo) return;
  const proto = location.protocol === "https:" ? "wss" : "ws";
  let ws;
  try { ws = new WebSocket(`${proto}://${location.host}/ws`); } catch (e) { enterDemo(); return; }
  state.ws = ws;
  const bar = $("#conn-bar");

  ws.onopen = () => {
    state.wsOk = true; bar.hidden = true; $("#st-ws").textContent = "online";
    ws.send(JSON.stringify({ type: "filters", query: collectQuery(), densities: false,
                             base: !!(state.focus.enabled && state.focus.server) }));
    if (state.view === "alerts") loadAlerts();
  };
  ws.onmessage = (ev) => {
    let m; try { m = JSON.parse(ev.data); } catch { return; }
    if (m.type === "rows") onRows(m);
    else if (m.type === "alert") onAlert(m.alert);
  };
  ws.onclose = () => {
    state.wsOk = false; $("#st-ws").textContent = "offline";
    bar.hidden = false; bar.className = "conn-bar";
    bar.textContent = "Соединение потеряно — переподключение через 3 сек…";
    setTimeout(() => { if (!state.wsOk && !state.demo) connect(); }, 3000);
  };
  ws.onerror = () => { /* onclose отработает следом */ };
}

function enterDemo() {
  if (state.demo) return;
  setMode("demo");
  state.demo = true;
  startDemo();
}

function onRows(m) {
  state.rows = m.rows || [];
  // фокус без бэкенда (DEMO-путь): сервер не может сузить пуш, режем сами
  if (state.focus.enabled && !state.focus.server) applyLocalFocus();
  state.rowIndex = new Map(state.rows.map((r) => [r.k, r]));
  state.overview = m.overview;
  renderOverview();
  if (state.view === "map") renderMap();
  else if (state.view === "screener") renderTable();
  if (state.chart && state.chartKey) {
    const cur = state.rowIndex.get(state.chartKey);
    if (cur) chartTick(cur);
  }
  if (state.view === "grid") {
    for (const r of state.rows) gridTick(r);
  }
  $("#st-updated").textContent = new Date().toLocaleTimeString("ru-RU");
  $("#st-total").textContent = displayedRows().length || (m.meta && m.meta.total) || 0;
  if (state.focus.enabled && m.meta && m.meta.focus_count != null) {
    state.focus.count = m.meta.focus_count;
    renderFocus();
  }
}

function renderOverview() {
  const o = state.overview; if (!o) return;
  $("#st-symbols").textContent = fmt.compact(o.symbols);
  $("#st-up").textContent = o.up;
  $("#st-down").textContent = o.down;
  $("#st-vol").textContent = "$" + fmt.compact(o.volume_usd);
}

/* ============================== КАРТА РЫНКА ============================= */
function renderMap() {
  const host = $("#map");
  const rows = displayedRows().filter((r) => r.vol > 0);
  if (!rows.length) {
    host.innerHTML = `<div class="empty">Нет данных, подходящих под фильтры</div>`;
    state.mapEls.clear();
    return;
  }
  // maxVol через цикл: spread по массиву из тысяч строк — лишний массив
  // плюс риск переполнения стека вызова
  let maxVol = 0;
  for (const r of rows) { const v = r.vol || 0; if (v > maxVol) maxVol = v; }
  maxVol = maxVol || 1;

  // переиспользуем DOM-узлы по ключу из Map: host.querySelector на каждую
  // плитку — это 300 полных обходов дерева в секунду, плюс 5 querySelector
  // внутри каждой плитки на каждый текст. Держим прямые ссылки на детей
  // и кэш последних значений: в DOM пишем только то, что реально изменилось.
  const seen = new Set();
  for (const r of rows.slice(0, 300)) {
    seen.add(r.k);
    let rec = state.mapEls.get(r.k);
    if (!rec || rec.el.parentNode !== host) {
      const el = document.createElement("div");
      el.className = "tile";
      el.dataset.k = r.k;
      el.innerHTML = `<div class="t-sym"></div><div class="t-chg"></div><div class="t-vol"></div>
                      <div class="t-ex"></div><div class="spike"></div>`;
      const key = r.k;
      el.addEventListener("click", () => openDrawer(key));
      host.appendChild(el);
      rec = { el, sym: el.children[0], chg: el.children[1], vol: el.children[2],
              ex: el.children[3], spike: el.children[4], c: {} };
      state.mapEls.set(r.k, rec);
    }
    const el = rec.el, c = rec.c;
    const frac = Math.sqrt((r.vol || 0) / maxVol);
    const w = Math.round(76 + 150 * frac);
    const h = Math.round(46 + 44 * frac);
    if (c.w !== w) { el.style.flexBasis = w + "px"; el.style.width = w + "px"; c.w = w; }
    if (c.h !== h) { el.style.height = h + "px"; c.h = h; }
    const bg = chgColor(r.chg);
    if (c.bg !== bg) { el.style.background = bg; c.bg = bg; }
    const big = w > 170;
    if (c.big !== big) { el.classList.toggle("big", big); c.big = big; }
    const sym = r.b || r.s;
    if (c.sym !== sym) { rec.sym.textContent = sym; c.sym = sym; }
    const chg = fmt.pct(r.chg, 1);
    if (c.chg !== chg) { rec.chg.textContent = chg; c.chg = chg; }
    const vol = "$" + fmt.compact(r.vol);
    if (c.vol !== vol) { rec.vol.textContent = vol; c.vol = vol; }
    const ex = (r.exl || "").replace(" Futures", " F").replace(" Spot", "") + (r.dex ? " ◆" : "");
    if (c.ex !== ex) { rec.ex.textContent = ex; c.ex = ex; }
    const sp = r.spike ? "⚡" : "";
    if (c.sp !== sp) { rec.spike.textContent = sp; c.sp = sp; }
    const title = `${r.s} · ${r.exl}${r.dex ? " [DEX]" : ""} (${r.mt})\nцена ${fmt.price(r.last)} · 24ч ${fmt.pct(r.chg)}\nобъём ${fmt.compact(r.vol)} · NATR ${fmt.num(r.natr, 2)}`;
    if (c.title !== title) { el.title = title; c.title = title; }
  }
  for (const [k, rec] of Array.from(state.mapEls)) {
    if (!seen.has(k)) { rec.el.remove(); state.mapEls.delete(k); }
  }
}

function cssEsc(s) { return String(s).replace(/["\\]/g, "\\$&"); }

/* ============================== ТАБЛИЦА ================================= */
function buildGridHead() {
  const tr = $("#grid-head");
  tr.innerHTML = "";
  for (const c of COLUMNS) {
    const th = document.createElement("th");
    th.textContent = c.label;
    if (c.r) th.className = "r";
    if (state.sortCol === c.k) {
      th.classList.add("sorted");
      th.innerHTML = esc(c.label) + `<span class="arr">${state.sortDesc ? "▼" : "▲"}</span>`;
    }
    th.addEventListener("click", () => {
      if (state.sortCol === c.k) state.sortDesc = !state.sortDesc;
      else { state.sortCol = c.k; state.sortDesc = true; }
      buildGridHead(); refilter();
    });
    tr.appendChild(th);
  }
}

function renderTable() {
  const body = $("#grid-body");
  const flash = $("#opt-flash").checked;
  const rows = displayedRows().slice(0, parseInt($("#row-limit").value, 10) || 150);
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="${COLUMNS.length}" class="empty">Нет данных под фильтры</td></tr>`;
    state._rowEls.clear(); state._cellCache.clear();
    return;
  }

  const seen = new Set();
  let node = body.firstElementChild;
  for (const r of rows) {
    seen.add(r.k);
    let tr = node;
    let fresh = false;
    if (!tr || tr.dataset.k !== r.k) {
      // переиспользуем строку по ключу из Map: querySelector по телу таблицы
      // на каждую переставленную строку — это O(n²) обходов DOM в секунду
      const existing = state._rowEls.get(r.k);
      if (existing && existing.parentNode === body) { body.insertBefore(existing, node); tr = existing; }
      else {
        tr = document.createElement("tr");
        tr.dataset.k = r.k;
        tr.innerHTML = COLUMNS.map((c) => `<td${c.r ? ' class="r"' : ""}></td>`).join("");
        tr.addEventListener("click", () => openDrawer(tr.dataset.k));
        body.insertBefore(tr, node);
        state._rowEls.set(r.k, tr);
        fresh = true;
      }
    }
    node = tr.nextElementSibling;

    // Diff ячеек через JS-кэш. Сравнение `td.innerHTML !== html` заставляет
    // браузер СЕРИАЛИЗОВАТЬ DOM каждой ячейки обратно в строку — ~2850
    // сериализаций в секунду на 150 строках. Кэш строк в Map отдаёт то же
    // сравнение за наносекунды, а в DOM пишутся только изменённые ячейки.
    let cc = state._cellCache.get(r.k);
    if (!cc || fresh) {
      cc = { html: new Array(COLUMNS.length).fill(null), cls: new Array(COLUMNS.length).fill(null) };
      state._cellCache.set(r.k, cc);
    }

    for (let i = 0; i < COLUMNS.length; i++) {
      const c = COLUMNS[i];
      const td = tr.children[i];
      const html = (c.k === "s")
        ? `${esc(r.b || r.s)}<small>${esc((r.q || "").replace(/^:/, ""))}</small>`
        : cellValue(c, r);
      if (cc.html[i] !== html) { td.innerHTML = html; cc.html[i] = html; }

      let cls = c.r ? "r" : "";
      if (c.cls) cls += (cls ? " " : "") + c.cls;
      if (c.color) {
        const v = r[c.k];
        if (v != null && !isNaN(v)) cls += (cls ? " " : "") + (v > 0 ? "pos" : v < 0 ? "neg" : "mut");
      }
      // className пишем только при изменении; flash-классы живут поверх и
      // сбрасываются сами (анимация одноразовая, .7s)
      if (cc.cls[i] !== cls || td.className.indexOf("flash-") >= 0) { td.className = cls; cc.cls[i] = cls; }

      if (c.flash && flash) {
        const prev = state.prevPrices.get(r.k);
        if (prev != null && r[c.k] != null && prev !== r[c.k]) {
          // Перезапуск анимации ЧЕРЕДОВАНИЕМ классов (flash-up ↔ flash-up-2,
          // keyframes идентичны): разное animation-name перезапускает эффект без
          // чтения layout. Прежний `void td.offsetWidth` форсил синхронный
          // reflow всей таблицы на каждую мигнувшую цену — до 150
          // принудительных пересчётов layout в секунду на живом потоке.
          cc.fp = ((cc.fp || 0) + 1) % 2;
          const base = r[c.k] > prev ? "flash-up" : "flash-dn";
          td.classList.remove("flash-up", "flash-dn", "flash-up-2", "flash-dn-2");
          td.classList.add(cc.fp ? base + "-2" : base);
        }
      }
    }
    state.prevPrices.set(r.k, r.last);
  }
  while (node) { const nx = node.nextElementSibling; node.remove(); node = nx; }
  // кэши удалённых строк не держим
  for (const k of Array.from(state._rowEls.keys())) if (!seen.has(k)) state._rowEls.delete(k);
  if (state._cellCache.size > seen.size) {
    for (const k of Array.from(state._cellCache.keys())) if (!seen.has(k)) state._cellCache.delete(k);
  }
  // prevPrices рос вместе с ротацией символов (Map никогда не чистился):
  // за сутки на 6000+ монет это десятки тысяч мёртвых записей
  if (state.prevPrices.size > seen.size) {
    for (const k of Array.from(state.prevPrices.keys())) if (!seen.has(k)) state.prevPrices.delete(k);
  }
}

/* ============================== ПЛОТНОСТИ =============================== */
async function loadDensities() {
  if (state.demo) { renderDensities(demoDensities()); return; }
  const min = $("#dens-min").value || 50000;
  const side = $("#dens-side").value;
  const ex = Array.from(state.exchanges)[0] || "";
  try {
    const r = await fetchT(`/api/densities?min_usd=${min}&side=${side}&ex=${encodeURIComponent(ex)}&limit=300`, 12000);
    const d = await r.json();
    state.densRows = d.rows || [];
  } catch (e) { state.densRows = []; }
  renderDensities(state.densRows);
}

function renderDensities(rows) {
  const body = $("#dens-body");
  if (!rows.length) { body.innerHTML = `<tr><td colspan="8" class="empty">Крупных плотностей не найдено — понизьте порог или дождитесь загрузки стакана</td></tr>`; return; }
  const max = Math.max(...rows.map((r) => r.q)) || 1;
  body.innerHTML = rows.map((r) => `
    <tr data-k="${esc(r.k)}">
      <td class="sym">${esc((r.s || "").split("/")[0])}</td>
      <td class="ex">${esc(r.exl)}</td>
      <td class="r">${fmt.price(r.p)}</td>
      <td class="r ${r.d > 0 ? "neg" : "pos"}">${fmt.pct(r.d, 2)}</td>
      <td class="r">
        <div style="display:flex;align-items:center;gap:6px;justify-content:flex-end">
          <span>$${fmt.compact(r.q)}</span>
          <div class="bar ${r.sd}" style="width:${Math.max(4, (r.q / max) * 60)}px"></div>
        </div>
      </td>
      <td class="r">${r.n}</td>
      <td class="${r.sd === "bid" ? "pos" : "neg"}">${r.sd === "bid" ? "BID (поддержка)" : "ASK (сопротивление)"}</td>
      <td class="r ${r.chg > 0 ? "pos" : "neg"}">${fmt.pct(r.chg, 2)}</td>
    </tr>`).join("");
  $$("#dens-body tr").forEach((tr) => tr.addEventListener("click", () => openDrawer(tr.dataset.k)));
}

/* ============================== АЛЕРТЫ ================================== */
function buildAlertForm() {
  const sel = $("#af-field");
  sel.innerHTML = "";
  const ranges = (state.meta.filters || []).filter((f) => f.kind === "range");
  for (const f of ranges) {
    const o = document.createElement("option");
    o.value = f.field; o.textContent = `${f.label}${f.unit ? ", " + f.unit : ""}`;
    sel.appendChild(o);
  }
  const exSel = $("#af-ex");
  const exi = state.meta.exchange_info || {};
  for (const e of state.meta.exchanges || []) {
    const o = document.createElement("option");
    o.value = e;
    o.textContent = e + (exi[e] && exi[e].dex ? " (DEX)" : "");
    exSel.appendChild(o);
  }
}

async function loadAlerts() {
  if (state.demo) { renderAlerts(); return; }
  try {
    const r = await fetch("/api/alerts"); const d = await r.json();
    state.alerts.rules = d.rules || []; state.alerts.events = d.events || [];
  } catch (e) { /* бэкенд недоступен */ }
  renderAlerts();
}

function renderAlerts() {
  const rl = $("#rules-list");
  $("#rules-count").textContent = state.alerts.rules.length ? `(${state.alerts.rules.length})` : "";
  rl.innerHTML = state.alerts.rules.length ? state.alerts.rules.map((r) => `
    <div class="rule">
      <div class="r-txt"><b>${esc(r.describe)}</b>
        <div class="r-meta">режим: ${r.mode === "crossing" ? "на пересечении" : "пока истинно"} · cooldown ${r.cooldown}с · сработал ${r.hits}× ${r.notify_telegram ? "· TG" : ""}</div>
      </div>
      <button class="icon-btn tgl ${r.enabled ? "on" : ""}" data-act="toggle" data-id="${r.id}">${r.enabled ? "вкл" : "выкл"}</button>
      <button class="icon-btn" data-act="del" data-id="${r.id}">✕</button>
    </div>`).join("") : `<div class="empty">Правил нет. Создайте первое — например «Изменение 24ч &gt; 8%» на Bybit.</div>`;

  const el = $("#events-list");
  $("#events-count").textContent = state.alerts.events.length ? `(${state.alerts.events.length})` : "";
  el.innerHTML = state.alerts.events.length ? state.alerts.events.map((e) => `
    <div class="event">
      <div class="e-txt">${esc(e.text)}</div>
      <div class="e-meta">${new Date(e.ts * 1000).toLocaleTimeString("ru-RU")}</div>
    </div>`).join("") : `<div class="empty">Событий пока нет.</div>`;

  $$("#rules-list [data-act]").forEach((b) => b.addEventListener("click", async () => {
    const id = b.dataset.id;
    if (b.dataset.act === "del") await fetch(`/api/alerts/${id}`, { method: "DELETE" });
    else {
      const rule = state.alerts.rules.find((r) => r.id === id);
      await fetch(`/api/alerts/${id}`, { method: "PATCH", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled: !rule.enabled }) });
    }
    loadAlerts();
  }));
}

function onAlert(a) {
  state.alerts.events.unshift(a);
  state.alerts.events = state.alerts.events.slice(0, 100);
  const rule = state.alerts.rules.find((r) => r.id === a.rule_id);
  if (rule) rule.hits = (rule.hits || 0) + 1;
  const b = $("#alert-badge");
  b.hidden = false; b.textContent = state.alerts.events.length;
  toast(a.text, "нажмите, чтобы открыть инструмент", () => openDrawer(a.key));
  if (state.view === "alerts") renderAlerts();
}

/* ============================== DRAWER ================================== */
async function openDrawer(key) {
  const d = $("#drawer"); d.hidden = false;
  $("#d-title").textContent = key;
  $("#drawer-body").innerHTML = `<div class="empty">загрузка…</div>`;
  let data = null;
  if (!state.demo) {
    try { const r = await fetchT(`/api/symbol?key=${encodeURIComponent(key)}`, 10000); if (r.ok) data = await r.json(); }
    catch (e) { data = null; }
  }
  if (!data) data = state.rowIndex.get(key) || demoSymbol(key);
  renderDrawer(data, key);
}

function renderDrawer(r, key) {
  $("#d-title").innerHTML = `${esc(r.b || r.s || key)} <span class="mut" style="font-size:12px">${esc(r.q || "")}</span>`;
  $("#d-sub").textContent = `${r.exl || r.ex || ""}${r.dex ? " · DEX" : ""} · ${r.mt || ""} · ${r.s || key}`;

  const stat = (label, val, cls = "") => `<div class="d-stat"><label>${label}</label><b class="${cls}">${val}</b></div>`;
  const pctCls = (v) => (v > 0 ? "pos" : v < 0 ? "neg" : "mut");

  const bookHtml = (r.book && (r.book.bids?.length || r.book.asks?.length)) ? `
    <div class="d-sec"><h4>Стакан (топ-12)</h4><div class="ladder">
      <table>${(r.book.bids || []).slice(0, 12).map(([p, a]) =>
        `<tr><td class="pos">${fmt.price(p)}</td><td class="sz mut">${fmt.compact(p * a)}</td></tr>`).join("")}</table>
      <table>${(r.book.asks || []).slice(0, 12).map(([p, a]) =>
        `<tr><td class="neg">${fmt.price(p)}</td><td class="sz mut">${fmt.compact(p * a)}</td></tr>`).join("")}</table>
    </div></div>` : "";

  const densHtml = (r.densities && r.densities.length) ? `
    <div class="d-sec"><h4>Плотности в стакане</h4>
      ${r.densities.slice(0, 12).map((dn) => `
        <div class="dens-row">
          <span class="${dn.sd === "bid" ? "pos" : "neg"}">${dn.sd === "bid" ? "BID" : "ASK"} ${fmt.price(dn.p)}</span>
          <span class="mut">${fmt.pct(dn.d, 2)} от цены</span>
          <b>$${fmt.compact(dn.q)}</b>
        </div>`).join("")}
    </div>` : "";

  const spikeHtml = (r.spikes && r.spikes.length) ? `
    <div class="d-sec"><h4>Спайки</h4>
      ${r.spikes.slice(0, 8).map((s) => `
        <div class="dens-row"><span class="spk">⚡ ${s.kind}</span>
        <span class="mut">${fmt.compact(s.value)} vs база ${fmt.compact(s.base)}</span>
        <b>${s.ratio}x · ${s.age}с назад</b></div>`).join("")}
    </div>` : "";

  $("#drawer-body").innerHTML = `
    <div class="d-sec"><h4>Цена</h4>
      <div class="d-stats">
        ${stat("last", fmt.price(r.last))}
        ${stat("24ч", fmt.pct(r.chg), pctCls(r.chg))}
        ${stat("диапазон", fmt.pct(r.rng))}
      </div>
    </div>
    <div class="d-sec">
      <h4>График</h4>
      <div class="tf-btns">${TFS.map((t) =>
        `<button class="tf-btn${t === state.chartTf ? " on" : ""}" data-tf="${t}">${t}</button>`).join("")}</div>
      <div class="chart-box">
        <canvas id="d-chart"></canvas>
        <div class="chart-state" id="d-chart-state">загрузка свечей…</div>
      </div>
      <div class="chart-legend">
        <span><i style="background:#16c784"></i>рост</span>
        <span><i style="background:#ea3943"></i>падение</span>
        <span><i style="background:rgba(22,199,132,.75)"></i>плотность bid</span>
        <span><i style="background:rgba(234,57,67,.75)"></i>плотность ask</span>
        <span><i style="background:#f0b90b"></i>⚡ спайк</span>
        <span><i style="background:#2563eb"></i>последняя цена</span>
      </div>
      <div class="chart-hint">колесо — зум · drag — панорама · двойной клик — сброс · наведение — OHLCV</div>
    </div>
    <div class="d-sec"><h4>Мульти-таймфрейм</h4>
      <div class="d-stats">
        ${stat("1м", fmt.pct(r.r60), pctCls(r.r60))}
        ${stat("5м", fmt.pct(r.r300), pctCls(r.r300))}
        ${stat("15м", fmt.pct(r.r900), pctCls(r.r900))}
        ${stat("1ч", fmt.pct(r.r3600), pctCls(r.r3600))}
        ${stat("4ч", fmt.pct(r.r14400), pctCls(r.r14400))}
        ${stat("NATR", fmt.num(r.natr, 2) + "%")}
      </div>
    </div>
    <div class="d-sec"><h4>Объём и поток</h4>
      <div class="d-stats">
        ${stat("объём 24ч", "$" + fmt.compact(r.vol))}
        ${stat("сделки 24ч", fmt.int(r.tr))}
        ${stat("имбаланс", fmt.num(r.imb, 2), r.imb > 1 ? "pos" : "neg")}
        ${stat("дельта 1м", "$" + fmt.compact(r.d1m), pctCls(r.d1m))}
        ${stat("CVD", "$" + fmt.compact(r.cvd), pctCls(r.cvd))}
        ${stat("OI", r.oiusd ? "$" + fmt.compact(r.oiusd) : "—")}
      </div>
    </div>
    ${r.fund != null ? `<div class="d-sec"><h4>Фандинг</h4><div class="d-stats">${stat("ставка", fmt.fund(r.fund), pctCls(r.fund))}${stat("годовых", fmt.pct(r.fund * 3 * 365 * 100, 1), pctCls(r.fund))}${stat("рынок", r.mt || "—")}</div></div>` : ""}
    ${bookHtml}${densHtml}${spikeHtml}
    <div class="d-sec"><h4>Быстрый алерт</h4>
      <div style="display:flex;gap:7px;flex-wrap:wrap">
        <button class="mini" data-q="chg-gt" style="flex:1">24ч &gt; ${fmt.num(r.chg, 1)}%</button>
        <button class="mini" data-q="chg-lt" style="flex:1">24ч &lt; ${fmt.num(r.chg, 1)}%</button>
      </div>
    </div>`;

  initChart(key, r);

  $$("[data-q]", $("#drawer-body")).forEach((b) => b.addEventListener("click", async () => {
    const [f, op] = b.dataset.q.split("-");
    const val = parseFloat((r[f] ?? 0).toFixed(4));
    await createAlert({ field: f, op, value: val, exchange: r.exl || "", market_type: r.mt || "" });
  }));
}

function sparkSVG(points) {
  const w = 430, h = 74, pad = 4;
  const min = Math.min(...points), max = Math.max(...points);
  const span = (max - min) || 1;
  const up = points[points.length - 1] >= points[0];
  const col = up ? "#16c784" : "#ea3943";
  const step = (w - pad * 2) / Math.max(1, points.length - 1);
  const d = points.map((p, i) =>
    `${i ? "L" : "M"}${(pad + i * step).toFixed(1)},${(h - pad - ((p - min) / span) * (h - pad * 2)).toFixed(1)}`).join("");
  const area = `${d}L${(pad + (points.length - 1) * step).toFixed(1)},${h - pad}L${pad},${h - pad}Z`;
  return `<svg class="spark" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
    <defs><linearGradient id="g" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="${col}" stop-opacity=".33"/><stop offset="100%" stop-color="${col}" stop-opacity="0"/>
    </linearGradient></defs>
    <path d="${area}" fill="url(#g)"/><path d="${d}" fill="none" stroke="${col}" stroke-width="1.4"/>
  </svg>`;
}

/* ============================== СЕТКА ГРАФИКОВ ========================== */
const GRID_SIZES = [1, 2, 4, 6, 9, 12, 16];
const GRID_TFS = ["1m", "5m", "15m", "1h"];

function gridCols(n) {
  return n <= 1 ? 1 : n <= 2 ? 2 : n <= 4 ? 2 : n <= 6 ? 3 : n <= 9 ? 3 : n <= 12 ? 4 : n <= 16 ? 4 : 5;
}

function buildGridControls() {
  const g = state.grid;
  const exInfo = (state.meta && state.meta.exchange_info) || {};
  const exHost = $("#g-ex");
  const focusOn = state.focus.enabled;
  if (exHost) {
    exHost.innerHTML = '<span class="tg-label">биржа</span>';
    const labels = (state.meta.exchanges || []).filter((e) => {
      const info = exInfo[e] || {};
      return info.configured === false ? true : (info.markets || [state.mt]).includes(state.mt);
    });
    if (focusOn) ensureGridEx(labels);
    // «Все» есть только в полном режиме: фокус-режим = ровно одна биржа
    if (!focusOn) {
      const all = document.createElement("button");
      all.className = "gbtn ex" + (g.ex === "" ? " on" : "");
      all.textContent = "Все";
      all.addEventListener("click", () => { g.ex = ""; buildGridControls(); loadGrid(); });
      exHost.appendChild(all);
    }
    for (const e of labels) {
      const b = document.createElement("button");
      const isDex = !!(exInfo[e] && exInfo[e].dex);
      b.className = "gbtn ex" + (g.ex === e ? " on" : "") + (isDex ? " has-dex" : "");
      b.textContent = e;
      b.title = isDex ? "DEX (perp)" : "";
      b.addEventListener("click", () => { g.ex = e; buildGridControls(); loadGrid(); });
      exHost.appendChild(b);
    }
  }
  const tfHost = $("#g-tf");
  if (tfHost) {
    tfHost.innerHTML = '<span class="tg-label">тф</span>';
    for (const t of GRID_TFS) {
      const b = document.createElement("button");
      b.className = "gbtn" + (g.tf === t ? " on" : "");
      b.textContent = t;
      b.addEventListener("click", () => { g.tf = t; buildGridControls(); loadGrid(); });
      tfHost.appendChild(b);
    }
  }
  const szHost = $("#g-size");
  if (szHost) {
    szHost.innerHTML = '<span class="tg-label">сетка</span>';
    for (const n of GRID_SIZES) {
      const b = document.createElement("button");
      b.className = "gbtn" + (g.n === n ? " on" : "");
      b.textContent = n;
      b.addEventListener("click", () => { g.n = n; buildGridControls(); loadGrid(); });
      szHost.appendChild(b);
    }
  }
}

async function loadGrid() {
  const g = state.grid;
  const host = $("#charts-grid");
  if (!host) return;
  host.style.setProperty("--cols", gridCols(g.n));
  let d = null;
  let failed = false;
  if (!state.demo) {
    try {
      const r = await fetchT(`/api/grid?ex=${encodeURIComponent(g.ex)}&mt=${state.mt}`
        + `&tf=${g.tf}&n=${g.n}&limit=250`, 15000);
      if (r.ok) d = await r.json();
      else failed = true;
    } catch (e) { failed = true; d = null; }
  }
  // Сервер ответил, но ячеек ноль (фокус ещё считает отбор, биржа офлайн,
  // фильтру ничего не соответствует) — это НЕ повод показывать синтетику:
  // демо-данные подставляем только когда ответа нет вовсе. Иначе пользователь
  // видит «графики», которых на бирже не существует.
  if (!d) d = demoGrid();
  // фокус без бэкенда (DEMO) или бэкенд не ответил: режем выборку на клиенте,
  // чтобы поведение сетки не зависело от источника данных
  if (state.focus.enabled && (!state.focus.server || failed)) d = applyLocalFocus(d);
  if (state.focus.enabled && d && d.focus) renderFocus(d.focus);
  g.lastFailed = failed;
  g.cells = d.cells || [];
  g.tfSeconds = d.tf_seconds || TF_SEC[g.tf] || 300;
  renderGridCells();
  const head = $("#view-grid .view-head h2");
  if (head) {
    head.textContent = "Графики" + (failed ? "  ·  сервер не ответил, показан кэш/демо" : "");
  }
  startGridTimer();
}

function renderGridCells() {
  const g = state.grid;
  const host = $("#charts-grid");
  if (!host) return;
  // уничтожаем лишние чарты
  for (const [k, ch] of Array.from(g.charts)) {
    if (!g.cells.some((c) => c.k === k)) { g.charts.delete(k); }
  }
  host.innerHTML = "";
  if (!g.cells.length) {
    const f = state.focus;
    const why = f.enabled
      ? `Фокус-режим: для «${esc(g.ex || f.ex || "биржи")}» / «${state.mt === "spot" ? "спот" : "фьючерсы"}»
         под текущий фильтр не нашлось монет${f.note ? " (" + esc(f.note) + ")" : ""}.
         Смягчите фильтр или увеличьте «монет в стриме».`
      : `Нет данных: для «${esc(g.ex || "всех бирж")}» рынок
         «${state.mt === "spot" ? "спот" : "фьючерсы"}» не подключён или пуст.
         ${state.mt === "spot" ? "Попробуйте переключиться на фьючерсы." : ""}`;
    host.innerHTML = `<div class="cc-empty" style="grid-column:1/-1">${why}</div>`;
    return;
  }
  for (const cell of g.cells) {
    const el = document.createElement("div");
    el.className = "chart-cell";
    el.dataset.k = cell.k;
    el.innerHTML = `
      <div class="cc-head">
        <span class="cc-sym">${esc(cell.b || cell.s)}</span>
        <span class="cc-ex">${esc(cell.exl)}${cell.dex ? " ◆" : ""}</span>
        <span class="cc-chg ${cell.chg > 0 ? "pos" : cell.chg < 0 ? "neg" : ""}">${fmt.pct(cell.chg, 2)}</span>
        <button class="cc-pick" title="Открыть карточку инструмента">⤢</button>
      </div>
      <div class="cc-body"><canvas></canvas></div>
      ${cell.spike ? '<div class="cc-spike" title="спайк объёма/сделок">⚡</div>' : ""}`;
    host.appendChild(el);
    $(".cc-pick", el).addEventListener("click", (ev) => { ev.stopPropagation(); openDrawer(cell.k); });
    el.addEventListener("dblclick", () => openDrawer(cell.k));

    const cv = $("canvas", el);
    if (!cell.candles || !cell.candles.length) {
      const body = $(".cc-body", el);
      body.innerHTML = `<div class="cc-empty">нет свечей${cell.candles_ok === false ? " (таймаут биржи)" : ""}</div>`;
      host.appendChild(el);
      continue;
    }
    let ch = g.charts.get(cell.k);
    if (!ch && typeof CandleChart !== "undefined") {
      ch = new CandleChart(cv, { padRight: 52, padBottom: 16, volRatio: 0.22, minBars: 20 });
      g.charts.set(cell.k, ch);
    }
    if (ch) {
      ch.cv = cv;
      ch.setTfSeconds(g.tfSeconds);
      ch.setData({ candles: cell.candles || [], densities: [], spikes: [], last: cell.last });
    }
  }
}

/** Живое обновление последней свечи ячеек из общего WS-потока строк. */
function gridTick(row) {
  const ch = state.grid.charts.get(row.k);
  if (ch && row.last) ch.updateLast(row.last, Date.now());
}

function startGridTimer() {
  const g = state.grid;
  if (g.timer) clearInterval(g.timer);
  // свечи дёргаем реже, чем приходят тики: у бэкенда кэш на CANDLES_TTL
  g.timer = setInterval(() => {
    if (state.view === "grid" && !document.hidden) loadGrid();
  }, 15000);
}

/** DEMO-вариант сетки: тот же формат, что отдаёт /api/grid. */
function demoGrid() {
  const rows = demoRows.slice().sort((a, b) => b.vol - a.vol).slice(0, state.grid.n);
  return {
    tf_seconds: TF_SEC[state.grid.tf] || 300,
    cells: rows.map((r) => {
      const dc = demoCandles(r.k, r);
      return Object.assign({ candles: dc.candles }, r);
    }),
  };
}

/* ============================== ГРАФИК ================================== */
async function initChart(key, row) {
  state.chartKey = key;
  // Сбрасываем источник ДО загрузки. Иначе при медленном или неудачном запросе
  // в state остаётся значение от предыдущей карточки, и интерфейс показывает
  // «данные с биржи» там, где их нет.
  state.chartSource = null;
  const cv = $("#d-chart");
  if (!cv || typeof CandleChart === "undefined") return;
  if (state.chart) { try { state.chart.destroy && state.chart.destroy(); } catch (e) {} }
  state.chart = new CandleChart(cv, {});
  $$(".tf-btn", $("#drawer-body")).forEach((b) => b.addEventListener("click", () => {
    state.chartTf = b.dataset.tf;
    $$(".tf-btn").forEach((x) => x.classList.toggle("on", x.dataset.tf === state.chartTf));
    loadCandles(key, row);
  }));
  await loadCandles(key, row);
}

async function loadCandles(key, row) {
  const st = $("#d-chart-state");
  if (st) { st.hidden = false; st.textContent = "загрузка свечей…"; }
  let d = null;
  let failed = false;
  if (!state.demo) {
    try {
      const r = await fetchT(`/api/candles?key=${encodeURIComponent(key)}&tf=${state.chartTf}&limit=400`, 12000);
      if (r.ok) d = await r.json();
      else failed = true;
    } catch (e) { failed = true; d = null; }
  }
  if (!d) { d = demoCandles(key, row); d.source = "demo"; }
  if (state.chartKey !== key) return;          // карточку уже закрыли/переключили
  if (!d || !d.candles || !d.candles.length) {
    // источник ставим только когда данные действительно есть — иначе «exchange»
    // от прошлой карточки выглядел бы как успешная загрузка
    state.chartSource = "empty";
    if (st) {
      st.hidden = false;
      st.innerHTML = failed
        ? `не удалось получить свечи (таймаут или сервер занят).
           <button class="mini" id="d-retry" style="margin-top:6px">повторить</button>`
        : "свечей нет: биржа не отдаёт этот таймфрейм для инструмента";
      const rb = $("#d-retry");
      if (rb) rb.addEventListener("click", () => loadCandles(key, row));
    }
    return;
  }
  state.chartSource = d.source || "?";
  if (st) st.hidden = true;
  state.chart.setTfSeconds(d.tf_seconds || TF_SEC[state.chartTf] || 900);
  state.chart.setData({
    candles: d.candles,
    densities: d.densities || [],
    spikes: d.spikes || [],
    last: d.last != null ? d.last : (row ? row.last : null),
  });
}

/**
 * Live-обновление последней свечи: цена приходит из общего WS-потока строк,
 * поэтому график дёргается в такт рынку без повторных запросов к бирже.
 */
function chartTick(row) {
  if (!state.chart || !state.chartKey || row.k !== state.chartKey) return;
  if (row.last) state.chart.updateLast(row.last, Date.now());
}

/** Синтетические свечи для DEMO-режима: тот же формат, что отдаёт бэкенд. */
function demoCandles(key, row) {
  const tf = TF_SEC[state.chartTf] || 900;
  const n = 187;   // намеренно нестандартное число: по нему тест отличает демо от ответа биржи
  const now = Math.floor(Date.now() / 1000);
  const end = now - (now % tf);
  const last = row && row.last ? row.last : 100;
  const natr = (row && row.natr) || 0.8;
  const sigma = Math.max(natr, 0.05) / 100 * Math.sqrt(tf / 60);
  // идём назад от последней цены, потом разворачиваем
  let px = last;
  const out = [];
  for (let i = 0; i < n; i++) {
    const o = px;
    const c = Math.max(o * (1 + gaussDemo(0, sigma)), 1e-12);
    const h = Math.max(o, c) * (1 + Math.abs(gaussDemo(0, sigma * 0.6)));
    const l = Math.min(o, c) * (1 - Math.abs(gaussDemo(0, sigma * 0.6)));
    const v = Math.exp(gaussDemo(6, 1.1));
    out.push([(end - (n - i) * tf) * 1000, o, h, l, c, v]);
    px = o;
  }
  out[out.length - 1][4] = last;
  return {
    candles: out, tf_seconds: tf, last,
    densities: [], spikes: [],
  };
}
function gaussDemo(m = 0, s = 1) {
  let u = 0, v = 0;
  while (!u) u = Math.random(); while (!v) v = Math.random();
  return m + s * Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}

/* ============================== ТОСТЫ =================================== */
function toast(title, sub = "", onClick = null) {
  const host = $("#toasts");
  const el = document.createElement("div");
  el.className = "toast";
  el.innerHTML = `<div>${esc(title)}</div>${sub ? `<div class="t-time">${esc(sub)}</div>` : ""}`;
  if (onClick) el.addEventListener("click", onClick);
  el.addEventListener("click", () => el.remove());
  host.appendChild(el);
  setTimeout(() => el.remove(), 7000);
  while (host.children.length > 5) host.firstElementChild.remove();
}

/* ============================== STATIC BINDINGS ========================= */
function bindStatic() {
  $$(".tab").forEach((t) => t.addEventListener("click", () => switchView(t.dataset.view)));
  $("#search").addEventListener("input", debounce(() => pushFilters(), 350));
  $("#btn-reset").addEventListener("click", () => resetFilters(true));
  $("#btn-export").addEventListener("click", exportCsv);
  $("#row-limit").addEventListener("change", () => refilter());
  $("#opt-dens").addEventListener("change", () => pushFilters());
  $("#opt-flash").addEventListener("change", () => renderTable());
  $("#dens-refresh").addEventListener("click", () => loadDensities());
  $("#dens-min").addEventListener("change", () => loadDensities());
  $("#dens-side").addEventListener("change", () => loadDensities());
  $("#g-refresh").addEventListener("click", () => loadGrid());
  $("#d-close").addEventListener("click", closeDrawer);
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeDrawer();
    if (e.key === "/" && document.activeElement.tagName !== "INPUT") { e.preventDefault(); $("#search").focus(); }
  });
  $("#alert-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    await createAlert({
      field: $("#af-field").value, op: $("#af-op").value,
      value: parseFloat($("#af-value").value), exchange: $("#af-ex").value,
      market_type: $("#af-mt").value, symbol_contains: $("#af-sym").value,
      mode: $("#af-mode").value, cooldown: parseFloat($("#af-cool").value) || 60,
      notify_telegram: $("#af-tg").checked,
    });
  });
}

async function createAlert(payload) {
  if (state.demo) {
    toast("DEMO-режим: алерты работают только с запущенным бэкендом", "python run.py");
    return;
  }
  try {
    const r = await fetch("/api/alerts", { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload) });
    const d = await r.json();
    if (!r.ok) { toast("Не удалось создать алерт", d.error || r.status); return; }
    toast("Алерт создан", d.rule.describe);
    loadAlerts();
  } catch (e) { toast("Ошибка сети", e.message); }
}

function closeDrawer() {
  $("#drawer").hidden = true;
  state.chart = null;
  state.chartKey = null;
  state.chartSource = null;
}

function switchView(v) {
  state.view = v;
  $$(".tab").forEach((t) => t.classList.toggle("active", t.dataset.view === v));
  $$(".view").forEach((s) => s.classList.toggle("active", s.id === "view-" + v));
  if (v === "grid") { buildGridControls(); loadGrid(); }
  if (v === "map") renderMap();
  if (v === "screener") { buildGridHead(); renderTable(); }
  if (v === "densities") loadDensities();
  if (v === "alerts") loadAlerts();
}

function exportCsv() {
  const q = new URLSearchParams(collectQuery());
  if (state.demo) { toast("DEMO-режим", "CSV-экспорт доступен с запущенным бэкендом"); return; }
  window.open(`/api/screener.csv?${q.toString()}`, "_blank");
}

/* ============================== DEMO-РЕЖИМ ==============================
   Полностью клиентская симуляция: тот же формат строк, что и с бэкенда,
   поэтому весь код рендера используется без изменений.                  */
const DEMO_BASES = ["BTC","ETH","SOL","BNB","XRP","DOGE","ADA","AVAX","LINK","TON","DOT","LTC","UNI","NEAR","APT",
  "ARB","OP","INJ","SUI","SEI","TIA","JUP","WIF","PEPE","BONK","FLOKI","RNDR","AAVE","MKR","LDO","ENA","ONDO","WLD",
  "ORDI","STX","IMX","GRT","FIL","ICP","ETC","XLM","ALGO","VET","HBAR","QNT","RUNE","FTM","AR","MINA","KAS","TAO","PYTH"];
const DEMO_EX = [["binanceusdm","Binance Futures","swap",false],["bybit","Bybit","swap",false],
  ["okx","OKX","swap",false],["binance","Binance Spot","spot",false],
  ["mexc","MEXC","swap",false],["gate","Gate.io","swap",false],
  ["aster","Aster","swap",true],["hyperliquid","Hyperliquid","swap",true]];
const DEMO_P = {BTC:83500,ETH:3100,SOL:190,BNB:640,XRP:0.62,DOGE:0.16,TON:5.4,LINK:17,PEPE:0.0000121,WIF:1.8};
let demoRows = [], demoTimer = null;

function demoMeta() {
  return {
    mode: "demo",
    filters: fallbackFilters(),
    sorts: {}, presets: fallbackPresets(),
    exchanges: DEMO_EX.map((e) => e[1]), market_types: ["spot", "swap"],
    exchange_info: Object.fromEntries(DEMO_EX.map((e) => [e[1], { id: e[0], market: e[2], dex: e[3] }])),
    quotes: ["USDT", "USDC"], big_density_usd: 50000, push_interval: 1,
  };
}

function fallbackFilters() {
  const mk = (field, label, group, unit, hint) => ({ field, label, kind: "range", group, unit, step: 0.01, hint: hint || "" });
  const fl = (field, label, group, hint) => ({ field, label, kind: "flag", group, hint: hint || "" });
  return [
    mk("chg", "Изменение 24ч", "Движение", "%"), mk("r60", "Изменение 1м", "Движение", "%"),
    mk("r300", "Изменение 5м", "Движение", "%"), mk("r900", "Изменение 15м", "Движение", "%"),
    mk("r3600", "Изменение 1ч", "Движение", "%"), mk("r14400", "Изменение 4ч", "Движение", "%"),
    mk("rng", "Диапазон 24ч", "Движение", "%", "high/low − 1 за сутки"),
    mk("natr", "NATR", "Волатильность", "%"), mk("last", "Цена", "Движение", ""),
    mk("vol", "Объём 24ч", "Объём", "$"), mk("tr", "Сделок 24ч", "Объём", ""),
    mk("d1m", "Дельта 1м", "Поток", "$"), mk("cvd", "CVD", "Поток", "$"),
    mk("imb", "Дисбаланс стакана", "Поток", "x"),
    mk("fund", "Funding", "Деривативы", ""), mk("oiusd", "Open Interest", "Деривативы", "$"),
    fl("spike", "Есть спайк", "События"), fl("dens", "Есть крупная плотность", "События"),
    fl("green", "Только зелёные", "Движение"), fl("red", "Только красные", "Движение"),
  ];
}
function fallbackPresets() {
  return [
    { id: "in_play", label: "Монеты в игре", hint: "Объём + движение", params: { vol_min: 5e6, r3600_min: 3, sort: "r3600" } },
    { id: "breakout", label: "Пробой", hint: "Спайк + рост 5м", params: { spike: 1, r300_min: 1.5, sort: "r300" } },
    { id: "volatile", label: "Волатильность", hint: "Топ NATR", params: { natr_min: 1, sort: "natr" } },
    { id: "dump", label: "Проливы", hint: "Красные за час", params: { r3600_max: -5, sort: "r3600", desc: 0 } },
  ];
}

function startDemo() {
  if (demoRows.length) return;
  const now = Date.now() / 1000;
  demoRows = DEMO_BASES.flatMap((b, i) => {
    const out = [];
    for (let e = 0; e < 2; e++) {
      const [ex, exl, mt, isDex] = DEMO_EX[(i + e) % DEMO_EX.length];
      const q = e === 0 ? "USDT" : "USDC";
      const price = (DEMO_P[b] || (0.05 + (i * 7919 % 9000) / 100)) * (1 + (Math.random() - .5) * .002);
      const vol = Math.exp(gauss(14.2, 2.0));
      const chg = gauss(0, 4.2) + (Math.random() < .07 ? (Math.random() < .5 ? -1 : 1) * (8 + Math.random() * 26) : 0);
      const sym = mt === "spot" ? `${b}/${q}` : `${b}/${q}:${q}`;
      out.push({
        k: `${ex}:${sym}`, ex, exl, mt, dex: isDex, s: sym, b, q,
        last: price, bid: price * .9999, ask: price * 1.0001,
        chg, rng: Math.abs(gauss(6, 4)) + Math.abs(chg) * .4,
        hi: price * 1.05, lo: price * .95, vol, volq: vol,
        tr: Math.round(vol / price * 5), natr: Math.abs(gauss(.9, .8)),
        cvd: gauss(0, vol * .05), d1m: gauss(0, vol * .002), imb: Math.exp(gauss(0, .35)),
        fund: mt === "swap" ? gauss(.0001, .0004) : null,
        oi: null, oiusd: mt === "swap" ? vol * (.05 + Math.random() * .35) : null,
        spike: Math.random() < .08 ? { kind: Math.random() < .5 ? "volume" : "trades", ratio: 3 + Math.random() * 9, age: 30 } : null,
        u: now, hist: [price],
        r60: gauss(0, .3), r300: gauss(0, .6), r900: gauss(0, 1), r3600: gauss(0, 1.8), r14400: gauss(0, 3.4),
      });
    }
    return out;
  });
  demoApplyFilters();
}

function gauss(m = 0, s = 1) {
  let u = 0, v = 0;
  while (!u) u = Math.random(); while (!v) v = Math.random();
  return m + s * Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}

function demoTick() {
  const now = Date.now() / 1000;
  for (const r of demoRows) {
    let sigma = (r.natr || .5) / 100 * .12;
    if (Math.random() < .004) sigma *= 5 + Math.random() * 14;
    r.last = Math.max(r.last * (1 + gauss(0, sigma)), 1e-12);
    r.vol += Math.max(0, r.vol / 1440 * Math.exp(gauss(0, .7)));
    r.tr += Math.floor(Math.random() * 9);
    r.chg = ((r.last / (r.last / (1 + r.chg / 100))) - 1) * 100;
    const d = r.last * gauss(0, sigma) * 6;
    r.d1m = r.d1m * .9 + d * 60;
    r.cvd += d;
    r.imb = Math.max(.05, r.imb * (1 + gauss(0, .02)));
    if (Math.random() < .001) r.spike = { kind: Math.random() < .5 ? "volume" : "trades", ratio: 3 + Math.random() * 10, age: 0 };
    else if (r.spike) { r.spike.age += 1; if (r.spike.age > 240) r.spike = null; }
    r.r60 = r.r60 * .97 + (r.last / r.hist[r.hist.length - 1] - 1) * 100;
    r.hist.push(r.last); if (r.hist.length > 200) r.hist.shift();
    r.r300 = r.r300 * .995 + gauss(0, .04); r.r900 = r.r900 * .995 + gauss(0, .05);
    r.r3600 = r.r3600 * .998 + gauss(0, .03); r.r14400 = r.r14400 * .999 + gauss(0, .02);
    r.u = now;
  }
  if (Math.random() < .25) {
    const r = demoRows[Math.floor(Math.random() * demoRows.length)];
    onAlert({ rule_id: "demo", key: r.k, field: "chg", value: r.chg, ts: now,
      text: `🔔 ${r.exl} ${r.s} — демо-событие: 24ч ${r.chg.toFixed(2)}%` });
  }
  demoApplyFilters();
}

function demoApplyFilters() {
  const q = collectQuery();
  let rows = demoRows.slice();
  for (const [k, v] of Object.entries(q)) {
    const m = k.match(/^(.+)_(min|max)$/);
    if (m && state.meta.filters.some((f) => f.field === m[1])) {
      const f = m[1], lim = parseFloat(v);
      rows = rows.filter((r) => r[f] != null && (m[2] === "min" ? r[f] >= lim : r[f] <= lim));
    }
  }
  if (q.q) { const s = q.q.toUpperCase(); rows = rows.filter((r) => r.s.toUpperCase().includes(s) || r.b.includes(s)); }
  if (q.ex) { const ex = q.ex.split(","); rows = rows.filter((r) => ex.includes(r.exl) || ex.includes(r.ex)); }
  if (q.mt) rows = rows.filter((r) => r.mt === q.mt);
  if (q.spike === "1") rows = rows.filter((r) => r.spike);
  if (q.green === "1") rows = rows.filter((r) => r.chg > 0);
  if (q.red === "1") rows = rows.filter((r) => r.chg < 0);
  const sk = q.sort || "vol", desc = q.desc !== "0";
  rows.sort((a, b) => ((b[sk] ?? -1e18) - (a[sk] ?? -1e18)) * (desc ? 1 : -1));
  state.rows = rows.slice(0, parseInt(q.limit || "150", 10));
  state.rowIndex = new Map(state.rows.map((r) => [r.k, r]));
  // в DEMO серверного отбора нет, поэтому фокус применяется здесь же:
  // иначе демо-тик каждую секунду возвращал бы полный список и «top-N» не работал
  if (state.focus.enabled && !state.focus.server) applyLocalFocus();
  state.overview = {
    symbols: demoRows.length, up: demoRows.filter((r) => r.chg > 0).length,
    down: demoRows.filter((r) => r.chg < 0).length, volume_usd: demoRows.reduce((s, r) => s + r.vol, 0),
  };
  renderOverview();
  renderFocus();
  $("#st-updated").textContent = new Date().toLocaleTimeString("ru-RU");
  $("#st-total").textContent = displayedRows().length || rows.length;
  $("#st-ws").textContent = "demo";
  if (state.view === "map") renderMap();
  if (state.view === "screener") renderTable();
}

function demoDensities() {
  const out = [];
  for (const r of demoRows.slice(0, 60)) {
    if (Math.random() > .55) continue;
    const side = Math.random() < .5 ? "bid" : "ask";
    const dist = (Math.random() * 1.6 + .05) * (side === "bid" ? -1 : 1);
    out.push({ k: r.k, s: r.s, exl: r.exl, mt: r.mt, p: r.last * (1 + dist / 100),
      q: Math.exp(gauss(11.6, 1.1)), base: 1000, sd: side, d: dist, n: 1 + Math.floor(Math.random() * 4),
      last: r.last, chg: r.chg, vol: r.vol });
  }
  return out.sort((a, b) => b.q - a.q).slice(0, 120);
}

function demoSymbol(key) {
  const r = state.rowIndex.get(key) || demoRows.find((x) => x.k === key) || demoRows[0];
  return Object.assign({}, r, { spark: (r.hist || []).slice(-120), book: { bids: [], asks: [] },
    densities: [], spikes: r.spike ? [{ kind: r.spike.kind, ratio: r.spike.ratio, value: r.vol / 60, base: r.vol / 300, age: r.spike.age }] : [] });
}

/* ============================== СТАРТ =================================== */
// отладочный хук: даёт смоук-тестам (и консоли браузера) доступ к состоянию,
// т.к. const-объявления верхнего уровня не попадают в свойства window
window.__screener = { state, fmt, chgColor, collectQuery, demoCandles, loadCandles,
                      closeDrawer, openDrawer, switchView,
                      toggleFocusMode, syncFocus, loadFocus, renderFocus,
                      applyLocalFocus, buildFocusPanel, syncChipsUI, focusEx,
                      buildChips, buildGridControls, loadGrid,
                      loadBaseRows, displayedRows, mergeBase, refilter };

boot();
