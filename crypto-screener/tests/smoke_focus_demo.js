/**
 * Смоук фокус-режима БЕЗ бэкенда (DEMO-путь).
 *
 * Отдельно от основного смока: здесь fetch всегда отказывает, WebSocket
 * недоступен, поэтому app.js обязан уйти в клиентскую симуляцию и сделать
 * отбор top-N сам. Проверяем, что фокус-режим не «отваливается молча»,
 * а работает и честно пишет, что отбор локальный.
 *
 * Запуск: NODE_PATH=<jsdom> WEB=<каталог web> node tests/smoke_focus_demo.js
 */
const fs = require("fs");
const path = require("path");
const { JSDOM } = require("jsdom");

const WEB = process.env.WEB || path.join(__dirname, "..", "web");
let fails = 0;
const check = (n, c, d = "") => {
  console.log(`${c ? "  ok  " : "  FAIL"}  ${n}${d ? " — " + d : ""}`);
  if (!c) process.exitCode = 1, fails++;
};

(async () => {
  const html = fs.readFileSync(path.join(WEB, "index.html"), "utf8");
  const dom = new JSDOM(html, { url: "http://127.0.0.1:9/", runScripts: "outside-only",
                                pretendToBeVisual: true });
  const { window } = dom;

  const ops = { fillRect: 0, stroke: 0, fillText: 0, clearRect: 0 };
  const ctxMock = new Proxy({}, {
    get: (t, k) => k in ops ? (() => { ops[k]++; })
      : k === "measureText" ? ((s) => ({ width: String(s).length * 6 }))
      : k === "canvas" ? null : (t[k] !== undefined ? t[k] : (() => {})),
    set: (t, k, v) => { t[k] = v; return true; },
  });
  window.HTMLCanvasElement.prototype.getContext = () => ctxMock;
  window.HTMLCanvasElement.prototype.getBoundingClientRect = () =>
    ({ width: 430, height: 236, top: 0, left: 0, right: 430, bottom: 236 });

  // бэкенда нет вовсе
  window.fetch = () => Promise.reject(new Error("no backend"));
  window.WebSocket = function () { throw new Error("no ws"); };
  const errs = [];
  window.addEventListener("error", (e) => errs.push("window.error: " + e.message));
  window.addEventListener("unhandledrejection", (e) =>
    errs.push("unhandledrejection: " + ((e.reason && e.reason.message) || e.reason || "?")));
  const origErr = window.console.error;
  window.console.error = (...a) => { errs.push("console.error: " + a.join(" ")); origErr(...a); };

  window.eval(fs.readFileSync(path.join(WEB, "chart.js"), "utf8"));
  window.eval(fs.readFileSync(path.join(WEB, "app.js"), "utf8"));
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  await sleep(4000);

  const d = window.document;
  const q = (s) => d.querySelector(s);
  const S = () => window.__screener.state;

  console.log("=== DEMO-РЕЖИМ БЕЗ БЭКЕНДА ===");
  check("UI живой без бэкенда", /DEMO/.test(q("#mode-pill").textContent), q("#mode-pill").textContent);
  check("строки демо-данных есть", S().rows.length > 10, S().rows.length + " строк");

  console.log("\n=== ФОКУС-РЕЖИМ В DEMO (локальный отбор) ===");
  q("#focus-limit").value = "12";
  q("#focus-limit").dispatchEvent(new window.Event("change", { bubbles: true }));
  q("#focus-on").checked = true;
  q("#focus-on").dispatchEvent(new window.Event("change", { bubbles: true }));
  await sleep(3500);
  check("режим включился без бэкенда", S().focus.enabled === true, "");
  check("серверный путь не заявлен", S().focus.server === false, "server=" + S().focus.server);
  check("статус честно пишет про локальный отбор",
    /локальный/.test(q("#focus-status").textContent), q("#focus-status").textContent);
  check("выбрана одна биржа", S().exchanges.size <= 1, Array.from(S().exchanges).join(",") || "—");
  const before = S().rows.length;
  check("строки сжаты до лимита на клиенте", S().rows.length <= 12,
    `было ${before} → стало ${S().rows.length}`);
  check("в сетке не больше ячеек, чем в отборе",
    d.querySelectorAll(".chart-cell").length <= 12,
    d.querySelectorAll(".chart-cell").length + " ячеек");

  q("#focus-on").checked = false;
  q("#focus-on").dispatchEvent(new window.Event("change", { bubbles: true }));
  await sleep(2500);
  check("после выключения выборка снова широкая", S().rows.length > 12, S().rows.length + " строк");
  check("нет ошибок JS", errs.length === 0, errs.slice(0, 3).join(" || ") || "чисто");

  console.log(fails ? "\n>>> ЕСТЬ ПАДЕНИЯ <<<" : "\n>>> ВСЁ ЗЕЛЁНОЕ <<<");
  process.exit(fails ? 1 : 0);
})();
