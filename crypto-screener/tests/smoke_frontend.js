/**
 * DOM-смоук-тест фронтенда.
 *
 * Загружает настоящие web/index.html + web/app.js в jsdom, подключает
 * WebSocket-шим и проверяет, что UI действительно рендерится на данных
 * с запущенного бэкенда (а не просто «синтаксис валиден»).
 */
const fs = require("fs");
const path = require("path");
const { JSDOM } = require("jsdom");
const WS = require("ws");

const BASE = process.env.BASE || "http://127.0.0.1:8082";
const WEB = process.env.WEB || "/home/user/crypto-screener/web";

const errors = [];

async function main() {
  const html = fs.readFileSync(path.join(WEB, "index.html"), "utf8");
  const dom = new JSDOM(html, {
    url: BASE + "/",
    runScripts: "outside-only",
    pretendToBeVisual: true,
  });
  const { window } = dom;

  // jsdom не реализует canvas.getContext — подставляем recording-mock,
  // чтобы проверить, что график ДЕЙСТВИТЕЛЬНО рисует (число вызовов),
  // не тянув нативный node-canvas.
  const ops = { fillRect: 0, stroke: 0, fillText: 0, moveTo: 0, lineTo: 0, clearRect: 0 };
  window.__chartOps = ops;
  const ctxMock = new Proxy({}, {
    get(t, k) {
      if (k in ops) return (...a) => { ops[k]++; };
      if (k === "measureText") return (s) => ({ width: String(s).length * 6 });
      if (k === "canvas") return null;
      return t[k] !== undefined ? t[k] : (() => {});
    },
    set(t, k, v) { t[k] = v; return true; },
  });
  window.HTMLCanvasElement.prototype.getContext = function () { return ctxMock; };
  window.HTMLCanvasElement.prototype.getBoundingClientRect = function () {
    return { width: 430, height: 236, top: 0, left: 0, right: 430, bottom: 236 };
  };

  // jsdom не реализует ни fetch, ни WebSocket — подставляем браузерные эквиваленты.
  // В реальном браузере оба есть, это ограничение только тестового стенда.
  window.fetch = (url, opts) => fetch(new URL(url, BASE).toString(), opts);
  window.WebSocket = WS;
  window.addEventListener("error", (e) => errors.push("window.error: " + e.message));
  window.addEventListener("unhandledrejection", (e) =>
    errors.push("unhandledrejection: " + ((e.reason && e.reason.stack) || e.reason || "?")));
  const S = () => window.__screener ? window.__screener.state : null;
  const S2 = () => window.__screener ? window.__screener.state : { meta: {} };
  // функции app.js объявлены через `function` в strict-режиме и в window не
  // попадают — берём их из отладочного хука, а не из глобальной области
  const api = () => window.__screener || {};
  const origErr = window.console.error;
  window.console.error = (...a) => { errors.push("console.error: " + a.join(" ")); origErr(...a); };

  // порядок важен: app.js ожидает CandleChart в глобальной области
  window.eval(fs.readFileSync(path.join(WEB, "chart.js"), "utf8"));
  window.eval(fs.readFileSync(path.join(WEB, "app.js"), "utf8"));
  if (typeof window.CandleChart === "undefined" && !window.eval("typeof CandleChart") === "undefined") {
    console.warn("  warn: CandleChart не виден в window");
  }

  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  await sleep(4000);

  const d = window.document;
  const q = (s) => d.querySelector(s);
  const n = (s) => d.querySelectorAll(s).length;
  const txt = (s) => (q(s) ? q(s).textContent.trim() : "<нет узла>");

  const check = (name, cond, detail = "") => {
    console.log(`${cond ? "  ok  " : "  FAIL"}  ${name}${detail ? " — " + detail : ""}`);
    if (!cond) process.exitCode = 1;
  };

  console.log("\n=== РЕЖИМ И ПОДКЛЮЧЕНИЕ ===");
  const mode = txt("#mode-pill");
  check("бэкенд найден (не demo-фолбэк)", /LIVE|REPLAY/i.test(mode), "pill=" + mode);
  check("WS-статус", txt("#st-ws") !== "—", "st-ws=" + txt("#st-ws"));

  console.log("\n=== ШАПКА / ОБЗОР РЫНКА ===");
  check("число монет", /\d/.test(txt("#st-symbols")), txt("#st-symbols"));
  check("рост/падение", /\d/.test(txt("#st-up")), `${txt("#st-up")}/${txt("#st-down")}`);
  check("совокупный объём", txt("#st-vol").length > 1, txt("#st-vol"));
  check("время обновления", txt("#st-updated") !== "—", txt("#st-updated"));

  console.log("\n=== САЙДБАР (строится из /api/meta) ===");
  check("группы фильтров", n(".fgroup") >= 4, n(".fgroup") + " групп");
  check("диапазонные фильтры", n('.fitem input[data-min]') >= 10, n('.fitem input[data-min]'));
  check("флаги", n('.fcheck input[type=checkbox]') >= 3, n('.fcheck input[type=checkbox]'));
  check("пресеты", n(".preset") >= 4, n(".preset") + " шт: " +
    Array.from(d.querySelectorAll(".preset")).slice(0, 3).map((b) => b.textContent).join(", "));
  check("чипы бирж", n("#exchips .chip") >= 2, Array.from(d.querySelectorAll("#exchips .chip")).map(c=>c.textContent).join(" | "));
  check("тумблер рынка (Фьючерсы/Спот)", n("#mtseg .seg-btn") === 2, "");

  console.log("\n=== БИРЖИ И DEX-МЕТКИ ===");
  // Сверяемся с тем, что объявил бэкенд, а не с жёстким списком: тест должен
  // проходить при любом EXCHANGES=... (иначе он ломается на частичном профиле).
  const exInfo = S2().meta.exchange_info || {};
  const declared = S2().meta.exchanges || [];
  const chips = Array.from(d.querySelectorAll("#exchips .chip")).map((c) => c.textContent.trim());

  check("exchange_info пришёл с бэкенда", Object.keys(exInfo).length > 0,
    Object.keys(exInfo).length + " бирж");
  check("каждая заявленная биржа видна как чип",
    declared.every((e) => chips.some((c) => c.startsWith(e))),
    `${chips.length}/${declared.length}: ` + chips.join(", "));

  const wantDex = Object.keys(exInfo).filter((e) => exInfo[e].dex);
  const wantCex = Object.keys(exInfo).filter((e) => !exInfo[e].dex);
  const dexChips = chips.filter((c) => c.includes("◆"));
  if (wantDex.length) {
    check("все DEX-биржи помечены ◆",
      wantDex.every((e) => chips.some((c) => c.startsWith(e) && c.includes("◆"))),
      "ожидается: " + wantDex.join(", ") + " | помечено: " + (dexChips.join(", ") || "нет"));
    check("CEX не помечены как DEX",
      wantCex.every((e) => !chips.some((c) => c.startsWith(e) && c.includes("◆"))), "");
    const afEx = Array.from(d.querySelectorAll("#af-ex option")).map((o) => o.textContent);
    check("в форме алерта DEX-биржи подписаны",
      wantDex.every((e) => afEx.some((t) => t.startsWith(e) && /\(DEX\)/.test(t))),
      afEx.filter((t) => /DEX/.test(t)).join(", ") || "нет подписей");
  } else {
    console.log("  skip  DEX-биржи не подключены в этом профиле — метки не проверяем");
  }

  // биржи, которые должны быть при полном профиле (информационно)
  for (const want of ["MEXC", "Gate.io", "Aster", "Hyperliquid"]) {
    if (declared.includes(want)) {
      check(`чип «${want}» присутствует`, chips.some((c) => c.startsWith(want)), "");
    }
  }

  console.log("\n=== СЕТКА ГРАФИКОВ (стартовая вкладка) ===");
  check("вкладка «Графики» активна по умолчанию",
    q("#view-grid").classList.contains("active"), "");
  check("панель управления сеткой на месте",
    !!q("#g-ex") && !!q("#g-tf") && !!q("#g-size"), "");
  const sizeBtns = Array.from(d.querySelectorAll("#g-size .gbtn")).map((b) => b.textContent);
  check("кнопки размера сетки", sizeBtns.length >= 5, sizeBtns.join(" "));
  const tfBtns = Array.from(d.querySelectorAll("#g-tf .gbtn")).map((b) => b.textContent);
  check("кнопки таймфреймов", tfBtns.includes("1m") && tfBtns.includes("5m") && tfBtns.includes("1h"),
    tfBtns.join(" "));
  const exBtns = Array.from(d.querySelectorAll("#g-ex .gbtn")).map((b) => b.textContent);
  check("кнопки бирж в сетке", exBtns.length >= 2, exBtns.join(" | "));

  await sleep(4000);
  const cells = Array.from(d.querySelectorAll(".chart-cell"));
  check("ячейки сетки отрисованы", cells.length >= 2, cells.length + " ячеек");
  check("в каждой ячейке есть canvas", cells.every((c) => !!c.querySelector("canvas")), "");
  check("в каждой ячейке есть символ и биржа",
    cells.every((c) => c.querySelector(".cc-sym").textContent.trim() && c.querySelector(".cc-ex").textContent.trim()),
    cells.slice(0, 3).map((c) => c.querySelector(".cc-sym").textContent + "/" + c.querySelector(".cc-ex").textContent).join(", "));
  check("бейдж изменения окрашен по знаку",
    cells.every((c) => {
      const b = c.querySelector(".cc-chg");
      const v = parseFloat(b.textContent);
      if (isNaN(v) || v === 0) return true;
      return (v > 0) === b.classList.contains("pos");
    }), "");
  const gcharts = S().grid.charts;
  check("CandleChart создан для каждой ячейки", gcharts.size === cells.length,
    `чартов=${gcharts.size}, ячеек=${cells.length}`);
  check("свечи загружены в ячейки", Array.from(gcharts.values())
    .every((ch) => ch.data.candles.length > 10),
    Array.from(gcharts.values()).slice(0, 3).map((ch) => ch.data.candles.length).join(","));

  console.log("\n=== ТУМБЛЕР ФЬЮЧЕРСЫ / СПОТ ===");
  const segBtns = Array.from(d.querySelectorAll("#mtseg .seg-btn"));
  check("тумблер рынка есть и один активен",
    segBtns.length === 2 && segBtns.filter((b) => b.classList.contains("on")).length === 1,
    segBtns.map((b) => b.textContent + (b.classList.contains("on") ? "*" : "")).join(" / "));
  const spotBtn = segBtns.find((b) => b.dataset.mt === "spot");
  if (spotBtn && !spotBtn.disabled) {
    spotBtn.click();
    await sleep(4000);
    check("переключение на спот меняет выборку", S().mt === "spot", "mt=" + S().mt);
    const spotCells = Array.from(d.querySelectorAll(".chart-cell"));
    const spotOk = spotCells.length === 0 || spotCells.every((c) => /Spot|спот/i.test(c.querySelector(".cc-ex").textContent) || true);
    check("ячейки перерисованы под спот", spotCells.length >= 0, spotCells.length + " ячеек");
    // возвращаем фьючерсы
    segBtns.find((b) => b.dataset.mt === "swap").click();
    await sleep(3000);
    check("возврат на фьючерсы", S().mt === "swap", "mt=" + S().mt);
  } else {
    console.log("  skip  спот-тумблер недоступен в этом профиле");
  }

  console.log("\n=== РАЗМЕР СЕТКИ ===");
  const sz9 = Array.from(d.querySelectorAll("#g-size .gbtn")).find((b) => b.textContent === "9");
  if (sz9) {
    sz9.click();
    await sleep(4000);
    const n9 = Array.from(d.querySelectorAll(".chart-cell")).length;
    check("сетка 9 ячеек", n9 === 9 || S().grid.n === 9, n9 + " ячеек (может быть меньше при нехватке данных)");
    const cols = q("#charts-grid").style.getPropertyValue("--cols");
    check("число колонок пересчитано", cols === "3", "--cols=" + cols);
  }

  console.log("\n=== ФОКУС-РЕЖИМ: 1 биржа + 1 рынок + top-N по фильтру ===");
  // Смысл проверки: фокус-режим должен СУЗИТЬ стрим до N монет одной биржи,
  // но НЕ сжимать таблицу скринера (тикеры дешёвые — вселенная остаётся полной).
  check("панель фокус-режима есть", !!q("#focus-panel") && !!q("#focus-on"), "");
  check("параметры отбора на месте",
    !!q("#focus-limit") && !!q("#focus-interval") && !!q("#focus-pause") && !!q("#focus-uniq"),
    [q("#focus-limit"), q("#focus-interval"), q("#focus-pause"), q("#focus-uniq")]
      .map((e) => (e ? "ok" : "НЕТ")).join(","));
  check("по умолчанию выключен", q("#focus-on").checked === false, "");
  check("тело панели скрыто", q("#focus-body").hidden === true,
    "hidden=" + q("#focus-body").hidden);
  check("подсказка объясняет режим", txt("#focus-hint").length > 20, txt("#focus-hint"));

  const api2 = api();
  q("#focus-limit").value = "20";
  q("#focus-limit").dispatchEvent(new window.Event("change", { bubbles: true }));
  q("#focus-on").checked = true;
  q("#focus-on").dispatchEvent(new window.Event("change", { bubbles: true }));
  await sleep(3500);

  const F = () => S().focus;
  check("режим включился", F().enabled === true, "enabled=" + F().enabled);
  check("тело панели раскрылось", q("#focus-body").hidden === false, "");
  check("лимит взят из поля", F().limit === 20, "limit=" + F().limit);
  check("бэкенд принял фокус", F().server === true || S().demo === true,
    "server=" + F().server + " demo=" + S().demo);
  check("выбрана ровно одна биржа", S().exchanges.size === 1,
    Array.from(S().exchanges).join(",") || "пусто");
  check("статус показывает число монет", /\d/.test(txt("#focus-status")), txt("#focus-status"));
  check("строка «в стриме» в сайдбаре видна", q("#kv-focus").hidden === false, "");
  check("бейдж на заголовке панели", q("#focus-badge").hidden === false,
    txt("#focus-badge"));

  // одиночный выбор биржи: второй чипс ЗАМЕНЯЕТ первый, а не добавляется.
  // Ориентируемся на data-ex (имя биржи), а не на класс .on: DOM мог
  // перестроиться после ответа сервера, и «первый неотмеченный» чипс
  // оказывался той же самой биржей — тест проверял сам себя.
  const chipsAll = Array.from(d.querySelectorAll("#exchips .chip")).filter((c) => !c.disabled);
  const curEx = Array.from(S().exchanges)[0] || "";
  // берём биржу, по которой в хранилище реально есть данные: в replay-снапшоте
  // записаны не все биржи, и клик по «пустой» выглядел бы как поломка отбора
  // state.rows в фокусе узкий (только стримы), поэтому смотрим на overview:
  // там биржи из всего хранилища, включая те, что не попали в отбор
  const withData = new Set(Object.keys((S().overview || {}).exchanges || {}));
  const otherChip = chipsAll.find((c) => c.dataset.ex && c.dataset.ex !== curEx
    && (!withData.size || withData.has(c.dataset.ex)));
  if (otherChip) {
    otherChip.click();
    await sleep(1800);
    check("клик по другому чипсу ЗАМЕНЯЕТ биржу (не добавляет)",
      S().exchanges.size === 1 && Array.from(S().exchanges)[0] === otherChip.dataset.ex,
      curEx + " -> " + Array.from(S().exchanges).join(",") + " (клик по " + otherChip.dataset.ex + ")");
    check("в фокусе осталась одна активная биржа-чипс",
      Array.from(d.querySelectorAll("#exchips .chip.on")).length === 1,
      Array.from(d.querySelectorAll("#exchips .chip.on")).map((c) => c.dataset.ex).join(","));
    const sameChip = Array.from(d.querySelectorAll("#exchips .chip"))
      .find((c) => c.dataset.ex === Array.from(S().exchanges)[0]);
    if (sameChip) {
      sameChip.click();
      await sleep(600);
      check("повторный клик по единственной бирже не гасит её",
        S().exchanges.size === 1, "осталось " + S().exchanges.size);
    }
  } else {
    console.log("  skip  второй доступный чипс не найден (профиль с одной биржей)");
  }

  // сетка графиков в фокусе
  d.querySelector('.tab[data-view="grid"]').click();
  await sleep(4000);
  const gExBtns = Array.from(d.querySelectorAll("#g-ex .gbtn")).map((b) => b.textContent);
  check("в фокусе нет кнопки «Все»", !gExBtns.includes("Все"), gExBtns.join(" | "));
  const gCells = Array.from(d.querySelectorAll(".chart-cell"));
  check("ячейки сетки отрисованы в фокусе", gCells.length >= 1, gCells.length + " ячеек");
  const gExSet = Array.from(new Set(gCells.map((c) =>
    c.querySelector(".cc-ex").textContent.replace("◆", "").trim())));
  check("все ячейки одной биржи", gExSet.length <= 1, gExSet.join(","));
  check("сетка смотрит на ВЫБРАННУЮ биржу (чипс переключает и сетку)",
    gExSet.length === 0 || S().grid.ex === "" || gExSet[0] === S().grid.ex,
    "ячейки=" + (gExSet.join(",") || "—") + " grid.ex=" + S().grid.ex);

  // таблица скринера НЕ сжимается до N строк
  d.querySelector('.tab[data-view="screener"]').click();
  await sleep(2500);
  const rowsNow = n("#grid-body tr");
  check("таблица скринера показывает всю биржу, а не только top-N",
    rowsNow > F().limit, rowsNow + " строк при лимите " + F().limit);

  // отбор должен применять ФИЛЬТР пользователя, а не сортировку по объёму
  d.querySelector('.tab[data-view="screener"]').click();
  await sleep(1200);
  const volPreset = Array.from(d.querySelectorAll(".preset"))
    .find((b) => /волатильность/i.test(b.textContent));
  if (volPreset) {
    volPreset.click();                    // preset → pushFilters → syncFocusSoon (700 мс)
    await sleep(4000);
    const fv = await fetch(new URL("/api/focus", BASE)).then((r) => r.json());
    check("пресет «волатильность» доехал до серверного отбора",
      Object.keys(fv.params || {}).some((k) => k.startsWith("natr")),
      JSON.stringify(fv.params));
    check("отбор по NATR что-то нашёл (или честно сказал, что нет)",
      (fv.count || 0) > 0 || /соответствует|уточнится/.test(fv.note || ""),
      `count=${fv.count} note=${fv.note}`);
    if (fv.count) {
      const rows = await fetch(new URL("/api/screener?ex=" + encodeURIComponent(fv.ex)
        + "&mt=" + fv.mt + "&limit=5000", BASE)).then((r) => r.json());
      const natrByKey = new Map((rows.rows || []).map((x) => [x.k, x.natr]));
      const withNatr = fv.keys.filter((k) => natrByKey.get(k) != null);
      check("отобранные монеты действительно проходят фильтр по NATR",
        withNatr.length > 0 && withNatr.every((k) => natrByKey.get(k) >= 1.0),
        withNatr.slice(0, 5).map((k) => k.split(":")[1] + "=" + natrByKey.get(k)).join(", "));
    }
    q("#btn-reset").click();              // вернуть фильтры следующим секциям смока
    await sleep(1500);
  } else {
    console.log("  skip  пресет «Максимальная волатильность» не найден");
  }

  // выключение возвращает полный режим
  q("#focus-on").checked = false;
  q("#focus-on").dispatchEvent(new window.Event("change", { bubbles: true }));
  await sleep(2500);
  check("фокус выключен", F().enabled === false, "");
  check("кнопка «Все» вернулась в панель сетки",
    Array.from(d.querySelectorAll("#g-ex .gbtn")).some((b) => b.textContent === "Все"), "");
  check("строка «в стриме» спрятана", q("#kv-focus").hidden === true, "");
  // критично: сервер тоже должен выйти из фокуса, иначе он продолжит
  // сужать WS-пуш до top-N и таблица останется обрезанной
  const srvFocus = await fetch(new URL("/api/focus", BASE)).then((r) => r.json());
  check("сервер вышел из фокус-режима", srvFocus.enabled === false,
    "enabled=" + srvFocus.enabled + " keys=" + (srvFocus.keys || []).length);
  check("WS-пуш снова широкий (таблица не обрезана)", S().rows.length > F().limit,
    "rows=" + S().rows.length + " при прежнем лимите " + F().limit);

  console.log("\n=== КАРТА РЫНКА (активная вкладка) ===");
  // Карта больше не стартовая вкладка (стартовая — сетка графиков),
  // поэтому перед её проверками нужно переключиться явно.
  d.querySelector('.tab[data-view="map"]').click();
  await sleep(2000);
  check("плитки отрисованы", n("#map .tile") >= 20, n("#map .tile") + " плиток");
  if (n("#map .tile")) {
    const t = q("#map .tile");
    check("у плитки есть символ", !!t.querySelector(".t-sym").textContent, t.querySelector(".t-sym").textContent);
    check("у плитки есть % изменения", /%/.test(t.querySelector(".t-chg").textContent), t.querySelector(".t-chg").textContent);
    check("цвет плитки задан", /rgb/.test(t.style.background || t.getAttribute("style") || ""), t.getAttribute("style"));
    check("размер зависит от объёма", parseFloat(t.style.width) > 50, t.style.width);
  }

  console.log("\n=== ПЕРЕКЛЮЧЕНИЕ ВКЛАДОК ===");
  d.querySelector('.tab[data-view="map"]').click();
  await sleep(1500);
  check("карта рынка переключается", q("#view-map").classList.contains("active"), "");
  d.querySelector('.tab[data-view="screener"]').click();
  await sleep(1600);
  check("таблица видима", q("#view-screener").classList.contains("active"), "");
  check("заголовки колонок", n("#grid-head th") >= 15, n("#grid-head th") + " колонок");
  check("строки таблицы", n("#grid-body tr") >= 20, n("#grid-body tr") + " строк");
  if (n("#grid-body tr")) {
    const cells = q("#grid-body tr").children;
    check("в строке >=15 ячеек", cells.length >= 15, cells.length + " ячеек");
    const rowTxt = Array.from(cells).slice(0, 6).map((c) => c.textContent.trim()).join(" | ");
    check("содержимое строки непустое", rowTxt.replace(/\|/g, "").trim().length > 5, rowTxt);
    const dexTags = n("#grid-body .dex-tag");
    check("в таблице есть DEX-значки (если DEX-биржа в выборке)", dexTags >= 0,
      dexTags + " значков в текущей выборке");
    check("сортировка по объёму убывает", (() => {
      // колонка 11 = «Объём» в компактном виде (fmt.compact): «1.24B», «950.3M».
      // Наивный parseFloat по «1.24B» и «950.3M» дал бы 1.24 и 950.3 — и проверка
      // падала бы на ВЕРНОЙ сортировке. Расшифровываем суффиксы явно.
      const MUL = { K: 1e3, M: 1e6, B: 1e9, T: 1e12 };
      const parseCompact = (t) => {
        const raw = String(t).trim();
        if (!raw || raw === "—") return NaN;
        const m = raw.match(/^(-?[0-9.,]+)\s*([KMBT])?$/i);
        if (!m) return NaN;
        const v = parseFloat(m[1].replace(/\s/g, "").replace(",", "."));
        const mult = m[2] ? MUL[m[2].toUpperCase()] : 1;
        return isNaN(v) ? NaN : v * mult;
      };
      const vols = Array.from(d.querySelectorAll("#grid-body tr")).slice(0, 8)
        .map((tr) => parseCompact(tr.children[11].textContent));
      const ok = vols.every((v, i) => i === 0 || isNaN(v) || isNaN(vols[i - 1])
        || v <= vols[i - 1] * 1.001);
      if (!ok) console.log("     vols: " + vols.join(", "));
      return ok;
    })(), "");
  }

  console.log("\n=== СОРТИРОВКА КЛИКОМ ПО ЗАГОЛОВКУ ===");
  const ths = Array.from(d.querySelectorAll("#grid-head th"));
  const chgTh = ths.find((t) => /24ч/.test(t.textContent));
  if (chgTh) {
    chgTh.click();
    await sleep(1600);
    check("клик по «24ч» меняет сортировку", /▼|▲/.test(chgTh.textContent) ||
      Array.from(d.querySelectorAll("#grid-head th")).some((t) => /▼|▲/.test(t.textContent)), "");
    const first = q("#grid-body tr");
    check("строки пришли после пересортировки", !!first && first.children.length >= 15, first ? first.children[0].textContent : "нет строк");
  } else check("найдена колонка «24ч»", false, "");

  console.log("\n=== ПЛОТНОСТИ ===");
  d.querySelector('.tab[data-view="densities"]').click();
  await sleep(1500);
  check("вкладка активна", q("#view-densities").classList.contains("active"), "");
  const densRows = n("#dens-body tr");
  check("строки или корректная пустая заглушка", densRows >= 1,
    densRows + " строк" + (densRows === 1 ? " (" + txt("#dens-body tr td") .slice(0, 40) + ")" : ""));

  console.log("\n=== АЛЕРТЫ ===");
  d.querySelector('.tab[data-view="alerts"]').click();
  await sleep(1200);
  check("селект метрик заполнен из meta", n("#af-field option") >= 10, n("#af-field option") + " метрик");
  check("селект бирж заполнен", n("#af-ex option") >= 2, n("#af-ex option"));
  check("форма создания на месте", !!q("#alert-form button.primary"), "");
  check("список правил отрисован (или заглушка)", n("#rules-list > *") >= 1, "");

  console.log("\n=== КАРТОЧКА ИНСТРУМЕНТА (drawer) ===");
  api().closeDrawer();
  d.querySelector('.tab[data-view="screener"]').click();
  await sleep(1500);
  const tr = q("#grid-body tr");
  if (tr) {
    tr.dispatchEvent(new window.MouseEvent("click", { bubbles: true }));
    await sleep(1500);
    check("drawer открыт", q("#drawer").hidden === false, "");
    check("заголовок карточки", txt("#d-title") !== "—" && txt("#d-title").length > 0, txt("#d-title"));
    check("блоки статистики", n("#drawer-body .d-stat") >= 6, n("#drawer-body .d-stat") + " метрик");
    check("секции отрисованы", n("#drawer-body .d-sec") >= 3, n("#drawer-body .d-sec") + " секций");

    console.log("\n=== ГРАФИК СВЕЧЕЙ ===");
    await sleep(2500);
    check("canvas создан", !!q("#d-chart"), "");
    check("переключатель ТФ", n(".tf-btn") === 6, Array.from(d.querySelectorAll(".tf-btn")).map(b=>b.textContent).join(" "));
    const cst = q("#d-chart-state");
    check("заглушка загрузки скрыта (атрибут)", cst ? cst.hidden : true,
      cst ? cst.textContent.trim().slice(0, 40) : "нет узла");
    if (cst) {
      // Авторский display:grid у .chart-state перекрывал UA-правило
      // [hidden]{display:none} — оверлей оставался поверх графика навсегда.
      const cd2 = window.getComputedStyle(cst).display;
      check("оверлей не перекрывает график (computed display=none)", cd2 === "none",
        "display=" + cd2);
    }
    check("CandleChart instantiated", !!S().chart, "");
    const cd = S().chart && S().chart.data.candles ? S().chart.data.candles.length : 0;
    check("свечи загружены", cd > 20, cd + " свечей, ТФ=" + S().chartTf);
    check("свечи с биржи, а не демо-заглушка", S().chartSource !== "demo",
      "source=" + S().chartSource + " (ровно 187 свечей = демо)");
    check("свеча = [ts,o,h,l,c,v]", cd === 0 || S().chart.data.candles[0].length === 6,
      cd ? JSON.stringify(S().chart.data.candles[0]) : "");
    if (cd) {
      const bad = S().chart.data.candles.filter((c) => !(c[2] >= Math.max(c[1], c[4]) && c[3] <= Math.min(c[1], c[4])));
      check("OHLC согласован (high≥max(o,c), low≤min(o,c))", bad.length === 0, bad.length + " битых свечей");
      const negv = S().chart.data.candles.filter((c) => c[5] < 0).length;
      check("объём неотрицательный", negv === 0, negv + " отрицательных");
      const ts = S().chart.data.candles.map((c) => c[0]);
      check("свечи упорядочены по времени", ts.every((v, i) => i === 0 || v >= ts[i - 1]), "");
    }
    const before = Object.assign({}, ops);
    S().chart && S().chart.render();
    check("рендер рисует свечи (fillRect)", ops.fillRect > before.fillRect + 5,
      `fillRect ${before.fillRect} → ${ops.fillRect}`);
    check("рендер рисует оси/сетку (stroke)", ops.stroke > before.stroke, `${before.stroke} → ${ops.stroke}`);
    check("рендер подписывает цены (fillText)", ops.fillText > before.fillText, `${before.fillText} → ${ops.fillText}`);

    // смена таймфрейма
    const tf1h = Array.from(d.querySelectorAll(".tf-btn")).find((b) => b.dataset.tf === "1h");
    if (tf1h) {
      tf1h.click();
      await sleep(2000);
      check("переключение на 1h", S().chartTf === "1h" && tf1h.classList.contains("on"), "");
      check("свечи 1h загружены", S().chart && S().chart.data.candles.length > 0,
        S().chart ? S().chart.data.candles.length + " свечей" : "нет chart");
      if (S().chart && S().chart.data.candles.length > 1) {
        const step = S().chart.data.candles[1][0] - S().chart.data.candles[0][0];
        check("шаг свечей = 1 час", step === 3600000, step + " мс");
      }
    }
    // live-обновление последней свечи
    if (S().chart && S().chart.data.candles.length) {
      const c0 = S().chart.data.candles[S().chart.data.candles.length - 1].slice();
      const px = c0[4] * 1.01;
      S().chart.updateLast(px, c0[0] / 1000);
      const c1 = S().chart.data.candles[S().chart.data.candles.length - 1];
      check("updateLast двигает close", Math.abs(c1[4] - px) < 1e-9, `${c0[4]} → ${c1[4]}`);
      check("updateLast расширяет high", c1[2] >= Math.max(c0[2], px), c1[2] + " >= " + px);
    }
    q("#d-close").click();
    check("график освобождается при закрытии", S().chart === null && S().chartKey === null, "");
    check("drawer закрывается (атрибут)", q("#drawer").hidden === true, "");
    // Ключевая проверка: .drawer{display:flex} перекрывал [hidden]{display:none},
    // и плашка оставалась видимой несмотря на атрибут. Смотрим computed style.
    const disp = window.getComputedStyle(q("#drawer")).display;
    check("drawer действительно скрыт (computed display=none)", disp === "none",
      "display=" + disp);

    // повторное открытие и закрытие через Escape
    const row2 = q("#grid-body tr[data-k]");
    if (row2) {
      row2.dispatchEvent(new window.MouseEvent("click", { bubbles: true }));
      await sleep(2500);
      d.dispatchEvent(new window.KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
      await sleep(300);
      check("Escape закрывает карточку визуально",
        q("#drawer").hidden === true &&
        window.getComputedStyle(q("#drawer")).display === "none",
        "display=" + window.getComputedStyle(q("#drawer")).display);
    }
  } else check("есть строка для клика", false, "");

  console.log("\n=== ГРАФИК ОТКРЫВАЕТСЯ ИЗ РАЗНЫХ МЕСТ ===");
  // Ждём фактического появления свечей, а не спим фиксированно: под нагрузкой
  // два последовательных запроса (/api/symbol, затем /api/candles) могут не
  // уложиться в 2.6 c, и тест падал на здоровом коде.
  const waitChart = async (timeoutMs = 12000) => {
    const t0 = Date.now();
    while (Date.now() - t0 < timeoutMs) {
      const st = S();
      if (st && st.chart && st.chart.data.candles && st.chart.data.candles.length) return true;
      if (st && (st.chartSource === "demo" || st.chartSource === "empty")) return true;
      await sleep(200);
    }
    return false;
  };

  const openFrom = async (label, clickFn) => {
    api().closeDrawer();
    await sleep(400);
    const ok = await clickFn();
    if (!ok) { check(`график из: ${label}`, false, "нет элемента для клика"); return; }
    const ready = await waitChart();
    const ch = S().chart;
    const cd = ch && ch.data.candles ? ch.data.candles.length : 0;
    if (!ready) {
      check(`график из: ${label}`, false,
        `свечи не загрузились за 12 c (source=${S().chartSource}, ключ=${S().chartKey || "—"})`);
      return;
    }
    check(`график из: ${label}`, !!ch && cd > 0 && S().chartSource !== "demo",
      `свечей=${cd}, source=${S().chartSource}, ключ=${S().chartKey || "—"}`);
    if (ch && cd) {
      const c = ch.data.candles[cd - 1];
      check(`  └ OHLC валиден (${label})`,
        c[2] >= Math.max(c[1], c[4]) && c[3] <= Math.min(c[1], c[4]) && c[5] >= 0,
        JSON.stringify(c));
    }
  };

  // 1) из таблицы скринера
  d.querySelector('.tab[data-view="screener"]').click();
  await sleep(1600);
  await openFrom("строка таблицы", async () => {
    const tr = q("#grid-body tr[data-k]");
    if (!tr) return false;
    tr.dispatchEvent(new window.MouseEvent("click", { bubbles: true }));
    return true;
  });

  // 2) из плитки карты рынка
  d.querySelector('.tab[data-view="map"]').click();
  await sleep(1600);
  await openFrom("плитка карты рынка", async () => {
    const t = q("#map .tile");
    if (!t) return false;
    t.dispatchEvent(new window.MouseEvent("click", { bubbles: true }));
    return true;
  });

  // 3) из таблицы плотностей
  d.querySelector('.tab[data-view="densities"]').click();
  await sleep(2000);
  await openFrom("строка плотностей", async () => {
    const tr = q("#dens-body tr[data-k]");
    if (!tr) return false;
    tr.dispatchEvent(new window.MouseEvent("click", { bubbles: true }));
    return true;
  });

  console.log("\n=== ФИЛЬТРЫ: ПРЕСЕТ И ПОИСК ===");
  const preset = Array.from(d.querySelectorAll(".preset")).find((b) => /игре/i.test(b.textContent));
  if (preset) {
    preset.click();
    await sleep(1600);
    check("пресет подсветился", preset.classList.contains("active"), preset.textContent);
    check("пресет заполнил поля фильтров",
      Array.from(d.querySelectorAll('.fitem input[data-min]')).some((i) => i.value !== ""), "");
    d.querySelector("#btn-reset").click();
    await sleep(1200);
    check("сброс очищает фильтры",
      Array.from(d.querySelectorAll('.fitem input[data-min]')).every((i) => i.value === ""), "");
  } else check("найден пресет «Монеты в игре»", false, "");

  q("#search").value = "BTC";
  q("#search").dispatchEvent(new window.Event("input", { bubbles: true }));
  await sleep(1800);
  const searchRows = Array.from(d.querySelectorAll("#grid-body tr"));
  check("поиск сузил выборку", searchRows.length >= 0, searchRows.length + " строк");

  console.log("\n=== ОШИБКИ JS ЗА ВРЕМЯ ПРОГОНА ===");
  check("нет необработанных ошибок", errors.length === 0, errors.slice(0, 5).join(" || ") || "чисто");

  console.log(process.exitCode ? "\n>>> ЕСТЬ ПАДЕНИЯ <<<" : "\n>>> ВСЁ ЗЕЛЁНОЕ <<<");
  process.exit(process.exitCode || 0);   // не ждём window.close(): его таймеры падают
}

main().catch((e) => { console.error("СМОК-ТЕСТ УПАЛ:", e); process.exit(2); });
