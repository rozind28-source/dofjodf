"""
HTTP/WebSocket API и точка входа приложения.

REST — снимки по запросу (таблицы, плотности, детали инструмента, CSV-экспорт).
WS   — push-стрим: клиент присылает свои фильтры, сервер раз в push_interval
       отдаёт только то, что попало в выборку, а не весь рынок целиком.
"""
from __future__ import annotations

import asyncio
import csv
import gc
import io
import json
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.requests import Request

from . import filters as F
from .metrics import TF_SECONDS, normalize_candle_volume, resample
from .alerts import AlertEngine, get_engine, init_engine
from .config import SETTINGS
from .focus import clamp_interval, clamp_limit, get_focus, init_focus
from .state import STORE

# пороги детекции спайков берём из конфига, а не из дефолтов метрик
STORE.spike_vol_ratio = SETTINGS.volume_spike_ratio
STORE.spike_tr_ratio = SETTINGS.trades_spike_ratio

log = logging.getLogger("api")
WEB_DIR = Path(__file__).resolve().parent.parent / "web"

# orjson — необязательное ускорение сериализации WS-пуша (в 3-5 раз быстрее
# json.dumps на больших payload). Нет пакета — молча используем stdlib:
# поведение идентично, разница только в CPU.
try:
    import orjson  # type: ignore

    def _dumps(obj: Any) -> str:
        return orjson.dumps(obj).decode()
except ImportError:  # pragma: no cover - зависит от окружения
    orjson = None

    def _dumps(obj: Any) -> str:
        return json.dumps(obj, separators=(",", ":"))


# --------------------------------------------------------------------------
# Выборки
# --------------------------------------------------------------------------
# Кэш сериализованных строк. build_rows() обходит ВСЕ символы и строит словари;
# при 10 000+ инструментов это десятки миллисекунд CPU, а вызывается она на
# каждый WS-push (раз в секунду) и на каждый REST-запрос. Без кэша event loop
# оказывался забит сериализацией, и запросы свечей (/api/grid, /api/candles)
# голодали до видимости «висит». TTL=1 c: свежее, чем период push, не нужно.
_ROWS_CACHE: dict[str, tuple[float, list[dict]]] = {}
ROWS_CACHE_TTL = 1.0

# Живые метрики производительности (см. /api/perf). Смысл: «тормозит» должно
# диагностироваться числами с машины пользователя, а не на глаз.
_PERF: dict[str, float] = {
    "loop_lag_ms": 0.0,        # EWMA задержки event loop (0 = дышит свободно)
    "build_rows_ms": 0.0,      # последняя сериализация всех строк
    "select_ms": 0.0,          # EWMA select() (фильтры+сортировка на пуш)
    "select_calls": 0,
    "select_cache_hits": 0,    # сколько раз select() отдал кэш (клиенты с одинаковым фильтром)
    "push_count": 0,           # сколько батчей отправлено всем клиентам
    "push_ms": 0.0,            # EWMA времени на один батч (select+overview+json)
    "push_payload_kb": 0.0,    # EWMA размера батча — диагностирует «толстый» пуш числами
}


def build_rows(with_densities: bool = False, ttl: float = ROWS_CACHE_TTL) -> list[dict]:
    key = "dens" if with_densities else "plain"
    hit = _ROWS_CACHE.get(key)
    now = time.time()
    if hit and now - hit[0] < ttl:
        return hit[1]
    t0 = time.perf_counter()
    rows = []
    for st in STORE.all():
        if st.last <= 0:
            continue
        rows.append(st.to_row(with_densities=with_densities))
    _PERF["build_rows_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    _ROWS_CACHE[key] = (now, rows)
    return rows


# Кэш select(): каждый WS-клиент раз в секунду гоняет фильтры+сортировку по
# всему снимку рынка (5-15 мс на 6000 строк), а запросы у клиентов обычно
# ОДИНАКОВЫЕ (один и тот же UI). TTL короткий — половина периода пуша:
# свежесть данных всё равно ограничена кэшем build_rows (1 c).
_SELECT_CACHE: dict[tuple, tuple[float, list[dict], dict]] = {}
SELECT_CACHE_TTL = 0.5
SELECT_CACHE_MAX = 64


def _restrict_universe(query: dict[str, Any], restrict: set) -> bool:
    """
    Можно ли сужать выборку до фокус-набора без потери строк.

    Смысл restrict — «не стримить лишнее»: он экономит WS-трафик, когда
    клиент и так хочет весь рынок, а стримится только top-N той же биржи и
    того же типа рынка. Если же в запросе нет фильтра по бирже, набор
    focus-ключей (он всегда с одной биржи) вырезал бы из ответа ВСЕ строки
    остальных бирж: карта рынка схлопывалась бы до чужого топ-50, а при
    несовпадении ключей — до нуля. В таких случаях сужение запрещено:
    отдаём полную вселенную.
    """
    exs = [x.strip().lower() for x in str(query.get("ex", "")).split(",") if x.strip()]
    if len(exs) != 1:
        return False
    wanted = {k.split(":", 1)[0] for k in restrict}
    return all(e.lower() in wanted for e in exs)


def _select_cache_key(query: dict[str, Any], with_densities: bool,
                      restrict: Optional[set]) -> tuple:
    rkey = None
    if restrict is not None:
        rkey = (len(restrict), hash(frozenset(restrict)))
    return (tuple(sorted((str(k), str(v)) for k, v in query.items())),
            with_densities, rkey)


def select(query: dict[str, Any], with_densities: bool = False,
           restrict: Optional[set] = None,
           prefiltered: Optional[list[dict]] = None) -> tuple[list[dict], dict]:
    """
    Фильтрация + сортировка + пагинация. Единая точка для REST и WS.

    restrict — набор ключей, за который нельзя выходить (узкий фокус-пуш:
    отдаём только то, что реально стримится). Никогда не применяем его к
    REST-выборке (/api/screener): там клиент сам выбирает объём среза.

    prefiltered — уже готовые строки (например, весь рынок из build_rows());
    переданы явно → restrict игнорируется: caller хочет полную вселенную,
    сужать её до top-N мы не имеем права.

    Результат кэшируется на SELECT_CACHE_TTL по отпечатку (query, densities,
    restrict): несколько клиентов с одинаковым фильтром (типичный случай)
    платят за выборку один раз в такт, а не по разу каждый.
    """
    if prefiltered is not None:
        restrict = None
    cache_key = _select_cache_key(query, with_densities, restrict)
    now = time.time()
    hit = _SELECT_CACHE.get(cache_key)
    if hit and now - hit[0] < SELECT_CACHE_TTL:
        _PERF["select_cache_hits"] += 1
        return hit[1], hit[2]

    params = F.parse_params(query)
    t0 = time.perf_counter()
    needs_dens = with_densities or bool(params.get("dens"))
    rows = prefiltered if prefiltered is not None \
        else build_rows(with_densities=needs_dens)
    if restrict is not None:
        # restrict — узкий WS-пуш: отдаём только то, что реально стримится
        # (фокус-набор всегда с одной биржи; запросы без фильтра по бирже
        # сюда не доходят — см. ws_stream). Флаг focus_ ставим ВСЕГДА, когда
        # restrict передан: meta может быть прочитана из кэша полной выборки
        # (тот же query без restrict), а клиент по этому флагу решает, резать
        # ли таблицу под фокус.
        rows = [r for r in rows if r["k"] in restrict]
    dup = F.base_universe(rows) if params.get("unique") else None
    rows = F.apply_filters(rows, params, dup)

    sort = str(query.get("sort", "vol"))
    desc = str(query.get("desc", "1")) not in {"0", "false", "no"}
    rows = F.sort_rows(rows, sort, desc)

    try:
        limit = max(1, min(int(query.get("limit", 200)), 5000))
    except (TypeError, ValueError):
        limit = 200

    total = len(rows)
    meta = {"total": total, "sort": sort, "desc": desc, "limit": limit, "universe": len(STORE)}
    if restrict is not None:
        meta["focus"] = True
        meta["focus_count"] = len(restrict)
    dt = (time.perf_counter() - t0) * 1000
    _PERF["select_calls"] += 1
    _PERF["select_ms"] = round(_PERF["select_ms"] * 0.8 + dt * 0.2, 2)
    page = rows[:limit]
    _SELECT_CACHE[cache_key] = (now, page, meta)
    if len(_SELECT_CACHE) > SELECT_CACHE_MAX:
        # чистим разом половину: запросы-«однодневки» (смена фильтра) не должны
        # копить память
        for k in sorted(_SELECT_CACHE, key=lambda k: _SELECT_CACHE[k][0])[:SELECT_CACHE_MAX // 2]:
            del _SELECT_CACHE[k]
    return page, meta


def density_table(query: dict[str, Any], limit: int = 200) -> list[dict]:
    """Топ крупных лимитных плотностей по всему рынку (отдельный раздел UI)."""
    min_usd = float(query.get("min_usd") or SETTINGS.big_density_usd)
    ex = query.get("ex") or ""
    side = query.get("side") or ""
    out: list[dict] = []
    for st in STORE.all():
        if not st.big_densities or st.last <= 0:
            continue
        if ex and st.exchange_label != ex and st.exchange != ex:
            continue
        for d in st.big_densities:
            if d.quote < min_usd:
                continue
            if side and d.side != side:
                continue
            out.append({
                "k": st.key, "s": st.symbol, "exl": st.exchange_label, "mt": st.market_type,
                "p": d.price, "q": round(d.quote, 2), "base": round(d.base, 4),
                "sd": d.side, "d": round(d.dist_pct, 3), "n": d.n_orders,
                "last": st.last, "chg": round(st.change_pct, 2), "vol": round(st.vol24_usd, 2),
            })
    out.sort(key=lambda r: r["q"], reverse=True)
    return out[:limit]


CANDLES_FETCH_TIMEOUT = 8.0   # сек: сколько ждём свечи с биржи прежде чем отдать пустоту


async def _candles_with_timeout(hub, st, tf: str, limit: int) -> list:
    """
    Свечи с жёстким таймаутом.

    Без него медленная биржа (Gate ~19 c на тяжёлые вызовы) или забитый event
    loop превращали /api/candles и /api/grid в вечно висящий запрос: фронтенд
    показывал «загрузка свечей…» бесконечно. Лучше отдать пустой ответ и
    понятный статус, чем держать соединение открытым.
    """
    try:
        return await asyncio.wait_for(
            hub.fetch_candles(st.exchange, st.market_type, st.symbol, tf, limit),
            timeout=CANDLES_FETCH_TIMEOUT)
    except asyncio.TimeoutError:
        log.warning("свечи %s %s: таймаут %.0f c — отдаём пустоту", st.key, tf,
                    CANDLES_FETCH_TIMEOUT)
        return []


def meta_exchanges() -> tuple[list[str], dict[str, dict]]:
    """
    Биржи и их атрибуты для UI, сгруппированные по ЛЕЙБЛУ (имени биржи).

    Лейбл общий для спота и свопа одной биржи: в интерфейсе (как в оригинале)
    чипс выбирает биржу, а рынок переключается отдельным тумблером
    «Фьючерсы / Спот». Поэтому на лейбл приходится список рынков.

    ВАЖНО про семантику `markets`: это рынки, для которых есть КОЛЛЕКТОР
    (то есть куда можно маршрутизировать запрос свечей/сетки). Данные в
    хранилище могут содержать и другие рынки (например, replay-снапшот,
    записанный с более широким профилем) — они видны в таблицах, но тумблер
    не должен предлагать рынок, который нечем обслуживать.

    Источники объединяются: конфиг (что подключено) + хранилище (что реально
    пришло). Иначе в replay списки расходились: чипсы по одному источнику,
    DEX-метки по другому.
    """
    by_label: dict[str, dict] = {}
    for e in SETTINGS.exchanges:
        info = by_label.setdefault(e.label, {
            "ids": {}, "dex": e.dex, "configured": True,
        })
        info["ids"][e.market] = e.id
        info["dex"] = info["dex"] or e.dex
    for info in by_label.values():
        info["markets"] = sorted(info["ids"])
        info["has_spot"] = "spot" in info["markets"]
        info["has_swap"] = "swap" in info["markets"]

    # биржи, которые есть только в данных (replay-снапшот с других бирж/лейблов):
    # показываем их как чипсы-фильтры, но без рынков для маршрутизации
    dex_by_label: dict[str, bool] = {}
    seen: set[str] = set()
    for st in STORE.all():
        seen.add(st.exchange_label)
        dex_by_label[st.exchange_label] = dex_by_label.get(st.exchange_label, False) or st.dex
    for lab in seen:
        if lab in by_label:
            by_label[lab]["dex"] = by_label[lab]["dex"] or dex_by_label.get(lab, False)
            continue
        by_label[lab] = {"ids": {}, "dex": dex_by_label.get(lab, False),
                         "configured": False, "markets": [],
                         "has_spot": False, "has_swap": False}
    return sorted(by_label), by_label


def rows_to_csv(rows: list[dict]) -> str:
    if not rows:
        return ""
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    w.writeheader()
    for r in rows:
        w.writerow({k: (json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v)
                    for k, v in r.items()})
    return buf.getvalue()


# --------------------------------------------------------------------------
# Жизненный цикл
# --------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    engine = init_engine(
        cooldown=SETTINGS.alert_cooldown,
        telegram_token=SETTINGS.telegram_token,
        telegram_chat_id=SETTINGS.telegram_chat_id,
    )
    focus = init_focus(SETTINGS)
    app.state.focus = focus
    tasks = [
        asyncio.create_task(engine.telegram_worker(), name="tg-worker"),
        asyncio.create_task(_alert_loop(engine), name="alert-loop"),
        asyncio.create_task(focus.start(), name="focus-loop"),
        asyncio.create_task(_loop_lag_monitor(), name="perf-monitor"),
        asyncio.create_task(_gc_freeze_after_warmup(), name="gc-freeze"),
    ]

    if SETTINGS.mode == "live":
        from . import collector as _collector
        # явный PROXY_URL из настроек имеет приоритет над переменными окружения
        _collector.SETTINGS_PROXY_OVERRIDE = SETTINGS.proxy_url
        if SETTINGS.proxy_url:
            log.info("прокси из настроек: %s", SETTINGS.proxy_url)
        from .collector import CollectorHub
        hub = CollectorHub(SETTINGS)
        app.state.hub = hub
        focus.attach_hub(hub)
        await hub.start()
        log.info("live mode: %s", ", ".join(e.label for e in SETTINGS.exchanges))
        tasks.append(asyncio.create_task(_startup_health_check(hub), name="startup-check"))
    else:
        from .replay import ReplaySource
        src = ReplaySource(SETTINGS)
        app.state.replay = src
        await src.start()
        log.info("replay mode: %s", SETTINGS.replay_file)

    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        foc = getattr(app.state, "focus", None)
        if foc:
            await foc.stop()
        hub = getattr(app.state, "hub", None)
        if hub:
            await hub.stop()
        rep = getattr(app.state, "replay", None)
        if rep:
            await rep.stop()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _startup_health_check(hub) -> None:
    """
    Если ни одна биржа не подключилась, сервер продолжает работать пустым —
    снаружи это выглядит как «скринер сломался». Сообщаем явно и подсказываем,
    что проверить, вместо того чтобы оставлять пользователя гадать.
    """
    await asyncio.sleep(30)
    if len(STORE) > 0:
        failed = [k for k, v in STORE.exchange_status.items() if v.get("state") == "error"]
        if failed:
            log.warning("не подключились: %s (остальные работают)", ", ".join(failed))
        return

    log.error("=" * 74)
    log.error("НИ ОДНА БИРЖА НЕ ПОДКЛЮЧИЛАСЬ — данных нет, интерфейс будет пустым.")
    log.error("")
    log.error("Причины по убыванию вероятности:")
    log.error("  1. Исходящий трафик блокируется: файрвол, антивирус, корпоративный")
    log.error("     прокси. Падают сразу все биржи — это главный признак.")
    log.error("  2. Биржи блокируют ваш регион (в логе выше будут 403/451/restricted).")
    log.error("     Нужен VPN, либо прокси:")
    log.error('       PowerShell:  $env:HTTPS_PROXY="http://127.0.0.1:1080"; python run.py')
    log.error("       cmd:         set HTTPS_PROXY=http://127.0.0.1:1080 && python run.py")
    log.error("  3. TLS-сертификаты (часто на Windows):  pip install -U certifi")
    log.error("")
    log.error("Точную причину покажет диагностика по слоям (DNS / TLS / HTTP / ccxt):")
    log.error("       python doctor.py")
    log.error("")
    log.error("Посмотреть интерфейс без бирж можно на записанном снимке рынка:")
    log.error("       PowerShell:  $env:MODE='replay'; python run.py")
    log.error("=" * 74)


async def _alert_loop(engine: AlertEngine) -> None:
    await asyncio.sleep(5)
    while True:
        try:
            rows = build_rows()
            if rows:
                await engine.evaluate(rows)
        except Exception as e:  # noqa: BLE001
            log.warning("alert loop: %s", str(e)[:200])
        await asyncio.sleep(1.0)


async def _gc_freeze_after_warmup() -> None:
    """
    gc.freeze(): переносит уже созданные объекты в «постоянное» поколение,
    и сборщики мусора их больше не сканируют.

    Тайминг важен: через ~3 минуты рынки всех бирж загружены (сотни МБ
    метаданных ccxt), первые ротации создали основную массу SymbolState —
    именно этот стабильный массив и выгодно заморозить. Новые символы
    ротации продолжают жить в молодых поколениях и собираются дёшево.
    """
    await asyncio.sleep(180)
    try:
        gc.collect()
        gc.freeze()
        log.info("gc.freeze(): стабильные объекты выведены из сканирования сборщиком")
    except Exception as e:  # noqa: BLE001
        log.debug("gc.freeze не удался: %s", e)


async def _loop_lag_monitor() -> None:
    """
    Измеряет задержку event loop: sleep(0.25) → фактическая пауза минус 0.25.

    Это главный индикатор «сервер захлёбывается»: когда WS-обработчики
    (стаканы/сделки) съедают ядро, loop lag растёт до сотен мс и ВСЕ
    остальные задачи (пуш, REST, алерты) начинают отставать. Значение
    отдаёт /api/perf — можно диагностировать тормоза числами с машины
    пользователя, а не на глаз.
    """
    lag = 0.0
    while True:
        t0 = time.monotonic()
        await asyncio.sleep(0.25)
        d = (time.monotonic() - t0 - 0.25) * 1000.0
        lag = lag * 0.8 + max(0.0, d) * 0.2
        _PERF["loop_lag_ms"] = round(lag, 1)


app = FastAPI(title="Crypto Screener (self-hosted)", version="0.1.0", lifespan=lifespan)


@app.middleware("http")
async def _log_errors(request: Request, call_next):
    """
    Единая точка логирования 5xx.

    Без неё браузер видит только «Failed to load resource: 500», а настоящая
    причина живёт в простыне uvicorn. Пишем одной строкой путь запроса + тип и
    текст исключения, и отдаём понятный JSON — так видно в логе, какой именно
    эндпоинт падает и почему.
    """
    try:
        return await call_next(request)
    except Exception as e:  # noqa: BLE001
        log.exception("HTTP 500 %s %s: %s: %s", request.method,
                      request.url.path, type(e).__name__, str(e)[:300])
        return JSONResponse({"error": f"{type(e).__name__}: {str(e)[:300]}",
                             "path": request.url.path}, status_code=500)


# --------------------------------------------------------------------------
# REST: диагностика состояния приложения для вкладки «Логи»
# --------------------------------------------------------------------------
_LOG_BUF_SIZE = 500
_LOG_BUFFER: list[dict] = []


class _MemoryLogHandler(logging.Handler):
    """Хранит последние N записей лога в памяти для /api/logs."""

    def emit(self, record: logging.LogRecord) -> None:  # noqa: D102
        try:
            _LOG_BUFFER.append({
                "t": time.strftime("%H:%M:%S", time.localtime(record.created)),
                "lvl": record.levelname,
                "src": record.name,
                "msg": record.getMessage()[:500],
            })
            if len(_LOG_BUFFER) > _LOG_BUF_SIZE:
                del _LOG_BUFFER[:-_LOG_BUF_SIZE]
        except Exception:  # noqa: BLE001 - лог-хендлер не должен ронять приложение
            pass


_mem_handler = _MemoryLogHandler()
_mem_handler.setLevel(logging.INFO)
logging.getLogger().addHandler(_mem_handler)


@app.get("/api/logs")
async def api_logs(since: int = 0, lvl: str = ""):
    """Последние сообщения лога (для встроенной панели диагностики)."""
    out = _LOG_BUFFER[since:] if since else list(_LOG_BUFFER)
    if lvl:
        want = lvl.upper()
        out = [x for x in out if x["lvl"] == want or
               (want == "WARN" and x["lvl"] == "WARNING")]
    return {"total": len(_LOG_BUFFER), "logs": out}


# --------------------------------------------------------------------------
# REST: данные
# --------------------------------------------------------------------------
@app.get("/api/overview")
async def api_overview():
    # ttl=1: обход всех символов не чаще раза в секунду — шапка и так
    # обновляется с периодом пуша
    return STORE.overview(ttl=1.0)


@app.get("/api/perf")
async def api_perf():
    """
    Живые метрики производительности — диагностика «тормозит» числами.

    Как читать:
      * loop_lag_ms > 50–100 стабильно → event loop захлёбывается (CPU);
        главные потребители видны в counters (book_updates/ws_messages в сек);
      * build_rows_ms — сколько стоит секундная сериализация всех строк;
      * push_ms — сколько стоит один батч одному клиенту (select+overview+json);
      * counters — накопительные счётчики STORE; дельта за N секунд = скорость.
    """
    ov = STORE.overview(ttl=1.0)
    foc = getattr(app.state, "focus", None)
    hub = getattr(app.state, "hub", None)
    books = None                    # None = не live-режим (считать нечего)
    ws_streams: dict[str, Any] = {}
    if hub is not None:
        try:
            books = sum(len(c._book_tasks) + len(c._batch["book"]["syms"])
                        for c in hub.collectors)
            # диагностика батч-подписок: сколько символов стримится батчами,
            # сколько поточечно, и где батч умер (fallback)
            ws_streams = {
                "batch_symbols": {k: sum(len(c._batch[k]["syms"]) for c in hub.collectors)
                                  for k in ("book", "trades", "tickers")},
                "per_symbol_tasks": {
                    "book": sum(len(c._book_tasks) for c in hub.collectors),
                    "trades": sum(len(c._trade_tasks) for c in hub.collectors),
                    "ticker": sum(len(c._ticker_tasks) for c in hub.collectors),
                },
                "batch_dead": {f"{c.cfg.id}:{c.cfg.market}":
                               [k for k, b in c._batch.items() if b["dead"]]
                               for c in hub.collectors
                               if any(b["dead"] for b in c._batch.values())},
            }
        except Exception:  # noqa: BLE001
            books = -1
    return {
        **_PERF,
        "uptime_s": round(time.time() - STORE.started_at, 1),
        "symbols": ov["symbols"],
        "universe": len(STORE),
        "books_streaming": books,
        "ws_streams": ws_streams,
        "counters": ov["stats"],
        "settings": {
            "push_interval": SETTINGS.push_interval,
            "book_density_interval": SETTINGS.book_density_interval,
            "mode": SETTINGS.mode,
        },
        "json_engine": "orjson" if orjson else "stdlib",
        "focus_enabled": bool(foc and foc.enabled),
        "focus_count": len(foc.key_set) if (foc and foc.enabled) else 0,
    }


@app.get("/api/meta")
async def api_meta():
    """Всё, что нужно фронту для построения UI, одним запросом."""
    return {
        "filters": F.catalog(),
        "sorts": F.sort_catalog(),
        "presets": F.preset_catalog(),
        "exchanges": meta_exchanges()[0],
        "exchange_info": meta_exchanges()[1],
        "market_types": ["spot", "swap"],
        "quotes": SETTINGS.quote_assets,
        "mode": SETTINGS.mode,
        "big_density_usd": SETTINGS.big_density_usd,
        "push_interval": SETTINGS.push_interval,
        "tf": list(SETTINGS.tf_seconds),
        "focus": get_focus().state(),
    }


def _full_market_rows(query: dict[str, Any]) -> list[dict]:
    """
    Строки всего рынка для REST-выборки в фокус-режиме.

    WS-пуш в фокусе намеренно узкий (restrict=key_set — только то, что
    стримится). Но REST (/api/screener, CSV) — это «полная база», из неё
    фронт кормит таблицу и карту рынка. Если бы он тоже резался по key_set,
    вселенная схлопывалась бы до top-N, а при пустом/грязном key_set (сервер
    пережил перезапуск с сохранённым фокусом, старый клиент шлёт base=0) —
    вплоть до нуля строк: интерфейс выглядел бы мёртвым и уходил в DEMO.

    Поэтому берём весь рынок целиком; дешёвые фильтры (биржа, рынок, поиск)
    применяем сразу, чтобы сортировка не гонялась по тысячам лишних строк,
    остальные доделает select() как обычно.
    """
    rows = build_rows()
    params = F.parse_params(query)
    if params.get("ex"):
        # то же регистронезависимое сравнение, что и в filters._passes:
        # UI шлёт лейбл («Binance»), в строках id («binanceusdm») — иначе
        # предфильтр вырезал всю вселенную и REST отдавал только top-N фокуса
        wanted = {x.lower() for x in params["ex"]}
        rows = [r for r in rows
                if (r["ex"] or "").lower() in wanted
                or (r["exl"] or "").lower() in wanted]
    if params.get("mt"):
        rows = [r for r in rows if r["mt"] == params["mt"]]
    q = params.get("q") or params.get("q_base")
    if q:
        rows = [r for r in rows if q in r["s"].upper() or q in (r["b"] or "").upper()]
    return rows


@app.get("/api/screener")
async def api_screener(request: Request):
    """Основная выборка. Все фильтры — обычные query-параметры (см. /api/meta)."""
    qp = dict(request.query_params)
    dens = qp.get("dens") == "1"
    rows, meta = select(qp, with_densities=dens,
                        prefiltered=_full_market_rows(qp))
    return {"rows": rows, "meta": meta}


@app.get("/api/screener.csv")
async def api_screener_csv(request: Request):
    qp = dict(request.query_params)
    rows, _ = select(qp, prefiltered=_full_market_rows(qp))
    body = rows_to_csv(rows)
    return Response(body, media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="screener-{int(time.time())}.csv"'})


@app.get("/api/densities")
async def api_densities(request: Request):
    qp = dict(request.query_params)
    try:
        limit = max(1, min(int(qp.get("limit", 200)), 2000))
    except (TypeError, ValueError):
        limit = 200
    return {"rows": density_table(qp, limit),
            "threshold": float(qp.get("min_usd") or SETTINGS.big_density_usd)}


@app.get("/api/symbol")
async def api_symbol(key: str):
    st = STORE.get(key)
    if not st:
        return JSONResponse({"error": f"инструмент не найден: {key}"}, status_code=404)
    row = st.to_row(with_densities=True)
    row["spark"] = st.sparkline(120)
    row["book"] = {
        # уровни уже нормализованы в apply_book(), но читаем защитно:
        # вдруг биржа подсунет лишние поля (MEXC отдаёт [price, amount, count])
        "bids": [[float(x[0]), float(x[1])] for x in (st.book or {}).get("bids", [])[:25]],
        "asks": [[float(x[0]), float(x[1])] for x in (st.book or {}).get("asks", [])[:25]],
    }
    row["densities"] = [
        {"p": d.price, "q": round(d.quote, 2), "sd": d.side, "d": round(d.dist_pct, 3), "n": d.n_orders}
        for d in st.densities[:30]
    ]
    row["spikes"] = [
        {"kind": s.kind, "ratio": round(s.ratio, 2), "value": round(s.value, 2),
         "base": round(s.base, 2), "age": int(s.age), "ts": s.ts}
        for s in list(st.spikes)[-20:][::-1]
    ]
    row["history"] = [[round(ts, 1), p] for ts, p in st.price_hist.tail(600)]
    return row


@app.get("/api/candles")
async def api_candles(request: Request):
    """
    Свечи для графика инструмента.

    live   → заказываем у коллектора нужной биржи (там кэш на CANDLES_TTL)
    replay → ресэмплим синтетические 1m-свечи в запрошенный таймфрейм

    Заодно отдаём плотности и спайки — график рисует их поверх свечей.
    """
    qp = dict(request.query_params)
    key = qp.get("key") or ""
    tf = qp.get("tf") or "15m"
    if tf not in TF_SECONDS:
        return JSONResponse({"error": f"неподдерживаемый таймфрейм {tf!r}",
                             "supported": sorted(TF_SECONDS)}, status_code=400)
    try:
        limit = max(20, min(int(qp.get("limit", 300)), 1000))
    except (TypeError, ValueError):
        limit = 300

    st = STORE.get(key)
    if not st:
        return JSONResponse({"error": f"инструмент не найден: {key}"}, status_code=404)

    candles: list = []
    hub = getattr(app.state, "hub", None)
    if hub is not None:
        candles = await _candles_with_timeout(hub, st, tf, limit)
    if not candles and st.ohlcv:
        # replay (или биржа не ответила) — собираем из накопленных 1m-свечей
        candles = resample(st.ohlcv, TF_SECONDS[tf])[-limit:]

    candles = normalize_candle_volume(candles, st.inverse, st.contract_size,
                                      st.ohlcv_vol_in_contracts)
    return {
        "key": key, "symbol": st.symbol, "exchange": st.exchange_label,
        "market_type": st.market_type, "tf": tf, "tf_seconds": TF_SECONDS[tf],
        "candles": candles,
        "last": st.last, "bid": st.bid, "ask": st.ask, "chg": round(st.change_pct, 3),
        "natr": round(st.natr, 3) if st.natr else None,
        "densities": [
            {"p": d.price, "q": round(d.quote, 2), "sd": d.side, "d": round(d.dist_pct, 3)}
            for d in st.densities[:25]
        ],
        "spikes": [{"ts": s.ts, "kind": s.kind, "ratio": round(s.ratio, 2)}
                   for s in list(st.spikes)[-40:]],
        "source": "exchange" if hub is not None and candles else "resampled",
    }


@app.get("/api/grid")
async def api_grid(request: Request):
    """
    Данные для сетки графиков (многоэкранный режим, как в оригинале).

    Один запрос возвращает свечи сразу для N инструментов выбранной биржи и
    рынка — иначе сетка из 9 ячеек делала бы 9 отдельных запросов каждые
    несколько секунд и упиралась в rate-limit биржи.

    Параметры: ex (лейбл биржи), mt (spot|swap), tf, n (число ячеек), sort.

    Фокус-режим меняет источник ячеек: вместо «топ по объёму среди всего,
    что случайно попало в горячий набор» берутся монеты текущего отбора —
    они гарантированно стримятся, и свечи для них биржа отдаёт быстро.
    """
    qp = dict(request.query_params)
    focus = get_focus()
    ex = qp.get("ex") or ""
    mt = qp.get("mt") or "swap"
    tf = qp.get("tf") or "5m"
    if tf not in TF_SECONDS:
        return JSONResponse({"error": f"неподдерживаемый tf {tf!r}",
                             "supported": sorted(TF_SECONDS)}, status_code=400)
    try:
        n = max(1, min(int(qp.get("n", 9)), 25))
        limit = max(20, min(int(qp.get("limit", 200)), 400))
    except (TypeError, ValueError):
        n, limit = 9, 200

    # «Графики» = то, что сейчас показывает скринер: те же query-параметры
    # (фильтры + сортировка + поиск), та же сортировка. Фронт присылает их
    # копией строки запроса таблицы + sort/desc из заголовков колонок.
    # build_rows() вызываем ОДИН раз — это самая дорогая операция (сериализация
    # всех символов); фильтрация/сортировка по готовым строкам дешёвые.
    all_rows = build_rows()
    available = sorted({x["exl"] for x in all_rows if x["mt"] == mt})
    focus_on = focus.enabled and focus.spec.mt == mt and (
        not ex or ex in (focus.spec.ex, "") or focus.spec.ex in ("", ex))
    rows = [r for r in all_rows if (not ex or r["exl"] == ex) and r["mt"] == mt]
    rows = F.apply_filters(rows, F.parse_params(qp),
                           F.base_universe(rows) if qp.get("unique") else None)
    rows = F.sort_rows(rows, qp.get("sort", "vol"), qp.get("desc") != "0")[:n]
    if focus_on and not ex:
        ex = focus.spec.ex

    hub = getattr(app.state, "hub", None)

    def build_cell(r: dict, candles: list) -> dict:
        # r["k"] может отсутствовать (dict без ключа или кортеж — если caller
        # ошибся форматом). Раньше это роняло весь /api/grid с TypeError.
        rk = r["k"] if isinstance(r, dict) else None
        st = STORE.get(rk) if rk else None
        if not candles and st and st.ohlcv:
            # биржа не ответила / нет REST-пути (replay) — собираем из 1m-буфера
            candles = resample(st.ohlcv, TF_SECONDS[tf])[-limit:]
        candles = normalize_candle_volume(candles, st.inverse, st.contract_size,
                                          st.ohlcv_vol_in_contracts) if (candles and st) else candles
        return {
            "k": rk or "", "s": r["s"], "b": r["b"], "exl": r["exl"], "mt": r["mt"],
            "dex": r.get("dex", False),
            "last": r["last"], "chg": r["chg"], "vol": r["vol"],
            "natr": r["natr"], "spike": r["spike"],
            "candles": candles,
            "candles_ok": bool(candles),
        }

    if hub is not None:
        # Кэш всей сетки на GRID_TTL + фоновый прогрев 1m-свечей (collector.
        # get_grid/warm_for_grid): раньше каждая плитка каждые 15 с тянула
        # klines через троттл-семафор биржи — при медленных биржах очередь
        # не расходилась за таймаут и плитки рождались пустыми («график
        # пропал»), а сами запросы держали бэкенд занятым минутами.
        # Тикерные поля (цена/изменение) внутри кэша устаревают на десятки
        # секунд — их фронт освежает из WS-потока (gridTick + строки скринера).
        gkey = (tuple(r["k"] for r in rows), ex, mt, tf, n, limit)
        try:
            # ВАЖНО: get_grid принимает СЛОВАРИ строк скринера (в них есть
            # "k"/"mt"), а не кортежи. С кортежем cell-строка теряла ключ и
            # build_cell падал с TypeError («tuple indices must be integers
            # or slices, not str») — весь grid отдавался без свечей.
            cells = await hub.get_grid(gkey, rows, tf, limit, build_cell)
        except Exception as e:  # noqa: BLE001
            # Сетка не должна ронять запрос: без свечей отдаём строки скринера
            # (плитки будут с ценой/объёмом, но пустым графиком до следующего
            # цикла прогрева). Причина падения — в лог одной строкой.
            log.exception("grid: hub.get_grid упал (%s: %s) — отдаём ячейки без свечей",
                          type(e).__name__, str(e)[:200])
            cells = [build_cell(r, []) for r in rows]
    else:
        cells = [build_cell(r, []) for r in rows]
    return {"cells": cells, "tf": tf, "tf_seconds": TF_SECONDS[tf], "mt": mt,
            "ex": ex, "available_exchanges": available,
            "total": len(rows),
            "focus": {"on": bool(focus_on and focus.keys), "count": len(focus.keys),
                      "limit": focus.spec.limit, "interval": focus.spec.interval,
                      "next_in": round(max(0.0, focus.next_at - time.time()), 1)
                      if focus.enabled else None,
                      "note": focus.note if focus_on else ""}}


# --------------------------------------------------------------------------
# REST: фокус-режим (одна биржа + один рынок + top-N по фильтру)
# --------------------------------------------------------------------------
def _focus_payload(data: dict) -> dict:
    """Нормализация тела запроса: мусор на входе не должен ломать отбор."""
    out: dict[str, Any] = {}
    if "ex" in data:
        out["ex"] = str(data.get("ex") or "").strip()
    if data.get("mt") in ("spot", "swap"):
        out["mt"] = str(data["mt"])
    if "limit" in data:
        out["limit"] = clamp_limit(data.get("limit"))
    if "interval" in data:
        out["interval"] = clamp_interval(data.get("interval"))
    if "sort" in data and str(data.get("sort")) in F.SORT_FIELDS:
        out["sort"] = str(data["sort"])
    if "desc" in data:
        out["desc"] = data.get("desc")
    if "pause_others" in data:
        out["pause_others"] = bool(data.get("pause_others"))
    if "dedupe_base" in data:
        out["dedupe_by_base"] = bool(data.get("dedupe_base"))
    q = data.get("query")
    if isinstance(q, dict):
        # те же параметры, что принимает /api/screener — фильтр один на всё
        out["params"] = F.parse_params({str(k): str(v) for k, v in q.items()
                                        if v not in (None, "")})
    return out


@app.get("/api/focus")
async def api_focus_get():
    """Текущий отбор: какие монеты стримятся, когда следующая перепроверка."""
    return get_focus().state()


@app.post("/api/focus")
async def api_focus_set(request: Request):
    """
    Включить/перенастроить фокус-режим.

    Тело: {ex, mt, limit, interval, sort, desc, pause_others, query:{...фильтры}}
    Отбор пересчитывается СРАЗУ, не дожидаясь следующего тика, иначе после
    нажатия кнопки пользователь 15 секунд смотрел бы на пустую сетку.
    """
    focus = get_focus()
    try:
        data = await request.json()
    except Exception:  # noqa: BLE001
        return JSONResponse({"error": "нужен JSON в теле запроса"}, status_code=400)
    if not isinstance(data, dict):
        return JSONResponse({"error": "тело запроса должно быть объектом"}, status_code=400)
    st = await focus.update(**_focus_payload(data))
    if not st["ex"]:
        return JSONResponse({**st, "error": "нет подключённой биржи для фокус-режима"},
                            status_code=409)
    return st


@app.delete("/api/focus")
async def api_focus_off():
    """Выключить: возвращаемся к полному горячему набору всех бирж."""
    return await get_focus().disable()


@app.get("/api/heatmap")
async def api_heatmap(request: Request):
    """
    Данные для «карты рынка»: плитка = инструмент, размер = объём, цвет = изменение.
    Отдаём компактными массивами — так в 5-8 раз меньше трафика, чем JSON-объектами.
    """
    qp = dict(request.query_params)
    params = F.parse_params(qp)
    rows = build_rows()
    dup = F.base_universe(rows) if params.get("unique") else None
    rows = F.apply_filters(rows, params, dup)
    try:
        limit = max(1, min(int(qp.get("limit", 300)), 2000))
    except (TypeError, ValueError):
        limit = 300
    rows = F.sort_rows(rows, "vol", True)[:limit]
    tiles = [
        {
            "k": r["k"], "s": r["s"].split("/")[0], "b": r["b"], "ex": r["exl"],
            "chg": r["chg"], "vol": r["vol"], "last": r["last"],
            "spike": bool(r["spike"]), "natr": r["natr"],
        }
        for r in rows
    ]
    return {"tiles": tiles, "total": len(tiles),
            "min_vol": tiles[-1]["vol"] if tiles else 0,
            "max_vol": tiles[0]["vol"] if tiles else 0}


# --------------------------------------------------------------------------
# REST: алерты
# --------------------------------------------------------------------------
@app.get("/api/alerts")
async def api_alerts_list():
    eng = get_engine()
    return {"rules": eng.list(), "events": eng.recent(100)}


@app.post("/api/alerts")
async def api_alerts_create(request: Request):
    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001
        return JSONResponse({"error": "нужен JSON в теле запроса"}, status_code=400)
    try:
        rule = get_engine().add(**payload)
    except (ValueError, KeyError, TypeError) as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    return {"rule": rule.to_dict()}


@app.patch("/api/alerts/{rid}")
async def api_alerts_update(rid: str, request: Request):
    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001
        payload = {}
    rule = get_engine().update(rid, **payload)
    if not rule:
        return JSONResponse({"error": "правило не найдено"}, status_code=404)
    return {"rule": rule.to_dict()}


@app.delete("/api/alerts/{rid}")
async def api_alerts_delete(rid: str):
    if not get_engine().remove(rid):
        return JSONResponse({"error": "правило не найдено"}, status_code=404)
    return {"ok": True}


# --------------------------------------------------------------------------
# WebSocket
# --------------------------------------------------------------------------
@app.websocket("/ws")
async def ws_stream(ws: WebSocket):
    await ws.accept()
    query: dict[str, Any] = {"sort": "vol", "desc": "1", "limit": "150"}
    dens_mode = False
    # base=1: клиент просит «полную базу»: он сам кормит таблицу из REST, а пуш
    # использует только для живых тиков. Тогда сужать выборку до фокус-набора
    # нельзя — иначе таблица и карта рынка схлопнутся до top-N.
    base_mode = False
    engine = get_engine()
    alert_q = engine.subscribe()
    stop = asyncio.Event()
    ready = asyncio.Event()   # выставится, когда клиент пришлёт свои фильтры

    async def reader() -> None:
        nonlocal query, dens_mode, base_mode
        try:
            while not stop.is_set():
                data = json.loads(await ws.receive_text())
                if data.get("type") == "filters":
                    query = {str(k): str(v) for k, v in (data.get("query") or {}).items()
                             if v not in (None, "")}
                    dens_mode = bool(data.get("densities"))
                    base_mode = bool(data.get("base"))
                    ready.set()
                elif data.get("type") == "ping":
                    await ws.send_text(json.dumps({"type": "pong", "ts": time.time()}))
        except WebSocketDisconnect:
            pass
        except Exception:  # noqa: BLE001
            pass
        finally:
            stop.set()

    async def pusher() -> None:
        """Шлёт выборку с фиксированным интервалом; медленный клиент отваливается сам."""
        interval = SETTINGS.push_interval
        # Ждём первые фильтры клиента, иначе ушёл бы батч с дефолтной выборкой —
        # клиенту пришлось бы его выбросить (и в UI мелькали «не те» строки).
        # Таймаут — чтобы клиент без filters тоже получил данные.
        try:
            await asyncio.wait_for(ready.wait(), timeout=3.0)
        except asyncio.TimeoutError:
            pass
        try:
            while not stop.is_set():
                t0 = time.time()
                tp = time.perf_counter()   # отдельный монотонный таймер для push_ms
                foc = getattr(app.state, "focus", None)
                focus_on = bool(foc and foc.enabled)
                restrict = foc.key_set if (focus_on and not base_mode) else None
                rows, meta = select(query, with_densities=dens_mode, restrict=restrict)
                if focus_on and base_mode:
                    # выборка полная, но число стримов клиенту показывать нужно.
                    # meta могла прийти из кэша select() — мутируем копию
                    meta = {**meta, "focus_count": len(foc.key_set)}
                # overview кэшируем на период пуша: его дёргает КАЖДЫЙ клиент
                # каждую секунду, а обход 6000+ символов стоит ~7 мс
                payload = _dumps(
                    {"type": "rows", "rows": rows, "meta": meta,
                     "overview": STORE.overview(ttl=max(0.5, interval * 0.9)),
                     "ts": time.time()})
                await ws.send_text(payload)
                _PERF["push_count"] += 1
                _PERF["push_payload_kb"] = round(
                    _PERF["push_payload_kb"] * 0.8 + len(payload) / 1024.0 * 0.2, 1)
                dt = (time.perf_counter() - tp) * 1000
                _PERF["push_ms"] = round(_PERF["push_ms"] * 0.8 + dt * 0.2, 2)
                await asyncio.sleep(max(0.1, interval - (time.time() - t0)))
        except Exception:  # noqa: BLE001
            pass
        finally:
            stop.set()

    async def alerter() -> None:
        try:
            while not stop.is_set():
                try:
                    ev = await asyncio.wait_for(alert_q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                await ws.send_text(json.dumps({"type": "alert", "alert": ev}, separators=(",", ":")))
        except Exception:  # noqa: BLE001
            pass
        finally:
            stop.set()

    tasks = [asyncio.create_task(reader()), asyncio.create_task(pusher()), asyncio.create_task(alerter())]
    try:
        await ws.send_text(json.dumps({"type": "hello", "mode": SETTINGS.mode, "ts": time.time()}))
        await stop.wait()
    finally:
        stop.set()
        engine.unsubscribe(alert_q)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


# --------------------------------------------------------------------------
# Фронтенд
# --------------------------------------------------------------------------
@app.get("/")
async def index():
    idx = WEB_DIR / "index.html"
    if idx.exists():
        return FileResponse(idx)
    return JSONResponse({"hint": "frontend не найден; API доступно на /api/*"}, status_code=200)


# Статические файлы фронта отдаём с no-cache: браузеры (и их disk cache)
# иначе держат старую версию скриптов после обновления кода — правки в
# chart.js/app.js «не применяются», пока не сделаешь жёсткое перезагружение.
_NOCACHE = {"Cache-Control": "no-cache, must-revalidate"}


@app.get("/app.js")
async def app_js():
    return FileResponse(WEB_DIR / "app.js", media_type="application/javascript",
                        headers=_NOCACHE)


@app.get("/chart.js")
async def chart_js():
    return FileResponse(WEB_DIR / "chart.js", media_type="application/javascript",
                        headers=_NOCACHE)


@app.get("/style.css")
async def style_css():
    return FileResponse(WEB_DIR / "style.css", media_type="text/css",
                        headers=_NOCACHE)


@app.get("/healthz")
async def healthz():
    ov = STORE.overview()
    return {"ok": ov["symbols"] > 0 or SETTINGS.mode == "replay", **{k: ov[k] for k in ("symbols", "volume_usd", "uptime")},
            "mode": SETTINGS.mode, "exchanges": list(ov["status"].keys())}


if WEB_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")


def create_app() -> FastAPI:
    return app
