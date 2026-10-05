"""
Сборщик рыночных данных.

Одна биржа = один ExchangeCollector = один набор asyncio-тасков:
  * WebSocket (ccxt.pro): БАТЧЕВЫЕ подписки там, где биржа их умеет
    (watch_order_book_for_symbols / watch_trades_for_symbols / watch_tickers —
    одна задача на чанк символов вместо задачи на символ), и поточечные
    watch_ticker / watch_trades / watch_order_book как запасной путь
  * периодический REST-срез тикеров (24h)           — объёмы, high/low, count
  * периодическая подкачка 1m-свечей                — NATR, мульти-ТФ доходности
  * bulk-запрос OI/funding нативным API биржи       — один вызов на всю биржу

Ключевая идея «горячего набора»: физически стримить 18 000 инструментов
невозможно (и не нужно), поэтому мы держим top_n по объёму и раз в
HOT_ROTATE секунд пересобираем список — монеты «в игре» сами всплывают
в стрим, протухшие выпадают.
"""
from __future__ import annotations

import asyncio
import contextlib
import heapq
import json
import logging
import os
import sys
import time
import urllib.parse
import urllib.request
from typing import Optional

import ccxt
import ccxt.pro as ccxtpro

from .config import ExchangeConfig, Settings
from .metrics import set_reference_prices
from .state import STORE

log = logging.getLogger("collector")

HOT_ROTATE = 45.0        # сек: как часто пересчитываем «горячий набор»
# Фокус-режим (см. app/focus.py) отдаёт коллектору готовый список монет и
# ждёт, что подписки изменятся СРАЗУ, а не через HOT_ROTATE. Поэтому ротация
# спит на Event и просыпается по set_focus(). Без этого первые графики после
# включения фокуса появлялись бы только через 45 секунд.
FOCUS_ROTATE_DELAY = 0.4
ROTATE_START_DELAY = 3.0  # сек: даём бирже загрузить тикеры до первой ротации
TRADES_STREAM_N = 120    # сколько символов стримим по сделкам (дельта/CVD)
FUNDING_REFRESH = 30.0   # сек
CANDLES_TTL = 12.0       # сек: время жизни кэша свечей для графиков
MAX_STREAM_FAILS = 5     # подряд идущих отказов биржи → гасим поток по символу
FAIL_WINDOW = 120.0      # сек: окно, внутри которого отказы считаются «подряд»
CORRELATED_FAIL_WINDOW = 2.0   # сек: если все отказы уложились сюда — это обрыв соединения
BAN_TTL = 600.0          # сек: через сколько амнистировать забаненные символы
PAUSED_TICKER_REFRESH = 120.0   # сек: период опроса тикеров у биржи вне фокуса
FOCUS_OHLCV_CONCURRENCY = 24    # параллельных запросов свечей при пересчёте отбора
CANDLES_CACHE_MAX = 512         # записей в кэше свечей: больше — чистим (см. _prune_candles_cache)
# Пауза между порциями фоновой подкачки 1m-свечей (_ohlcv_loop). Без неё
# раунд из сотен klines-запросов забивал REST-очередь биржи, и /api/candles
# отваливался по таймауту («бэкенд не ответил» → фронт уходил в демо).
OHLCV_CHUNK_PAUSE = 1.5         # сек
GRID_TTL = 45.0                 # сек: кэш готовых сеток /api/grid (см. hub.get_grid)
CANDLES_FETCH_TIMEOUT = 8.0    # сек: ждём klines у биржи прежде отдать пустоту (api._candles_with_timeout дублирует осознанно)
GRID_CACHE_MAX = 24             # сколько разных сеток держать (ex×mt×tf×n×порядок)

# --- батчевые WS-подписки -------------------------------------------------
# Одна задача на чанк символов вместо задачи на символ: при горячем наборе
# 300 монет это ~320 корутин и (у Binance) до 50 WS-соединений на биржу,
# которые превращаются в 3 задачи и 3 соединения. Методы ccxt.pro есть не у
# всех бирж (gate — без батчевых стаканов, mexc/hyperliquid — без батчевых
# сделок), поэтому поточечный путь сохранён как запасной — он же включается,
# если батч-подписка стабильно отказывает (изоляция плохих символов).
BATCH_CHUNK = 100        # символов в одной батч-подписке (лимит ccxt для binance — 200, берём с запасом)
BATCH_CAP = {"book": "watchOrderBookForSymbols",
             "trades": "watchTradesForSymbols",
             "tickers": "watchTickers"}
BATCH_UNWATCH = {"book": "un_watch_order_book_for_symbols",
                 "trades": "un_watch_trades_for_symbols",
                 "tickers": "un_watch_tickers"}
# Префиксы, из которых ccxt (binance) строит идентификатор потока в
# options['streamBySubscriptionsHash'] — нужны, чтобы возвращать счётчики
# подписок при пересборке списка (см. _release_stream_counters).
BATCH_STREAM_HASH = {"book": "multipleOrderbook",
                     "trades": "multipleTrades",
                     "tickers": "miniTicker"}

# Binance-совместимый bulk-эндпоинт funding (поле lastFundingRate по всем контрактам)
# Точечный OI через нативный API — там, где ccxt fetchOpenInterest не реализовал.
# Обе биржи отвечают в Binance-формате: {"symbol":..., "openInterest": "<в базовой монете>"}
NATIVE_OI_URL = {
    "binanceusdm": "https://fapi.binance.com/fapi/v1/openInterest?symbol=",
    "aster": "https://fapi.asterdex.com/fapi/v1/openInterest?symbol=",
}

PREMIUM_INDEX_URLS = {
    "binanceusdm": "https://fapi.binance.com/fapi/v1/premiumIndex",
    "aster": "https://fapi.asterdex.com/fapi/v1/premiumIndex",
}
SNAPSHOT_EVERY = 300.0   # сек: дамп снапшота на диск (для replay-режима)


# --------------------------------------------------------------------------
# Классификация ошибок загрузки: от неё зависит, повторять ли запрос
# --------------------------------------------------------------------------
_OPTION_ERROR_HINTS = ("fetchMarkets", "is not a supported market", "not supported")


def is_option_error(e: BaseException) -> bool:
    """Биржа отвергла саму опцию fetchMarkets (а не сеть)."""
    if not isinstance(e, (ccxt.ExchangeError, ccxt.NotSupported)):
        return False
    if isinstance(e, (ccxt.NetworkError, ccxt.DDoSProtection, ccxt.RateLimitExceeded)):
        return False
    return any(h.lower() in str(e).lower() for h in _OPTION_ERROR_HINTS)


def is_network_error(e: BaseException) -> bool:
    """Сетевой уровень: DNS, TLS, прокси, таймаут, геоблокировка."""
    return isinstance(e, (ccxt.NetworkError, ccxt.RequestTimeout, ccxt.DDoSProtection))


def _root_cause(e: BaseException) -> Optional[BaseException]:
    """
    Первопричина из цепочки __cause__/__context__.

    ccxt маскирует сетевые ошибки так:
        raise ExchangeNotAvailable(' '.join([id, method, url])) from e
    — в сообщении остаётся ТОЛЬКО URL, а настоящая причина (SSL-ошибка,
    сброс соединения, отказ прокси, gaierror) уходит в сцеплённое исключение.
    Без размотки цепочки все отказы выглядели одинаково («ExchangeNotAvailable:
    <биржа> GET <url>»), и диагноз терялся и в логах, и в doctor.py.
    """
    seen = {id(e)}
    cur = e.__cause__ or e.__context__
    root: Optional[BaseException] = None
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        root = cur
        cur = cur.__cause__ or cur.__context__
    return root


def describe_error(e: BaseException) -> str:
    """
    Человекочитаемая причина. ccxt кладёт в сообщение URL, а настоящую причину
    (getaddrinfo failed, CERTIFICATE_VERIFY_FAILED, 403, tunnel connection failed)
    — в конец строки ИЛИ в __cause__ (см. _root_cause), поэтому короткая обрезка
    показывала только URL.
    """
    raw = " ".join(str(e).split())
    tail = raw[-220:] if len(raw) > 220 else raw
    root = _root_cause(e)
    cause_txt = ""
    if root is not None:
        rtxt = " ".join(str(root).split())
        if rtxt and rtxt != raw:
            cause_txt = f" ← {type(root).__name__}: {rtxt[:200]}"
    hint = ""
    low = (raw + " " + type(e).__name__
           + (" " + type(root).__name__ + " " + str(root) if root is not None else "")).lower()
    if "could not contact dns servers" in low or "clientconnectordnserror" in low:
        # Отдельный класс: системный DNS (getaddrinfo) работает, а aiohttp с
        # aiodns шлёт UDP-запросы к DNS-серверу напрямую и они блокируются.
        # Проверяется ПЕРЕД ssl-веткой: в сообщении ClientConnectorDNSError
        # есть «ssl:default», и без порядка веток причина терялась бы.
        hint = (" — асинхронный DNS-резолвер aiohttp (aiodns) не может достучаться "
                "до DNS-сервера; приложение автоматически использует системный "
                "резолвер (ThreadedResolver), если всё равно не работает — "
                "смените DNS на 1.1.1.1")
    elif "getaddrinfo" in low or "name or service not known" in low or "nodename nor servname" in low:
        hint = " — не резолвится DNS (нет интернета или проблема с DNS)"
    elif "certificate" in low or "ssl" in low or "cert" in low:
        hint = " — проблема с TLS-сертификатами (частый случай на Windows: обновите certifi)"
    elif "tunnel" in low or "proxy" in low or "407" in low:
        hint = " — требуется прокси (задайте HTTPS_PROXY)"
    elif "451" in raw or "403" in raw or "restricted" in low or "not available in your" in low:
        hint = " — биржа блокирует ваш регион (нужен VPN)"
    elif "timed out" in low or "timeout" in low:
        hint = " — таймаут соединения (файрвол/антивирус может блокировать исходящие)"
    elif "actively refused" in low or "connection refused" in low:
        hint = " — соединение отклонено (файролл/антивирус)"
    elif ("forcibly closed" in low or "connection reset" in low or "10054" in low
          or "server disconnected" in low):
        hint = " — соединение оборвала удалённая сторона (DPI/файрвол/антивирус рвёт TCP; на Windows так же выглядит работа через заблокированный прямой выход при включённом системном прокси)"
    return f"{type(e).__name__}: {tail}{cause_txt}{hint}"


# --------------------------------------------------------------------------
# Прокси
# --------------------------------------------------------------------------
PROXY_ENV = ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy")


def proxy_from_env() -> Optional[str]:
    for k in PROXY_ENV:
        v = os.getenv(k)
        if v:
            return v.strip()
    return None


def apply_proxy(opts: dict) -> Optional[str]:
    """
    Проксирование REST и WebSocket одновременно.

    ccxt требует РОВНО ОДНУ настройку на слой, иначе InvalidProxySettings
    «multiple conflicting proxy settings» (проверено живым прогоном на 4.5.85):
      * REST → `httpsProxy`: все URL бирж https, ccxt передаёт значение в
        aiohttp `proxy=` (настоящий forward-прокси, не префикс URL);
      * WS   → `wssProxy`: потоки wss, их ccxt читает отдельной группой
        атрибутов wsProxy/wssProxy/wsSocksProxy через check_ws_proxy_settings().

    Прежняя версия ставила `httpProxy` И `httpsProxy` вместе — современный
    ccxt отвергает эту комбинацию сразу, т.е. PROXY_URL ломал ВСЕ биржи.
    `aiohttp_trust_env` оставлен как страховка для HTTPS_PROXY из окружения.
    """
    url = SETTINGS_PROXY_OVERRIDE or proxy_from_env()
    if not url:
        _warn_windows_system_proxy()
        return None
    opts["aiohttp_trust_env"] = True
    opts["httpsProxy"] = url      # REST (https)
    opts["wssProxy"] = url        # WebSocket (wss)
    log.info("используется прокси %s (REST: httpsProxy, WS: wssProxy)", url)
    return url


_warned_system_proxy = False


def _warn_windows_system_proxy() -> None:
    """
    Одноразовое предупреждение про системный прокси Windows.

    Ловушка, в которую попал пользователь: VPN-приложение (Clash, v2rayN,
    Outline и т.п.) включает системный прокси в реестре Windows. Браузер и
    urllib (через getproxies) ходят через него, а aiohttp/ccxt — напрямую,
    и прямой выход заблокирован. Снаружи это выглядит как «все биржи
    ExchangeNotAvailable за 0.3 c», хотя «прямые» запросы в докторе проходят.
    """
    global _warned_system_proxy
    if sys.platform != "win32" or _warned_system_proxy:
        return
    _warned_system_proxy = True
    try:
        from .preflight import windows_system_proxy
        enabled, server, pac = windows_system_proxy()
    except Exception:
        return
    if enabled and server:
        log.warning(
            "в Windows включён системный прокси (%s), но PROXY_URL/HTTPS_PROXY не заданы: "
            "браузер и urllib ходят через него, а ccxt подключается напрямую и не "
            'подключается. Исправление:  $env:PROXY_URL="%s"; python run.py',
            server, server)
    elif pac:
        log.warning(
            "в Windows задано автоматическое конфигурирование прокси (PAC: %s), но "
            "PROXY_URL не задан. Если биржи не подключаются — пропишите порт локального "
            'VPN-прокси вручную:  $env:PROXY_URL="http://127.0.0.1:PORT"; python run.py',
            pac)


# --------------------------------------------------------------------------
# DNS-резолвер aiohttp
# --------------------------------------------------------------------------
_dns_resolver_logged = False


def apply_dns_resolver() -> str:
    """
    Переключить aiohttp на системный DNS-резолвер (ThreadedResolver).

    Реальный случай с Windows: aiohttp по умолчанию при установленном aiodns
    (ccxt.pro ставит его всегда) использует AsyncResolver — тот шлёт UDP-запросы
    НАПРЯМУЮ к DNS-серверам из настроек адаптера, минуя службу DNS-клиента
    Windows (dnscache). Антивирус, файрвол, правила VPN-приложений и
    «шифрованный DNS» (DoH) этот путь нередко блокируют, и тогда ВСЕ биржи
    мгновенно падают с:

        ClientConnectorDNSError: Cannot connect to host ...
        [Could not contact DNS servers]

    при этом браузер, urllib и socket.getaddrinfo работают нормально (шаг 4
    doctor зелёный, шаг 5 красный на всех биржах).

    ThreadedResolver выполняет тот же getaddrinfo в пуле потоков — системный
    путь, который работает всегда, когда работает сеть. Хостов у нас десяток,
    ОС кэширует ответы, поэтому разницы в производительности нет. Патч
    глобальный на процесс: TCPConnector берёт DefaultResolver из модуля
    aiohttp.connector в момент создания, так что он покрывает и REST, и
    WebSocket ccxt (WS использует ту же aiohttp-сессию биржи).

    DNS_RESOLVER=aiodns в .env возвращает дефолтное поведение aiohttp.
    """
    global _dns_resolver_logged
    mode = (os.getenv("DNS_RESOLVER", "") or "threaded").strip().lower()
    if mode in ("aiodns", "async", "default"):
        return "aiodns"
    try:
        import aiohttp.connector
        import aiohttp.resolver
    except Exception:
        return "threaded"
    if aiohttp.connector.DefaultResolver is not aiohttp.resolver.ThreadedResolver:
        aiohttp.connector.DefaultResolver = aiohttp.resolver.ThreadedResolver
        if not _dns_resolver_logged:
            log.info("aiohttp: DNS-резолвер переключён на системный (ThreadedResolver)")
    _dns_resolver_logged = True
    return "threaded"


# явное переопределение из настроек (PROXY_URL), имеет приоритет над окружением
SETTINGS_PROXY_OVERRIDE: str = ""


class ExchangeCollector:
    def __init__(self, cfg: ExchangeConfig, settings: Settings) -> None:
        self.cfg = cfg
        self.settings = settings
        cls = getattr(ccxtpro, cfg.id)
        opts = {
            "enableRateLimit": True,
            "timeout": settings.request_timeout,
            "options": dict(cfg.options),
        }
        if cfg.market == "swap":
            opts["options"]["defaultType"] = "swap"
        # ограничиваем кэш сделок ccxt: иначе на каждой бирже копится по 1000
        # сделок на символ и память растёт на сотни мегабайт
        opts["options"].setdefault("tradesLimit", cfg.trades_limit)
        # Системный DNS-резолвер вместо aiodns: на Windows прямой UDP к
        # DNS-серверу часто блокируют (см. apply_dns_resolver). Патч глобальный
        # и идемпотентный, применяется до создания сессий aiohttp.
        apply_dns_resolver()
        apply_proxy(opts)
        self.ex = cls(opts)   # ccxt.pro умеет и WebSocket, и обычный REST

        self.symbols: list[str] = []          # полный отфильтрованный список биржи
        self.hot: list[str] = []              # активный стрим
        self.tasks: set[asyncio.Task] = set()
        self._stop = asyncio.Event()
        self._book_tasks: dict[str, asyncio.Task] = {}
        self._trade_tasks: dict[str, asyncio.Task] = {}
        self._ticker_tasks: dict[str, asyncio.Task] = {}
        # WS-подписки на kline для сетки графиков (см. watch_candles)
        self._candle_tasks: dict[str, asyncio.Task] = {}
        # tf каждого kline-потока: без этого un_watch уходил с НЕТОПОВЫМ tf
        # (топик на бирже оставался висеть), а watch того же символа на новом
        # tf натыкался на уже зарегистрированный messageHash → «already
        # subscribed»/вечно висящий future → пустые плитки при смене отбора/tf
        self._candle_tfs: dict[str, str] = {}
        # целевой набор символов сетки графиков (последний watch_candles):
        # keep-список для паузы — kline-потоки сетки не закрываем (см. _rotate_loop)
        self._grid_syms: set[str] = set()
        self._id_map: dict[str, str] = {}
        self._sem_ohlcv = asyncio.Semaphore(settings.ohlcv_concurrency)
        # Троттл ФОНОВЫХ REST-запросов (подкачка 1m-свечей). Раньше фоновый
        # цикл и интерактивные запросы графиков делили одну очередь на 6
        # слотов: стартующий раунд из сотен klines ставил /api/candles в конец
        # очереди, ответ не успевал за 8 c — фронт показывал «бэкенд не
        # ответил» и уходил в демо. Теперь у срочных путей своей очереди нет
        # (они идут напрямую), а фоновая подкачка дополнительно прорежена
        # OHLCV_CHUNK_PAUSE и идёт через этот узкий семафор.
        self._sem_rest = asyncio.Semaphore(max(2, settings.ohlcv_concurrency // 2))
        # Отбору по волатильности нужны свечи сразу на весь пул кандидатов
        # (до 240 штук), и первый пересчёт с общим семафором (6) занимал 9.3 c -
        # UI всё это время держал POST /api/focus открытым. Отдельный семафор
        # шире: klines-эндпоинты бирж это переносят спокойно, а время первого
        # отбора падает в разы.
        self._sem_focus = asyncio.Semaphore(FOCUS_OHLCV_CONCURRENCY)
        self._sem_oi = asyncio.Semaphore(4)   # троттлинг точечных OI/funding-запросов
        self._candles_cache: dict[tuple, tuple[float, list]] = {}
        self._book_banned: set[str] = set()   # символы, по которым биржа стабильно отказывает
        self._ban_ts: float = time.time()
        self._book_limit_suspect: bool = False
        self._t0 = time.time()
        # --- фокус-режим ---
        self._focus_syms: Optional[list[str]] = None   # жёсткий список стрима
        self._focus_pool: Optional[list[str]] = None   # пул для подкачки свечей
        self._focus_wake = asyncio.Event()             # «список изменился, ротация»
        self._paused = False                           # биржа не в фокусе → спим
        # --- батчевые подписки (см. BATCH_CAP) ---
        # kind -> {syms: текущий список, tasks: задачи по чанкам, gen: поколение
        #          (задачи старого поколения сами выходят), dead: батч не удался,
        #          работаем поточечно}
        self._batch: dict[str, dict] = {
            k: {"syms": [], "tasks": [], "gen": 0, "dead": False,
                "chunk": cfg.batch_chunk, "rebuild": False}
            for k in ("book", "trades", "tickers")
        }

    # ------------------------------------------------------------------
    async def run(self) -> None:
        """
        Запуск коллектора биржи.

        Если рынки не загрузились, процесс НЕ останавливается: переходим в
        режим восстановления и периодически повторяем попытку. Транзиентный
        сбой DNS или кратковременная недоступность биржи не должны требовать
        ручного перезапуска скринера.
        """
        if not await self._load_markets():
            await self._revive_loop()
            return

        await self._after_markets_loaded()

    async def _load_markets(self) -> bool:
        """
        Загрузка метаданных рынков.

        ccxt по умолчанию тянет ВСЕ типы рынков биржи: у Gate это 7166 объектов
        (хотя свопов только 1025), у OKX — 4389, у Binance Spot — 4670. Только
        метаданные рынков на 8 биржах съедают ~380 МБ, и это главный потребитель
        памяти в приложении (история цен на 6000 символов — всего ~70 МБ).

        Ограничение `options.fetchMarkets` резко это режет (OKX: 4389 → 500),
        но реализовано у бирж непоследовательно:
          binanceusdm, bybit → ExchangeError
          hyperliquid        → возвращает 0 рынков (тихая поломка!)
        Поэтому пробуем с ограничением и откатываемся, если рынков нужного
        типа не оказалось. Молча остаться с пустым списком нельзя.
        """
        want = self.cfg.market
        # Значение по умолчанию у каждой биржи своё (у Hyperliquid — ["spot","swap"]).
        # Удалять ключ в откате нельзя: ccxt останется без ориентира и вернёт
        # 0 рынков. Только сохраняем и восстанавливаем исходное.
        sentinel = object()
        original = self.ex.options.get("fetchMarkets", sentinel)

        async def try_load(restrict: bool) -> int:
            if restrict:
                self.ex.options["fetchMarkets"] = [want]
            elif original is sentinel:
                self.ex.options.pop("fetchMarkets", None)
            else:
                self.ex.options["fetchMarkets"] = original
            await self.ex.load_markets(reload=True)
            key = "swap" if want == "swap" else "spot"
            return sum(1 for m in self.ex.markets.values() if m.get(key))

        option_rejected = False
        for restrict in (True, False):
            try:
                got = await self._try_load_with_retries(try_load, restrict)
            except Exception as e:  # noqa: BLE001
                msg = describe_error(e)
                # Повторять без ограничения имеет смысл ТОЛЬКО если биржа
                # отвергла саму опцию. При сетевой ошибке второй запрос —
                # это просто потерянное время и вдвое более запутанный лог:
                # причина не в опции, а в доступности API.
                if restrict and is_option_error(e):
                    option_rejected = True
                    continue
                level = log.warning if is_network_error(e) else log.error
                level("[%s] load_markets не удался: %s", self.cfg.label, msg)
                STORE.set_status(self.cfg.label, state="error", error=msg[:300])
                STORE.bump("errors")
                return False
            if got > 0:
                if restrict:
                    log.info("[%s] рынков %s: %d (всего метаданных %d)",
                             self.cfg.label, want, got, len(self.ex.markets))
                elif option_rejected:
                    # ограничение отвергла сама биржа — это нормально, просто
                    # памяти уйдёт больше (ccxt тянет метаданные всех рынков)
                    log.info("[%s] fetchMarkets=[%s] не поддержан — загружены все рынки, "
                             "нужного типа %d (метаданных %d)",
                             self.cfg.label, want, got, len(self.ex.markets))
                else:
                    log.info("[%s] fetchMarkets=[%s] не дал рынков этого типа — "
                             "загружены все (%d), нужного типа %d",
                             self.cfg.label, want, len(self.ex.markets), got)
                return True
        log.warning("[%s] рынков типа %s не найдено", self.cfg.label, want)
        STORE.set_status(self.cfg.label, state="error", error=f"нет рынков типа {want}")
        return False

    async def _try_load_with_retries(self, try_load, restrict: bool) -> int:
        """
        Повторяет load_markets с нарастающей задержкой.

        Зачем: у Gate загрузка рынков занимает ~19 c, у Hyperliquid ~16 c,
        а дефолтный таймаут ccxt — 10 c на запрос. На нестабильном канале
        отдельный запрос периодически не укладывается, и без ретраев биржа
        оставалась неподключённой до перезапуска процесса.

        Сетевые ошибки повторяем; ошибки конфигурации (неверная опция,
        неподдерживаемый рынок) — нет, они от повтора не пройдут.
        """
        attempts = max(1, self.settings.load_retries)
        delay = 2.0
        last: Optional[BaseException] = None
        for attempt in range(1, attempts + 1):
            try:
                return await try_load(restrict)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                last = e
                if not is_network_error(e):
                    raise                      # не сеть — повторять бессмысленно
                if attempt < attempts:
                    log.info("[%s] load_markets: попытка %d/%d не удалась (%s), "
                             "повтор через %.0f c",
                             self.cfg.label, attempt, attempts,
                             describe_error(e)[:110], delay)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 20.0)
        if last:
            raise last
        return 0

    async def _revive_loop(self) -> None:
        """Периодически повторяет подключение биржи, отвалившейся на старте."""
        interval = max(15.0, self.settings.revive_interval)
        log.warning("[%s] биржа не подключилась — буду повторять каждые %.0f c "
                    "(остальные биржи работают)", self.cfg.label, interval)
        STORE.set_status(self.cfg.label, state="retrying")
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
                return                          # поступил сигнал остановки
            except asyncio.TimeoutError:
                pass
            if await self._load_markets():
                log.info("[%s] подключение восстановлено", self.cfg.label)
                await self._after_markets_loaded()
                return
        return

    async def _after_markets_loaded(self) -> None:
        """Запуск рабочих циклов — вынесено, чтобы его мог вызвать и revive."""
        self.symbols = self._select_symbols()
        STORE.set_status(self.cfg.label, state="online", symbols=len(self.symbols),
                         total=len(self.symbols))
        log.info("[%s] %d symbols (%s)", self.cfg.label, len(self.symbols), self.cfg.market)

        self._spawn(self._ticker_loop(), name="tickers")
        self._spawn(self._rotate_loop(), name="rotate")
        if self.settings.fetch_ohlcv:
            self._spawn(self._ohlcv_loop(), name="ohlcv")
        self._spawn(self._funding_loop(), name="funding")
        # снапшот пишем только в live-режиме: иначе replay перезаписал бы
        # боевой снимок рынка своей синтетикой
        if self.settings.mode == "live":
            self._spawn(self._snapshot_dump_loop(), name="snapshot")

        await self._stop.wait()
        for t in list(self.tasks):
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        with contextlib.suppress(Exception):
            await self.ex.close()

    def _spawn(self, coro, name: str) -> asyncio.Task:
        t = asyncio.create_task(coro, name=f"{self.cfg.id}:{name}")
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)
        return t

    async def stop(self) -> None:
        self._stop.set()

    # ------------------------------------------------------------------
    # Фокус-режим: внешний жёсткий список стрима
    # ------------------------------------------------------------------
    def set_focus(self, symbols: Optional[list[str]], pool: Optional[list[str]] = None) -> bool:
        """
        Переводит коллектор в фокус-режим (список) или возвращает в обычный (None).

        Возвращает True, если состав изменился и подписки надо пересобрать.
        Список копируется: владелец (FocusManager) свой может менять на месте.
        """
        new = list(symbols) if symbols else None
        changed = (new or []) != (self._focus_syms or [])
        self._focus_syms = new
        self._focus_pool = list(pool) if pool else (list(new) if new else None)
        if changed:
            self._focus_wake.set()
        return changed

    def set_ohlcv_pool(self, pool: Optional[list[str]]) -> None:
        """
        Пул кандидатов фокус-отбора (уровень 2 по волатильности).

        Свечи пулу добирает сам FocusManager (ensure_ohlcv — порционно, из
        бюджета пересчёта, только протухшие). Фоновый _ohlcv_loop опрашивает
        исключительно отобранный набор, поэтому здесь пул хранится для
        диагностики и тестов, а не для постоянного polling'а.
        """
        self._focus_pool = list(pool) if pool else None

    def pause(self) -> bool:
        """
        Биржа НЕ в фокусе: гасим тяжёлые потоки, опрос тикеров замедляем.

        Тикеры остаются — таблица скринера и карта рынка живут именно на них,
        а стоят один REST-запрос на биржу раз в PAUSED_TICKER_REFRESH секунд.
        """
        if self._paused:
            return False
        self._paused = True
        self._focus_wake.set()
        log.info("[%s] пауза: биржа вне фокуса, потоки закрыты", self.cfg.label)
        return True

    def resume(self) -> bool:
        if not self._paused:
            return False
        self._paused = False
        self._focus_wake.set()
        log.info("[%s] возобновление стримов", self.cfg.label)
        return True

    @property
    def focus_mode(self) -> bool:
        return bool(self._focus_syms)

    @property
    def paused(self) -> bool:
        return self._paused

    def _close_streams(self, keep_candles: set | None = None) -> None:
        """
        Закрыть все WS-потоки; отписки на бирже — фоново.

        keep_candles: символы, чьи kline-потоки сетки графиков ТРОГАТЬ НЕЛЬЗЯ
        (пауза биржи вне фокуса не должна обесценивать плитки «Графиков»).
        Поток живёт в _candle_tasks и сам засыпает во время паузы (_paused →
        sleep), подписка на бирже остаётся в силе — данные придут сразу после
        resume без переподписки и без «already subscribed».

        Раньше каждый un_watch_* ожидался здесь последовательно, и пауза биржи
        со 150 подписками (~130 отписок по ~0.3 c) блокировала ротацию на
        ~40 секунд: сигнал resume() приходил, пока цикл был занят, и биржа
        возвращалась к работе только через HOT_ROTATE. Живой замер: через 15 c
        после выключения фокуса Binance всё ещё стримил 50 монет вместо 150.

        Поточечные подписки при этом сбрасываются в ccxt ПОЛНОСТЬЮ (не только
        un_watch_*): у bybit/okx watch_*_for_symbols переиспользуют одно
        соединение, и если resume() переподпишет топик раньше, чем долетит
        фоновая отписка, биржа отвечает «already subscribed» лавиной ошибок
        (живой случай: bybit orderbook.200.BTCUSDT каждые 15 с ротации).
        Отписка + очистка messageHashes живут в одной фоновой задаче, а
        _sync_batch перед повторной подпиской ждёт её завершения.
        """
        books = sorted(self._book_tasks)
        trades = sorted(self._trade_tasks)
        tickers = sorted(self._ticker_tasks)
        for d in (self._book_tasks, self._trade_tasks, self._ticker_tasks):
            for t in list(d.values()):
                t.cancel()
            d.clear()
        for kind, b in self._batch.items():
            for t in b["tasks"]:
                t.cancel()
            b["tasks"] = []
            b["gen"] += 1          # задачи старого поколения сами выйдут, если отменятся не сразу
            if b["syms"]:
                self._detach_batch_unwatch(kind, b["syms"])
                b["syms"] = []
        if books or trades or tickers:
            keep = keep_candles or set()
            self._pending_unwatch = asyncio.create_task(
                self._drop_single_all(books, trades, tickers, keep),
                name=f"{self.cfg.id}:unwatch-all")
            self.tasks.add(self._pending_unwatch)
            self._pending_unwatch.add_done_callback(self.tasks.discard)

    async def _drop_single_all(self, books: list[str], trades: list[str],
                               tickers: list[str],
                               keep_candles: set | None = None) -> None:
        await self._drop_single_subscriptions("book", books)
        await self._drop_single_subscriptions("trades", trades)
        await self._drop_single_subscriptions("tickers", tickers)
        # kline-потоки сетки графиков — те же правила пересборки подписок,
        # но символы из keep_candles остаются подписанными (пауза биржи не
        # должна обрывать живые графики /api/grid)
        keep = keep_candles or set()
        candles = sorted(s for s in self._candle_tasks if s not in keep)
        for sym in candles:
            t = self._candle_tasks.pop(sym, None)
            if t:
                t.cancel()
        if candles:
            await self._drop_single_subscriptions("candles", candles)

    async def ensure_ohlcv(self, symbols: list[str], limit: int = 400) -> int:
        """
        Принудительно добить 1m-свечи списку символов (вне цикла _ohlcv_loop).

        Нужно отбору по волатильности: кандидаты ещё не входят в горячий набор,
        их st.ohlcv пуст, NATR=None — и фильтр молча отбросил бы их все.
        Возвращает число символов, которым свечи реально обновили.
        """
        ok = 0

        async def one(sym: str) -> bool:
            async with self._sem_focus:
                try:
                    candles = await self.ex.fetch_ohlcv(sym, "1m", limit=limit)
                    STORE.bump("rest_calls")
                    if candles:
                        self._state(sym).apply_ohlcv(candles)
                        return True
                except (ccxt.RateLimitExceeded, ccxt.DDoSProtection):
                    await asyncio.sleep(1.5)
                except Exception as e:  # noqa: BLE001
                    STORE.bump("errors")
                    log.debug("[%s] focus ohlcv %s: %s", self.cfg.label, sym, str(e)[:120])
                return False

        res = await asyncio.gather(*[one(s) for s in symbols], return_exceptions=True)
        ok = sum(1 for r in res if r is True)
        return ok

    # ------------------------------------------------------------------
    # Отбор инструментов
    # ------------------------------------------------------------------
    def _select_symbols(self) -> list[str]:
        quotes = set(self.settings.quote_assets)
        out: list[str] = []
        for sym, m in self.ex.markets.items():
            if not m.get("active", True):
                continue
            if self.cfg.market == "swap" and not m.get("swap"):
                continue
            if self.cfg.market == "spot" and not m.get("spot"):
                continue
            if m.get("quote") not in quotes:
                continue
            out.append(sym)
        return out

    async def _refresh_top(self) -> list[str]:
        """top_n инструментов по 24h объёму в.quote."""
        try:
            # None → биржа отдаёт все тикеры одним запросом (~0.2 c на Binance);
            # список из 700+ символов заставлял ccxt работать заметно дольше
            tickers = await self.ex.fetch_tickers(None)
            STORE.bump("rest_calls")
        except Exception as e:  # noqa: BLE001
            log.warning("[%s] fetch_tickers: %s", self.cfg.label, str(e)[:160])
            STORE.bump("errors")
            return self.hot

        allowed = set(self.symbols)
        rows = []
        for sym, t in tickers.items():
            if sym not in self.ex.markets or sym not in allowed:
                continue
            st = self._state(sym)
            st.apply_ticker(t)
            STORE.bump("ticker_updates")
            qv = t.get("quoteVolume") or 0.0
            rows.append((qv, sym))
        # nlargest вместо полной сортировки: топ нужен, а упорядоченный хвост — нет
        top = [sym for _, sym in heapq.nlargest(self.cfg.top_n, rows)]

        # референсные цены BTC/ETH — чтобы объёмы в BTC-парах приводились к USD
        btc = _find_last(tickers, "BTC")
        eth = _find_last(tickers, "ETH")
        if btc or eth:
            set_reference_prices(btc, eth)

        return top

    def _state(self, symbol: str):
        m = self.ex.market(symbol)
        st = STORE.get_or_create(self.cfg.id, self.cfg.label, self.cfg.market, symbol,
                                 m.get("base") or symbol.split("/")[0], m.get("quote") or "")
        # без этого инверсные контракты считаются в неверных единицах
        st.set_contract(bool(m.get("inverse")), m.get("contractSize"))
        st.ohlcv_vol_in_contracts = self.cfg.ohlcv_vol_in_contracts
        st.dex = self.cfg.dex
        return st

    # ------------------------------------------------------------------
    # Циклы
    # ------------------------------------------------------------------
    async def _ticker_loop(self) -> None:
        while not self._stop.is_set():
            t0 = time.time()
            await self._refresh_top()
            STORE.set_status(self.cfg.label, latency_ms=int((time.time() - t0) * 1000))
            base = self.cfg.ticker_refresh or self.settings.ticker_refresh
            # биржа вне фокуса: тикеры всё равно нужны (таблица/карта), но
            # долбить биржу раз в 20 секунд ради невидимых данных незачем
            await asyncio.sleep(max(base, PAUSED_TICKER_REFRESH) if self._paused else base)
            if self._stop.is_set():
                break

    async def _rotate_loop(self) -> None:
        """
        Пересобирает горячий набор и (пере)подключает WS-потоки.

        Монета «в игре» (вырос объём) → попадает в топ → на неё заводятся
        стримы. Протухла → выпадает из топа → стримы гасятся И снимается
        подписка на бирже через un_watch_*, иначе список подписок растёт
        бесконечно и соединение деградирует.

        В фокус-режиме список приходит снаружи (set_focus), а ротация
        просыпается по _focus_wake — поэтому первые графики появляются
        через FOCUS_ROTATE_DELAY, а не через HOT_ROTATE.
        """
        await asyncio.sleep(ROTATE_START_DELAY)
        while not self._stop.is_set():
            try:
                if self._paused:
                    # биржа вне фокуса: подписок не держим вовсе — КРОМЕ
                    # kline-потоков сетки графиков. Раньше _close_streams()
                    # гасил и их: плитки оставались без свечей, пока REST-
                    # прогрев не пробивался через занятый семафор (таймаут 8 c)
                    # → «выбрал биржу — графики есть; новый отбор/пауза фокуса
                    # — свечи пропали». Символы сетки входят в пул стрима
                    # (добавлены в hot/focus), поэтому лишней нагрузки нет.
                    #
                    # ВАЖНО: keep строится по _grid_syms (цели WS-подписки
                    # сетки), а НЕ по пересечению с self.hot. Grid-символы
                    # добавляются в hot только когда уже стримятся, поэтому
                    # пересечение давало ПУСТОЙ keep для свежих кандидатов
                    # нового отбора → пауза биржи снимала их kline-топики на
                    # стороне ccxt, _candle_tasks оставались «живыми», и
                    # watch_candles никогда их не переподписывал (sym in
                    # _candle_tasks → continue). Итог: «плитки есть, свечей
                    # нет» ровно после смены отбора/фокуса.
                    grid_syms = set(getattr(self, "_grid_syms", set()) or ()) \
                        | set(self._candle_tasks)
                    if (self._book_tasks or self._trade_tasks or self._ticker_tasks
                            or self._batch_alive()):
                        self._close_streams(keep_candles=grid_syms)
                    STORE.set_status(self.cfg.label, state="paused",
                                     symbols=len(self.symbols), books=0, trades=0,
                                     hot=0, banned=len(self._book_banned))
                    self._focus_wake.clear()
                    await self._sleep_rotate(HOT_ROTATE)
                    continue

                if self._focus_syms is not None:
                    # фокус-режим: список пришёл снаружи (отбор по фильтру),
                    # собственный расчёт топа по объёму не нужен.
                    # Флаг сбрасываем ДО чтения списка: всё, что придёт после,
                    # гарантированно разбудит ротацию ещё раз.
                    self._focus_wake.clear()
                    top = list(self._focus_syms)
                else:
                    top = await self._refresh_top()
                    if not top:
                        top = self.hot
                self.hot = top
                book_n = min(self.cfg.books, len(top)) if self._focus_syms else self.cfg.books
                trade_n = min(TRADES_STREAM_N, len(top)) if self._focus_syms else TRADES_STREAM_N
                want_books = [s for s in top[:book_n] if s not in self._book_banned]
                want_trades = list(top[:trade_n])

                # --- батчевые подписки: одна задача на чанк вместо задачи на символ ---
                await self._apply_batch_streams(want_books, want_trades)

                # --- поточечно: только для потоков, где батч недоступен/умер ---
                drop_books: list[str] = []
                drop_trades: list[str] = []
                drop_tickers: list[str] = []
                if not self._batch_on("book"):
                    wb = set(want_books)
                    for s in wb - set(self._book_tasks) - self._book_banned:
                        self._book_tasks[s] = self._spawn(self._book_stream(s), name=f"book:{s}")
                    drop_books = sorted(set(self._book_tasks) - wb)
                    for s in drop_books:
                        self._book_tasks.pop(s).cancel()
                if not self._batch_on("trades"):
                    wt = set(want_trades)
                    for s in wt - set(self._trade_tasks):
                        self._trade_tasks[s] = self._spawn(self._trade_stream(s), name=f"trades:{s}")
                    drop_trades = sorted(set(self._trade_tasks) - wt)
                    for s in drop_trades:
                        self._trade_tasks.pop(s).cancel()
                if not self._batch_on("tickers"):
                    wt = set(want_trades)
                    for s in wt - set(self._ticker_tasks):
                        self._ticker_tasks[s] = self._spawn(self._ticker_stream(s), name=f"tk:{s}")
                    drop_tickers = sorted(set(self._ticker_tasks) - wt)
                    for s in drop_tickers:
                        self._ticker_tasks.pop(s).cancel()
                if drop_books or drop_trades or drop_tickers:
                    self._detach_cleanup(drop_books, drop_trades, drop_tickers)

                # Массовый бан почти всегда означает НЕ плохие символы, а
                # невалидную глубину стакана: Gate на limit=200 отвечает
                # BadRequest по всем подпискам, и без этой проверки биржа
                # молча осталась бы вообще без плотностей.
                wanted = max(self.cfg.books, 1)
                if len(self._book_banned) >= max(3, int(wanted * 0.5)):
                    log.error("[%s] забанено %d из %d стаканов — похоже на невалидный "
                              "book_limit=%d (допустимые значения см. VALID_BOOK_LIMITS). "
                              "Снимаем бан и продолжаем с увеличенным backoff.",
                              self.cfg.label, len(self._book_banned), wanted, self.cfg.book_limit)
                    self._book_banned.clear()
                    self._book_limit_suspect = True
                # амнистия: раз в BAN_TTL даём забаненным символам ещё один шанс —
                # делистинги и сбои биржи проходят, а навсегда выключенный стакан
                # сам не восстановится
                elif self._book_banned and time.time() - self._ban_ts > BAN_TTL:
                    log.info("[%s] амнистия %d забаненных стаканов",
                             self.cfg.label, len(self._book_banned))
                    self._book_banned.clear()
                    self._ban_ts = time.time()

                STORE.set_status(self.cfg.label, state="online", symbols=len(self.symbols),
                                 books=len(self._book_tasks) + len(self._batch["book"]["syms"]),
                                 trades=len(self._trade_tasks) + len(self._batch["trades"]["syms"]),
                                 hot=len(self.hot), banned=len(self._book_banned),
                                 focus=self.focus_mode,
                                 book_limit_suspect=self._book_limit_suspect)
            except Exception as e:  # noqa: BLE001
                log.warning("[%s] rotate: %s", self.cfg.label, str(e)[:160])
                STORE.bump("errors")
            if self._stopped():
                break
            await self._sleep_rotate(HOT_ROTATE)

    def _stopped(self) -> bool:
        """Стоп ИЛИ пауза: оба случая прерывают ожидание ротации."""
        return self._stop.is_set() or self._paused

    async def _sleep_rotate(self, delay: float) -> None:
        """
        Сон ротации с ранним пробуждением по set_focus()/pause()/resume().

        Флаг намеренно НЕ сбрасывается здесь. Раньше он чистился перед
        ожиданием, и сигнал, пришедший ПОКА ротация разбиралась с подписками
        (а это десятки секунд на массовых отписках), терялся: следующий
        пересчёт случался только через HOT_ROTATE. Живой замер показывал
        hot=50 ещё 50 секунд после выключения фокус-режима. Теперь флаг
        сбрасывается в момент потребления списка (см. _rotate_loop).
        """
        try:
            await asyncio.wait_for(self._focus_wake.wait(), timeout=delay)
            # список изменился — пересобираем подписки почти сразу, но не
            # чаще FOCUS_ROTATE_DELAY, чтобы серия обновлений не долбила биржу
            await asyncio.sleep(FOCUS_ROTATE_DELAY)
        except asyncio.TimeoutError:
            pass

    def _detach_cleanup(self, books: list[str], trades: list[str],
                        tickers: Optional[list[str]] = None) -> None:
        """
        Снятие подписок — фоново и параллельно.

        Последовательные `await un_watch_*` внутри ротации стоили 56 секунд на
        переходе 150 → 50 подписок (замер на живом Binance: ~170 отписок по
        ~0.3 c). Всё это время ротация не могла применить новый список, то есть
        фокус-режим «залипал» на старом наборе. Отписки не блокируют rotaciju:
        задачи на бирже уже отменены, остальное - вежливость к соединению.

        tickers=None — обратная совместимость: снимем тикеры с тех же символов,
        что и сделки (прежнее поведение, когда тикер-задача жила парой со сделками).
        """
        if tickers is None:
            tickers = trades
        t = asyncio.create_task(self._unwatch_many(books, trades, tickers),
                                name=f"{self.cfg.id}:unwatch")
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)

    async def _unwatch_many(self, books: list[str], trades: list[str],
                            tickers: list[str]) -> None:
        coros = [self._unwatch("order_book", s) for s in books]
        coros += [self._unwatch("trades", s) for s in trades]
        coros += [self._unwatch("ticker", s) for s in tickers]
        if coros:
            await asyncio.gather(*coros, return_exceptions=True)

    async def _unwatch(self, kind: str, symbol: str) -> None:
        """Снимаем подписку на стороне биржи (best effort)."""
        method = {"order_book": "un_watch_order_book", "trades": "un_watch_trades",
                  "ticker": "un_watch_ticker"}.get(kind)
        if not method or not hasattr(self.ex, method):
            return
        try:
            await getattr(self.ex, method)(symbol)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            log.debug("[%s] unwatch %s %s: %s: %s", self.cfg.label, kind, symbol,
                      type(e).__name__, str(e)[:120])

    async def _drop_single_subscriptions(self, kind: str, symbols: list[str]) -> None:
        """
        Полностью «забыть» локальные подписки ccxt.pro данного вида.

        Нужен при паузе/возобновлении биржи: watch_*_for_symbols у bybit/okx
        переиспользуют ОДНО соединение, и если ротация подписала топик ещё раз
        до того, как отработала фоновая отписка, биржа отвечает
        «already subscribed» лавиной ошибок на каждой ротации фокуса
        (живой случай: bybit orderbook.200.BTCUSDT). Удаление messageHashes —
        штатный способ ccxt сбросить состояние подписки; следующий watch
        переподпишется корректно.

        Сначала ждём завершения всех un_watch_* по символам — иначе отписка
        долетит до биржи ПОСЛЕ повторной подписки и снимет уже новый поток.
        """
        if kind == "candles":
            # kline-подписки сетки графиков живут отдельным dict и своим
            # методом un_watch_ohlcv_for_symbols ([[sym, tf], ...]). TF берём
            # ИЗ ЗАПИСИ НА СИМВОЛ (_candle_tfs): раньше подставляли общий
            # self._candle_tf — после смены таймфрейма отписка уходила с
            # нетоповым ключом, ccxt не удалял messageHash, топик оставался
            # «висеть», а следующая подписка того же символа на новый tf
            # резолвила чужой future → пустые плитки до REST-прогрева.
            pairs = [[s, self._candle_tfs.get(s)
                      or getattr(self, "_candle_tf", "") or "1m"] for s in symbols]
            for s in symbols:
                self._candle_tfs.pop(s, None)
            if pairs and hasattr(self.ex, "un_watch_ohlcv_for_symbols"):
                try:
                    await self.ex.un_watch_ohlcv_for_symbols(pairs)
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # noqa: BLE001
                    log.debug("[%s] drop candles %d: %s: %s", self.cfg.label,
                              len(pairs), type(e).__name__, str(e)[:120])
            return
        coros = [self._unwatch(kind, s) for s in symbols]
        if coros:
            await asyncio.gather(*coros, return_exceptions=True)
        store_attr = {"book": "orders", "trades": "trades", "tickers": "tickers"}[kind]
        removed = 0
        try:
            store = getattr(self.ex, "subscriptions", {}).get(store_attr) or {}
            for h in list(store):
                if any(sym in h for sym in symbols):
                    store.pop(h, None)
                    removed += 1
        except Exception as e:  # noqa: BLE001
            log.debug("[%s] drop %s subscription state: %s", self.cfg.label,
                      kind, str(e)[:120])
        if removed:
            log.info("[%s] сброшено %d зависших подписок %s (пересборка потоков)",
                     self.cfg.label, removed, kind)

    # ------------------------------------------------------------------
    # Батчевые WS-подписки (одна задача на чанк символов)
    # ------------------------------------------------------------------
    def _batch_on(self, kind: str) -> bool:
        """Батч-подписки доступны биржей И не умерли в этой сессии."""
        if self._batch[kind]["dead"]:
            return False
        has = getattr(self.ex, "has", None)
        return bool(has and has.get(BATCH_CAP[kind]))

    def _batch_alive(self) -> bool:
        return any(b["tasks"] or b["syms"] for b in self._batch.values())

    async def _apply_batch_streams(self, want_books: list[str], want_trades: list[str]) -> None:
        """
        Пересобирает батч-подписки под новый горячий набор.

        Для «умершего» потока сначала ДОЖИДАЕМСЯ отписки старого списка:
        фоновая отписка могла бы снять подписки уже после того, как поточечный
        запасной путь подпишется заново (у бирж с общим URL соединения это
        одни и те же топики).
        """
        for kind, want in (("book", want_books), ("trades", want_trades),
                           ("tickers", want_trades)):
            b = self._batch[kind]
            if b["dead"] and b["syms"]:
                await self._unwatch_batch(kind, b["syms"])
                b["syms"] = []
                b["gen"] += 1
            if self._batch_on(kind):
                await self._sync_batch(kind, want)

    async def _sync_batch(self, kind: str, want: list[str]) -> None:
        """
        Список изменился → перезапускаем задачи чанков; отписка — фоново.

        Тот же принцип, что в _detach_cleanup: ротацию нельзя блокировать
        ответами биржи на un_watch_*, иначе фокус-режим залипает на старом
        наборе (замер: 56 c на переходе 150 → 50 поточечных подписок).
        """
        b = self._batch[kind]
        # После паузы биржи фоновая отписка ещё может жить в ccxt: если
        # подписать топик раньше, чем она завершится, bybit/okx отвечают
        # «already subscribed» (см. _close_streams). Ждём — обычно секунды.
        pu = getattr(self, "_pending_unwatch", None)
        if pu is not None and not pu.done():
            try:
                await asyncio.wait_for(asyncio.shield(pu), timeout=5.0)
            except asyncio.TimeoutError:
                pass
            except Exception as e:  # noqa: BLE001
                log.debug("[%s] ожидание отписки: %s: %s", self.cfg.label,
                          type(e).__name__, str(e)[:120])
        alive = [t for t in b["tasks"] if not t.done()]
        rebuild = b.pop("rebuild", False)
        if want == b["syms"] and not rebuild and (alive or not want):
            b["tasks"] = alive     # состав не изменился — ничего не пересобираем
            return
        old = b["syms"]
        b["syms"] = list(want)
        b["gen"] += 1
        for t in b["tasks"]:
            t.cancel()
        b["tasks"] = []
        if old and (old != want or (rebuild and self.cfg.batch_full_unwatch)):
            if self.cfg.batch_full_unwatch:
                # Binance-семейство: URL соединения строится из СПИСКА символов,
                # поэтому старый список снимается целиком (иначе старое
                # соединение продолжит стримить оставшиеся символы в общий кэш
                # ccxt вперемешку с новым). При rebuild (уменьшение чанка) список
                # тот же, но соединения старых чанков обязаны быть сняты.
                self._detach_batch_unwatch(kind, old)
            else:
                removed = [s for s in old if s not in set(want)]
                if removed:
                    self._detach_batch_unwatch(kind, removed)
                if rebuild:
                    # Чанк урезали — старые задачи отменены, но подписки на
                    # бирже живут под СТАРЫМ размером чанка. Фоновая отписка
                    # «только выпавших» тут ничего не снимет (состав тот же),
                    # и новая подписка тем же топиком получит от биржи
                    # «already subscribed» лавиной на каждой ротации фокуса
                    # (живой случай: bybit orderbook.200.BTCUSDT). Снимаем
                    # весь старый список; новые чанки доедут до подписки сами.
                    self._detach_batch_unwatch(kind, old)
        if not want:
            return
        coro = {"book": self._book_stream_batch,
                "trades": self._trade_stream_batch,
                "tickers": self._ticker_stream_batch}[kind]
        gen = b["gen"]
        chunk_size = max(1, int(b["chunk"]))
        for i in range(0, len(want), chunk_size):
            chunk = want[i:i + chunk_size]
            b["tasks"].append(self._spawn(coro(chunk, gen),
                                          name=f"{kind}-batch{i // chunk_size}"))

    def _detach_batch_unwatch(self, kind: str, symbols: list[str]) -> None:
        t = asyncio.create_task(self._unwatch_batch(kind, symbols),
                                name=f"{self.cfg.id}:unwatch-{kind}")
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)

    async def _unwatch_batch(self, kind: str, symbols: list[str]) -> None:
        """Снятие батч-подписки (best effort, чанками по BATCH_CHUNK)."""
        method = getattr(self.ex, BATCH_UNWATCH[kind], None)
        chunk_size = max(1, int(self._batch[kind]["chunk"]))
        if method is not None:
            for i in range(0, len(symbols), chunk_size):
                try:
                    await method(symbols[i:i + chunk_size])
                except Exception as e:  # noqa: BLE001
                    log.debug("[%s] unwatch %s batch: %s: %s", self.cfg.label, kind,
                              type(e).__name__, str(e)[:120])
        if self.cfg.batch_full_unwatch:
            self._release_stream_counters(kind, symbols)

    def _release_stream_counters(self, kind: str, symbols: list[str]) -> None:
        """
        Возврат внутренних счётчиков ccxt (binance-семейство).

        Binance строит WS-URL батч-подписки из списка символов: каждому
        уникальному списку — свой «стрим» (индекс 0..49, лимит 200 подписок на
        индекс). При подписке ccxt увеличивает options['numSubscriptionsByStream'],
        а при отписке НЕ уменьшает. Список у нас меняется на каждой ротации
        (в фокус-режиме — каждые 15 секунд), поэтому без возврата счётчиков
        примерно через сотню пересборок ccxt бросает BadRequest «reached the
        limit of subscriptions by stream» и батч-подписки умирают.

        Ключ потока повторяет формат ccxt (проверено на 4.5.52 и 4.5.85):
        '<prefix>::' + ','.join(символы). Если ccxt изменит формат — pop просто
        не найдёт ключ, счётчики вырастут и сработает запасной путь
        (_batch_dead → поточечные подписки), то есть отказ безопасный.
        """
        try:
            opts = getattr(self.ex, "options", None) or {}
            by_hash = opts.get("streamBySubscriptionsHash")
            counters = opts.get("numSubscriptionsByStream")
            prefix = BATCH_STREAM_HASH.get(kind)
            if not by_hash or counters is None or not prefix:
                return
            chunk_size = max(1, int(self._batch[kind]["chunk"]))
            for i in range(0, len(symbols), chunk_size):
                chunk = symbols[i:i + chunk_size]
                h = prefix + "::" + ",".join(chunk)
                stream = by_hash.pop(h, None)
                if stream is not None and stream in counters:
                    counters[stream] = max(0, counters[stream] - len(chunk))
        except Exception as e:  # noqa: BLE001
            log.debug("[%s] release stream counters: %s", self.cfg.label, str(e)[:120])

    def _batch_dead(self, kind: str, e: BaseException) -> None:
        """
        Батч-поток стабильно отказывает → переходим на поточечные подписки.

        Поточечный путь медленнее (задача и, у Binance, соединение на символ),
        но умеет банить отдельные символы: один «плохой» инструмент не должен
        ронять стакан всей биржи (реальный случай: Gate 龙虾/USDT:USDT).
        Старые батч-подписки снимет _apply_batch_streams на следующей ротации —
        ДО запуска поточечных, чтобы отписки не пересеклись с новыми подписками.
        """
        b = self._batch[kind]
        if b["dead"]:
            return
        b["dead"] = True
        for t in b["tasks"]:
            t.cancel()
        b["tasks"] = []
        STORE.bump("errors")
        log.warning("[%s] батч-подписки (%s) отключены, переход на поточечные: %s: %s",
                    self.cfg.label, kind, type(e).__name__, str(e)[:140])
        self._focus_wake.set()   # пересобрать подписки сразу, не дожидаясь HOT_ROTATE

    # Признаки «сервер не принял РАЗМЕР подписки»: чанк можно уменьшить и
    # продолжить батчами, вместо полного падения на поточечный режим.
    _SIZE_ERROR_HINTS = ("args size", "too many", "at most", "exceed",
                         "more than", "too large", "limit of")

    def _batch_fail(self, kind: str, e: BaseException, fails: int,
                    first_fail: float) -> tuple[int, float, bool]:
        """Счётчик отказов батч-потока: та же идея, что в _book_stream."""
        STORE.bump("errors")
        b = self._batch[kind]
        low = str(e).lower()
        if "already subscribed" in low:
            # ccxt потерял состояние гонки пересборки подписок (ротация фокуса
            # сняла и тут же подписала тот же топик). Для bybit это штатная
            # ситуация «подписка уже живёт»: ждём, пока поток переживёт
            # пересборку; счётчик отказов не трогаем, чтобы не хоронить батч.
            log.debug("[%s] %s batch: already subscribed — ждём пересборку",
                      self.cfg.label, kind)
            return 0, first_fail, False
        if b["chunk"] > 10 and any(h in low for h in self._SIZE_ERROR_HINTS):
            # биржа отвергла размер списка (реальный случай: bybit spot —
            # «args size >10»): режем чанк вдвое и пересобираем подписки
            old_chunk = b["chunk"]
            b["chunk"] = max(10, old_chunk // 2)
            b["rebuild"] = True
            log.info("[%s] %s: биржа отвергла список из %d символов (%s) — "
                     "уменьшаю чанк до %d и повторяю",
                     self.cfg.label, kind, old_chunk, str(e)[:80], b["chunk"])
            self._focus_wake.set()
            return 0, 0.0, False
        now = time.time()
        fails += 1
        if not first_fail:
            first_fail = now
        elif now - first_fail > FAIL_WINDOW:
            fails, first_fail = 1, now
        if fails >= MAX_STREAM_FAILS:
            self._batch_dead(kind, e)
            return fails, first_fail, True
        log.debug("[%s] %s batch: %s: %s", self.cfg.label, kind,
                  type(e).__name__, str(e)[:140])
        return fails, first_fail, False

    async def _book_stream_batch(self, chunk: list[str], gen: int) -> None:
        """
        Стаканы чанка одной задачей.

        watch_order_book_for_symbols возвращает ОДИН ближайший обновившийся
        стакан из чанка (ccxt устраивает гонку future'ей всех символов);
        символ — в book['symbol']. Семантика та же, что у поточечного
        watch_order_book: у binance/okx/bybit/aster одиночный метод — это
        делегат в forSymbols([symbol]) (проверено по исходникам ccxt 4.5.52
        и 4.5.85 и живым зондом tools/probe_batch_ws.py).
        """
        b = self._batch["book"]
        chunk_set = set(chunk)
        fails = 0
        first_fail = 0.0
        backoff = 2.0
        while not self._stop.is_set() and not b["dead"] and gen == b["gen"]:
            try:
                book = await self.ex.watch_order_book_for_symbols(
                    chunk, limit=self.cfg.book_limit)
                sym = book.get("symbol") if isinstance(book, dict) else None
                if sym in chunk_set:
                    try:
                        self._state(sym).apply_book(
                            book,
                            big_usd=self.settings.big_density_usd,
                            huge_usd=self.settings.huge_density_usd,
                            depth=self.cfg.book_limit,
                            # тяжёлая часть (кластеризация+имбаланс) — не чаще 4 Гц
                            min_interval=self.settings.book_density_interval,
                        )
                    except Exception as e:  # noqa: BLE001
                        STORE.bump("errors")
                        log.debug("[%s] book apply %s: %s", self.cfg.label, sym, str(e)[:120])
                    STORE.bump("book_updates")
                    STORE.bump("ws_messages")
                fails = 0
                backoff = 2.0
            except asyncio.CancelledError:
                return
            except (ccxt.NotSupported, ccxt.ArgumentsRequired, ccxt.BadRequest) as e:
                self._batch_dead("book", e)   # биржа/конфиг не принимает батч — уходим поточечно
                return
            except (ccxt.NetworkError, ccxtpro.NetworkError):
                await asyncio.sleep(1)        # транзиентная — счётчик не трогаем
            except Exception as e:  # noqa: BLE001
                if "already subscribed" in str(e).lower():
                    # подписка уже живёт на этом же соединении (гонка пересборки
                    # при ротации фокуса) — ждём, пока ccxt сам разберётся;
                    # иначе future с ошибкой «висит» и сыпется в лог asyncio
                    await asyncio.sleep(2.0)
                    continue
                fails, first_fail, dead = self._batch_fail("book", e, fails, first_fail)
                if dead:
                    return
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.7, 30.0)

    async def _trade_stream_batch(self, chunk: list[str], gen: int) -> None:
        """
        Лента сделок чанка одной задачей.

        watch_trades_for_symbols возвращает список сделок ближайшего
        обновившегося символа чанка; символ — в trade['symbol'] (у binance
        одиночный watch_trades — делегат в forSymbols([symbol]), семантика
        совпадает).
        """
        b = self._batch["trades"]
        chunk_set = set(chunk)
        fails = 0
        first_fail = 0.0
        backoff = 2.0
        while not self._stop.is_set() and not b["dead"] and gen == b["gen"]:
            try:
                trades = await self.ex.watch_trades_for_symbols(chunk)
                for tr in trades or []:
                    sym = tr.get("symbol")
                    if sym not in chunk_set:
                        continue
                    try:
                        self._state(sym).apply_trade(tr)
                        STORE.bump("trades")
                    except Exception as e:  # noqa: BLE001
                        STORE.bump("errors")
                        log.debug("[%s] trade apply %s: %s", self.cfg.label, sym, str(e)[:120])
                STORE.bump("ws_messages")
                fails = 0
                backoff = 2.0
            except asyncio.CancelledError:
                return
            except (ccxt.NotSupported, ccxt.ArgumentsRequired, ccxt.BadRequest) as e:
                self._batch_dead("trades", e)
                return
            except (ccxt.NetworkError, ccxtpro.NetworkError):
                await asyncio.sleep(1)
            except Exception as e:  # noqa: BLE001
                if "already subscribed" in str(e).lower():
                    await asyncio.sleep(2.0)   # гонка пересборки — см. _book_stream_batch
                    continue
                fails, first_fail, dead = self._batch_fail("trades", e, fails, first_fail)
                if dead:
                    return
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.7, 30.0)

    async def _ticker_stream_batch(self, chunk: list[str], gen: int) -> None:
        """
        Тикеры чанка одной задачей: watch_tickers возвращает dict обновившихся
        тикеров (у binance — один {symbol: ticker} за вызов). Применяем тот же
        apply_quote, что и поточечный поток: bookTicker-поля (bid/ask/last)
        обязаны оставаться realtime.
        """
        b = self._batch["tickers"]
        chunk_set = set(chunk)
        fails = 0
        first_fail = 0.0
        backoff = 2.0
        while not self._stop.is_set() and not b["dead"] and gen == b["gen"]:
            try:
                tickers = await self.ex.watch_tickers(chunk)
                for sym, q in (tickers or {}).items():
                    if sym not in chunk_set or not isinstance(q, dict):
                        continue
                    try:
                        self._state(sym).apply_quote(q)
                    except Exception as e:  # noqa: BLE001
                        STORE.bump("errors")
                        log.debug("[%s] ticker apply %s: %s", self.cfg.label, sym, str(e)[:120])
                STORE.bump("ws_messages")
                fails = 0
                backoff = 2.0
            except asyncio.CancelledError:
                return
            except (ccxt.NotSupported, ccxt.ArgumentsRequired, ccxt.BadRequest) as e:
                self._batch_dead("tickers", e)
                return
            except (ccxt.NetworkError, ccxtpro.NetworkError):
                await asyncio.sleep(1)
            except Exception as e:  # noqa: BLE001
                if "already subscribed" in str(e).lower():
                    await asyncio.sleep(2.0)   # гонка пересборки — см. _book_stream_batch
                    continue
                fails, first_fail, dead = self._batch_fail("tickers", e, fails, first_fail)
                if dead:
                    return
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.7, 30.0)

    async def _ticker_stream(self, symbol: str) -> None:
        while not self._stop.is_set():
            try:
                q = await self.ex.watch_ticker(symbol)
                self._state(symbol).apply_quote(q)
                STORE.bump("ws_messages")
            except asyncio.CancelledError:
                return
            except (ccxt.BadSymbol, ccxtpro.BadSymbol):
                self._ticker_tasks.pop(symbol, None)
                return
            except (ccxt.NetworkError, ccxtpro.NetworkError):
                await asyncio.sleep(1)
            except Exception as e:  # noqa: BLE001
                STORE.bump("errors")
                log.debug("[%s] ticker %s: %s: %s", self.cfg.label, symbol,
                          type(e).__name__, str(e)[:140])
                await asyncio.sleep(2)

    async def _trade_stream(self, symbol: str) -> None:
        while not self._stop.is_set():
            try:
                tr = await self.ex.watch_trades(symbol)
                st = self._state(symbol)
                for t in tr:
                    st.apply_trade(t)
                    STORE.bump("trades")
                STORE.bump("ws_messages")
            except asyncio.CancelledError:
                return
            except (ccxt.BadSymbol, ccxtpro.BadSymbol):
                self._trade_tasks.pop(symbol, None)
                return
            except (ccxt.NetworkError, ccxtpro.NetworkError):
                await asyncio.sleep(1)
            except Exception as e:  # noqa: BLE001
                STORE.bump("errors")
                log.debug("[%s] trades %s: %s: %s", self.cfg.label, symbol,
                          type(e).__name__, str(e)[:140])
                await asyncio.sleep(2)

    async def _book_stream(self, symbol: str) -> None:
        fails = 0
        first_fail = 0.0
        backoff = 2.0
        while not self._stop.is_set():
            try:
                book = await self.ex.watch_order_book(symbol, limit=self.cfg.book_limit)
                self._state(symbol).apply_book(
                    book,
                    big_usd=self.settings.big_density_usd,
                    huge_usd=self.settings.huge_density_usd,
                    depth=self.cfg.book_limit,
                    # тяжёлая часть (кластеризация+имбаланс) — не чаще 4 Гц
                    # на символ, иначе ~510 книг × 10 Гц съедают >1.5 ядра
                    min_interval=self.settings.book_density_interval,
                )
                STORE.bump("book_updates")
                STORE.bump("ws_messages")
                fails = 0
                backoff = 2.0
            except asyncio.CancelledError:
                return
            except (ccxt.BadSymbol, ccxtpro.BadSymbol):
                self._book_tasks.pop(symbol, None)
                return
            except (ccxt.NetworkError, ccxtpro.NetworkError):
                await asyncio.sleep(1)          # транзиентная — счётчик не трогаем
            except Exception as e:  # noqa: BLE001
                STORE.bump("errors")
                now = time.time()
                fails += 1
                if not first_fail:
                    first_fail = now
                elif now - first_fail > FAIL_WINDOW:
                    # отказы идут вразнобой, а не одной пачкой — сбрасываем счёт,
                    # иначе транзитентные сбои постепенно забанят весь стакан
                    fails, first_fail, backoff = 1, now, 2.0

                if fails >= MAX_STREAM_FAILS:
                    # Ключевое различие: биржа стабильно отказывает ПО ЭТОМУ
                    # символу (у Gate так ведёт себя 龙虾/USDT:USDT) — или
                    # оборвалось соединение, и отказали сразу все подписки.
                    # Во втором случае банить нельзя: после обрыва все 12
                    # стаканов Gate отказали в пределах 100 мс с одним conn_id,
                    # и выключатель навсегда выключил бы стакан биржи.
                    if now - first_fail < CORRELATED_FAIL_WINDOW:
                        log.info("[%s] book %s: отказ в общей пачке (%d за %.1f c) — "
                                 "похоже на обрыв соединения, продолжаем с backoff",
                                 self.cfg.label, symbol, fails, now - first_fail)
                        fails, first_fail = 0, 0.0
                    else:
                        log.warning("[%s] book %s: %d отказов за %.0f c, поток остановлен (%s)",
                                    self.cfg.label, symbol, fails, now - first_fail, str(e)[:90])
                        if self._book_limit_suspect:
                            # не банить: проблема в конфигурации, а не в символе
                            fails, first_fail, backoff = 0, 0.0, 30.0
                        else:
                            self._book_tasks.pop(symbol, None)
                            self._book_banned.add(symbol)
                            return
                else:
                    log.debug("[%s] book %s: %s: %s", self.cfg.label, symbol,
                              type(e).__name__, str(e)[:140])
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.7, 30.0)   # не долбим биржу в tight-цикле

    # ------------------------------------------------------------------
    def _ohlcv_batch(self) -> list[str]:
        """
        Кому добираем 1m-свечи фоновым циклом.

        В фокус-режиме — ТОЛЬКО текущему отбору (self.hot, ≤ limit монет).
        Раньше здесь опрашивался весь пул кандидатов (pool[:top_n] — до 300
        символов каждую минуту): сотни REST-запросов, которые дублировали
        работу пересчёта фокуса (он сам добирает протухшие свечи пула через
        ensure_ohlcv). Именно это выглядело как «программа долбит все монеты».
        """
        return self.hot[: self.cfg.top_n]

    async def _ohlcv_loop(self) -> None:
        """
        Подкачка 1m-свечей для горячего набора → NATR и мульти-ТФ.

        Порционная раздача (не gather всей пачки разом): при 150 монетах и
        параллельности 6 это был непрерывный поток из сотен klines-запросов,
        который забивал REST-очередь биржи — запросы графиков (/api/candles,
        /api/grid) стояли в ней за ними и отваливались по таймауту. Между
        порциями — пауза, чтобы срочные запросы проходили первыми; весь цикл
        идёт через общий троттл-семафор _sem_rest.
        """
        await asyncio.sleep(8)
        while not self._stop.is_set():
            batch = self._ohlcv_batch()
            if not self._paused and batch:
                for i in range(0, len(batch), max(1, self.settings.ohlcv_concurrency)):
                    if self._stop.is_set() or self._paused:
                        break
                    chunk = batch[i:i + max(1, self.settings.ohlcv_concurrency)]
                    await asyncio.gather(*[self._fetch_ohlcv(s) for s in chunk],
                                         return_exceptions=True)
                    await asyncio.sleep(OHLCV_CHUNK_PAUSE)
            await asyncio.sleep(max(60.0, self.settings.ticker_refresh * 3))

    async def _fetch_ohlcv(self, symbol: str, *, force: bool = False,
                           ttl: float = CANDLES_TTL) -> None:
        async with self._sem_rest:
            now = time.time()
            if not force:
                st0 = self._state(symbol)
                if st0.ohlcv and now - getattr(st0, "ohlcv_ts", 0.0) < ttl:
                    return              # свечи свежие — не плодим лишний REST
            try:
                candles = await self.ex.fetch_ohlcv(symbol, "1m", limit=self.settings.ohlcv_limit)
                STORE.bump("rest_calls")
                if candles:
                    self._state(symbol).apply_ohlcv(candles)
            except (ccxt.RateLimitExceeded, ccxt.DDoSProtection):
                await asyncio.sleep(2)
            except Exception as e:  # noqa: BLE001
                STORE.bump("errors")
                log.debug("[%s] ohlcv %s: %s: %s", self.cfg.label, symbol,
                          type(e).__name__, str(e)[:140])

    # ------------------------------------------------------------------
    async def _funding_loop(self) -> None:
        """
        Funding + Open Interest.

        Приоритет источников:
          1. ccxt fetch_funding_rates() — bulk-вызов сразу по всем символам.
             Работает у Gate/Aster/Hyperliquid/Bybit/OKX, поэтому новую биржу
             не нужно прописывать вручную.
          2. Нативные bulk-эндпоинты — там, где ccxt bulk не даёт (Binance).
          3. Точечно по топ-N — для OI, bulk-эндпоинта нет почти ни у кого.
        """
        if self.cfg.market != "swap":
            return
        await asyncio.sleep(10)
        while not self._stop.is_set():
            try:
                self._apply_oi_funding(await self._funding_bulk())
                self._apply_oi_funding(await self._oi_bulk())
                self._apply_oi_funding(await self._oi_targeted())
            except Exception as e:  # noqa: BLE001
                log.debug("[%s] funding loop: %s", self.cfg.label, str(e)[:160])
                STORE.bump("errors")
            await asyncio.sleep(FUNDING_REFRESH)

    def _apply_oi_funding(self, data: list[tuple[str, Optional[float], Optional[float], Optional[float]]]) -> None:
        """data: (symbol, oi, oi_usd, funding) — None означает «метрика не получена»."""
        if not data:
            return
        applied = 0
        for symbol, oi, oi_usd, funding in data:
            if symbol not in self.ex.markets:
                continue
            st = self._state(symbol)
            if oi is not None:
                prev = st.open_interest
                st.open_interest = oi
                if prev:
                    st.oi_change_pct = (oi / prev - 1.0) * 100.0
            if oi_usd is not None:
                st.open_interest_usd = oi_usd
            if funding is not None:
                st.funding = funding
            st.touch()
            applied += 1
        if applied:
            STORE.bump("rest_calls")
            log.debug("[%s] OI/funding applied for %d symbols", self.cfg.label, applied)

    async def _funding_bulk(self) -> list[tuple[str, Optional[float], Optional[float], Optional[float]]]:
        # 1) универсальный путь через ccxt
        if self.ex.has.get("fetchFundingRates"):
            try:
                rates = await self.ex.fetch_funding_rates()
                STORE.bump("rest_calls")
                return [(sym, None, None, _f(r.get("fundingRate")))
                        for sym, r in rates.items() if r.get("fundingRate") is not None]
            except Exception as e:  # noqa: BLE001
                log.debug("[%s] fetch_funding_rates: %s", self.cfg.label, str(e)[:140])
                STORE.bump("errors")
        # 2) нативный bulk. Binance-совместимый premiumIndex отдают Binance и
        #    Aster (проверено: 923 и 767 контрактов, поле lastFundingRate).
        prem = PREMIUM_INDEX_URLS.get(self.cfg.id)
        if prem:
            loop = asyncio.get_running_loop()
            try:
                raw = await loop.run_in_executor(None, _get_json, prem)
                STORE.bump("rest_calls")
                out = []
                for r in raw:
                    sym = self._to_ccxt_symbol(r.get("symbol", ""))
                    if sym:
                        out.append((sym, None, None, _f(r.get("lastFundingRate"))))
                return out
            except Exception as e:  # noqa: BLE001
                log.debug("[%s] premiumIndex: %s", self.cfg.label, str(e)[:140])
                STORE.bump("errors")
        return []

    async def _oi_bulk(self) -> list[tuple[str, Optional[float], Optional[float], Optional[float]]]:
        """OI одним запросом на всю биржу — только там, где такой эндпоинт есть."""
        loop = asyncio.get_running_loop()
        eid = self.cfg.id
        out: list[tuple[str, Optional[float], Optional[float], Optional[float]]] = []
        try:
            if eid == "bybit":
                res = await loop.run_in_executor(
                    None, _get_json, "https://api.bybit.com/v5/market/tickers?category=linear")
                for r in (res.get("result") or {}).get("list") or []:
                    sym = self._to_ccxt_symbol(r.get("symbol", ""))
                    if sym:
                        out.append((sym, _f(r.get("openInterest")),
                                    _f(r.get("openInterestValue")), None))
            elif eid == "okx":
                res = await loop.run_in_executor(
                    None, _get_json, "https://www.okx.com/api/v5/public/open-interest?instType=SWAP")
                for r in res.get("data") or []:
                    sym = self._to_ccxt_symbol(r.get("instId", ""))
                    if sym:
                        out.append((sym, _f(r.get("oiCcy")), _f(r.get("oiUsd")), None))
            elif eid == "gate":
                # /contract_stats требует contract= и потому НЕ bulk (400 без него).
                # /contracts отдаёт все ~1024 контракта одним запросом и содержит
                # position_size — суммарный открытый интерес в контрактах.
                #
                # Важно: contract_stats.open_interest ровно вдвое больше
                # position_size · quanto_multiplier (Gate считает обе стороны),
                # поэтому берём position_size — это стандартная конвенция OI,
                # как у Binance/Bybit/OKX. Сверено: BTC 2.379B vs 4.756B (×2.000),
                # ETH 1.106B vs 2.229B (×2.000).
                res = await loop.run_in_executor(
                    None, _get_json, "https://api.gateio.ws/api/v4/futures/usdt/contracts")
                for r in res or []:
                    sym = self._to_ccxt_symbol(str(r.get("name", "")))
                    if not sym:
                        continue
                    pos = _f(r.get("position_size"))
                    mult = _f(r.get("quanto_multiplier"))
                    mark = _f(r.get("mark_price")) or _f(r.get("last_price"))
                    oi_base = pos * mult if (pos is not None and mult) else None
                    oi_usd = oi_base * mark if (oi_base is not None and mark) else None
                    out.append((sym, oi_base, oi_usd, None))
            elif eid == "mexc":
                # /contract/ticker без symbol отдаёт ВСЕ контракты: holdVol (OI),
                # fundingRate и amount24 одним запросом. ccxt для MEXC
                # fetchOpenInterest/fetchFundingRates не реализовал.
                res = await loop.run_in_executor(
                    None, _get_json, "https://contract.mexc.com/api/v1/contract/ticker")
                for r in res.get("data") or []:
                    sym = self._to_ccxt_symbol(str(r.get("symbol", "")))
                    if not sym:
                        continue
                    hold = _f(r.get("holdVol"))
                    mark = _f(r.get("fairPrice")) or _f(r.get("lastPrice"))
                    cs = self.ex.market(sym).get("contractSize") or 1.0
                    # holdVol в тех же единицах, что и volume24; соотношение
                    # volume24·contractSize·price ≈ amount24 сверено на BTC (±2.5%)
                    oi_base = hold * cs if hold is not None else None
                    oi_usd = oi_base * mark if (oi_base is not None and mark) else None
                    out.append((sym, oi_base, oi_usd, _f(r.get("fundingRate"))))
            if out:
                STORE.bump("rest_calls")
        except Exception as e:  # noqa: BLE001
            log.debug("[%s] oi bulk: %s", self.cfg.label, str(e)[:140])
            STORE.bump("errors")
        return out

    async def _oi_targeted(self) -> list[tuple[str, Optional[float], Optional[float], Optional[float]]]:
        """
        Точечный OI по горячему топу — для бирж без bulk-эндпоинта
        (Binance, MEXC, Aster, Hyperliquid).
        """
        if self._has_oi_bulk():
            return []
        native = NATIVE_OI_URL.get(self.cfg.id)
        if not native and not self.ex.has.get("fetchOpenInterest"):
            return []
        targets = self.hot[: self.cfg.oi_top_n]
        if not targets:
            return []

        loop = asyncio.get_running_loop()

        async def one(sym: str):
            async with self._sem_oi:
                try:
                    m = self.ex.market(sym)
                    st = STORE.get(f"{self.cfg.id}:{sym}")
                    px = st.last if st and st.last else None

                    # 1) нативный эндпоинт — для бирж, где ccxt не реализовал
                    #    fetchOpenInterest (Aster). Ответ в Binance-формате.
                    if native:
                        url = native + urllib.parse.quote(str(m["id"]))
                        r = await loop.run_in_executor(None, _get_json, url)
                        oi = _f(r.get("openInterest"))
                        if oi is None and isinstance(r.get("data"), dict):
                            oi = _f(r["data"].get("openInterest"))
                        oi_usd = oi * px if (oi is not None and px) else None
                        return (sym, oi, oi_usd, None)

                    # 2) OKX: bulk OI уже получен, добираем только funding
                    if self.cfg.id == "okx":
                        url = ("https://www.okx.com/api/v5/public/funding-rate?instId="
                               + urllib.parse.quote(str(m["id"])))
                        r = await loop.run_in_executor(None, _get_json, url)
                        d = (r.get("data") or [{}])[0]
                        return (sym, None, None, _f(d.get("fundingRate")))

                    # 3) универсальный путь через ccxt (Binance, Hyperliquid, ...)
                    oi = await self.ex.fetch_open_interest(sym)
                    amt = _f(oi.get("openInterestAmount") if isinstance(oi, dict) else oi)
                    val = _f(oi.get("openInterestValue")) if isinstance(oi, dict) else None
                    if val is None and amt is not None and px:
                        val = amt * (1.0 if st and st.inverse else (m.get("contractSize") or 1.0)) * px
                    return (sym, amt, val, None)
                except Exception as e:  # noqa: BLE001
                    STORE.bump("errors")
                    log.debug("[%s] oi targeted %s: %s: %s", self.cfg.label, sym,
                              type(e).__name__, str(e)[:120])
                return None

        res = await asyncio.gather(*[one(s) for s in targets], return_exceptions=True)
        out = [r for r in res if isinstance(r, tuple)]
        if out:
            STORE.bump("rest_calls")
        return out

    def _has_oi_bulk(self) -> bool:
        return self.cfg.id in ("bybit", "okx", "gate", "mexc")

    async def warm_for_grid(self, symbols: list[str], tf: str, limit: int) -> None:
        """
        Подкачка свечей символам сетки графиков (/api/grid).

        Ключевое: прогреваем НЕ только 1m-буфер, а сразу запрошенный ТФ —
        он кладётся в _candles_cache с TTL=GRID_TTL, и get_grid отдаёт
        плитки из кэша без похода на биржу. Раньше грелся лишь 1m-буфер,
        но у свежих кандидатов нового отбора он ПУСТОЙ (монета только что
        попала в сетку), resample из STORE невозможен, и каждая такая
        плитка уходила в REST-очередь с таймаутом 8 c → «графики есть при
        выборе биржи, а после нового отбора пропадают».

        Очередь прогрева ограничена (не больше ohlcv_concurrency*2 за проход)
        и идёт через _sem_rest — медленная биржа не захлёбывается.
        """
        now = time.time()
        todo = []
        ck_tail = (tf, limit)
        for sym in symbols:
            cached = self._candles_cache.get((sym,) + ck_tail)
            if cached and now - cached[0] < GRID_TTL:
                continue                       # сетка этого ТФ уже прогрета
            st = self._state(sym)
            # WS kline уже принёс свечи нужного ТФ — REST не нужен вовсе
            if (getattr(st, "grid_ohlcv", None)
                    and getattr(st, "grid_ohlcv_tf", "") == tf
                    and now - getattr(st, "grid_ohlcv_ts", 0.0) < GRID_TTL):
                continue
            if st.ohlcv and now - getattr(st, "ohlcv_ts", 0.0) < CANDLES_TTL \
                    and self._fresh_candles_from_store(sym, tf, limit, now) is not None:
                continue                       # resample из свежего 1m закроет нужду
            todo.append(sym)
        if not todo:
            return
        chunk = max(1, self.settings.ohlcv_concurrency)
        await asyncio.gather(*[self._warm_tf(s, tf, limit)
                               for s in todo[:chunk * 2]],
                             return_exceptions=True)

    async def _warm_tf(self, symbol: str, tf: str, limit: int) -> None:
        """Разовое REST-получение свечей нужного ТФ в _candles_cache (GRID_TTL)."""
        if symbol not in self.ex.markets:
            return
        async with self._sem_rest:
            try:
                candles = await self.ex.fetch_ohlcv(symbol, tf, limit=limit)
                STORE.bump("rest_calls")
            except Exception as e:  # noqa: BLE001
                STORE.bump("errors")
                log.debug("[%s] grid-warm %s %s: %s: %s", self.cfg.label, symbol,
                          tf, type(e).__name__, str(e)[:140])
                return
            if candles:
                self._candles_cache[(symbol, tf, limit)] = (time.time(), candles)
                # параллельно наполняем 1m-буфер для будущих resample
                if not self._state(symbol).ohlcv:
                    asyncio.create_task(self._fetch_ohlcv(symbol, force=True,
                                                          ttl=GRID_TTL),
                                        name=f"{self.cfg.id}:warm1m:{symbol}")

    async def watch_candles(self, symbols: list[str], tf: str, limit: int) -> None:
        """
        WS-подписка на kline для сетки графиков (там, где ccxt.pro её умеет).

        Ключевое отличие от REST-прогрева: свечи приходят САМИ, st.ohlcv_ts
        обновляется на каждый тик — и grid-cache после истечения GRID_TTL
        пересобирается мгновенно из свежих данных без единого запроса к бирже.
        Именно поэтому «графики есть при выборе биржи, а через минуту
        пропадают»: при паузе фокуса стримы биржи закрываются, 1m-буфер
        перестаёт пополняться, CANDLES_TTL (12 c) истекает, и каждая плитка
        уходит в REST-очередь медленной биржи, где большинство ячеек
        отваливается по таймауту → «нет свечей».

        Поток живёт как обычная поточечная задача (_candle_tasks): его снимают
        pause/resume/_close_streams вместе с остальными подписками, ротация
        состава — по принципу last-writer-wins (повторный вызов заменяет набор).
        """
        if self._stop.is_set():
            return
        # Целевой набор сетки — переживает паузу: именно по нему ротация
        # решает, какие kline-потоки НЕ закрывать (keep_candles в _close_streams).
        self._grid_syms = set(symbols)
        # набор плиток изменился: снимаем потоки символов, которых больше нет
        # в запросе (у старых топиков освобождаем messageHashes ccxt — иначе
        # следующая подписка тем же символом получит «already subscribed»)
        want = set(symbols)
        for sym in [s for s in self._candle_tasks if s not in want]:
            t = self._candle_tasks.pop(sym, None)
            if t:
                t.cancel()
            await self._drop_single_subscriptions("candles", [sym])
        # СНИМАЕМ старые kline-потоки символов, которые переподписываются на
        # ДРУГОЙ таймфрейм. Без этого watch_ohlcv_for_symbols([sym, новый_tf])
        # натыкался на уже зарегистрированный messageHash того же символа —
        # ccxt никогда не резолвил новый future, задача «висела вечно», и
        # сетка оставалась без свечей до REST-прогрева («свечи пропадают при
        # новом отборе»: фронт переключает tf → ротация подписок ломалась).
        resub = [s for s in symbols
                 if s in self._candle_tasks and self._candle_tfs.get(s) != tf]
        for sym in resub:
            t = self._candle_tasks.pop(sym, None)
            if t:
                t.cancel()
            await self._drop_single_subscriptions("candles", [sym])
        prev_tf = getattr(self, "_candle_tf", "")
        self._candle_tf = tf
        added = 0
        for sym in symbols:
            if sym not in self.ex.markets:
                continue
            t = self._candle_tasks.get(sym)
            if t is not None and not t.done():
                continue
            if t is not None:
                # задача потока УМЕРЛА (ccxt NotSupported/NetworkError/
                # BadSymbol): словарь мог остаться с finished-задачей —
                # раньше watch пропускал её («sym in _candle_tasks →
                # continue»), и символ навсегда оставался без WS-свечей,
                # даже когда биржа снова отвечала. Чистим запись и
                # переподписываемся заново.
                self._candle_tasks.pop(sym, None)
                self._candle_tfs.pop(sym, None)
            self._candle_tfs[sym] = tf
            t = asyncio.create_task(self._candle_stream(sym, tf),
                                    name=f"{self.cfg.id}:candle:{sym}")
            self._candle_tasks[sym] = t

            def _on_done(x, s=sym):
                if self._candle_tasks.get(s) is x:
                    self._candle_tasks.pop(s, None)
                    self._candle_tfs.pop(s, None)
            t.add_done_callback(_on_done)
            added += 1
        if added or resub:
            log.info("[сетка %s] ws-kline: +%d потоков tf=%s (был %s), "
                     "переподписано %d, всего активных %d/%d",
                     self.cfg.label, added, tf, prev_tf or "-", len(resub),
                     len(self._candle_tasks), len(want))

    async def _candle_stream(self, symbol: str, tf: str) -> None:
        while not self._stop.is_set():
            if self._paused:
                await asyncio.sleep(2.0)   # пауза фокуса: ждём resume, не лезем на биржу
                continue
            try:
                ohlcv = await self.ex.watch_ohlcv_for_symbols([[symbol, tf]])
                candles = None
                if isinstance(ohlcv, dict):
                    v = ohlcv.get(symbol)
                    if isinstance(v, dict):
                        candles = v.get("info") or v.get("candles") or v.get("ohlcv")
                    elif isinstance(v, list):
                        candles = v
                elif isinstance(ohlcv, list) and ohlcv:
                    first = ohlcv[0]
                    candles = first.get("candles") if isinstance(first, dict) else None
                if candles:
                    st = self._state(symbol)
                    # ВАЖНО: пишем в отдельный буфер st.grid_ohlcv, а НЕ через
                    # apply_ohlcv(). WS kline отдаёт свечи ЗАПРОШЕННОГО ТФ (5m),
                    # а st.ohlcv — это 1m-буфер: NATR/фокус/скринер считаются
                    # именно по нему. Раньше apply_ohlcv затирал 1m-историю
                    # горсткой 5m-свечей → natr падал до 0 → монета вылетала
                    # из отбора → состав сетки менялся каждые ~30 с →
                    # perpetual churn подписок → «плитки есть, свечей нет».
                    # Плюс метрики скринера начинали считаться по чужому ТФ.
                    if (not isinstance(candles, list) or not candles
                            or not isinstance(candles[0], (list, tuple))):
                        continue
                    st.grid_ohlcv = [list(c) for c in candles][-600:]
                    st.grid_ohlcv_tf = tf
                    st.grid_ohlcv_ts = time.time()
                    # держим 1m-буфер живым для resample/NATR: если он пуст или
                    # протух (пауза биржи вне фокуса добивала и этот поток),
                    # фоновый REST подкачает 1m через _sem_rest
                    if not st.ohlcv or time.time() - st.ohlcv_ts >= CANDLES_TTL:
                        t = asyncio.create_task(
                            self._fetch_ohlcv(symbol),
                            name=f"{self.cfg.id}:gridfill:{symbol}")
                        _GRID_WARM_TASKS.add(t)
                        t.add_done_callback(_GRID_WARM_TASKS.discard)
                    STORE.bump("ws_messages")
            except asyncio.CancelledError:
                return
            except (ccxt.BadSymbol, ccxtpro.BadSymbol):
                self._candle_tasks.pop(symbol, None)
                return
            except (ccxt.NotSupported, ccxt.BadRequest, ccxt.ArgumentsRequired):
                # биржа не отдаёт kline по WS — тихо выходим, остаётся REST-путь
                self._candle_tasks.pop(symbol, None)
                return
            except (ccxt.NetworkError, ccxtpro.NetworkError):
                await asyncio.sleep(1)
            except Exception as e:  # noqa: BLE001
                if "already subscribed" in str(e).lower():
                    await asyncio.sleep(2.0)   # гонка пересборки — см. _book_stream_batch
                    continue
                log.debug("[%s] candle %s: %s: %s", self.cfg.label, symbol,
                          type(e).__name__, str(e)[:140])
                await asyncio.sleep(2)

    # ------------------------------------------------------------------
    # Свечи для графиков (по требованию, с кэшем)
    # ------------------------------------------------------------------
    async def fetch_candles(self, symbol: str, tf: str, limit: int) -> list:
        """
        OHLCV для карточки инструмента. Кэш на CANDLES_TTL секунд: график
        открывают часто, а дёргать биржу на каждый запрос — путь к 429.

        Свежесть кэша проверяется отдельно для каждой «поколенческой» пары
        (tf, limit): если у символа уже есть свежие 1m-свечи на весь нужный
        диапазон, они отдаются сразу без похода на биржу. Без этого каждый
        второй график (после истечения 12-секундного TTL) ждал ответ медленной
        биржи 8+ секунд и фронт уходил в «бэкенд не ответил».
        """
        cache_key = (symbol, tf, limit)
        now = time.time()
        hit = self._candles_cache.get(cache_key)
        if hit and now - hit[0] < CANDLES_TTL:
            return hit[1]
        fresh = self._fresh_candles_from_store(symbol, tf, limit, now)
        if fresh is not None:
            # переупаковываем в кэш под этим ключом: следующий запрос в пределах
            # CANDLES_TTL попадёт в hit и не будет трогать ни STORE, ни биржу
            self._candles_cache[cache_key] = (now, fresh)
            return fresh
        if symbol not in self.ex.markets:
            return []
        try:
            candles = await self.ex.fetch_ohlcv(symbol, tf, limit=limit)
            STORE.bump("rest_calls")
        except Exception as e:  # noqa: BLE001
            log.debug("[%s] fetch_candles %s %s: %s", self.cfg.label, symbol, tf, str(e)[:140])
            STORE.bump("errors")
            return hit[1] if hit else []
        self._candles_cache[cache_key] = (time.time(), candles)
        if len(self._candles_cache) > CANDLES_CACHE_MAX:
            self._prune_candles_cache(time.time())
        return candles

    def _fresh_candles_from_store(self, symbol: str, tf: str, limit: int,
                                  now: float) -> Optional[list]:
        """
        Подходящие ли 1m-свечи лежат в STORE, чтобы отдать график без биржи?

        Возвращает список свечей нужного ТФ либо None (нужен REST). Условия:
          * st.ohlcv свежая (не старше CANDLES_TTL);
          * таймфрейм 1m и запрошенное число свечей влезает в накопленный
            буфер (обычно это ~400 минут ≈ 6.6 часа истории);
          * сам символ принадлежит этому коллектору (иначе возьмём чужой
            формат объёма).
        """
        st = STORE.get(f"{self.cfg.id}:{symbol}")
        if st is None:
            return None
        from .metrics import TF_SECONDS, resample   # локально: снять риск цикла импортов
        tf_sec = TF_SECONDS.get(tf)
        if tf_sec is None:
            return None
        # Приоритет 1: WS kline-поток сетки уже держит готовые свечи НУЖНОГО ТФ
        # (st.grid_ohlcv). Отдаём их мгновенно без биржи — это главный путь для
        # /api/grid: раньше при паузе/ротации фокуса буфер пустел, TTL истёк, и
        # каждая плитка уходила в REST-очередь с таймаутом → «свечи пропали».
        gbuf = getattr(st, "grid_ohlcv", None)
        if (gbuf and getattr(st, "grid_ohlcv_tf", "") == tf
                and now - getattr(st, "grid_ohlcv_ts", 0.0) < GRID_TTL):
            return [list(c) for c in gbuf[-limit:]]
        # Приоритет 1b: тот же WS-буфер, но протухший (биржа замолчала на
        # минуту). Раньше такая плитка шла в REST-очередь; если очередь была
        # занята (ROTATE снимал/ставил подписки, warm_for_grid держал семафор),
        # wait_for(8 c) отваливался и отдавал [] — фронт показывал «нет
        # свечей», хотя история у него уже была в руках. Лучше показать
        # чуть отстающий график, чем стереть живой: grid-буфер храним до
        # 600 свечей, он актуален минутами.
        if gbuf and getattr(st, "grid_ohlcv_tf", "") == tf \
                and len(gbuf) >= 2:
            return [list(c) for c in gbuf[-limit:]]
        # Приоритет 2: resample из свежего 1m-буфера скринера
        if not st.ohlcv:
            return None
        if now - getattr(st, "ohlcv_ts", 0.0) >= CANDLES_TTL:
            return None
        need_min = limit * 60 // max(1, tf_sec) + 2   # минут истории на нужный ТФ
        if len(st.ohlcv) < need_min:
            return None
        if tf_sec == 60:
            return [list(c) for c in st.ohlcv[-limit:]]
        return resample(st.ohlcv, tf_sec)[-limit:]

    def _prune_candles_cache(self, now: float) -> None:
        """
        Чистка кэша свечей: раньше записи только протухали (TTL проверялся при
        чтении), но НЕ удалялись — за долгую сессию кэш рос монотонно
        (ключ = symbol×таймфрейм×limit, запись = до 1000 свечей ≈ 100 КБ).

        Сначала выбрасываем протухшее; если и этого мало — самую старую
        половину сверх лимита (обычно это разовые запросы закрытых карточек).
        """
        expired = [k for k, (ts, _) in self._candles_cache.items()
                   if now - ts >= CANDLES_TTL]
        for k in expired:
            del self._candles_cache[k]
        if len(self._candles_cache) > CANDLES_CACHE_MAX:
            by_age = sorted(self._candles_cache, key=lambda k: self._candles_cache[k][0])
            excess = len(self._candles_cache) - CANDLES_CACHE_MAX // 2
            for k in by_age[:excess]:
                del self._candles_cache[k]


    def _to_ccxt_symbol(self, raw: str) -> Optional[str]:
        """
        Биржевой идентификатор (BTCUSDT / BTC_USDT / BTC-USDT-SWAP) → ccxt-символ.

        Все ветки отфильтрованы по типу рынка: у Gate/Bybit/Aster один и тот же
        id есть и у спота, и у свопа, поэтому наивный поиск «первого в markets»
        возвращал BTC/USDT (спот) вместо BTC/USDT:USDT — и OI/funding уходили
        не в тот инструмент.
        """
        if not raw:
            return None
        want = self.cfg.market
        cands = [raw, raw.replace("_", "/"), raw.replace("-", "/")]
        # сначала — точное совпадение по нужному типу рынка
        for c in cands:
            m = self.ex.markets.get(c)
            if m and m.get("type") == want:
                return c
        # затем — карта id (собрана с приоритетом своего типа рынка)
        if not self._id_map:
            self._build_id_map()
        hit = self._id_map.get(raw) or self._id_map.get(raw.upper())
        if hit:
            return hit
        # крайний случай: любой существующий символ (лучше чужой тип, чем ничего)
        for c in cands:
            if c in self.ex.markets:
                return c
        return None

    def _build_id_map(self) -> None:
        """
        Карта биржевого id → ccxt-символ.

        Один и тот же id бывает у спота и у свопа: у Gate таких коллизий 526,
        у Bybit 292, у Aster 40 («BTC_USDT» → и «BTC/USDT», и «BTC/USDT:USDT»).
        При наивной сборке побеждает последний записанный рынок, и OI/funding
        уезжают в спотовый инструмент вместо фьючерсного. Поэтому рынки
        чужого типа пишем первыми — свой тип перезапишет их.
        """
        items = list(self.ex.markets.items())
        ours = [(s_, m) for s_, m in items if m.get("type") == self.cfg.market]
        other = [(s_, m) for s_, m in items if m.get("type") != self.cfg.market]
        for sym, m in other + ours:
            self._id_map[str(m.get("id") or "")] = sym
            self._id_map[sym.replace("/", "")] = sym

    # ------------------------------------------------------------------
    async def _snapshot_dump_loop(self) -> None:
        """Периодически пишет снапшот на диск — из него потом работает replay-режим."""
        path = self.settings.replay_file
        while not self._stop.is_set():
            await asyncio.sleep(SNAPSHOT_EVERY)
            try:
                rows = STORE.snapshot()
                if not rows:
                    continue
                payload = {
                    "ts": time.time(),
                    "mode": "snapshot",
                    "rows": rows[:2000],
                    "overview": STORE.overview(),
                }
                with open(path, "w", encoding="utf-8") as fh:
                    json.dump(payload, fh, separators=(",", ":"))
                log.info("[%s..] snapshot dumped: %d rows -> %s", self.cfg.label, len(payload["rows"]), path)
            except Exception as e:  # noqa: BLE001
                log.debug("snapshot dump failed: %s", e)


# --------------------------------------------------------------------------
# HTTP-хелперы (синхронные, для тред-пула)
# --------------------------------------------------------------------------
def _get_json(url: str, timeout: float = 8.0):
    req = urllib.request.Request(url, headers={"User-Agent": "crypto-screener/0.1"})
    with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310
        return json.loads(r.read().decode("utf-8"))


def _f(x) -> Optional[float]:
    try:
        if x in (None, ""):
            return None
        v = float(x)
        return v if v == v else None  # отбрасываем NaN
    except (TypeError, ValueError):
        return None


def _find_last(tickers: dict, base: str) -> Optional[float]:
    for q in ("USDT", "USDC", "USD"):
        for suffix in (f"{base}/{q}", f"{base}/{q}:{q}"):
            t = tickers.get(suffix)
            if t and t.get("last"):
                return float(t["last"])
    return None


# --------------------------------------------------------------------------
# Оркестрация всех бирж
# --------------------------------------------------------------------------
# Оркестратор хранит long-lived задачи бирж в списке (см. CollectorHub.tasks),
# а разовые фоновые прогревы сетки — здесь, во множестве: у list нет .discard(),
# и короткие таски не должны попадать в gather() остановки хаба.
_GRID_WARM_TASKS: set[asyncio.Task] = set()


class CollectorHub:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.collectors: list[ExchangeCollector] = []
        self.tasks: list[asyncio.Task] = []
        # кэш готовых сеток /api/grid: key → (ts, cells); см. get_grid()
        self._grids: dict[tuple, tuple[float, list]] = {}
        # WS kline-подписки сетки: collector id → {tf: набор символов}
        self._grid_subs: dict[str, dict[str, set]] = {}
        # незавершённые подписки по коллектору: не даём двум волнам /api/grid
        # одновременно дёргать watch/unwatch одного и того же стрима
        self._grid_sub_active: dict[str, int] = {}

    async def _resubscribe_grid(self, col, syms: list[str], tf: str) -> None:
        """
        Пересобрать WS kline-поток биржи под текущий набор плиток сетки.

        Повторный вызов с тем же составом — no-op; изменился набор или ТФ —
        старые потоки снимаются внутри watch_candles. Биржи без WS-kline
        (ccxt пройдёт в NotSupported) тихо остаются на REST-прогреве.

        «Графики пропадают при новом отборе» усугублялось гонкой: каждые
        ~15 с фронт звал /api/grid, каждый вызов создавал задачу подписки,
        а медленный watch_candles (отписка от старых пар + подписка на новые)
        выполнялся параллельно с себе подобными — ccxt.pro снимал подписку
        только что созданного потока, и плитки оставались без свечей. Теперь
        одновременна только одна подписка на коллектор, а устаревшая волна
        просто выходит (её состав уже не актуален).
        """
        want = set(syms)
        cur = self._grid_subs.setdefault(col.cfg.id, {})
        active = self._grid_sub_active.get(col.cfg.id, 0)
        if active:
            # другая подписка этого коллектора ещё выполняется — не лезем
            # вторым потоком в те же стримы; дождёмся её на следующем цикле
            return
        if cur.get(tf) == want and all(not t.done() for t in col._candle_tasks.values()):
            return
        # Состав СМЕНИЛСЯ (новый отбор): снимаем отметку «подписано» ДО await.
        # watch_candles у медленной биржи висит десятки секунд (un_watch_* по
        # каждой старой паре). Пока он висит, _grid_subs хранил УЖЕ НОВЫЙ
        # состав — следующая волна /api/grid (через 15 с) видела «состав
        # совпал», выходила как no-op, а реальные потоки ещё не были
        # подписаны → get_grid шёл в REST-очередь, получал таймаут 8 c и
        # отдавал плитки без свечей («свечи пропадают при новом отборе»).
        # Теперь устаревшая волна не считается успешной и повторит попытку.
        cur[tf] = None
        # ФЛАГ ЗАНЯТОСТИ ДО await. Раньше счётчик ставился после первого
        # await внутри watch_candles: две волны /api/grid успевали пройти
        # проверку «active==0» одна за другой в одном тике цикла событий и
        # параллельно отписывали/подписывали одни и те же kline-топики —
        # gacha «already subscribed»/пустые плитки при каждом новом отборе.
        self._grid_sub_active[col.cfg.id] = active + 1
        try:
            await col.watch_candles(sorted(want), tf, 0)
            cur[tf] = want      # отмечаем состав только когда подписка реальна
                                # (см. комментарий про cur[tf] = None выше)
        except Exception as e:  # noqa: BLE001
            log.debug("[сетка] ws-kline %s: %s: %s", col.cfg.label,
                      type(e).__name__, str(e)[:140])
        finally:
            self._grid_sub_active[col.cfg.id] = max(
                0, self._grid_sub_active.get(col.cfg.id, 1) - 1)

    async def start(self) -> None:
        if not self.settings.exchanges:
            log.warning("no exchanges enabled")
            return
        for cfg in self.settings.exchanges:
            c = ExchangeCollector(cfg, self.settings)
            self.collectors.append(c)
            self.tasks.append(asyncio.create_task(c.run(), name=f"hub:{cfg.id}"))
        log.info("collector hub started: %s", ", ".join(c.cfg.label for c in self.collectors))

    def find(self, exchange_id: str, market_type: str = "") -> Optional[ExchangeCollector]:
        """
        Коллектор по id+рынок или по лейблу+рынок.

        Лейбл теперь общий для спота и свопа одной биржи («Binance»), поэтому
        поиск по одному лейблу неоднозначен — рынок желателен. Если рынок пуст
        ("") — берём первого коллектора этой биржи (в сетке графиков тип рынка
        уже задан фильтром строк). Сравнение регистронезависимое: фронт шлёт
        лейблы вида «Aster», «MEXC», а cfg.id — это ccxt-идентификаторы в
        нижнем регистре («aster», «mexc»).
        """
        want = (exchange_id or "").lower()
        if not want:
            return None
        if market_type:
            for c in self.collectors:
                if c.cfg.market != market_type:
                    continue
                if c.cfg.id.lower() == want or c.cfg.label.lower() == want:
                    return c
        # запасной вариант: рынок не совпал (биржа подключена только одним рынком)
        for c in self.collectors:
            if c.cfg.id.lower() == want or c.cfg.label.lower() == want:
                return c
        return None

    async def fetch_candles(self, exchange_id: str, market_type: str,
                            symbol: str, tf: str, limit: int) -> list:
        c = self.find(exchange_id, market_type)
        if c is None:
            return []
        return await c.fetch_candles(symbol, tf, limit)

    # ------------------------------------------------------------------
    # Сетка графиков (/api/grid): кэш готовых ответов целиком
    # ------------------------------------------------------------------
    async def get_grid(self, key: tuple, rows: list[tuple[str, str]], tf: str,
                 limit: int, build_cell) -> list[dict]:
        """
        Ячейки сетки с кэшем на GRID_TTL секунд.

        Ключевое отличие от «кэша свечей на CANDLES_TTL»: ответ сетки
        переиспользуют ВСЕ клиенты и все варианты ключа (tf, limit), поэтому
        тикерные поля (цена/изменение) живут дольше своей TTL — их освежает
        WS-поток: фронт обновляет последнюю свечу каждой плитки на каждом
        тике (gridTick), а шапку — из строк скринера. Это позволяет отдавать
        /api/grid мгновенно без единого REST-запроса к бирже в steady-state:
        сетка больше не зависит от того, успела ли медленная биржа ответить за
        8 секунд.

        rows: [(exchange_id, symbol)] — маршрутизация и прогрев по ним.
        build_cell(row, candles) — упаковка строки+свечей в ячейку ответа.
        """
        now = time.time()
        hit = self._grids.get(key)
        if hit and now - hit[0] < GRID_TTL:
            return hit[1]
        cells = []
        by_col: dict = {}
        routes: list = []
        unresolved: set[str] = set()
        for row in rows:
            # row может быть кортежем (ex, symbol) — тогда market_type берём
            # из ключа символа ("mexc:BTC/USDT:USDT" -> swap), либо словарём
            # строки скринера.
            if isinstance(row, dict):
                rk = str(row.get("k") or "")
                # КЛЮЧ строки скринера имеет вид "<cfg.id>:<symbol>"
                # («binanceusdm:BTC/USDT:USDT»). Раньше маршрутизация шла по
                # полю "ex"/"exl": у части строк оно пустое или это ЛЕЙБЛ
                # («Binance»), а лейбл общий для спота и свопа — find() без
                # подсказки рынка возвращал ПЕРВЫЙ коллектор биржи. Для
                # Binance/Bybit/MEXC/Gate с приоритетом спот это значило:
                # своповые плитки молча уходили в спотовый REST-коллектор,
                # где символа нет → fetch_candles отдавал [] → «нет свечей»
                # ровно на тех монетах, что меняются при новом отборе.
                # Теперь первый сегмент ключа = точный cfg.id — всегда.
                if ":" in rk:
                    ex_id, sym = rk.split(":", 1)
                else:
                    ex_id, sym = (row.get("ex") or row.get("exl") or ""), \
                                 row.get("s") or ""
                mt_hint = "swap" if ":USDT:USDT" in sym or "/USDT:USDT" in sym \
                    else ("spot" if row.get("mt") == "spot" else "")
            else:
                ex_id, sym = row[0], row[1]
                mt_hint = "swap" if "/USDT:USDT" in sym or ":USDT:USDT" in sym else ""
            c = self.find(ex_id, mt_hint)
            if c is None:
                c = self.find(ex_id, "")
            if c is None:
                unresolved.add(str(ex_id))
            routes.append(c)
            if c is not None:
                by_col.setdefault(c, []).append(sym)
        if unresolved:
            log.warning("сетка: не найдены коллекторы для %s (доступны: %s)",
                        ", ".join(sorted(unresolved)),
                        ", ".join(f"{c.cfg.id}/{c.cfg.market}" for c in self.collectors))
        # WS kline-поток под текущий набор плиток. Состав сетки меняется
        # каждые ~15 с; метод async, но раньше он запускался фоновой задачей
        # НА КАЖДЫЙ вызов get_grid — параллельные волны подписок снимали
        # стримы друг у друга (см. _resubscribe_grid) и плитки рождались
        # без свечей. Теперь одновременна только одна подписка на коллектор,
        # устаревшие волны отбрасываются. Запуск через create_task +
        # wait_for(shield(t), 0): если в этом цикле событий уже есть I/O
        # (запросы к биржам), таймаут 0 мгновенно вернёт TimeoutError —
        # выдача ячеек не блокируется подпиской; если планировщик пуст
        # (тесты/демо), подписка успевает зарегистрироваться до gather.
        for c, syms in by_col.items():
            t = asyncio.create_task(self._resubscribe_grid(c, syms, tf),
                                    name=f"gridsub:{c.cfg.id}")
            _GRID_WARM_TASKS.add(t)
            t.add_done_callback(_GRID_WARM_TASKS.discard)
            try:
                await asyncio.wait_for(asyncio.shield(t), timeout=0)
            except asyncio.TimeoutError:
                pass
        # фон: добираем 1m-свечи недостающим символам (см. warm_for_grid)
        for c, syms in by_col.items():
            self._grid_warm(c, syms, tf, limit)
        async def one(row: tuple, c) -> list:
            if c is None:
                return []
            sym = row["s"] if isinstance(row, dict) else row[1]
            try:
                return await asyncio.wait_for(
                    c.fetch_candles(sym, tf, limit),
                    timeout=CANDLES_FETCH_TIMEOUT)
            except asyncio.TimeoutError:
                log.warning("свечи сетки %s %s: таймаут %.0f c — отдаём resample/пустоту",
                            row[0] if not isinstance(row, dict) else row.get("ex"),
                            sym, CANDLES_FETCH_TIMEOUT)
                return []
            except Exception as e:  # noqa: BLE001
                log.warning("свечи сетки %s: %s: %s",
                            sym, type(e).__name__, str(e)[:200])
                return []

        # параллельно по всем ячейкам: при последовательном опросе сетка из
        # 25 плиток на медленной бирже складывалась в минуты ожидания
        results = await asyncio.gather(*[one(r, c) for r, c in zip(rows, routes)])
        for row, candles in zip(rows, results):
            cells.append(build_cell(row, candles))
        self._grids[key] = (time.time(), cells)
        if len(self._grids) > GRID_CACHE_MAX:
            for k in sorted(self._grids, key=lambda k: self._grids[k][0])[:GRID_CACHE_MAX // 2]:
                del self._grids[k]
        return cells

    def _grid_warm(self, col, syms: list[str], tf: str, limit: int) -> None:
        """Разовая фоновая задача прогрева свечей сетки (не ждём ответа)."""
        t = asyncio.create_task(col.warm_for_grid(syms, tf, limit),
                                name=f"gridwarm:{col.cfg.id}")
        # self.tasks — список LONG-lived задач бирж (см. __init__/start);
        # у list нет .discard(), и сам прогрев не должен попадать в gather
        # остановки. Храним слабую ссылку, чтобы таск не собирались GC-раньше
        # времени: done-колбэк убирает его из «держателя».
        _GRID_WARM_TASKS.add(t)
        t.add_done_callback(_GRID_WARM_TASKS.discard)

    async def stop(self) -> None:
        for c in self.collectors:
            await c.stop()
        await asyncio.gather(*self.tasks, return_exceptions=True)
