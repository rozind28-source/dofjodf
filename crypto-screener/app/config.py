"""
Конфигурация Crypto Screener (self-hosted prototype).

Все настройки читаются из переменных окружения, чтобы не править код.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _env_list(name: str, default: str) -> list[str]:
    raw = os.getenv(name, default)
    return [x.strip() for x in raw.split(",") if x.strip()]


@dataclass
class ExchangeConfig:
    """Одна биржа = один ccxt-класс + тип рынка (spot / swap)."""

    id: str            # имя ccxt-класса, напр. "binance", "binanceusdm"
    label: str         # как показываем в UI
    market: str        # "spot" | "swap"
    enabled: bool = True
    top_n: int = 250   # сколько инструментов реально стримим (по объёму)
    books: int = 60    # для скольких держим стакан (плотности) — самое дорогое
    book_limit: int = 200  # глубина стакана; у Binance валидны только 5/10/20/50/100/500/1000
    dex: bool = False      # децентрализованная биржа (метка для UI и для bulk-API)
    oi_top_n: int = 40     # скольким символам из топа добираем OI точечно (bulk нет)
    # MEXC/Hyperliquid отдают fetch_tickers заметно медленнее остальных (6-7 c),
    # поэтому им нужен более редкий опрос, иначе цикл не успевает прокрутиться
    ticker_refresh: float = 0.0   # 0 = взять глобальный TICKER_REFRESH
    candles: bool = True       # доступна ли подкачка свечей для графиков
    # В каких единицах биржа отдаёт объём в OHLCV. Проверено по исходникам ccxt:
    #   gate/mexc  → контракты (contractSize=0.0001 BTC), нужен пересчёт
    #   okx        → index 6 = base volume, уже в BTC, пересчёт НЕ нужен
    #   binance    → index 5 = base volume (для inverse — index 7)
    #   aster/bybit/hyperliquid → base volume, contractSize=1
    # Без этого флага гистограмма объёма на графике расходится с реальностью
    # в сотни раз (у OKX получалось $75k/мин вместо ~$7M).
    ohlcv_vol_in_contracts: bool = False
    # Сколько последних сделок ccxt держит в кэше на символ. Дефолт ccxt — 1000,
    # это ~300 КБ на символ: при 8 биржах × 50 символов только кэш сделок
    # занимает ~120 МБ. Нам нужно лишь столько, чтобы не потерять сделки между
    # двумя вызовами watch_trades, поэтому 200 с большим запасом.
    trades_limit: int = 200
    # Батчевые WS-подписки (watch_*_for_symbols): при смене списка символов
    # отписываться ВСЕМ старым списком, а не только выпавшими символами.
    # Нужно биржам семейства Binance: у них WS-URL строится из списка символов
    # (отдельное соединение на каждый уникальный список), поэтому старый список
    # обязан быть снят целиком — иначе дубли подписок и «мёртвые» соединения.
    # У bybit/okx/gate/aster URL от списка не зависит: там отписка только
    # выпавших символов дешевле (оставшиеся не переподписываются).
    batch_full_unwatch: bool = False
    # Сколько символов в одной батч-подписке (одна задача = один чанк).
    # Bybit SPOT на сервере принимает не больше 10 топиков на запрос
    # («args size >10», проверено живым прогоном); linear — до 20, но не
    # документировано, поэтому 10 на оба рынка bybit. У остальных бирж
    # ограничение ccxt — 200 (binance), берём 100 с запасом.
    # Если биржа всё же отвергнет список, коллектор уменьшит чанк вдвое
    # (см. _batch_fail) и повторит, так что это лишь стартовая точка.
    batch_chunk: int = 100
    options: dict = field(default_factory=dict)


# Биржи. Все проверены живыми запросами (см. EXCHANGES.md):
# что отвечает, сколько рынков, какие возможности по funding/OI/OHLCV.
#
# book_limit — валидная глубина стакана: у Binance это 5/10/20/50/100/500/1000,
# остальные принимают 200.
# Биржи. Лейбл = ИМЯ БИРЖИ без указания рынка: в интерфейсе (как в оригинале)
# чипсы выбирают биржу, а рынок переключается отдельным тумблером
# «Фьючерсы / Спот». Поэтому у одной биржи может быть две конфигурации
# (spot и swap) с одинаковым лейблом — их различают cfg.id и cfg.market,
# а ключ инструмента в STORE строится по id, так что коллизий нет.
CORE_EXCHANGES: list[ExchangeConfig] = [
    # --- фьючерсы ---
    ExchangeConfig("binanceusdm", "Binance",     "swap", top_n=300, books=80, book_limit=100,
                   batch_full_unwatch=True),
    ExchangeConfig("bybit",       "Bybit",       "swap", top_n=250, books=60, batch_chunk=10),
    ExchangeConfig("okx",         "OKX",         "swap", top_n=250, books=60),
    ExchangeConfig("mexc",        "MEXC",        "swap", top_n=250, books=50, ticker_refresh=30.0,
                   ohlcv_vol_in_contracts=True),
    # Gate futures.order_book_update принимает limit не больше 100:
    # при 200 отвечает BadRequest на ВСЕ символы (проверено замером).
    ExchangeConfig("gate",        "Gate.io",     "swap", top_n=250, books=50, book_limit=100,
                   ohlcv_vol_in_contracts=True),
    # --- спот: нужен для тумблера «Фьючерсы / Спот».
    # Спот-коллекторы дешевле по подпискам, но метаданные рынков всё равно
    # стоят памяти — для экономии отключайте рынки через MARKETS=swap.
    ExchangeConfig("binance",     "Binance",     "spot", top_n=200, books=30, book_limit=100,
                   batch_full_unwatch=True),
    ExchangeConfig("bybit",       "Bybit",       "spot", top_n=150, books=20, batch_chunk=10),
    ExchangeConfig("okx",         "OKX",         "spot", top_n=150, books=20),
    ExchangeConfig("mexc",        "MEXC",        "spot", top_n=150, books=20, ticker_refresh=30.0),
    ExchangeConfig("gate",        "Gate.io",     "spot", top_n=150, books=20, book_limit=100),
    # --- DEX (perp): спота у них нет, тумблер это учитывает ---
    ExchangeConfig("aster",       "Aster",       "swap", top_n=200, books=40, dex=True),
    ExchangeConfig("hyperliquid", "Hyperliquid", "swap", top_n=200, books=40, dex=True, ticker_refresh=30.0),
]

# Эти в ccxt есть, но проверкой не прошли — включаются вручную на свой риск.
EXTRA_EXCHANGES: list[ExchangeConfig] = [
    ExchangeConfig("bitget",        "Bitget",          "swap", top_n=200, books=40, enabled=False),
    # KuCoin Futures принимает только 50/100 — при 200 отвечает ExchangeError
    ExchangeConfig("kucoinfutures", "KuCoin Futures",  "swap", top_n=150, books=30,
                   book_limit=100, enabled=False),
    # HTX: watch_order_book падает на любой глубине → стакан не держим вовсе
    ExchangeConfig("htx",           "HTX",             "swap", top_n=120, books=0,
                   enabled=False),
    ExchangeConfig("bingx",         "BingX",           "swap", top_n=120, books=25, enabled=False),
    ExchangeConfig("kraken",        "Kraken",          "spot", top_n=100, books=20,
                   book_limit=100, enabled=False),
    # dYdX: ccxt.fetchTicker() не реализован → скринер не сможет получить цены.
    # Paradex: load_markets() возвращает 0 рынков, WS отвечает 400.
    # Оба оставляем объявленными, но disabled — включать бессмысленно до фикса в ccxt.
    ExchangeConfig("dydx",          "dYdX",            "swap", top_n=100, books=20, enabled=False, dex=True),
    ExchangeConfig("paradex",       "Paradex",         "swap", top_n=100, books=20, enabled=False, dex=True),
]


@dataclass
class Settings:
    host: str = "0.0.0.0"
    port: int = 8000

    # --- режим работы источника данных ---
    # live   — реальные WebSocket-потоки бирж через ccxt.pro
    # replay — детерминированная симуляция из записанного снапшота (демо/офлайн/тесты)
    mode: str = "live"
    replay_file: str = "data/replay_snapshot.json"
    replay_speed: float = 1.0

    exchanges: list[ExchangeConfig] = field(default_factory=list)
    quote_assets: list[str] = field(default_factory=lambda: ["USDT", "USDC", "USD", "BUSD", "FDUSD"])

    # --- сбор данных ---
    ticker_refresh: float = 20.0     # полный REST-срез тикеров, сек
    fetch_ohlcv: bool = True         # качать 1m-свечи для NATR / мульти-ТФ
    ohlcv_limit: int = 400           # ~6.6 часа 1m-свечей: хватает на 4h-доходность
    ohlcv_concurrency: int = 6       # сколько параллельных запросов на биржу
    tf_seconds: tuple = (60, 300, 900, 3600, 14400)   # 1м / 5м / 15м / 1ч / 4ч

    # --- производные метрики ---
    history_window: int = 900        # сколько снапшотов цены держим в памяти (1 сек ≈ 15 мин)
    volume_spike_ratio: float = 3.0  # во сколько раз объём должен превысить базу, чтобы засчитать спайк
    trades_spike_ratio: float = 3.0
    big_density_usd: float = 50_000.0  # порог «крупной плотности» в стакане, $
    huge_density_usd: float = 250_000.0
    # Как часто пересчитывать плотности/имбаланс стакана (тяжёлая часть
    # apply_book, ~290 мкс на книгу 200+200). Стаканы приходят 5–10 раз/с
    # на символ × ~510 книг → без троттлинга это 1–1.7 ядра и «тормозит всё».
    # 0.25 с = 4 Гц на символ: глазу плотности быстрее не нужны. 0 — без троттлинга.
    book_density_interval: float = 0.25

    # --- стрим во фронтенд ---
    push_interval: float = 1.0       # как часто шлём батч подписчикам, сек
    push_max_symbols: int = 400      # лимит символов в одном батче (защита канала)

    # --- алерты ---
    alert_cooldown: float = 60.0     # сек между повторными срабатываниями одного правила на символе

    # --- надёжность подключения к биржам ---
    # Таймаут одного запроса, мс. Дефолт ccxt — 10 000, но load_markets у Gate
    # занимает ~19 c, а у Hyperliquid ~16 c: запросов несколько, и на медленном
    # канале отдельный запрос легко выходит за 10 c.
    request_timeout: int = 20_000
    # Сколько раз повторять load_markets при неудаче. Без ретраев один
    # транзиентный сбой DNS навсегда оставляет биржу неподключённой до
    # перезапуска процесса — именно так выглядел первый запуск у пользователя.
    load_retries: int = 3
    # Как часто заново пытаться подключить биржу, отвалившуюся на старте.
    revive_interval: float = 120.0

    telegram_token: str = ""
    telegram_chat_id: str = ""
    # прокси для REST и WebSocket бирж. Если пусто — берутся HTTPS_PROXY/HTTP_PROXY
    # из окружения. Нужно, когда биржи блокируют регион или трафик идёт через VPN.
    proxy_url: str = ""


# Проверено живыми запросами (2026-10-01): какие depth принимает каждая биржа.
# Невалидное значение = BadRequest на ВСЕ подписки стакана этой биржи, причём
# снаружи это выглядит как «на бирже просто нет плотностей».
# Ключ — пара (ccxt id, рынок), потому что валидные глубины у спота и свопа
# ОДНОЙ биржи различаются. Замерено живыми запросами (2026-10-01).
#
# Невалидное значение = BadRequest на ВСЕ подписки стакана этой биржи, и снаружи
# это выглядит как «на бирже просто нет плотностей» — молчаливая потеря данных.
# Пустой кортеж означает, что стакан не работает вовсе: тогда books обязан быть 0.
VALID_BOOK_LIMITS: dict[tuple[str, str], tuple[int, ...]] = {
    # --- подключены по умолчанию ---
    ("binance", "spot"):       (5, 10, 20, 50, 100, 500, 1000, 5000),
    ("binanceusdm", "swap"):   (5, 10, 20, 50, 100, 500, 1000),  # 200 → BadRequest -4021
    ("bybit", "swap"):         (1, 50, 200, 500),                # 100 → BadRequest
    ("bybit", "spot"):         (1, 50, 200, 500),
    ("okx", "swap"):           (100, 200, 400),                  # 50 → AuthenticationError
    ("okx", "spot"):           (100, 200, 400),
    ("mexc", "swap"):          (50, 100, 200, 500),              # всегда отдаёт ~1500 уровней
    ("gate", "swap"):          (10, 20, 50, 100),                # 200/500 → BadRequest
    ("aster", "swap"):         (5, 10, 20, 50, 100, 200),        # фактически возвращает 20
    ("hyperliquid", "swap"):   (5, 10, 20, 50, 100, 200),        # фактически возвращает 20
    # --- optional (EXTRA_EXCHANGES), тоже замерены ---
    ("bitget", "swap"):        (100, 200, 500),                  # 50 → BadRequest
    ("kucoinfutures", "swap"): (50, 100),                        # 200/500 → ExchangeError
    ("bingx", "swap"):         (50, 100, 200, 500),              # всегда отдаёт 100 уровней
    ("kraken", "spot"):        (100, 500),                       # 50/200 → NotSupported
    # Gate SPOT принимает любую глубину (всегда возвращает 50 уровней) —
    # ограничение «не больше 100» относится только к futures.order_book_update
    ("gate", "spot"):          (10, 20, 50, 100, 200, 500),
    ("mexc", "spot"):          (50, 100, 200, 500),
    # --- стакан не работает вовсе ---
    ("htx", "swap"):           (),   # watch_order_book → ExchangeError на любой глубине
    ("dydx", "swap"):          (),   # fetchTicker не реализован в ccxt
    ("paradex", "swap"):       (),   # load_markets даёт 0 рынков, WS → 400
}


def load_settings() -> Settings:
    s = Settings()
    s.host = os.getenv("HOST", s.host)
    s.port = _env_int("PORT", s.port)
    s.mode = os.getenv("MODE", "live").strip().lower()
    s.replay_file = os.getenv("REPLAY_FILE", s.replay_file)
    s.replay_speed = _env_float("REPLAY_SPEED", s.replay_speed)
    s.quote_assets = _env_list("QUOTES", ",".join(s.quote_assets))

    enabled = [e for e in CORE_EXCHANGES]
    if _env_bool("EXCHANGES_ALL", False):
        for e in EXTRA_EXCHANGES:
            e.enabled = True
            enabled.append(e)
    # MARKETS=swap оставляет только фьючерсы (спот-коллекторы не поднимаются —
    # экономит память). MARKETS=spot,swap или пусто — оба рынка.
    markets_only = {m.strip().lower() for m in _env_list("MARKETS", "") if m.strip()}
    if markets_only:
        enabled = [e for e in enabled if e.market in markets_only]

    only = _env_list("EXCHANGES", "")
    if only:
        allowed = {x.lower() for x in only}
        pool = CORE_EXCHANGES + EXTRA_EXCHANGES
        enabled = []
        # Дедупликация по паре (id, рынок), а не по id: у биржи legitimately две
        # конфигурации (spot и swap), и «EXCHANGES=gate» обязана включать обе.
        # Гасить нужно только одинаковые пары (id, рынок), приходящие из CORE и EXTRA.
        taken: set[tuple[str, str]] = set()
        for e in pool:
            key = (e.id, e.market)
            by_label = e.label.lower() in allowed
            by_id = e.id.lower() in allowed and key not in taken
            if (by_label or by_id) and key not in taken \
                    and (not markets_only or e.market in markets_only):
                e.enabled = True
                enabled.append(e)
                taken.add(key)
    s.exchanges = [e for e in enabled if e.enabled]

    s.top_n_override = _env_int("TOP_N", 0)          # 0 = использовать per-exchange значение
    s.books_override = _env_int("BOOKS", 0)
    if s.top_n_override:
        for e in s.exchanges:
            e.top_n = s.top_n_override
    if s.books_override:
        for e in s.exchanges:
            e.books = min(s.books_override, e.top_n)

    s.ticker_refresh = _env_float("TICKER_REFRESH", s.ticker_refresh)
    # биржа может переопределить период опроса (MEXC/Hyperliquid медленные)
    for e in s.exchanges:
        if not e.ticker_refresh:
            e.ticker_refresh = s.ticker_refresh
    s.fetch_ohlcv = _env_bool("FETCH_OHLCV", s.fetch_ohlcv)
    s.ohlcv_limit = _env_int("OHLCV_LIMIT", s.ohlcv_limit)
    s.push_interval = _env_float("PUSH_INTERVAL", s.push_interval)
    s.big_density_usd = _env_float("BIG_DENSITY_USD", s.big_density_usd)
    s.book_density_interval = _env_float("BOOK_DENSITY_INTERVAL", s.book_density_interval)
    s.telegram_token = os.getenv("TELEGRAM_TOKEN", "")
    s.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    s.proxy_url = os.getenv("PROXY_URL", "").strip()
    s.request_timeout = _env_int("REQUEST_TIMEOUT", s.request_timeout)
    s.load_retries = _env_int("LOAD_RETRIES", s.load_retries)
    s.revive_interval = _env_float("REVIVE_INTERVAL", s.revive_interval)
    return s


SETTINGS = load_settings()
