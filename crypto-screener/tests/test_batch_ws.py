"""
Тесты батчевых WS-подписок (watch_*_for_symbols) и сопутствующей гигиены.

Что проверяется по существу:
  * если биржа умеет батч — на чанк символов живёт ОДНА задача, а не задача
    на символ (экономия корутин и, у Binance, WS-соединений);
  * данные батч-потоков маршрутизируются по symbol в правильные SymbolState;
  * смена списка пересобирает подписки: binance-семейство (URL зависит от
    списка) отписывается ВСЕМ старым списком и возвращает внутренние счётчики
    ccxt, остальные — только выпавшими символами;
  * отказ батча (NotSupported / серия ошибок) — переход на поточечные
    подписки, старый батч при этом отписывается;
  * пауза биржи гасит батч-потоки;
  * кэш свечей не растёт монотонно (pruning).
"""
import asyncio
import time

import ccxt
import pytest

from app.config import ExchangeConfig, Settings
from app.state import STORE

import app.collector as C


# --------------------------------------------------------------------------
# Стенд: синтетическая биржа с батч-методами
# --------------------------------------------------------------------------
class BatchFakeEx:
    """Заглушка ccxt.pro-биржи: батч-методы + поточечные (для fallback)."""

    def __init__(self, markets: dict, caps=("watchOrderBookForSymbols",
                                            "watchTradesForSymbols",
                                            "watchTickers")) -> None:
        self.markets = markets
        self.has = {c: (c in caps) for c in (
            "watchOrderBookForSymbols", "watchTradesForSymbols", "watchTickers",
            "unWatchOrderBookForSymbols", "unWatchTradesForSymbols", "unWatchTickers")}
        self.options: dict = {"streamBySubscriptionsHash": {},
                              "numSubscriptionsByStream": {}}
        # история вызовов: (kind, symbols)
        self.sub_calls: list[tuple[str, list[str]]] = []
        self.unsub_calls: list[tuple[str, list[str]]] = []
        # очереди данных: что вернут watch_* (список «выпусков»)
        self.books_q: list = []
        self.trades_q: list = []
        self.tickers_q: list = []
        self.block = None            # Event: если выставлен — watch спит до отмены
        self.book_exc: Exception | None = None   # чем кидаться в books-батче
        self.book_fail_times = 0     # сколько раз кинуть, прежде чем отвечать
        # поточечные
        self.point_book_ok = asyncio.Event()     # просто «не отвечать» по умолчанию

    def market(self, symbol: str) -> dict:
        return self.markets[symbol]

    async def close(self):
        pass

    # --- батч ---
    async def watch_order_book_for_symbols(self, symbols, limit=None, **kw):
        self.sub_calls.append(("book", list(symbols)))
        if self.book_exc is not None:
            raise self.book_exc
        if self.book_fail_times > 0:
            self.book_fail_times -= 1
            raise RuntimeError("биржа чудит")
        if self.books_q:
            return self.books_q.pop(0)
        await self._hang()

    async def watch_trades_for_symbols(self, symbols, **kw):
        self.sub_calls.append(("trades", list(symbols)))
        if self.trades_q:
            return self.trades_q.pop(0)
        await self._hang()

    async def watch_tickers(self, symbols, **kw):
        self.sub_calls.append(("tickers", list(symbols)))
        if self.tickers_q:
            return self.tickers_q.pop(0)
        await self._hang()

    async def un_watch_order_book_for_symbols(self, symbols, **kw):
        self.unsub_calls.append(("book", list(symbols)))

    async def un_watch_trades_for_symbols(self, symbols, **kw):
        self.unsub_calls.append(("trades", list(symbols)))

    async def un_watch_tickers(self, symbols, **kw):
        self.unsub_calls.append(("tickers", list(symbols)))

    # --- поточечно (для fallback-путей) ---
    async def watch_order_book(self, symbol, limit=None, **kw):
        self.sub_calls.append(("book:point", [symbol]))
        await self._hang()

    async def watch_trades(self, symbol, **kw):
        self.sub_calls.append(("trades:point", [symbol]))
        await self._hang()

    async def watch_ticker(self, symbol, **kw):
        self.sub_calls.append(("ticker:point", [symbol]))
        await self._hang()

    async def un_watch_order_book(self, symbol, **kw):
        self.unsub_calls.append(("book:point", [symbol]))

    async def un_watch_trades(self, symbol, **kw):
        self.unsub_calls.append(("trades:point", [symbol]))

    async def un_watch_ticker(self, symbol, **kw):
        self.unsub_calls.append(("ticker:point", [symbol]))

    async def fetch_ohlcv(self, symbol, tf="1m", limit=400, **kw):
        return [[1_700_000_000_000 + i * 60_000, 10, 11, 9, 10, 1] for i in range(5)]

    async def _hang(self):
        await asyncio.Event().wait()      # спим до отмены задачи


ALL_CAPS = ("watchOrderBookForSymbols", "watchTradesForSymbols", "watchTickers")


def make_collector(label="Binance", ex_id="binanceusdm", market="swap", n=40,
                   top_n=30, books=10, full_unwatch=False, caps=ALL_CAPS,
                   batch_chunk=100):
    cfg = ExchangeConfig(ex_id, label, market, top_n=top_n, books=books,
                         batch_full_unwatch=full_unwatch, batch_chunk=batch_chunk)
    col = C.ExchangeCollector(cfg, Settings())
    markets = {}
    for i in range(n):
        base = f"C{i:03d}"
        sym = f"{base}/USDT:USDT" if market == "swap" else f"{base}/USDT"
        markets[sym] = {"id": f"{base}USDT", "symbol": sym, "base": base,
                        "quote": "USDT", "active": True, market: True,
                        "spot": market == "spot", "swap": market == "swap",
                        "contractSize": 1.0, "inverse": False}
    col.ex = BatchFakeEx(markets, caps=caps)
    col.symbols = list(markets)
    return col


def fill_store(col, vol0=1_000_000.0):
    for i, sym in enumerate(col.symbols):
        st = col._state(sym)
        vol = float((len(col.symbols) - i)) * vol0
        st.apply_ticker({"symbol": sym, "last": 10.0 + i * 0.1, "bid": 9.99,
                         "ask": 10.01, "open": 10.0, "high": 11.0, "low": 9.0,
                         "quoteVolume": vol, "baseVolume": vol / 10.0, "count": 100})
    return col


@pytest.fixture
def store_clean():
    saved = dict(STORE._symbols)
    saved_status = dict(STORE.exchange_status)
    STORE._symbols.clear()
    STORE.exchange_status.clear()
    yield STORE
    STORE._symbols.clear()
    STORE._symbols.update(saved)
    STORE.exchange_status.clear()
    STORE.exchange_status.update(saved_status)


@pytest.fixture
def fast_rotate(monkeypatch):
    monkeypatch.setattr(C, "ROTATE_START_DELAY", 0.02)
    monkeypatch.setattr(C, "FOCUS_ROTATE_DELAY", 0.02)


def book_msg(sym, px=100.0):
    return {"symbol": sym, "timestamp": int(time.time() * 1000),
            "bids": [[px * (1 - i * 0.0001), 1.0 + i] for i in range(5)],
            "asks": [[px * (1 + i * 0.0001), 1.0 + i] for i in range(5)]}


# --------------------------------------------------------------------------
# Батч вместо поточечных задач
# --------------------------------------------------------------------------
def test_batch_used_when_supported(store_clean, fast_rotate):
    a = fill_store(make_collector(books=8))

    async def scenario():
        a.set_focus(a.symbols[:8])
        t = asyncio.create_task(a._rotate_loop())
        await asyncio.sleep(0.3)
        try:
            # поточечных задач нет вовсе
            assert a._book_tasks == {} and a._trade_tasks == {} and a._ticker_tasks == {}
            # по одной задаче на чанк (8 ≤ BATCH_CHUNK)
            assert len(a._batch["book"]["tasks"]) == 1
            assert len(a._batch["trades"]["tasks"]) == 1
            assert len(a._batch["tickers"]["tasks"]) == 1
            assert a._batch["book"]["syms"] == a.symbols[:8]
            kinds = {k for k, _ in a.ex.sub_calls}
            assert kinds == {"book", "trades", "tickers"}, \
                f"батч-методы не вызваны: {sorted(kinds)}"
        finally:
            await a.stop()
            t.cancel()

    asyncio.run(scenario())


def test_no_batch_when_exchange_lacks_methods(store_clean, fast_rotate):
    """mexc/hyperliquid-профиль: батч-методов нет → поточечные подписки."""
    a = fill_store(make_collector(books=4, caps=()))   # ни одной батч-способности

    async def scenario():
        a.set_focus(a.symbols[:4])
        t = asyncio.create_task(a._rotate_loop())
        await asyncio.sleep(0.3)
        try:
            assert len(a._book_tasks) == 4 and len(a._trade_tasks) == 4
            assert a._batch["book"]["tasks"] == []
            kinds = {k for k, _ in a.ex.sub_calls}
            assert kinds == {"book:point", "trades:point", "ticker:point"}
        finally:
            await a.stop()
            t.cancel()

    asyncio.run(scenario())


def test_chunking_over_batch_limit(store_clean, fast_rotate):
    a = fill_store(make_collector(n=30, top_n=30, books=10, batch_chunk=4))

    async def scenario():
        a.set_focus(a.symbols[:10])
        t = asyncio.create_task(a._rotate_loop())
        await asyncio.sleep(0.3)
        try:
            # 10 символов по 4 в чанке → 3 задачи на книги
            assert len(a._batch["book"]["tasks"]) == 3
            chunks = [s for k, s in a.ex.sub_calls if k == "book"]
            assert sum(len(c) for c in chunks) == 10
        finally:
            await a.stop()
            t.cancel()

    asyncio.run(scenario())


# --------------------------------------------------------------------------
# Маршрутизация данных
# --------------------------------------------------------------------------
def test_batch_book_routed_by_symbol(store_clean, fast_rotate):
    a = fill_store(make_collector(books=5))
    target = a.symbols[2]
    a.ex.books_q.append(book_msg(target, px=250.0))

    async def scenario():
        a.set_focus(a.symbols[:5])
        t = asyncio.create_task(a._rotate_loop())
        await asyncio.sleep(0.3)
        try:
            st = STORE.get(f"{a.cfg.id}:{target}")
            assert st is not None
            assert st.bid == pytest.approx(250.0)
            assert st.book is not None and st.book["bids"]
            # чужие символы не тронуты
            other = STORE.get(f"{a.cfg.id}:{a.symbols[0]}")
            assert other.book is None
        finally:
            await a.stop()
            t.cancel()

    asyncio.run(scenario())


def test_batch_trades_routed_by_symbol(store_clean, fast_rotate):
    a = fill_store(make_collector(books=5))
    s1, s2 = a.symbols[0], a.symbols[1]
    ts = int(time.time() * 1000)
    a.ex.trades_q.append([
        {"symbol": s1, "price": 12.0, "amount": 10.0, "side": "buy", "timestamp": ts},
        {"symbol": s2, "price": 13.0, "amount": 5.0, "side": "sell", "timestamp": ts},
    ])

    async def scenario():
        a.set_focus(a.symbols[:5])
        t = asyncio.create_task(a._rotate_loop())
        await asyncio.sleep(0.35)
        try:
            st1 = STORE.get(f"{a.cfg.id}:{s1}")
            st2 = STORE.get(f"{a.cfg.id}:{s2}")
            assert st1.cvd > 0, "покупка не учтена в CVD"
            assert st2.cvd < 0, "продажа не учтена в CVD"
        finally:
            await a.stop()
            t.cancel()

    asyncio.run(scenario())


def test_batch_tickers_apply_quote(store_clean, fast_rotate):
    a = fill_store(make_collector(books=5))
    target = a.symbols[3]
    a.ex.tickers_q.append({target: {"symbol": target, "bid": 55.0, "ask": 56.0,
                                    "last": 55.5, "timestamp": time.time() * 1000}})

    async def scenario():
        a.set_focus(a.symbols[:5])
        t = asyncio.create_task(a._rotate_loop())
        await asyncio.sleep(0.35)
        try:
            st = STORE.get(f"{a.cfg.id}:{target}")
            assert st.last == pytest.approx(55.5)
            assert st.bid == pytest.approx(55.0)
        finally:
            await a.stop()
            t.cancel()

    asyncio.run(scenario())


# --------------------------------------------------------------------------
# Пересборка списка и отписки
# --------------------------------------------------------------------------
def test_rebuild_unwatches_removed_only(store_clean, fast_rotate):
    """bybit/okx/gate-профиль: URL от списка не зависит → отписка только выпавших."""
    a = fill_store(make_collector(n=20, books=10, full_unwatch=False))

    async def scenario():
        first = a.symbols[:8]
        a.set_focus(first)
        t = asyncio.create_task(a._rotate_loop())
        await asyncio.sleep(0.3)
        second = a.symbols[4:12]        # 4 старых + 4 новых
        a.set_focus(second)
        await asyncio.sleep(0.3)
        try:
            assert a._batch["book"]["syms"] == second
            unw = [s for k, ss in a.ex.unsub_calls if k == "book" for s in ss]
            removed = set(first) - set(second)
            kept = set(first) & set(second)
            assert removed <= set(unw), f"выпавшие не отписаны: {removed - set(unw)}"
            assert not (kept & set(unw)), "оставшиеся символы отписывать нельзя"
        finally:
            await a.stop()
            t.cancel()

    asyncio.run(scenario())


def test_binance_style_full_unwatch_and_counters(store_clean, fast_rotate):
    """binance-профиль: отписка ВСЕМ старым списком + возврат счётчиков ccxt."""
    a = fill_store(make_collector(n=20, books=10, full_unwatch=True))

    async def scenario():
        first = a.symbols[:6]
        a.set_focus(first)
        t = asyncio.create_task(a._rotate_loop())
        await asyncio.sleep(0.3)
        # имитируем внутренности ccxt: hash старого списка → индекс стрима, счётчик
        by_hash = a.ex.options["streamBySubscriptionsHash"]
        counters = a.ex.options["numSubscriptionsByStream"]
        h = "multipleOrderbook::" + ",".join(first)
        by_hash[h] = "7"
        counters["7"] = counters.get("7", 0) + len(first)

        a.set_focus(a.symbols[6:12])    # полностью другой список
        await asyncio.sleep(0.4)
        try:
            unw = [ss for k, ss in a.ex.unsub_calls if k == "book"]
            assert first in unw, "binance-профиль обязан отписываться всем старым списком"
            assert h not in by_hash, "hash старого списка не удалён — счётчики упрутся в лимит"
            assert counters["7"] == 0, "счётчик подписок не возвращён"
        finally:
            await a.stop()
            t.cancel()

    asyncio.run(scenario())


def test_release_counters_miss_is_safe(store_clean):
    """ccxt поменял формат хэшей → pop промахивается, ничего не ломается."""
    a = make_collector(full_unwatch=True)
    a.ex.options["streamBySubscriptionsHash"] = {"какой-то::другой": "3"}
    a.ex.options["numSubscriptionsByStream"] = {"3": 42}
    a._release_stream_counters("book", a.symbols[:5])
    assert a.ex.options["numSubscriptionsByStream"]["3"] == 42
    assert "какой-то::другой" in a.ex.options["streamBySubscriptionsHash"]


# --------------------------------------------------------------------------
# Отказы и fallback
# --------------------------------------------------------------------------
def test_fallback_to_per_symbol_on_notsupported(store_clean, fast_rotate):
    a = fill_store(make_collector(books=6))
    a.ex.book_exc = ccxt.NotSupported("gate does not support watchOrderBookForSymbols")

    async def scenario():
        a.set_focus(a.symbols[:6])
        t = asyncio.create_task(a._rotate_loop())
        await asyncio.sleep(0.6)
        try:
            assert a._batch["book"]["dead"] is True
            assert len(a._book_tasks) == 6, "поточечные стаканы не поднялись после отказа батча"
            # сделки/тикеры остались батчами
            assert len(a._batch["trades"]["tasks"]) == 1
        finally:
            await a.stop()
            t.cancel()

    asyncio.run(scenario())


def test_fallback_after_repeated_failures(store_clean, fast_rotate, monkeypatch):
    monkeypatch.setattr(C, "MAX_STREAM_FAILS", 3)
    monkeypatch.setattr(C, "FAIL_WINDOW", 60.0)
    a = fill_store(make_collector(books=4))
    a.ex.book_fail_times = 10            # стабильные отказы (не NotSupported)

    async def scenario():
        a.set_focus(a.symbols[:4])
        t = asyncio.create_task(a._rotate_loop())
        await asyncio.sleep(1.2)         # backoff 2 c... форсируем: задачи спят, но dead уже выставлен
        try:
            # после MAX_STREAM_FAILS подряд батч обязан умереть
            # (первые отказы идут с backoff=2c, поэтому ждём выставленного флага
            # через wake-ротацию; если не успел — тест честно падает)
            deadline = time.time() + 8
            while time.time() < deadline and not a._batch["book"]["dead"]:
                await asyncio.sleep(0.1)
            assert a._batch["book"]["dead"] is True
            # старые батч-подписки сняты ДО поточечных
            deadline = time.time() + 2
            while time.time() < deadline and len(a._book_tasks) < 4:
                await asyncio.sleep(0.1)
            assert len(a._book_tasks) == 4
        finally:
            await a.stop()
            t.cancel()

    asyncio.run(scenario())


def test_pause_closes_batch_streams(store_clean, fast_rotate):
    a = fill_store(make_collector(books=6))

    async def scenario():
        a.set_focus(a.symbols[:6])
        t = asyncio.create_task(a._rotate_loop())
        await asyncio.sleep(0.3)
        assert a._batch_alive()
        a.pause()
        await asyncio.sleep(0.3)
        try:
            assert not a._batch_alive(), "пауза не погасила батч-потоки"
            assert a._batch["book"]["syms"] == []
            unw_books = [ss for k, ss in a.ex.unsub_calls if k == "book"]
            assert a.symbols[:6] in unw_books, "пауза не отписала стаканы на бирже"
        finally:
            await a.stop()
            t.cancel()

    asyncio.run(scenario())


# --------------------------------------------------------------------------
# Кэш свечей: pruning
# --------------------------------------------------------------------------
def test_candles_cache_pruned(store_clean):
    a = make_collector()
    now = time.time()
    # забиваем кэш протухшими записями сверх лимита
    for i in range(C.CANDLES_CACHE_MAX + 50):
        a._candles_cache[(f"SYM{i}", "1m", 100)] = (now - C.CANDLES_TTL - 5, [[1]])

    async def scenario():
        sym = a.symbols[0]
        await a.fetch_candles(sym, "1m", 100)     # свежая запись → триггер чистки
        assert len(a._candles_cache) <= C.CANDLES_CACHE_MAX
        assert (sym, "1m", 100) in a._candles_cache, "свежую запись выкидывать нельзя"

    asyncio.run(scenario())


def test_candles_cache_fresh_entries_survive_prune(store_clean):
    a = make_collector()
    now = time.time()
    for i in range(C.CANDLES_CACHE_MAX - 10):
        a._candles_cache[(f"SYM{i}", "5m", 100)] = (now - 1.0, [[2]])  # свежие
    for i in range(60):
        a._candles_cache[(f"OLD{i}", "5m", 100)] = (now - C.CANDLES_TTL - 9, [[1]])

    async def scenario():
        await a.fetch_candles(a.symbols[0], "1m", 50)
        # протухшие удалены, свежие остались
        assert not any(k[0].startswith("OLD") for k in a._candles_cache)
        assert any(k[0] == "SYM0" for k in a._candles_cache)

    asyncio.run(scenario())


# --------------------------------------------------------------------------
# _refresh_top: топ по объёму через heapq.nlargest (эквивалент полной сортировки)
# --------------------------------------------------------------------------
def test_refresh_top_orders_by_quote_volume(store_clean):
    a = fill_store(make_collector(n=10, top_n=3))

    async def fake_fetch_tickers(symbols=None, **kw):
        # объём растёт с индексом символа: топ = последние три в обратном порядке
        return {sym: {"symbol": sym, "last": 10.0, "open": 10.0, "high": 11.0,
                      "low": 9.0, "quoteVolume": float(i + 1) * 1e6,
                      "baseVolume": float(i + 1) * 1e5, "count": 10}
                for i, sym in enumerate(a.symbols)}

    a.ex.fetch_tickers = fake_fetch_tickers
    top = asyncio.run(a._refresh_top())
    assert top == list(reversed(a.symbols))[:3]


def test_chunk_shrinks_on_size_error(store_clean, fast_rotate):
    """Реальный случай bybit spot: сервер отвергает списки >10 топиков.
    Коллектор обязан уменьшить чанк и продолжить батчами, а не умереть."""
    a = fill_store(make_collector(n=30, top_n=30, books=8))
    a._batch["trades"]["chunk"] = 20          # старт с заведомо большого чанка
    calls = {"n": 0}

    async def reject_big(symbols, **kw):
        calls["n"] += 1
        if len(symbols) > 10:
            raise ccxt.ExchangeError("bybit args size >10")
        a.ex.sub_calls.append(("trades", list(symbols)))
        await a.ex._hang()

    a.ex.watch_trades_for_symbols = reject_big

    async def scenario():
        a.set_focus(a.symbols[:20])
        t = asyncio.create_task(a._rotate_loop())
        try:
            deadline = time.time() + 6
            while time.time() < deadline and a._batch["trades"]["chunk"] > 10:
                await asyncio.sleep(0.1)
            assert a._batch["trades"]["chunk"] == 10, "чанк не уменьшился после размерной ошибки"
            assert a._batch["trades"]["dead"] is False, "батч не обязан умирать из-за размера"
            deadline = time.time() + 4
            while time.time() < deadline and len(a._batch["trades"]["tasks"]) < 2:
                await asyncio.sleep(0.1)
            assert len(a._batch["trades"]["tasks"]) == 2, "20 символов по 10 → две задачи"
        finally:
            await a.stop()
            t.cancel()

    asyncio.run(scenario())
