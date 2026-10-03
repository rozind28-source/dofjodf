"""
Тесты фокус-режима: «стримим только top-N одной биржи по фильтру».

Что здесь проверяется по существу:
  * отбор действительно применяет ФИЛЬТР пользователя, а не сортировку по объёму;
  * для «дорогих» метрик (NATR) кандидатам добираются свечи — иначе фильтр по
    волатильности молча отсеял бы все монеты (natr=None);
  * подписки переводятся ровно на отобранный набор (strict: выпал → погас);
  * прочие биржи ставятся на паузу и восстанавливаются после выключения;
  * ограничения параметров не дают UI увести сервер в деградацию.
"""
import asyncio
import time

import pytest

from app.config import ExchangeConfig, Settings
from app.focus import (FOCUS_CANDIDATES_MAX, FocusManager, FocusSpec, clamp_interval,
                       clamp_limit, needs_ohlcv)
from app.metrics import SymbolState
from app.state import STORE


# --------------------------------------------------------------------------
# Стенд: синтетическая биржа без сети
# --------------------------------------------------------------------------
class FakeEx:
    """Заглушка ccxt.pro-биржи: только то, что дёргает коллектор в фокусе."""

    def __init__(self, markets: dict) -> None:
        self.markets = markets
        self.ohlcv_calls: list[str] = []
        self.ohlcv_fail: set[str] = set()
        self.unwatched: list[tuple[str, str]] = []

    def market(self, symbol: str) -> dict:
        return self.markets[symbol]

    async def fetch_ohlcv(self, symbol, tf="1m", limit=400, **kw):
        self.ohlcv_calls.append(symbol)
        if symbol in self.ohlcv_fail:
            raise RuntimeError("нет свечей")
        # синтетика с реальной волатильностью: NATR получается ненулевым
        base = 100.0 + (hash(symbol) % 50)
        out, t = [], 1_700_000_000_000
        for i in range(60):
            o = base + i * 0.7 + (i % 5)
            h, l, c = o + 1.5, o - 1.2, o + 0.6
            out.append([t + i * 60_000, o, h, l, c, 10.0 + i])
        return out

    async def close(self):
        pass

    # un_watch_* — снимаем подписку на стороне биржи
    async def un_watch_order_book(self, symbol, *a, **k):
        self.unwatched.append(("book", symbol))

    async def un_watch_trades(self, symbol, *a, **k):
        self.unwatched.append(("trades", symbol))

    async def un_watch_ticker(self, symbol, *a, **k):
        self.unwatched.append(("ticker", symbol))


class FakeHub:
    def __init__(self, collectors):
        self.collectors = collectors


def make_collector(label="Binance", ex_id="binanceusdm", market="swap", n=80,
                   top_n=30, books=10, settings=None):
    """Настоящий ExchangeCollector, но с подставленной биржей и рынками."""
    from app.collector import ExchangeCollector

    cfg = ExchangeConfig(ex_id, label, market, top_n=top_n, books=books)
    col = ExchangeCollector(cfg, settings or Settings())
    markets = {}
    for i in range(n):
        base = f"C{i:03d}"
        sym = f"{base}/USDT:USDT" if market == "swap" else f"{base}/USDT"
        markets[sym] = {"id": f"{base}USDT", "symbol": sym, "base": base, "quote": "USDT",
                        "active": True, market: True, "spot": market == "spot",
                        "swap": market == "swap", "contractSize": 1.0, "inverse": False}
    col.ex = FakeEx(markets)
    col.symbols = list(markets)
    return col


def fill_store(col, vols=None, natr_seed=False, hl=None):
    """Наполняет STORE тикерами по всем символам коллектора (как REST-срез).

    hl: {symbol: (high, low)} — диапазон 24ч на символ (rng считается из него).
    """
    vols = vols or {}
    hl = hl or {}
    for i, sym in enumerate(col.symbols):
        st = col._state(sym)
        vol = vols.get(sym, float((len(col.symbols) - i) * 1_000_000))
        hi, lo = hl.get(sym, (11.0, 9.0))
        st.apply_ticker({"symbol": sym, "last": 10.0 + i * 0.1, "bid": 9.99, "ask": 10.01,
                         "open": 10.0, "high": hi, "low": lo,
                         "quoteVolume": vol, "baseVolume": vol / 10.0, "count": 1000 + i})
        STORE.bump("ticker_updates")
        if natr_seed:
            st.ohlcv = [[1_700_000_000 + j * 60, 10, 11, 9, 10 + j * 0.01, 1]
                        for j in range(40)]
            st.apply_ohlcv(st.ohlcv)
    return col


@pytest.fixture
def store_clean():
    """STORE глобальный — чистим до и после, чтобы тесты не влияли друг на друга."""
    saved = dict(STORE._symbols)
    saved_status = dict(STORE.exchange_status)
    STORE._symbols.clear()
    STORE.exchange_status.clear()
    yield STORE
    STORE._symbols.clear()
    STORE._symbols.update(saved)
    STORE.exchange_status.clear()
    STORE.exchange_status.update(saved_status)


def make_manager(collectors, settings=None):
    s = settings or Settings()
    s.exchanges = [c.cfg for c in collectors]
    m = FocusManager(s)
    m.attach_hub(FakeHub(collectors))
    return m


# --------------------------------------------------------------------------
# Ограничения параметров
# --------------------------------------------------------------------------
def test_clamp_limit_bounds():
    assert clamp_limit(50) == 50
    assert clamp_limit(1) == 5, "меньше 5 подписок смысла нет"
    assert clamp_limit(10_000) == 200, "иначе «фокус» = весь рынок"
    assert clamp_limit("abc") == 50
    assert clamp_limit(None) == 50


def test_clamp_interval_bounds():
    assert clamp_interval(15) == 15.0
    assert clamp_interval(0.1) == 5.0, "чаще 5 c — долбёжка биржи REST-запросами"
    assert clamp_interval(9999) == 300.0
    assert clamp_interval("xx", default=20.0) == 20.0


def test_needs_ohlcv_detects_expensive_metrics():
    assert needs_ohlcv({}, "natr") is True
    assert needs_ohlcv({"natr_min": 1.0}, "vol") is True
    assert needs_ohlcv({"r300_min": 2.0}, "vol") is True
    assert needs_ohlcv({"vol_min": 1e6}, "vol") is False
    assert needs_ohlcv({}, "chg") is False
    # rng (диапазон 24ч) считается из high/low тикера — свечи ему не нужны
    assert needs_ohlcv({}, "rng") is False
    assert needs_ohlcv({"rng_min": 5.0}, "vol") is False
    # spike рождается из ленты сделок; apply_ohlcv спайки не создаёт, поэтому
    # добор свечей фильтру по спайкам не помогает — только лишние запросы бирже
    assert needs_ohlcv({"spike": True}, "vol") is False


# --------------------------------------------------------------------------
# Отбор
# --------------------------------------------------------------------------
def test_selects_top_n_of_one_exchange_only(store_clean):
    a = fill_store(make_collector("Binance", "binanceusdm", "swap", n=80))
    b = fill_store(make_collector("Bybit", "bybit", "swap", n=40))
    m = make_manager([a, b])

    asyncio.run(m.update(ex="Binance", mt="swap", limit=20, sort="vol", desc=True))

    assert len(m.keys) == 20
    assert all(k.startswith("binanceusdm:") for k in m.keys), \
        "в отбор попала чужая биржа — фильтр по бирже не сработал"
    # топ по объёму: первые символы (у них vol максимальный)
    assert m.symbols[0] == a.symbols[0]
    assert m.stats["universe"] == 80


def test_respects_market_type(store_clean):
    swap = fill_store(make_collector("Binance", "binanceusdm", "swap", n=30))
    spot = fill_store(make_collector("Binance", "binance", "spot", n=30))
    m = make_manager([swap, spot])

    asyncio.run(m.update(ex="Binance", mt="spot", limit=10))

    assert len(m.keys) == 10
    assert all(k.startswith("binance:") for k in m.keys)
    assert m.target_collector() is spot


def test_filter_by_volume_threshold(store_clean):
    a = fill_store(make_collector(n=60))
    # объёмы: у первых 10 символов 100M, у остальных 1M
    vols = {s: (100_000_000.0 if i < 10 else 1_000_000.0) for i, s in enumerate(a.symbols)}
    fill_store(a, vols)
    m = make_manager([a])

    asyncio.run(m.update(ex="Binance", mt="swap", limit=50,
                         params={"vol_min": 50_000_000.0}))

    assert len(m.keys) == 10, "фильтр по объёму обязан резать выборку"
    assert "фильтру соответствует 10" in m.note


def test_filter_by_volatility_fetches_candles_for_candidates(store_clean):
    """
    Ключевой случай: NATR есть только у горячего набора. Если не добить свечи
    кандидатам, «отбор по волатильности» молча превратится в пустой результат.
    """
    a = fill_store(make_collector(n=40))
    m = make_manager([a])

    asyncio.run(m.update(ex="Binance", mt="swap", limit=5, params={"natr_min": 0.001},
                         sort="natr"))

    assert a.ex.ohlcv_calls, "кандидатам не заказали свечи — NATR остался бы None"
    assert len(a.ex.ohlcv_calls) >= 5
    assert len(m.keys) == 5
    for k in m.keys:
        assert STORE.get(k).natr > 0, "отобранная монета без NATR"
    # отобранные — действительно самые волатильные из пула
    natrs = [STORE.get(k).natr for k in m.keys]
    assert natrs == sorted(natrs, reverse=True)


def test_candles_not_refetched_within_ttl(store_clean):
    a = fill_store(make_collector(n=30))
    m = make_manager([a])
    asyncio.run(m.update(ex="Binance", mt="swap", limit=5, params={"natr_min": 0.001}))
    first = len(a.ex.ohlcv_calls)
    assert first > 0

    asyncio.run(m.refresh())          # второй проход сразу же
    assert len(a.ex.ohlcv_calls) == first, \
        "свечи перезапрашиваются чаще CANDLES_TTL — лишняя нагрузка на биржу"


def test_candle_enrichment_is_batched(store_clean, monkeypatch):
    """
    Замер на живом Binance: fetch_ohlcv ~1.2 c. Если добивать свечи всему пулу
    одним проходом, POST /api/focus висит 9+ секунд (фронтенд обрывает на 12).
    Поэтому добор порционный: первый отбор быстрый и помечен «уточняется»,
    следующий проход добирает остаток.
    """
    import app.focus as FM
    monkeypatch.setattr(FM, "FOCUS_OHLCV_MAX", 5)

    a = fill_store(make_collector(n=30))
    m = make_manager([a])
    asyncio.run(m.update(ex="Binance", mt="swap", limit=5, params={"natr_min": 0.001},
                         sort="natr"))
    assert m.enriching is True, "не сообщили, что отбор ещё уточняется"
    assert m.pending > 0
    assert "свечи ещё не загружены" in m.note
    assert len(a.ex.ohlcv_calls) <= FM.FOCUS_OHLCV_MAX * 3, \
        f"за один проход заказали {len(a.ex.ohlcv_calls)} свечей"
    st = m.state()
    assert st["enriching"] is True and st["pending"] > 0

    # следующие проходы добирают остаток (свечи уже в кэше → запросов меньше)
    before = len(a.ex.ohlcv_calls)
    for _ in range(12):
        asyncio.run(m.refresh())
        if not m.enriching:
            break
    assert m.enriching is False, "пул так и не добрал свечи"
    assert len(a.ex.ohlcv_calls) > before, "остаток пула не добирали"
    assert all(STORE.get(k).natr > 0 for k in m.keys)


def test_candidate_pool_capped(store_clean):
    assert FOCUS_CANDIDATES_MAX <= 600
    a = fill_store(make_collector(n=50))
    m = make_manager([a])
    asyncio.run(m.update(ex="Binance", mt="swap", limit=10))
    assert m.stats["pool"] <= FOCUS_CANDIDATES_MAX


def test_strict_rotation_unwatches_dropped_symbols(store_clean, monkeypatch):
    """
    Strict-режим (выбран пользователем): выпал из топа → подписка снимается
    немедленно, «липкого» набора нет. И обратно: вернулся в топ → застримился.
    """
    import app.collector as C
    # не ждём боевые 3 c до первой ротации и 0.4 c между пересборками:
    # иначе тест проверяет не ротацию, а то, кто успеет первым
    monkeypatch.setattr(C, "ROTATE_START_DELAY", 0.02)
    monkeypatch.setattr(C, "FOCUS_ROTATE_DELAY", 0.02)

    a = fill_store(make_collector(n=20, books=6))
    m = make_manager([a])
    asyncio.run(m.update(ex="Binance", mt="swap", limit=6))
    first = list(m.symbols)
    assert len(first) == 6

    async def scenario():
        a.set_focus(first)
        t = asyncio.create_task(a._rotate_loop())
        await asyncio.sleep(0.2)
        assert set(a._trade_tasks) == set(first), "подписки не совпали с отбором"
        assert len(a._book_tasks) == min(a.cfg.books, len(first))
        assert a.focus_mode is True

        # меняем объёмы детерминированной перестановкой. Ни reversed(...),
        # ни enumerate по прямому списку не годятся: оба дают ТОТ ЖЕ топ,
        # и тест молча выродился бы в проверку «ничего не изменилось».
        order = [(i * 7 + 3) % len(a.symbols) for i in range(len(a.symbols))]
        new_vols = {a.symbols[idx]: float(pos * 1_000_000)
                    for pos, idx in enumerate(order)}
        fill_store(a, new_vols)
        await m.refresh()
        second = list(m.symbols)
        assert set(second) != set(first), "отбор не отреагировал на смену объёмов"
        a.set_focus(second)
        await asyncio.sleep(0.4)
        assert set(a._trade_tasks) == set(second), "стрим не пересобран после ротации"
        assert a.hot == second, "горячий набор не совпадает с отбором"

        dropped = set(first) - set(second)
        assert dropped, "тест вырожден: состав не изменился"
        # стрим сделок был на всех шести — значит и снять подписку обязаны со всех
        unw_trades = {sym for kind, sym in a.ex.unwatched if kind == "trades"}
        unw_ticker = {sym for kind, sym in a.ex.unwatched if kind == "ticker"}
        assert dropped <= unw_trades, f"не сняли подписку на сделки: {dropped - unw_trades}"
        assert dropped <= unw_ticker, f"не сняли подписку на тикер: {dropped - unw_ticker}"

        # возвращаем прежние объёмы: выпавшие монеты обязаны снова застримиться
        fill_store(a)
        await m.refresh()
        third = list(m.symbols)
        assert set(third) == set(first), "отбор не вернулся к прежнему составу"
        a.set_focus(third)
        await asyncio.sleep(0.4)
        assert set(a._trade_tasks) == set(third), "подписки не восстановились"
        await a.stop()
        t.cancel()

    asyncio.run(scenario())


def test_pause_others_and_restore(store_clean):
    a = fill_store(make_collector("Binance", "binanceusdm", "swap", n=30))
    b = fill_store(make_collector("Bybit", "bybit", "swap", n=30))
    m = make_manager([a, b])

    asyncio.run(m.update(ex="Binance", mt="swap", limit=10, pause_others=True))
    assert b.paused is True, "чужая биржа не поставлена на паузу"
    assert a.paused is False
    assert a.focus_mode is True

    asyncio.run(m.update(ex="Binance", mt="swap", limit=10, pause_others=False))
    assert b.paused is False, "pause_others=False обязан вернуть стримы"

    asyncio.run(m.disable())
    assert a.paused is False and b.paused is False
    assert a.focus_mode is False and b.focus_mode is False
    assert m.keys == [] and m.enabled is False
    assert "focus" not in STORE.exchange_status


def test_disable_during_slow_refresh_is_not_overridden(store_clean, monkeypatch):
    """
    Регресс живого замера: пересчёт по NATR шёл 13 c, пользователь за это
    время выключил фокус, а завершившийся пересчёт СНОВА поставил прочие биржи
    на паузу и вернул фокус-набор. Теперь устаревшее применение отменяется.
    """
    import app.focus as FM

    a = fill_store(make_collector("Binance", "binanceusdm", "swap", n=30))
    b = fill_store(make_collector("Bybit", "bybit", "swap", n=30))
    m = make_manager([a, b])
    monkeypatch.setattr(FM, "CANDLES_TTL", 0.0)      # свечи всегда «протухшие»

    async def slow_fetch(_syms, limit=400):
        await asyncio.sleep(0.4)
        return 0

    a.ensure_ohlcv = slow_fetch
    b.ensure_ohlcv = slow_fetch

    async def scenario():
        task = asyncio.create_task(m.update(ex="Binance", mt="swap", limit=5,
                                            params={"natr_min": 0.001}, sort="natr",
                                            budget=10.0))
        await asyncio.sleep(0.1)          # пересчёт уже внутри добора свечей
        await m.disable()                 # пользователь выключил фокус
        await task
        await asyncio.sleep(0.8)          # даём устаревшему пересчёту завершиться
        assert m.enabled is False
        assert m.keys == []
        assert a.focus_mode is False, "устаревший пересчёт вернул фокус-набор"
        assert b.paused is False, "устаревший пересчёт снова поставил биржу на паузу"
        assert a.paused is False

    asyncio.run(scenario())
    assert "focus" not in STORE.exchange_status


def test_switch_exchange_during_slow_refresh(store_clean, monkeypatch):
    """Переключение биржи во время долгого пересчёта: побеждает НОВЫЙ выбор."""
    import app.focus as FM

    a = fill_store(make_collector("Binance", "binanceusdm", "swap", n=30))
    b = fill_store(make_collector("Bybit", "bybit", "swap", n=30))
    m = make_manager([a, b])
    monkeypatch.setattr(FM, "CANDLES_TTL", 0.0)

    def binder(col):
        async def slow_fetch(syms, limit=400):
            # как настоящий ensure_ohlcv: медленно И с применением свечей,
            # иначе NATR останется None и отбор будет пустым (тест выродится)
            await asyncio.sleep(0.3)
            for sym in syms:
                st = col._state(sym)
                st.apply_ohlcv([[1_700_000_000 + j * 60, 10, 11, 9, 10 + j * 0.01, 1]
                                for j in range(40)])
            return len(syms)
        return slow_fetch

    a.ensure_ohlcv = binder(a)
    b.ensure_ohlcv = binder(b)

    async def scenario():
        t1 = asyncio.create_task(m.update(ex="Binance", mt="swap", limit=5,
                                          params={"natr_min": 0.001}, sort="natr",
                                          budget=10.0))
        await asyncio.sleep(0.1)
        t2 = asyncio.create_task(m.update(ex="Bybit", mt="swap", limit=5,
                                          params={"natr_min": 0.001}, sort="natr",
                                          budget=10.0))
        await asyncio.gather(t1, t2)
        await asyncio.sleep(0.6)
        assert m.spec.ex == "Bybit"
        assert all(k.startswith("bybit:") for k in m.keys), \
            f"устаревший пересчёт Binance перезаписал выбор: {m.keys[:3]}"
        assert b.focus_mode is True
        assert a.paused is True, "Binance теперь «чужая» биржа — должна быть на паузе"

    asyncio.run(scenario())


def test_interactive_budget_caps_first_rank(store_clean, monkeypatch):
    """
    Интерактивный пересчёт (POST /api/focus) не должен висеть, пока свечи
    добираются до всего пула: при исчерпанном бюджете отдаём best-effort и
    помечаем «уточняется», остальное доделает фоновый цикл.
    """
    import app.focus as FM

    a = fill_store(make_collector(n=40))
    calls: list[int] = []

    async def counting_fetch(syms, limit=400):
        calls.append(len(syms))
        for sym in syms:
            st = a._state(sym)
            st.ohlcv = [[1_700_000_000 + j * 60, 10, 11, 9, 10 + j * 0.01, 1]
                        for j in range(40)]
            st.apply_ohlcv(st.ohlcv)
        return len(syms)

    a.ensure_ohlcv = counting_fetch
    m = make_manager([a])
    monkeypatch.setattr(FM, "CANDLES_TTL", 0.0)

    # бюджет уже исчерпан → свечей не заказываем вовсе, отбор помечен
    asyncio.run(m.update(ex="Binance", mt="swap", limit=5, budget=-1.0,
                         params={"natr_min": 0.001}, sort="natr"))
    assert calls == [], f"при нулевом бюджете заказали {calls} свечей"
    assert m.enriching is True and m.pending > 0
    assert "уточнится" in m.note
    assert m.last_ms < 1000, f"«быстрый» пересчёт занял {m.last_ms} мс"

    # фоновый проход (бюджет по умолчанию) добирает свечи
    calls.clear()
    asyncio.run(m.refresh())
    assert calls and calls[0] > 0, "фоновый проход не добрал свечи"
    assert all(STORE.get(k).natr > 0 for k in m.keys)


def test_pool_expansion_respects_budget(store_clean, monkeypatch):
    """
    Расширение пула кандидатов - это ещё один круг добора свечей. Без проверки
    бюджета фоновый пересчёт занимал 12.3 c (замер), а интерактивный рисковал
    упереться в 12-секундный таймаут фронтенда.
    """
    import app.focus as FM

    a = fill_store(make_collector(n=40))
    rounds: list[int] = []

    async def counting_fetch(syms, limit=400):
        rounds.append(len(syms))
        for sym in syms:
            st = a._state(sym)
            st.apply_ohlcv([[1_700_000_000 + j * 60, 10, 11, 9, 10 + j * 0.01, 1]
                            for j in range(40)])
        await asyncio.sleep(0.05)
        return len(syms)

    a.ensure_ohlcv = counting_fetch
    m = make_manager([a])
    monkeypatch.setattr(FM, "CANDLES_TTL", 0.0)
    # заведомо исчерпанный бюджет: ни одного расширения пула быть не должно
    asyncio.run(m.update(ex="Binance", mt="swap", limit=5, budget=0.0,
                         params={"natr_min": 1e9}, sort="natr"))
    assert len(rounds) <= 1, f"с исчерпанным бюджетом сделано {len(rounds)} кругов добора"
    assert m.enriching is True
    assert "бюджет" in m.note or "уточнится" in m.note


def test_wakeup_not_lost_when_focus_changes_during_rotation(store_clean, monkeypatch):
    """
    Регресс живого замера: сигнал set_focus() приходил, пока ротация была
    занята подписками, и СБРАСЫВАЛСЯ при входе в сон — следующий пересчёт
    случался только через HOT_ROTATE (45 c). После выключения фокуса биржа
    ещё минуту стримила прежние 50 монет вместо 150.
    """
    import app.collector as C
    monkeypatch.setattr(C, "ROTATE_START_DELAY", 0.02)
    monkeypatch.setattr(C, "FOCUS_ROTATE_DELAY", 0.0)

    a = fill_store(make_collector(n=20, books=6, top_n=15))
    full_top = a.symbols[:15]

    async def fake_refresh_top():
        # в обычном режиме список даёт REST-срез тикеров; у заглушки его нет
        return list(full_top)

    a._refresh_top = fake_refresh_top

    async def scenario():
        a.set_focus(a.symbols[:5])
        t = asyncio.create_task(a._rotate_loop())
        await asyncio.sleep(0.25)
        assert set(a._trade_tasks) == set(a.symbols[:5])
        assert a.hot == a.symbols[:5]

        # «выключили фокус» ровно в тот момент, когда ротация ещё не спала:
        # HOT_ROTATE огромен, поэтому без корректного пробуждения hot остался бы 5
        monkeypatch.setattr(C, "HOT_ROTATE", 600.0)
        a.set_focus(None)
        await asyncio.sleep(0.6)
        assert a.hot == full_top, \
            f"сигнал пробуждения потерян: hot={len(a.hot)} вместо {len(full_top)}"
        assert len(a._trade_tasks) == len(full_top)
        await a.stop()
        t.cancel()

    asyncio.run(scenario())


def test_mass_unwatch_does_not_block_rotation(store_clean, monkeypatch):
    """
    Переход 150 → 50 подписок требовал ~170 последовательных un_watch_*,
    и ротация залипала на 56 секунд (замер на живом Binance). Отписки
    вынесены в фоновую задачу — ротация обязана applied новый набор сразу.
    """
    import app.collector as C
    monkeypatch.setattr(C, "ROTATE_START_DELAY", 0.02)
    monkeypatch.setattr(C, "FOCUS_ROTATE_DELAY", 0.0)

    a = fill_store(make_collector(n=60, books=6, top_n=40))

    async def slow_unwatch(kind, symbol):
        await asyncio.sleep(0.05)          # имитируем медленный ответ биржи
        a.ex.unwatched.append((kind, symbol))

    a._unwatch = slow_unwatch

    async def scenario():
        a.set_focus(a.symbols[:40])
        t = asyncio.create_task(a._rotate_loop())
        await asyncio.sleep(0.4)
        assert len(a._trade_tasks) == 40

        a.set_focus(a.symbols[:5])
        await asyncio.sleep(0.4)
        assert len(a._trade_tasks) == 5, "ротация залипла на массовых отписках"
        assert a.hot == a.symbols[:5]
        await asyncio.sleep(0.6)           # фоновые отписки успевают дойти
        kinds = {k for k, _ in a.ex.unwatched}
        assert "trades" in kinds and "ticker" in kinds, \
            f"подписки на бирже не сняты: {sorted(kinds)}"
        await a.stop()
        t.cancel()

    asyncio.run(scenario())


def test_pause_then_resume_is_not_blocked_by_unwatch(store_clean, monkeypatch):
    """
    Пауза биржи закрывала ~130 подписок ПОСЛЕДОВАТЕЛЬНО (каждый un_watch_* —
    ~0.3 c ответа биржи), поэтому ротация залипала на ~40 c: сигнал resume()
    приходил, пока цикл был занят, и биржа возвращалась только через HOT_ROTATE.
    Живой замер: через 15 c после выключения фокуса Binance стримил 50 вместо 150.
    """
    import app.collector as C
    monkeypatch.setattr(C, "ROTATE_START_DELAY", 0.02)
    monkeypatch.setattr(C, "FOCUS_ROTATE_DELAY", 0.0)

    a = fill_store(make_collector(n=40, books=6, top_n=20))
    full_top = a.symbols[:20]

    async def fake_refresh_top():
        return list(full_top)

    a._refresh_top = fake_refresh_top

    async def slow_unwatch(kind, symbol):
        await asyncio.sleep(0.05)
        a.ex.unwatched.append((kind, symbol))

    a._unwatch = slow_unwatch

    async def scenario():
        a.set_focus(a.symbols[:10])
        t = asyncio.create_task(a._rotate_loop())
        await asyncio.sleep(0.3)
        assert len(a._trade_tasks) == 10

        a.pause()                                   # биржа ушла из фокуса
        await asyncio.sleep(0.3)
        assert a._book_tasks == {} and a._trade_tasks == {} and a._ticker_tasks == {}

        # ключевой момент: resume во время фоновых отписок
        a.set_focus(None)
        a.resume()
        await asyncio.sleep(0.8)
        assert a.hot == full_top, \
            f"после resume биржа не вернулась к полному набору: hot={len(a.hot)}"
        assert len(a._trade_tasks) == len(full_top)
        await asyncio.sleep(0.6)                    # фоновые отписки доедают
        assert a.ex.unwatched, "подписки на бирже не сняты"
        await a.stop()
        t.cancel()

    asyncio.run(scenario())


def test_ohlcv_pool_given_to_target_collector(store_clean):
    a = fill_store(make_collector(n=30))
    m = make_manager([a])
    asyncio.run(m.update(ex="Binance", mt="swap", limit=5, params={"natr_min": 0.001}))
    assert a._focus_pool, "пул кандидатов не передан коллектору"
    assert len(a._focus_pool) >= len(a._focus_syms)


def test_dedupe_by_base_keeps_one_line_per_coin(store_clean):
    """
    Замер на живых Binance Futures показал: в топ-10 отбора попадали
    BTC/USDT:USDT И BTC/USDC:USDT, ETH/USDT:USDT И ETH/USDC:USDT. То есть
    «top-50 монет» на деле был top-40, а сетка графиков рисовала одну монету
    дважды. Дедупликация по базовому активу это лечит.
    """
    a = make_collector(n=10)
    extra = {}
    for sym in list(a.symbols[:5]):
        base = sym.split("/")[0]
        alt = f"{base}/USDC:USDC"
        extra[alt] = {"id": base + "USDC", "symbol": alt, "base": base, "quote": "USDC",
                      "active": True, "swap": True, "spot": False,
                      "contractSize": 1.0, "inverse": False}
        a.symbols.append(alt)
    a.ex.markets.update(extra)
    fill_store(a)
    m = make_manager([a])

    asyncio.run(m.update(ex="Binance", mt="swap", limit=12, dedupe_by_base=True))
    bases = [STORE.get(k).base for k in m.keys]
    assert len(bases) == len(set(bases)), f"в отборе дубли монет: {bases}"

    asyncio.run(m.update(ex="Binance", mt="swap", limit=12, dedupe_by_base=False))
    bases2 = [STORE.get(k).base for k in m.keys]
    assert len(bases2) > len(set(bases2)), "без дедупликации дубли обязаны появиться"
    assert m.spec.dedupe_by_base is False


def test_dedupe_prefers_more_liquid_quote(store_clean):
    """Дедуп оставляет первую строку в порядке «дешёвой» сортировки (объём)."""
    from app.focus import dedupe_by_base

    rows = [
        {"k": "e:BTC/USDT:USDT", "b": "BTC", "s": "BTC/USDT:USDT", "vol": 9e9},
        {"k": "e:BTC/USDC:USDC", "b": "BTC", "s": "BTC/USDC:USDC", "vol": 1e6},
        {"k": "e:ETH/USDT:USDT", "b": "ETH", "s": "ETH/USDT:USDT", "vol": 4e9},
    ]
    out = dedupe_by_base(rows)
    assert [r["s"] for r in out] == ["BTC/USDT:USDT", "ETH/USDT:USDT"]


def test_default_exchange_picked_when_empty(store_clean):
    a = fill_store(make_collector("Gate.io", "gate", "swap", n=10))
    m = make_manager([a])
    asyncio.run(m.update(ex="", mt="swap", limit=5))
    assert m.spec.ex == "Gate.io"
    assert len(m.keys) == 5


def test_no_data_gives_note_not_crash(store_clean):
    a = make_collector("Binance", "binanceusdm", "swap", n=10)   # STORE пуст
    m = make_manager([a])
    asyncio.run(m.update(ex="Binance", mt="swap", limit=5))
    assert m.keys == []
    assert "нет данных" in m.note
    assert m.last_error == ""


def test_unknown_exchange_falls_back_to_virtual(store_clean):
    a = fill_store(make_collector(n=20))
    m = make_manager([a])
    asyncio.run(m.update(ex="Несуществующая", mt="swap", limit=5))
    assert m.virtual is True, "без коллектора должен включаться виртуальный отбор"
    assert m.keys == []


def test_virtual_mode_uses_store_rows(store_clean):
    """replay/демо: коллектора нет, но отбор и ограничение пуша работать должны."""
    fill_store(make_collector(n=25))
    m = make_manager([])          # hub без коллекторов
    asyncio.run(m.update(ex="Binance", mt="swap", limit=7))
    assert m.virtual is True
    assert len(m.keys) == 7
    assert m.key_set == set(m.keys)


def test_refresh_keeps_previous_selection_on_error(store_clean):
    a = fill_store(make_collector(n=20))
    m = make_manager([a])
    asyncio.run(m.update(ex="Binance", mt="swap", limit=5))
    good = list(m.keys)
    assert good

    def boom(_col):
        raise RuntimeError("биржа легла")

    m._universe_rows = boom      # noqa: SLF001 — проверяем отказоустойчивость
    asyncio.run(m.refresh())
    assert m.keys == good, "при сбое отбора подписки не должны обнуляться"
    assert "RuntimeError" in m.last_error


def test_rotation_stats_and_timing(store_clean):
    a = fill_store(make_collector(n=20))
    m = make_manager([a])
    m.spec.interval = 15.0
    asyncio.run(m.update(ex="Binance", mt="swap", limit=5))
    st = m.state()
    assert st["stats"]["rotations"] == 1
    assert st["last_ms"] >= 0
    assert 0 < st["next_in"] <= 15.0
    assert st["count"] == 5
    assert st["enabled"] is True


# --------------------------------------------------------------------------
# Коллектор: ensure_ohlcv
# --------------------------------------------------------------------------
def test_ensure_ohlcv_returns_success_count(store_clean):
    a = make_collector(n=6)
    syms = a.symbols[:5]
    a.ex.ohlcv_fail = {syms[0]}
    got = asyncio.run(a.ensure_ohlcv(syms))
    assert got == 4, "должны считаться только успешные подкачки"
    st = a._state(syms[1])
    assert st.natr > 0
    assert st.ohlcv_ts > 0, "без метки времени TTL-проверка не работает"


def test_symbolstate_ohlcv_ts_updates():
    st = SymbolState("e", "E", "swap", "BTC/USDT:USDT", "BTC", "USDT")
    assert st.ohlcv_ts == 0.0
    st.apply_ohlcv([[1, 10, 11, 9, 10.5, 1]] * 20)
    assert st.ohlcv_ts > 0
    assert abs(st.ohlcv_ts - time.time()) < 5


# --------------------------------------------------------------------------
# Предварительный отбор: подписки сужаются ДО подкачки свечей
# (регресс жалобы «при фокусе подписывается на все монеты… это долго»)
# --------------------------------------------------------------------------
def test_provisional_narrows_subscriptions_before_candles(store_clean):
    """
    При включении фокуса с фильтром по NATR биржа раньше продолжала стримить
    полный горячий набор, пока шёл добор свечей сотням кандидатов. Теперь
    подписки сужаются СРАЗУ по тикерным данным (ноль запросов к бирже), а
    свечи только уточняют состав.
    """
    a = fill_store(make_collector("Binance", "binanceusdm", "swap", n=40, top_n=40))
    b = fill_store(make_collector("Bybit", "bybit", "swap", n=20))
    m = make_manager([a, b])

    release = asyncio.Event()

    async def slow_fetch(syms, limit=400):
        await release.wait()          # свечи «едут» бесконечно долго
        for sym in syms:
            st = a._state(sym)
            st.apply_ohlcv([[1_700_000_000 + j * 60, 10, 11, 9, 10 + j * 0.01, 1]
                            for j in range(40)])
        return len(syms)

    a.ensure_ohlcv = slow_fetch

    async def scenario():
        task = asyncio.create_task(m.update(ex="Binance", mt="swap", limit=6,
                                            params={"natr_min": 0.001}, sort="natr",
                                            budget=5.0))
        await asyncio.sleep(0.05)     # refresh уже внутри добора свечей
        # подписки УЖЕ сужены предварительным отбором
        assert a._focus_syms is not None, \
            "фокус не применён, пока добираются свечи — биржа стримит всё"
        assert len(a._focus_syms) == 6
        assert m.provisional is True
        assert b.paused is True, "чужая биржа не поставлена на паузу сразу"
        release.set()
        await task
        assert m.provisional is False, "после точного отбора флаг обязан сняться"
        assert len(m.keys) == 6
        assert all(STORE.get(k).natr > 0 for k in m.keys)

    asyncio.run(scenario())


def test_provisional_skipped_for_cheap_filters(store_clean):
    """Для дешёвых фильтров (объём и т.п.) предварительный отбор не нужен:
    обычный пересчёт и так мгновенный — двойная пересборка подписок ни к чему."""
    a = fill_store(make_collector(n=40))
    m = make_manager([a])
    asyncio.run(m.update(ex="Binance", mt="swap", limit=10, sort="vol"))
    assert m.provisional is False
    assert a.ex.ohlcv_calls == [], "дешёвый отбор не должен качать свечи"


def test_pool_ranked_by_volatility_proxy_not_volume(store_clean):
    """
    Пул кандидатов для «дорогого» фильтра ранжируется прокси-метрикой из
    тикеров (для NATR — диапазон 24ч), а не объёмом. Иначе топ по объёму —
    мейджоры с низким NATR — фильтр не набирает limit, пул расширяется до 600
    и свечи качаются всем подряд (замер старого поведения: 600 fetch_ohlcv).
    """
    a = make_collector(n=60)
    # первые 40 — «мейджоры»: гигантский объём, диапазон 24ч крошечный;
    # последние 20 — «ракеты»: объём скромный, швыряет на десятки процентов
    vols = {s: (100_000_000.0 if i < 40 else 600_000.0)
            for i, s in enumerate(a.symbols)}
    hl = {s: ((10.2, 10.0) if i < 40 else (15.0, 8.0))
          for i, s in enumerate(a.symbols)}
    fill_store(a, vols, hl=hl)
    m = make_manager([a])

    asyncio.run(m.update(ex="Binance", mt="swap", limit=5,
                         params={"natr_min": 0.001}, sort="natr"))

    assert a.ex.ohlcv_calls, "кандидатам не заказали свечи"
    volatile = set(a.symbols[40:])
    majors = set(a.symbols[:40])
    first20 = set(a.ex.ohlcv_calls[:20])
    assert first20 <= volatile, \
        f"первыми за свечами пошли неволатильные мейджоры: {sorted(first20 & majors)[:5]}"
    # всем волатильным достались свечи (старое поведение: пул по объёму —
    # «ракеты» вообще не попали бы в кандидаты)
    assert volatile <= set(a.ex.ohlcv_calls), \
        "волатильным монетам не достались свечи — пул собран не по прокси"
    assert set(m.symbols) <= set(a.ex.ohlcv_calls), "отобраны монеты вне пула"


def test_cheap_prefilter_cuts_pool_before_candles(store_clean):
    """
    Дешёвая часть фильтра (vol_min из пресета «Максимальная волатильность»)
    применяется ко всей вселенной ДО подкачки свечей: монеты, которые заведомо
    не пройдут по объёму, не должны её заказывать.
    """
    a = make_collector(n=60)
    vols = {}
    for i, s in enumerate(a.symbols):
        vols[s] = 100_000_000.0 if i < 25 else (10_000_000.0 if i < 35 else 500_000.0)
    fill_store(a, vols)
    m = make_manager([a])

    asyncio.run(m.update(ex="Binance", mt="swap", limit=5,
                         params={"natr_min": 0.001, "vol_min": 50_000_000.0},
                         sort="natr"))

    weak = set(a.symbols[25:35])      # объём 10M — vol_min их срезает сразу
    assert a.ex.ohlcv_calls, "свечи не заказаны"
    assert not (set(a.ex.ohlcv_calls) & weak), \
        "свечи качаются монетам, которые и так не пройдут дешёвый vol_min"
    assert len(m.keys) == 5


def test_empty_selection_while_enriching_keeps_focus(store_clean):
    """
    Регресс: пока свечи кандидатов не доехали, «дорогой» фильтр не проходит ни
    одной монеты. Раньше пустой отбор применялся к коллектору → set_focus([])
    → фокус снимался вовсе и биржа возвращалась к ПОЛНОМУ горячему набору
    (те самые «все монеты»), а после добора свечей снова сужалась — скачки
    подписок каждые 15 секунд.
    """
    a = fill_store(make_collector(n=40, top_n=40))
    m = make_manager([a])

    async def no_candles(syms, limit=400):
        return 0                      # биржа пока не отдаёт свечи

    a.ensure_ohlcv = no_candles
    asyncio.run(m.update(ex="Binance", mt="swap", limit=6,
                         params={"natr_min": 0.001}, sort="natr"))

    assert m.provisional is True
    assert len(m.keys) == 6, "предварительный отбор обязан остаться в строю"
    assert a._focus_syms is not None and len(a._focus_syms) == 6, \
        "пустой отбор сбросил коллектор в полный горячий набор"
    # следующий проход (свечей по-прежнему нет) — набор не шатается
    asyncio.run(m.refresh())
    assert a.focus_mode is True
    assert len(a._focus_syms) == 6
    assert m.enriching is True


def test_rng_sort_needs_no_candles(store_clean):
    """rng (диапазон 24ч) живёт в тикерах — отбор по нему не качает свечи."""
    a = make_collector(n=30)
    hl = {s: (12.0 + i * 0.5, 10.0) for i, s in enumerate(a.symbols)}
    fill_store(a, hl=hl)
    m = make_manager([a])

    asyncio.run(m.update(ex="Binance", mt="swap", limit=5, sort="rng", desc=True))

    assert a.ex.ohlcv_calls == [], "заказаны свечи — хотя rng уже есть в тикерах"
    rngs = [STORE.get(k).range_pct for k in m.keys]
    assert rngs == sorted(rngs, reverse=True), "отбор не отранжирован по rng"
    assert m.provisional is False


def test_ohlcv_batch_is_selection_not_pool(store_clean):
    """
    Фоновый цикл свечей в фокусе опрашивает ОТБОР (≤ limit монет), а не пул
    кандидатов: свечи пула обновляет сам пересчёт фокуса (ensure_ohlcv).
    Раньше: pool[:top_n] — до 300 fetch_ohlcv каждую минуту сверх пересчёта.
    """
    a = fill_store(make_collector(n=60, top_n=50))
    m = make_manager([a])
    asyncio.run(m.update(ex="Binance", mt="swap", limit=10,
                         params={"natr_min": 0.001}, sort="natr"))
    assert a._focus_pool and len(a._focus_pool) > len(a._focus_syms), \
        "тест вырожден: пул не больше отбора"
    a.hot = list(a._focus_syms)       # как делает ротация
    batch = a._ohlcv_batch()
    assert batch == a.hot
    assert len(batch) == 10


def test_same_params_update_does_not_reprovision(store_clean):
    """
    Если параметры отбора не менялись (подкрутили лишь interval), предвари-
    тельный отбор не перезапускается: иначе точный набор дёргался бы
    туда-сюда (cheap → precise) на каждый POST /api/focus.
    """
    a = make_collector(n=60)
    vols = {s2: (100_000_000.0 if i < 40 else 600_000.0)
            for i, s2 in enumerate(a.symbols)}
    hl = {s2: ((10.2, 10.0) if i < 40 else (15.0, 8.0))
          for i, s2 in enumerate(a.symbols)}
    fill_store(a, vols, hl=hl)
    m = make_manager([a])
    asyncio.run(m.update(ex="Binance", mt="swap", limit=5,
                         params={"natr_min": 0.001}, sort="natr"))
    precise = list(m.keys)
    assert m.provisional is False

    churn = []
    orig = a.set_focus

    def rec(syms, pool=None):
        if syms is not None and list(syms) != list(a._focus_syms or []):
            churn.append(list(syms))
        return orig(syms, pool=pool)

    a.set_focus = rec
    asyncio.run(m.update(ex="Binance", mt="swap", limit=5, interval=20,
                         params={"natr_min": 0.001}, sort="natr"))
    assert churn == [], f"пересборка подписок на ровном месте: {len(churn)} рывка"
    assert m.provisional is False
    assert list(m.keys) == precise, "точный отбор потерялся"

    # а вот смена фильтра обязана снова сузить подписки сразу (пре-отбор)
    churn.clear()
    asyncio.run(m.update(ex="Binance", mt="swap", limit=5,
                         params={"natr_min": 0.001, "vol_min": 1_000_000.0},
                         sort="natr"))
    assert m.provisional is False       # к концу update — уже точный отбор
    assert churn, "сменили фильтр — а подписки не пересобирались вовсе"
    assert all(STORE.get(k).vol24_usd >= 1_000_000.0 for k in m.keys), \
        "vol_min не применён к финальному отбору"
