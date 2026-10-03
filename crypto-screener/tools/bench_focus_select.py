"""
Замер схемы фокус-отбора по «дорогому» фильтру (пресет «Максимальная
волатильность»: natr_min + сортировка по NATR).

Сравниваются две схемы на синтетической вселенной из 500 инструментов,
устроенной как реальная биржа:
  * 40 мейджоров  — гигантский объём, низкая волатильность (NATR 0.3);
  * 310 середняков — нормальный объём, NATR 0.4…0.9 (фильтр НЕ проходят);
  * 90 «ракет»    — скромный объём, NATR 1.2…3.0 (то, что ищет фильтр);
  * 60 мёртвых    — объём ниже vol_min пресета, но высокий диапазон 24ч.

СТАРАЯ схема: пул кандидатов ранжируется по объёму, дешёвого префильтра нет,
подписки сужаются только ПОСЛЕ завершения всего отбора.
НОВАЯ схема: дешёвый префильтр (vol_min) по всей вселенной + пул по прокси
(диапазон 24ч из тикеров) + предварительный отбор, который сужает подписки
сразу (ноль запросов к бирже).

Метрики:
  * fetch_ohlcv до первого полного отбора (главная стоимость: на живом
    Binance один запрос ~1.2 c, параллелизм 24);
  * время до первого сужения подписок (set_focus);
  * время до финального отбора.

Запуск:  python tools/bench_focus_select.py
"""
from __future__ import annotations

import asyncio
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import app.focus as FM                                        # noqa: E402
from app.config import ExchangeConfig, Settings                # noqa: E402
from app.focus import FocusManager                             # noqa: E402
from app.state import STORE                                    # noqa: E402

FETCH_LATENCY = 0.003      # c: имитация ответа биржи (в жизни ~1.2 c)
REAL_FETCH_S = 1.2         # c: живая латентность fetch_ohlcv на Binance (для оценки)
CONCURRENCY = 24           # параллелизм ensure_ohlcv (FOCUS_OHLCV_CONCURRENCY)


def make_universe(rnd: random.Random):
    """(markets, vols, hl, natrs) по 500 символам."""
    symbols, vols, hl, natrs = [], {}, {}, {}
    plan = ([("major", 40), ("mid", 310), ("rocket", 90), ("dead", 60)])
    i = 0
    for kind, n in plan:
        for _ in range(n):
            sym = f"S{i:03d}/USDT:USDT"
            symbols.append(sym)
            if kind == "major":
                vols[sym] = 80_000_000.0
                hl[sym] = (10.2, 10.0)            # rng ~2%
                natrs[sym] = 0.3
            elif kind == "mid":
                vols[sym] = 2_000_000.0
                lo = 10.0
                hi = lo * (1 + rnd.uniform(3.0, 8.0) / 100)
                hl[sym] = (hi, lo)                # rng 3…8%
                natrs[sym] = rnd.uniform(0.4, 0.9)
            elif kind == "rocket":
                vols[sym] = 800_000.0             # проходит vol_min=500k
                lo = 10.0
                hi = lo * (1 + rnd.uniform(15.0, 60.0) / 100)
                hl[sym] = (hi, lo)                # rng 15…60%
                natrs[sym] = rnd.uniform(1.2, 3.0)
            else:                                 # dead
                vols[sym] = 200_000.0             # НЕ проходит vol_min=500k
                lo = 10.0
                hi = lo * (1 + rnd.uniform(20.0, 70.0) / 100)
                hl[sym] = (hi, lo)                # высокий rng — ловушка для прокси
                natrs[sym] = rnd.uniform(1.5, 3.0)
            i += 1
    return symbols, vols, hl, natrs


def candles_for(natr_pct: float) -> list:
    """1m-свечи, у которых NATR(14) ≈ natr_pct."""
    out, c, t = [], 100.0, 1_700_000_000_000
    for i in range(80):
        c *= 1.0002
        h = c * (1 + natr_pct / 200.0)
        l = c * (1 - natr_pct / 200.0)
        out.append([t + i * 60_000, c, h, l, c, 10.0])
    return out


class BenchEx:
    def __init__(self, markets: dict, natrs: dict):
        self.markets = markets
        self.natrs = natrs
        self.calls: list[str] = []

    def market(self, symbol: str) -> dict:
        return self.markets[symbol]

    async def fetch_ohlcv(self, symbol, tf="1m", limit=400, **kw):
        self.calls.append(symbol)
        await asyncio.sleep(FETCH_LATENCY)
        return candles_for(self.natrs[symbol])

    async def close(self):
        pass


def build_collector(symbols, vols, hl, natrs):
    from app.collector import ExchangeCollector

    cfg = ExchangeConfig("binanceusdm", "Binance", "swap", top_n=300, books=80)
    col = ExchangeCollector(cfg, Settings())
    markets = {}
    for sym in symbols:
        base = sym.split("/")[0]
        markets[sym] = {"id": base + "USDT", "symbol": sym, "base": base,
                        "quote": "USDT", "active": True, "swap": True,
                        "spot": False, "contractSize": 1.0, "inverse": False}
    col.ex = BenchEx(markets, natrs)
    col.symbols = list(symbols)
    for sym in symbols:
        st = col._state(sym)
        hi, lo = hl[sym]
        st.apply_ticker({"symbol": sym, "last": 10.0, "bid": 9.99, "ask": 10.01,
                         "open": 10.0, "high": hi, "low": lo,
                         "quoteVolume": vols[sym], "baseVolume": vols[sym] / 10.0,
                         "count": 5000})
    return col


class FakeHub:
    def __init__(self, collectors):
        self.collectors = collectors


def reset_store():
    STORE._symbols.clear()
    STORE.exchange_status.clear()


async def run_arm(label: str, old_scheme: bool) -> dict:
    rnd = random.Random(42)
    symbols, vols, hl, natrs = make_universe(rnd)
    reset_store()
    col = build_collector(symbols, vols, hl, natrs)

    # снимаем порцию добора: сравниваем ИМЕННО схему формирования пула
    # (порционность FOCUS_OHLCV_MAX одинакова в обеих ветках)
    fm_cap = FM.FOCUS_OHLCV_MAX
    FM.FOCUS_OHLCV_MAX = 10_000

    first_focus_at: list[float] = []
    orig_set_focus = col.set_focus

    def timed_set_focus(syms, pool=None):
        if syms and not first_focus_at:
            first_focus_at.append(time.perf_counter())
        return orig_set_focus(syms, pool=pool)

    col.set_focus = timed_set_focus

    m = FocusManager(Settings())
    m.attach_hub(FakeHub([col]))

    saved = {}
    if old_scheme:
        # эмуляция старой схемы: пул по объёму, без префильтра, без предотбора
        saved["proxy"] = FM.PROXY_SORT
        saved["cheap"] = FM.cheap_params
        saved["prov"] = FocusManager._apply_provisional
        FM.PROXY_SORT = {}
        FM.cheap_params = lambda p: {}
        FocusManager._apply_provisional = lambda self, col, gen: None

    t0 = time.perf_counter()
    try:
        await m.update(ex="Binance", mt="swap", limit=50,
                       params={"natr_min": 1.0, "vol_min": 500_000.0},
                       sort="natr", budget=60.0)
    finally:
        if old_scheme:
            FM.PROXY_SORT = saved["proxy"]
            FM.cheap_params = saved["cheap"]
            FocusManager._apply_provisional = saved["prov"]
        FM.FOCUS_OHLCV_MAX = fm_cap
    t_total = time.perf_counter() - t0
    t_focus = (first_focus_at[0] - t0) if first_focus_at else None

    fetches = len(col.ex.calls)
    rockets = {f"S{i:03d}/USDT:USDT" for i in range(350, 440)}
    result = {
        "label": label,
        "fetches": fetches,
        "rockets_fetched": len(rockets & set(col.ex.calls)),
        "rockets_selected": len(rockets & set(m.symbols)),
        "real_est_s": round(fetches * REAL_FETCH_S / CONCURRENCY, 1),
        "t_focus_ms": round(t_focus * 1000, 1) if t_focus is not None else None,
        "t_total_ms": round(t_total * 1000, 1),
        "selected": len(m.keys),
        "matched": m.stats["matched"],
        "pool": m.stats["pool"],
        "note": m.note,
    }
    await col.ex.close()
    return result


async def main():
    old = await run_arm("СТАРАЯ схема (пул по объёму, всё сразу)", old_scheme=True)
    new = await run_arm("НОВАЯ схема (префильтр + прокси + предотбор)", old_scheme=False)

    w = max(len(r["label"]) for r in (old, new)) + 2
    print("=" * 78)
    print("Отбор пресета «Максимальная волатильность» (natr_min=1.0, limit=50),")
    print("вселенная 500 инструментов, из них фильтру соответствуют 90 «ракет»")
    print("=" * 78)
    for r in (old, new):
        print(f"\n{r['label']:<{w}}")
        print(f"  fetch_ohlcv до отбора : {r['fetches']}")
        print(f"  оценка на живой бирже : ~{r['real_est_s']} c "
              f"(1.2 c/запрос, {CONCURRENCY} параллельно)")
        tf = "—" if r["t_focus_ms"] is None else f"{r['t_focus_ms']} мс"
        print(f"  подписки сужены через : {tf}")
        print(f"  финальный отбор через : {r['t_total_ms']} мс")
        print(f"  отобрано / совпало    : {r['selected']} / {r['matched']} (пул {r['pool']})")
        print(f"  «ракет» проверено/взято: {r['rockets_fetched']} / {r['rockets_selected']}")
        if r["note"]:
            print(f"  note                  : {r['note']}")

    if old["fetches"] and new["fetches"]:
        print("\n" + "-" * 78)
        print(f"ИТОГ: запросов свечей {old['fetches']} -> {new['fetches']} "
              f"({old['fetches'] / max(new['fetches'], 1):.1f}x меньше); "
              f"сужение подписок: {old['t_focus_ms']} мс -> {new['t_focus_ms']} мс")


if __name__ == "__main__":
    asyncio.run(main())
