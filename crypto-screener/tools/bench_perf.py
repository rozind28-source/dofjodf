#!/usr/bin/env python3
"""
Офлайн-бенчмарк горячих точек сервера.

Зачем: «тормозит» нельзя чинить на глаз. Скрипт собирает синтетическое
хранилище того же размера, что боевое (по умолчанию 6000 символов — как
профиль из 8 бирж), и измеряет в миллисекундах операции, которые сервер
выполняет КАЖДУЮ СЕКУНДУ:

  * to_row() по всем символам        — сериализация для WS-пуша и REST;
  * ret()                            — самый дорогой кусок to_row (скан истории);
  * apply_book()                     — каждый WS-апдейт стакана;
  * overview()                       — каждый WS-пуш каждому клиенту;
  * select()-подобная сортировка     — каждый WS-пуш каждому клиенту.

Запуск:  python tools/bench_perf.py [n_symbols] [n_book_updates]
Печатает таблицу; используется для сравнения «до/после» оптимизаций.
"""
from __future__ import annotations

import random
import statistics
import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))

from app.metrics import SymbolState  # noqa: E402
from app.state import Store  # noqa: E402

N = int(sys.argv[1]) if len(sys.argv) > 1 else 6000
BOOK_UPDATES = int(sys.argv[2]) if len(sys.argv) > 2 else 2000


def make_states(n: int, seed: int = 7) -> list[SymbolState]:
    rng = random.Random(seed)
    out = []
    now = time.time()
    for i in range(n):
        st = SymbolState("bench", "Bench", "swap", f"C{i:05d}/USDT:USDT",
                         f"C{i:05d}", "USDT")
        st.set_contract(False, 1.0)
        px = rng.uniform(0.01, 500.0)
        st.apply_ticker({"last": px, "open": px * 0.98, "high": px * 1.05,
                         "low": px * 0.95, "quoteVolume": rng.uniform(1e6, 1e10),
                         "baseVolume": rng.uniform(1e3, 1e7), "count": rng.randint(1e3, 1e6)})
        # история цен как в бою: ~900 точек на символ
        for k in range(900):
            st.price_hist.append(now - 900 + k, px * (1 + rng.gauss(0, 0.0004)))
        st.last = px
        st.ohlcv = [[int((now - 300 + j) * 1000), px, px * 1.002, px * 0.998, px, 10.0]
                    for j in range(300)]
        out.append(st)
    return out


def bench(name: str, fn, repeats: int = 3) -> float:
    ts = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        ts.append((time.perf_counter() - t0) * 1000)
    med = statistics.median(ts)
    print(f"  {name:44s} {med:9.1f} мс")
    return med


def main() -> None:
    print(f"Бенчмарк: {N} символов, {BOOK_UPDATES} апдейтов стакана")
    states = make_states(N)
    store = Store()
    store._symbols = {st.key: st for st in states}

    rng = random.Random(11)

    def to_rows():
        for st in states:
            st.to_row()

    def rets():
        for st in states[:1000]:
            for sec in (60, 300, 900, 3600, 14400):
                st.ret(sec)

    def books():
        for _ in range(BOOK_UPDATES):
            st = states[rng.randrange(N)]
            px = st.last or 100.0
            bids = [[px * (1 - i * 0.0001), rng.uniform(0.1, 50)] for i in range(1, 101)]
            asks = [[px * (1 + i * 0.0001), rng.uniform(0.1, 50)] for i in range(1, 101)]
            st.apply_book({"bids": bids, "asks": asks}, big_usd=50_000, huge_usd=250_000)

    # --- реалистичная симуляция: секунда книжного потока CORE-профиля ---
    # ~510 книг × 10 Гц (binance depth@100ms) = ~5100 апдейтов. Монотонные
    # часы подменяем, чтобы троттлинг min_interval=0.25 видел «настоящие»
    # 100-мс такты, а не сжатый в миллисекунды цикл бенчмарка.
    import app.metrics as _M

    def sim_books(throttled: bool, n_books: int = 510, hz: int = 10) -> float:
        books_states = states[:n_books]
        # один заранее собранный стакан на символ (содержимое не влияет на CPU)
        rnd = random.Random(5)
        pre = []
        for st in books_states:
            px = st.last or 100.0
            pre.append({"bids": [[px * (1 - i * 0.0001), rnd.uniform(0.1, 50)] for i in range(1, 101)],
                        "asks": [[px * (1 + i * 0.0001), rnd.uniform(0.1, 50)] for i in range(1, 101)]})
        fake = [1000.0]
        orig = _M.time.monotonic
        _M.time.monotonic = lambda: fake[0]
        try:
            t0 = time.perf_counter()
            for _ in range(hz):
                fake[0] += 0.1
                for st, bk in zip(books_states, pre):
                    st.apply_book(bk, min_interval=0.25 if throttled else 0.0)
            return (time.perf_counter() - t0) * 1000
        finally:
            _M.time.monotonic = orig

    def overview():
        store.overview()

    rows = [st.to_row() for st in states]

    def sorting():
        rs = sorted(rows, key=lambda r: (r.get("vol") is None, -(r.get("vol") or 0)))
        rs[:200]

    print("\nОперации, которые сервер делает КАЖДУЮ СЕКУНДУ:")
    bench(f"to_row() × {N} (сериализация для пуша/REST)", to_rows)
    bench(f"ret() × 5 × 1000 (внутри to_row)", rets)
    bench(f"apply_book() × {BOOK_UPDATES} (WS-апдейты стаканов, без троттлинга)", books)
    bench(f"overview() (шапка на каждый пуш)", overview)
    bench(f"сортировка {N} строк (каждый пуш каждому клиенту)", sorting)

    # --- WS-пуш: выборка + сериализация (каждый такт каждому клиенту) ---
    page = sorting_rows = None

    def select_like():
        """Аналог app.api.select: фильтр + сортировка полного снимка, top-150."""
        filtered = [r for r in rows if r["vol"] >= 1e6 and (r.get("natr") or 0) >= 0.0]
        srt = sorted(filtered, key=lambda r: (r.get("natr") is None, -(r.get("natr") or 0)))
        return srt[:150]

    page = select_like()
    ov = store.overview()
    import json as _json

    def push_json_stdlib():
        return _json.dumps({"type": "rows", "rows": page, "meta": {"total": len(page)},
                            "overview": ov, "ts": 1.0}, separators=(",", ":"))

    print("\nОперации WS-пуша (один такт одному клиенту):")
    t_sel = bench("select(): фильтр+сортировка 6000 → 150", select_like)
    t_json = bench("json.dumps(payload 150 строк, stdlib)", push_json_stdlib)
    try:
        import orjson

        def push_json_or():
            return orjson.dumps({"type": "rows", "rows": page, "meta": {"total": len(page)},
                                 "overview": ov, "ts": 1.0}).decode()

        t_or = bench("orjson.dumps(payload 150 строк)", push_json_or)
        b_std = len(push_json_stdlib()); b_or = len(push_json_or())
        print(f"  размер payload: stdlib {b_std/1024:.1f} КБ, orjson {b_or/1024:.1f} КБ")
        print(f"  ускорение сериализации: {t_json/max(t_or,1e-9):.1f}×")
    except ImportError:
        print("  orjson не установлен — используется stdlib json")

    print("\nРеалистичная симуляция: секунда книжного потока (510 книг × 10 Гц):")
    unthr = sim_books(throttled=False)
    thr = sim_books(throttled=True)
    print(f"  без троттлинга (min_interval=0):    {unthr:9.0f} мс CPU")
    print(f"  с троттлингом  (min_interval=0.25): {thr:9.0f} мс CPU")
    print(f"  выигрыш: {unthr / max(thr, 1e-9):.1f}×")

    print("\nИтого за секунду (оценка): to_row + books(throttle) + overview + sort")
    t1 = bench("  to_row", to_rows, 1)
    t3 = bench("  overview", overview, 1)
    t4 = bench("  sort", sorting, 1)
    total = t1 + thr + t3 + t4
    print(f"\n  СУММА: {total:.0f} мс CPU на секунду wall-time "
          f"({total / 10:.1f}% одного ядра)")


if __name__ == "__main__":
    main()
