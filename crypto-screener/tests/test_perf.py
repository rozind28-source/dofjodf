"""
Регрессия на оптимизации производительности (раунд «всё тормозит»).

Замер до фикса (tools/bench_perf.py, 6000 символов): apply_book ~350 мкс на
апдейт, книжный поток CORE-профиля (510 книг × 10 Гц) съедал 1.5–1.8 с CPU на
секунду wall-time — больше целого ядра, event loop захлёбывался, и тормозило
всё: WS-пуш, REST, графики. Эти тесты фиксируют три решения:

  1. ret() — бинарный поиск вместо линейного скана хвоста истории
     (30 000 вызовов/с на сериализации);
  2. apply_book(min_interval) — тяжёлая часть (кластеризация плотностей +
     имбаланс, ~290 из ~350 мкс) не чаще 4 Гц на символ; лёгкая часть
     (нормализованный стакан + bid/ask) — каждый апдейт;
  3. overview(ttl) — кэш шапки: её дёргает каждый WS-клиент каждую секунду.

Плюс smoke-проверка нового /api/perf (диагностика тормозов числами).
"""
import random
import time

import pytest

from app.metrics import RingBuffer, SymbolState
from app.state import Store


def _mk_state(px: float = 100.0) -> SymbolState:
    st = SymbolState("t", "T", "swap", "AAA/USDT:USDT", "AAA", "USDT")
    st.last = px
    return st


def _book(px: float = 100.0, fat_bid: bool = False) -> dict:
    bids = [[px * (1 - i * 0.0002), 10.0] for i in range(1, 51)]
    asks = [[px * (1 + i * 0.0002), 10.0] for i in range(1, 51)]
    if fat_bid:
        bids[5][1] = 5000.0
    return {"bids": bids, "asks": asks}


# --------------------------------------------------------------------------
# 1. ret(): bisect обязан давать ровно тот же результат, что линейный скан
# --------------------------------------------------------------------------
class TestRetBisect:
    @staticmethod
    def _brute(st: SymbolState, seconds: int):
        """Эталонная (старая) реализация — линейный скан."""
        if len(st.price_hist) < 2 or not st.last:
            return None
        newest = st.price_hist[-1][0]
        oldest = st.price_hist[0][0]
        if newest - oldest < seconds * 0.9:
            return None
        cutoff = newest - seconds
        for ts, p in st.price_hist:
            if ts >= cutoff and p > 0:
                return (st.last / p - 1.0) * 100.0
        return None

    def test_matches_bruteforce(self):
        rng = random.Random(42)
        st = _mk_state()
        now = 1_700_000_000.0
        for k in range(500):
            st.price_hist.append(now - 500 + k, 100.0 + rng.gauss(0, 1.0))
        st.last = st.price_hist[-1][1]
        for sec in (30, 60, 300, 499, 3600):
            want = self._brute(st, sec)
            got = st.ret(sec)
            if want is None:
                assert got is None, f"ret({sec}): ждали None, получили {got}"
            else:
                assert got == pytest.approx(want), f"ret({sec}) разошёлся с brute"

    def test_short_history_returns_none(self):
        """Гард «история короче окна» обязан пережить переход на bisect."""
        st = _mk_state()
        now = time.time()
        for k in range(10):
            st.price_hist.append(now - 10 + k, 100.0)
        assert st.ret(3600) is None
        assert st.ret(60) is None       # 9 c истории < 60*0.9

    def test_empty_and_single_point(self):
        st = _mk_state()
        assert st.ret(60) is None
        st.price_hist.append(time.time(), 100.0)
        assert st.ret(60) is None

    def test_ringbuffer_bisect_index(self):
        rb = RingBuffer(100)
        for i in range(50):
            rb.append(float(i), float(i * 2))
        assert rb.bisect_ts(10.0) == 10     # точное совпадение
        assert rb.bisect_ts(10.5) == 11     # между точками
        assert rb.bisect_ts(-1.0) == 0
        assert rb.bisect_ts(999.0) == 50    # за концом
        assert rb.value_at(10) == 20.0


# --------------------------------------------------------------------------
# 2. apply_book(min_interval): тяжёлая часть троттлится, лёгкая — всегда
# --------------------------------------------------------------------------
class TestBookThrottle:
    def test_first_call_computes(self):
        st = _mk_state()
        st.apply_book(_book(), min_interval=0.25)
        assert st.densities
        assert st.imbalance.bid_quote > 0

    def test_heavy_part_skipped_within_interval(self):
        st = _mk_state()
        st.apply_book(_book(), min_interval=0.25)
        d1 = st.densities
        st.apply_book(_book(fat_bid=True), min_interval=0.25)
        # кластеризация НЕ пересчитана — объект плотностей тот же
        assert st.densities is d1

    def test_light_part_always_fresh(self):
        """Top-of-book (bid/ask) обновляется КАЖДЫЙ раз — подсветка цен в UI
        обязана видеть свежую цену даже между тяжёлыми пересчётами. Полный
        список уровней self.book живёт на тяжёлом пути (раз в min_interval):
        лестница drawer читается в момент открытия и 4 Гц ей хватает."""
        st = _mk_state()
        st.apply_book(_book(px=100.0), min_interval=0.25)
        st.apply_book(_book(px=101.0), min_interval=0.25)
        want = 101.0 * (1 - 0.0002)
        assert st.bid == pytest.approx(want)
        assert st.ask == pytest.approx(101.0 * (1 + 0.0002))
        # внутри окна троттлинга нормализованный стакан НЕ пересобирается...
        assert st.book["bids"][0][0] == pytest.approx(100.0 * (1 - 0.0002))

    def test_light_path_no_normalization(self):
        """Лёгкий путь не строит списки уровней: на 510 книг × 10 Гц это
        были миллионы аллокаций в секунду (см. bench_perf sim_books)."""
        st = _mk_state()
        st.apply_book(_book(px=100.0), min_interval=10.0)
        book_before = st.book
        st.apply_book(_book(px=105.0), min_interval=10.0)
        assert st.book is book_before, "в окне троттлинга self.book заменяться не должен"
        assert st.bid == pytest.approx(105.0 * (1 - 0.0002))

    def test_light_path_tolerates_junk_levels(self):
        """Пустые/мусорные уровни не должны ронять лёгкий путь."""
        st = _mk_state()
        st.apply_book(_book(px=100.0), min_interval=10.0)
        st.apply_book({"bids": [], "asks": [[]], "timestamp": 1}, min_interval=10.0)
        assert st.bid == pytest.approx(100.0 * (1 - 0.0002))

    def test_recomputes_after_interval(self):
        st = _mk_state()
        st.apply_book(_book(), min_interval=0.05)
        d1 = st.densities
        time.sleep(0.06)
        st.apply_book(_book(fat_bid=True), min_interval=0.05)
        assert st.densities is not d1

    def test_zero_interval_recomputes_every_call(self):
        """Дефолт (0) — старое поведение: replay/тесты не зависят от троттлинга."""
        st = _mk_state()
        st.apply_book(_book())
        d1 = st.densities
        st.apply_book(_book())
        assert st.densities is not d1

    def test_throttled_book_still_valid_for_api(self):
        """Пропущенный пересчёт не обязан оставлять пустой стакан."""
        st = _mk_state()
        st.apply_book(_book(), min_interval=10.0)
        st.apply_book(_book(px=102.0), min_interval=10.0)
        assert st.book is not None
        assert len(st.book["bids"]) == 50
        assert st.densities  # от первого (полного) пересчёта


# --------------------------------------------------------------------------
# 3. overview(ttl): кэш в пределах ttl, без ttl — всегда свежий
# --------------------------------------------------------------------------
class TestOverviewCache:
    def test_ttl_returns_cached(self):
        store = Store()
        st = store.get_or_create("t", "T", "swap", "AAA/USDT:USDT", "AAA", "USDT")
        st.last = 100.0
        st.vol24_usd = 1000.0
        o1 = store.overview(ttl=5.0)
        st.vol24_usd = 9999.0
        o2 = store.overview(ttl=5.0)
        assert o2 is o1
        assert o2["volume_usd"] == pytest.approx(1000.0)

    def test_zero_ttl_always_fresh(self):
        store = Store()
        st = store.get_or_create("t", "T", "swap", "AAA/USDT:USDT", "AAA", "USDT")
        st.last = 100.0
        st.vol24_usd = 1000.0
        a = store.overview()
        st.vol24_usd = 9999.0
        b = store.overview()
        assert a is not b
        assert b["volume_usd"] == pytest.approx(9999.0)

    def test_ttl_expiry(self):
        store = Store()
        st = store.get_or_create("t", "T", "swap", "AAA/USDT:USDT", "AAA", "USDT")
        st.last = 100.0
        st.vol24_usd = 1.0
        store.overview(ttl=0.05)
        st.vol24_usd = 2.0
        time.sleep(0.06)
        o = store.overview(ttl=0.05)
        assert o["volume_usd"] == pytest.approx(2.0)


# --------------------------------------------------------------------------
# 4. Настройки и API
# --------------------------------------------------------------------------
def test_book_density_interval_setting():
    from app.config import SETTINGS
    assert SETTINGS.book_density_interval >= 0.0
    assert SETTINGS.book_density_interval <= 5.0


def test_perf_endpoint():
    from fastapi.testclient import TestClient
    from app.api import app
    with TestClient(app) as c:
        r = c.get("/api/perf")
        assert r.status_code == 200
        d = r.json()
        for key in ("loop_lag_ms", "build_rows_ms", "select_ms", "push_ms",
                    "uptime_s", "counters", "settings"):
            assert key in d, f"в /api/perf нет {key}"
        assert d["settings"]["push_interval"] >= 0.1
        assert "book_density_interval" in d["settings"]


def test_build_rows_measures_time():
    """build_rows обязан писать замер в _PERF — иначе /api/perf врёт."""
    from app import api
    from app.state import STORE
    st = STORE.get_or_create("bench", "Bench", "swap", "P1/USDT:USDT", "P1", "USDT")
    st.last = 1.0
    api._ROWS_CACHE.clear()
    api.build_rows(ttl=0.0)
    assert api._PERF["build_rows_ms"] >= 0.0
    assert api._PERF["build_rows_ms"] < 60_000  # защита от «забыли perf_counter»
