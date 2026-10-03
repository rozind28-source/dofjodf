"""Тесты фильтр-движка: парсинг параметров, диапазоны, флаги, сортировка."""
import pytest

from app import filters as F


def row(**kw):
    base = dict(k="binanceusdm:BTC/USDT:USDT", ex="binanceusdm", exl="Binance Futures",
                mt="swap", s="BTC/USDT:USDT", b="BTC", q="USDT", last=83000.0,
                chg=1.5, rng=4.0, vol=1_000_000.0, tr=1000, natr=0.8,
                cvd=500.0, d1m=10.0, imb=1.1, fund=0.0001, oiusd=5e8,
                spike=None, r60=0.1, r300=0.5, r900=1.0, r3600=2.0, r14400=3.0)
    base.update(kw)
    return base


# --------------------------------------------------------------------------
def test_parse_params_ranges_and_flags():
    p = F.parse_params({"vol_min": "1000", "chg_max": "5.5", "spike": "1",
                        "natr": "", "ex": "Bybit,OKX", "q": "btc"})
    assert p["vol_min"] == 1000.0
    assert p["chg_max"] == 5.5
    assert p["spike"] is True
    assert "natr_min" not in p and "natr_max" not in p
    assert p["ex"] == ["Bybit", "OKX"]
    assert p["q"] == "BTC"


def test_parse_params_ignores_garbage():
    p = F.parse_params({"vol_min": "abc", "chg_min": "", "unknown_field": "1"})
    assert p == {}


def test_parse_params_rejects_unknown_flag():
    """Флаг вне каталога не должен становиться условием (иначе молча фильтрует всё)."""
    assert F.parse_params({"nosuchflag": "1"}) == {}


# --------------------------------------------------------------------------
def test_range_filter_min_only():
    rows = [row(vol=500), row(k="b", vol=2000)]
    out = F.apply_filters(rows, {"vol_min": 1000})
    assert len(out) == 1 and out[0]["vol"] == 2000


def test_range_filter_both_bounds():
    rows = [row(chg=-5), row(k="b", chg=2), row(k="c", chg=50)]
    out = F.apply_filters(rows, {"chg_min": 0, "chg_max": 10})
    assert [r["chg"] for r in out] == [2]


def test_none_values_are_excluded_from_range():
    """None ≠ 0: инструмент без метрики не должен проходить числовой фильтр."""
    rows = [row(natr=None), row(k="b", natr=2.0)]
    out = F.apply_filters(rows, {"natr_min": 1.0})
    assert [r["natr"] for r in out] == [2.0]


def test_flag_spike():
    rows = [row(), row(k="b", spike={"kind": "volume", "ratio": 4.2, "age": 10})]
    assert len(F.apply_filters(rows, F.parse_params({"spike": "1"}))) == 1


def test_flag_green_red():
    rows = [row(chg=3), row(k="b", chg=-3), row(k="c", chg=0)]
    assert len(F.apply_filters(rows, F.parse_params({"green": "1"}))) == 1
    assert len(F.apply_filters(rows, F.parse_params({"red": "1"}))) == 1


def test_unique_filter_uses_exchange_universe():
    rows = [row(b="BTC"), row(k="bybit:BTC", exl="Bybit", b="BTC"), row(k="c", b="SOLO")]
    dup = F.base_universe(rows)
    assert dup["BTC"] == {"Binance Futures", "Bybit"}
    out = F.apply_filters(rows, F.parse_params({"unique": "1"}), dup)
    assert [r["b"] for r in out] == ["SOLO"]


def test_exchange_and_market_filters():
    rows = [row(), row(k="b", exl="OKX", ex="okx"), row(k="c", mt="spot", exl="OKX", ex="okx")]
    assert len(F.apply_filters(rows, F.parse_params({"ex": "OKX"}))) == 2
    assert len(F.apply_filters(rows, F.parse_params({"ex": "binanceusdm"}))) == 1
    assert len(F.apply_filters(rows, F.parse_params({"mt": "spot"}))) == 1


def test_search_by_base_and_symbol():
    rows = [row(b="BTC"), row(k="b", s="ETH/USDT", b="ETH")]
    assert len(F.apply_filters(rows, F.parse_params({"q": "BTC"}))) == 1
    # поиск регистронезависимый: parse_params приводит к верхнему регистру
    assert len(F.apply_filters(rows, F.parse_params({"q": "eth"}))) == 1


# --------------------------------------------------------------------------
def test_sort_rows_desc_and_asc():
    rows = [row(vol=1), row(k="b", vol=3), row(k="c", vol=2)]
    assert [r["vol"] for r in F.sort_rows(rows, "vol", True)] == [3, 2, 1]
    assert [r["vol"] for r in F.sort_rows(rows, "vol", False)] == [1, 2, 3]


def test_sort_rows_none_goes_last():
    """None обязан уходить в конец при любой направленности, а не ломать порядок."""
    rows = [row(natr=None), row(k="b", natr=5.0), row(k="c", natr=1.0)]
    desc = F.sort_rows(rows, "natr", True)
    asc = F.sort_rows(rows, "natr", False)
    assert desc[-1]["natr"] is None
    assert asc[-1]["natr"] is None
    assert [r["natr"] for r in desc[:2]] == [5.0, 1.0]
    assert [r["natr"] for r in asc[:2]] == [1.0, 5.0]


def test_sort_unknown_field_falls_back_to_volume():
    rows = [row(vol=1), row(k="b", vol=9)]
    assert F.sort_rows(rows, "no_such_field", True)[0]["vol"] == 9


# --------------------------------------------------------------------------
def test_catalog_is_self_consistent():
    cat = F.catalog()
    assert cat and len(cat) == len(F.FILTER_SPECS)
    fields = {c["field"] for c in cat}
    # каждое поле сортировки должно существовать в каталоге фильтров
    for s in F.sort_catalog():
        assert s in fields or s in ("u", "last", "vol", "tr"), s


def test_all_presets_reference_known_fields():
    """Пресет с опечаткой в поле молча ничего не фильтровал бы."""
    known = {c["field"] for c in F.catalog()}
    for p in F.preset_catalog():
        for k in p["params"]:
            if k in ("sort", "desc"):
                continue
            base = k.rsplit("_", 1)[0] if k.endswith(("_min", "_max")) else k
            assert base in known, f"пресет {p['id']}: неизвестное поле {base}"


def test_preset_params_are_parseable():
    for p in F.preset_catalog():
        raw = {k: str(v) for k, v in p["params"].items() if k not in ("sort", "desc")}
        parsed = F.parse_params(raw)
        assert parsed or not raw


def test_sort_fields_cover_row_keys():
    from app.metrics import SymbolState
    st = SymbolState("e", "E", "swap", "BTC/USDT:USDT", "BTC", "USDT")
    st.apply_ticker({"last": 1.0, "quoteVolume": 1.0})
    keys = set(st.to_row())
    for f in F.sort_catalog():
        assert f in keys, f"поле сортировки {f} отсутствует в строке"
