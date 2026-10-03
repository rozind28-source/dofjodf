"""
Тесты на функции, появившиеся вместе с графиками и новыми биржами:
ресэмплинг свечей, нормализация объёма и разбор «нестандартных» стаканов.

Все ожидаемые значения получены из живых API и сверены между собой
(см. EXCHANGES.md), поэтому тест ловит именно ошибку единиц измерения,
а не просто «что-то вернулось».
"""
import pytest

from app.metrics import (SymbolState, TF_SECONDS, normalize_candle_volume,
                         resample)


def candle(ts_min, o, h, l, c, v):
    return [ts_min * 60_000, o, h, l, c, v]


# --------------------------------------------------------------------------
# Ресэмплинг
# --------------------------------------------------------------------------
def test_resample_groups_by_period_boundary():
    """Группировка по границам периода, а не «по N штук» — иначе свечи разъезжаются."""
    # 12 минутных свечей, старт со смещением, не кратным периоду
    base = 1_700_000_090
    src = [[(base + i * 60) * 1000, 100, 101, 99, 100, 10] for i in range(12)]
    out = resample(src, 300)
    assert len(out) >= 2
    for c in out:
        assert c[0] % (300 * 1000) == 0, "бакет не выровнен по границе периода"


def test_resample_ohlcv_aggregation():
    src = [
        candle(0, 100, 110, 90, 105, 10),
        candle(1, 105, 120, 100, 115, 20),
        candle(2, 115, 118, 95, 98, 30),
        candle(3, 98, 102, 96, 101, 40),
        candle(4, 101, 108, 100, 107, 50),
    ]
    out = resample(src, 300)                      # все 5 попадают в один бакет
    assert len(out) == 1
    _, o, h, l, c, v = out[0]
    assert o == 100                               # open первой
    assert h == 120                               # max high
    assert l == 90                                # min low
    assert c == 107                               # close последней
    assert v == 150                               # сумма объёмов


def test_resample_splits_into_buckets():
    src = [candle(i, 100, 101, 99, 100, 1) for i in range(10)]
    out = resample(src, 300)                      # 10 минут → два бакета по 5
    assert len(out) == 2
    assert out[1][0] - out[0][0] == 300_000


def test_resample_empty_and_passthrough():
    assert resample([], 300) == []
    src = [candle(0, 1, 2, 0.5, 1.5, 3)]
    assert resample(src, 60)[0][1:] == src[0][1:]


def test_tf_seconds_covers_ui_timeframes():
    for tf in ("1m", "5m", "15m", "1h", "4h", "1d"):
        assert tf in TF_SECONDS


# --------------------------------------------------------------------------
# Нормализация объёма свечей в USD
# --------------------------------------------------------------------------
def test_linear_volume_multiplied_by_close():
    """Binance/Aster/Hyperliquid: объём уже в базовой монете, contractSize=1."""
    out = normalize_candle_volume([candle(0, 100, 110, 90, 100, 53.81)],
                                  inverse=False, contract_size=1.0,
                                  vol_in_contracts=False)
    assert out[0][5] == pytest.approx(53.81 * 100)


def test_gate_mexc_volume_in_contracts():
    """
    Gate/MEXC отдают объём в контрактах по 0.0001 BTC.
    Сверка: Gate BTC 1m raw=1.03e6 → 103 BTC → ~$8.9M при $86.6k.
    """
    out = normalize_candle_volume([candle(0, 86600, 86700, 86500, 86600, 1_030_000)],
                                  inverse=False, contract_size=0.0001,
                                  vol_in_contracts=True)
    assert out[0][5] == pytest.approx(1_030_000 * 0.0001 * 86600)
    assert 8e6 < out[0][5] < 1e7


def test_okx_volume_already_in_base():
    """
    OKX для swap берёт из ответа индекс 6 = base volume (см. parse_ohlcv в ccxt),
    то есть объём уже в BTC. Домножать на contractSize=0.01 НЕЛЬЗЯ —
    именно так получалось $75k/мин вместо ~$7M.
    """
    raw = 86.5
    wrong = normalize_candle_volume([candle(0, 86600, 86700, 86500, 86600, raw)],
                                    inverse=False, contract_size=0.01,
                                    vol_in_contracts=True)
    right = normalize_candle_volume([candle(0, 86600, 86700, 86500, 86600, raw)],
                                    inverse=False, contract_size=0.01,
                                    vol_in_contracts=False)
    assert right[0][5] == pytest.approx(raw * 86600)
    assert wrong[0][5] == pytest.approx(right[0][5] / 100)
    assert 7e6 < right[0][5] < 8e6


def test_inverse_volume_not_scaled_by_price():
    out = normalize_candle_volume([candle(0, 83776, 84000, 83000, 83776, 500)],
                                  inverse=True, contract_size=100.0,
                                  vol_in_contracts=True)
    assert out[0][5] == pytest.approx(500 * 100)     # контракт уже в USD


def test_normalize_preserves_ohlc_and_order():
    src = [candle(i, 100 + i, 105 + i, 95 + i, 101 + i, 10) for i in range(5)]
    out = normalize_candle_volume(src, False, 1.0, False)
    assert len(out) == 5
    for a, b in zip(src, out):
        assert b[0] == a[0] and b[1:5] == [float(x) for x in a[1:5]]


# --------------------------------------------------------------------------
# Стакан с нестандартным числом полей (MEXC)
# --------------------------------------------------------------------------
def test_book_levels_with_three_fields(sym):
    """
    MEXC отдаёт [price, amount, count]. Жёсткая распаковка `for p, a in levels`
    падала с ValueError, и стаканов MEXC не было вовсе (350 ошибок в логе).
    """
    real = [[86608.0, 450512, 1.0], [86607.9, 12000, 2.0]]
    st = sym
    st.set_contract(False, 0.0001)
    st.set_price(86608.0, 1.0)
    st.apply_book({"bids": real, "asks": [[86608.1, 900000, 3.0]]},
                  big_usd=1, huge_usd=2)
    assert len(st.book["bids"]) == 2
    assert all(len(x) == 2 for x in st.book["bids"]), "уровни должны быть нормализованы до пар"
    assert st.imbalance.bid_quote > 0
    # 450512 контрактов · 0.0001 BTC · $86608 ≈ $3.9M
    assert st.imbalance.bid_quote == pytest.approx((450512 + 12000) * 0.0001 * 86608, rel=1e-3)


def test_norm_levels_skips_garbage(sym):
    assert sym._norm_levels([[100.0, 5.0], [None, 3], [], "x", [99.0, 2.0, 7]]) == \
        [(100.0, 5.0), (99.0, 2.0)]


def test_stored_book_is_normalized(sym):
    """Потребители (лестница цен в API) не должны знать про лишние поля биржи."""
    sym.set_price(100.0, 1.0)
    sym.apply_book({"bids": [[99.0, 1.0, 5]], "asks": [[101.0, 2.0, 9]]},
                   big_usd=1, huge_usd=2)
    assert sym.book["bids"] == [(99.0, 1.0)]
    assert sym.book["asks"] == [(101.0, 2.0)]


# --------------------------------------------------------------------------
# Идентификаторы рынков: коллизия спот/своп
# --------------------------------------------------------------------------
def test_exchange_config_flags():
    """
    Флаги нормализации объёма. Проверяем ПО РЫНКАМ, а не по id биржи:
    у Gate и MEXC спот отдаёт объём в базовой монете, а своп — в контрактах.
    Значения получены замерами (см. EXCHANGES.md), а не предположением.
    """
    from app.config import CORE_EXCHANGES, EXTRA_EXCHANGES
    by_key = {(e.id, e.market): e for e in CORE_EXCHANGES + EXTRA_EXCHANGES}

    # свопы Gate/MEXC — объём в контрактах (contractSize = 0.0001 BTC)
    for key in (("gate", "swap"), ("mexc", "swap")):
        assert key in by_key, f"{key} не объявлен"
        assert by_key[key].ohlcv_vol_in_contracts is True, f"{key}: объём в контрактах"

    # спот Gate/MEXC — объём в базовой монете, флаг обязан быть выключен
    for key in (("gate", "spot"), ("mexc", "spot")):
        if key in by_key:
            assert by_key[key].ohlcv_vol_in_contracts is False, \
                f"{key}: спот отдаёт объём в базовой монете"

    # у остальных бирж contractSize = 1 и объём сразу в базовой монете
    for eid in ("okx", "binanceusdm", "bybit", "aster", "hyperliquid"):
        for (i, _), e in by_key.items():
            if i == eid:
                assert e.ohlcv_vol_in_contracts is False, f"{eid}: флаг должен быть выключен"
    # глубина стакана обязана быть в списке валидных значений биржи, иначе она
    # отвечает BadRequest на ВСЕ подписки (Binance: -4021 "200 is not valid depth
    # limit"; Gate отказывает при limit > 100). Снаружи это выглядит как
    # «на бирже просто нет плотностей», поэтому проверяем по реальным замерам.
    from app.config import VALID_BOOK_LIMITS
    for (eid, mkt), e in by_key.items():
        valid = VALID_BOOK_LIMITS.get((eid, mkt))
        assert valid is not None, f"{eid}/{mkt}: нет замера валидных глубин — добавьте"
        if not valid:
            # пустой кортеж = стакан у биржи не работает вовсе (htx/dydx/paradex).
            # Тогда и подписки быть не должно, иначе поток будет вечно падать.
            assert e.books == 0 or not e.enabled, \
                f"{eid}: watch_order_book не работает, books обязан быть 0 (или биржа выключена)"
        else:
            assert e.book_limit in valid, \
                f"{eid}: book_limit={e.book_limit} невалиден, допустимы {valid}"


def test_valid_book_limits_cover_all_configured_exchanges():
    """Каждая объявленная биржа обязана иметь замеренную таблицу глубин."""
    from app.config import CORE_EXCHANGES, EXTRA_EXCHANGES, VALID_BOOK_LIMITS
    declared = {(e.id, e.market) for e in CORE_EXCHANGES + EXTRA_EXCHANGES}
    missing = declared - set(VALID_BOOK_LIMITS)
    assert not missing, f"не замерены валидные глубины для: {sorted(missing)}"


def test_new_exchanges_declared():
    from app.config import CORE_EXCHANGES
    labels = {e.label for e in CORE_EXCHANGES}
    for want in ("MEXC", "Gate.io", "Aster", "Hyperliquid"):
        assert want in labels, f"{want} не подключён по умолчанию"
    dex = {e.label for e in CORE_EXCHANGES if e.dex}
    assert dex == {"Aster", "Hyperliquid"}


def test_exchanges_env_does_not_duplicate_market_pair():
    """
    EXCHANGES=gate включает ОБА рынка биржи (spot и swap) — это задуманное
    поведение, т.к. рынок переключается тумблером в UI. Но одна и та же пара
    (биржа, рынок) не должна подключаться дважды.
    """
    import os
    from importlib import reload
    from app import config as cfg
    old = os.environ.get("EXCHANGES")
    try:
        os.environ["EXCHANGES"] = "gate"
        reload(cfg)
        s = cfg.load_settings()
        pairs = [(e.id, e.market) for e in s.exchanges]
        assert len(pairs) == len(set(pairs)), f"дубликаты пар: {pairs}"
        assert set(p[0] for p in pairs) == {"gate"}
        assert set(p[1] for p in pairs) <= {"spot", "swap"}
    finally:
        if old is None:
            os.environ.pop("EXCHANGES", None)
        else:
            os.environ["EXCHANGES"] = old
        reload(cfg)


def test_per_exchange_ticker_refresh_defaults_to_global():
    from app.config import load_settings
    s = load_settings()
    for e in s.exchanges:
        assert e.ticker_refresh > 0, f"{e.label}: период опроса не выставлен"
    slow = {e.label: e.ticker_refresh for e in s.exchanges
            if e.id in ("mexc", "hyperliquid")}
    assert all(v >= 30 for v in slow.values()), \
        f"MEXC/Hyperliquid отдают fetch_tickers 6-7 c, нужен более редкий опрос: {slow}"
