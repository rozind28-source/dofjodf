"""
Тесты метрик. Каждый тест привязан к реальному багу, который был найден
и починен при разработке — поэтому здесь зашиты конкретные числа с бирж.
"""
import time

import pytest

from app.metrics import SymbolState, atr, natr, true_range


# --------------------------------------------------------------------------
# Пересчёт оборота в USD: четыре реальных случая с четырёх бирж
# --------------------------------------------------------------------------
# Числа сняты с живых API (2026-10-01) и сверены между собой:
#   Bybit inverse turnover24h = 2663.47 BTC · $83.8k ≈ baseVolume(контракты, USD)
#   OKX   inverse volCcy24h   = 6925.85 BTC · $83.8k ≈ baseVolume · contractSize
REAL_CASES = [
    # (биржа, символ, inverse, contractSize, ticker, ожидаемый USD)
    ("Bybit linear", "BTC/USDT:USDT", False, 1.0,
     {"last": 83813.4, "baseVolume": 75922.419, "quoteVolume": 6386753044.2181},
     6_386_753_044.2181),
    ("Bybit inverse", "BTC/USD:BTC", True, 1.0,
     {"last": 83759.3, "baseVolume": 224364679.0, "quoteVolume": 2663.4717},
     224_364_679.0),
    ("OKX linear", "BTC/USDT:USDT", False, 0.01,
     {"last": 83830.0, "baseVolume": 9735576.04, "quoteVolume": None},
     9735576.04 * 0.01 * 83830.0),
    ("OKX inverse", "BTC/USD:BTC", True, 100.0,
     {"last": 83776.1, "baseVolume": 5825120.2, "quoteVolume": None},
     582_512_020.0),
]


@pytest.mark.parametrize("name,symbol,inverse,cs,ticker,expected", REAL_CASES)
def test_usd_volume_real_cases(name, symbol, inverse, cs, ticker, expected):
    st = SymbolState("ex", "EX", "swap", symbol, "BTC",
                     "USD" if inverse else "USDT")
    st.set_contract(inverse, cs)
    st.apply_ticker(dict(ticker))
    assert st.vol24_usd == pytest.approx(expected, rel=1e-6), name


def test_inverse_volume_not_multiplied_by_price(sym, inv):
    """
    Главный баг: у инверсных контрактов amount уже в USD.
    Умножение на цену давало price² и «плотности» в триллион долларов.
    """
    inv.apply_ticker({"last": 83776.1, "baseVolume": 5825120.2})
    sym.apply_ticker({"last": 83776.1, "baseVolume": 6925.85, "quoteVolume": None})
    # инверс: контракты уже доллары
    assert inv.vol24_usd == pytest.approx(582_512_020.0, rel=1e-6)
    # линейный без quoteVolume: базовый объём · цену
    assert sym.vol24_usd == pytest.approx(6925.85 * 83776.1, rel=1e-6)
    # главное — у инверса НЕ должно получиться baseVolume·price (= $488 млрд)
    buggy = 5825120.2 * 83776.1
    assert inv.vol24_usd < buggy / 100


def test_quote_value_inverse_vs_linear(sym, inv):
    price = 83776.1
    assert inv.quote_value(500.0, price) == pytest.approx(500.0 * 100.0)
    assert sym.quote_value(0.5, price) == pytest.approx(0.5 * price)
    assert sym.base_amount(0.5, price) == pytest.approx(0.5)
    # contractSize=100: 100 контрактов = $10 000 = 10000/price BTC
    assert inv.base_amount(100.0, price) == pytest.approx(100.0 * 100.0 / price)


# --------------------------------------------------------------------------
# Число сделок: Binance отдаёт его только в raw info
# --------------------------------------------------------------------------
def test_trades_count_from_raw_info(sym):
    sym.apply_ticker({"last": 83845.0, "count": None,
                      "info": {"count": 4021437}})
    assert sym.trades24 == 4021437


def test_trades_count_absent_stays_none(sym):
    """Bybit/OKX число сделок не отдают — None честнее нуля (иначе сортировка врёт)."""
    sym.apply_ticker({"last": 83845.0, "info": {}})
    assert sym.trades24 is None
    assert sym.to_row()["tr"] is None


# --------------------------------------------------------------------------
# Доходность: окно ретроспективы обязано быть покрыто историей
# --------------------------------------------------------------------------
def test_ret_returns_none_when_history_shorter_than_window(sym):
    """
    Баг, который портил данные в LIVE: при 15-минутном буфере ret(3600)
    молча возвращал 15-минутную доходность под видом часовой.
    """
    now = time.time()
    for i in range(20):                      # 20 точек по 1 секунде = 20 секунд истории
        sym.set_price(100.0 + i * 0.1, now - (19 - i))
    assert sym.ret(60) is None               # окно 60с > 20с истории
    assert sym.ret(3600) is None
    assert sym.ret(14400) is None
    assert sym.to_row()["r3600"] is None


def test_ret_works_when_history_covers_window(sym):
    now = time.time()
    for i in range(200):
        sym.set_price(100.0 + i * 0.05, now - (199 - i))
    r = sym.ret(60)
    assert r is not None
    assert r == pytest.approx((sym.last / (sym.last - 60 * 0.05) - 1) * 100, abs=0.05)


def test_ret_falls_back_to_ohlcv(sym):
    """Если истории цен не хватает, но свечи есть — считаем по свечам."""
    candles = [[0, 100, 101, 99, 100 + i * 0.1, 10] for i in range(120)]
    sym.apply_ohlcv(candles)
    assert sym.ret(3600) is None              # истории цен нет вовсе
    assert sym.ret_ohlcv(60) is not None      # но свечной путь работает


# --------------------------------------------------------------------------
# Плотности: кластеризация не должна сливать весь стакан
# --------------------------------------------------------------------------
def test_book_clustering_does_not_merge_whole_book(sym):
    """
    Баг: допуск 0.05% от цены BTC = $42, а весь стакан из 200 уровней
    умещается в $20 → все уровни сливались в одну «плотность».
    """
    px = 83813.4
    tick = 0.1
    bids = [[px - i * tick, 1.0] for i in range(1, 201)]
    asks = [[px + i * tick, 1.0] for i in range(1, 201)]
    sym.set_price(px, time.time())
    sym.apply_book({"bids": bids, "asks": asks}, big_usd=50_000, huge_usd=250_000)

    assert len(sym.densities) > 1, "стакан слился в один кластер"
    total_book = sum(p * a for p, a in bids) + sum(p * a for p, a in asks)
    assert sym.densities[0].quote < total_book, "крупнейшая плотность = весь стакан"
    # суммарный объём плотностей не превышает физический объём стакана
    assert sum(d.quote for d in sym.densities) <= total_book * 1.0001
    # и кластеры не вырождаются в один уровень
    assert sym.densities[0].n_orders > 1


def test_book_totals_match_physical_book(sym):
    """Суммарная оценка стакана обязана совпадать с суммой p·a по уровням."""
    px = 100.0
    bids = [[px - i * 0.01, 10.0] for i in range(1, 51)]
    asks = [[px + i * 0.01, 10.0] for i in range(1, 51)]
    sym.set_price(px, time.time())
    sym.apply_book({"bids": bids, "asks": asks}, big_usd=1, huge_usd=2)
    expected_bid = sum(p * a for p, a in bids)
    expected_ask = sum(p * a for p, a in asks)
    assert sym.imbalance.bid_quote == pytest.approx(expected_bid, rel=1e-9)
    assert sym.imbalance.ask_quote == pytest.approx(expected_ask, rel=1e-9)
    # симметричный по размеру стакан: биды дешевле → ratio слегка < 1
    assert sym.imbalance.ratio == pytest.approx(expected_bid / expected_ask, rel=1e-9)
    assert 0.99 < sym.imbalance.ratio < 1.0


def test_big_density_threshold(sym):
    px = 100.0
    bids = [[px - 0.01, 10.0]]                     # $1 000
    asks = [[px + 0.01, 6000.0]]                   # $600 000 — «крупная»
    sym.set_price(px, time.time())
    sym.apply_book({"bids": bids, "asks": asks}, big_usd=50_000, huge_usd=250_000)
    assert len(sym.big_densities) == 1
    assert sym.big_densities[0].side == "ask"
    assert sym.big_densities[0].quote == pytest.approx(100.01 * 6000.0, rel=1e-9)


# --------------------------------------------------------------------------
# Спайки: база только из закрытых минут
# --------------------------------------------------------------------------
def test_no_spikes_before_three_minutes(sym):
    """
    Баг: на старте база бралась как «суточный объём / 1440» в других единицах,
    из-за чего появлялись спайки вида 723205x. Первые минуты обязаны молчать.
    """
    t0 = 1_700_000_000.0
    for minute in range(2):
        for k in range(3):
            sym.apply_trade({"amount": 10.0, "price": 100.0, "side": "buy",
                             "timestamp": (t0 + minute * 60 + k) * 1000})
        sym._roll_minute(t0 + (minute + 1) * 60 + 1)
    assert len(sym.spikes) == 0


def test_spike_detected_after_warmup(sym):
    t0 = 1_700_000_000.0
    # четыре спокойные минуты — набираем базу
    for minute in range(4):
        sym.apply_trade({"amount": 1.0, "price": 100.0, "side": "buy",
                         "timestamp": (t0 + minute * 60 + 5) * 1000})
        sym._roll_minute(t0 + (minute + 1) * 60 + 1)
    assert len(sym.spikes) == 0
    # пятая — в 40 раз больше
    for k in range(40):
        sym.apply_trade({"amount": 1.0, "price": 100.0, "side": "buy",
                         "timestamp": (t0 + 4 * 60 + 5 + k * 0.1) * 1000})
    sym._roll_minute(t0 + 5 * 60 + 1)
    kinds = {sp.kind for sp in sym.spikes}
    assert "volume" in kinds
    assert kinds <= {"volume", "trades"}     # 40 сделок против 1 в минуту — тоже спайк
    for sp in sym.spikes:
        assert 3.0 < sp.ratio < 1000.0, f"ratio вне разумных пределов: {sp.ratio}"


def test_spike_ratio_capped(sym):
    """Аномалия в данных не должна порождать ratio в сотни тысяч."""
    t0 = 1_700_000_000.0
    for minute in range(4):
        sym.apply_trade({"amount": 1.0, "price": 100.0, "side": "buy",
                         "timestamp": (t0 + minute * 60 + 5) * 1000})
        sym._roll_minute(t0 + (minute + 1) * 60 + 1)
    sym.apply_trade({"amount": 1e9, "price": 100.0, "side": "buy",
                     "timestamp": (t0 + 4 * 60 + 5) * 1000})
    sym._roll_minute(t0 + 5 * 60 + 1)
    for sp in sym.spikes:
        assert sp.ratio < 1000.0


def test_delta_and_cvd_sign(sym):
    sym.apply_trade({"amount": 2.0, "price": 100.0, "side": "buy", "timestamp": time.time() * 1000})
    sym.apply_trade({"amount": 1.0, "price": 100.0, "side": "sell", "timestamp": time.time() * 1000})
    assert sym.cvd == pytest.approx(100.0)          # +200 − 100
    assert sym.buy_quote_min == pytest.approx(200.0)
    assert sym.sell_quote_min == pytest.approx(100.0)


# --------------------------------------------------------------------------
# NATR / ATR
# --------------------------------------------------------------------------
def test_true_range_uses_previous_close():
    candles = [[0, 10, 12, 9, 11, 1], [1, 11, 15, 5, 14, 1]]
    tr = true_range(candles)
    assert tr[0] == pytest.approx(3.0)              # 12 − 9
    assert tr[1] == pytest.approx(10.0)             # max(15−5, |15−11|, |5−11|)


def test_natr_normalized_by_close():
    candles = [[i, 100, 102, 98, 100, 1] for i in range(40)]
    assert atr(candles, 14) == pytest.approx(4.0, rel=0.05)
    assert natr(candles, 14) == pytest.approx(4.0, rel=0.05)   # close = 100 → 4%


def test_natr_zero_on_empty():
    assert natr([], 14) == 0.0
    assert atr([], 14) == 0.0


def test_to_row_shape(sym):
    sym.apply_ticker({"last": 100.0, "open": 95.0, "high": 105.0, "low": 94.0,
                      "baseVolume": 10.0, "quoteVolume": 1000.0, "info": {"count": 7}})
    row = sym.to_row()
    for k in ("k", "ex", "exl", "mt", "s", "b", "q", "last", "chg", "rng",
              "vol", "tr", "natr", "cvd", "imb", "fund", "oiusd", "spike", "u"):
        assert k in row, f"нет поля {k}"
    for tf in (60, 300, 900, 3600, 14400):
        assert f"r{tf}" in row
    assert row["chg"] == pytest.approx(5.263, abs=0.01)
    assert row["tr"] == 7
