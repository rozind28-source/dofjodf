"""Тесты движка алертов: режимы срабатывания, cooldown, область действия."""
import asyncio
import time

import pytest

from app.alerts import AlertEngine


def row(**kw):
    base = dict(k="bybit:BTC/USDT:USDT", ex="bybit", exl="Bybit", mt="swap",
                s="BTC/USDT:USDT", b="BTC", q="USDT", last=83000.0,
                chg=6.0, vol=1e9, natr=1.0, fund=0.0001, oiusd=5e8, spike=None)
    base.update(kw)
    return base


@pytest.fixture
def eng():
    return AlertEngine(cooldown=60.0)


def run(coro):
    """Каждый вызов — свой event loop: движжок от состояния loop не зависит."""
    return asyncio.run(coro)


# --------------------------------------------------------------------------
def test_add_validates_field_and_op(eng):
    with pytest.raises(ValueError):
        eng.add(field="no_such_metric", op="gt", value=1)
    with pytest.raises(ValueError):
        eng.add(field="chg", op="~=", value=1)
    r = eng.add(field="chg", op="gt", value=5)
    assert r.id and r.enabled and r.mode == "crossing"


def test_update_and_remove(eng):
    r = eng.add(field="chg", op="gt", value=5)
    eng.update(r.id, value=9.5, enabled=False)
    assert eng.rules[r.id].value == 9.5
    assert eng.rules[r.id].enabled is False
    assert eng.update("missing", value=1) is None
    assert eng.remove(r.id) is True
    assert eng.remove(r.id) is False


def test_describe_is_human_readable(eng):
    r = eng.add(field="chg", op="gt", value=5, exchange="Bybit")
    assert "Bybit" in r.describe() and ">" in r.describe()


# --------------------------------------------------------------------------
def test_crossing_fires_once(eng):
    """Режим «на пересечении»: один переход через порог = одно событие."""
    eng.add(field="chg", op="gt", value=5, mode="crossing", cooldown=0)
    r = row(chg=6.0)
    assert len(run(eng.evaluate([r]))) == 1        # False → True
    assert len(run(eng.evaluate([r]))) == 0        # уже True, пересечения нет
    assert len(run(eng.evaluate([row(chg=4.0)]))) == 0   # опустилось ниже
    assert len(run(eng.evaluate([r]))) == 1        # снова пересекло


def test_while_mode_fires_every_cycle(eng):
    eng.add(field="chg", op="gt", value=5, mode="while", cooldown=0)
    r = row(chg=6.0)
    assert len(run(eng.evaluate([r]))) == 1
    assert len(run(eng.evaluate([r]))) == 1


def test_cooldown_suppresses_repeats(eng):
    eng.add(field="chg", op="gt", value=5, mode="while", cooldown=3600)
    r = row(chg=6.0)
    assert len(run(eng.evaluate([r]))) == 1
    assert len(run(eng.evaluate([r]))) == 0        # внутри cooldown
    # и на повторном пересечении тоже
    run(eng.evaluate([row(chg=1.0)]))
    assert len(run(eng.evaluate([r]))) == 0


def test_lt_operator(eng):
    eng.add(field="fund", op="lt", value=-0.0001, cooldown=0)
    assert len(run(eng.evaluate([row(fund=-0.0005)]))) == 1
    assert len(run(eng.evaluate([row(fund=0.0005)]))) == 0


def test_none_value_never_fires(eng):
    """Инструмент без метрики не должен триггерить алерт."""
    eng.add(field="natr", op="gt", value=0.5, cooldown=0)
    assert len(run(eng.evaluate([row(natr=None)]))) == 0


# --------------------------------------------------------------------------
def test_scope_exchange(eng):
    eng.add(field="chg", op="gt", value=5, exchange="OKX", cooldown=0)
    assert len(run(eng.evaluate([row(exl="Bybit", chg=9)]))) == 0
    assert len(run(eng.evaluate([row(exl="OKX", chg=9)]))) == 1


def test_scope_market_type(eng):
    eng.add(field="chg", op="gt", value=5, market_type="spot", cooldown=0)
    assert len(run(eng.evaluate([row(mt="swap", chg=9)]))) == 0
    assert len(run(eng.evaluate([row(mt="spot", chg=9, k="x")])) ) == 1


def test_scope_symbol_contains(eng):
    eng.add(field="chg", op="gt", value=5, symbol_contains="ETH", cooldown=0)
    assert len(run(eng.evaluate([row(s="BTC/USDT", chg=9)]))) == 0
    assert len(run(eng.evaluate([row(s="ETH/USDT", chg=9, k="y")])) ) == 1


def test_disabled_rule_never_fires(eng):
    eng.add(field="chg", op="gt", value=5, enabled=False, cooldown=0)
    assert len(run(eng.evaluate([row(chg=99)]))) == 0


# --------------------------------------------------------------------------
def test_events_history_and_ui_payload(eng):
    eng.add(field="chg", op="gt", value=5, cooldown=0, label="тест")
    fired = run(eng.evaluate([row(chg=7.0)]))
    assert len(fired) == 1
    ev = fired[0].to_dict()
    assert ev["field"] == "chg" and ev["value"] == 7.0
    assert "Bybit" in ev["text"]
    assert eng.recent(10)[0]["rule_id"] == fired[0].rule_id
    # в UI уходит только нужный минимум полей строки
    assert set(ev["row"]) <= {"k", "s", "exl", "mt", "last", "chg", "vol",
                              "natr", "fund", "oiusd", "spike"}


def test_subscriber_receives_event(eng):
    eng.add(field="chg", op="gt", value=5, cooldown=0)
    q = eng.subscribe()
    run(eng.evaluate([row(chg=9.0)]))
    assert not q.empty()
    assert q.get_nowait()["field"] == "chg"
    eng.unsubscribe(q)
    run(eng.evaluate([row(chg=3.0)]))
    run(eng.evaluate([row(chg=9.0)]))
    assert q.empty()


def test_prev_state_pruned(eng):
    """Кэш состояний не должен расти бесконечно."""
    eng.add(field="chg", op="gt", value=5, cooldown=0)
    for i in range(50):
        run(eng.evaluate([row(k=f"s{i}", chg=1.0)]))
    before = len(eng._prev_state)
    for i in range(50):
        run(eng.evaluate([row(k=f"n{i}", chg=1.0)]))
    assert len(eng._prev_state) <= before + 50


def test_hits_counter(eng):
    eng.add(field="chg", op="gt", value=5, cooldown=0)
    run(eng.evaluate([row(chg=9.0)]))
    run(eng.evaluate([row(chg=1.0)]))
    run(eng.evaluate([row(chg=9.0)]))
    assert eng.list()[0]["hits"] == 2


# --------------------------------------------------------------------------
# Предрасчёт области: правила с биржей/рынком не сканируют весь снимок
# --------------------------------------------------------------------------
def test_scoped_grouping_fires_same_as_full_scan():
    """Группировка строк по (биржа, рынок) обязана давать ТЕ ЖЕ срабатывания,
    что и полный перебор: ищется и лейбл («Bybit»), и ccxt-id («bybit»)."""
    rows = [
        row(k="bybit:BTC/USDT:USDT", ex="bybit", exl="Bybit", mt="swap", chg=9.0),
        row(k="binanceusdm:ETH/USDT:USDT", ex="binanceusdm", exl="Binance", mt="swap",
            s="ETH/USDT:USDT", chg=9.0),
        row(k="binance:ETH/USDT", ex="binance", exl="Binance", mt="spot",
            s="ETH/USDT", chg=9.0),
        row(k="bybit:SOL/USDT:USDT", ex="bybit", exl="Bybit", mt="swap",
            s="SOL/USDT:USDT", chg=1.0),   # ниже порога — не сработает нигде
    ]

    by_label = AlertEngine(cooldown=60.0)
    by_label.add(field="chg", op="gt", value=5, exchange="Bybit", cooldown=0)
    assert {e.key for e in run(by_label.evaluate(rows))} == {"bybit:BTC/USDT:USDT"}

    by_id = AlertEngine(cooldown=60.0)
    by_id.add(field="chg", op="gt", value=5, exchange="bybit", cooldown=0)
    assert {e.key for e in run(by_id.evaluate(rows))} == {"bybit:BTC/USDT:USDT"}

    by_mt = AlertEngine(cooldown=60.0)
    by_mt.add(field="chg", op="gt", value=5, market_type="spot", cooldown=0)
    assert {e.key for e in run(by_mt.evaluate(rows))} == {"binance:ETH/USDT"}

    by_ex_mt = AlertEngine(cooldown=60.0)
    by_ex_mt.add(field="chg", op="gt", value=5, exchange="Binance",
                 market_type="swap", cooldown=0)
    assert {e.key for e in run(by_ex_mt.evaluate(rows))} == {"binanceusdm:ETH/USDT:USDT"}

    unscoped = AlertEngine(cooldown=60.0)
    unscoped.add(field="chg", op="gt", value=5, cooldown=0)
    assert {e.key for e in run(unscoped.evaluate(rows))} == {
        "bybit:BTC/USDT:USDT", "binanceusdm:ETH/USDT:USDT", "binance:ETH/USDT"}
