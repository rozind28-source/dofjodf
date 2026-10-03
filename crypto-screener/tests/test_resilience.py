"""
Тесты устойчивости подключения к биржам.

Реальный сценарий, который они закрывают: у пользователя все 8 бирж упали на
`load_markets` за две секунды (транзиентный сбой DNS), и скринер остался
пустым до ручного перезапуска. Ретраев не было вовсе, а дефолтный таймаут ccxt
(10 c) меньше времени загрузки рынков у Gate (~19 c) и Hyperliquid (~16 c).
"""
import asyncio

import ccxt
import pytest

from app.collector import ExchangeCollector, describe_error, is_network_error
from app.config import ExchangeConfig, Settings
from app.state import STORE


@pytest.fixture
def cfg():
    return ExchangeConfig("bybit", "Bybit", "swap", top_n=5, books=2)


@pytest.fixture
def settings():
    s = Settings()
    s.mode = "replay"
    s.load_retries = 3
    s.request_timeout = 20_000
    s.revive_interval = 15.0
    s.fetch_ohlcv = False
    return s


@pytest.fixture
def collector(cfg, settings):
    return ExchangeCollector(cfg, settings)


# --------------------------------------------------------------------------
def test_timeout_is_applied(collector, settings):
    """Дефолт ccxt (10 c) меньше реальной загрузки рынков у Gate/Hyperliquid."""
    assert collector.ex.timeout == settings.request_timeout
    assert collector.ex.timeout > 10_000, "таймаут должен превышать дефолт ccxt"


# --------------------------------------------------------------------------
def test_retries_on_network_error(collector):
    """Сетевая ошибка повторяется — именно так выглядит транзиентный сбой DNS."""
    calls = {"n": 0}

    async def flaky(restrict):
        calls["n"] += 1
        if calls["n"] < 3:
            raise ccxt.NetworkError("bybit GET https://api.bybit.com/... getaddrinfo failed")
        return 873

    async def run():
        return await collector._try_load_with_retries(flaky, True)

    got = asyncio.run(run())
    assert got == 873
    assert calls["n"] == 3, "должно было сделать ровно 3 попытки"


def test_no_retry_on_config_error(collector):
    """Ошибку конфигурации повторять бессмысленно — она от повтора не пройдёт."""
    calls = {"n": 0}

    async def bad_option(restrict):
        calls["n"] += 1
        raise ccxt.ExchangeError(
            'bybit fetchMarkets() self.options fetchMarkets "swap" is not a supported market type')

    async def run():
        return await collector._try_load_with_retries(bad_option, True)

    with pytest.raises(ccxt.ExchangeError):
        asyncio.run(run())
    assert calls["n"] == 1, "опционную ошибку повторять не должны"


def test_retries_exhausted_raises_last(collector, settings):
    calls = {"n": 0}

    async def always_fails(restrict):
        calls["n"] += 1
        raise ccxt.RequestTimeout("bybit timed out")

    async def run():
        return await collector._try_load_with_retries(always_fails, True)

    with pytest.raises(ccxt.RequestTimeout):
        asyncio.run(run())
    assert calls["n"] == settings.load_retries


def test_retries_respects_single_attempt(collector, settings):
    settings.load_retries = 1
    calls = {"n": 0}

    async def fails(restrict):
        calls["n"] += 1
        raise ccxt.NetworkError("net down")

    async def run():
        return await collector._try_load_with_retries(fails, True)

    with pytest.raises(ccxt.NetworkError):
        asyncio.run(run())
    assert calls["n"] == 1


# --------------------------------------------------------------------------
def test_failed_exchange_does_not_kill_the_process(collector, monkeypatch):
    """
    Ключевое требование: отвалившаяся биржа не должна ронять запуск.
    run() обязан перейти в режим восстановления, а не выйти с ошибкой.
    """
    async def fail_load():
        return False

    revived = {"called": False}

    async def fake_revive():
        revived["called"] = True

    monkeypatch.setattr(collector, "_load_markets", fail_load)
    monkeypatch.setattr(collector, "_revive_loop", fake_revive)
    asyncio.run(collector.run())
    assert revived["called"], "при неудачной загрузке рынков revive-цикл не запущен"


def test_revive_sets_retrying_status(collector, monkeypatch):
    """Статус биржи должен показывать «повторяем», а не зависшее «error»."""
    async def fail_load():
        return False

    attempts = {"n": 0}

    async def fake_wait_for(coro, timeout):
        # имитируем один цикл ожидания и останавливаем
        coro.close()
        attempts["n"] += 1
        if attempts["n"] >= 2:
            collector._stop.set()
            raise asyncio.TimeoutError
        raise asyncio.TimeoutError

    monkeypatch.setattr(collector, "_load_markets", fail_load)
    monkeypatch.setattr(asyncio, "wait_for", fake_wait_for)
    collector.settings.revive_interval = 0.01
    asyncio.run(collector._revive_loop())
    st = STORE.exchange_status.get(collector.cfg.label, {})
    assert st.get("state") in ("retrying", "online"), f"неожиданный статус: {st}"


def test_revive_recovers_when_exchange_returns(collector, monkeypatch):
    """Как только биржа ожила, коллектор должен поднять рабочие циклы сам."""
    state = {"loaded": False}

    async def load_markets():
        return state["loaded"]

    started = {"cycles": False}

    async def fake_after():
        started["cycles"] = True

    async def wait_once(coro, timeout):
        coro.close()
        state["loaded"] = True          # ко второй попытке биржа доступна
        raise asyncio.TimeoutError

    monkeypatch.setattr(collector, "_load_markets", load_markets)
    monkeypatch.setattr(collector, "_after_markets_loaded", fake_after)
    monkeypatch.setattr(asyncio, "wait_for", wait_once)
    collector.settings.revive_interval = 0.01
    asyncio.run(collector._revive_loop())
    assert started["cycles"], "после восстановления рабочие циклы не запущены"


# --------------------------------------------------------------------------
def test_settings_expose_reliability_knobs():
    from app.config import load_settings
    s = load_settings()
    assert s.request_timeout >= 15_000, "таймаут меньше времени загрузки Gate/Hyperliquid"
    assert s.load_retries >= 2
    assert s.revive_interval >= 15


def test_reliability_fields_are_on_settings_not_exchange():
    """
    Регрессия: поля однажды добавили в ExchangeConfig вместо Settings,
    и ExchangeCollector падал с AttributeError при создании.
    """
    s = Settings()
    for f in ("request_timeout", "load_retries", "revive_interval"):
        assert hasattr(s, f), f"в Settings нет {f}"
    e = ExchangeConfig("bybit", "Bybit", "swap")
    for f in ("request_timeout", "load_retries", "revive_interval"):
        assert not hasattr(e, f), f"{f} не должно быть в ExchangeConfig"


def test_network_error_classification_used_for_retry():
    """Ретрай делается только по сетевым ошибкам — проверяем саму классификацию."""
    assert is_network_error(ccxt.NetworkError("getaddrinfo failed"))
    assert is_network_error(ccxt.RequestTimeout("timed out"))
    assert not is_network_error(ccxt.ExchangeError("fetchMarkets not supported"))
    assert "getaddrinfo" in describe_error(ccxt.NetworkError("x" * 300 + " getaddrinfo failed"))
