"""
Тесты диагностики ошибок подключения.

Когда биржа недоступна, пользователь видит сообщение — и от его точности
зависит, найдёт он причину сам или нет. ccxt кладёт в исключение URL, а
настоящую причину (getaddrinfo failed, CERTIFICATE_VERIFY_FAILED, 403) —
в конец строки, поэтому короткая обрезка показывала только URL.
"""
import ccxt
import pytest

from app.collector import describe_error, is_network_error, is_option_error


# --------------------------------------------------------------------------
# Классификация: «опция не поддержана» против «сеть недоступна»
# --------------------------------------------------------------------------
def test_option_error_recognised():
    e = ccxt.ExchangeError(
        'binanceusdm fetchMarkets() self.options fetchMarkets "swap" is not a '
        'supported market type')
    assert is_option_error(e)
    assert not is_network_error(e)


def test_network_error_not_mistaken_for_option_error():
    """
    Ключевой случай из реального запуска: при недоступной сети ccxt бросает
    NetworkError, и её нельзя принимать за «биржа не поддерживает опцию» —
    иначе код делает второй бессмысленный запрос и вдвое путает лог.
    """
    e = ccxt.NetworkError("gate GET https://api.gateio.ws/api/v4/spot/currencies")
    assert is_network_error(e)
    assert not is_option_error(e)


@pytest.mark.parametrize("exc", [
    ccxt.RequestTimeout("okx GET https://www.okx.com/... timed out"),
    ccxt.DDoSProtection("bybit 429 Too Many Requests"),
    ccxt.ExchangeNotAvailable("mexc service unavailable"),
])
def test_transient_errors_are_network(exc):
    assert is_network_error(exc)
    assert not is_option_error(exc)


def test_option_error_hint_not_triggered_by_network_with_word():
    """NetworkError со словом «not supported» в тексте не должен считаться опционной."""
    e = ccxt.NetworkError("fetchMarkets endpoint is not supported by this host")
    assert not is_option_error(e)


# --------------------------------------------------------------------------
# Человекочитаемое описание причины
# --------------------------------------------------------------------------
def test_describe_includes_exception_type():
    e = ccxt.NetworkError("binance GET https://fapi.binance.com/fapi/v1/exchangeInfo")
    d = describe_error(e)
    assert d.startswith("NetworkError:")
    assert "fapi.binance.com" in d


def test_describe_keeps_the_tail_not_the_head():
    """
    Настоящая причина в конце сообщения ccxt. Обрезка с начала (как было)
    показывала только URL и оставляла пользователя без диагноза.
    """
    cause = "Cannot connect to host api.binance.com:443 ssl:default [getaddrinfo failed]"
    e = ccxt.NetworkError("binance GET https://api.binance.com/fapi/v1/exchangeInfo " + cause)
    d = describe_error(e)
    assert "getaddrinfo failed" in d
    assert "DNS" in d


def test_long_message_is_truncated():
    e = ccxt.NetworkError("x" * 900 + " getaddrinfo failed")
    d = describe_error(e)
    assert len(d) < 400
    assert "getaddrinfo failed" in d


@pytest.mark.parametrize("text,hint", [
    ("Name or service not known / getaddrinfo failed", "DNS"),
    ("ssl.SSLCertVerificationError CERTIFICATE_VERIFY_FAILED", "сертификат"),
    ("Cannot connect to proxy tunnel connection failed", "прокси"),
    ("HTTP 451 restricted region", "регион"),
    ("HTTP 403 Forbidden not available in your jurisdiction", "регион"),
    ("operation timed out", "таймаут"),
    ("[WinError 10061] No connection could be made because the target machine actively refused it",
     "отклонено"),
])
def test_hints_for_common_causes(text, hint):
    d = describe_error(ccxt.NetworkError("binance GET https://api.binance.com/x " + text))
    assert hint.lower() in d.lower(), f"для {text!r} ожидалась подсказка про {hint!r}, получено: {d}"


def test_windows_specific_errors_hinted():
    """Пользователь запускал на Windows — типичные WinError должны опознаваться."""
    win = ("Cannot connect to host api.gateio.ws:443 ssl:default "
           "[Connect call failed ('127.0.0.1', 8888)]")
    d = describe_error(ccxt.NetworkError("gate GET https://api.gateio.ws/... " + win))
    assert "Connect call failed" in d


# --------------------------------------------------------------------------
# Регрессия (BUGFIXES #44): ccxt прячет настоящую причину в __cause__.
# В логе пользователя это выглядело как
#   ExchangeNotAvailable: binanceusdm GET https://fapi.binance.com/fapi/v1/exchangeInfo
# — сообщение обрывается на URL, потому что ccxt делает
#   raise ExchangeNotAvailable(' '.join([id, method, url])) from e
# и вся диагностика (SSL? сброс? прокси?) оставалась за кадром.
# --------------------------------------------------------------------------
def _wrapped(outer_msg: str, inner: BaseException) -> BaseException:
    try:
        try:
            raise inner
        except BaseException as src:
            raise ccxt.ExchangeNotAvailable(outer_msg) from src
    except ccxt.ExchangeNotAvailable as e:
        return e


def test_describe_unwraps_cause_chain():
    inner = OSError(10054, "An existing connection was forcibly closed by the remote host")
    e = _wrapped("binanceusdm GET https://fapi.binance.com/fapi/v1/exchangeInfo", inner)
    d = describe_error(e)
    assert "fapi.binance.com" in d            # URL на месте
    assert "OSError" in d                     # тип первопричины виден
    assert "10054" in d or "forcibly closed" in d
    assert "оборвал" in d                     # подсказка про обрыв соединения


def test_describe_unwraps_ssl_cause_and_hints():
    import ssl as _ssl
    inner = _ssl.SSLCertVerificationError(
        1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: "
           "self-signed certificate in certificate chain")
    e = _wrapped("bybit GET https://api.bybit.com/v5/market/instruments-info", inner)
    d = describe_error(e)
    assert "CERTIFICATE_VERIFY_FAILED" in d
    assert "сертификат" in d.lower() or "tls" in d.lower()


def test_describe_unwraps_multilevel_cause():
    """Цепочка из трёх уровней: нужен самый глубокий корень."""
    deep = OSError(10061, "Connection refused")
    mid = OSError("tunnel connection failed")
    mid.__cause__ = deep
    e = _wrapped("gate GET https://api.gateio.ws/api/v4/spot/currencies", mid)
    d = describe_error(e)
    assert "10061" in d or "Connection refused" in d
    assert "прокси" in d.lower() or "отклонено" in d.lower()


def test_describe_cause_cycle_terminates():
    """Цикл в __context__ не должен вешать describe_error."""
    a = RuntimeError("a")
    b = RuntimeError("b")
    a.__context__ = b
    b.__context__ = a
    d = describe_error(a)
    assert d.startswith("RuntimeError:")
    assert "b" in d


def test_describe_without_cause_unchanged():
    """Поведение для одиночного исключения не меняется."""
    e = ccxt.NetworkError("binance GET https://api.binance.com/x timed out")
    d = describe_error(e)
    assert "←" not in d
    assert "таймаут" in d.lower()


# --------------------------------------------------------------------------
# Стартовая проверка: пустой STORE обязан давать внятную инструкцию
# --------------------------------------------------------------------------
def test_startup_health_check_reports_when_empty(caplog):
    import asyncio
    import logging

    from app.api import _startup_health_check
    from app.state import STORE

    saved = dict(STORE._symbols)
    STORE._symbols.clear()
    try:
        caplog.set_level(logging.ERROR)
        # ускоряем: патчим задержку, чтобы тест не спал 30 секунд
        orig = asyncio.sleep

        async def fast(_):
            await orig(0)

        asyncio.sleep = fast
        try:
            asyncio.run(_startup_health_check(hub=None))
        finally:
            asyncio.sleep = orig
        text = caplog.text
        assert "НИ ОДНА БИРЖА НЕ ПОДКЛЮЧИЛАСЬ" in text
        assert "doctor.py" in text
        assert "HTTPS_PROXY" in text
        assert "replay" in text
    finally:
        STORE._symbols.update(saved)


def test_startup_health_check_silent_when_data_present(caplog):
    import asyncio
    import logging

    from app.api import _startup_health_check
    from app.state import STORE

    st = STORE.get_or_create("t", "Test", "swap", "BTC/USDT:USDT", "BTC", "USDT")
    st.set_price(1.0, 1.0)
    caplog.set_level(logging.ERROR)
    orig = asyncio.sleep

    async def fast(_):
        await orig(0)

    asyncio.sleep = fast
    try:
        asyncio.run(_startup_health_check(hub=None))
    finally:
        asyncio.sleep = orig
    assert "НИ ОДНА БИРЖА" not in caplog.text


# --------------------------------------------------------------------------
# Прокси: должен покрывать и REST, и WebSocket
# --------------------------------------------------------------------------
def test_proxy_from_env(monkeypatch):
    from app.collector import proxy_from_env
    for k in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy",
              "ALL_PROXY", "all_proxy"):
        monkeypatch.delenv(k, raising=False)
    assert proxy_from_env() is None
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:1080")
    assert proxy_from_env() == "http://127.0.0.1:1080"


def test_proxy_covers_rest_and_websocket(monkeypatch):
    """
    Одного aiohttp_trust_env мало: он влияет только на REST, а WebSocket в ccxt
    идёт через check_ws_proxy_settings() и атрибуты wsProxy/wssProxy/wsSocksProxy.

    И ровно ОДНА настройка на слой: ccxt бросает InvalidProxySettings
    («multiple conflicting proxy settings»), если заданы и httpProxy, и
    httpsProxy одновременно — проверено живым прогоном doctor.py.
    """
    from app import collector as C
    monkeypatch.setattr(C, "SETTINGS_PROXY_OVERRIDE", "")
    monkeypatch.setenv("HTTPS_PROXY", "http://vpn.local:8080")
    opts: dict = {}
    assert C.apply_proxy(opts) == "http://vpn.local:8080"
    assert opts["aiohttp_trust_env"] is True             # страховка REST
    assert opts["httpsProxy"] == "http://vpn.local:8080"  # REST (https)
    assert opts["wssProxy"] == "http://vpn.local:8080"    # WebSocket (wss)
    assert "httpProxy" not in opts                        # конфликт в ccxt


def test_proxy_opts_accepted_by_ccxt(monkeypatch):
    """
    Регрессия: opts из apply_proxy обязаны проходить валидацию ccxt.
    Конструктор + check_proxy_settings/check_ws_proxy_settings на реальном
    классе биржи — без сети, но с настоящим кодом ccxt.
    """
    import ccxt
    from app import collector as C
    monkeypatch.setattr(C, "SETTINGS_PROXY_OVERRIDE", "http://vpn.local:8080")
    opts: dict = {"enableRateLimit": True}
    C.apply_proxy(opts)
    ex = ccxt.binanceusdm(opts)
    # ни одна из проверок не должна бросить InvalidProxySettings
    ex.check_proxy_settings("https://fapi.binance.com/fapi/v1/ping", "GET", None, None)
    ex.check_ws_proxy_settings()


def test_proxy_override_beats_env(monkeypatch):
    from app import collector as C
    monkeypatch.setenv("HTTPS_PROXY", "http://env.proxy:1080")
    monkeypatch.setattr(C, "SETTINGS_PROXY_OVERRIDE", "http://explicit.proxy:8080")
    opts: dict = {}
    assert C.apply_proxy(opts) == "http://explicit.proxy:8080"


def test_no_proxy_leaves_opts_untouched(monkeypatch):
    from app import collector as C
    for k in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy",
              "ALL_PROXY", "all_proxy"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(C, "SETTINGS_PROXY_OVERRIDE", "")
    opts: dict = {}
    assert C.apply_proxy(opts) is None
    assert opts == {}


def test_proxy_url_setting_reads_env(monkeypatch):
    import importlib

    from app import config as cfg
    monkeypatch.setenv("PROXY_URL", "http://settings.proxy:3128")
    importlib.reload(cfg)
    try:
        assert cfg.load_settings().proxy_url == "http://settings.proxy:3128"
    finally:
        monkeypatch.delenv("PROXY_URL", raising=False)
        importlib.reload(cfg)
