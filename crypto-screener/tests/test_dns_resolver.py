"""
Тесты переключения DNS-резолвера aiohttp и диагностики aiodns.

Реальный случай (Windows, октябрь 2026): шаги doctor 1–4 полностью зелёные
(urllib и socket резолвят имена и подключаются), а шаг 5 мгновенно падает
на ВСЕХ биржах:

    ExchangeNotAvailable: binanceusdm GET https://fapi.binance.com/fapi/v1/exchangeInfo
      ClientConnectorDNSError: Cannot connect to host fapi.binance.com:443 ssl:default
                               [Could not contact DNS servers]
        OSError: [Errno None] Could not contact DNS servers
          DNSError: (11, 'Could not contact DNS servers')

Причина: при установленном aiodns (ccxt.pro тянет его всегда) дефолтный
резолвер aiohttp — AsyncResolver: он шлёт DNS-запросы прямым UDP к серверам
из настроек адаптера, МИНУЯ службу DNS-клиента Windows. Антивирус, файрвол,
правила VPN-приложений и «шифрованный DNS» (DoH) этот путь блокируют, а
системный getaddrinfo (через dnscache) продолжает работать.

Лечение: apply_dns_resolver() переключает aiohttp.connector.DefaultResolver
на ThreadedResolver (тот же getaddrinfo в пуле потоков) — патч применяется
в ExchangeCollector.__init__ до создания сессий, поэтому покрывает и REST,
и WebSocket (WS ccxt использует ту же aiohttp-сессию биржи).
"""
import asyncio
import socket
import sys
import types

import aiohttp
import aiohttp.connector
import aiohttp.resolver
import ccxt
import pytest

from app.collector import apply_dns_resolver, describe_error


@pytest.fixture
def restore_resolver():
    """DefaultResolver — глобальное состояние aiohttp: восстанавливаем за собой."""
    orig = aiohttp.connector.DefaultResolver
    yield
    aiohttp.connector.DefaultResolver = orig


@pytest.fixture
def doctor():
    import doctor as d
    return d


# --------------------------------------------------------------------------
# apply_dns_resolver
# --------------------------------------------------------------------------
def test_threaded_forced_by_default(monkeypatch, restore_resolver):
    """Без DNS_RESOLVER дефолт aiohttp (AsyncResolver) заменяется на системный."""
    monkeypatch.delenv("DNS_RESOLVER", raising=False)
    aiohttp.connector.DefaultResolver = object()  # sentinel: «как будто AsyncResolver»
    assert apply_dns_resolver() == "threaded"
    assert aiohttp.connector.DefaultResolver is aiohttp.resolver.ThreadedResolver


def test_aiodns_mode_leaves_default_untouched(monkeypatch, restore_resolver):
    """DNS_RESOLVER=aiodns — осознанный возврат к дефолтному поведению aiohttp."""
    sentinel = object()
    aiohttp.connector.DefaultResolver = sentinel
    monkeypatch.setenv("DNS_RESOLVER", "aiodns")
    assert apply_dns_resolver() == "aiodns"
    assert aiohttp.connector.DefaultResolver is sentinel


def test_threaded_connector_actually_uses_threaded_resolver(monkeypatch, restore_resolver):
    """
    Патч модуля aiohttp.connector действует: TCPConnector берёт DefaultResolver
    в момент создания — новый коннектор обязан получить ThreadedResolver.
    """
    monkeypatch.delenv("DNS_RESOLVER", raising=False)
    aiohttp.connector.DefaultResolver = object()  # sentinel: «как будто AsyncResolver»
    apply_dns_resolver()

    async def run():
        c = aiohttp.TCPConnector()
        try:
            assert isinstance(c._resolver, aiohttp.resolver.ThreadedResolver)
        finally:
            await c.close()
    asyncio.run(run())


def test_resolver_applied_on_collector_init(monkeypatch, restore_resolver):
    """ExchangeCollector переключает резолвер до создания сессий aiohttp."""
    from app import collector as C
    from app.config import ExchangeConfig, Settings

    calls = []
    monkeypatch.setattr(C, "apply_dns_resolver",
                        lambda: calls.append(1) or "threaded")
    monkeypatch.setattr(C, "apply_proxy", lambda opts: None)
    cfg = ExchangeConfig("binanceusdm", "Binance", "swap", top_n=10, books=5)
    C.ExchangeCollector(cfg, Settings())
    assert calls == [1]


# --------------------------------------------------------------------------
# describe_error: подсказка про aiodns
# --------------------------------------------------------------------------
def test_describe_hints_aiodns_dns_chain():
    """Точная цепочка пользователя: ExchangeNotAvailable ← OSError ← DNSError."""
    try:
        try:
            try:
                raise Exception("(11, 'Could not contact DNS servers')")
            except Exception as dnse:
                raise OSError("[Errno None] Could not contact DNS servers") from dnse
        except OSError as oserr:
            raise ccxt.ExchangeNotAvailable(
                "binanceusdm GET https://fapi.binance.com/fapi/v1/exchangeInfo"
            ) from oserr
    except ccxt.ExchangeNotAvailable as e:
        d = describe_error(e)
    assert "aiodns" in d.lower()
    assert "ThreadedResolver" in d
    assert "Could not contact DNS servers" in d


def test_dns_hint_not_stolen_by_ssl_branch():
    """
    В сообщении ClientConnectorDNSError есть «ssl:default» — без порядка
    веток сработала бы подсказка про TLS-сертификаты и увела бы не туда.
    """
    e = ccxt.NetworkError(
        "Cannot connect to host fapi.binance.com:443 ssl:default "
        "[Could not contact DNS servers]")
    d = describe_error(e)
    assert "aiodns" in d.lower()
    assert "TLS-сертификат" not in d


def test_ordinary_getaddrinfo_hint_unchanged():
    """Обычный gaierror — это по-прежнему «не резолвится DNS», не aiodns."""
    e = ccxt.NetworkError(
        "Cannot connect to host api.binance.com:443 ssl:default [getaddrinfo failed]")
    d = describe_error(e)
    assert "не резолвится DNS" in d
    assert "aiodns" not in d.lower()


# --------------------------------------------------------------------------
# doctor: шаг 4c (aiodns против системного резолвера)
# --------------------------------------------------------------------------
def test_advice_has_aiodns_verdict(doctor):
    assert "aiodns_dns_blocked" in doctor.ADVICE
    text = " ".join(doctor.ADVICE["aiodns_dns_blocked"])
    assert "python run.py" in text
    assert "DNS_RESOLVER=aiodns" in text


def test_check_aiodns_layer_absent(doctor, monkeypatch):
    """aiodns не установлен → aiohttp и так на системном резолвере, проблем нет."""
    monkeypatch.setitem(sys.modules, "aiodns", None)  # import aiodns → ImportError
    problems, verdict = asyncio.run(doctor.check_aiodns_layer(2.0))
    assert problems == []
    assert verdict == ""


def _fake_aiodns(monkeypatch, fail: bool):
    fake = types.ModuleType("aiodns")

    class _Resolver:
        def __init__(self, *a, **k):
            pass

        async def gethostbyname(self, *a, **k):
            if fail:
                raise RuntimeError("(11, 'Could not contact DNS servers')")
            return types.SimpleNamespace(addresses=["1.2.3.4"])

    fake.DNSResolver = _Resolver
    monkeypatch.setitem(sys.modules, "aiodns", fake)


def _fake_system_dns(monkeypatch, ok: bool):
    def _getaddrinfo(*a, **k):
        if not ok:
            raise socket.gaierror(11001, "getaddrinfo failed")
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("1.2.3.4", 443))]
    monkeypatch.setattr(socket, "getaddrinfo", _getaddrinfo)


def test_check_aiodns_layer_detects_blocked(doctor, monkeypatch):
    """
    Сигнатура пользователя: системный getaddrinfo работает, aiodns не может
    достучаться до DNS-сервера → вердикт aiodns_dns_blocked, проблем нет
    (run.py чинит автоматически, пугать пользователя нечем).
    """
    _fake_aiodns(monkeypatch, fail=True)
    _fake_system_dns(monkeypatch, ok=True)
    problems, verdict = asyncio.run(doctor.check_aiodns_layer(2.0))
    assert verdict == "aiodns_dns_blocked"
    assert problems == []


def test_check_aiodns_layer_both_ok(doctor, monkeypatch):
    _fake_aiodns(monkeypatch, fail=False)
    _fake_system_dns(monkeypatch, ok=True)
    problems, verdict = asyncio.run(doctor.check_aiodns_layer(2.0))
    assert verdict == ""
    assert problems == []


def test_check_aiodns_layer_system_dead_too(doctor, monkeypatch):
    """Системный DNS тоже мёртв → это уже dns_dead из шага 3, не aiodns-кейс."""
    _fake_aiodns(monkeypatch, fail=True)
    _fake_system_dns(monkeypatch, ok=False)
    problems, verdict = asyncio.run(doctor.check_aiodns_layer(2.0))
    assert verdict == ""


# --------------------------------------------------------------------------
# doctor: шаг 4b — контрольный зонд через системный резолвер
# --------------------------------------------------------------------------
class _FakeContent:
    async def read(self, n):
        return b"{}"


class _FakeResp:
    status = 200

    def __init__(self):
        self.content = _FakeContent()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class ClientConnectorDNSError(Exception):
    """Двойник aiohttp.ClientConnectorDNSError: важно имя типа и текст."""


class _FakeSession:
    """
    Двойник aiohttp.ClientSession: с дефолтным коннектором (aiodns) падает
    с DNS-ошибкой, с явным ThreadedResolver — работает. Ровно картина на
    машине пользователя.
    """

    def __init__(self, *a, **kw):
        self._kw = kw

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        c = self._kw.get("connector")
        if c is not None:
            await c.close()   # иначе pytest тонет в «Unclosed connector»
        return False

    def get(self, url, **kw):
        if self._kw.get("connector") is None:
            raise ClientConnectorDNSError(
                "Cannot connect to host fapi.binance.com:443 ssl:default "
                "[Could not contact DNS servers]")
        return _FakeResp()


def test_aiohttp_layer_dns_retry_verdict(doctor, monkeypatch):
    """
    Все прямые варианты 4b падают с DNS-ошибкой, контрольный зонд через
    ThreadedResolver проходит → вердикт aiodns_dns_blocked, а DNS-провалы
    НЕ попадают в problems: run.py это чинит автоматически.
    """
    monkeypatch.delenv("PROXY_URL", raising=False)
    monkeypatch.delenv("HTTPS_PROXY", raising=False)
    monkeypatch.delenv("https_proxy", raising=False)
    monkeypatch.setattr(aiohttp, "ClientSession", _FakeSession)
    problems, verdict = asyncio.run(doctor.check_aiohttp_layer(2.0))
    assert verdict == "aiodns_dns_blocked"
    assert problems == []


def test_aiohttp_layer_real_block_still_reported(doctor, monkeypatch):
    """
    Если и системный резолвер не помогает (реальная блокировка), вердикт
    прежний — aiohttp_blocked, проблемы на месте.
    """
    class _AllFail(_FakeSession):
        def get(self, url, **kw):
            raise ConnectionResetError("connection reset by peer")

    monkeypatch.delenv("PROXY_URL", raising=False)
    monkeypatch.delenv("HTTPS_PROXY", raising=False)
    monkeypatch.delenv("https_proxy", raising=False)
    monkeypatch.setattr(aiohttp, "ClientSession", _AllFail)
    problems, verdict = asyncio.run(doctor.check_aiohttp_layer(2.0))
    assert verdict == "aiohttp_blocked"
    assert problems
