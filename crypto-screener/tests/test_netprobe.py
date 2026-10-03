"""
Тесты сетевого зонда и матрицы вердиктов диагностики.

Главный сценарий — реальный случай пользователя: системный DNS не находит
`www.gstatic.com` (getaddrinfo failed), хотя `one.one.one.one` резолвится.
По одному контрольному хосту это выглядело как «интернета нет вовсе», что
неверно и уводит не туда. Правильный диагноз даёт только сравнение трёх
независимых слоёв: TCP по IP, системный DNS, прямой DNS-запрос.
"""
import socket

import pytest

from app import netprobe


# --------------------------------------------------------------------------
# DNS-клиент
# --------------------------------------------------------------------------
def test_encode_name():
    assert netprobe._encode_name("api.bybit.com") == (
        b"\x03api\x05bybit\x03com\x00")
    assert netprobe._encode_name("a.io") == b"\x01a\x02io\x00"


def test_encode_name_rejects_bad_label():
    with pytest.raises(ValueError):
        netprobe._encode_name("a" * 64 + ".com")


def test_query_resolves_real_domain():
    ips, err = netprobe.query("one.one.one.one", server="1.1.1.1", timeout=6)
    if err and "таймаут" in err:
        pytest.skip("UDP 53 заблокирован в этой среде")
    assert err is None, err
    assert ips, "не получили адрес"
    assert all(p.count(".") == 3 for p in ips)


def test_query_nxdomain():
    ips, err = netprobe.query("this-domain-must-not-exist-xyz123.invalid",
                              server="1.1.1.1", timeout=6)
    if ips:
        pytest.skip("резолвер отдаёт адреса для .invalid — вероятно, перехват DNS")
    assert err and "NXDOMAIN" in err


def test_query_aaaa_message_mentions_type():
    """Сообщение должно говорить про AAAA, а не про A, иначе вводит в заблуждение."""
    ips, err = netprobe.query("one.one.one.one", server="1.1.1.1", timeout=6, qtype=28)
    if ips:
        assert all(":" in ip for ip in ips)
    else:
        assert err is None or "AAAA" in err or "таймаут" in err


def test_query_handles_unreachable_server():
    ips, err = netprobe.query("one.one.one.one", server="10.255.255.1", timeout=2)
    assert ips == []
    assert err and ("таймаут" in err or "Error" in err or "Unreachable" in err
                    or "Network" in err)


def test_decode_name_handles_pointer_and_plain():
    # ответ с обычным именем и со сжатой меткой (указатель 0xC00C → offset 12)
    data = b"\x00" * 12 + b"\x03api\x05bybit\x03com\x00" + b"\xc0\x0c"
    name, off = netprobe._decode_name(data, 12)
    assert name == "api.bybit.com"
    assert off > 12
    name2, _ = netprobe._decode_name(data, len(data) - 2)
    assert name2 == "api.bybit.com"


def test_decode_name_pointer_loop_is_bounded():
    """Циклический указатель не должен вешать диагностику."""
    data = b"\x00" * 12 + b"\xc0\x0c"      # указатель сам на себя
    name, _ = netprobe._decode_name(data, 12)
    assert name == ""


# --------------------------------------------------------------------------
# TCP-зонд
# --------------------------------------------------------------------------
def test_tcp_probe_reachable():
    ok, info = netprobe.tcp_probe("1.1.1.1", 443, timeout=5)
    if not ok and "таймаут" in info:
        pytest.skip("исходящий трафик в этой среде ограничен")
    assert ok, info


def test_tcp_probe_blackhole_times_out():
    """Адрес из TEST-NET-1: пакеты гарантированно теряются."""
    ok, info = netprobe.tcp_probe("192.0.2.1", 443, timeout=2)
    assert not ok
    assert "таймаут" in info or "Error" in info


def test_tcp_probe_refused():
    # локальный порт, где точно никто не слушает
    ok, info = netprobe.tcp_probe("127.0.0.1", 1, timeout=3)
    assert not ok


# --------------------------------------------------------------------------
# Матрица вердиктов
# --------------------------------------------------------------------------
NEUTRAL_OK = {"one.one.one.one": "1.1.1.1", "www.google.com": "1.1.1.2",
              "github.com": "1.1.1.3", "www.microsoft.com": "1.1.1.4"}


def _patch_resolver(monkeypatch, neutral: dict, exchange_ok: bool):
    """Подменяет системный резолвер; прямой DNS-запрос остаётся настоящим."""
    real = socket.gethostbyname

    def fake(host):
        if host in neutral:
            return neutral[host]
        if exchange_ok:
            return real(host)
        raise socket.gaierror(11001, "getaddrinfo failed")

    monkeypatch.setattr(socket, "gethostbyname", fake)


@pytest.fixture
def doctor():
    import importlib
    import doctor as d
    importlib.reload(d)
    return d


def test_verdict_exchange_dns_blocked(doctor, monkeypatch):
    """
    Точный сценарий пользователя: нейтральные домены резолвятся,
    биржевые — нет. Вердикт обязан быть про выборочную блокировку DNS,
    а НЕ про «интернета нет».
    """
    _patch_resolver(monkeypatch, NEUTRAL_OK, exchange_ok=False)
    monkeypatch.setattr(doctor, "tcp_probe", lambda *a, **k: (True, "ok"), raising=False)
    from app import netprobe as np
    monkeypatch.setattr(np, "tcp_probe", lambda *a, **k: (True, "ok"))

    _problems, verdict = doctor.check_network(3.0, ["bybit", "okx"])
    assert verdict == "exchange_dns_blocked"


def test_verdict_net_ok_when_everything_resolves(doctor, monkeypatch):
    from app import netprobe as np
    monkeypatch.setattr(np, "tcp_probe", lambda *a, **k: (True, "ok"))
    _problems, verdict = doctor.check_network(3.0, ["bybit"])
    assert verdict in ("net_ok", "partial_dns", "partial_neutral")


def test_verdict_no_network(doctor, monkeypatch):
    from app import netprobe as np
    monkeypatch.setattr(np, "tcp_probe", lambda *a, **k: (False, "таймаут"))
    _problems, verdict = doctor.check_network(2.0, ["bybit"])
    assert verdict == "no_network"


def test_verdict_dns_dead(doctor, monkeypatch):
    """TCP работает, но системный DNS не резолвит ничего."""
    from app import netprobe as np
    monkeypatch.setattr(np, "tcp_probe", lambda *a, **k: (True, "ok"))
    monkeypatch.setattr(np, "query", lambda *a, **k: ([], "таймаут"))

    def fail(host):
        raise socket.gaierror(11001, "getaddrinfo failed")
    monkeypatch.setattr(socket, "gethostbyname", fail)
    _problems, verdict = doctor.check_network(2.0, ["bybit"])
    assert verdict == "dns_dead"


# --------------------------------------------------------------------------
def test_win_errno_hints():
    import doctor
    for code, word in (("11001", "WSAHOST_NOT_FOUND"), ("10060", "WSAETIMEDOUT"),
                       ("10061", "WSAECONNREFUSED")):
        assert word in doctor._win_errno(Exception(f"[Errno {code}] getaddrinfo failed"))
    assert doctor._win_errno(Exception("какая-то другая ошибка")) == ""


def test_every_verdict_has_advice():
    """Вердикт без рекомендаций бесполезен — пользователь не поймёт, что делать."""
    import doctor
    for verdict in ("no_network", "dns_dead", "exchange_dns_blocked", "partial_dns"):
        assert verdict in doctor.ADVICE, f"нет рекомендаций для {verdict}"
        assert len(doctor.ADVICE[verdict]) >= 2


def test_advice_mentions_concrete_fix():
    import doctor
    text = " ".join(doctor.ADVICE["exchange_dns_blocked"])
    assert "1.1.1.1" in text or "8.8.8.8" in text, "не указано, какой DNS ставить"
    assert "flushdns" in text or "hosts" in text or "VPN" in text


def test_no_network_skips_slow_exchange_sweep(doctor, monkeypatch):
    """
    При мёртвом канале обход бирж должен пропускаться: каждая биржа висела бы
    до таймаута, и пользователь ждал бы минуты ради известного ответа.
    """
    called = {"http": 0, "ccxt": 0}
    monkeypatch.setattr(doctor, "check_env", lambda: [])
    monkeypatch.setattr(doctor, "check_network", lambda t, n: (["нет сети"], "no_network"))
    monkeypatch.setattr(doctor, "check_tls", lambda t: called.__setitem__("http", called["http"] + 1) or [])
    monkeypatch.setattr(doctor, "check_http", lambda n, t: called.__setitem__("http", called["http"] + 1) or [])

    import asyncio
    rc = asyncio.run(doctor.main(["--quick", "bybit"]))
    assert rc == 1
    assert called["http"] == 0, "при отсутствии сети медленные проверки всё равно запустились"
