"""
Минимальный DNS-клиент: один A/AAAA-запрос напрямую на указанный сервер.

Зачем это нужно диагностике: системный резолвер (`socket.getaddrinfo`) проходит
через файл hosts, кэш службы DNS, а на Windows ещё и через сетевые экраны
антивирусов. Если `getaddrinfo` не находит домен, непонятно, кто именно виноват:
локальная машина или вышестоящий DNS.

Прямой запрос к 1.1.1.1/8.8.8.8 в обход системного резолвера разделяет эти два
случая:
  * getaddrinfo FAIL, прямой запрос OK  → вмешивается локальная машина
                                          (hosts, антивирус, кэш службы DNS)
  * getaddrinfo FAIL, прямой запрос FAIL → блокирует вышестоящий DNS
                                          (провайдер/фильтр/регион)

Реализация намеренно крошечная и без зависимостей: только UDP + struct.
"""
from __future__ import annotations

import random
import socket
import struct
from typing import Optional

PUBLIC_DNS = (
    ("1.1.1.1", "Cloudflare"),
    ("8.8.8.8", "Google"),
    ("9.9.9.9", "Quad9"),
)


def _encode_name(name: str) -> bytes:
    out = b""
    for label in name.strip(".").split("."):
        raw = label.encode("idna") if any(ord(c) > 127 for c in label) else label.encode("ascii")
        if not raw or len(raw) > 63:
            raise ValueError(f"некорректная метка в имени: {label!r}")
        out += bytes([len(raw)]) + raw
    return out + b"\x00"


def _decode_name(data: bytes, offset: int) -> tuple[str, int]:
    labels: list[str] = []
    jumped = False
    end = offset
    hops = 0
    while True:
        if offset >= len(data):
            break
        length = data[offset]
        if length == 0:
            offset += 1
            break
        if length & 0xC0 == 0xC0:                 # указатель на сжатую метку
            if hops > 8:
                break
            pointer = struct.unpack("!H", data[offset:offset + 2])[0] & 0x3FFF
            if not jumped:
                end = offset + 2
            offset = pointer
            jumped = True
            hops += 1
            continue
        labels.append(data[offset + 1:offset + 1 + length].decode("ascii", "replace"))
        offset += 1 + length
        if not jumped:
            end = offset
    return ".".join(labels), (end if jumped else offset)


def query(name: str, server: str = "1.1.1.1", timeout: float = 4.0,
          port: int = 53, qtype: int = 1) -> tuple[list[str], Optional[str]]:
    """
    Возвращает (список адресов, ошибка).

    qtype: 1 = A (IPv4), 28 = AAAA (IPv6).
    """
    tid = random.randint(0, 0xFFFF)
    header = struct.pack("!HHHHHH", tid, 0x0100, 1, 0, 0, 0)  # recursion desired
    packet = header + _encode_name(name) + struct.pack("!HH", qtype, 1)  # type, class IN

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(timeout)
            sock.sendto(packet, (server, port))
            data, _ = sock.recvfrom(4096)
    except socket.timeout:
        return [], f"таймаут {timeout:.0f} c (UDP {port} заблокирован?)"
    except OSError as e:
        return [], f"{type(e).__name__}: {e}"

    if len(data) < 12:
        return [], "обрывочный ответ"
    rid, flags, qd, an, _ns, _ar = struct.unpack("!HHHHHH", data[:12])
    if rid != tid:
        return [], "несовпал идентификатор запроса"
    rcode = flags & 0x000F
    if rcode == 3:
        return [], "NXDOMAIN (домен не существует по мнению сервера)"
    if rcode == 5:
        return [], "REFUSED (сервер отказал)"
    if rcode != 0:
        return [], f"rcode={rcode}"

    offset = 12
    for _ in range(qd):                            # пропускаем секцию вопроса
        _, offset = _decode_name(data, offset)
        offset += 4

    answers: list[str] = []
    for _ in range(an):
        _, offset = _decode_name(data, offset)
        if offset + 10 > len(data):
            break
        atype, _aclass, _ttl, rdlen = struct.unpack("!HHIH", data[offset:offset + 10])
        offset += 10
        rdata = data[offset:offset + rdlen]
        offset += rdlen
        if atype == 1 and rdlen == 4:
            answers.append(".".join(str(b) for b in rdata))
        elif atype == 28 and rdlen == 16:
            answers.append(socket.inet_ntop(socket.AF_INET6, rdata))
        elif atype == 5:                            # CNAME — просто идём дальше
            continue
    if not answers:
        kind = "A" if qtype == 1 else ("AAAA" if qtype == 28 else str(qtype))
        return [], f"ответ получен, но {kind}-записей нет"
    return answers, None


def tcp_probe(ip: str, port: int = 443, timeout: float = 5.0) -> tuple[bool, str]:
    """
    Прямое TCP-соединение по IP-литералу, полностью минуя DNS.

    Отделяет «нет исходящего трафика» от «не работает DNS»: если сюда
    соединяемся, а домены не резолвятся — проблема именно в DNS.
    """
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True, f"TCP {ip}:{port} установлен"
    except socket.timeout:
        return False, f"TCP {ip}:{port} — таймаут (пакеты теряются/фильтруются)"
    except OSError as e:
        return False, f"TCP {ip}:{port} — {type(e).__name__}: {e}"
