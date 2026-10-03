#!/usr/bin/env python3
"""
Диагностика подключения к биржам.

    python doctor.py                  # все биржи из конфига
    python doctor.py bybit okx        # только эти
    python doctor.py --quick          # только сеть, без ccxt (быстро)
    python doctor.py --timeout 5      # свои таймауты, сек

Печатает, на каком именно слое обрыв: DNS / TLS / прокси / геоблокировка /
таймаут / сама ccxt. Заодно проверяет зависимости и версии.

ВАЖНО про скорость: все проверки идут с короткими таймаутами и печатаются
по мере выполнения (flush), потому что при недоступной сети «тихий» режим
выглядит как зависание — пользователь ждёт несколько минут и не понимает,
идёт ли вообще что-то.
"""
from __future__ import annotations

import asyncio
import os
import platform
import socket
import ssl
import sys
import time
import urllib.error
import urllib.request
from urllib.parse import urlparse

# Пинг до публичного REST-эндпоинта каждой биржи (без ключей и без ccxt):
# если здесь не работает, то дело в сети, а не в коде скринера.
PROBES = {
    "binance":     "https://api.binance.com/api/v3/ping",
    "binanceusdm": "https://fapi.binance.com/fapi/v1/ping",
    "bybit":       "https://api.bybit.com/v5/market/time",
    "okx":         "https://www.okx.com/api/v5/public/time",
    "mexc":        "https://api.mexc.com/api/v3/contract/ticker?symbol=BTC_USDT",
    "gate":        "https://api.gateio.ws/api/v4/spot/time",
    "aster":       "https://fapi.asterdex.com/fapi/v1/ping",
    "hyperliquid": "https://api.hyperliquid.xyz/info",
    "bitget":      "https://api.bitget.com/api/v2/public/time",
    "kucoinfutures": "https://api-futures.kucoin.com/api/v1/timestamp",
    "htx":         "https://api.huobi.pro/v1/common/timestamp",
    "bingx":       "https://open-api.bingx.com/openApi/ping",
    "kraken":      "https://api.kraken.com/0/public/Time",
}

NEEDED_CLASSES = ("binance", "binanceusdm", "bybit", "okx",
                  "mexc", "gate", "aster", "hyperliquid")

OK = "[ OK ]"
BAD = "[FAIL]"
WARN = "[WARN]"
TIMEOUT = 6.0          # сек на один HTTP-запрос
CCXT_TIMEOUT = 12000   # мс на load_markets одной биржи


def say(text: str = "") -> None:
    """Печать с немедленным сбросом — иначе при медленной сети вывод «залипает»."""
    print(text, flush=True)


def head(text: str) -> None:
    say(f"\n{text}")
    say("-" * len(text))


def parse_args(argv: list[str]) -> tuple[list[str], bool, float]:
    names, quick, timeout = [], False, TIMEOUT
    i = 0
    while i < len(argv):
        a = argv[i]
        if a in ("--quick", "-q"):
            quick = True
        elif a in ("--timeout", "-t") and i + 1 < len(argv):
            try:
                timeout = max(1.0, float(argv[i + 1]))
            except ValueError:
                pass
            i += 1
        elif not a.startswith("-"):
            names.append(a)
        i += 1
    return names, quick, timeout


# --------------------------------------------------------------------------
def check_env() -> list[str]:
    """
    Раздел 1. Использует общий app.preflight, а не собственный список пакетов:
    иначе требования в двух местах разъезжаются, и диагностика рапортует «всё
    хорошо» при ccxt, в котором нет нужных классов бирж.
    """
    problems: list[str] = []
    head("1. Окружение")
    say(f"  Python   : {sys.version.split()[0]}  ({platform.system()} {platform.release()})")

    try:
        from app.preflight import REQUIRED, OPTIONAL, check as preflight_check
    except Exception as e:
        say(f"  {BAD} не удалось импортировать app.preflight: {e}")
        return ["app.preflight не импортируется"]

    rep = preflight_check(verify_exchanges=True)
    for mod, ver in sorted(rep.versions.items()):
        say(f"  {OK} {mod:20s} {ver}")

    # явно показываем и те пакеты, которые не импортировались
    for mod, _min, _why, _crit in REQUIRED:
        if mod not in rep.versions:
            say(f"  {BAD} {mod:20s} НЕ УСТАНОВЛЕН")
    for mod, _min, _why in OPTIONAL:
        if mod not in rep.versions:
            say(f"  {WARN} {mod:20s} не установлен (не критично)")

    # минимальные версии
    for mod, minver, _why, _crit in REQUIRED:
        ver = rep.versions.get(mod)
        if ver and minver:
            good = _ver_ge(ver, minver)
            say(f"  {OK if good else BAD} {mod} >= {minver:12s} фактически {ver}")
            if not good:
                problems.append(f"{mod} {ver} < {minver}")

    # классы бирж
    try:
        import ccxt.pro as pro
        absent = [c for c in NEEDED_CLASSES if not hasattr(pro, c)]
        if absent:
            say(f"  {BAD} в ccxt нет классов бирж: {', '.join(absent)}")
            problems.append(f"ccxt слишком старый, нет: {', '.join(absent)}")
        else:
            say(f"  {OK} классы бирж на месте: {len(NEEDED_CLASSES)} шт.")
    except Exception as e:
        say(f"  {BAD} ccxt.pro не импортируется: {e}")
        problems.append("ccxt.pro не импортируется")

    # Транспорт ccxt.async_support. В REQUIRED его нет (он тянется за ccxt),
    # но несовместимая пара aiohttp/yarl роняет ВСЕ биржи на HTTP-слое, а без
    # вывода версий это не диагностируется — поэтому показываем явно.
    for mod in ("aiohttp", "yarl", "multidict"):
        try:
            m = __import__(mod)
            say(f"  {OK} {mod:20s} {getattr(m, '__version__', '?')}")
        except Exception:
            say(f"  {BAD} {mod:20s} НЕ УСТАНОВЛЕН — ccxt (async) не работает")
            problems.append(f"{mod} не установлен → pip install -r requirements.txt")

    # DNS-резолвер aiohttp. При установленном aiodns (ccxt.pro ставит его
    # всегда) aiohttp по умолчанию резолвит домены AsyncResolver'ом: UDP-запрос
    # НАПРЯМУЮ к DNS-серверу адаптера, мимо службы DNS-клиента Windows. Этот
    # путь блокируют антивирус/файрвол/VPN/«шифрованный DNS» — и тогда все
    # биржи падают с «Could not contact DNS servers», хотя шаги 1–4 зелёные.
    # Шаг 4c сравнивает оба резолвера; run.py принудительно включает системный
    # ThreadedResolver (отключить: DNS_RESOLVER=aiodns).
    try:
        import aiodns  # noqa: F401
        say(f"  {WARN} {'aiodns':20s} установлен — aiohttp по умолчанию использует")
        say("       асинхронный DNS (прямые UDP-запросы к серверу, мимо службы DNS")
        say("       Windows); run.py переключает его на системный резолвер (шаг 4c)")
    except Exception:
        say(f"  {OK} {'aiodns':20s} не установлен — aiohttp использует системный резолвер")
    _dns_env = os.getenv("DNS_RESOLVER", "").strip()
    say(f"  {OK} {'DNS_RESOLVER':20s} {_dns_env or 'не задан (по умолчанию threaded — системный)'}")

    for i in rep.errors:
        say(f"  {BAD} {i.text}")
        if i.fix:
            say(f"       → {i.fix}")
        problems.append(i.text)
    for i in rep.warnings:
        say(f"  {WARN} {i.text}")
        if i.fix:
            say(f"       → {i.fix}")

    proxy = {k: v for k, v in os.environ.items()
             if k.lower() in ("http_proxy", "https_proxy", "all_proxy", "no_proxy")}
    say(f"  {WARN} заданы прокси-переменные: {proxy}" if proxy
        else f"  {OK} прокси-переменные не заданы")

    # Системный прокси Windows (реестр). Критично показать именно здесь:
    # urllib (шаг 4) его ИСПОЛЬЗУЕТ, а aiohttp/ccxt (шаг 5) — НЕТ. Когда он
    # включён VPN-приложением, «прямые» запросы проходят, а ccxt падает
    # мгновенно на всех биржах сразу — и без этой строки причина не видна.
    sp_on = sp_url = sp_pac = ""
    try:
        from app.preflight import windows_system_proxy
        sp_on, sp_url, sp_pac = windows_system_proxy()
    except Exception:
        pass
    proxy_url_env = os.getenv("PROXY_URL", "").strip()
    if sp_on and sp_url:
        say(f"  {WARN} системный прокси Windows: ВКЛЮЧЁН → {sp_url}")
        say("       (браузер и urllib ходят через него, ccxt/aiohttp — напрямую)")
        if not proxy and not proxy_url_env:
            say("       PROXY_URL не задан — если шаг 5 упадёт на всех биржах, это оно:")
            say(f'         PowerShell:  $env:PROXY_URL="{sp_url}"; python doctor.py')
    elif sp_pac:
        say(f"  {WARN} автонастройка прокси Windows (PAC): {sp_pac}")
        if not proxy and not proxy_url_env:
            say("       ccxt PAC не понимает — при проблемах задайте PROXY_URL вручную")
    elif sys.platform == "win32":
        say(f"  {OK} системный прокси Windows: не задан")
    if proxy_url_env:
        say(f"  {OK} PROXY_URL: {proxy_url_env}")
    return problems


def _ver_ge(v: str, want: str) -> bool:
    def t(x: str) -> tuple:
        out = []
        for part in x.split(".")[:3]:
            d = "".join(ch for ch in part if ch.isdigit())
            out.append(int(d) if d else 0)
        return tuple(out)
    return t(v) >= t(want)


def check_network(timeout: float, names: list[str]) -> tuple[list[str], str]:
    """
    Слой «сеть против DNS». Возвращает (список проблем, вердикт).

    Почему одного контрольного хоста недостаточно: на машине пользователя
    `one.one.one.one` резолвился, а `www.gstatic.com` — нет (getaddrinfo failed).
    По одному хосту это выглядело как «интернета нет вовсе», хотя DNS работал
    выборочно. Вывод был неверным и уводил не туда.

    Поэтому проверяем три независимых вещи:
      a) TCP по IP-литералу, минуя DNS  → есть ли исходящий трафик вообще;
      b) DNS через системный резолвер  → работает ли он и для всех ли имён;
      c) DNS прямым запросом к 1.1.1.1/8.8.8.8 в обход системного резолвера
         → кто виноват: локальная машина (hosts/антивирус) или вышестоящий DNS.
    """
    from app.netprobe import PUBLIC_DNS, query, tcp_probe

    head("2. Сетевой слой: трафик отдельно от DNS")
    problems: list[str] = []

    # --- a) исходящий трафик, DNS не участвует ---
    say("  a) TCP по IP напрямую (в обход DNS)")
    tcp_ok = 0
    for ip, label in (("1.1.1.1", "Cloudflare"), ("8.8.8.8", "Google"), ("104.16.0.1", "Cloudflare CDN")):
        good, info = tcp_probe(ip, 443, timeout=min(timeout, 5.0))
        say(f"     {OK if good else BAD} {label:16s} {info}")
        tcp_ok += good
    say(f"     → исходящий трафик на 443: {'работает' if tcp_ok else 'НЕ работает'} ({tcp_ok}/3)")

    # --- b) системный резолвер на нейтральных доменах ---
    say("")
    say("  b) DNS через системный резолвер (нейтральные домены)")
    neutral = ("one.one.one.one", "www.google.com", "github.com", "www.microsoft.com")
    sys_ok: dict[str, bool] = {}
    for h in neutral:
        t = time.time()
        try:
            ip = socket.gethostbyname(h)
            sys_ok[h] = True
            say(f"     {OK} {h:24s} → {ip}  ({(time.time()-t)*1000:.0f} мс)")
        except Exception as e:
            sys_ok[h] = False
            say(f"     {BAD} {h:24s} → {type(e).__name__} {_win_errno(e)}")
    n_sys = sum(sys_ok.values())
    say(f"     → системный DNS: {n_sys}/{len(neutral)}")

    # --- c) хосты бирж тем же резолвером ---
    say("")
    say("  c) DNS хостов бирж")
    ex_hosts = sorted({urlparse(PROBES[n]).netloc for n in names if n in PROBES})
    ex_sys: dict[str, bool] = {}
    for h in ex_hosts:
        try:
            ip = socket.gethostbyname(h)
            ex_sys[h] = True
            say(f"     {OK} {h:28s} → {ip}")
        except Exception as e:
            ex_sys[h] = False
            say(f"     {BAD} {h:28s} → {type(e).__name__} {_win_errno(e)}")
    n_ex = sum(ex_sys.values())
    say(f"     → хосты бирж: {n_ex}/{len(ex_hosts)}")

    # --- d) прямой DNS-запрос в обход системного резолвера ---
    failed_hosts = [h for h, v in list(sys_ok.items()) + list(ex_sys.items()) if not v]
    verdict = ""
    if failed_hosts:
        say("")
        say("  d) Прямой DNS-запрос к публичным серверам (в обход системного резолвера)")
        local_block = 0
        for h in failed_hosts[:4]:
            for server, label in PUBLIC_DNS[:2]:
                ips, err = query(h, server=server, timeout=min(timeout, 4.0))
                if ips:
                    say(f"     {OK} {h:26s} через {label} ({server}) → {ips[0]}")
                    local_block += 1
                else:
                    say(f"     {BAD} {h:26s} через {label} ({server}) → {err}")
        if local_block and local_block >= len(failed_hosts):
            say("     → публичный DNS находит то, что не находит системный:")
            say("       вмешивается ЛОКАЛЬНАЯ машина (файл hosts, кэш службы DNS,")
            say("       веб-экран антивируса) или провайдерский резолвер.")

    # --- вердикт ---
    say("")
    say("  Вердикт по сетевому слою")
    if not tcp_ok:
        verdict = "no_network"
        say(f"     {BAD} Исходящий трафик на 443 не проходит вовсе.")
        say("        Дело не в биржах и не в коде: проверьте кабель/Wi-Fi/VPN,")
        say("        а также файрвол и антивирус (они чаще всего и блокируют).")
        problems.append("нет исходящего трафика на порт 443")
    elif n_sys == 0:
        verdict = "dns_dead"
        say(f"     {BAD} TCP работает, но системный DNS не резолвит ничего.")
        say("        Смените DNS на 1.1.1.1 или 8.8.8.8:")
        say("        Панель управления → Сеть и Интернет → Свойства адаптера →")
        say("        IPv4 → использовать следующие адреса DNS-серверов.")
        problems.append("системный DNS не работает при живом трафике")
    elif n_ex == 0 and n_sys > 0:
        verdict = "exchange_dns_blocked"
        say(f"     {BAD} Нейтральные домены резолвятся, а хосты бирж — нет.")
        say("        Это выборочная блокировка DNS именно биржевых доменов")
        say("        (провайдерский фильтр или веб-экран антивируса).")
        say("        Лечится VPN либо сменой DNS на 1.1.1.1 / 8.8.8.8.")
        problems.append("DNS блокирует домены бирж выборочно")
    elif n_ex < len(ex_hosts):
        verdict = "partial_dns"
        blocked = [h for h, v in ex_sys.items() if not v]
        say(f"     {WARN} Часть хостов бирж не резолвится: {', '.join(blocked)}")
        say("        Остальные должны работать — запускайте скринер,")
        say("        недоступные биржи просто не подключатся.")
        problems.append(f"не резолвятся: {', '.join(blocked)}")
    elif n_sys < len(neutral):
        verdict = "partial_neutral"
        say(f"     {WARN} Хосты бирж резолвятся, а часть нейтральных — нет.")
        say("        Значит, блокировка не про биржи: скринер должен работать.")
    else:
        verdict = "net_ok"
        say(f"     {OK} Трафик есть, DNS резолвит и нейтральные домены, и биржи.")
        say("        Если биржи всё равно не подключаются, причина выше уровня DNS:")
        say("        геоблокировка (403/451), файрвол на 443 или TLS.")
    return problems, verdict


def _win_errno(e: BaseException) -> str:
    """Пояснение к типовым кодам ошибок Windows, чтобы вывод был читаемым."""
    text = str(e)
    hints = {
        "11001": "(WSAHOST_NOT_FOUND — имя не найдено; обычно hosts/фильтр/кэш DNS)",
        "11002": "(WSATRY_AGAIN — DNS-сервер не ответил)",
        "11003": "(WSANO_RECOVERY — неисправимая ошибка DNS)",
        "11004": "(WSANO_DATA — имя есть, но A-записи нет)",
        "10060": "(WSAETIMEDOUT — таймаут, пакеты теряются)",
        "10061": "(WSAECONNREFUSED — соединение отклонено)",
        "10065": "(WSAEHOSTUNREACH — хост недоступен)",
    }
    for code, hint in hints.items():
        if code in text:
            return hint
    return ""


def check_tls(timeout: float) -> list[str]:
    head("3. TLS / сертификаты")
    problems = []
    try:
        import certifi
        ctx = ssl.create_default_context(cafile=certifi.where())
        say(f"  {OK} certifi: {certifi.where()}")
    except Exception as e:
        ctx = ssl.create_default_context()
        say(f"  {WARN} certifi недоступен ({e}) — используется системное хранилище")
        problems.append("certifi не установлен")

    host = "api.binance.com"
    try:
        with socket.create_connection((host, 443), timeout=timeout) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                say(f"  {OK} TLS к {host}: {ssock.version()}, cipher={ssock.cipher()[0]}")
    except ssl.SSLCertVerificationError as e:
        say(f"  {BAD} проверка сертификата не пройдена: {e}")
        problems.append("TLS: сертификат не проверяется → pip install -U certifi")
    except Exception as e:
        say(f"  {BAD} TLS-соединение: {type(e).__name__}: {str(e)[:110]}")
        problems.append(f"TLS к {host} не устанавливается")
    return problems


def _probe(url: str, timeout: float) -> tuple[bool, str, float]:
    req = urllib.request.Request(url, headers={
        "User-Agent": "crypto-screener-doctor/1.0",
        "Accept": "application/json",
    })
    t = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310
            r.read(200)
            return True, f"HTTP {r.status}", time.time() - t
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode(errors="ignore")[:110]
        except Exception:
            pass
        # Важна СВЯЗНОСТЬ, а не корректность вызова: сервер ответил — значит
        # DNS, TLS и маршрутизация работают. Hyperliquid /info принимает только
        # POST и на GET отвечает 405 — это не поломка сети.
        if e.code in (403, 451, 429):
            return False, f"HTTP {e.code} {body}", time.time() - t
        return True, f"HTTP {e.code} (сервер отвечает) {body}".strip(), time.time() - t
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:130]}", time.time() - t


def check_http(names: list[str], timeout: float) -> list[str]:
    head("4. Прямые HTTPS-запросы к биржам (без ccxt)")
    say("  Каждый запрос печатается сразу; таймаут "
        f"{timeout:.0f} c. Если все FAIL при живом DNS/TLS —")
    say("  исходящий трафик блокируют файрвол/антивирус/провайдер.")
    say("")
    problems = []
    for n in names:
        url = PROBES.get(n)
        if not url:
            continue
        say(f"  ... {n}")
        good, info, dt = _probe(url, timeout)
        extra = ""
        if not good:
            low = info.lower()
            if "451" in info or "403" in info or "restricted" in low or "unavailable" in low:
                extra = "  ← похоже на геоблокировку (нужен VPN)"
            elif "timed out" in low:
                extra = "  ← таймаут: файрвол/антивирус блокирует исходящие"
            elif "refused" in low:
                extra = "  ← соединение отклонено"
            elif "certificate" in low or "ssl" in low:
                extra = "  ← TLS: pip install -U certifi"
            elif "getaddrinfo" in low or "name or service" in low:
                extra = "  ← DNS"
            problems.append(f"{n}: {info[:90]}{extra.strip()}")
        say(f"  {OK if good else BAD} {n:15s} {dt*1000:6.0f} мс  {info[:95]}{extra}")
    return problems


async def check_aiohttp_layer(timeout: float) -> tuple[list[str], str]:
    """
    Шаг 4b. Тот же запрос через aiohttp — транспорт, которым ходит ccxt.

    Зачем: шаг 4 идёт через urllib, а urllib на Windows уважает системный
    прокси из реестра; ccxt/aiohttp его игнорирует и подключается напрямую
    (trust_env=False). Комбинация «шаг 4 OK, шаг 5 FAIL мгновенно на всех
    биржах» без этой проверки неразличима: системный прокси, TLS-перехват
    антивируса и файрвол выглядят одинаково.

    Варианты одного запроса:
      1. certifi + напрямую      — в точности как ccxt;
      2. системное хранилище CA  — если 1 FAIL, а 2 OK: антивирус/VPN подменяет
                                    сертификаты (его CA в хранилище Windows есть,
                                    а в certifi нет) → TLS MITM;
      3. без проверки сертификатов — отделяет TLS от всего остального;
      4. через системный прокси / PROXY_URL — если OK, прямой выход закрыт,
                                    лечится PROXY_URL.
    """
    head("4b. Тот же запрос через aiohttp (транспорт ccxt)")
    say("  Шаг 4 ходил через urllib: на Windows он использует системный прокси,")
    say("  а ccxt/aiohttp — нет. Здесь тот же хост, но путём ccxt, напрямую.")
    say("")
    problems: list[str] = []
    verdict = ""
    try:
        import aiohttp
    except Exception as e:
        say(f"  {BAD} aiohttp не установлен ({e}) — шаг пропускается")
        return ["aiohttp не установлен"], verdict

    from app.collector import describe_error

    certifi_ctx = None
    try:
        import certifi
        certifi_ctx = ssl.create_default_context(cafile=certifi.where())
    except Exception:
        pass

    url = PROBES.get("binanceusdm", "https://fapi.binance.com/fapi/v1/ping")
    sp_on = sp_url = ""
    try:
        from app.preflight import windows_system_proxy
        sp_on, sp_url, _pac = windows_system_proxy()
    except Exception:
        pass
    env_proxy = (os.getenv("PROXY_URL", "").strip()
                 or os.getenv("HTTPS_PROXY", "").strip()
                 or os.getenv("https_proxy", "").strip())

    variants: list[tuple[str, dict, str | None]] = [
        ("напрямую + certifi (как у ccxt)", {"ssl": certifi_ctx} if certifi_ctx else {}, None),
        ("напрямую + системное хранилище CA", {}, None),
        ("напрямую, без проверки сертификатов", {"ssl": False}, None),
    ]
    if sp_on and sp_url:
        variants.append((f"через системный прокси {sp_url}",
                         {"ssl": certifi_ctx} if certifi_ctx else {}, sp_url))
    if env_proxy and env_proxy not in (sp_url,):
        variants.append((f"через PROXY_URL/env {env_proxy}",
                         {"ssl": certifi_ctx} if certifi_ctx else {}, env_proxy))

    results: dict[str, bool] = {}
    infos: dict[str, str] = {}
    for label, kw, proxy in variants:
        t = time.time()
        try:
            async with aiohttp.ClientSession(trust_env=False) as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=timeout),
                                 proxy=proxy, **kw) as r:
                    await r.content.read(200)
                    ok, info = True, f"HTTP {r.status}"
        except Exception as e:
            ok, info = False, describe_error(e)[:170]
        dt = (time.time() - t) * 1000
        results[label] = ok
        infos[label] = info
        say(f"  {OK if ok else BAD} {label:42s} {dt:6.0f} мс  {info}")

    # Контрольный зонд через системный резолвер. Отделяет «aiohttp заблокирован
    # целиком» от «сломан только асинхронный резолвер aiodns»: ccxt.pro ставит
    # aiodns, и aiohttp начинает резолвить домены прямыми UDP-запросами к
    # DNS-серверу мимо службы DNS Windows — этот путь часто блокируют
    # антивирус/файрвол/VPN/«шифрованный DNS», и все прямые варианты падают с
    # «Could not contact DNS servers» при живом urllib (шаг 4). run.py лечит
    # это автоматически (apply_dns_resolver → ThreadedResolver).
    resolver_ok = False
    _direct_label = "напрямую + certifi (как у ccxt)"
    if not results.get(_direct_label) and "dns" in infos.get(_direct_label, "").lower():
        import aiohttp.resolver
        label = "напрямую + системный резолвер"
        t = time.time()
        try:
            conn = aiohttp.TCPConnector(resolver=aiohttp.resolver.ThreadedResolver())
            async with aiohttp.ClientSession(connector=conn, trust_env=False) as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=timeout),
                                 **({"ssl": certifi_ctx} if certifi_ctx else {})) as r:
                    await r.content.read(200)
                    resolver_ok, rinfo = True, f"HTTP {r.status}"
        except Exception as e:
            resolver_ok, rinfo = False, describe_error(e)[:170]
        dt = (time.time() - t) * 1000
        say(f"  {OK if resolver_ok else BAD} {label:42s} {dt:6.0f} мс  {rinfo}")

    # DNS-провалы прямых вариантов, объяснённые сломанным aiodns и чинимые
    # автоматически, в список проблем не идут: приложение работает через
    # системный резолвер, а «проблема» пугала бы пользователя впустую.
    for label, ok in results.items():
        if ok:
            continue
        if resolver_ok and label.startswith("напрямую") and "dns" in infos.get(label, "").lower():
            continue
        problems.append(f"aiohttp [{label}]: {infos.get(label, '')[:110]}")

    # Диагностическое дерево: что именно сломано
    direct = results.get("напрямую + certifi (как у ccxt)")
    winstore = results.get("напрямую + системное хранилище CA")
    noverify = results.get("напрямую, без проверки сертификатов")
    viaproxy = next((v for k, v in results.items() if k.startswith("через")), None)
    say("")
    if resolver_ok:
        verdict = "aiodns_dns_blocked"
        say("  → Резолвер aiodns не может достучаться до DNS-сервера, а системный")
        say("    резолвер (ThreadedResolver) работает. run.py автоматически")
        say("    переключает aiohttp на него — на подключение бирж это не влияет.")
    elif direct:
        say("  → aiohttp напрямую работает: транспорт не виноват. Если шаг 5")
        say("    всё равно падает — причина в ccxt (версия/настройки), детали там.")
    elif viaproxy and (sp_on or env_proxy):
        verdict = "proxy_split"
        say("  → Напрямую не выходит, а через прокси — выходит. Ровно поэтому")
        say("    шаг 4 (urllib) был OK, а ccxt падает: ccxt идёт мимо прокси.")
        proxy_addr = sp_url or env_proxy
        say(f'    Исправление:  $env:PROXY_URL="{proxy_addr}"; python run.py')
    elif winstore or noverify:
        verdict = "tls_mitm"
        say("  → Сертификаты certifi не принимаются, а системное хранилище /")
        say("    режим без проверки проходят: TLS-трафик перехватывает антивирус")
        say("    или VPN (его CA нет в certifi).")
    else:
        verdict = "aiohttp_blocked"
        say("  → aiohttp не подключается даже без проверки сертификатов:")
        say("    исходящие соединения этого процесса обрывают (файрвол/DPI/антивирус).")
    return problems, verdict


async def check_aiodns_layer(timeout: float) -> tuple[list[str], str]:
    """
    Шаг 4c. Асинхронный DNS-резолвер (aiodns) против системного.

    Повторяет ровно ту поломку, из-за которой ccxt падает с
    ClientConnectorDNSError «Could not contact DNS servers»: aiohttp при
    установленном aiodns шлёт DNS-запросы прямым UDP к серверам из настроек
    адаптера, минуя службу DNS-клиента Windows. Антивирус/файрвол/VPN/
    «шифрованный DNS» (DoH) этот путь блокируют, а системный getaddrinfo
    продолжает работать — отсюда зелёные шаги 1–4 и красный шаг 5 на всех
    биржах сразу.

    Проблем в общий список не добавляет: run.py автоматически переключает
    aiohttp на системный резолвер (apply_dns_resolver), то есть поломка
    чинится сама; вердикт нужен, чтобы итоговый совет объяснил пользователю,
    что произошло и как при желании починить DNS на уровне системы.
    """
    head("4c. Асинхронный DNS-резолвер (aiodns) против системного")
    problems: list[str] = []
    verdict = ""
    try:
        import aiodns
    except Exception:
        say(f"  {OK} aiodns не установлен — aiohttp и так использует системный")
        say("     резолвер (ThreadedResolver), этот класс поломок невозможен.")
        return problems, verdict

    from app.collector import describe_error

    host = "fapi.binance.com"
    loop = asyncio.get_running_loop()

    t = time.time()
    try:
        await asyncio.wait_for(loop.getaddrinfo(host, 443), timeout)
        sys_ok, sys_info = True, f"{host} резолвится"
    except Exception as e:
        sys_ok, sys_info = False, describe_error(e)[:120]
    say(f"  {OK if sys_ok else BAD} системный резолвер (getaddrinfo){'':7s} "
        f"{(time.time()-t)*1000:6.0f} мс  {sys_info}")

    t = time.time()
    try:
        import warnings

        ares = aiodns.DNSResolver()
        # getaddrinfo в aiodns 3.x и 4.x имеет несовместимые сигнатуры, а
        # gethostbyname есть во всех версиях (в 4.x лишь deprecated) —
        # гасим предупреждение и пользуемся им.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            coro = ares.gethostbyname(host, socket.AF_INET)
        await asyncio.wait_for(coro, timeout)
        aio_ok, aio_info = True, f"{host} резолвится"
    except Exception as e:
        aio_ok, aio_info = False, describe_error(e)[:120]
    say(f"  {OK if aio_ok else BAD} асинхронный резолвер (aiodns){'':11s} "
        f"{(time.time()-t)*1000:6.0f} мс  {aio_info}")

    say("")
    if sys_ok and not aio_ok:
        verdict = "aiodns_dns_blocked"
        say("  → Системный резолвер работает, а aiodns не может отправить запрос")
        say("    к DNS-серверу. Именно это роняло ccxt на всех биржах с")
        say("    ClientConnectorDNSError «Could not contact DNS servers».")
        say("    run.py автоматически переключает aiohttp на системный резолвер —")
        say("    на подключение бирж это больше не влияет.")
        say("    Починить DNS на уровне системы (по желанию): DNS 1.1.1.1/8.8.8.8")
        say("    в свойствах адаптера, отключить «шифрованный DNS» (DoH),")
        say("    ipconfig /flushdns, исключить python.exe в антивирусе/файрволе.")
    elif sys_ok and aio_ok:
        say("  → Оба резолвера работают.")
    else:
        say("  → Системный резолвер тоже не работает — причина в шаге 3 (DNS).")
    return problems, verdict


async def check_ccxt(names: list[str]) -> list[str]:
    head("5. Загрузка рынков через ccxt")
    say(f"  Таймаут на биржу {CCXT_TIMEOUT/1000:.0f} c. Это самый медленный этап.")
    import ccxt
    import ccxt.pro as ccxtpro
    from app.config import load_settings
    from app import collector as collector_mod
    from app.collector import apply_dns_resolver, apply_proxy, describe_error

    # Тот же прокси, что использует run.py: PROXY_URL из настроек имеет
    # приоритет над HTTPS_PROXY и т.п. Без этой строки doctor проверял
    # прямой выход, даже когда приложение уже настроено ходить через прокси.
    settings = load_settings()
    collector_mod.SETTINGS_PROXY_OVERRIDE = settings.proxy_url
    proxy = apply_proxy({})
    if proxy:
        say(f"  Запросы пойдут через прокси: {proxy} (как в run.py)")
    # Тот же DNS-резолвер, что в run.py: системный ThreadedResolver вместо
    # aiodns (иначе шаг 5 воспроизводил бы поломку, которую приложение уже
    # обходит, и диагноз «всё починилось» было не отличить от «всё сломано»).
    dns_mode = apply_dns_resolver()
    say("  DNS-резолвер: " + ("системный (ThreadedResolver), как в run.py"
                             if dns_mode == "threaded" else
                             "дефолтный aiohttp/aiodns (DNS_RESOLVER=aiodns)"))
    say("")

    cfgs = {e.id: e for e in settings.exchanges}
    problems = []
    for n in names:
        if n not in PROBES:
            continue
        cfg = cfgs.get(n)
        mtype = cfg.market if cfg else "swap"
        say(f"  ... {n} [{mtype}]")
        try:
            opts = {"enableRateLimit": True,
                    "options": {"defaultType": mtype},
                    "timeout": CCXT_TIMEOUT}
            apply_proxy(opts)
            ex = getattr(ccxtpro, n)(opts)
        except AttributeError:
            say(f"  {BAD} {n:15s} нет в ccxt {ccxt.__version__}")
            problems.append(f"{n}: отсутствует в установленной версии ccxt")
            continue
        t = time.time()
        try:
            await ex.load_markets()
            swap = sum(1 for m in ex.markets.values() if m.get("swap"))
            spot = sum(1 for m in ex.markets.values() if m.get("spot"))
            say(f"  {OK} {n:15s} {(time.time()-t):5.1f} c  рынков={len(ex.markets):5d} "
                f"(spot={spot}, swap={swap})")
            if cfg and swap == 0 and spot == 0:
                problems.append(f"{n}: рынков загружено 0")
        except Exception as e:
            # describe_error разворачивает __cause__: ccxt в самом сообщении
            # оставляет только URL, а настоящую причину (SSL/сброс/прокси)
            # прячет в сцепленном исключении. Обрезка до 150 символов её
            # отрубала — пользователь видел «ExchangeNotAvailable: <url>».
            say(f"  {BAD} {n:15s} {(time.time()-t):5.1f} c  {describe_error(e)[:400]}")
            problems.append(f"{n}: {describe_error(e)[:220]}")
        finally:
            try:
                await ex.close()
            except Exception:
                pass
    # aiohttp закрывает SSL-соединения с небольшой задержкой; без паузы
    # процесс успевает завершиться раньше и печатает «Unclosed client session».
    await asyncio.sleep(0.3)
    return problems


ADVICE = {
    "no_network": [
        "Исходящего трафика нет вообще — биржи и код скринера ни при чём.",
        "Проверьте: кабель/Wi-Fi, включён ли VPN, не блокирует ли файрвол или",
        "антивирус исходящие соединения на порт 443.",
        "Быстрая проверка: откройте в браузере https://1.1.1.1 — если не",
        "открывается, проблема на уровне сети.",
    ],
    "dns_dead": [
        "Трафик есть, но системный DNS не резолвит домены.",
        "Смените DNS на 1.1.1.1 (Cloudflare) или 8.8.8.8 (Google):",
        "  Панель управления → Сеть и Интернет → Центр управления сетями →",
        "  Изменение параметров адаптера → свойства подключения →",
        "  IP версии 4 (TCP/IPv4) → Свойства → «Использовать следующие адреса».",
        "Затем сбросьте кэш:  ipconfig /flushdns",
    ],
    "exchange_dns_blocked": [
        "Нейтральные домены резолвятся, а домены бирж — нет. Это выборочная",
        "блокировка на уровне DNS: провайдерский фильтр или веб-экран антивируса",
        "(Kaspersky / ESET / Dr.Web часто перехватывают DNS и фильтруют категории).",
        "Что попробовать по порядку:",
        "  1. Сменить DNS на 1.1.1.1 или 8.8.8.8 (инструкция выше) + ipconfig /flushdns",
        "  2. Отключить веб-экран/«безопасный DNS» в антивирусе на время проверки",
        "  3. Проверить файл hosts: C:\\Windows\\System32\\drivers\\etc\\hosts",
        "  4. Включить VPN — он уводит DNS вместе с трафиком",
        "Если прямой запрос к 1.1.1.1 (раздел 2d) домен находит, а системный",
        "резолвер нет — виновата точно локальная машина, а не провайдер.",
    ],
    "partial_dns": [
        "Часть бирж недоступна по DNS, остальные должны работать.",
        "Можно запускать скринер — недоступные биржи просто не подключатся,",
        "а в логе будет понятная причина по каждой.",
        "Чтобы не ждать их таймауты, оставьте только работающие:",
        '  PowerShell:  $env:EXCHANGES="bybit,okx"; python run.py',
    ],
    "proxy_split": [
        "Прямые запросы к биржам (шаг 4, urllib) проходят, а ccxt/aiohttp",
        "падает мгновенно на всех биржах. На Windows это почти всегда означает:",
        "включён СИСТЕМНЫЙ прокси — его прописывают VPN-приложения (Clash,",
        "v2rayN, Outline, Amnezia и т.п.) в настройках Windows. Браузер и urllib",
        "ходят через него, а ccxt подключается напрямую — и прямой выход закрыт.",
        "Что делать:",
        "  1. Адрес прокси виден в шаге 1 (строка «системный прокси Windows»)",
        "     либо: Параметры → Сеть и Интернет → Прокси-сервер.",
        "  2. Сообщите его скринеру (покрывает и REST, и WebSocket):",
        '     PowerShell:  $env:PROXY_URL="http://127.0.0.1:PORT"; python doctor.py',
        '                  $env:PROXY_URL="http://127.0.0.1:PORT"; python run.py',
        "  3. Если VPN/прокси не нужен — выключите системный прокси и повторите.",
    ],
    "tls_mitm": [
        "TCP-соединение устанавливается, но проверка сертификата пачкой certifi",
        "не проходит, хотя системное хранилище Windows (или режим без проверки)",
        "проходит. Это перехват TLS: антивирус (Kaspersky/ESET/Dr.Web — «проверка",
        "защищённых соединений») или корпоративный файрвол подменяет сертификаты.",
        "Что делать:",
        "  1. Отключите в антивирусе проверку HTTPS-трафика или добавьте домены",
        "     бирж в исключения, затем повторите: python doctor.py",
        "  2. Альтернатива: экспортируйте корневой сертификат антивируса и допишите",
        "     его в файл certifi (путь напечатан в шаге 3).",
        "  3. На всякий случай: pip install -U certifi",
    ],
    "aiohttp_blocked": [
        "aiohttp не может подключиться к бирже даже без проверки сертификатов,",
        "при этом urllib (шаг 4) проходит — исходящие соединения обрывают",
        "файрвол, DPI или антивирус (возможно, по процессу python.exe).",
        "Что делать:",
        "  1. Посмотрите в шаге 1, через какой прокси ходил urllib (системный",
        "     прокси Windows / переменные окружения) — и задайте тот же адрес:",
        '     $env:PROXY_URL="http://127.0.0.1:PORT"; python run.py',
        "  2. Разрешите python.exe исходящие соединения в брандмауэре/антивирусе.",
        "  3. Если VPN работает в режиме TUN — убедитесь, что python.exe не в",
        "     списке исключений VPN (bypass-режим часто действует по процессам).",
    ],
    "aiodns_dns_blocked": [
        "Асинхронный резолвер aiohttp (aiodns) не может достучаться до DNS-",
        "сервера, хотя системный резолвер работает. Поэтому браузер и шаг 4",
        "были в порядке, а ccxt падал на всех биржах мгновенно с",
        "ClientConnectorDNSError «Could not contact DNS servers».",
        "run.py УЖЕ автоматически переключает aiohttp на системный резолвер",
        "(ThreadedResolver) — просто запустите его заново:  python run.py",
        "Починить DNS на уровне системы (по желанию):",
        "  1. Свойства адаптера → IP версии 4 (TCP/IPv4) → DNS 1.1.1.1 / 8.8.8.8;",
        "  2. Отключите «шифрованный DNS» (DoH) для адаптера — aiodns его не умеет;",
        "  3. Разрешите python.exe UDP/53 исходящим в антивирусе/файрволе,",
        "     затем:  ipconfig /flushdns",
        "Вернуть дефолтное поведение aiohttp: DNS_RESOLVER=aiodns в .env",
    ],
}


def report(problems: list[str], quick: bool, verdict: str = "") -> int:
    head("Итог")
    if not problems:
        say(f"  {OK} Все проверки пройдены. Запускайте: python run.py")
        return 0
    say(f"  Найдено проблем: {len(problems)}")
    for p in problems:
        say(f"   - {p}")

    advice = ADVICE.get(verdict)
    if advice:
        say("")
        say("  Diagnosis / что делать:")
        for line in advice:
            say("   " + line)

    say("")
    say("  Общие шаги:")
    say("   1. Региональная блокировка (в логе 403/451/restricted) — нужен VPN.")
    say("      Прокси задаётся так (он покрывает и REST, и WebSocket):")
    say('         PowerShell:  $env:PROXY_URL="http://127.0.0.1:1080"; python run.py')
    say('         cmd:         set PROXY_URL=http://127.0.0.1:1080 && python run.py')
    say("   2. Ошибки про сертификаты — pip install -U certifi; если не помогло —")
    say("      отключите проверку HTTPS-трафика в антивирусе (TLS-перехват).")
    say("   3. MEXC ругается на protobuf — pip install protobuf==5.29.5")
    say("   4. Интерфейс можно смотреть и без бирж:")
    say("         PowerShell:  $env:MODE='replay'; python run.py")
    say("         cmd:         set MODE=replay && python run.py")
    say("   5. «Шаг 4 OK, а ccxt FAIL на всех биржах» — системный прокси Windows:")
    say("         Get-ItemProperty 'HKCU:\\Software\\Microsoft\\Windows\\CurrentVersion\\Internet Settings' |")
    say("           Select-Object ProxyEnable, ProxyServer, AutoConfigURL")
    say('      Если ProxyEnable=1:  $env:PROXY_URL="http://127.0.0.1:PORT"; python run.py')
    say("   6. «Could not contact DNS servers» — aiodns не достучался до DNS-сервера.")
    say("      run.py автоматически использует системный резолвер; если DNS починили")
    say("      и хотите вернуть дефолт aiohttp: DNS_RESOLVER=aiodns")
    if quick:
        say("")
        say("  Был запущен --quick: слой ccxt не проверялся. Полный прогон:")
        say("         python doctor.py")
    return 1


async def main(argv: list[str]) -> int:
    from app.config import load_settings
    names, quick, timeout = parse_args(argv)
    if not names:
        names = [e.id for e in load_settings().exchanges]
    names = list(dict.fromkeys(names))

    say("=" * 74)
    say("  Crypto Screener — диагностика подключения")
    say("=" * 74)
    say(f"  Проверяем: {', '.join(names)}")
    say(f"  Таймаут HTTP: {timeout:.0f} c | режим: {'быстрый (без ccxt)' if quick else 'полный'}")

    problems: list[str] = []
    problems += check_env()
    net_problems, verdict = check_network(timeout, names)
    problems += net_problems

    # Полный обход бирж имеет смысл только если исходящий трафик в принципе есть.
    # При мёртвом канале каждая биржа будет висеть до таймаута — это минуты
    # ожидания ради заранее известного ответа.
    if verdict == "no_network":
        say("")
        say("  Обход бирж пропускаю: без исходящего трафика он ничего не даст.")
        return report(problems, quick, verdict)
    if verdict == "dns_dead" and not quick:
        say("")
        ans = input("  DNS не работает. Всё равно продолжить обход бирж? (y/N): ").strip().lower()
        if ans not in ("y", "yes", "д", "да"):
            return report(problems, quick, verdict)

    problems += check_tls(timeout)
    http_problems = check_http(names, timeout)
    problems += http_problems

    # 4b: транспорт ccxt (aiohttp) напрямую — дёшево (несколько запросов),
    # поэтому выполняется и в quick-режиме. 4c: сравнение асинхронного
    # DNS-резолвера (aiodns) с системным — объясняет «шаг 4 OK, шаг 5 FAIL
    # на всех биржах» с ClientConnectorDNSError.
    aio_problems, aio_verdict = await check_aiohttp_layer(timeout)
    problems += aio_problems
    dns_problems, dns_verdict = await check_aiodns_layer(timeout)
    problems += dns_problems
    # aiodns-вердикт приоритетнее: он объясняет провалы прямых зондов 4b,
    # и run.py чинит его автоматически (ThreadedResolver).
    transport_verdict = dns_verdict or aio_verdict

    if not quick:
        ccxt_problems = await check_ccxt(names)
        problems += ccxt_problems
        # Классический раскол: «прямые» запросы через urllib проходят (они
        # используют системный прокси), а ccxt напрямую падает на всех биржах.
        # Совет должен быть про это, а не про DNS. transport_verdict непустой
        # только когда шаги 4b/4c нашли конкретную поломку транспорта.
        if ccxt_problems and not http_problems and transport_verdict:
            verdict = transport_verdict
    elif transport_verdict and not http_problems:
        verdict = transport_verdict
    return report(problems, quick, verdict)


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main(sys.argv[1:])))
    except KeyboardInterrupt:
        print("\nпрервано пользователем")
        sys.exit(130)
