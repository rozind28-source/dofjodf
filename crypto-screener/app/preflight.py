"""
Предполётная проверка зависимостей.

Зачем отдельный модуль, а не просто `pip install -r requirements.txt`:
`import ccxt` успешно проходит и на старых версиях, а классов `aster` /
`hyperliquid` там нет — биржа просто не подключится, и снаружи это выглядит
как «скринер её не видит». Плюс часть пакетов опциональна и их отсутствие
ломает не всё, а одну биржу (protobuf нужен только для WebSocket MEXC).

Поэтому проверяем не факт импорта, а:
  * наличие пакета;
  * минимальную версию;
  * наличие конкретных классов бирж, которые нам нужны;
  * extras у uvicorn (от них зависит скорость, но не работоспособность).
"""
from __future__ import annotations

import importlib
import sys
from dataclasses import dataclass, field

# (модуль, минимальная версия или None, зачем нужен, критично ли)
REQUIRED: list[tuple[str, str | None, str, bool]] = [
    ("ccxt", "4.5.0", "подключение к биржам; 4.5+ нужен для Aster и Hyperliquid", True),
    ("fastapi", "0.115.0", "HTTP/WebSocket API", True),
    ("uvicorn", "0.30.0", "ASGI-сервер", True),
    ("websockets", "12.0", "WS-транспорт для ccxt.pro", True),
]

# Не критично: без них приложение работает, но часть возможностей теряется.
OPTIONAL: list[tuple[str, str | None, str]] = [
    ("google.protobuf", None,
     "без него WebSocket MEXC сыплет NotSupported «requires protobuf» "
     "→ pip install protobuf==5.29.5"),
    ("certifi", None,
     "корневые TLS-сертификаты; на Windows без них возможны ошибки проверки "
     "сертификата → pip install -U certifi"),
    ("orjson", None,
     "сериализация WS-пуша в 3-9 раз быстрее stdlib json (без него работает "
     "встроенный fallback) → pip install orjson"),
]

# Классы ccxt.pro, которые обязаны существовать для подключённых бирж.
# Проверяются по факту, потому что ccxt добавлял их постепенно.
NEEDED_EXCHANGE_CLASSES = ("binance", "binanceusdm", "bybit", "okx",
                           "mexc", "gate", "aster", "hyperliquid")

# extras у uvicorn[standard] — влияют на производительность, не на запуск
UVICORN_EXTRAS = ("httptools", "watchfiles", "uvloop")


@dataclass
class Issue:
    level: str          # "error" | "warn"
    text: str
    fix: str = ""


@dataclass
class Report:
    issues: list[Issue] = field(default_factory=list)
    versions: dict[str, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not any(i.level == "error" for i in self.issues)

    @property
    def errors(self) -> list[Issue]:
        return [i for i in self.issues if i.level == "error"]

    @property
    def warnings(self) -> list[Issue]:
        return [i for i in self.issues if i.level == "warn"]


def _ver_tuple(v: str) -> tuple[int, ...]:
    """
    Версия → кортеж чисел для сравнения.

    Берём только ВЕДУЩИЕ цифры каждой части: иначе у «4.5.0rc1» хвост «rc1»
    давал «01» → 1, и pre-release оказывался новее релиза 4.5.0. Такое
    сравнение молча пропустило бы слишком старую/нестабильную версию.
    """
    out = []
    for part in v.split(".")[:3]:
        digits = ""
        for ch in part:
            if ch.isdigit():
                digits += ch
            else:
                break
        out.append(int(digits) if digits else 0)
    return tuple(out)


def check(verify_exchanges: bool = True) -> Report:
    """Проверяет окружение. Исключений не бросает — возвращает отчёт."""
    rep = Report()

    if sys.version_info < (3, 10):
        rep.issues.append(Issue(
            "error",
            f"Python {sys.version.split()[0]}, а нужно 3.10+",
            "установите Python 3.10 или новее с https://python.org"))

    for mod, minver, why, critical in REQUIRED:
        try:
            m = importlib.import_module(mod)
        except Exception as e:
            rep.issues.append(Issue(
                "error" if critical else "warn",
                f"не установлен пакет {mod} ({type(e).__name__}) — {why}",
                f"pip install -r requirements.txt"))
            continue
        ver = getattr(m, "__version__", None)
        rep.versions[mod] = ver or "?"
        if ver and minver and _ver_tuple(ver) < _ver_tuple(minver):
            rep.issues.append(Issue(
                "error",
                f"{mod} {ver} старше требуемого {minver} — {why}",
                f"pip install -U \"{mod}>={minver}\""))

    for mod, minver, why in OPTIONAL:
        try:
            m = importlib.import_module(mod)
            rep.versions[mod] = getattr(m, "__version__", "?")
        except Exception:
            rep.issues.append(Issue("warn", f"нет необязательного пакета {mod}", why))

    # extras uvicorn — только предупреждение
    missing_extras = []
    for e in UVICORN_EXTRAS:
        try:
            importlib.import_module(e)
        except Exception:
            missing_extras.append(e)
    if missing_extras:
        # uvloop на Windows не используется в принципе — это не дефект
        if sys.platform == "win32" and missing_extras == ["uvloop"]:
            pass
        else:
            rep.issues.append(Issue(
                "warn",
                f"нет uvicorn-extras: {', '.join(missing_extras)} — сервер будет работать, "
                f"но медленнее",
                'pip install "uvicorn[standard]"'))

    if verify_exchanges:
        try:
            pro = importlib.import_module("ccxt.pro")
            absent = [c for c in NEEDED_EXCHANGE_CLASSES if not hasattr(pro, c)]
            if absent:
                # версию берём из уже собранного отчёта: повторный `import ccxt`
                # вернул бы закэшированный модуль и мог показать не то число
                rep.issues.append(Issue(
                    "error",
                    f"в ccxt {rep.versions.get('ccxt', '?')} нет классов бирж: "
                    f"{', '.join(absent)}",
                    'pip install -U "ccxt>=4.5.0"'))
        except Exception as e:
            rep.issues.append(Issue("error", f"не импортируется ccxt.pro: {e}",
                                    "pip install -r requirements.txt"))
    return rep


def format_report(rep: Report) -> str:
    lines = []
    if rep.versions:
        lines.append("  версии: " + ", ".join(f"{k} {v}" for k, v in sorted(rep.versions.items())))
    for i in rep.errors:
        lines.append(f"  [ОШИБКА] {i.text}")
        if i.fix:
            lines.append(f"           → {i.fix}")
    for i in rep.warnings:
        lines.append(f"  [внимание] {i.text}")
        if i.fix:
            lines.append(f"           → {i.fix}")
    if rep.ok and not rep.warnings:
        lines.append("  [ OK ] все зависимости на месте")
    return "\n".join(lines)


def ensure_or_exit(argv: list[str] | None = None) -> Report:
    """
    Проверка при старте run.py.

    Без неё отсутствие ccxt выглядело как голый traceback из недр коллектора,
    а старая версия ccxt — как «Aster не подключается», хотя дело в версии.
    """
    rep = check()
    if rep.ok:
        return rep
    print("=" * 74, flush=True)
    print("  Не хватает зависимостей — сервер не запускается.", flush=True)
    print("=" * 74, flush=True)
    print(format_report(rep), flush=True)
    print("", flush=True)
    print("  Установить всё сразу:", flush=True)
    print("      python -m pip install -r requirements.txt", flush=True)
    print("", flush=True)
    print("  Или запустите start.bat / start.ps1 — они ставят зависимости сами", flush=True)
    print("  и предлагают меню режимов.", flush=True)
    print("", flush=True)
    print("  Проверить окружение подробнее:  python doctor.py", flush=True)
    print("=" * 74, flush=True)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Системный прокси Windows (WinINET / реестр)
# ---------------------------------------------------------------------------
# Зачем: urllib.request (и браузер) на Windows читают прокси из реестра, а
# aiohttp — транспорт ccxt — нет. VPN-приложения (Clash, v2rayN, Outline и т.п.)
# включают именно этот прокси, и тогда «прямые» проверки проходят, а ccxt
# мгновенно падает с ExchangeNotAvailable. Диагноз без реестра невозможен:
# переменные окружения в этой схеме пустые.
_INTERNET_SETTINGS_KEY = r"Software\Microsoft\Windows\CurrentVersion\Internet Settings"


def normalize_proxy_server(server: str) -> str:
    """
    Значение ProxyServer из реестра → URL вида http://host:port.

    Форматы, которые встречаются в реальности:
      * «127.0.0.1:7890»                       — один прокси на все протоколы;
      * «http=h1:p1;https=h2:p2;ftp=h3:p3»     — раздельно по протоколам;
      * «h1:p1;h2:p2»                          — legacy-список без ключей.
    Пустая строка → пустой результат.
    """
    s = (server or "").strip()
    if not s:
        return ""
    if "=" in s:
        parts: dict[str, str] = {}
        for chunk in s.split(";"):
            if "=" in chunk:
                k, v = chunk.split("=", 1)
                if v.strip():
                    parts[k.strip().lower()] = v.strip()
        s = parts.get("https") or parts.get("http") or next(iter(parts.values()), "")
    else:
        s = s.split(";")[0].strip()
    if s and "://" not in s:
        s = "http://" + s
    return s


def windows_system_proxy() -> tuple[bool, str, str]:
    """
    (включён, адрес прокси, адрес PAC) — то, чем Windows кормит браузер/urllib.

    Вне Windows и при любой ошибке реестра возвращает (False, "", "") —
    диагностика не должна падать из-за того, что не смогла заглянуть в реестр.
    """
    if sys.platform != "win32":
        return False, "", ""
    try:
        import winreg

        def val(key, name: str, default):
            try:
                return winreg.QueryValueEx(key, name)[0]
            except OSError:
                return default

        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, _INTERNET_SETTINGS_KEY) as key:
            try:
                enabled = bool(int(val(key, "ProxyEnable", 0) or 0))
            except (TypeError, ValueError):
                enabled = False
            server = normalize_proxy_server(str(val(key, "ProxyServer", "") or ""))
            pac = str(val(key, "AutoConfigURL", "") or "").strip()
        return enabled, server, pac
    except Exception:
        return False, "", ""
