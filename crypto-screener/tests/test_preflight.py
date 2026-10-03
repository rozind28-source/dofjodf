"""
Тесты предполётной проверки зависимостей.

Ключевой сценарий, который они закрывают: `import ccxt` успешно проходит и на
старых версиях, где нет классов `aster`/`hyperliquid`. Наивная проверка
«импортируется — значит нормально» отрапортовала бы OK, а биржи молча не
подключились бы. Поэтому проверяются версии и наличие конкретных классов.
"""
import importlib
import sys

import pytest

from app import preflight as pf


@pytest.fixture
def fake_import(monkeypatch):
    """Подменяет importlib.import_module внутри preflight."""
    real = importlib.import_module

    def install(mapping):
        def fake(name):
            if name in mapping:
                target = mapping[name]
                if isinstance(target, Exception):
                    raise target
                return target
            return real(name)
        monkeypatch.setattr(pf.importlib, "import_module", fake)
    return install


class _Mod:
    def __init__(self, version):
        self.__version__ = version


class _Pro:
    """Заглушка ccxt.pro с настраиваемым набором классов бирж."""
    def __init__(self, have):
        for c in have:
            setattr(self, c, object)


# --------------------------------------------------------------------------
def test_ok_on_healthy_environment():
    rep = pf.check()
    assert rep.ok, pf.format_report(rep)
    assert not rep.errors
    for mod in ("ccxt", "fastapi", "uvicorn", "websockets"):
        assert mod in rep.versions


def test_missing_ccxt_is_error(fake_import):
    fake_import({"ccxt": ImportError("No module named 'ccxt'"),
                 "ccxt.pro": ImportError("No module named 'ccxt'")})
    rep = pf.check()
    assert not rep.ok
    assert any("ccxt" in e.text and "не установлен" in e.text for e in rep.errors)
    assert any("pip install" in e.fix for e in rep.errors)


def test_old_ccxt_version_is_error(fake_import):
    """Версия ниже минимальной — ошибка, даже если импорт проходит."""
    fake_import({"ccxt": _Mod("4.1.10")})
    rep = pf.check()
    assert not rep.ok
    assert any("4.1.10" in e.text and "4.5.0" in e.text for e in rep.errors)


def test_version_boundary_accepted(fake_import):
    fake_import({"ccxt": _Mod("4.5.0")})
    rep = pf.check()
    assert not any("старше требуемого" in e.text for e in rep.errors)


def test_missing_exchange_classes_is_error(fake_import):
    """
    Главный случай: версия подходящая, но классов Aster/Hyperliquid нет.
    Наивная проверка импорта здесь сказала бы «всё хорошо».
    """
    have = ("binance", "binanceusdm", "bybit", "okx", "mexc", "gate")
    fake_import({"ccxt.pro": _Pro(have)})
    rep = pf.check()
    assert not rep.ok
    joined = " ".join(e.text for e in rep.errors)
    assert "aster" in joined and "hyperliquid" in joined


def test_all_exchange_classes_present_is_ok():
    rep = pf.check(verify_exchanges=True)
    assert not any("нет классов бирж" in e.text for e in rep.errors)


def test_missing_protobuf_is_warning_not_error(fake_import):
    """
    protobuf нужен только для WebSocket MEXC: без него приложение работает,
    поэтому это предупреждение, а не блокирующая ошибка.
    """
    fake_import({"google.protobuf": ImportError("No module named 'google'")})
    rep = pf.check()
    assert rep.ok, "отсутствие protobuf не должно блокировать запуск"
    assert any("protobuf" in w.text for w in rep.warnings)
    assert any("MEXC" in w.fix for w in rep.warnings)


def test_uvicorn_extras_are_warning(fake_import):
    fake_import({"httptools": ImportError("no httptools")})
    rep = pf.check()
    assert rep.ok
    assert any("uvicorn-extras" in w.text for w in rep.warnings)


def test_old_python_is_error(monkeypatch):
    monkeypatch.setattr(pf.sys, "version_info", (3, 8, 10, "final", 0))
    rep = pf.check(verify_exchanges=False)
    assert not rep.ok
    assert any("3.10" in e.text for e in rep.errors)


# --------------------------------------------------------------------------
def test_ver_tuple_handles_suffixes():
    assert pf._ver_tuple("4.5.85") == (4, 5, 85)
    assert pf._ver_tuple("4.5.0rc1") == (4, 5, 0)
    assert pf._ver_tuple("2026.06.17") == (2026, 6, 17)
    assert pf._ver_tuple("4.5") == (4, 5)


def test_report_format_mentions_fix():
    rep = pf.Report(issues=[pf.Issue("error", "нет ccxt", "pip install -r requirements.txt")])
    text = pf.format_report(rep)
    assert "нет ccxt" in text and "pip install" in text
    assert rep.ok is False


def test_ensure_or_exit_passes_when_ok():
    rep = pf.ensure_or_exit()
    assert rep.ok


def test_ensure_or_exit_blocks_on_error(monkeypatch):
    monkeypatch.setattr(pf, "check",
                        lambda **kw: pf.Report(issues=[pf.Issue("error", "сломано", "почините")]))
    with pytest.raises(SystemExit) as ei:
        pf.ensure_or_exit()
    assert ei.value.code == 1


# --------------------------------------------------------------------------
def test_run_py_checks_dependencies_before_starting():
    """run.py обязан проверять зависимости до импорта тяжёлых модулей."""
    import pathlib
    root = pathlib.Path(pf.__file__).resolve().parent.parent
    text = (root / "run.py").read_text(encoding="utf-8")
    assert "ensure_or_exit" in text, "run.py не вызывает предполётную проверку"
    # проверка должна идти до импорта uvicorn/config
    pos_check = text.index("ensure_or_exit")
    pos_uvicorn = text.index("import uvicorn")
    assert pos_check < pos_uvicorn, "проверка зависимостей выполняется позже импорта uvicorn"


def test_launchers_use_preflight_not_bare_import():
    """
    Запускальщики должны гонять preflight, а не «import ccxt, fastapi, ...»:
    иначе старая версия ccxt проходит проверку и биржи молча не подключаются.
    """
    import pathlib
    root = pathlib.Path(__file__).resolve().parent.parent
    for name in ("start.bat", "start.ps1"):
        raw = (root / name).read_bytes()
        text = raw.decode("utf-8-sig")
        assert "preflight" in text, f"{name} не использует app.preflight"
        naive = "import ccxt, fastapi" in text
        assert not naive, f"{name} всё ещё проверяет зависимости наивным импортом"


# ---------------------------------------------------------------------------
# Системный прокси Windows (реестр WinINET)
# ---------------------------------------------------------------------------
# Регрессия (BUGFIXES #45): urllib на Windows берёт прокси из реестра, aiohttp —
# нет. У пользователя «прямые» запросы doctor.py проходили (через системный
# прокси VPN-приложения), а ccxt падал мгновенно на всех биржах. Без чтения
# реестра диагностика этот раскол не видела.
@pytest.mark.parametrize("raw,want", [
    ("", ""),
    ("127.0.0.1:7890", "http://127.0.0.1:7890"),
    ("http=10.0.0.1:8080;https=10.0.0.2:8443", "http://10.0.0.2:8443"),
    ("http=10.0.0.1:8080", "http://10.0.0.1:8080"),
    ("https=10.0.0.2:8443", "http://10.0.0.2:8443"),
    ("http://127.0.0.1:1080", "http://127.0.0.1:1080"),
    ("socks=127.0.0.1:1080", "http://127.0.0.1:1080"),
    ("  proxy.local:3128 ; backup:3128 ", "http://proxy.local:3128"),
    ("http=;https=1.2.3.4:80", "http://1.2.3.4:80"),
])
def test_normalize_proxy_server(raw, want):
    assert pf.normalize_proxy_server(raw) == want


def test_windows_system_proxy_safe_off_windows():
    """На не-Windows функция обязана молча возвращать пустой результат."""
    on, url, pac = pf.windows_system_proxy()
    if sys.platform != "win32":
        assert (on, url, pac) == (False, "", "")


def test_apply_proxy_warns_once_on_windows_system_proxy(monkeypatch, caplog):
    """
    Если PROXY_URL не задан, а системный прокси Windows включён, коллектор
    обязан один раз предупредить в лог — иначе причина «все биржи
    ExchangeNotAvailable» остаётся невидимой.
    """
    import logging

    from app import collector as C
    monkeypatch.setattr(C, "_warned_system_proxy", False)
    monkeypatch.setattr(C, "SETTINGS_PROXY_OVERRIDE", "")
    for k in C.PROXY_ENV:
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(C.sys, "platform", "win32")
    monkeypatch.setattr(pf, "windows_system_proxy",
                        lambda: (True, "http://127.0.0.1:7890", ""))
    with caplog.at_level(logging.WARNING, logger="collector"):
        assert C.apply_proxy({}) is None
        C.apply_proxy({})   # флаг уже взведён — второго предупреждения нет
    warnings = [r for r in caplog.records if "7890" in r.getMessage()]
    assert len(warnings) == 1, "предупреждение должно печататься ровно один раз"
    assert "PROXY_URL" in warnings[0].getMessage()


def test_apply_proxy_no_warning_when_proxy_set(monkeypatch, caplog):
    """С заданным PROXY_URL предупреждение про системный прокси не нужно."""
    import logging

    from app import collector as C
    monkeypatch.setattr(C, "_warned_system_proxy", False)
    monkeypatch.setattr(C, "SETTINGS_PROXY_OVERRIDE", "http://explicit:8080")
    monkeypatch.setattr(C.sys, "platform", "win32")
    with caplog.at_level(logging.WARNING, logger="collector"):
        assert C.apply_proxy({}) == "http://explicit:8080"
    assert not [r for r in caplog.records if "системный прокси" in r.getMessage()]
