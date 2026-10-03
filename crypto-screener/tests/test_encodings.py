"""
Кодировки файлов запуска.

Реальный случай: start.ps1 был сохранён в UTF-8 БЕЗ BOM. Windows PowerShell 5.1
читает .ps1 без BOM как ANSI (в русской локали — cp1251), кириллица
превращается в «Р”РРђР“РќРћРЎРўРРљРђ», и парсер ломается на первой же «)»
внутри строки:

    Unexpected token ')' in expression or statement.
    Missing closing '}' in statement block or type definition.

Починить это можно только на уровне байтов файла, поэтому проверяем байты.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

BOM = b"\xef\xbb\xbf"


def test_start_ps1_has_utf8_bom():
    """PowerShell 5.1 без BOM читает файл как ANSI и ломается на кириллице."""
    raw = (ROOT / "start.ps1").read_bytes()
    assert raw.startswith(BOM), (
        "start.ps1 должен быть сохранён как UTF-8 with BOM, иначе Windows "
        "PowerShell 5.1 прочитает кириллицу как cp1251 и упадёт с "
        "«Unexpected token ')'»"
    )


def test_start_ps1_decodes_as_utf8():
    raw = (ROOT / "start.ps1").read_bytes()
    text = raw.decode("utf-8-sig")
    # ключевые русские строки меню должны пережить декодирование
    for probe in ("ДЕМО", "ДИАГНОСТИКА", "Открываю"):
        assert probe in text, f"в start.ps1 пропала строка {probe!r}"


def test_start_ps1_no_mojibake():
    """Признак двойного перекодирования: «Р”», «Рѕ», «вЂ”»."""
    text = (ROOT / "start.ps1").read_bytes().decode("utf-8-sig")
    for bad in ("Р”", "Рѕ", "Р†", "вЂ”", "вЂ“", "Гў"):
        assert bad not in text, f"в start.ps1 найдена мохибека {bad!r}"


def test_start_bat_is_pure_ascii():
    """
    cmd.exe читает .bat в активной кодовой странице, а не в UTF-8.
    Любая кириллица там превращается в мусор и может сломать парсинг,
    поэтому launch-файл для cmd держим строго в ASCII.
    """
    raw = (ROOT / "start.bat").read_bytes()
    bad = [(i, b) for i, b in enumerate(raw) if b > 127]
    assert not bad, f"в start.bat {len(bad)} не-ASCII байт, первые: {bad[:5]}"


def test_start_bat_has_no_bom():
    """BOM в .bat ломает cmd.exe — первая команда читается как мусор."""
    raw = (ROOT / "start.bat").read_bytes()
    assert not raw.startswith(BOM), "start.bat не должен иметь BOM"


def test_start_bat_no_delayed_expansion_vars():
    """
    !VAR! работает только при EnableDelayedExpansion. Внутри блока (...)
    переменные раскрываются на этапе разбора, то есть ДО set /p, поэтому
    введённое пользователем значение там недоступно.
    """
    text = (ROOT / "start.bat").read_text(encoding="ascii")
    if "EnableDelayedExpansion" not in text:
        import re
        bad = re.findall(r"![A-Za-z_][A-Za-z0-9_]*!", text)
        assert not bad, f"используется !VAR! без EnableDelayedExpansion: {bad}"


def test_batch_labels_resolvable():
    """Каждый `goto X` должен иметь метку `:X` — иначе cmd аварийно завершится."""
    import re
    text = (ROOT / "start.bat").read_text(encoding="ascii")
    labels = set(re.findall(r"^:([A-Za-z_][A-Za-z0-9_]*)", text, re.M))
    gotos = set(re.findall(r"goto\s+([A-Za-z_][A-Za-z0-9_]*)", text))
    missing = gotos - labels - {"eof"}
    assert not missing, f"goto ведёт в никуда: {sorted(missing)}; метки: {sorted(labels)}"


def test_python_sources_are_utf8():
    """Python 3 читает исходники как UTF-8 — проверяем, что они действительно валидны."""
    bad = []
    for f in list((ROOT / "app").glob("*.py")) + [ROOT / "run.py", ROOT / "doctor.py"]:
        try:
            f.read_bytes().decode("utf-8")
        except UnicodeDecodeError as e:
            bad.append(f"{f.name}: {e}")
    assert not bad, f"не-UTF8 исходники: {bad}"


def test_web_assets_are_utf8_and_declare_charset():
    html = (ROOT / "web" / "index.html").read_bytes()
    html.decode("utf-8")                      # должен быть валидным UTF-8
    has_charset = b'charset="utf-8"' in html or b"charset=utf-8" in html
    assert has_charset, "в index.html нет <meta charset> — кириллица в браузере поедет"
    for name in ("app.js", "chart.js", "style.css"):
        (ROOT / "web" / name).read_bytes().decode("utf-8")
