"""
Статический аудит связки HTML ↔ JS ↔ CSS.

Зачем отдельный тест: любая ссылка JS на несуществующий ID или класс без стилей
ломается МОЛЧА — элемент не находится, фича не работает, а в консоли чисто.
Интеграционные тесты такое ловят не всегда, потому что проверяют поведение,
а не согласованность разметки.

Плюс мутационная проверка: аудит, который никогда не падает, бесполезен,
поэтому отдельно убеждаемся, что на сломанной разметке он действительно ругается.
"""
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
AUDIT = ROOT / "tests" / "audit_frontend.py"
WEB = ROOT / "web"


def run_audit(web_dir: Path | None = None) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    if web_dir is not None:
        env["AUDIT_WEB_DIR"] = str(web_dir)
    return subprocess.run([sys.executable, str(AUDIT)], cwd=str(ROOT),
                          capture_output=True, text=True, env=env, timeout=120)


def test_frontend_is_consistent():
    """Боевая разметка, JS и CSS согласованы."""
    r = run_audit()
    assert r.returncode == 0, r.stdout[-3000:] + r.stderr[-2000:]
    assert "ПРОБЛЕМ НЕ НАЙДЕНО" in r.stdout


# --------------------------------------------------------------------------
# Мутационная проверка: аудит обязан ловить поломки
# --------------------------------------------------------------------------
MUTATIONS = [
    ("chart.js отключён", "index.html",
     '<script src="chart.js"></script>\n', ""),
    ("несуществующий таймфрейм", "app.js",
     'const TFS = ["1m", "5m", "15m", "1h", "4h", "1d"];',
     'const TFS = ["1m", "7m", "15m", "1h", "4h", "1d"];'),
    ("сломана колонка плотностей", "index.html",
     '<th class="r">Кол-во заявок</th>', ""),
    ("JS обращается к несуществующему ID", "app.js",
     '$("#d-chart")', '$("#d-chart-typo")'),
    ("внешний ресурс (не работает офлайн)", "index.html",
     '<link rel="stylesheet" href="style.css">',
     '<link rel="stylesheet" href="https://cdn.example.com/x.css">'),
]


@pytest.mark.parametrize("name,filename,old,new", MUTATIONS)
def test_audit_catches_breakage(tmp_path, name, filename, old, new):
    work = tmp_path / "web"
    shutil.copytree(WEB, work)
    target = work / filename
    text = target.read_text(encoding="utf-8")
    assert old in text, f"мутация {name!r} не применима — шаблон изменился"
    target.write_text(text.replace(old, new, 1), encoding="utf-8")

    r = run_audit(work)
    assert r.returncode != 0, \
        f"аудит НЕ поймал поломку {name!r} — проверки бесполезны\n{r.stdout[-2000:]}"
    assert "НАЙДЕНО ПРОБЛЕМ" in r.stdout


def test_mutation_isolated_from_real_files():
    """Мутационные тесты работают на копии — боевые файлы не должны меняться."""
    r = run_audit()
    assert r.returncode == 0, "боевые файлы оказались сломаны после мутационных тестов"
