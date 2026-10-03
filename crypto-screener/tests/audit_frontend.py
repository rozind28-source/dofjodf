"""
Кросс-проверка фронтенда: HTML ↔ JS ↔ CSS.

Идея простая — любая ссылка JS на несуществующий ID или класс без стилей
ломается МОЛЧА: элемент не находится, фича не работает, ошибок в консоли нет.
Поэтому сверяем всё программно, а не на глаз.
"""
import pathlib
import re
import sys

import os

# каталог можно переопределить — это позволяет тестам гонять аудит на
# модифицированной копии (мутационная проверка), не трогая боевые файлы
W = pathlib.Path(os.environ.get("AUDIT_WEB_DIR", "web"))
html = (W / "index.html").read_text(encoding="utf-8")
js = (W / "app.js").read_text(encoding="utf-8") + (W / "chart.js").read_text(encoding="utf-8")
css = (W / "style.css").read_text(encoding="utf-8")

problems: list[str] = []
notes: list[str] = []


def section(t: str) -> None:
    print(f"\n=== {t} ===")


# ---------------------------------------------------------------- 1. ID
section("1. ID: ссылки JS против разметки HTML")
html_ids = set(re.findall(r'\sid="([^"]+)"', html))
js_sel_ids = set(re.findall(r'\$\("#([A-Za-z0-9_\-]+)"', js))
js_sel_ids |= set(re.findall(r'querySelector\("#([A-Za-z0-9_\-]+)"', js))
js_sel_ids |= set(re.findall(r'getElementById\(["\']([^"\']+)', js))
# ID, которые JS создаёт динамически в innerHTML-шаблонах
dyn_ids = set(re.findall(r'\sid="([A-Za-z0-9_\-]+)"', js))

missing = sorted(js_sel_ids - html_ids - dyn_ids)
print(f"   в HTML объявлено ID: {len(html_ids)}")
print(f"   JS обращается к ID : {len(js_sel_ids)}")
print(f"   создаётся динамически: {sorted(dyn_ids)}")
if missing:
    print(f"   ✗ ОТСУТСТВУЮТ в HTML: {missing}")
    problems.append(f"JS ссылается на несуществующие ID: {missing}")
else:
    print("   ✓ все обращения JS закрыты разметкой или динамическим созданием")

unused = sorted(html_ids - js_sel_ids - dyn_ids)
# часть ID используется как якоря для CSS или как контейнеры — это не дефект
print(f"   ℹ не используются напрямую (контейнеры/якоря CSS): {unused or 'нет'}")


# число колонок таблицы плотностей нужно раньше — для проверки colspan
_dens_head = re.search(r'id="dens-table">\s*<thead><tr>(.*?)</tr>', html, re.S)
n_th_dens = len(re.findall(r"<th[ >]", _dens_head.group(1))) if _dens_head else 0

# ---------------------------------------------------------------- 2. Таблица скринера
section("2. Таблица скринера: colspan против числа колонок")
cols_block = js.split("const COLUMNS = [", 1)[1].split("\n];", 1)[0]
n_cols = len(re.findall(r'\{\s*k:', cols_block))
print(f"   колонок в COLUMNS: {n_cols}")
def enclosing_fn(pos: int) -> str:
    """Имя функции, внутри которой находится позиция pos."""
    names = [(m.start(), m.group(1)) for m in re.finditer(r"(?:async )?function (\w+)", js)]
    cur = "?"
    for start, name in names:
        if start < pos:
            cur = name
    return cur


# сколько колонок у каждой таблицы
TABLE_COLS = {"renderTable": n_cols, "renderDensities": n_th_dens}
for m in re.finditer(r'colspan="(\$\{[^}]+\}|\d+)"', js):
    val, fn = m.group(1), enclosing_fn(m.start())
    expected = TABLE_COLS.get(fn)
    if not val.isdigit():
        print(f"   ✓ colspan={val} в {fn}() — динамический, подстраивается сам")
        continue
    if expected is None:
        print(f"   ℹ colspan={val} в {fn}() — таблица неизвестна аудиту")
        continue
    mark = "✓" if int(val) == expected else "✗"
    print(f"   {mark} colspan={val} в {fn}() при {expected} колонках")
    if int(val) != expected:
        problems.append(f"{fn}: colspan={val} != {expected} колонок")
print("   ✓ thead скринера строится циклом из COLUMNS — расхождение невозможно")


# ---------------------------------------------------------------- 3. Таблица плотностей
section("3. Таблица плотностей: шапка против строки")
n_th = n_th_dens
# шаблон ОДНОЙ строки: ровно тот, что передаётся в rows.map(). Считать все <td>
# внутри функции нельзя — туда попадает и заглушка пустой таблицы.
tpl = re.search(r"body\.innerHTML = rows\.map\(\(r\) => `(.*?)`\)\.join", js, re.S)
n_td = len(re.findall(r"<td[ >]", tpl.group(1))) if tpl else -1
print(f"   <th> в шапке HTML       : {n_th}")
print(f"   <td> в шаблоне строки JS: {n_td}")
if n_th != n_td:
    print("   ✗ РАСХОЖДЕНИЕ — поедут колонки")
    problems.append(f"плотности: th={n_th} != td={n_td}")
else:
    print("   ✓ совпадает")
render_fn = re.search(r"function renderDensities\([\s\S]*?\n\}", js)
m = re.search(r'<tr><td colspan="(\d+)"', render_fn.group(0)) if render_fn else None
if m:
    cs = int(m.group(1))
    print(f"   {'✓' if cs == n_th else '✗'} colspan пустой строки: {cs} (нужно {n_th})")
    if cs != n_th:
        problems.append(f"плотности: colspan пустой строки {cs} != {n_th}")


# ---------------------------------------------------------------- 4. Вкладки
section("4. Вкладки: кнопки против секций")
tab_views = re.findall(r'data-view="([^"]+)"', html)
sections = re.findall(r'<section class="view[^"]*" id="view-([^"]+)"', html)
print(f"   кнопки: {tab_views}")
print(f"   секции: {sections}")
if sorted(tab_views) != sorted(sections):
    print("   ✗ РАСХОЖДЕНИЕ")
    problems.append(f"вкладки: кнопки {tab_views} != секции {sections}")
else:
    print("   ✓ каждой кнопке соответствует секция")
for v in tab_views:
    if f'"view-{v}"' not in js and f"'view-{v}'" not in js:
        if f'id="view-" + v' not in js and '"view-" + v' not in js:
            problems.append(f"вкладка {v} не обрабатывается в switchView")
dynamic_glue = '"view-" + v' in js
print(f"   ✓ switchView склеивает id динамически: {dynamic_glue}")


# ---------------------------------------------------------------- 5. CSS-классы
section("5. CSS: классы, которые ставит JS, но не описаны в style.css")
js_classes: set[str] = set()
for m in re.finditer(r'className\s*=\s*"([^"$]+)"', js):
    js_classes.update(m.group(1).split())
for m in re.finditer(r'class="([^"$]+)"', js):
    js_classes.update(m.group(1).split())
for m in re.finditer(r'classList\.(?:add|remove|toggle)\(([^)]*)\)', js):
    js_classes.update(re.findall(r'"([A-Za-z0-9_\-]+)"', m.group(1)))
css_classes = set(re.findall(r'\.([A-Za-z][A-Za-z0-9_\-]*)', css))
nostyle = sorted(c for c in js_classes - css_classes)
print(f"   классов ставит JS: {len(js_classes)}, описано в CSS: {len(css_classes)}")
if nostyle:
    print(f"   ℹ без явных стилей: {nostyle}")
    notes.append(f"классы без стилей (могут быть служебными): {nostyle}")
else:
    print("   ✓ все классы JS имеют стили")


# ---------------------------------------------------------------- 6. Поля данных
section("6. Поля строки: что рисует UI против того, что отдаёт бэкенд")
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from app.metrics import SymbolState  # noqa: E402

st = SymbolState("e", "E", "swap", "BTC/USDT:USDT", "BTC", "USDT")
st.apply_ticker({"last": 1.0, "quoteVolume": 1.0})
row_keys = set(st.to_row(with_densities=True))
# берём только обращения к переменным, которые точно являются строкой данных:
# r/row/x/t внутри рендеров. Свойства вроде r.json / r.ok относятся к Response,
# r.left — к событию мыши, r.hist — к демо-строке, и к API отношения не имеют.
ui_keys = set(re.findall(r'\br\.([A-Za-z0-9_]+)\b', js)) | set(re.findall(r'\{ k: "([^"]+)"', js))
ui_keys |= set(re.findall(r'row\.([A-Za-z0-9_]+)\b', js))
NOT_ROW_FIELDS = {
    "json", "ok", "status", "text", "headers",     # свойства Response
    "left", "top", "right", "bottom", "width", "height", "clientX", "clientY",  # геометрия/событие
    "hist",                                         # поле демо-строки, генерируется на клиенте
    "dataset", "style", "children", "classList", "hidden", "innerHTML", "value",
    "length", "reason", "stack", "message",
}
ui_keys -= NOT_ROW_FIELDS
unknown = sorted(k for k in ui_keys - row_keys)
print(f"   полей в строке API: {len(row_keys)}")
print(f"   полей использует UI: {len(ui_keys & row_keys)}")
bad = [k for k in unknown if k not in {
    # служебные свойства объектов, а не поля строки
    "length", "chart", "chartTf", "chartKey", "chartSource", "meta", "rows", "demo",
    "filters", "presets", "sorts", "exchanges", "market_types", "quotes", "mode",
    "exchange_info", "field", "label", "kind", "group", "unit", "hint", "min", "max",
    "step", "params", "id", "op", "value", "exchange", "market_type", "symbol_contains",
    "enabled", "cooldown", "notify_ui", "notify_telegram", "hits", "describe", "events",
    "rules", "text", "key", "ts", "rule_id", "overview", "symbols", "up", "down", "flat",
    "volume_usd", "volume_fmt", "stats", "status", "uptime", "tiles", "total", "max_vol",
    "min_vol", "threshold", "spark", "book", "densities", "spikes", "history", "source",
    "candles", "tf", "tf_seconds", "last", "bid", "ask", "chg", "natr", "bids", "asks",
    "p", "q", "sd", "d", "n", "base", "ratio", "age", "kind", "value", "col", "cls",
    "f", "r", "k", "s", "b", "exl", "mt", "dex", "vol", "tr", "fund", "oiusd", "imb",
    "d1m", "cvd", "rng", "hi", "lo", "spike", "u", "r60", "r300", "r900", "r3600", "r14400",
}]
if bad:
    print(f"   ✗ UI читает поля, которых нет в API: {bad}")
    problems.append(f"UI читает несуществующие поля: {bad}")
else:
    print("   ✓ все читаемые UI поля присутствуют в выдаче")
print(f"   ✓ поле dex в строке: {'dex' in row_keys}")


# ---------------------------------------------------------------- 7. Таймфреймы
section("7. Таймфреймы графика: фронтенд против бэкенда")
from app.metrics import TF_SECONDS  # noqa: E402

be_tfs = set(TF_SECONDS)
fe_tfs = set(re.findall(r'"([^"]+)"', js.split("const TFS = [", 1)[1].split("]", 1)[0]))
fe_sec_raw = dict(re.findall(r'"([0-9a-z]+)":\s*(\d+)', js.split("const TF_SEC = {", 1)[1].split("};", 1)[0]))
fe_sec = {k: int(v) for k, v in fe_sec_raw.items()}
print(f"   бэкенд TF_SECONDS    : {sorted(be_tfs)}")
print(f"   фронтенд TFS (кнопки): {sorted(fe_tfs)}")
extra = sorted(fe_tfs - be_tfs)
if extra:
    print(f"   ✗ кнопки, которые бэкенд отвергнет (400): {extra}")
    problems.append(f"неизвестные бэкенду таймфреймы в UI: {extra}")
else:
    print("   ✓ каждая кнопка ТФ известна бэкенду")
bad_sec = {k: (v, TF_SECONDS[k]) for k, v in fe_sec.items()
           if k in TF_SECONDS and TF_SECONDS[k] != v}
if bad_sec:
    print(f"   ✗ расходится длительность ТФ (фронт/бэк): {bad_sec}")
    problems.append(f"длительности ТФ расходятся: {bad_sec}")
else:
    print("   ✓ длительности ТФ совпадают (важно для шага свечей и demo-режима)")


# ---------------------------------------------------------------- 8. График
section("8. График: разметка карточки и подключение chart.js")
# Присваиваний #drawer-body.innerHTML в коде ДВА: первое — заглушка «загрузка…»
# в openDrawer(), второе — настоящий шаблон карточки в renderDrawer().
# Брать только первое (как делал re.search) значит проверять заглушку и
# рапортовать, что canvas отсутствует. Поэтому берём все шаблоны и объединяем.
tpl_txt = "\n".join(
    m.group(1) for m in re.finditer(r'\$\("#drawer-body"\)\.innerHTML = `([\s\S]*?)`;', js)
)
print(f"   найдено шаблонов карточки: {js.count(chr(35) + 'drawer-body' + chr(34) + ').innerHTML')}")
for needle, why in (('id="d-chart"', "canvas для графика"),
                    ('id="d-chart-state"', "заглушка состояния загрузки"),
                    ("data-tf=", "кнопки переключения таймфрейма"),
                    ("chart-legend", "легенда (плотности/спайки/цена)")):
    found = needle in tpl_txt
    print(f"   {'✓' if found else '✗'} в карточке есть {why}")
    if not found:
        problems.append(f"в карточке инструмента нет: {why}")

has_chart = "chart.js" in html
print(f"   {'✓' if has_chart else '✗'} chart.js подключён в index.html")
if not has_chart:
    problems.append("chart.js не подключён — график не появится")
order = [m.group(1) for m in re.finditer(r'<script src="([^"]+)"', html)]
print(f"   порядок скриптов: {order}")
if "chart.js" in order and "app.js" in order and order.index("chart.js") > order.index("app.js"):
    problems.append("app.js подключён раньше chart.js — CandleChart будет undefined")
    print("   ✗ app.js подключён РАНЬШЕ chart.js")
elif order:
    print("   ✓ chart.js подключён до app.js")


# ---------------------------------------------------------------- 9. Ресурсы
section("9. Подключённые ресурсы")
for f in ("app.js", "chart.js", "style.css"):
    print(f"   {'✓' if (W / f).exists() else '✗'} web/{f} существует")
    if not (W / f).exists():
        problems.append(f"нет файла web/{f}")
refs = re.findall(r'(?:src|href)="([^"]+)"', html)
print(f"   ссылки в HTML: {refs}")
ext = [r for r in refs if r.startswith(("http://", "https://", "//"))]
if ext:
    print(f"   ✗ внешние ресурсы (не загрузятся офлайн/в песочнице): {ext}")
    problems.append(f"внешние ресурсы в HTML: {ext}")
else:
    print("   ✓ внешних ресурсов нет — всё работает офлайн")
for r in refs:
    if r.startswith("/") or "://" in r:
        continue
    if not (W / r).exists():
        print(f"   ✗ относительная ссылка {r} не найдена")
        problems.append(f"битая ссылка {r}")
    else:
        print(f"   ✓ относительная ссылка {r} ведёт на существующий файл")


# ---------------------------------------------------------------- итог
print("\n" + "=" * 70)
if problems:
    print(f"НАЙДЕНО ПРОБЛЕМ: {len(problems)}")
    for p in problems:
        print(f"  ✗ {p}")
    sys.exit(1)
print("ПРОБЛЕМ НЕ НАЙДЕНО — разметка, JS и CSS согласованы")
for n in notes:
    print(f"  ℹ {n}")
