"""
Фильтры и сортировки.

Декларативное описание всех доступных фильтров: от него же генерируется UI,
поэтому «добавить фильтр» = добавить одну запись в FILTER_SPECS, а не править
три места (бэкенд + фронт + документацию).

Формат параметров в запросе:  <field>_min / <field>_max  для диапазонов,
<field> = 1/0 для флагов,  ex = список бирж,  q = поисковая строка.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional


@dataclass(frozen=True)
class FilterSpec:
    field: str
    label: str
    kind: str            # "range" | "flag" | "select" | "search"
    group: str
    unit: str = ""
    default_min: Optional[float] = None
    default_max: Optional[float] = None
    step: float = 0.01
    hint: str = ""


# --------------------------------------------------------------------------
# Каталог фильтров
# --------------------------------------------------------------------------
FILTER_SPECS: list[FilterSpec] = [
    # --- движение ---
    FilterSpec("chg", "Изменение 24ч", "range", "Движение", "%", -100, 100, 0.1,
               "Процент изменения за 24 часа"),
    FilterSpec("r60", "Изменение 1м", "range", "Движение", "%", -100, 100, 0.1),
    FilterSpec("r300", "Изменение 5м", "range", "Движение", "%", -100, 100, 0.1),
    FilterSpec("r900", "Изменение 15м", "range", "Движение", "%", -100, 100, 0.1),
    FilterSpec("r3600", "Изменение 1ч", "range", "Движение", "%", -100, 100, 0.1),
    FilterSpec("r14400", "Изменение 4ч", "range", "Движение", "%", -100, 100, 0.1),
    FilterSpec("rng", "Диапазон 24ч", "range", "Движение", "%", 0, 1000, 0.1,
               "high/low - 1 за сутки: насколько монету швыряло"),
    FilterSpec("natr", "NATR (1m, 14)", "range", "Волатильность", "%", 0, 100, 0.01,
               "Normalized ATR — волатильность, сравнимая между монетами"),
    FilterSpec("last", "Цена", "range", "Движение", "", 0, None, 0.000001),

    # --- объём и активность ---
    FilterSpec("vol", "Объём 24ч", "range", "Объём", "$", 0, None, 1000,
               "Оборот в USD за 24 часа"),
    FilterSpec("tr", "Сделок 24ч", "range", "Объём", "", 0, None, 1),
    FilterSpec("d1m", "Дельта 1м", "range", "Поток", "$", None, None, 100,
               "buy volume - sell volume за последнюю минуту"),
    FilterSpec("cvd", "CVD", "range", "Поток", "$", None, None, 100,
               "Cumulative Volume Delta с момента запуска скринера"),
    FilterSpec("imb", "Дисбаланс стакана", "range", "Поток", "x", 0, 100, 0.01,
               "bid quote / ask quote: >1 — перевес покупателей"),

    # --- деривативы ---
    FilterSpec("fund", "Funding", "range", "Деривативы", "", -1, 1, 0.0001,
               "Ставка финансирования (0.0001 = 0.01%)"),
    FilterSpec("oiusd", "Open Interest", "range", "Деривативы", "$", 0, None, 1000),
    FilterSpec("oi", "OI (контракты)", "range", "Деривативы", "", 0, None, 1),

    # --- флаги ---
    FilterSpec("spike", "Есть спайк", "flag", "События", hint="Объём/сделки всплеснули ≥3x от базовой минуты"),
    FilterSpec("dens", "Есть крупная плотность", "flag", "События",
               hint="В стакане стоит лимитный объём выше порога BIG_DENSITY_USD"),
    FilterSpec("green", "Только зелёные", "flag", "Движение", hint="chg > 0"),
    FilterSpec("red", "Только красные", "flag", "Движение", hint="chg < 0"),
    FilterSpec("unique", "Независимые инструменты", "flag", "Отбор",
               hint="Исключить монеты, которые торгуются ещё и на других биржах"),
]

SORT_FIELDS = {
    "vol": "Объём 24ч",
    "chg": "Изменение 24ч",
    "rng": "Диапазон 24ч",
    "natr": "NATR",
    "tr": "Сделки 24ч",
    "d1m": "Дельта 1м",
    "cvd": "CVD",
    "imb": "Дисбаланс",
    "fund": "Funding",
    "oiusd": "OI, $",
    "last": "Цена",
    "r60": "Δ 1ч",
    "r300": "Δ 5м",
    "r900": "Δ 15м",
    "r14400": "Δ 4ч",
    "r3600": "Δ 1ч",
    "u": "Обновлено",
}


# --------------------------------------------------------------------------
# Парсинг параметров
# --------------------------------------------------------------------------
def parse_params(raw: dict[str, Any]) -> dict[str, Any]:
    """Превращаем query-параметры (строки) в типизированный dict условий."""
    p: dict[str, Any] = {}
    for spec in FILTER_SPECS:
        if spec.kind == "range":
            lo = _num(raw.get(f"{spec.field}_min"))
            hi = _num(raw.get(f"{spec.field}_max"))
            if lo is not None:
                p[f"{spec.field}_min"] = lo
            if hi is not None:
                p[f"{spec.field}_max"] = hi
        elif spec.kind == "flag":
            if str(raw.get(spec.field, "")).lower() in {"1", "true", "yes", "on"}:
                p[spec.field] = True
    if raw.get("ex"):
        p["ex"] = [x for x in str(raw["ex"]).split(",") if x]
    if raw.get("mt"):
        p["mt"] = str(raw["mt"])
    if raw.get("q"):
        p["q"] = str(raw["q"]).strip().upper()
    if raw.get("q_base"):
        p["q_base"] = str(raw["q_base"]).strip().upper()
    return p


def _num(v: Any) -> Optional[float]:
    if v in (None, ""):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------
# Применение
# --------------------------------------------------------------------------
def _passes(row: dict, p: dict[str, Any], dup_bases: Optional[dict[str, set[str]]] = None) -> bool:
    # диапазоны
    for spec in FILTER_SPECS:
        if spec.kind != "range":
            continue
        v = row.get(spec.field)
        lo = p.get(f"{spec.field}_min")
        hi = p.get(f"{spec.field}_max")
        if lo is not None or hi is not None:
            if v is None:
                return False
            if lo is not None and v < lo:
                return False
            if hi is not None and v > hi:
                return False

    # флаги
    if p.get("spike") and not row.get("spike"):
        return False
    if p.get("dens") and not row.get("dens"):
        return False
    if p.get("green") and not (row.get("chg") or 0) > 0:
        return False
    if p.get("red") and not (row.get("chg") or 0) < 0:
        return False
    if p.get("unique") and dup_bases is not None:
        exs = dup_bases.get(row.get("b") or "", set())
        if len(exs) > 1:
            return False

    # срезы
    # биржа: сравниваем без регистра — UI шлёт лейбл («Binance»), а в строках
    # лежит id в нижнем регистре («binanceusdm»); раньше из-за этого фильтр
    # «ex=Binance» вырезал ВСЕ строки (сетка графиков рождалась пустой)
    if p.get("ex"):
        exl = (row.get("exl") or "").lower()
        eid = (row.get("ex") or "").lower()
        wanted = {str(x).lower() for x in p["ex"]}
        if exl not in wanted and eid not in wanted:
            return False
    if p.get("mt") and row.get("mt") != p["mt"]:
        return False
    if p.get("q"):
        q = p["q"]
        if q not in (row.get("s") or "").upper() and q not in (row.get("b") or "").upper():
            return False
    if p.get("q_base") and row.get("q") != p["q_base"]:
        return False
    return True


def apply_filters(
    rows: Iterable[dict],
    params: dict[str, Any],
    dup_bases: Optional[dict[str, set[str]]] = None,
) -> list[dict]:
    return [r for r in rows if _passes(r, params, dup_bases)]


def sort_rows(rows: list[dict], sort: str = "vol", desc: bool = True) -> list[dict]:
    if sort not in SORT_FIELDS:
        sort = "vol"

    def key(r: dict):
        v = r.get(sort)
        # None всегда уходит в конец; знак минус даёт убывание
        return (v is None, -(v or 0) if desc else (v or 0))

    return sorted(rows, key=key)


def base_universe(rows: Iterable[dict]) -> dict[str, set[str]]:
    """base → множество бирж, где он торгуется. Нужно для фильтра «независимые»."""
    out: dict[str, set[str]] = {}
    for r in rows:
        out.setdefault(r.get("b") or "", set()).add(r.get("exl") or r.get("ex") or "")
    return out


def catalog() -> list[dict]:
    """Метаданные фильтров для построения UI."""
    return [
        {
            "field": s.field, "label": s.label, "kind": s.kind, "group": s.group,
            "unit": s.unit, "min": s.default_min, "max": s.default_max,
            "step": s.step, "hint": s.hint,
        }
        for s in FILTER_SPECS
    ]


def sort_catalog() -> dict[str, str]:
    return dict(SORT_FIELDS)


# пресеты — «готовые наборы» под типовые задачи трейдера
PRESETS: dict[str, dict[str, Any]] = {
    "in_play": {
        "label": "Монеты в игре",
        "hint": "Высокий оборот + движение за час",
        "params": {"vol_min": 5_000_000, "r3600_min": 3.0, "sort": "r3600"},
    },
    "breakout": {
        "label": "Пробой / всплеск",
        "hint": "Спайк объёма и рост на 5м",
        "params": {"spike": 1, "r300_min": 1.5, "vol_min": 1_000_000, "sort": "r300"},
    },
    "volatile": {
        "label": "Максимальная волатильность",
        "hint": "NATR топ, для скальпинга",
        "params": {"vol_min": 500_000, "natr_min": 1.0, "sort": "natr"},
    },
    "dump": {
        "label": "Проливы",
        "hint": "Сильно красные за час на объёме",
        "params": {"r3600_max": -5.0, "vol_min": 2_000_000, "sort": "r3600", "desc": 0},
    },
    "squeeze": {
        "label": "Отрицательный фандинг",
        "hint": "Шорты переполнены — топливо для сквиза",
        "params": {"fund_max": -0.0001, "oiusd_min": 1_000_000, "sort": "fund", "desc": 0},
    },
    "thin_unique": {
        "label": "Неэффективности (независимые)",
        "hint": "Монеты только с одной биржи — там живут расхождения",
        "params": {"unique": 1, "vol_min": 100_000, "sort": "rng"},
    },
    "big_densities": {
        "label": "Крупные плотности",
        "hint": "В стакане стоят большие лимитники",
        "params": {"dens": 1, "sort": "vol"},
    },
}


def preset_catalog() -> list[dict]:
    return [{"id": k, **v} for k, v in PRESETS.items()]
