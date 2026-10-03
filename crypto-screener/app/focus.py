"""
Фокус-режим: «стримим только то, что реально смотрим».

Проблема, которую решает модуль
-------------------------------
Полный профиль — это 12 коллекторов × (top_n=150…300 тикер-стримов +
books=20…80 стаканов + trades=120 лент сделок) ≈ 10 500 инструментов в
хранилище. На каждый WS-пуш (раз в секунду) приходится сериализация всего
этого объёма, а /api/grid заказывает свечи у биржи для ячеек, которые
физически не входят в горячий набор. Отсюда «загрузка свечей…» на десятки
секунд и 2-3 ГБ памяти.

Что делает фокус-режим
----------------------
1. Ровно ОДНА биржа и ОДИН рынок (как в оригинальном cryptoscreener.app:
   чипс биржи + тумблер «Фьючерсы / Спот»).
2. Раз в `interval` секунд (по умолчанию 15) пересчитывается отбор:
   top-`limit` монет (по умолчанию 50) ПО ТЕКУЩЕМУ ФИЛЬТРУ пользователя —
   волатильность, объём, дельта, фандинг, что угодно из FILTER_SPECS.
   При включении/смене фильтра подписки сужаются не дожидаясь отбора:
   сначала применяется мгновенный предварительный отбор по тикерам.
3. На эти монеты заводятся WS-потоки, остальные гасятся через un_watch_*
   (подписка снимается и на стороне биржи, иначе список подписок растёт).
4. Прочие биржи ставятся на паузу: тяжёлые потоки закрываются, опрос
   тикеров замедляется. Таблица скринера при этом продолжает показывать
   ВСЮ выбранную биржу — REST-срез тикеров стоит один запрос на биржу.

Конвейер отбора (важно)
-----------------------
Часть метрик (NATR, r60/r300/r900) считается из 1m-свечей, а свечи мы держим
только для горячего набора. Ранжировать «по волатильности» весь рынок, не
скачав свечи, физически нельзя. Поэтому отбор трёхступенчатый:

  ступень 0 (миг)     — предварительный отбор: подписки сужаются СРАЗУ по
                        тикерам (дешёвая часть фильтра + прокси-метрика:
                        NATR→диапазон 24ч, Δ→изменение 24ч), ноль запросов
                        к бирже — «стримит всё» не существует даже в первые
                        секунды после включения фокуса;
  уровень 1 (дешёвый) — пул кандидатов: вселенная предварительно режется
                        дешёвой частью фильтра (vol_min и т.п.), ранжируется
                        прокси-метрикой (PROXY_SORT), limit*3…limit*12 штук;
  уровень 2 (точный)  — кандидатам добираем 1m-свечи и уже по ним применяем
                        полный фильтр и финальную сортировку.

Точный отбор заменяет предварительный сразу, как готов (strict-ротация), а
пока свечи добираются, пустой промежуточный результат НЕ применяется — иначе
подписки скакали бы в полный горячий набор и обратно каждые 15 секунд.

Если фильтр очень узкий и кандидатов не хватило, пул расширяется ступенями
до FOCUS_CANDIDATES_MAX, а в ответе появляется note — UI честно пишет
«фильтру соответствует K монет».

Режим strict
------------
Отбор жёсткий: выпал из топ-50 → потоки гасятся сразу (без «липкого» набора).
Компенсация — частая перепроверка (15 c) и то, что /api/grid берёт ячейки
из текущего отбора, а не из случайного топа по объёму.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from . import filters as F
from .config import SETTINGS, Settings
from .state import STORE

log = logging.getLogger("focus")

# Поля, для которых нужны 1m-свечи. Всё остальное (объём, изменение 24ч,
# цена, диапазон 24ч, фандинг, OI, дисбаланс, дельта) приходит из тикеров/
# потоков и доступно для всей биржи сразу.
# Здесь НЕ должно быть "rng" и "spike":
#   * rng (диапазон 24ч) считается из high/low тикера (_recalc_24h) — он есть
#     у всей вселенной бесплатно. Ошибочное присутствие rng в этом наборе
#     заставляло отбор по диапазону качать свечи всему пулу кандидатов;
#   * spike рождается из ленты сделок (apply_ohlcv спайки не создаёт) — добор
#     свечей фильтру по спайкам помочь не может в принципе, это были сотни
#     лишних REST-запросов бирже.
OHLCV_FIELDS = frozenset({"natr", "r60", "r300", "r900", "r3600", "r14400"})

# Дешёвые прокси для ранжирования пула кандидатов, когда запрошена «дорогая»
# метрика. Пул — это ответ на вопрос «кто вероятнее всего пройдёт фильтр», и
# ранжировать его по объёму для фильтра по волатильности бессмысленно: топ-150
# по объёму — мейджоры с низким NATR, фильтр не набирает limit монет, пул
# расширяется до 600 и свечи качаются всем подряд. Прокси берётся из тикеров
# (бесплатно для всей вселенной):
#   natr → rng  диапазон 24ч хорошо коррелирует с волатильностью 1m-свечей;
#   r*   → chg  монеты с экстремальным изменением за 24ч почти наверняка
#               содержат экстремальные часовые/5-минутные движения.
PROXY_SORT = {"natr": "rng", "r60": "chg", "r300": "chg", "r900": "chg",
              "r3600": "chg", "r14400": "chg"}

# Поля фильтров, которые есть у ВСЕЙ вселенной из одного REST-среза тикеров
# (+ bulk funding/OI). Их можно применить ДО подкачки свечей: это режет пул
# кандидатов (например vol_min=500k из пресета «Максимальная волатильность»
# отсекает мёртвые пары) и экономит запросы свечей.
CHEAP_RANGE_FIELDS = frozenset({"vol", "chg", "rng", "last", "tr",
                                "fund", "oi", "oiusd"})
CHEAP_FLAGS = frozenset({"green", "red"})


def cheap_params(params: dict) -> dict:
    """Проекция параметров фильтра на «дешёвые» поля (доступны без свечей)."""
    out = {}
    for k, v in params.items():
        base = k[:-4] if k.endswith(("_min", "_max")) else k
        if base in CHEAP_RANGE_FIELDS or k in CHEAP_FLAGS or k in ("q", "q_base"):
            out[k] = v
    return out

# Насколько пул кандидатов больше финального отбора
FOCUS_CANDIDATES_MULT = 3
FOCUS_CANDIDATES_MAX = 600
# Ступени расширения пула: сначала пробуем дёшево, расширяем только если
# фильтр не набирает нужное число монет
POOL_STEPS = (3, 6, 12)
# Через сколько секунд свечи кандидата считаются протухшими
CANDLES_TTL = 60.0
# Сколько свечей добираем ОДНИМ проходом отбора. Замер на живом Binance:
# один fetch_ohlcv("1m", limit=400) занимает ~1.2 c, то есть 90 кандидатов -
# это 9 c даже при 12 параллельных запросах. POST /api/focus держал бы
# соединение всё это время (фронтенд обрывает запрос на 12 c). Поэтому добор
# порционный: за один проход не больше FOCUS_OHLCV_MAX символов, остальные -
# в следующих (они идут каждые interval секунд, а свечи кэшируются на
# CANDLES_TTL). Первый отбор приезжает за ~2 c и помечается «уточняется»,
# через 15 c он уже точный.
FOCUS_OHLCV_MAX = 60
# Сколько секунд первый (интерактивный) пересчёт вправе тратить на свечи.
# Дальше отдаём best-effort и помечаем «уточняется»: фоновые проходы
# (каждые interval секунд) добирают остаток, а свечи кэшируются на CANDLES_TTL.
FOCUS_CANDLE_BUDGET = 5.0
# Замедление опроса тикеров у «чужих» бирж в фокус-режиме
PAUSED_TICKER_REFRESH = 120.0


@dataclass
class FocusSpec:
    """Параметры фокус-режима. Меняются из UI без перезапуска сервера."""

    ex: str = ""                 # лейбл биржи («Binance») или ccxt id («binanceusdm»)
    mt: str = "swap"             # один рынок: spot | swap
    limit: int = 50              # сколько монет стримим
    interval: float = 15.0       # как часто перепроверяем отбор, сек
    params: dict = field(default_factory=dict)   # фильтры (формат filters.parse_params)
    sort: str = "vol"
    desc: bool = True
    pause_others: bool = True    # гасить потоки прочих бирж
    # Одна монета = одна строка отбора. Без этого в топ-50 Binance Futures
    # попадают BTC/USDT:USDT И BTC/USDC:USDT, ETH/USDT:USDT И ETH/USDC:USDC
    # (замер на живых данных: в первых 10 строках было 2 BTC и 2 ETH), то есть
    # реальное разнообразие выборки - 40 монет вместо 50, а сетка графиков
    # показывает одну и ту же монету дважды.
    dedupe_by_base: bool = True


def clamp_limit(v: Any, default: int = 50, lo: int = 5, hi: int = 200) -> int:
    try:
        n = int(v)
    except (TypeError, ValueError):
        return default
    return max(lo, min(n, hi))


def clamp_interval(v: Any, default: float = 15.0, lo: float = 5.0, hi: float = 300.0) -> float:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return default
    return max(lo, min(x, hi))


def dedupe_by_base(rows: list[dict]) -> list[dict]:
    """
    Оставляет одну строку на базовый актив (первую по текущему порядку).

    Порядок входа уже отсортирован «дешёвой» метрикой (обычно объём), поэтому
    остаётся наиболее ликвидный инструмент монеты - USDT-пара, а не USDC.
    """
    seen: set = set()
    out: list[dict] = []
    for row in rows:
        base = row.get("b") or row.get("s") or ""
        if base in seen:
            continue
        seen.add(base)
        out.append(row)
    return out


def needs_ohlcv(params: dict, sort: str) -> bool:
    """Требуется ли для отбора подкачка свечей (дорогой уровень 2)."""
    if sort in OHLCV_FIELDS:
        return True
    for k in params:
        base = k[:-4] if k.endswith(("_min", "_max")) else k
        if base in OHLCV_FIELDS:
            return True
    return False


class _StaleSelection(Exception):
    """Пересчёт устарел: параметры сменились, пока мы добирали свечи."""


class FocusManager:
    """
    Владелец фокус-набора.

    Живёт в app.state.focus, один на процесс. UI дёргает update()/disable(),
    внутренний цикл сам пересчитывает отбор раз в interval секунд и
    раскладывает результат по коллекторам.
    """

    def __init__(self, settings: Settings = SETTINGS) -> None:
        self.settings = settings
        self.hub = None                       # CollectorHub, назначается в lifespan
        self.spec = FocusSpec()
        self.enabled = False
        self.virtual = False                  # True, когда коллектора нет (replay/demo)
        self.symbols: list[str] = []          # итоговый отбор (ccxt-символы)
        self.keys: list[str] = []             # те же монеты в терминах ключей STORE
        self.key_set: set[str] = set()
        self.ranked_at: float = 0.0
        self.next_at: float = 0.0
        self.last_ms: int = 0
        self.last_error: str = ""
        self.note: str = ""
        self.enriching = False                # часть кандидатов ещё без свечей
        self.pending = 0
        # предварительный отбор: подписки уже сужены по тикерным данным,
        # точный отбор по свечам ещё считается (см. _apply_provisional)
        self.provisional = False
        self._need_provisional = False
        self._last_sig = None       # повторное включение — всегда с предотбором
        # «поколение» параметров. Отбор по «дорогим» метрикам длится секунды
        # (свечи с биржи ~1.2 c/запрос), и за это время пользователь успевает
        # выключить фокус или сменить биржу. Без проверки поколения такой
        # устаревший пересчёт ПОСЛЕ disable() снова ставил биржи на паузу и
        # возвращал фокус-набор (поймано живым замером: через 8 c после
        # выключения Bybit/MEXC опять оказывались paused).
        self._gen = 0
        self.stats: dict = {"universe": 0, "pool": 0, "matched": 0, "rotations": 0,
                            "changed": 0, "candle_fetches": 0}
        self._task: Optional[asyncio.Task] = None
        self._change = asyncio.Event()
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Управление
    # ------------------------------------------------------------------
    def attach_hub(self, hub) -> None:
        self.hub = hub

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop(), name="focus-loop")

    async def stop(self) -> None:
        """
        Остановка цикла. Важно НЕ вызывать disable(): при shutdown коллекторы
        уже закрываются, и повторный un_watch_* на мёртвом соединении только
        добавляет ошибок в лог. Состояние фокуса остаётся как есть - его
        видно в /api/focus до самого выхода из lifespan.
        """
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
        self._task = None

    async def update(self, budget: float = FOCUS_CANDLE_BUDGET, **kw) -> dict:
        """Включить/перенастроить фокус-режим и СРАЗУ пересчитать отбор."""
        self._gen += 1        # устаревший пересчёт (если он ещё идёт) отменяется
        spec = self.spec
        if "ex" in kw and kw["ex"] is not None:
            spec.ex = str(kw["ex"]).strip()
        if "mt" in kw and kw["mt"] in ("spot", "swap"):
            spec.mt = str(kw["mt"])
        if "limit" in kw:
            spec.limit = clamp_limit(kw["limit"], spec.limit)
        if "interval" in kw:
            spec.interval = clamp_interval(kw["interval"], spec.interval)
        if "sort" in kw and kw["sort"] in F.SORT_FIELDS:
            spec.sort = str(kw["sort"])
        if "desc" in kw:
            spec.desc = str(kw["desc"]) not in ("0", "false", "no", False)
        if "pause_others" in kw:
            spec.pause_others = bool(kw["pause_others"])
        if "dedupe_by_base" in kw:
            spec.dedupe_by_base = bool(kw["dedupe_by_base"])
        if "params" in kw and isinstance(kw["params"], dict):
            spec.params = dict(kw["params"])

        self.enabled = True
        # биржа не выбрана → берём первую подключённую, у которой есть рынок mt
        if not spec.ex:
            spec.ex = self._default_exchange()
        # подписки должны сузиться СРАЗУ, а не когда доедут свечи сотен
        # кандидатов: ближайший refresh() начнёт с предварительного отбора.
        # Но если параметры отбора не менялись (подкрутили лишь interval или
        # pause_others), предотбор пропускаем — иначе точный набор каждый раз
        # дёргался бы туда-сюда (cheap → precise) на ровном месте.
        sig = self._sig()
        self._need_provisional = not self.keys or sig != self._last_sig
        self._last_sig = sig
        self._change.set()
        await self.refresh(budget=budget)
        return self.state()

    async def disable(self) -> dict:
        """Выключить: коллекторы возвращаются к обычному горячему набору."""
        self._gen += 1        # отменить пересчёт, который ещё не завершился
        self.enabled = False
        self.symbols, self.keys, self.key_set = [], [], set()
        self.note, self.last_error = "", ""
        self.next_at = 0.0
        self.provisional = False
        self._need_provisional = False
        self.enriching = False
        self.pending = 0
        for c in self._collectors():
            c.set_focus(None)
            c.set_ohlcv_pool(None)
            c.resume()
        # статус «focus» убираем: иначе в шапке/диагностике остаётся биржа,
        # которой уже нет, с ненулевым числом инструментов
        STORE.exchange_status.pop("focus", None)
        log.info("фокус-режим выключен - коллекторы вернулись к горячему набору")
        return self.state()

    def _sig(self) -> tuple:
        """Подпись параметров, влияющих на СОСТАВ отбора (interval не входит)."""
        spec = self.spec
        items = tuple(sorted(
            (k, tuple(v) if isinstance(v, list) else v)
            for k, v in spec.params.items()))
        return (spec.ex, spec.mt, spec.limit, spec.sort, spec.desc,
                spec.dedupe_by_base, items)

    def _default_exchange(self) -> str:
        for e in self.settings.exchanges:
            if e.market == self.spec.mt:
                return e.label
        return self.settings.exchanges[0].label if self.settings.exchanges else ""

    def _collectors(self) -> list:
        return list(self.hub.collectors) if self.hub is not None else []

    def target_collector(self):
        """Коллектор выбранной биржи и рынка (или None, если его нет)."""
        if self.hub is None or not self.spec.ex:
            return None
        for c in self.hub.collectors:
            if c.cfg.market != self.spec.mt:
                continue
            if self.spec.ex in (c.cfg.label, c.cfg.id):
                return c
        return None

    # ------------------------------------------------------------------
    # Состояние для API/UI
    # ------------------------------------------------------------------
    def state(self) -> dict:
        age = (time.time() - self.ranked_at) if self.ranked_at else None
        return {
            "enabled": self.enabled,
            "virtual": self.virtual,
            "ex": self.spec.ex,
            "mt": self.spec.mt,
            "limit": self.spec.limit,
            "interval": self.spec.interval,
            "sort": self.spec.sort,
            "desc": self.spec.desc,
            "pause_others": self.spec.pause_others,
            "params": dict(self.spec.params),
            "symbols": list(self.symbols),
            "keys": list(self.keys),
            "count": len(self.keys),
            "ranked_at": self.ranked_at or None,
            "age": round(age, 1) if age is not None else None,
            "next_in": round(max(0.0, self.next_at - time.time()), 1) if self.enabled else None,
            "last_ms": self.last_ms,
            "error": self.last_error,
            "note": self.note,
            "enriching": self.enriching,
            "provisional": self.provisional,
            "pending": self.pending,
            "stats": dict(self.stats),
        }

    # ------------------------------------------------------------------
    # Отбор
    # ------------------------------------------------------------------
    async def _loop(self) -> None:
        """Перепроверка отбора раз в interval секунд + мгновенно по запросу UI."""
        while True:
            try:
                if self.enabled:
                    wait = max(0.5, self.next_at - time.time())
                else:
                    wait = 1.0
                try:
                    await asyncio.wait_for(self._change.wait(), timeout=wait)
                except asyncio.TimeoutError:
                    pass
                self._change.clear()
                if self.enabled:
                    await self.refresh()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                self.last_error = f"{type(e).__name__}: {str(e)[:160]}"
                log.warning("focus loop: %s", self.last_error)
                await asyncio.sleep(1.0)

    async def refresh(self, budget: Optional[float] = None) -> None:
        """Один цикл отбора. Вызывается и из цикла, и сразу при смене фильтра."""
        async with self._lock:
            t0 = time.time()
            gen = self._gen
            self.next_at = t0 + self.spec.interval
            # проверка ПОСЛЕ взятия блокировки: disable()/update() могли пройти,
            # пока мы ждали завершения предыдущего (долгого) пересчёта
            if not self.enabled or gen != self._gen:
                return
            col = self.target_collector()
            self.virtual = col is None
            # Предварительный отбор: при включении/смене параметров сужаем
            # подписки НЕМЕДЛЕННО по тикерным данным (доли секунды, ноль
            # запросов к бирже), точный отбор по свечам догонит следом.
            # Без этого биржа продолжала стримить полный горячий набор
            # (300 монет) всё время, пока шли сотни fetch_ohlcv.
            if self._need_provisional:
                self._need_provisional = False
                if col is not None:
                    self._apply_provisional(col, gen)
            try:
                res = await self._rank(col, gen, t0,
                                       FOCUS_CANDLE_BUDGET if budget is None else budget)
            except _StaleSelection:
                log.info("[фокус] пересчёт отменён: параметры сменились во время добора свечей")
                return
            except Exception as e:  # noqa: BLE001
                self.last_error = f"{type(e).__name__}: {str(e)[:160]}"
                log.warning("[%s %s] отбор не выполнен: %s", self.spec.ex, self.spec.mt,
                            self.last_error)
                # отбор не удался → НЕ трогаем текущие подписки: пусть лучше
                # останется прошлый набор, чем пустая сетка графиков
                self.last_ms = int((time.time() - t0) * 1000)
                self.ranked_at = time.time()
                return

            self.last_error = ""
            if not res["keys"] and res.get("enriching") and self.keys:
                # Свечи пула ещё не доехали, и «дорогой» фильтр пока некому
                # пройти. Применять пустой отбор нельзя: set_focus([]) снимает
                # фокус вовсе, и коллектор возвращается к ПОЛНОМУ горячему
                # набору — получаются скачки подписок 300→N→300 каждые 15 с.
                # Держим текущий (предварительный или прошлый) набор.
                self.note = res.get("note", "")
                self.enriching = True
                self.pending = int(res.get("pending", 0))
                self.ranked_at = time.time()
                self.last_ms = int((time.time() - t0) * 1000)
                self.stats.update({
                    "universe": res["universe"], "pool": res["pool"], "matched": 0,
                    "rotations": self.stats["rotations"] + 1,
                    "candle_fetches": self.stats["candle_fetches"] + res.get("fetched", 0),
                })
                log.info("[фокус] фильтр пока не набрал монет (свечи добираются) — "
                         "держим текущие %d подписок", len(self.keys))
                return
            new_syms, new_keys = res["symbols"], res["keys"]
            changed = set(new_keys) != set(self.keys)
            self.provisional = False
            self.symbols, self.keys = new_syms, new_keys
            self.key_set = set(new_keys)
            self.ranked_at = time.time()
            self.last_ms = int((time.time() - t0) * 1000)
            self.note = res.get("note", "")
            self.enriching = bool(res.get("enriching"))
            self.pending = int(res.get("pending", 0))
            self.stats.update({
                "universe": res["universe"], "pool": res["pool"],
                "matched": res["matched"], "rotations": self.stats["rotations"] + 1,
                "changed": self.stats["changed"] + (1 if changed else 0),
                "candle_fetches": self.stats["candle_fetches"] + res.get("fetched", 0),
            })
            if gen != self._gen or not self.enabled:
                # пока считали, фокус выключили или перенастроили — применять
                # устаревший набор нельзя (иначе биржи снова уйдут на паузу).
                # Именно return, а не raise: мы уже вне try, исключение ушло бы
                # наружу в HTTP-обработчик.
                log.info("[фокус] устаревший отбор выброшен (поколение %s -> %s)",
                         gen, self._gen)
                return
            self._apply(col, res)
            if changed:
                log.info("[фокус %s/%s] отбор обновлён: %d монет за %d мс (вселенная %d, пул %d)%s",
                         self.spec.ex, self.spec.mt, len(new_keys), self.last_ms,
                         res["universe"], res["pool"], f" — {self.note}" if self.note else "")

    def _apply(self, col, res: dict) -> None:
        """Раскладываем результат по коллекторам."""
        others_paused = self.spec.pause_others and col is not None
        for c in self._collectors():
            if col is not None and c is col:
                c.set_focus(res["symbols"], pool=res.get("pool_symbols") or res["symbols"])
                c.resume()
            elif others_paused:
                c.set_focus(None)
                c.set_ohlcv_pool(None)
                c.pause()
            else:
                c.set_focus(None)
                c.set_ohlcv_pool(None)
                c.resume()
        # виртуальный режим (replay/демо): коллекторов нет, отбор всё равно
        # применяется к WS-пушу и к /api/grid
        STORE.set_status("focus", state="online", symbols=len(self.keys),
                         total=len(self.keys), hot=len(self.keys),
                         ex=self.spec.ex, mt=self.spec.mt,
                         virtual=self.virtual, interval=self.spec.interval)

    def _pool_sort(self) -> str:
        """Какое дешёвое поле ранжирует пул кандидатов вместо «дорогого»."""
        spec = self.spec
        if spec.sort in OHLCV_FIELDS:
            return PROXY_SORT.get(spec.sort, "vol")
        for k in sorted(spec.params):
            base = k[:-4] if k.endswith(("_min", "_max")) else k
            if base in OHLCV_FIELDS:
                return PROXY_SORT.get(base, "vol")
        return spec.sort if spec.sort in F.SORT_FIELDS else "vol"

    def _apply_provisional(self, col, gen: int) -> None:
        """
        Мгновенный предварительный отбор: только тикерные данные, ноль запросов.

        Точный отбор по «дорогим» метрикам (NATR, r3600, ...) требует свечей
        на пул кандидатов — это секунды и сотни REST-запросов. Раньше всё это
        время биржа стримила полный горячий набор: пользователь включал
        «Максимальную волатильность» и наблюдал «подписывается на все монеты».
        Теперь вселенная сразу ранжируется дешёвой прокси-метрикой (для
        волатильности — диапазон 24ч из тикера) с дешёвой частью фильтра, и
        подписки сужаются до limit монет в первый же момент; фоновый проход по
        свечам затем уточняет состав (strict-ротация, как и предусмотрено).
        """
        spec = self.spec
        if not needs_ohlcv(spec.params, spec.sort):
            return          # фильтр и так дешёвый — обычный отбор мгновенный
        rows = self._universe_rows(col)
        if not rows:
            return
        universe = len(rows)
        rows = F.apply_filters(rows, cheap_params(spec.params))
        pool_n = min(max(spec.limit * POOL_STEPS[0], 30), FOCUS_CANDIDATES_MAX)
        cand = F.sort_rows(rows, self._pool_sort(), spec.desc)[:pool_n]
        sel = dedupe_by_base(cand) if spec.dedupe_by_base else list(cand)
        sel = sel[: spec.limit]
        if not sel or gen != self._gen or not self.enabled:
            return
        res = {"symbols": [r["s"] for r in sel], "keys": [r["k"] for r in sel],
               "universe": universe, "pool": len(cand), "matched": len(sel),
               "fetched": 0, "note": "", "pool_symbols": [r["s"] for r in cand],
               "enriching": True, "pending": len(cand)}
        self.symbols, self.keys = res["symbols"], res["keys"]
        self.key_set = set(self.keys)
        self.provisional = True
        self.enriching = True
        self.pending = len(cand)
        self.ranked_at = time.time()
        self.stats.update({"universe": universe, "pool": len(cand),
                           "matched": len(sel),
                           "rotations": self.stats["rotations"] + 1})
        self._apply(col, res)
        log.info("[фокус %s/%s] предварительный отбор: %d монет застримлено сразу "
                 "(точный отбор по свечам ещё считается)",
                 spec.ex, spec.mt, len(self.keys))

    async def _rank(self, col, gen: Optional[int] = None, t0: float = 0.0,
                    budget: float = FOCUS_CANDLE_BUDGET) -> dict:
        """Двухуровневый отбор: тикеры → (свечи) → фильтр → top-limit."""
        spec = self.spec
        rows = self._universe_rows(col)
        universe = len(rows)
        if not rows:
            return {"symbols": [], "keys": [], "universe": 0, "pool": 0, "matched": 0,
                    "fetched": 0, "note": "нет данных по бирже — она не подключилась?"}

        want_ohlcv = needs_ohlcv(spec.params, spec.sort)
        # Дешёвая часть фильтра (vol_min, chg, fund, ...) применяется ко ВСЕЙ
        # вселенной до подкачки свечей: эти поля есть в тикерах бесплатно, а
        # пул кандидатов становится заметно меньше → меньше fetch_ohlcv.
        cheap = cheap_params(spec.params)
        rows = F.apply_filters(rows, cheap)
        # «дешёвая» сортировка для первичного пула: если запрошенное поле есть
        # в тикерах — ранжируем по нему, иначе по прокси (см. PROXY_SORT)
        cheap_sort = self._pool_sort()

        pool_mult_idx = 0
        fetched = 0
        note = ""
        enriching = False
        pending = 0
        while True:
            mult = POOL_STEPS[min(pool_mult_idx, len(POOL_STEPS) - 1)]
            pool_n = min(max(spec.limit * mult, 30), FOCUS_CANDIDATES_MAX)
            cand = F.sort_rows(rows, cheap_sort, spec.desc)[:pool_n]

            if want_ohlcv and col is not None:
                self._check_gen(gen)
                left = budget - (time.time() - t0) if t0 else None
                got, pending = await self._ensure_candles(col, cand, time_left=left)
                fetched += got
                enriching = pending > 0
                if enriching:
                    # часть пула ещё без свечей: ранжирование по NATR для них
                    # невозможно, поэтому честно пишем об этом в UI, а не
                    # делаем вид, что отбор окончательный
                    note = (f"у {pending} монет свечи ещё не загружены - "
                            f"отбор уточнится в следующие проходы")
                # после подкачки свечей метрики изменились → пересобираем строки
                rows = F.apply_filters(self._universe_rows(col), cheap)
                cand = F.sort_rows(rows, cheap_sort, spec.desc)[:pool_n]

            sel = F.apply_filters(cand, spec.params)
            if spec.dedupe_by_base:
                sel = dedupe_by_base(sel)
            sel = F.sort_rows(sel, spec.sort, spec.desc)
            matched = len(sel)
            if matched >= spec.limit or pool_n >= FOCUS_CANDIDATES_MAX \
                    or pool_mult_idx >= len(POOL_STEPS) - 1:
                if matched < spec.limit:
                    extra = (f"фильтру соответствует {matched} монет "
                             f"(просмотрено {pool_n} из {universe})")
                    note = f"{note}; {extra}" if note else extra
                break
            pool_mult_idx += 1
            # расширение пула - это ещё один заход за свечами (замер: +6 c на
            # Binance). Если бюджет интерактивного пересчёта исчерпан, лучше
            # отдать best-effort сейчас и доделать в фоне, чем держать запрос
            # пользователя открытым вдвое дольше.
            if t0 and (time.time() - t0) > budget:
                enriching = True
                note = (note + "; " if note else "") + \
                    "бюджет добора свечей исчерпан - отбор уточняется в фоне"
                break

        sel = sel[: spec.limit]
        symbols = [r["s"] for r in sel]
        keys = [r["k"] for r in sel]
        pool_symbols = [r["s"] for r in cand]
        return {"symbols": symbols, "keys": keys, "universe": universe,
                "pool": len(cand), "matched": matched, "fetched": fetched,
                "note": note, "pool_symbols": pool_symbols,
                "enriching": enriching, "pending": pending}

    def _universe_rows(self, col) -> list[dict]:
        """
        Все инструменты выбранной биржи и рынка.

        Данные берутся из STORE: тикер-цикл коллектора обновляет их раз в
        ticker_refresh секунд одним REST-запросом на всю биржу. Отдельный
        fetch_tickers здесь был бы лишним дублем того же запроса.
        """
        want_label = self.spec.ex
        want_id = col.cfg.id if col is not None else ""
        mt = self.spec.mt
        out = []
        for st in STORE.all():
            if st.market_type != mt or st.last <= 0:
                continue
            if st.exchange_label == want_label or (want_id and st.exchange == want_id):
                out.append(st.to_row())
        return out

    def _check_gen(self, gen: Optional[int]) -> None:
        """Прервать устаревший пересчёт (фокус выключили/перенастроили)."""
        if gen is not None and (gen != self._gen or not self.enabled):
            raise _StaleSelection()

    async def _ensure_candles(self, col, cand: list[dict],
                              time_left: Optional[float] = None) -> tuple[int, int]:
        """
        Добираем 1m-свечи кандидатам, у которых их нет или они протухли.

        Без этого фильтр «NATR ≥ 1%» ранжировал бы монеты по заглушке None
        (to_row отдаёт None, а фильтр такие строки отбрасывает) — то есть
        «отбор по волатильности» молча превращался бы в «отбор по объёму».

        Возвращает (сколько обновилось, сколько осталось без свечей). Второе
        число ненулевое, когда сработал предел FOCUS_OHLCV_MAX ИЛИ загрузка
        не удалась: запрос свечей медленный (~1.2 c на Binance), и выгружать
        весь пул одним проходом означало бы держать HTTP-запрос пользователя
        открытым по 9 секунд. Неудачи считаются «без свечей» намеренно: пока
        свечей нет, «дорогой» фильтр некому пройти, и отбор обязан остаться
        предварительным (иначе пустой результат сбросил бы подписки в полный
        горячий набор).
        """
        now = time.time()
        todo = []
        for row in cand:
            st = STORE.get(row["k"])
            if st is None:
                continue
            if not st.ohlcv or (now - getattr(st, "ohlcv_ts", 0.0)) > CANDLES_TTL:
                todo.append(row["s"])
        if not todo:
            return 0, 0
        cap = FOCUS_OHLCV_MAX
        if time_left is not None and time_left <= 0:
            cap = 0                      # бюджет исчерпан → добор в фоне
        batch = todo[:cap]
        got = await col.ensure_ohlcv(batch, limit=self.settings.ohlcv_limit) if batch else 0
        return got, len(todo) - got


# ----------------------------------------------------------------------
# Синглтон (один фокус на процесс — как и один «активный экран» у пользователя)
# ----------------------------------------------------------------------
_FOCUS: Optional[FocusManager] = None


def init_focus(settings: Settings = SETTINGS, hub=None) -> FocusManager:
    global _FOCUS
    _FOCUS = FocusManager(settings)
    if hub is not None:
        _FOCUS.attach_hub(hub)
    return _FOCUS


def get_focus() -> FocusManager:
    global _FOCUS
    if _FOCUS is None:
        _FOCUS = FocusManager(SETTINGS)
    return _FOCUS
