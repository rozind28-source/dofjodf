"""
Движок алертов.

Правило = набор условий (те же поля, что и в фильтрах) + направление
("на пересечении вверх/вниз" или "пока истинно"). Проверка идёт раз в
`interval` по всем строкам снимка; срабатывание запоминается в cooldown-кэше,
чтобы Telegram не лёг от одного волатильного альта.

Доставка: in-process очередь → WebSocket-подписчики (UI-тосты) и, если задан
TELEGRAM_TOKEN/CHAT_ID, сообщение в Telegram.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Iterable, Optional

from .filters import FILTER_SPECS, SORT_FIELDS

log = logging.getLogger("alerts")

RANGE_FIELDS = {s.field for s in FILTER_SPECS if s.kind == "range"}
COMPARATORS = {"gt": ">", "lt": "<", "gte": ">=", "lte": "<="}


@dataclass
class AlertRule:
    id: str
    field: str
    op: str                       # gt | lt | gte | lte
    value: float
    exchange: str = ""            # "" = любая
    market_type: str = ""         # "" = любой
    symbol_contains: str = ""     # подстрока в символе
    mode: str = "crossing"        # crossing | while
    enabled: bool = True
    label: str = ""
    cooldown: float = 60.0
    notify_ui: bool = True
    notify_telegram: bool = True
    created: float = field(default_factory=time.time)
    hits: int = 0

    def matches_scope(self, row: dict) -> bool:
        if self.exchange and row.get("exl") != self.exchange and row.get("ex") != self.exchange:
            return False
        if self.market_type and row.get("mt") != self.market_type:
            return False
        if self.symbol_contains and self.symbol_contains.upper() not in (row.get("s") or "").upper():
            return False
        return True

    def is_true(self, row: dict) -> bool:
        v = row.get(self.field)
        if v is None:
            return False
        if self.op == "gt":
            return v > self.value
        if self.op == "lt":
            return v < self.value
        if self.op == "gte":
            return v >= self.value
        if self.op == "lte":
            return v <= self.value
        return False

    def describe(self) -> str:
        if self.label:
            return self.label
        name = dict(SORT_FIELDS).get(self.field, self.field)
        scope = self.exchange or "все биржи"
        return f"{name} {COMPARATORS.get(self.op, self.op)} {self.value} ({scope})"

    def to_dict(self) -> dict:
        return {
            "id": self.id, "field": self.field, "op": self.op, "value": self.value,
            "exchange": self.exchange, "market_type": self.market_type,
            "symbol_contains": self.symbol_contains, "mode": self.mode,
            "enabled": self.enabled, "label": self.label, "cooldown": self.cooldown,
            "notify_ui": self.notify_ui, "notify_telegram": self.notify_telegram,
            "hits": self.hits, "describe": self.describe(),
        }


@dataclass
class AlertEvent:
    rule_id: str
    text: str
    key: str
    field: str
    value: Optional[float]
    row: dict
    ts: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "rule_id": self.rule_id, "text": self.text, "key": self.key,
            "field": self.field, "value": self.value, "ts": self.ts,
            "row": {k: self.row.get(k) for k in ("k", "s", "exl", "mt", "last", "chg", "vol", "natr", "fund", "oiusd", "spike")},
        }


class AlertEngine:
    def __init__(self, cooldown: float = 60.0,
                 telegram_token: str = "", telegram_chat_id: str = "",
                 history: int = 200) -> None:
        self.rules: dict[str, AlertRule] = {}
        self.default_cooldown = cooldown
        self.telegram_token = telegram_token
        self.telegram_chat_id = telegram_chat_id
        self.events: list[AlertEvent] = []
        self.max_events = history
        self._last_fired: dict[tuple[str, str], float] = {}
        self._prev_state: dict[tuple[str, str], bool] = {}
        self._subscribers: set[asyncio.Queue] = set()
        self._lock = asyncio.Lock()
        self._tg_queue: asyncio.Queue = asyncio.Queue(maxsize=500)

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------
    def add(self, **kw) -> AlertRule:
        rid = kw.pop("id", None) or uuid.uuid4().hex[:10]
        fld = kw.get("field")
        if fld not in RANGE_FIELDS:
            raise ValueError(f"неизвестное поле алерта: {fld!r} (доступно: {sorted(RANGE_FIELDS)})")
        if kw.get("op") not in COMPARATORS:
            raise ValueError(f"неизвестный оператор: {kw.get('op')!r}")
        rule = AlertRule(id=rid, field=fld, op=kw["op"], value=float(kw["value"]),
                         exchange=kw.get("exchange", "") or "",
                         market_type=kw.get("market_type", "") or "",
                         symbol_contains=kw.get("symbol_contains", "") or "",
                         mode=kw.get("mode", "crossing"),
                         enabled=bool(kw.get("enabled", True)),
                         label=kw.get("label", "") or "",
                         cooldown=float(kw.get("cooldown", self.default_cooldown)),
                         notify_ui=bool(kw.get("notify_ui", True)),
                         notify_telegram=bool(kw.get("notify_telegram", True)))
        self.rules[rid] = rule
        return rule

    def update(self, rid: str, **kw) -> Optional[AlertRule]:
        rule = self.rules.get(rid)
        if not rule:
            return None
        for k, v in kw.items():
            if not hasattr(rule, k) or k == "id":
                continue
            if k == "value" or k == "cooldown":
                v = float(v)
            elif k in ("enabled", "notify_ui", "notify_telegram"):
                v = bool(v)
            setattr(rule, k, v)
        return rule

    def remove(self, rid: str) -> bool:
        existed = self.rules.pop(rid, None) is not None
        for k in [k for k in self._last_fired if k[0] == rid]:
            del self._last_fired[k]
        return existed

    def list(self) -> list[dict]:
        return [r.to_dict() for r in self.rules.values()]

    # ------------------------------------------------------------------
    # Подписчики (WebSocket)
    # ------------------------------------------------------------------
    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=256)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subscribers.discard(q)

    async def _publish(self, ev: AlertEvent) -> None:
        payload = ev.to_dict()
        for q in list(self._subscribers):
            if q.full():
                try:
                    q.get_nowait()      # выталкиваем самое старое — live важнее истории
                except asyncio.QueueEmpty:
                    pass
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                pass

    # ------------------------------------------------------------------
    # Проверка
    # ------------------------------------------------------------------
    def _scoped_rows(self, rule: AlertRule, rows: list[dict],
                     groups: Optional[dict]) -> Iterable[dict]:
        """
        Строки, до которых правилу есть дело.

        evaluate() — это O(rules × rows) на каждом такте (20 правил × 6000
        строк = 120 000 проверок области в секунду). Правило с биржей/рынком
        смотрит только свою группу (построена один раз на такт), правило без
        области — как раньше, все строки. На типичном «правило на одну биржу»
        это срезает 90%+ итераций.
        """
        if groups is None:
            return rows
        if rule.market_type and not rule.exchange:
            return groups.get(("", rule.market_type), ())
        if rule.exchange and not rule.market_type:
            out: list[dict] = []
            for mt in ("spot", "swap"):
                out.extend(groups.get((rule.exchange, mt), ()))
            return out
        return groups.get((rule.exchange, rule.market_type), ())

    async def evaluate(self, rows: list[dict]) -> list[AlertEvent]:
        if not self.rules:
            return []
        now = time.time()
        fired: list[AlertEvent] = []
        active = [r for r in self.rules.values() if r.enabled]

        # группы строим только если есть правила с областью (иначе это чистые
        # накладные расходы на такт)
        groups: Optional[dict] = None
        if any(r.exchange or r.market_type for r in active):
            groups = {}
            for row in rows:
                mt = row.get("mt") or ""
                groups.setdefault(("", mt), []).append(row)
                exl = row.get("exl") or ""
                ex = row.get("ex") or ""
                groups.setdefault((exl, mt), []).append(row)
                if ex and ex != exl:
                    groups.setdefault((ex, mt), []).append(row)

        seen: set[tuple[str, str]] = set()
        for rule in active:
            for row in self._scoped_rows(rule, rows, groups):
                if not rule.matches_scope(row):
                    continue
                k = (rule.id, row["k"])
                cond = rule.is_true(row)
                seen.add(k)
                prev = self._prev_state.get(k, False)
                self._prev_state[k] = cond

                if rule.mode == "crossing":
                    # срабатываем только на переходе False -> True
                    if not (cond and not prev):
                        continue
                elif not cond:
                    continue

                last = self._last_fired.get(k, 0.0)
                if now - last < rule.cooldown:
                    continue
                self._last_fired[k] = now
                rule.hits += 1

                ev = AlertEvent(
                    rule_id=rule.id, key=row["k"], field=rule.field,
                    value=row.get(rule.field), row=row,
                    text=self._format(rule, row),
                )
                fired.append(ev)
                self.events.append(ev)
                if len(self.events) > self.max_events:
                    del self.events[: len(self.events) - self.max_events]

        # чистим состояние для правил/символов, которые исчезли из снимка
        if len(self._prev_state) > 20_000:
            self._prev_state = {k: v for k, v in self._prev_state.items() if k in seen}

        for ev in fired:
            rule = self.rules.get(ev.rule_id)
            if rule and rule.notify_ui:
                await self._publish(ev)
            if rule and rule.notify_telegram and self.telegram_token and self.telegram_chat_id:
                await self._tg_queue.put(ev.text)
        return fired

    def _format(self, rule: AlertRule, row: dict) -> str:
        name = dict(SORT_FIELDS).get(rule.field, rule.field)
        val = row.get(rule.field)
        val_s = f"{val:g}" if isinstance(val, (int, float)) else "—"
        unit = "%" if rule.field in ("chg", "rng", "natr", "r60", "r300", "r900", "r3600", "r14400") else ""
        if rule.field in ("vol", "oiusd", "d1m", "cvd"):
            unit = "$"
        spike = row.get("spike")
        extra = f" | спайк {spike['kind']} x{spike['ratio']}" if spike else ""
        return (f"🔔 {row.get('exl')} {row.get('s')} [{row.get('mt')}] — "
                f"{name} = {val_s}{unit} ({COMPARATORS.get(rule.op, rule.op)} {rule.value}{unit})"
                f" | цена {row.get('last')}{extra}")

    def recent(self, limit: int = 50) -> list[dict]:
        return [e.to_dict() for e in self.events[-limit:][::-1]]

    # ------------------------------------------------------------------
    # Telegram
    # ------------------------------------------------------------------
    async def telegram_worker(self) -> None:
        if not (self.telegram_token and self.telegram_chat_id):
            log.info("telegram не настроен — алерты идут только в UI")
            return
        loop = asyncio.get_running_loop()
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        while True:
            text = await self._tg_queue.get()
            try:
                await loop.run_in_executor(None, self._tg_send, url, text)
            except Exception as e:  # noqa: BLE001
                log.warning("telegram send failed: %s", str(e)[:160])

    def _tg_send(self, url: str, text: str) -> None:
        data = urllib.parse.urlencode({
            "chat_id": self.telegram_chat_id, "text": text,
            "disable_web_page_preview": "true",
        }).encode()
        req = urllib.request.Request(url, data=data, headers={"User-Agent": "crypto-screener/0.1"})
        with urllib.request.urlopen(req, timeout=10) as r:  # noqa: S310
            json.loads(r.read().decode())


ENGINE: Optional[AlertEngine] = None


def get_engine() -> AlertEngine:
    global ENGINE
    if ENGINE is None:
        raise RuntimeError("AlertEngine не инициализирован")
    return ENGINE


def init_engine(**kw) -> AlertEngine:
    global ENGINE
    ENGINE = AlertEngine(**kw)
    return ENGINE
