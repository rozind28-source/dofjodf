"""
Хранилище состояния скринера.

Один процесс = одна in-memory база. Всё, что нужно фронтенду, берётся отсюда.
Для продакшена этот слой заменяется на Redis (см. README → «Масштабирование»),
интерфейс класса при этом не меняется.
"""
from __future__ import annotations

import threading
import time
from typing import Iterable, Optional

from .metrics import SymbolState, fmt_compact


class Store:
    def __init__(self, history_window: int = 900,
                 spike_vol_ratio: float = 3.0, spike_tr_ratio: float = 3.0) -> None:
        self.history_window = history_window
        self.spike_vol_ratio = spike_vol_ratio
        self.spike_tr_ratio = spike_tr_ratio
        self._symbols: dict[str, SymbolState] = {}
        self._lock = threading.RLock()
        self.started_at = time.time()
        self.stats: dict[str, float] = {
            "ws_messages": 0,
            "trades": 0,
            "book_updates": 0,
            "ticker_updates": 0,
            "rest_calls": 0,
            "errors": 0,
        }
        self.exchange_status: dict[str, dict] = {}
        # кэш overview(): шапка пересчитывается на каждый WS-пуш каждому
        # клиенту + REST; итерация по всем символам не обязана происходить
        # чаще раза в ttl (см. overview())
        self._ov_cache: Optional[tuple[float, dict]] = None

    # ------------------------------------------------------------------
    def bump(self, name: str, n: int = 1) -> None:
        self.stats[name] = self.stats.get(name, 0) + n

    def set_status(self, exchange: str, **kw) -> None:
        st = self.exchange_status.setdefault(exchange, {"state": "init", "symbols": 0, "books": 0, "latency_ms": None})
        st.update(kw)
        st["ts"] = time.time()

    # ------------------------------------------------------------------
    def get_or_create(self, exchange: str, exchange_label: str, market_type: str,
                      symbol: str, base: str, quote: str) -> SymbolState:
        key = f"{exchange}:{symbol}"
        with self._lock:
            st = self._symbols.get(key)
            if st is None:
                st = SymbolState(exchange, exchange_label, market_type, symbol, base, quote,
                                 history_window=self.history_window,
                                 spike_vol_ratio=self.spike_vol_ratio,
                                 spike_tr_ratio=self.spike_tr_ratio)
                self._symbols[key] = st
            return st

    def get(self, key: str) -> Optional[SymbolState]:
        return self._symbols.get(key)

    def all(self) -> list[SymbolState]:
        with self._lock:
            return list(self._symbols.values())

    def keys(self) -> list[str]:
        with self._lock:
            return list(self._symbols.keys())

    def purge(self, exchange: str, keep: Iterable[str]) -> int:
        """Удаляет инструменты биржи, которых больше нет в топе (делистинги/ротация)."""
        keep = set(keep)
        removed = 0
        with self._lock:
            for key in list(self._symbols.keys()):
                st = self._symbols[key]
                if st.exchange == exchange and st.symbol not in keep:
                    del self._symbols[key]
                    removed += 1
        return removed

    def __len__(self) -> int:
        return len(self._symbols)

    # ------------------------------------------------------------------
    def snapshot(self, limit: int = 100000, sort: str = "vol", desc: bool = True) -> list[dict]:
        rows = [s.to_row() for s in self.all()]
        rows = [r for r in rows if r["last"] > 0]
        rows.sort(key=lambda r: (r.get(sort) if r.get(sort) is not None else -1e18), reverse=desc)
        return rows[:limit]

    def overview(self, ttl: float = 0.0) -> dict:
        """
        Сводка по рынку для шапки UI.

        ttl > 0 включает кэш: pusher дёргает overview раз в секунду на
        КАЖДОГО WS-клиента, а обход всех символов на 6000+ строк стоит
        ~7 мс — суммарно это заметная доля ядра. В пределах ttl отдаём
        прошлый результат (данные шапки и так обновляются раз в секунду).
        """
        now = time.time()
        if ttl > 0.0 and self._ov_cache is not None and (now - self._ov_cache[0]) < ttl:
            return self._ov_cache[1]
        rows = [s for s in self.all() if s.last > 0]
        up = sum(1 for s in rows if s.change_pct > 0)
        down = sum(1 for s in rows if s.change_pct < 0)
        total_vol = sum(s.vol24_usd for s in rows)
        by_ex: dict[str, dict] = {}
        for s in rows:
            e = by_ex.setdefault(s.exchange_label, {"symbols": 0, "vol": 0.0, "up": 0, "down": 0})
            e["symbols"] += 1
            e["vol"] += s.vol24_usd
            e["up"] += s.change_pct > 0
            e["down"] += s.change_pct < 0
        out = {
            "symbols": len(rows),
            "up": up,
            "down": down,
            "flat": len(rows) - up - down,
            "volume_usd": total_vol,
            "volume_fmt": fmt_compact(total_vol),
            "exchanges": by_ex,
            "status": self.exchange_status,
            "stats": dict(self.stats),
            "uptime": time.time() - self.started_at,
        }
        if ttl > 0.0:
            self._ov_cache = (now, out)
        return out


STORE = Store()
