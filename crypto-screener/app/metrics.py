"""
Метрики и производные показатели по каждому инструменту.

Здесь живёт вся «математика» скринера: спайки объёма/сделок, NATR,
мульти-таймфреймовая доходность, дельта и CVD, плотности стакана.

Принцип: все расчёты O(1) или O(малое_окно) на один апдейт, потому что
вызываются десятки тысяч раз в минуту.
"""
from __future__ import annotations

import bisect
import math
import time
from array import array
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Optional


# --------------------------------------------------------------------------
# Вспомогательные структуры
# --------------------------------------------------------------------------
class RingBuffer:
    """
    Кольцевой буфер пар (timestamp, value) на `array('d')`.

    Зачем не deque[tuple]: на 6000 инструментов × 900 точек deque с кортежами
    занимает ~1075 МБ (104 байта на точку: tuple + два boxed float). Два
    массива doubles — 83 МБ, то есть в 13 раз меньше. Это разница между
    «работает на ноутбуке» и OOM-kill при подключении всех бирж.

    Интерфейс совместим с deque в той части, которую используют вызывающие:
    len(), итерация парами, индексация [0] / [-1], .maxlen.
    """

    __slots__ = ("_ts", "_val", "maxlen", "_slack")

    def __init__(self, maxlen: int) -> None:
        self.maxlen = int(maxlen)
        self._ts = array("d")
        self._val = array("d")
        # подрезаем не каждый append (это memmove на всю длину), а пачками
        self._slack = max(16, self.maxlen // 8)

    def append(self, ts: float, value: float) -> None:
        self._ts.append(ts)
        self._val.append(value)
        over = len(self._ts) - self.maxlen
        if over >= self._slack:
            del self._ts[:over]
            del self._val[:over]

    def __len__(self) -> int:
        return len(self._ts)

    def __iter__(self):
        ts, val = self._ts, self._val
        for i in range(len(ts)):
            yield ts[i], val[i]

    def __getitem__(self, i: int) -> tuple[float, float]:
        return (self._ts[i], self._val[i])

    def bisect_ts(self, ts: float) -> int:
        """Индекс первой точки с timestamp >= ts (массив всегда отсортирован).

        Зачем: ret() вызывается 5 раз на символ на каждой секундной
        сериализации (6000 символов → 30 000 вызовов/с). Линейный скан
        хвоста истории — сотни итераций на вызов; бинарный поиск — ~10.
        """
        return bisect.bisect_left(self._ts, ts)

    def value_at(self, i: int) -> float:
        return self._val[i]

    def __bool__(self) -> bool:
        return len(self._ts) > 0

    def tail(self, n: int) -> list[tuple[float, float]]:
        ts, val = self._ts, self._val
        start = max(0, len(ts) - n)
        return [(ts[i], val[i]) for i in range(start, len(ts))]

    def values_tail(self, n: int) -> list[float]:
        start = max(0, len(self._val) - n)
        return list(self._val[start:])


@dataclass(slots=True)
class Density:
    """Скопление лимитного объёма в стакане (агрегированный кластер цен)."""

    price: float
    base: float
    quote: float
    side: str          # "bid" | "ask"
    dist_pct: float    # расстояние от последней цены, %
    n_orders: int = 1


@dataclass(slots=True)
class BookImbalance:
    bid_quote: float = 0.0
    ask_quote: float = 0.0

    @property
    def ratio(self) -> float:
        """>1 — перевес бидов, <1 — перевес асков."""
        if self.ask_quote <= 0:
            return 0.0 if self.bid_quote <= 0 else 99.0
        return self.bid_quote / self.ask_quote


@dataclass(slots=True)
class Spike:
    ts: float
    kind: str          # "volume" | "trades" | "print"
    value: float
    base: float
    ratio: float

    @property
    def age(self) -> float:
        return time.time() - self.ts


# --------------------------------------------------------------------------
# Состояние инструмента
# --------------------------------------------------------------------------
class SymbolState:
    """Всё, что мы знаем про одну монету на одной бирже, в один момент времени."""

    __slots__ = (
        "key", "exchange", "exchange_label", "market_type", "symbol",
        "base", "quote", "last", "bid", "ask", "ts",
        "inverse", "contract_size", "ohlcv_vol_in_contracts", "dex",
        "open24", "high24", "low24", "vol24_base", "vol24_quote", "trades24",
        "change_pct", "range_pct", "vol24_usd",
        "price_hist",
        "buy_quote_min", "sell_quote_min", "trades_min", "prints_min",
        "cvd", "cvd_1m", "cvd_5m", "delta_1m",
        "ohlcv", "ohlcv_ts", "natr", "natr_period",
        "minute_vols", "minute_trades", "spike_vol_ratio", "spike_tr_ratio",
        "book", "imbalance", "densities", "big_densities", "_dens_calc",
        "funding", "open_interest", "open_interest_usd", "oi_change_pct",
        "spikes", "last_spike_ts",
        "updated", "seq", "_min_lock",
    )

    def __init__(
        self,
        exchange: str,
        exchange_label: str,
        market_type: str,
        symbol: str,
        base: str,
        quote: str,
        history_window: int = 900,
        spike_vol_ratio: float = 3.0,
        spike_tr_ratio: float = 3.0,
    ) -> None:
        self.key = f"{exchange}:{symbol}"
        self.exchange = exchange
        self.exchange_label = exchange_label
        self.market_type = market_type
        self.symbol = symbol
        self.base = base
        self.quote = quote
        # тип контракта критичен для пересчёта объёмов: у инверсных
        # (Bybit BTC/USD:BTC) amount уже номинирован в USD, а не в монете
        self.inverse = False
        self.contract_size = 1.0
        self.ohlcv_vol_in_contracts = False
        self.dex = False          # децентрализованная биржа (метка для UI)

        self.last: float = 0.0
        self.bid: float = 0.0
        self.ask: float = 0.0
        self.ts: float = 0.0

        self.open24: float = 0.0
        self.high24: float = 0.0
        self.low24: float = 0.0
        self.vol24_base: float = 0.0
        self.vol24_quote: float = 0.0
        self.trades24: Optional[int] = None   # Bybit/OKX число сделок не отдают — None честнее нуля

        self.change_pct: float = 0.0
        self.range_pct: float = 0.0
        self.vol24_usd: float = 0.0

        # кольцевые буферы: (timestamp, value)
        # только история цен: quote_hist писался и никогда не читался,
        # trades_hist не использовался вовсе — вместе они давали 2/3 расхода памяти
        self.price_hist: RingBuffer = RingBuffer(history_window)

        # минутные накопители для спайков
        self.buy_quote_min: float = 0.0
        self.sell_quote_min: float = 0.0
        self.trades_min: int = 0
        self.prints_min: int = 0
        self._min_lock: float = 0.0   # ts начала текущей минуты

        self.cvd: float = 0.0
        self.cvd_1m: float = 0.0
        self.cvd_5m: float = 0.0
        self.delta_1m: float = 0.0

        self.ohlcv: list = []
        # когда последний раз обновляли свечи. Нужно фокус-режиму: отбор по
        # волатильности обязан считать NATR по свежим свечам, а не по тем,
        # что остались от прошлого горячего набора (иначе монета ранжируется
        # по протухшей метрике).
        self.ohlcv_ts: float = 0.0
        # Отдельный буфер для сетки графиков: WS kline отдаёт свечи ЗАПРОШЕННОГО
        # таймфрейма (5m/15m...). Раньше они писались в ohlcv через apply_ohlcv
        # и затирали 1m-историю → NATR считался по горстке чужих свечей →
        # монеты вылетали из фокус-отбора → состав сетки перемешивался каждые
        # ~30 с («графики меняются, свечи пропадают при новом отборе»).
        self.grid_ohlcv: list = []
        self.grid_ohlcv_tf: str = ""
        self.grid_ohlcv_ts: float = 0.0
        self.natr: float = 0.0
        self.natr_period: int = 14

        self.book: Optional[dict] = None
        self.imbalance: BookImbalance = BookImbalance()
        self.densities: list[Density] = []
        self.big_densities: list[Density] = []
        # монотонное время последнего тяжёлого пересчёта стакана (см. apply_book)
        self._dens_calc: float = 0.0

        self.funding: Optional[float] = None
        self.open_interest: Optional[float] = None
        self.open_interest_usd: Optional[float] = None
        self.oi_change_pct: Optional[float] = None

        self.spikes: Deque[Spike] = deque(maxlen=50)
        self.last_spike_ts: float = 0.0

        # история минутных окон — из неё считается «нормальная» база для спайков
        self.minute_vols: Deque[float] = deque(maxlen=30)
        self.minute_trades: Deque[int] = deque(maxlen=30)
        self.spike_vol_ratio = spike_vol_ratio
        self.spike_tr_ratio = spike_tr_ratio

        self.updated: float = 0.0
        self.seq: int = 0

    # ------------------------------------------------------------------
    # Тикер (REST 24h + WS bookTicker)
    # ------------------------------------------------------------------
    def apply_ticker(self, t: dict) -> None:
        last = t.get("last") or t.get("close")
        if last:
            self.set_price(float(last), float(t.get("timestamp") or time.time() * 1000) / 1000)
        if t.get("bid"):
            self.bid = float(t["bid"])
        if t.get("ask"):
            self.ask = float(t["ask"])
        if t.get("open") is not None:
            self.open24 = float(t["open"])
        if t.get("high") is not None:
            self.high24 = float(t["high"])
        if t.get("low") is not None:
            self.low24 = float(t["low"])
        if t.get("baseVolume") is not None:
            self.vol24_base = float(t["baseVolume"])
        if t.get("quoteVolume") is not None:
            self.vol24_quote = float(t["quoteVolume"])
        cnt = t.get("count")
        if cnt is None:
            cnt = (t.get("info") or {}).get("count")   # Binance отдаёт его только в raw
        if cnt is not None:
            try:
                self.trades24 = int(cnt)
            except (TypeError, ValueError):
                pass
        self.vol24_usd = self.compute_usd_volume(t)
        self._recalc_24h()
        self.touch()

    def compute_usd_volume(self, t: dict) -> float:
        """
        Оборот за 24ч в USD. Единое правило для всех типов контрактов.

        Проверено на реальных данных:
          Bybit linear  BTC/USDT:USDT → quoteVolume = turnover24h      = $6.39B
          Bybit inverse BTC/USD:BTC   → baseVolume·contractSize        = $224M
                                        (= volCcy24h 2663 BTC · цена)
          OKX  linear   BTC/USDT:USDT → baseVolume·cs·price            = $8.16B
                                        (quoteVolume биржа не отдаёт)
          OKX  inverse  BTC/USD:BTC   → baseVolume·contractSize(100)   = $582M
                                        (≈ volCcy24h 6925 BTC · цена)
        """
        bv = t.get("baseVolume")
        qv = t.get("quoteVolume")
        cs = self.contract_size or 1.0
        if self.inverse:
            # контракт номинирован в USD → baseVolume уже доллары
            return float(bv or 0.0) * cs
        if qv is not None:
            return self._to_usd(float(qv), self.quote)
        if bv is not None and self.last:
            return float(bv) * cs * self.last
        return 0.0

    def apply_quote(self, q: dict) -> None:
        """WS bookTicker: bid/ask/last — самый горячий поток."""
        if q.get("bid"):
            self.bid = float(q["bid"])
        if q.get("ask"):
            self.ask = float(q["ask"])
        last = q.get("last") or self._mid()
        if last:
            self.set_price(float(last), float(q.get("timestamp") or time.time() * 1000) / 1000)
        self.touch()

    def set_contract(self, inverse: bool, contract_size: Optional[float]) -> None:
        self.inverse = bool(inverse)
        self.contract_size = float(contract_size) if contract_size else 1.0

    def quote_value(self, amount: float, price: float) -> float:
        """
        Стоимость объёма в валюте котировки (≈USD для стейбл-пар).

        Инверсный контракт: amount уже в USD → умножать на цену НЕЛЬЗЯ,
        иначе получается price² и плотности «в триллион долларов».
        Линейный/спот: amount в базовой монете → цена нужна.
        """
        cs = self.contract_size or 1.0
        if self.inverse:
            return amount * cs
        return amount * cs * price

    def base_amount(self, amount: float, price: float) -> float:
        cs = self.contract_size or 1.0
        if self.inverse:
            return (amount * cs) / price if price else 0.0
        return amount * cs

    def _mid(self) -> Optional[float]:
        if self.bid and self.ask:
            return (self.bid + self.ask) / 2.0
        return self.bid or self.ask or None

    def _recalc_24h(self) -> None:
        if self.open24 and self.last:
            self.change_pct = (self.last / self.open24 - 1.0) * 100.0
        if self.high24 and self.low24 and self.low24 > 0:
            self.range_pct = (self.high24 / self.low24 - 1.0) * 100.0
        # vol24_usd считается в compute_usd_volume(); здесь только если его ещё нет
        if not self.vol24_usd and self.vol24_quote:
            self.vol24_usd = self._to_usd(self.vol24_quote, self.quote)

    def _to_usd(self, amount: float, quote: str) -> float:
        if quote in {"USDT", "USDC", "USD", "BUSD", "FDUSD", "DAI", "TUSD", "USDE"}:
            return amount
        if quote in {"BTC", "XBT"}:
            return amount * (_BTC_USD or 0.0)
        if quote == "ETH":
            return amount * (_ETH_USD or 0.0)
        return amount

    # ------------------------------------------------------------------
    # Цена / история
    # ------------------------------------------------------------------
    def set_price(self, price: float, ts: float) -> None:
        if price <= 0 or not math.isfinite(price):
            return
        self.last = price
        self.ts = ts
        self.price_hist.append(ts, price)
        self._recalc_24h()

    # ------------------------------------------------------------------
    # Сделки (WS trades) — дельта, CVD, спайки
    # ------------------------------------------------------------------
    def apply_trade(self, tr: dict) -> None:
        amount = float(tr.get("amount") or 0.0)
        price = float(tr.get("price") or 0.0)
        if amount <= 0 or price <= 0:
            return
        quote_amt = self.quote_value(amount, price)
        side = (tr.get("side") or "").lower()
        ts = float(tr.get("timestamp") or time.time() * 1000) / 1000

        self._roll_minute(ts)
        if side == "buy":
            self.buy_quote_min += quote_amt
        elif side == "sell":
            self.sell_quote_min += quote_amt
        self.trades_min += 1
        self.prints_min += 1

        delta = quote_amt if side == "buy" else (-quote_amt if side == "sell" else 0.0)
        self.cvd += delta
        self.set_price(price, ts)
        self.touch()

    def _roll_minute(self, now: float) -> None:
        """Переход минутного окна: фиксируем спайки и сбрасываем накопители."""
        if self._min_lock == 0.0:
            self._min_lock = now - (now % 60)
            return
        cur = now - (now % 60)
        if cur <= self._min_lock:
            return
        self._finish_minute(self._min_lock)
        self._min_lock = cur
        self.buy_quote_min = 0.0
        self.sell_quote_min = 0.0
        self.trades_min = 0
        self.prints_min = 0

    def _finish_minute(self, minute_ts: float) -> None:
        """
        Закрываем минутное окно: сравниваем его с медианой предыдущих минут.

        Медиана (а не среднее) — потому что прошлые спайки не должны завышать
        базу и «прятать» следующий всплеск.
        """
        minute_vol = self.buy_quote_min + self.sell_quote_min
        self.delta_1m = self.buy_quote_min - self.sell_quote_min

        # Базу берём ТОЛЬКО из уже закрытых минут, посчитанных тем же способом,
        # что и текущая. Прикидка «суточный объём / 1440» имеет другие единицы
        # (и ничего не знает о размере контракта) — на старте она давала
        # спайки вида «723205x». Первые 3 минуты просто молчим.
        prev_vols = list(self.minute_vols)[-15:]
        if len(prev_vols) < 3:
            self.minute_vols.append(minute_vol)
            self.minute_trades.append(self.trades_min)
            return
        base_vol = _median(prev_vols)
        if base_vol > 0 and base_vol * self.spike_vol_ratio < minute_vol < base_vol * 1000:
            self.add_spike(minute_ts, "volume", minute_vol, base_vol, minute_vol / base_vol)

        prev_tr = [float(x) for x in list(self.minute_trades)[-15:]]
        base_tr = _median(prev_tr) if len(prev_tr) >= 3 else 0.0
        if base_tr > 0 and base_tr * self.spike_tr_ratio < self.trades_min < base_tr * 1000:
            self.add_spike(minute_ts, "trades", float(self.trades_min), base_tr,
                           self.trades_min / base_tr)

        self.minute_vols.append(minute_vol)
        self.minute_trades.append(self.trades_min)

    def add_spike(self, ts: float, kind: str, value: float, base: float, ratio: float) -> None:
        self.spikes.append(Spike(ts=ts, kind=kind, value=value, base=base, ratio=ratio))
        self.last_spike_ts = ts

    # ------------------------------------------------------------------
    # Стакан и плотности
    # ------------------------------------------------------------------
    @staticmethod
    def _norm_levels(levels: list) -> list[tuple[float, float]]:
        """
        Приводит уровни стакана к [(price, amount)].

        Формат зависит от биржи: MEXC отдаёт три поля [price, amount, count],
        остальные — два. Жёсткая распаковка `for p, a in levels` на MEXC падала
        с ValueError, из-за чего стаканов MEXC не было вовсе (350 ошибок в лог).
        """
        out: list[tuple[float, float]] = []
        for lvl in levels:
            try:
                out.append((float(lvl[0]), float(lvl[1])))
            except (IndexError, TypeError, ValueError):
                continue
        return out

    def _touch_top_of_book(self, book: dict) -> None:
        """
        Лёгкий путь apply_book внутри окна троттлинга: только top-of-book.

        Никакой нормализации уровней: bid/ask — единственное, что обязано быть
        realtime (подсветка цен в таблице, лестница drawer при открытии).
        Защита от мусора та же, что в _norm_levels (у MEXC уровни из 3 полей,
        у отдельных бирж случаются пустые списки).
        """
        for side, attr in (("bids", "bid"), ("asks", "ask")):
            levels = book.get(side)
            if not levels:
                continue
            try:
                setattr(self, attr, float(levels[0][0]))
            except (IndexError, TypeError, ValueError):
                continue

    def apply_book(self, book: dict, big_usd: float = 50_000.0, huge_usd: float = 250_000.0,
                   cluster_pct: float = 0.05, depth: int = 200,
                   min_interval: float = 0.0) -> None:
        """
        Обновление стакана.

        min_interval > 0 включает троттлинг ТЯЖЁЛОЙ части (имбаланс +
        кластеризация плотностей + полная нормализация уровней): она делается
        не чаще раза в min_interval секунд на символ. Замер (tools/bench_perf.py):
        полный пересчёт стакана 200+200 уровней стоит ~350 мкс, из них ~290 мкс —
        кластеризация и имбаланс. Биржи шлют стаканы по 5–10 раз в секунду
        на символ, а книг в CORE-профиле ~510 → без троттлинга только стаканы
        съедают 1–1.7 ядра и душат event loop (тормозит ВСЁ: пуш, REST, WS).
        Глазу плотности достаточно обновлять 4 раза в секунду, поэтому
        дефолт в настройках — 0.25 с.

        Внутри окна троттлинга работает ЛЁГКИЙ путь (_touch_top_of_book):
        bid/ask обновляются каждый раз, а нормализованный список уровней
        self.book — только вместе с тяжёлым пересчётом. Раньше лёгкий путь
        всё равно строил два списка по 200 кортежей на КАЖДЫЙ апдейт
        (~2 млн аллокаций/с на CORE-профиле — заметная доля CPU и давления
        на GC). Потребители self.book от этого не страдают: лестница цен в
        drawer читается в момент открытия карточки, плотности/имбаланс и так
        живут на тяжёлом пути.
        """
        if min_interval > 0.0:
            now = time.monotonic()
            if self._dens_calc and (now - self._dens_calc) < min_interval:
                self._touch_top_of_book(book)
                return          # тяжёлый пересчёт — не чаще min_interval
            self._dens_calc = now

        bids = self._norm_levels(book.get("bids") or [])[:depth]
        asks = self._norm_levels(book.get("asks") or [])[:depth]
        # храним УЖЕ нормализованный стакан: тогда ни один потребитель
        # (лестница цен в API, плотности, дисбаланс) не споткнётся о
        # трёхэлементные уровни MEXC
        self.book = {"bids": bids, "asks": asks, "timestamp": book.get("timestamp")}
        if bids:
            self.bid = float(bids[0][0])
        if asks:
            self.ask = float(asks[0][0])

        imb = BookImbalance()
        for p, a in bids:
            imb.bid_quote += self.quote_value(float(a), float(p))
        for p, a in asks:
            imb.ask_quote += self.quote_value(float(a), float(p))
        self.imbalance = imb

        ref = self.last or self._mid() or 0.0
        if ref <= 0:
            self.densities = []
            self.big_densities = []
            return

        dens: list[Density] = []
        for side, levels in (("bid", bids), ("ask", asks)):
            dens.extend(self._cluster_levels(levels, side, ref, cluster_pct))
        dens.sort(key=lambda d: d.quote, reverse=True)
        self.densities = dens[:40]
        self.big_densities = [d for d in dens if d.quote >= big_usd][:20]

    # ------------------------------------------------------------------
    # Свечи / NATR / мульти-ТФ
    # ------------------------------------------------------------------
    def apply_ohlcv(self, candles: list, period: int = 14) -> None:
        """candles: [[ts, o, h, l, c, v], ...] по возрастанию (1m)."""
        if not candles:
            return
        self.ohlcv = candles[-300:]
        self.ohlcv_ts = time.time()
        self.natr = natr(self.ohlcv, period)
        self.natr_period = period

    def ret(self, seconds: int) -> Optional[float]:
        """
        Доходность за последние N секунд по in-memory истории.

        Возвращает None, если история КОРОЧЕ запрошенного окна. Без этой
        проверки ret(3600) при буфере в 15 минут молча отдавал бы 15-минутную
        доходность под видом часовой — то есть wrong data в UI и в алертах.
        """
        if len(self.price_hist) < 2 or not self.last:
            return None
        newest = self.price_hist[-1][0]
        oldest = self.price_hist[0][0]
        if newest - oldest < seconds * 0.9:
            return None
        # бинарный поиск вместо линейного скана: та же семантика («первая
        # точка не старше cutoff»), но O(log n) — см. RingBuffer.bisect_ts
        i = self.price_hist.bisect_ts(newest - seconds)
        if i >= len(self.price_hist):
            return None
        p = self.price_hist.value_at(i)
        if p <= 0:
            return None
        return (self.last / p - 1.0) * 100.0

    def ret_ohlcv(self, minutes: int) -> Optional[float]:
        """Доходность по свечам (работает сразу после старта, до накопления истории)."""
        c = self.ohlcv
        if not c or len(c) < minutes + 1:
            return None
        past = float(c[-minutes - 1][4])
        if past <= 0:
            return None
        return (float(c[-1][4]) / past - 1.0) * 100.0

    def sparkline(self, n: int = 60) -> list[float]:
        if not len(self.price_hist):
            return []
        vals = self.price_hist.values_tail(n)
        step = max(1, len(vals) // 40)
        return [round(v, 10) for v in vals[::step]]

    def _cluster_levels(self, levels: list, side: str, ref: float,
                        cluster_pct: float) -> list[Density]:
        """
        Склеиваем соседние уровни стакана в кластеры («плотности»).

        Допуск задаётся в процентах от цены, но ограничен снизу и сверху
        в тиках: иначе для BTC 0.05% = $42 и в один кластер сливается весь
        стакан целиком, а для дешёвых альт-коинов кластер вырождается в один тик.
        """
        if not levels or ref <= 0:
            return []
        tick = min((abs(float(levels[i][0]) - float(levels[i + 1][0]))
                    for i in range(len(levels) - 1)), default=0.0) or ref * 1e-5
        tol = min(max(ref * cluster_pct / 100.0, tick * 1.5), tick * 40)
        out: list[Density] = []
        cur: Optional[list] = None
        for p, a in levels:
            p = float(p); a = float(a)
            q = self.quote_value(a, p)
            if cur is None or abs(p - cur[0]) > tol:
                if cur:
                    out.append(Density(price=cur[0], base=cur[1], quote=cur[2], side=side,
                                       dist_pct=(cur[0] / ref - 1.0) * 100.0, n_orders=cur[3]))
                cur = [p, self.base_amount(a, p), q, 1]
            else:
                cur[1] += self.base_amount(a, p)
                cur[2] += q
                cur[3] += 1
        if cur:
            out.append(Density(price=cur[0], base=cur[1], quote=cur[2], side=side,
                               dist_pct=(cur[0] / ref - 1.0) * 100.0, n_orders=cur[3]))
        return out

    # ------------------------------------------------------------------
    def touch(self) -> None:
        self.updated = time.time()
        self.seq += 1

    # ------------------------------------------------------------------
    # Сериализация
    # ------------------------------------------------------------------
    def to_row(self, tf: tuple = (60, 300, 900, 3600, 14400), with_densities: bool = False) -> dict:
        rets = {}
        for sec in tf:
            r = self.ret(sec)
            if r is None and sec % 60 == 0:
                r = self.ret_ohlcv(sec // 60)
            rets[f"r{sec}"] = round(r, 3) if r is not None else None

        spike = None
        if self.spikes:
            last = self.spikes[-1]
            if last.age < 600:
                spike = {"kind": last.kind, "ratio": round(last.ratio, 2), "age": int(last.age)}

        row: dict = {
            "k": self.key,
            "ex": self.exchange,
            "exl": self.exchange_label,
            "mt": self.market_type,
            "dex": self.dex,
            "s": self.symbol,
            "b": self.base,
            "q": self.quote,
            "last": self.last,
            "bid": self.bid,
            "ask": self.ask,
            "chg": round(self.change_pct, 3),
            "rng": round(self.range_pct, 3),
            "hi": self.high24,
            "lo": self.low24,
            "vol": round(self.vol24_usd, 2),
            "volq": round(self.vol24_quote, 2),
            "tr": self.trades24,
            "natr": round(self.natr, 3) if self.natr else None,
            "cvd": round(self.cvd, 2),
            "d1m": round(self.delta_1m, 2),
            "imb": round(self.imbalance.ratio, 3),
            "fund": self.funding,
            "oi": self.open_interest,
            "oiusd": round(self.open_interest_usd, 2) if self.open_interest_usd else None,
            "spike": spike,
            "u": round(self.updated, 1),
            **rets,
        }
        if with_densities:
            row["dens"] = [
                {"p": d.price, "q": round(d.quote, 2), "sd": d.side, "d": round(d.dist_pct, 3), "n": d.n_orders}
                for d in self.big_densities[:12]
            ]
        return row


# --------------------------------------------------------------------------
# Модуль-уровень: референсные цены для пересчёта объёмов в USD
# --------------------------------------------------------------------------
_BTC_USD: Optional[float] = None
_ETH_USD: Optional[float] = None


def set_reference_prices(btc: Optional[float] = None, eth: Optional[float] = None) -> None:
    global _BTC_USD, _ETH_USD
    if btc:
        _BTC_USD = btc
    if eth:
        _ETH_USD = eth


# --------------------------------------------------------------------------
# Чистые функции-индикаторы
# --------------------------------------------------------------------------
# таймфреймы, которые умеет отдавать эндпоинт /api/candles
TF_SECONDS: dict[str, int] = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "12h": 43200, "1d": 86400,
}


def resample(candles: list, tf_seconds: int) -> list:
    """
    Пересобирает младшие свечи в старший таймфрейм.

    Нужно в replay-режиме (там есть только 1m) и как запасной путь, если биржа
    не отдаёт запрошенный ТФ. Группировка — по границам периода, а не «по N штук»,
    иначе свечи разъезжаются относительно реального времени.
    """
    if not candles or tf_seconds <= 0:
        return list(candles)
    out: list = []
    cur: Optional[list] = None
    bucket = -1
    for c in candles:
        ts = int(c[0])
        b = ts - (ts % (tf_seconds * 1000))
        if b != bucket:
            if cur:
                out.append(cur)
            bucket = b
            cur = [b, float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5] or 0.0)]
        else:
            cur[2] = max(cur[2], float(c[2]))      # high
            cur[3] = min(cur[3], float(c[3]))      # low
            cur[4] = float(c[4])                   # close
            cur[5] += float(c[5] or 0.0)           # volume
    if cur:
        out.append(cur)
    return out


def normalize_candle_volume(candles: list, inverse: bool, contract_size: float,
                            vol_in_contracts: bool = False) -> list:
    """
    Приводит объём свечей к USD.

    Двухступенчато, потому что ccxt нормализует OHLCV по-разному:
      1. `vol_in_contracts` — биржа отдаёт объём в контрактах (Gate, MEXC),
         тогда домножаем на contractSize, чтобы получить базовую монету.
         OKX/Binance/Aster/Bybit/Hyperliquid отдают сразу базовый объём.
      2. дальше базовый объём → USD: для линейных умножаем на close,
         для инверсных контракт уже номинирован в USD.

    Без первого шага Gate/MEXC завышают объём в 10 000 раз, без второго —
    OKX занижает в 100 раз (contractSize=0.01 применяется к уже базовому объёму).
    """
    cs = (contract_size or 1.0) if vol_in_contracts else 1.0
    out: list = []
    for c in candles:
        o, h, lo, cl = float(c[1]), float(c[2]), float(c[3]), float(c[4])
        base = float(c[5] or 0.0) * cs
        v_usd = base if inverse else base * cl
        out.append([int(c[0]), o, h, lo, cl, v_usd])
    return out


def true_range(candles: list) -> list[float]:
    out: list[float] = []
    prev_close: Optional[float] = None
    for c in candles:
        h, l, cl = float(c[2]), float(c[3]), float(c[4])
        if prev_close is None:
            tr = h - l
        else:
            tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        out.append(tr)
        prev_close = cl
    return out


def atr(candles: list, period: int = 14) -> float:
    trs = true_range(candles)
    if not trs:
        return 0.0
    if len(trs) <= period:
        return sum(trs) / len(trs)
    a = sum(trs[:period]) / period          # SMA как затравка
    for tr in trs[period:]:
        a = (a * (period - 1) + tr) / period  # сглаживание Уайлдера
    return a


def natr(candles: list, period: int = 14) -> float:
    """Normalized ATR, % — волатильность, сравнимая между монетами."""
    if not candles:
        return 0.0
    close = float(candles[-1][4])
    if close <= 0:
        return 0.0
    return atr(candles, period) / close * 100.0


def _median(xs: list[float]) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0


def fmt_compact(x: Optional[float], digits: int = 2) -> str:
    if x is None:
        return "—"
    ax = abs(x)
    for lim, suf in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if ax >= lim:
            return f"{x / lim:.{digits}f}{suf}"
    return f"{x:.{digits}f}"
