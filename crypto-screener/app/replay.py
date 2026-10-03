"""
Replay-источник данных.

Зачем: скринер должен запускаться и работать без доступа к биржам —
для демо, скриншотов, unit-тестов и разработки фронтенда.

Берёт снапшот, который live-режим периодически пишет на диск
(data/replay_snapshot.json), и «оживляет» его случайным блужданием цен,
объёмов и стакана. Если снапшота нет — генерирует синтетический рынок
с реалистичным распределением (логнормальный объём, тяжёлые хвосты у движений).
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import time
from pathlib import Path
from typing import Optional

from .config import Settings
from .metrics import natr
from .state import STORE

log = logging.getLogger("replay")

SYNTH_BASES = [
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK", "TON",
    "DOT", "MATIC", "LTC", "BCH", "UNI", "ATOM", "NEAR", "APT", "ARB", "OP",
    "INJ", "SUI", "SEI", "TIA", "JUP", "WIF", "PEPE", "BONK", "FLOKI", "RNDR",
    "AAVE", "MKR", "LDO", "ENA", "ONDO", "WLD", "ORDI", "STX", "IMX", "GRT",
    "FIL", "ICP", "ETC", "XLM", "ALGO", "VET", "HBAR", "QNT", "RUNE", "FTM",
]
BOOK_EVERY_N = 7   # каждый N-й инструмент за тик получает свежий стакан

SYNTH_PRICES = {
    "BTC": 83500.0, "ETH": 3100.0, "SOL": 190.0, "BNB": 640.0, "XRP": 0.62,
    "DOGE": 0.16, "TON": 5.4, "LINK": 17.0, "PEPE": 0.0000121, "WIF": 1.8,
}


class ReplaySource:
    def __init__(self, settings: Settings, path: Optional[str] = None) -> None:
        self.settings = settings
        self.path = Path(path or settings.replay_file)
        self.speed = settings.replay_speed
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self.rows: list[dict] = []
        self._book_cursor: int = 0

    async def start(self) -> None:
        self.rows = self._load()
        log.info("replay: %d инструментов", len(self.rows))
        self._seed_store()
        self._task = asyncio.create_task(self._loop(), name="replay")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    # ------------------------------------------------------------------
    def _load(self) -> list[dict]:
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text(encoding="utf-8"))
                rows = data.get("rows") or []
                if rows:
                    log.info("replay: загружен снапшот %s (%d строк)", self.path.name, len(rows))
                    return rows
            except Exception as e:  # noqa: BLE001
                log.warning("replay: не удалось прочитать %s: %s", self.path, e)
        return self._synthetic()

    def _synthetic(self, n: int = 420) -> list[dict]:
        """
        Синтетический рынок.

        Важно: ключи (биржа+символ) обязаны быть уникальными. Иначе одно и то же
        SymbolState засеивается дважды разными ценами, и история цен внутри него
        оказывается от другого инструмента — метрики улетают на десятки процентов.
        """
        log.info("replay: снапшота нет — генерируем синтетический рынок")
        rng = random.Random(20261001)
        exchanges = ([(e.id, e.label, e.market) for e in self.settings.exchanges]
                     or [("demo", "Demo", "swap")])
        quotes = self.settings.quote_assets[:3] or ["USDT"]
        now = time.time()

        # уникальные комбинации (база, биржа); часть баз намеренно оставляем
        # на одной бирже — чтобы фильтр «независимые инструменты» имел смысл
        combos: list[tuple[str, tuple[str, str, str]]] = []
        for i, base in enumerate(SYNTH_BASES):
            n_ex = 1 if i % 5 == 4 else min(len(exchanges), rng.randint(2, len(exchanges)))
            for e in range(n_ex):
                combos.append((base, exchanges[(i + e) % len(exchanges)]))
        rng.shuffle(combos)
        combos = combos[: max(n, len(exchanges))]

        rows: list[dict] = []
        seen: set[str] = set()
        for base, (ex_id, ex_label, mt) in combos:
            quote = quotes[len(rows) % len(quotes)]
            symbol = f"{base}/{quote}" if mt == "spot" else f"{base}/{quote}:{quote}"
            key = f"{ex_id}:{symbol}"
            if key in seen:          # тот же base+quote на той же бирже — меняем квоту
                quote = quotes[(quotes.index(quote) + 1) % len(quotes)]
                symbol = f"{base}/{quote}" if mt == "spot" else f"{base}/{quote}:{quote}"
                key = f"{ex_id}:{symbol}"
                if key in seen:
                    continue
            seen.add(key)

            price = SYNTH_PRICES.get(base) or round(rng.uniform(0.02, 90), 6)
            # логнормальный объём: пара китов + длинный хвост мелочи
            vol = math.exp(rng.gauss(14.5, 2.1))
            chg = rng.gauss(0, 4.5)
            if rng.random() < 0.06:                       # тяжёлые хвосты
                chg += rng.choice([-1, 1]) * rng.uniform(8, 35)
            rows.append({
                "k": key, "ex": ex_id, "exl": ex_label, "mt": mt,
                "s": symbol, "b": base, "q": quote,
                "last": price, "bid": price * 0.9999, "ask": price * 1.0001,
                "chg": round(chg, 3), "rng": round(abs(rng.gauss(6, 4)) + abs(chg) * 0.4, 3),
                "hi": price * (1 + abs(chg) / 100 + 0.01), "lo": price * (1 - abs(chg) / 100 - 0.01),
                "vol": round(vol, 2), "volq": round(vol, 2),
                "tr": int(vol / max(price, 1e-9) * rng.uniform(2, 9)),
                "natr": round(abs(rng.gauss(0.9, 0.8)), 3),
                "cvd": round(rng.gauss(0, vol * 0.05), 2), "d1m": round(rng.gauss(0, vol * 0.002), 2),
                "imb": round(math.exp(rng.gauss(0, 0.35)), 3),
                "fund": round(rng.gauss(0.0001, 0.0004), 6) if mt == "swap" else None,
                "oi": None, "oiusd": round(vol * rng.uniform(0.05, 0.4), 2) if mt == "swap" else None,
                "spike": ({"kind": rng.choice(["volume", "trades"]), "ratio": round(rng.uniform(3, 12), 2),
                           "age": rng.randint(2, 300)} if rng.random() < 0.07 else None),
                "u": round(now, 1),
                **{f"r{tf}": round(rng.gauss(0, 0.35 * math.sqrt(tf / 60)), 3)
                   for tf in self.settings.tf_seconds},
            })
        return rows

    # ------------------------------------------------------------------
    def _backfill_history(self, st, r: dict, rng: random.Random) -> None:
        """
        Заполняем историю цен ДО старта, а не ждём, пока она накопится.

        Без этого первые ~15 минут все мульти-ТФ доходности (1м/5м/15м/1ч/4ч)
        идентичны: окно ретроспективы упирается в самую первую точку. Здесь генерируем
        согласованную траекторию GBM длиной history_window и пересчитываем
        r* из неё, чтобы таблица, фильтры и алерты видели одни и те же числа.
        """
        n = st.price_hist.maxlen or 900
        last = float(r["last"])
        if last <= 0:
            return
        natr = float(r.get("natr") or 0.5)
        sigma = max(natr, 0.05) / 100.0 * 0.12          # та же волатильность, что в _step
        # суммарный дрейф за окно берём из 4ч-доходности строки (или из chg, если её нет)
        total = float(r.get("r14400") or 0.0)
        drift_ps = (total / 100.0) / max(n, 1)
        now = time.time()
        path: list[float] = []
        px = last
        for _ in range(n):                              # идём назад от текущей цены
            px = px * (1.0 - drift_ps + rng.gauss(0, sigma))
            path.append(max(px, 1e-12))
        path.reverse()
        path[-1] = last
        for i, price in enumerate(path):
            st.price_hist.append(now - (n - 1 - i), price)
        # база для детекции спайков — чтобы она была с первой же минуты
        per_min = max(float(r.get("volq") or 0.0), 1e4) / 1440.0
        for _ in range(15):
            st.minute_vols.append(max(0.0, rng.lognormvariate(math.log(per_min + 1), 0.5)))
            # tr может быть None: Bybit/OKX число сделок не отдают, а снапшот
            # пишется из live-режима — делить None на 1440 нельзя
            per_min_tr = max((r.get("tr") or 1440) / 1440.0, 1.0)
            st.minute_trades.append(max(1, int(rng.lognormvariate(math.log(per_min_tr + 1), 0.5))))
        st.set_price(last, now)

        # Синтезируем 1m-свечи: без них окна 1ч/4ч нечем закрыть, потому что
        # in-memory история цен — всего 15 минут. Путь расчёта при этом тот же,
        # что и в live-режиме (ret → ret_ohlcv), поэтому демо честно отражает прод.
        st.apply_ohlcv(self._synth_candles(last, float(r.get("r14400") or r.get("chg") or 0.0),
                                           natr, rng,
                                           vol_per_min=max(float(r.get("volq") or 0.0), 1e3) / 1440.0),
                       period=14)
        r["natr"] = round(st.natr, 3) if st.natr else r.get("natr")

        # пересчитываем r* из реальной траектории и пишем обратно в строку
        for tf in self.settings.tf_seconds:
            v = st.ret(tf)
            if v is None and tf % 60 == 0:
                v = st.ret_ohlcv(tf // 60)
            r[f"r{tf}"] = round(v, 3) if v is not None else None

    def _synth_candles(self, last: float, total_pct: float, natr: float,
                       rng: random.Random, n: int = 400,
                       vol_per_min: float = 1000.0) -> list:
        """
        n одно-минутных свечей, заканчивающихся на цене last.

        Объём масштабируется реальным оборотом инструмента (vol_per_min),
        а не берётся из фиксированного распределения: иначе гистограмма объёма
        на графике одинакова у BTC и у ноунейм-альта и не сопоставима с
        суточным объёмом из тикера.
        """
        sigma = max(natr, 0.05) / 100.0 * math.sqrt(1.0)   # минутная волатильность
        drift = (total_pct / 100.0) / max(n, 1)
        # Метки времени выравниваем по границе минуты, и ПОСЛЕДНЯЯ свеча —
        # текущая (незакрытая) минута.
        #
        # Иначе на стыке возможен пропуск: если сид случился в 12:00:59.9,
        # последняя синтетическая свеча попадала в бакет 11:59:00, а первый
        # тик _roll_candle приходил уже на 12:01:00 — минута 12:00 пропадала,
        # и шаг свечей становился 120 000 мс вместо 60 000. На графике это
        # выглядело бы как разрыв, а тест непрерывности закономерно падал.
        now_ms = int(time.time() // 60) * 60_000
        start = last / (1.0 + total_pct / 100.0) if total_pct != -100 else last
        px = max(start, 1e-12)
        out: list = []
        for i in range(n):
            o = px
            c = max(o * (1.0 + drift + rng.gauss(0, sigma)), 1e-12)
            h = max(o, c) * (1.0 + abs(rng.gauss(0, sigma * 0.6)))
            lo = min(o, c) * (1.0 - abs(rng.gauss(0, sigma * 0.6)))
            # объём в OHLCV биржи отдают в БАЗОВОЙ монете (или контрактах),
            # а normalize_candle_volume() потом домножает его на close.
            # Поэтому здесь делим quote-оборот на цену, иначе объём
            # задваивается ценой и расходится с суточным в десятки раз.
            v = max(0.0, (vol_per_min / max(c, 1e-12)) * rng.lognormvariate(0.0, 0.7))
            out.append([now_ms - (n - 1 - i) * 60_000, o, h, lo, c, v])
            px = c
        out[-1][4] = last          # последняя свеча обязана закрыться на актуальной цене
        out[-1][2] = max(out[-1][2], last)
        out[-1][3] = min(out[-1][3], last)
        return out

    def _config_by_exchange(self) -> dict:
        """
        Настройки бирж по id и по label — чтобы replay проставлял те же атрибуты,
        что и live (dex, тип контракта, единицы объёма).

        Без этого в демо-режиме DEX-бейджи не появлялись вовсе: снапшот писался
        из live, но поле dex в нём могло отсутствовать, а _seed_store его
        не восстанавливал.
        """
        out: dict = {}
        for e in self.settings.exchanges:
            out[e.id] = e
            out[e.label] = e
        return out

    def _seed_store(self) -> None:
        rng = random.Random(4242)
        cfgs = self._config_by_exchange()
        for r in self.rows:
            st = STORE.get_or_create(r["ex"], r["exl"], r["mt"], r["s"], r["b"], r["q"])
            st.open24 = r["last"] / (1 + r["chg"] / 100) if r["chg"] != -100 else r["last"]
            st.high24, st.low24 = r["hi"], r["lo"]
            st.vol24_quote = r["volq"]
            st.trades24 = r.get("tr")
            cfg = cfgs.get(r["ex"]) or cfgs.get(r["exl"])
            if cfg is not None:
                # те же атрибуты, что ставит live-коллектор: иначе в демо
                # не будет ни DEX-метки, ни корректных единиц объёма
                st.dex = cfg.dex
                st.ohlcv_vol_in_contracts = cfg.ohlcv_vol_in_contracts
            st.natr = r["natr"] or 0.0
            if isinstance(r.get("dex"), bool):
                st.dex = r["dex"]
            st.funding = r.get("fund")
            st.open_interest = r.get("oi")
            st.open_interest_usd = r.get("oiusd")
            st.cvd = r.get("cvd") or 0.0
            st.set_price(r["last"], time.time())
            self._backfill_history(st, r, rng)
            st.touch()
        STORE.set_status("Replay", state="replay", symbols=len(self.rows), hot=len(self.rows))

    # ------------------------------------------------------------------
    async def _loop(self) -> None:
        rng = random.Random()
        tick = 1.0 / max(0.1, self.speed)
        while not self._stop.is_set():
            t0 = time.time()
            try:
                self._step(rng)
            except Exception as e:  # noqa: BLE001
                log.warning("replay step: %s", str(e)[:200])
            await asyncio.sleep(max(0.05, tick - (time.time() - t0)))

    def _step(self, rng: random.Random) -> None:
        now = time.time()
        symbols = STORE.all()
        for i, st in enumerate(symbols):
            if st.last <= 0:
                continue
            # волатильность пропорциональна NATR; редкие «выстрелы» дают спайки
            sigma = (st.natr or 0.5) / 100.0 * 0.12
            if rng.random() < 0.002:
                sigma *= rng.uniform(6, 20)
            drift = rng.gauss(0, sigma)
            st.set_price(max(st.last * (1 + drift), 1e-12), now)

            # прирост оборота за ОДИН тик = суточный объём / 86400 сек
            per_sec = max(st.vol24_quote, 1.0) / 86400.0
            inc = max(0.0, per_sec * rng.lognormvariate(0.0, 0.8))
            st.vol24_quote += inc
            st.vol24_usd = st.vol24_quote
            if st.trades24 is not None:      # None = биржа не отдаёт метрику
                st.trades24 += rng.randint(0, 12)
            if st.open24:
                st.change_pct = (st.last / st.open24 - 1.0) * 100.0
            st.high24 = max(st.high24, st.last)
            st.low24 = min(st.low24 or st.last, st.last)
            st.range_pct = (st.high24 / st.low24 - 1.0) * 100.0 if st.low24 else 0.0

            side = "buy" if drift > 0 else "sell"
            st.apply_trade({"amount": inc / max(st.last, 1e-12), "price": st.last,
                            "side": side, "timestamp": now * 1000})
            if st.open_interest is not None:
                st.open_interest *= 1 + rng.gauss(0, 0.002)
                if st.open_interest_usd:
                    st.open_interest_usd *= 1 + rng.gauss(0, 0.002)
            if rng.random() < 0.0008:
                ratio = rng.uniform(3.2, 14.0)
                st.add_spike(now, rng.choice(["volume", "trades"]), inc * ratio, inc, ratio)

            self._roll_candle(st, now, inc)

            # стакан синтезируем не всем и не каждый тик — это самая дорогая часть
            if (self._book_cursor + i) % BOOK_EVERY_N == 0:
                st.apply_book(self._synth_book(st, rng),
                              big_usd=self.settings.big_density_usd,
                              huge_usd=self.settings.huge_density_usd)
            st.touch()
        self._book_cursor += 1

    def _roll_candle(self, st, now: float, inc: float) -> None:
        """
        Обновляет последнюю 1m-свечу и заводит новую на границе минуты.

        `inc` приходит в валюте котировки (USD), а объём в OHLCV хранится
        в базовой монете — как отдают биржи. Без деления на цену объём свечи
        раздувается в ~price раз (для BTC — в 80 с лишним тысяч), и гистограмма
        на графике расходится с суточным объёмом на порядки.
        """
        if not st.ohlcv:
            return
        inc_base = inc / st.last if st.last else 0.0
        minute_ms = int(now - (now % 60)) * 1000
        cur = st.ohlcv[-1]
        if cur[0] < minute_ms:                       # минута сменилась → новая свеча
            st.ohlcv.append([minute_ms, st.last, st.last, st.last, st.last, inc_base])
            if len(st.ohlcv) > 400:
                del st.ohlcv[0]
            st.natr = natr(st.ohlcv, st.natr_period)   # пересчёт волатильности
        else:
            cur[4] = st.last
            cur[2] = max(cur[2], st.last)
            cur[3] = min(cur[3], st.last)
            cur[5] = float(cur[5]) + inc_base

    def _synth_book(self, st, rng: random.Random) -> dict:
        """
        Правдоподобная лестница цен вокруг last + периодические «стены».
        Проходит через тот же apply_book(), что и live-данные, поэтому
        плотности и дисбаланс считаются одинаково в обоих режимах.
        """
        px = st.last
        if px <= 0:
            return {"bids": [], "asks": []}
        tick = max(px * 0.0002, 1e-9)
        depth = 60
        # типичный уровень: 0.05–0.6% суточного объёма, распределён по 300 уровням
        unit = max(st.vol24_quote, 1e4) / 300.0
        bids, asks = [], []
        wall_side = "bid" if rng.random() < 0.5 else "ask"
        wall_at = rng.randint(4, depth - 6)
        for i in range(1, depth + 1):
            for side, out in (("bid", bids), ("ask", asks)):
                p = px - i * tick if side == "bid" else px + i * tick
                if p <= 0:
                    continue
                q = unit * rng.lognormvariate(-1.2, 0.9) / max(p, 1e-12)
                if i == wall_at and side == wall_side:
                    # стена: заметно выше порога «крупной плотности»
                    q = self.settings.big_density_usd * rng.uniform(1.3, 6.0) / p
                out.append([p, q])
        return {"bids": bids, "asks": asks, "timestamp": time.time() * 1000}
