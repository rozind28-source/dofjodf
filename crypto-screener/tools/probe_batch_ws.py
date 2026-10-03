"""
Живой зонд батчевых WS-подписок ccxt.pro (одноразовый инструмент проверки).

Проверяет на реальных биржах семантику, на которую опирается коллектор:
  * watch_order_book_for_symbols → один стакан за вызов, book['symbol'] заполнен;
  * watch_trades_for_symbols     → список сделок, у каждой trade['symbol'];
  * watch_tickers(symbols)       → dict {symbol: ticker};
  * un_watch_*_for_symbols       → отписка не ломает соединение.

Запуск:  python tools/probe_batch_ws.py [exchange_id ...]
По умолчанию: binanceusdm bybit okx gate aster
"""
from __future__ import annotations

import asyncio
import sys
import time

import ccxt
import ccxt.pro as ccxtpro


async def probe(exchange_id: str) -> None:
    cls = getattr(ccxtpro, exchange_id)
    ex = cls({"enableRateLimit": True, "timeout": 15000})
    ex.options["defaultType"] = "spot" if exchange_id == "binance" else "swap"
    print(f"\n=== {exchange_id} (ccxt {ccxt.__version__}) ===")
    try:
        await ex.load_markets()
        # берём три ликвидных символа нужного рынка
        want = "spot" if exchange_id in ("binance",) else "swap"
        syms = []
        for s, m in ex.markets.items():
            if not m.get("active", True):
                continue
            if not m.get(want):
                continue
            if m.get("quote") not in ("USDT", "USDC"):
                continue
            if m.get("base") in ("BTC", "ETH", "SOL", "DOGE", "XRP", "BNB"):
                syms.append(s)
            if len(syms) >= 3:
                break
        print("символы:", syms)

        # --- стаканы ---
        if ex.has.get("watchOrderBookForSymbols"):
            seen_books: dict[str, int] = {}
            t0 = time.time()
            # глубины ровно как в app/config.py (ExchangeConfig.book_limit):
            # у OKX limit=50 уехал бы в VIP-канал books50-l2-tbt (AuthenticationError),
            # а 200 → публичный канал books (400 уровней)
            limit = {"binanceusdm": 100, "binance": 100, "gate": 100}.get(exchange_id, 200)
            while time.time() - t0 < 8 and len(seen_books) < len(syms):
                try:
                    book = await asyncio.wait_for(
                        ex.watch_order_book_for_symbols(syms, limit=limit), timeout=10)
                except asyncio.TimeoutError:
                    print("  books: ТАЙМАУТ"); break
                sym = book.get("symbol")
                seen_books[sym] = seen_books.get(sym, 0) + 1
            print(f"  books: за {time.time()-t0:.1f}c пришли стаканы {seen_books} "
                  f"(symbol заполнен: {all(seen_books)})")
            try:
                await ex.un_watch_order_book_for_symbols(syms[:1])
                print("  un_watch_order_book_for_symbols: OK")
            except Exception as e:
                print("  unwatch books:", type(e).__name__, str(e)[:80])
        else:
            print("  books: watchOrderBookForSymbols НЕТ")

        # --- сделки ---
        if ex.has.get("watchTradesForSymbols"):
            seen_trades: dict[str, int] = {}
            t0 = time.time()
            while time.time() - t0 < 6 and sum(seen_trades.values()) < 15:
                try:
                    trades = await asyncio.wait_for(
                        ex.watch_trades_for_symbols(syms), timeout=10)
                except asyncio.TimeoutError:
                    print("  trades: ТАЙМАУТ"); break
                for t in trades:
                    s = t.get("symbol")
                    seen_trades[s] = seen_trades.get(s, 0) + 1
            print(f"  trades: {seen_trades} (symbol заполнен: {all(seen_trades)})")
            try:
                await ex.un_watch_trades_for_symbols(syms[:1])
                print("  un_watch_trades_for_symbols: OK")
            except Exception as e:
                print("  unwatch trades:", type(e).__name__, str(e)[:80])
        else:
            print("  trades: watchTradesForSymbols НЕТ")

        # --- тикеры ---
        if ex.has.get("watchTickers"):
            seen_tk: dict[str, int] = {}
            t0 = time.time()
            while time.time() - t0 < 5 and len(seen_tk) < len(syms):
                try:
                    tickers = await asyncio.wait_for(ex.watch_tickers(syms), timeout=8)
                except asyncio.TimeoutError:
                    print("  tickers: ТАЙМАУТ"); break
                if not isinstance(tickers, dict):
                    print("  tickers: НЕ dict:", type(tickers)); break
                for s in tickers:
                    seen_tk[s] = seen_tk.get(s, 0) + 1
            print(f"  tickers: {seen_tk}")
        else:
            print("  tickers: watchTickers НЕТ")
    except Exception as e:
        print("  ОШИБКА:", type(e).__name__, str(e)[:200])
    finally:
        try:
            await ex.close()
        except Exception as e:  # noqa: BLE001
            print("  (close:", type(e).__name__, str(e)[:60], ")")


async def main() -> None:
    ids = sys.argv[1:] or ["binanceusdm", "bybit", "okx", "gate", "aster"]
    for i in ids:
        await probe(i)


if __name__ == "__main__":
    asyncio.run(main())
