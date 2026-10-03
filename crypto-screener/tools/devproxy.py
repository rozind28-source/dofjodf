#!/usr/bin/env python3
"""
Минимальный HTTP CONNECT-прокси для локальной проверки PROXY_URL.

Зачем живёт в репозитории: исправление apply_proxy (httpsProxy + wssProxy
вместо конфликтующих httpProxy+httpsProxy) проверялось не только юнит-тестами,
но и сквозным прогоном doctor.py и watch_tickers через настоящий прокси —
этот скрипт и был тем прокси.

    python tools/devproxy.py 8899          # слушать 127.0.0.1:8899
    $env:PROXY_URL="http://127.0.0.1:8899"; python doctor.py binanceusdm

Поддерживает только CONNECT (туннель для https/wss) — именно так ходят
aiohttp/ccxt, когда задан прокси. Обычные GET «в лоб» не обслуживаются.
"""
from __future__ import annotations

import asyncio
import sys


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def handle(client_r: asyncio.StreamReader, client_w: asyncio.StreamWriter) -> None:
    try:
        line = await asyncio.wait_for(client_r.readline(), 10)
        parts = line.decode(errors="ignore").split()
        if len(parts) < 2 or parts[0].upper() != "CONNECT":
            client_w.write(b"HTTP/1.1 405 Only CONNECT supported\r\n\r\n")
            await client_w.drain()
            client_w.close()
            return
        # дочитываем заголовки до пустой строки
        while True:
            h = await asyncio.wait_for(client_r.readline(), 10)
            if h in (b"\r\n", b"\n", b""):
                break
        host, _, port = parts[1].partition(":")
        remote_r, remote_w = await asyncio.wait_for(
            asyncio.open_connection(host, int(port or 443)), 10)
        client_w.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        await client_w.drain()
        await asyncio.gather(_pipe(client_r, remote_w), _pipe(remote_r, client_w))
    except Exception as e:
        print(f"[devproxy] {type(e).__name__}: {e}", flush=True)
        try:
            client_w.close()
        except Exception:
            pass


async def main(port: int) -> None:
    server = await asyncio.start_server(handle, "127.0.0.1", port)
    print(f"[devproxy] CONNECT-прокси на 127.0.0.1:{port}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    p = int(sys.argv[1]) if len(sys.argv) > 1 else 8899
    try:
        asyncio.run(main(p))
    except KeyboardInterrupt:
        pass
