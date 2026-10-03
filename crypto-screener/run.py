#!/usr/bin/env python3
"""
Точка входа.

  python run.py                 # live-режим: реальные данные с бирж
  python run.py --doctor        # диагностика: DNS / TLS / HTTP / ccxt по каждой бирже
  MODE=replay python run.py     # демо из снапшота/синтетики, без интернета
  EXCHANGES=bybit,okx TOP_N=120 python run.py

Windows PowerShell:
  $env:MODE='replay'; python run.py
  python doctor.py

Полный список переменных — в README и app/config.py.
"""
import logging
import os
import sys


def main() -> None:
    if "--doctor" in sys.argv or "-d" in sys.argv:
        import asyncio

        from doctor import main as doctor_main
        sys.exit(asyncio.run(doctor_main([a for a in sys.argv[1:] if not a.startswith("-")])))

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-7s %(name)-10s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    for noisy in ("asyncio", "websockets", "ccxt", "urllib3", "httpx"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    # Проверка зависимостей ДО импорта всего остального: иначе отсутствие ccxt
    # выглядит как голый traceback из недр коллектора, а старая версия ccxt —
    # как «Aster не подключается», хотя дело в версии библиотеки.
    from app.preflight import ensure_or_exit, format_report
    rep = ensure_or_exit()
    for w in rep.warnings:
        logging.warning("зависимости: %s", w.text)

    import uvicorn
    from app.config import SETTINGS

    ex = ", ".join(f"{e.label}(top {e.top_n}/books {e.books})" for e in SETTINGS.exchanges) or "нет"
    logging.info("режим=%s порт=%s биржи=%s", SETTINGS.mode, SETTINGS.port, ex)
    if not SETTINGS.exchanges and SETTINGS.mode == "live":
        logging.warning("список бирж пуст — скринер не будет получать данные")

    uvicorn.run("app.api:app", host=SETTINGS.host, port=SETTINGS.port,
                log_level=os.getenv("LOG_LEVEL", "info").lower(), access_log=False)


if __name__ == "__main__":
    main()
