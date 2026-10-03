"""
Общие фикстуры. Важно: настройки читаются на импорте модуля, поэтому
переменные окружения выставляются ДО первого импорта app.*.
"""
import os
import sys
from pathlib import Path

os.environ.setdefault("MODE", "replay")
os.environ.setdefault("PORT", "8099")
os.environ.setdefault("EXCHANGES", "binanceusdm,bybit")
os.environ.setdefault("TOP_N", "40")
os.environ.setdefault("BOOKS", "10")
os.environ.setdefault("FETCH_OHLCV", "0")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest  # noqa: E402


@pytest.fixture
def sym():
    """Чистый SymbolState для линейного USDT-контракта."""
    from app.metrics import SymbolState
    st = SymbolState("binanceusdm", "Binance Futures", "swap",
                     "BTC/USDT:USDT", "BTC", "USDT")
    st.set_contract(inverse=False, contract_size=1.0)
    return st


@pytest.fixture
def inv():
    """Чистый SymbolState для инверсного (coin-margined) контракта."""
    from app.metrics import SymbolState
    st = SymbolState("okx", "OKX", "swap", "BTC/USD:BTC", "BTC", "USD")
    st.set_contract(inverse=True, contract_size=100.0)
    return st
