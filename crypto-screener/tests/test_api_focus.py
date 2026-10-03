"""
API фокус-режима: /api/focus, /api/grid и WS-пуш в режиме «одна биржа + top-N».

Тесты идут в replay-режиме (conftest выставляет MODE=replay): коллекторов нет,
поэтому FocusManager работает в «виртуальном» режиме — отбор считается по
строкам хранилища, а подписки не трогаются. Это ровно тот код-путь, который
видит пользователь без бирж, и он же проверяет ограничение пуша и сетки.
Поведение с реальными коллекторами покрыто в tests/test_focus.py.
"""
import json

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client():
    from app.api import app
    with TestClient(app) as c:      # TestClient прогоняет lifespan
        yield c


@pytest.fixture(autouse=True)
def focus_off():
    """Фокус — состояние сервера: после каждого теста возвращаем полный режим."""
    from app.focus import get_focus
    yield
    get_focus().enabled = False
    get_focus().symbols, get_focus().keys, get_focus().key_set = [], [], set()


def post_focus(client, **kw):
    return client.post("/api/focus", json=kw)


# --------------------------------------------------------------------------
def test_meta_exposes_focus_block(client):
    d = client.get("/api/meta").json()
    assert "focus" in d
    assert {"enabled", "limit", "interval", "ex", "mt"} <= set(d["focus"])
    assert d["focus"]["limit"] == 50, "дефолт — 50 монет"
    assert d["focus"]["interval"] == 15.0, "дефолт — перепроверка раз в 15 секунд"


def test_focus_off_by_default(client):
    d = client.get("/api/focus").json()
    assert d["enabled"] is False
    assert d["keys"] == []


def test_enable_focus_selects_top_n(client):
    r = post_focus(client, ex="Binance", mt="swap", limit=20, interval=15)
    assert r.status_code == 200
    d = r.json()
    assert d["enabled"] is True
    assert d["count"] == 20
    assert len(d["keys"]) == 20
    assert all(k.startswith("binanceusdm:") for k in d["keys"])
    # replay: коллекторов нет → отбор виртуальный, но для UI рабочий
    assert d["virtual"] is True
    assert d["next_in"] is not None and 0 < d["next_in"] <= 15.0


def test_focus_default_exchange_when_empty(client):
    d = post_focus(client, ex="", mt="swap", limit=10).json()
    assert d["ex"], "сервер обязан сам выбрать биржу, если чипс не нажат"
    assert d["count"] == 10


def test_focus_params_clamped(client):
    d = post_focus(client, ex="Binance", mt="swap", limit=100000, interval=0.01).json()
    assert d["limit"] == 200, "иначе «фокус» неотличим от полного профиля"
    assert d["interval"] == 5.0, "чаще 5 секунд — долбёжка биржи"

    d = post_focus(client, ex="Binance", mt="swap", limit=1, interval=99999).json()
    assert d["limit"] == 5
    assert d["interval"] == 300.0


def test_focus_rejects_garbage_body(client):
    r = client.post("/api/focus", content=b"{not json",
                    headers={"Content-Type": "application/json"})
    assert r.status_code == 400
    assert "JSON" in r.json()["error"]

    r = client.post("/api/focus", json=["not", "an", "object"])
    assert r.status_code == 400


def test_focus_applies_filter_not_just_volume(client):
    """Фильтр по волатильности в replay: NATR есть не у всех строк снапшота."""
    # limit большой намеренно: пул кандидатов ранжируется прокси-метрикой
    # (диапазон 24ч), и в отбор попадают среднекапы за пределами топ-500
    # по объёму — усечённая выборка ссыпала бы тест на легитимных строках
    all_rows = client.get("/api/screener?ex=Binance&mt=swap&limit=5000").json()["rows"]
    with_natr = [r for r in all_rows if r.get("natr")]
    if not with_natr:
        pytest.skip("в снапшоте нет строк с NATR")

    d = post_focus(client, ex="Binance", mt="swap", limit=50, sort="natr",
                   query={"natr_min": 0.001, "ex": "Binance", "mt": "swap"}).json()
    keys = set(d["keys"])
    assert keys, "отбор по волатильности вернул пустоту"
    assert keys <= {r["k"] for r in with_natr}, \
        "в отбор попали монеты без NATR — фильтр проигнорирован"
    assert d["count"] <= 50


def test_screener_table_keeps_full_universe_in_focus(client):
    """
    Прямое требование: таблица скринера показывает ВСЮ биржу (тикеры дешёвые),
    а стримится только отобранный top-N. Иначе «фокус» превратил бы скринер
    в таблицу из 50 строк и искать монеты стало бы негде.
    """
    post_focus(client, ex="Binance", mt="swap", limit=10)
    d = client.get("/api/screener?ex=Binance&mt=swap&limit=2000").json()
    assert len(d["rows"]) > 100, f"в таблице всего {len(d['rows'])} строк — вселенная сжата"
    assert "focus" not in d["meta"], "REST-скринер не должен резаться по фокусу"

    heat = client.get("/api/heatmap?ex=Binance&mt=swap&limit=2000").json()
    assert heat["total"] > 100, "карта рынка тоже обязана показывать всю биржу"


def test_grid_uses_focus_selection(client):
    post_focus(client, ex="Binance", mt="swap", limit=12, sort="vol")
    focus_keys = set(client.get("/api/focus").json()["keys"])
    assert focus_keys

    d = client.get("/api/grid?ex=Binance&mt=swap&n=6&tf=5m").json()
    assert d["focus"]["on"] is True
    assert d["focus"]["count"] == 12
    assert len(d["cells"]) == 6
    cell_keys = [c["k"] for c in d["cells"]]
    assert set(cell_keys) <= focus_keys, "сетка взяла монеты вне отбора"
    # порядок ячеек = порядок отбора, а не произвольный топ по объёму
    ordered = [k for k in client.get("/api/focus").json()["keys"] if k in set(cell_keys)]
    assert cell_keys == ordered[: len(cell_keys)] or set(cell_keys) == set(ordered[:6])


def test_grid_without_focus_is_unchanged(client):
    client.delete("/api/focus")
    d = client.get("/api/grid?ex=Binance&mt=swap&n=4&tf=5m").json()
    assert d["focus"]["on"] is False
    assert len(d["cells"]) == 4


def test_disable_focus_restores_full_mode(client):
    post_focus(client, ex="Binance", mt="swap", limit=10)
    assert client.get("/api/focus").json()["enabled"] is True

    d = client.delete("/api/focus").json()
    assert d["enabled"] is False
    assert d["keys"] == []
    assert d["count"] == 0
    assert client.get("/api/grid?ex=Binance&mt=swap&n=4").json()["focus"]["on"] is False


def test_ws_push_restricted_to_focus_set(client):
    """WS-пуш — самое дорогое место: в фокусе он обязан слать только отобранные монеты."""
    post_focus(client, ex="Binance", mt="swap", limit=10)
    keys = set(client.get("/api/focus").json()["keys"])

    with client.websocket_connect("/ws") as ws:
        assert json.loads(ws.receive_text())["type"] == "hello"
        ws.send_text(json.dumps({"type": "filters",
                                 "query": {"sort": "vol", "limit": "500",
                                           "ex": "Binance", "mt": "swap"}}))
        msg = None
        for _ in range(6):
            m = json.loads(ws.receive_text())
            if m["type"] == "rows":
                msg = m
                break
        assert msg, "не дождались пакета rows"
        assert msg["meta"].get("focus") is True
        assert msg["meta"]["focus_count"] == len(keys)
        assert len(msg["rows"]) <= 10
        got = {r["k"] for r in msg["rows"]}
        assert got <= keys, f"пуш отдаёт монеты вне фокуса: {got - keys}"


def test_ws_push_base_mode_keeps_full_universe(client):
    """
    base=1 (его шлёт фронтенд в фокус-режиме): таблица и карта рынка кормятся
    из REST, а пуш нужен только для живых тиков — сужать его нельзя, иначе
    «фокус» схлопнул бы всю таблицу до top-N вопреки замыслу.
    """
    post_focus(client, ex="Binance", mt="swap", limit=10)
    rest_total = len(client.get("/api/screener?ex=Binance&mt=swap&limit=2000").json()["rows"])
    assert rest_total > 100

    with client.websocket_connect("/ws") as ws:
        assert json.loads(ws.receive_text())["type"] == "hello"
        ws.send_text(json.dumps({"type": "filters", "base": True,
                                 "query": {"sort": "vol", "limit": "2000",
                                           "ex": "Binance", "mt": "swap"}}))
        msg = None
        for _ in range(6):
            m = json.loads(ws.receive_text())
            if m["type"] == "rows":
                msg = m
                break
        assert msg, "не дождались rows"
        assert "focus" not in msg["meta"], "base-режим не должен сужать выборку"
        assert msg["meta"]["focus_count"] == 10, "число стримов клиенту всё равно нужно"
        assert len(msg["rows"]) > 100, f"пришло {len(msg['rows'])} строк вместо полной вселенной"


def test_ws_push_full_after_disable(client):
    client.delete("/api/focus")
    with client.websocket_connect("/ws") as ws:
        assert json.loads(ws.receive_text())["type"] == "hello"
        ws.send_text(json.dumps({"type": "filters",
                                 "query": {"sort": "vol", "limit": "300", "mt": "swap"}}))
        for _ in range(6):
            m = json.loads(ws.receive_text())
            if m["type"] == "rows":
                assert "focus" not in m["meta"]
                assert len(m["rows"]) > 10, "полный режим обязан отдавать больше, чем фокус"
                break
        else:
            pytest.fail("не дождались rows")
