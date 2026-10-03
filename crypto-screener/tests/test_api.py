"""
Интеграционные тесты API. Запускаются в replay-режиме: данные детерминированные,
сеть не нужна, а код-путь тот же, что и в live (select → build_rows → to_row).
"""
import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client():
    from app.api import app
    with TestClient(app) as c:      # TestClient прогоняет lifespan → replay стартует
        yield c


# --------------------------------------------------------------------------
def test_healthz(client):
    d = client.get("/healthz").json()
    assert d["ok"] is True
    assert d["mode"] == "replay"
    assert d["symbols"] > 0


def test_meta_describes_ui(client):
    d = client.get("/api/meta").json()
    assert len(d["filters"]) >= 15
    assert {"field", "label", "kind", "group"} <= set(d["filters"][0])
    assert d["presets"] and d["sorts"] and d["exchanges"]
    assert d["mode"] == "replay"
    kinds = {f["kind"] for f in d["filters"]}
    assert kinds == {"range", "flag"}


def test_overview_consistent(client):
    d = client.get("/api/overview").json()
    assert d["up"] + d["down"] + d["flat"] == d["symbols"]
    assert d["volume_usd"] > 0


# --------------------------------------------------------------------------
def test_screener_default_sorted_by_volume_desc(client):
    d = client.get("/api/screener").json()
    vols = [r["vol"] for r in d["rows"]]
    assert vols == sorted(vols, reverse=True)
    assert d["meta"]["sort"] == "vol"


def test_screener_limit(client):
    assert len(client.get("/api/screener?limit=7").json()["rows"]) == 7


def test_screener_limit_clamped(client):
    d = client.get("/api/screener?limit=999999").json()
    assert d["meta"]["limit"] == 5000


def test_screener_sort_asc(client):
    d = client.get("/api/screener?sort=chg&desc=0&limit=20").json()
    vals = [r["chg"] for r in d["rows"]]
    assert vals == sorted(vals)


def test_screener_range_filter(client):
    d = client.get("/api/screener?vol_min=1000000&limit=500").json()
    assert d["rows"], "фильтр отсёк всё — порог слишком высокий для синтетики"
    assert all(r["vol"] >= 1_000_000 for r in d["rows"])


def test_screener_exchange_filter(client):
    meta = client.get("/api/meta").json()
    ex = meta["exchanges"][0]
    d = client.get("/api/screener", params={"ex": ex, "limit": 500}).json()
    assert d["rows"] and all(r["exl"] == ex for r in d["rows"])


def test_screener_search(client):
    d = client.get("/api/screener?q=BTC&limit=500").json()
    assert d["rows"] and all("BTC" in r["s"].upper() for r in d["rows"])


def test_screener_flag_green(client):
    d = client.get("/api/screener?green=1&limit=500").json()
    assert d["rows"] and all(r["chg"] > 0 for r in d["rows"])


def test_screener_densities_flag_populates_field(client):
    d = client.get("/api/screener?dens=1&limit=50").json()
    for r in d["rows"]:
        assert "dens" in r and r["dens"], "dens=1 обязан подмешивать плотности"


def test_screener_multi_tf_fields_present(client):
    r = client.get("/api/screener?limit=1").json()["rows"][0]
    for tf in (60, 300, 900, 3600, 14400):
        assert f"r{tf}" in r


def test_multi_tf_values_are_distinct(client):
    """
    Регрессия: когда-то все таймфреймы показывали одно число, потому что
    история цен короче окна, а ret() не проверял покрытие.
    """
    rows = client.get("/api/screener?limit=25").json()["rows"]
    distinct = 0
    for r in rows:
        vals = [r[f"r{tf}"] for tf in (60, 300, 900, 3600, 14400) if r[f"r{tf}"] is not None]
        if len(set(vals)) >= 3:
            distinct += 1
    assert distinct >= len(rows) * 0.6, f"ТФ не различаются у {len(rows)-distinct} строк"


# --------------------------------------------------------------------------
def test_heatmap_compact_and_sorted(client):
    d = client.get("/api/heatmap?limit=40").json()
    assert 0 < d["total"] <= 40
    vols = [t["vol"] for t in d["tiles"]]
    assert vols == sorted(vols, reverse=True)
    assert {"k", "s", "chg", "vol", "last", "spike"} <= set(d["tiles"][0])


def test_densities_endpoint(client):
    d = client.get("/api/densities?min_usd=10000&limit=20").json()
    assert d["threshold"] == 10000
    for r in d["rows"]:
        assert r["q"] >= 10000
        assert r["sd"] in ("bid", "ask")
    qs = [r["q"] for r in d["rows"]]
    assert qs == sorted(qs, reverse=True)


def test_densities_side_filter(client):
    d = client.get("/api/densities?min_usd=1&side=bid&limit=50").json()
    assert all(r["sd"] == "bid" for r in d["rows"])


# --------------------------------------------------------------------------
def test_symbol_detail(client):
    k = client.get("/api/screener?limit=1").json()["rows"][0]["k"]
    d = client.get("/api/symbol", params={"key": k}).json()
    assert d["k"] == k
    assert "spark" in d and "book" in d and "densities" in d and "spikes" in d
    assert d["last"] > 0


def test_symbol_404(client):
    r = client.get("/api/symbol", params={"key": "nope:NOPE/USDT"})
    assert r.status_code == 404


# --------------------------------------------------------------------------
# Сетка графиков (многоэкранный режим)
# --------------------------------------------------------------------------
def test_grid_returns_cells_with_candles(client):
    d = client.get("/api/grid", params={"mt": "swap", "tf": "5m", "n": 4}).json()
    assert 0 < len(d["cells"]) <= 4
    for c in d["cells"]:
        assert {"k", "s", "b", "exl", "mt", "last", "chg", "candles"} <= set(c)
        assert c["mt"] == "swap"
        assert len(c["candles"]) > 0, f"у {c['s']} пустые свечи"
        for cd in c["candles"]:
            assert len(cd) == 6
            assert cd[2] >= max(cd[1], cd[4]) and cd[3] <= min(cd[1], cd[4])


def test_grid_respects_market_type(client):
    for mt in ("spot", "swap"):
        d = client.get("/api/grid", params={"mt": mt, "n": 5}).json()
        assert all(c["mt"] == mt for c in d["cells"]), f"в {mt}-сетке чужой рынок"


def test_grid_filters_by_exchange(client):
    d = client.get("/api/grid", params={"mt": "swap", "n": 6}).json()
    if not d["cells"]:
        pytest.skip("нет данных")
    ex = d["cells"][0]["exl"]
    d2 = client.get("/api/grid", params={"mt": "swap", "n": 6, "ex": ex}).json()
    assert d2["cells"] and all(c["exl"] == ex for c in d2["cells"])


def test_grid_available_exchanges_matches_market(client):
    for mt in ("spot", "swap"):
        d = client.get("/api/grid", params={"mt": mt, "n": 2}).json()
        for ex in d["available_exchanges"]:
            sub = client.get("/api/grid", params={"mt": mt, "n": 3, "ex": ex}).json()
            assert sub["cells"], f"{ex} заявлен для {mt}, но ячеек не дал"


def test_grid_n_clamped(client):
    assert len(client.get("/api/grid", params={"n": 9999}).json()["cells"]) <= 25
    assert len(client.get("/api/grid", params={"n": 0}).json()["cells"]) >= 1


def test_grid_tf_validated(client):
    assert client.get("/api/grid", params={"tf": "7m"}).status_code == 400
    d = client.get("/api/grid", params={"tf": "1h", "n": 2}).json()
    assert d["tf_seconds"] == 3600


def test_grid_sorted_by_volume(client):
    d = client.get("/api/grid", params={"mt": "swap", "n": 6, "sort": "vol"}).json()
    vols = [c["vol"] for c in d["cells"]]
    assert vols == sorted(vols, reverse=True)


# --------------------------------------------------------------------------
# Графики
# --------------------------------------------------------------------------
def _first_key(client):
    return client.get("/api/screener?limit=1").json()["rows"][0]["k"]


def test_candles_returns_ohlc(client):
    k = _first_key(client)
    d = client.get("/api/candles", params={"key": k, "tf": "15m", "limit": 100}).json()
    assert d["key"] == k and d["tf"] == "15m"
    assert len(d["candles"]) > 5
    for c in d["candles"]:
        assert len(c) == 6, "свеча = [ts, o, h, l, c, v]"
        ts, o, h, l, cl, v = c
        assert h >= max(o, cl) and l <= min(o, cl), f"OHLC несогласован: {c}"
        assert v >= 0


def test_candles_sorted_by_time_and_unique(client):
    k = _first_key(client)
    d = client.get("/api/candles", params={"key": k, "tf": "1m", "limit": 200}).json()
    ts = [c[0] for c in d["candles"]]
    assert ts == sorted(ts), "свечи должны идти по возрастанию времени"
    assert len(ts) == len(set(ts)), "дубликаты свечей сломают график"


def test_candles_step_matches_timeframe(client):
    """Шаг свечей обязан соответствовать запрошенному ТФ (иначе ресэмплинг врёт)."""
    k = _first_key(client)
    for tf, sec in (("1m", 60), ("5m", 300), ("15m", 900)):
        d = client.get("/api/candles", params={"key": k, "tf": tf, "limit": 200}).json()
        assert d["tf_seconds"] == sec
        ts = [c[0] for c in d["candles"]]
        if len(ts) < 3:
            continue
        steps = {b - a for a, b in zip(ts, ts[1:])}
        # пропусков быть не должно: разрыв на графике = свеча с шагом 2×ТФ
        assert steps <= {sec * 1000}, \
            f"{tf}: разрыв в свечах, шаги {sorted(steps)} (ожидался только {sec * 1000})"
        assert all(t % (sec * 1000) == 0 for t in ts), f"{tf}: свечи не выровнены по границе"


def test_candles_volume_in_usd(client):
    """
    Объём приводится к USD: у синтетики 1m-объём должен быть того же порядка,
    что и суточный/1440, а не отличаться в 10 000 раз (ошибка единиц).
    """
    k = _first_key(client)
    d = client.get("/api/candles", params={"key": k, "tf": "1m", "limit": 120}).json()
    sym = client.get("/api/symbol", params={"key": k}).json()
    vols = [c[5] for c in d["candles"] if c[5] > 0]
    if not vols or not sym.get("vol"):
        pytest.skip("нет данных объёма")
    per_min = sum(vols) / len(vols)
    expected = sym["vol"] / 1440.0
    assert 0.02 < per_min / expected < 50, \
        f"объём свечи {per_min:.0f} несопоставим с суточным/1440={expected:.0f}"


def test_candles_all_timeframes(client):
    k = _first_key(client)
    for tf in ("1m", "5m", "15m", "1h", "4h", "1d"):
        d = client.get("/api/candles", params={"key": k, "tf": tf, "limit": 100}).json()
        assert d["candles"], f"{tf}: пусто"


def test_candles_rejects_bad_timeframe(client):
    k = _first_key(client)
    r = client.get("/api/candles", params={"key": k, "tf": "7m"})
    assert r.status_code == 400
    assert "supported" in r.json()


def test_candles_404_for_unknown_key(client):
    assert client.get("/api/candles", params={"key": "nope:X/Y", "tf": "1m"}).status_code == 404


def test_candles_limit_clamped(client):
    k = _first_key(client)
    d = client.get("/api/candles", params={"key": k, "tf": "1m", "limit": 999999}).json()
    assert len(d["candles"]) <= 1000


def test_candles_carry_overlays(client):
    """График рисует плотности и спайки поверх свечей — они должны приезжать."""
    k = _first_key(client)
    d = client.get("/api/candles", params={"key": k, "tf": "15m"}).json()
    assert "densities" in d and "spikes" in d and "last" in d


def test_chart_js_served(client):
    r = client.get("/chart.js")
    assert r.status_code == 200
    assert "CandleChart" in r.text


def test_index_references_chart_relative(client):
    """Относительные пути: index.html обязан работать и открытым напрямую."""
    html = client.get("/").text
    assert 'src="chart.js"' in html and 'src="app.js"' in html
    assert "/static/" not in html


# --------------------------------------------------------------------------
# Биржи и DEX-метки в выдаче
# --------------------------------------------------------------------------
def test_meta_exchanges_consistent_with_exchange_info(client):
    """
    Список бирж и их атрибуты обязаны совпадать.

    Раньше `exchanges` строился из хранилища, а `exchange_info` — из конфига,
    и в replay они расходились (4 против 8): чипсы рисовались по одному
    источнику, а DEX-метки искались в другом.
    """
    m = client.get("/api/meta").json()
    assert set(m["exchanges"]) == set(m["exchange_info"]), \
        f"расхождение: {set(m['exchanges']) ^ set(m['exchange_info'])}"
    for label, info in m["exchange_info"].items():
        assert {"ids", "markets", "dex", "has_spot", "has_swap", "configured"} <= set(info), \
            f"{label}: неполная схема exchange_info: {sorted(info)}"
        assert isinstance(info["dex"], bool)
        # markets и флаги has_* обязаны быть согласованы
        assert info["has_spot"] == ("spot" in info["markets"])
        assert info["has_swap"] == ("swap" in info["markets"])
        # у подключённой биржи каждый заявленный рынок имеет id коллектора;
        # у биржи только из данных (старый снапшот) рынков и id быть не должно
        if info["configured"]:
            for mk in info["markets"]:
                assert mk in info["ids"], f"{label}: рынок {mk} без id коллектора"
        else:
            assert info["markets"] == [] and info["ids"] == {}


def test_dex_flag_present_in_every_row(client):
    rows = client.get("/api/screener?limit=200").json()["rows"]
    assert rows
    for r in rows:
        assert "dex" in r and isinstance(r["dex"], bool)


def test_dex_flag_matches_config(client):
    """
    Метка DEX обязана совпадать с конфигом — и не быть всегда False.

    Сверяем только биржи, которые объявлены в конфиге: в replay-режиме данные
    могут прийти из снапшота, записанного с другим набором бирж, и тогда в
    хранилище legitimately есть биржи вне текущего конфига. Требовать от meta
    ровно конфигного набора было бы неверно.
    """
    from app.config import SETTINGS
    m = client.get("/api/meta").json()
    by_label = {e.label: e for e in SETTINGS.exchanges}
    checked = 0
    for label, info in m["exchange_info"].items():
        cfg = by_label.get(label)
        if cfg is None:
            continue
        assert info["dex"] is cfg.dex, \
            f"{label}: в meta dex={info['dex']}, в конфиге {cfg.dex}"
        checked += 1
    assert checked > 0, "ни одна биржа из конфига не попала в meta"

    # строки обязаны быть согласованы с exchange_info — иначе бейдж в таблице
    # и метка в чипсе могут разойтись
    rows = client.get("/api/screener?limit=3000").json()["rows"]
    for r in rows:
        info = m["exchange_info"].get(r["exl"])
        if info is not None and r["exl"] in by_label:
            assert r["dex"] == info["dex"], \
                f"{r['s']} на {r['exl']}: dex={r['dex']} в строке, {info['dex']} в meta"


def test_dex_rows_exist_when_dex_exchange_configured(client):
    """Если DEX-биржа подключена, её инструменты обязаны нести dex=true."""
    from app.config import SETTINGS
    dex_labels = {e.label for e in SETTINGS.exchanges if e.dex}
    if not dex_labels:
        pytest.skip("в этом профиле DEX-биржи не подключены")
    rows = client.get("/api/screener?limit=3000").json()["rows"]
    flagged = {r["exl"] for r in rows if r.get("dex")}
    present = {r["exl"] for r in rows} & dex_labels
    assert flagged >= present, \
        f"биржи {sorted(present)} в выборке, но dex=true только у {sorted(flagged)}"


def test_configured_exchanges_all_reachable_in_meta(client):
    """Каждая биржа из конфига видна в meta — иначе её нельзя выбрать в фильтре."""
    from app.config import SETTINGS
    m = client.get("/api/meta").json()
    missing = [e.label for e in SETTINGS.exchanges if e.label not in m["exchange_info"]]
    assert not missing, f"нет в meta: {missing}"


# --------------------------------------------------------------------------
def test_alert_crud(client):
    r = client.post("/api/alerts", json={"field": "chg", "op": "gt", "value": 3.0,
                                         "exchange": "Bybit", "mode": "crossing"})
    assert r.status_code == 200, r.text
    rid = r.json()["rule"]["id"]

    lst = client.get("/api/alerts").json()
    assert any(x["id"] == rid for x in lst["rules"])

    p = client.patch(f"/api/alerts/{rid}", json={"enabled": False, "value": 7.5})
    assert p.json()["rule"]["enabled"] is False
    assert p.json()["rule"]["value"] == 7.5

    assert client.delete(f"/api/alerts/{rid}").json() == {"ok": True}
    assert client.delete(f"/api/alerts/{rid}").status_code == 404


def test_alert_rejects_bad_field(client):
    r = client.post("/api/alerts", json={"field": "banana", "op": "gt", "value": 1})
    assert r.status_code == 400
    assert "error" in r.json()


def test_alert_rejects_bad_json(client):
    r = client.post("/api/alerts", content=b"not json",
                    headers={"Content-Type": "application/json"})
    assert r.status_code == 400


def test_alert_fires_and_lands_in_events(client):
    """Полный цикл: правило → оценка снимка → событие в /api/alerts."""
    import asyncio
    from app.alerts import get_engine
    from app.api import build_rows

    r = client.post("/api/alerts", json={"field": "vol", "op": "gt", "value": 1.0,
                                         "mode": "while", "cooldown": 0,
                                         "notify_telegram": False})
    rid = r.json()["rule"]["id"]
    try:
        eng = get_engine()
        fired = asyncio.run(eng.evaluate(build_rows()))
        assert any(e.rule_id == rid for e in fired)
        assert any(e["rule_id"] == rid for e in client.get("/api/alerts").json()["events"])
    finally:
        client.delete(f"/api/alerts/{rid}")


# --------------------------------------------------------------------------
def test_csv_export(client):
    r = client.get("/api/screener.csv?limit=5")
    assert r.status_code == 200
    assert "text/csv" in r.headers["content-type"]
    lines = r.text.strip().splitlines()
    assert len(lines) == 6                        # заголовок + 5 строк
    assert lines[0].startswith("k,ex,exl")
    assert len(lines[1].split(",")) == len(lines[0].split(","))


def test_index_and_static(client):
    assert client.get("/").status_code == 200
    assert "CRYPTO SCREENER" in client.get("/").text
    assert client.get("/static/app.js").status_code == 200
    assert client.get("/static/style.css").status_code == 200


def test_websocket_streams_rows(client):
    import json
    with client.websocket_connect("/ws") as ws:
        hello = json.loads(ws.receive_text())
        assert hello["type"] == "hello" and hello["mode"] == "replay"

        ws.send_text(json.dumps({"type": "filters",
                                 "query": {"sort": "vol", "limit": "10"}}))
        msg = None
        for _ in range(5):
            m = json.loads(ws.receive_text())
            if m["type"] == "rows":
                msg = m
                break
        assert msg, "не дождались пакета rows"
        assert len(msg["rows"]) <= 10
        assert msg["overview"]["symbols"] > 0

        ws.send_text(json.dumps({"type": "ping"}))
        for _ in range(10):
            m = json.loads(ws.receive_text())
            if m.get("type") == "pong":
                break
        else:
            pytest.fail("нет ответа на ping")


# --------------------------------------------------------------------------
# Кэш select(): одинаковые запросы клиентов не пересчитывают выборку
# --------------------------------------------------------------------------
def test_select_cache_shared_by_identical_queries(client):
    from app import api as A
    A._SELECT_CACHE.clear()
    hits0 = A._PERF["select_cache_hits"]
    calls0 = A._PERF["select_calls"]

    q = {"sort": "vol", "desc": "1", "limit": "25"}
    rows1, meta1 = A.select(dict(q))
    rows2, meta2 = A.select(dict(q))
    assert A._PERF["select_calls"] == calls0 + 1, \
        "повторный идентичный запрос обязан браться из кэша"
    assert A._PERF["select_cache_hits"] == hits0 + 1
    assert rows1 is rows2 and meta1 == meta2

    # другой limit — другая запись кэша
    A.select({**q, "limit": "26"})
    assert A._PERF["select_calls"] == calls0 + 2

    # restrict (фокус-режим) входит в отпечаток
    keys = {r["k"] for r in rows1[:3]}
    rows_f, meta_f = A.select(dict(q), restrict=keys)
    assert A._PERF["select_calls"] == calls0 + 3
    assert meta_f["focus"] is True and all(r["k"] in keys for r in rows_f)
    rows_f2, _ = A.select(dict(q), restrict=set(keys))
    assert rows_f2 is rows_f, "тот же restrict — тот же кэш"

    # протухание: очищенный кэш пересчитывает
    A._SELECT_CACHE.clear()
    A.select(dict(q))
    assert A._PERF["select_calls"] == calls0 + 4
    A._SELECT_CACHE.clear()
