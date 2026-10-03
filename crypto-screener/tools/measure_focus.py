"""
Живой замер фокус-режима на реальных биржах.

Смысл: проверить не «код запускается», а что подписок действительно стало
меньше, свечи для сетки приезжают быстрее, и отбор по волатильности работает
на боевых данных (NATR берётся из реальных 1m-свечей биржи).
"""
import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.request

BASE = os.environ.get("BASE", "http://127.0.0.1:8097")
PID = int(os.environ["SRVPID"])


def get(path, timeout=40):
    with urllib.request.urlopen(BASE + path, timeout=timeout) as r:
        return json.loads(r.read().decode())


def post(path, body, timeout=60):
    req = urllib.request.Request(BASE + path, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def delete(path, timeout=30):
    req = urllib.request.Request(BASE + path, method="DELETE")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def rss_mb():
    with open(f"/proc/{PID}/status", encoding="utf-8") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024.0
    return -1.0


def grid_time(n=9, tf="5m", ex="", mt="swap", tries=3):
    ts = []
    ok = 0
    cells = 0
    for _ in range(tries):
        t0 = time.time()
        d = get(f"/api/grid?ex={ex}&mt={mt}&tf={tf}&n={n}&limit=200")
        ts.append(time.time() - t0)
        ok += sum(1 for c in d["cells"] if c.get("candles_ok"))
        cells = len(d["cells"])
    return statistics.median(ts), cells, ok


def subs():
    ov = get("/api/overview")
    out = {}
    for name, stt in (ov.get("status") or {}).items():
        out[name] = {k: stt.get(k) for k in ("state", "hot", "books", "trades", "symbols")}
    return ov.get("symbols"), out


def wait_ready(max_wait=180):
    t0 = time.time()
    while time.time() - t0 < max_wait:
        try:
            n, st = subs()
            online = [k for k, v in st.items() if v["state"] in ("online", "paused")]
            if n and n > 200 and online:
                return n, st
        except Exception:
            pass
        time.sleep(3)
    raise SystemExit("биржи не поднялись за отведённое время")


print("=" * 78)
print("ЖИВОЙ ЗАМЕР: полный профиль против фокус-режима")
print("=" * 78)
n, st = wait_ready()
print(f"\nподнялось: {n} инструментов, биржи: {json.dumps(st, ensure_ascii=False)}")
print(f"RSS после старта: {rss_mb():.0f} МБ")

print("\n--- 1. ПОЛНЫЙ РЕЖИМ (все биржи стримят топ по объёму) ---")
time.sleep(25)
full_n, full_st = subs()
t_full, cells_full, ok_full = grid_time(ex="Binance")
hot_full = sum((v["hot"] or 0) for v in full_st.values())
books_full = sum((v["books"] or 0) for v in full_st.values())
trades_full = sum((v["trades"] or 0) for v in full_st.values())
print(f"инструментов в хранилище : {full_n}")
print(f"горячих подписок (hot)   : {hot_full}")
print(f"стаканов / лент сделок   : {books_full} / {trades_full}")
print(f"/api/grid 9 ячеек        : {t_full:.2f} c, свечей пришло в {ok_full}/{cells_full} ячеек")
print(f"RSS                      : {rss_mb():.0f} МБ")

print("\n--- 2. ФОКУС: Binance / фьючерсы / top-50, перепроверка 15 c ---")
d = post("/api/focus", {"ex": "Binance", "mt": "swap", "limit": 50, "interval": 15,
                        "sort": "vol", "pause_others": True})
print(f"отбор: {d['count']} монет за {d['last_ms']} мс, вселенная {d['stats']['universe']}, "
      f"пул {d['stats']['pool']}")
print("первые 10:", ", ".join(s.split("/")[0] for s in d["symbols"][:10]))
print("ждём 25 c (ротация подписок + перепроверка отбора)...")
time.sleep(25)
foc_n, foc_st = subs()
t_focus, cells_f, ok_f = grid_time(ex="Binance")
hot_f = sum((v["hot"] or 0) for k, v in foc_st.items() if k != "focus")
books_f = sum((v["books"] or 0) for k, v in foc_st.items() if k != "focus")
trades_f = sum((v["books"] or 0) * 0 + (v["trades"] or 0) for k, v in foc_st.items() if k != "focus")
print(f"инструментов в хранилище : {foc_n} (таблица скринера не сжалась: "
      f"{len(get('/api/screener?ex=Binance&mt=swap&limit=5000')['rows'])} строк Binance/swap)")
print(f"горячих подписок (hot)   : {hot_f}   (было {hot_full})")
print(f"стаканов / лент сделок   : {books_f} / {trades_f}   (было {books_full} / {trades_full})")
print(f"статусы бирж             : " + ", ".join(
    f"{k}={v['state']}(hot {v['hot']})" for k, v in foc_st.items() if k != "focus"))
print(f"/api/grid 9 ячеек        : {t_focus:.2f} c, свечей пришло в {ok_f}/{cells_f} ячеек")
print(f"RSS                      : {rss_mb():.0f} МБ")
print(f"rotations={d['stats']['rotations']} -> "
      f"{get('/api/focus')['stats']['rotations']} (перепроверки идут)")

print("\n--- 3. ФОКУС ПО ВОЛАТИЛЬНОСТИ (NATR >= 0.5%, сортировка по NATR) ---")
t_post = time.time()
d = post("/api/focus", {"ex": "Binance", "mt": "swap", "limit": 20, "interval": 15,
                        "sort": "natr", "query": {"natr_min": 0.5, "vol_min": 1000000}})
print(f"отбор: {d['count']} монет за {d['last_ms']} мс, пул {d['stats']['pool']}, "
      f"подкачек свечей {d['stats']['candle_fetches']}")
print(f"POST /api/focus занял {time.time() - t_post:.2f} c, enriching={d.get('enriching')} "
      f"pending={d.get('pending')} note={d.get('note')!r}")
time.sleep(20)
d2 = get("/api/focus")
print(f"через 20 c: count={d2['count']} enriching={d2['enriching']} pending={d2['pending']} "
      f"last_ms={d2['last_ms']} rotations={d2['stats']['rotations']} "
      f"candle_fetches={d2['stats']['candle_fetches']} note={d2['note']!r}")
d = d2
print("топ по NATR:", ", ".join(f"{s.split('/')[0]}" for s in d["symbols"][:10]))
if d["note"]:
    print("примечание:", d["note"])
rows = {r["k"]: r for r in get("/api/screener?ex=Binance&mt=swap&limit=5000")["rows"]}
natrs = [rows[k]["natr"] for k in d["keys"] if k in rows and rows[k].get("natr")]
print(f"NATR отобранных: min={min(natrs):.3f} max={max(natrs):.3f} (порог 0.5)"
      if natrs else "NATR нет в строках")
assert not natrs or min(natrs) >= 0.5, "фильтр по волатильности не применён!"

print("\n--- 4. ВЫКЛЮЧЕНИЕ ФОКУСА ---")
delete("/api/focus")
for wait_s in (10, 20, 40, 60):
    time.sleep(wait_s if wait_s == 10 else 20)
    off_n, off_st = subs()
    print(f"  +{wait_s} c: " + ", ".join(
        f"{k}={v['state']}(hot {v['hot']})" for k, v in off_st.items()))
    if all(v["state"] == "online" for k, v in off_st.items()):
        break
hot_off = sum((v["hot"] or 0) for v in off_st.values())
print(f"горячих подписок (hot)   : {hot_off} (восстановилось с {hot_f})")
print("статусы бирж             : " + ", ".join(
    f"{k}={v['state']}(hot {v['hot']})" for k, v in off_st.items()))
print(f"RSS                      : {rss_mb():.0f} МБ")

print("\n" + "=" * 78)
print(f"ИТОГ: подписок {hot_full} -> {hot_f} (фокус) -> {hot_off} (выключен); "
      f"стаканов {books_full} -> {books_f};")
print(f"      /api/grid 9 ячеек: {t_full:.2f} c -> {t_focus:.2f} c "
      f"({(1 - t_focus / t_full) * 100 if t_full else 0:.0f}% быстрее); "
      f"свечи в {ok_f}/{cells_f} ячейках")
print(f"      RSS: {rss_mb():.0f} МБ")
print("=" * 78)
