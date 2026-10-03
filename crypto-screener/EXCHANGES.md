# Биржи: что проверено и как подключено

Все данные ниже получены **живыми запросами** к API бирж (2026-10-01), а не из
документации. Там, где ccxt и нативный API расходятся, указан фактический ответ.

## Сводка

| Биржа | ccxt id | Рынок | Инструментов | `fetch_tickers` | WS | Свечи | Funding | OI |
|---|---|---|---|---|---|---|---|---|
| Binance Spot | `binance` | spot | 4670 | 3723 | ✅ | ✅ | — | — |
| Binance Futures | `binanceusdm` | swap | 778 | 788 за 0.2 c | ✅ | ✅ | bulk `premiumIndex` | точечно |
| Bybit | `bybit` | swap | 873 | 896 за 2.5 c | ✅ | ✅ | bulk v5 tickers | bulk v5 tickers |
| OKX | `okx` | swap | 495 | 1148 за 1.9 c | ✅ | ✅ | bulk `fetchFundingRates` | bulk `open-interest` |
| **MEXC** | `mexc` | swap | 1172 | 1215 за 5.9 c | ✅ (нужен protobuf) | ✅ | bulk `/contract/ticker` | bulk `/contract/ticker` |
| **Gate.io** | `gate` | swap | 1025 | 1024 за 0.2 c | ✅ (limit ≤100) | ✅ | bulk `fetchFundingRates` | bulk `/contracts` |
| **Aster** (DEX) | `aster` | swap | 572 | 609 за 0.4 c | ✅ | ✅ | bulk `premiumIndex` | точечно (нативный) |
| **Hyperliquid** (DEX) | `hyperliquid` | swap | 330 | 834 за 6.8 c | ✅ | ✅ | bulk `fetchFundingRates` | точечно через ccxt |

Сверка цены BTC по всем биржам в один момент: **86 372 – 86 423** (спред 0.06%) —
данные консистентны, ни одна биржа не отдаёт устаревшую цену.

### Как DEX помечены в интерфейсе

Флаг `dex=True` в `ExchangeConfig` пробрасывается в `SymbolState.dex`, далее в
каждую строку выдачи (`to_row()`) и в `/api/meta → exchange_info`. В UI:

| Где | Как выглядит |
|---|---|
| Чипсы фильтра бирж | `Aster ◆`, `Hyperliquid ◆` |
| Колонка «Биржа» в таблице | бейдж `DEX` после названия |
| Плитка карты рынка | `◆` в углу + `[DEX]` в тултипе |
| Карточка инструмента | `Aster · DEX · swap · BTC/USDT:USDT` |
| Форма алерта | `Aster (DEX)` в выпадающем списке |

Список бирж нигде не захардкожен во фронтенде — он целиком приходит из
`/api/meta`, поэтому добавление биржи в конфиг сразу отражается в интерфейсе.

## Не подключено и почему

| Биржа | Причина | Что делает код |
|---|---|---|
| **dYdX** (`dydx`) | `NotSupported: dydx fetchTicker() is not supported yet` — без тикера скринер не получит ни цену, ни объём | объявлен в `EXTRA_EXCHANGES` с `enabled=False` |
| **Paradex** (`paradex`) | `load_markets()` возвращает 0 рынков, WS отвечает `400 Invalid response status` | так же объявлен с `enabled=False` |
| **Spot-DEX** (Uniswap, PancakeSwap, Raydium, Jupiter) | в ccxt 4.5.85 их нет вовсе: `ccxt.exchanges` не содержит ни одного такого id | не объявлены |

Обе disabled-биржи оставлены в конфиге с комментарием — включить их можно флагом,
но работать они не будут до соответствующего фикса в ccxt.

## Особенности, которые пришлось учесть

### 1. MEXC требует protobuf

Часть WS-сообщений MEXC приходит в Protocol Buffers. Без пакета ccxt бросает:
```
NotSupported: mexc requires protobuf to decode messages, install it with `pip install "protobuf==5.29.5"`
```
`protobuf==5.29.5` добавлен в `requirements.txt`.

### 2. MEXC отдаёт уровни стакана тремя полями

```
bid[0] = [86608.0, 450512, 1.0]      # [цена, объём, число заявок]
```
У всех остальных бирж — двумя. Жёсткая распаковка `for p, a in levels` падала с
`ValueError`, из-за чего стаканов MEXC не было вовсе (350 ошибок в логе за минуту).
Уровни нормализуются один раз в `SymbolState._norm_levels()`.

### 3. Валидная глубина стакана отличается у каждой биржи

Полный замер — каждое значение проверялось живым `watch_order_book`:

| Биржа | 50 | 100 | 200 | 500 | Что ставим |
|---|---|---|---|---|---|
| binanceusdm | ok | ok | **BadRequest −4021** | ok | 100 |
| binance (spot) | ok | ok | ok | ok | 100 |
| bybit | ok | **BadRequest** | ok | **BadRequest** | 200 |
| okx | **AuthError** | ok(400) | ok(400) | ok(400) | 200 |
| mexc | ok(1501) | ok | ok | ok | 200 |
| gate (**swap**) | ok | ok | **BadRequest** | **BadRequest** | **100** |
| gate (**spot**) | ok | ok | ok | ok | 200 |
| aster | ok(20) | ok(20) | ok(20) | ok(20) | 200 |
| hyperliquid | ok(20) | ok(20) | ok(20) | ok(20) | 200 |
| bitget | **BadRequest** | ok | ok | ok | 200 |
| kucoinfutures | ok | ok | **ExchangeError** | **ExchangeError** | 100 |
| bingx | ok(100) | ok(100) | ok(100) | ok(100) | 200 |
| kraken | **NotSupported** | ok | **NotSupported** | ok | 100 |
| htx | **ExchangeError** | **ExchangeError** | **ExchangeError** | **ExchangeError** | `books=0` |

Три вывода, которые не угадываются из документации:

1. Ограничения **разные для спота и свопа одной биржи**: Gate futures принимает
   только ≤100 (канал `futures.order_book_update`), а Gate spot — любую глубину.
   Поэтому `VALID_BOOK_LIMITS` хранится по паре `(id, market)`, а не по `id`.
2. MEXC, Aster и Hyperliquid **игнорируют** `limit`: MEXC всегда присылает
   ~1500 уровней, Aster и Hyperliquid — 20.
3. У htx `watch_order_book` не работает ни на какой глубине — ему выставлен
   `books=0`, иначе поток падал бы вечно.

Невалидное значение не выглядит как ошибка: биржа отвечает `BadRequest` на
**все** подписки, а снаружи это читается как «на бирже просто нет плотностей».
Именно так Gate молча потерял весь стакан (см. `BUGFIXES.md`, п.23).

Тесты `test_exchange_config_flags` и
`test_valid_book_limits_cover_all_configured_exchanges` сверяют каждое
configured-значение с этой таблицей и не дают добавить биржу без замера.

### 4. Единицы объёма в OHLCV различаются даже внутри одной биржи

Это самый коварный момент: неправильные единицы не ломают график, они просто
показывают объём, отличающийся в разы. Замеры (сырой объём 1m-свечи против
`baseVolume/1440` из тикера):

| Биржа | Рынок | `contractSize` | ratio | Вывод |
|---|---|---|---|---|
| Gate | spot | 1 | 2.22 | базовая монета |
| Gate | **swap** | 0.0001 | **17 407** | **контракты** |
| MEXC | spot | 1 | 1.92 | базовая монета |
| MEXC | **swap** | 0.0001 | 1.36 | контракты (тикер MEXC тоже в контрактах, поэтому ratio ≈ 1) |
| OKX | swap | 0.01 | — | базовая монета: ccxt берёт индекс 6 = `base volume` |
| Binance / Bybit / Aster / Hyperliquid | swap | 1 | — | базовая монета |

Для MEXC swap ratio ≈ 1 обманчив: ccxt кладёт в `baseVolume` то же значение, что
и в OHLCV, то есть сравнивать не с чем. Независимая сверка:
```
volume24 · contractSize · price = 387 657 815 · 0.0001 · 86 570 = $3.356B
amount24 (эталон биржи)                                        = $3.275B   ✓ ±2.5%
```
Значит `volume24` в контрактах, и OHLCV — тоже.

Отсюда флаг `ohlcv_vol_in_contracts` **на пару (биржа, рынок)**, а не на биржу:
у Gate и MEXC своп требует пересчёта, а спот — нет.

Для OKX флаг обязан быть выключен: ccxt в `parse_ohlcv` берёт
`volumeIndex = 5 if spot else 6`, где 6 — это уже `base volume`. Домножение на
`contractSize=0.01` занижало объём в 100 раз ($75k/мин вместо ~$7M).

Итоговая нормализация в `normalize_candle_volume()`:
```
base = volume · contractSize   (только если объём в контрактах)
usd  = base            для инверсных контрактов
usd  = base · close    для линейных и спота
```

### 5. Коллизия идентификаторов спот/своп

Один и тот же биржевой `id` бывает у двух рынков:

| Биржа | Коллизий id | Пример |
|---|---|---|
| Gate | **526** | `OGN_USDT` → `OGN/USDT` и `OGN/USDT:USDT` |
| Bybit | **292** | `BTCUSDT` → `BTC/USDT` и `BTC/USDT:USDT` |
| Aster | 40 | `USD1USDT` → `USD1/USDT` и `USD1/USDT:USDT` |
| Binance, OKX, MEXC, Hyperliquid | 0 | — |

При наивном поиске «первого подходящего символа в `markets`» OI и funding
записывались в **спотовый** инструмент вместо фьючерсного: у Gate BTC OI был `$0`,
хотя bulk-эндпоинт честно вернул данные. Исправлено в `_to_ccxt_symbol()` —
все ветки поиска фильтруются по `cfg.market`, а `_build_id_map()` пишет рынки
чужого типа первыми, чтобы свой тип их перезаписал.

Результат: Gate BTC OI `$2 386 441 369` — совпадает с независимым расчётом
`position_size · quanto_multiplier · mark_price = $2.379B`.

### 6. Gate: какой эндпоинт брать для OI

`/futures/usdt/contract_stats` **не bulk** — без `contract=` отвечает
`400 MISSING_REQUIRED_PARAM`. Зато `/futures/usdt/contracts` отдаёт все 1024
контракта одним запросом с полем `position_size`.

Расхождение между источниками ровно двукратное:

| Контракт | `position_size · mult · mark` | `contract_stats.open_interest_usd` |
|---|---|---|
| BTC_USDT | $2 379 137 289 | $4 756 345 776 |
| ETH_USDT | $1 105 687 923 | $2 228 940 138 |

`contract_stats` считает обе стороны позиции. Берём `position_size` — это
стандартная конвенция OI, как у Binance/Bybit/OKX, иначе Gate выглядел бы
вдвое крупнее остальных.

### 7. У Aster Binance-совместимый API

```
https://fapi.asterdex.com/fapi/v1/premiumIndex  → 767 контрактов, lastFundingRate
https://fapi.asterdex.com/fapi/v1/openInterest?symbol=BTCUSDT → {"openInterest":"5580.531"}
```
Поэтому funding берётся тем же bulk-путём, что и у Binance, а OI — точечно.
ccxt `fetchOpenInterest` для Aster не реализовал (`has.fetchOpenInterest = False`),
показать `$481 638 898` = 5580.5 BTC · $86 570 удалось только нативным запросом.

### 8. MEXC: один вызов закрывает и funding, и OI

`/api/v1/contract/ticker` **без** параметра `symbol` возвращает список всех
контрактов с полями `holdVol` (OI), `fundingRate`, `amount24`. ccxt для MEXC не
реализовал ни `fetchFundingRates`, ни `fetchOpenInterest`, так что это единственный
bulk-путь. Результат: MEXC BTC OI `$4 997 257 109`, funding заполнен на 100%.

### 9. Скорость `fetch_tickers`

MEXC (5.9 c) и Hyperliquid (6.8 c) заметно медленнее остальных. При общем
`TICKER_REFRESH=20` их цикл опроса почти не оставляет запаса, поэтому у них
персональный `ticker_refresh=30`. Тест `test_per_exchange_ticker_refresh_defaults_to_global`
следит, чтобы значение не сбросилось.

### 10. Отказ биржи по отдельному символу

Gate стабильно отвечает `BadRequest` на подписку стакана по некоторым символам
(например `龙虾/USDT:USDT`, `SOXS/USDT:USDT`). Без защиты поток перезапускался
каждые 2 секунды и давал сотни ошибок в минуту. Добавлен автоматический
выключатель: после `MAX_STREAM_FAILS = 5` подряд идущих **не-сетевых** отказов
поток гасится, символ попадает в `_book_banned` и больше не перезапускается
ротацией. Сетевые ошибки не считаются — они транзиентные.

Замер: ошибок за 2.5 минуты работы **649 → 101**, остановлено 20 безнадёжных потоков.

### 11. Батч-подписки (`watch_*_for_symbols`) поддерживаются не универсально

Замер `has`-флагов ccxt (4.5.52 и 4.5.85 — идентичны) + живой зонд
`tools/probe_batch_ws.py`:

| Биржа | стаканы батчем | сделки батчем | тикеры батчем | batч-отписка |
|---|---|---|---|---|
| binance / binanceusdm | ✓ | ✓ | ✓ | ✓ |
| bybit (spot+swap) | ✓ | ✓ | ✓ | ✓ |
| okx (spot+swap) | ✓ | ✓ | ✓ | ✓ |
| gate (spot+swap) | — | ✓ | ✓ | сделки ✓, тикеры — |
| mexc (spot+swap) | — | — | ✓ | — |
| aster | ✓ | ✓ | ✓ | книги: TypeError в ccxt (ловим), остальное ✓ |
| hyperliquid | — | — | ✓ | — |

Особенности, зашитые в коллектор:

* **Bybit SPOT принимает ≤10 топиков на subscribe-запрос** — сервер отвечает
  «args size >10» (linear терпит и 20, но недокументированно). В конфиге
  bybit `batch_chunk=10`; на любую «размерную» ошибку коллектор уменьшает
  чанк вдвое автоматически.
* **У Binance WS-URL батча строится из списка символов** (`/public/ws/<0..49>`,
  лимит 200 подписок на индекс): список изменился → новое соединение. Отписка
  — всем старым списком (`batch_full_unwatch=True`), и коллектор возвращает
  внутренние счётчики ccxt (`numSubscriptionsByStream`), иначе после ~40
  пересборок — BadRequest «reached the limit of subscriptions by stream».
* **OKX: `limit=50` у батч-стаканов требует VIP4** (канал `books50-l2-tbt` →
  AuthenticationError). Наши 200 мапятся в публичный канал `books` — как и у
  поточечного `watch_order_book`.
* Где батчей нет или они стабильно отказывают, коллектор сам падает в
  поточечный режим с баном отдельных символов (п.10).

## Как добавить биржу

1. Проверить, что она есть в `ccxt.exchanges` и у `ccxt.pro`:
   ```python
   import ccxt, ccxt.pro as p
   print("binance" in ccxt.exchanges, hasattr(p, "binance"))
   ```
2. Прогнать зонд: `load_markets()`, `fetch_tickers()`, `fetch_ticker()`,
   `watch_ticker()`, `fetch_ohlcv()`, `watch_order_book()` — и записать
   фактические значения в таблицу выше.
3. Добавить `ExchangeConfig(...)` в `app/config.py`. Обязательные поля:
   - `book_limit` — только из списка валидных значений биржи;
   - `ohlcv_vol_in_contracts` — по замеру, отдельно для spot и swap;
   - `ticker_refresh` — если `fetch_tickers` дольше ~3 c;
   - `dex=True` — для децентрализованных бирж (метка в UI).
4. Если есть bulk-эндпоинт OI/funding — добавить ветку в `_oi_bulk()`
   или URL в `PREMIUM_INDEX_URLS` / `NATIVE_OI_URL`.
   Иначе OI подхватится автоматически через ccxt `fetch_open_interest`.
5. Прогнать `pytest tests/ -q` — `test_exchange_config_flags` проверит валидность
   `book_limit` и согласованность флагов объёма.


---

## Тайминги REST-вызовов (замер 2026-10-02, 12 параллельных запросов)

Нужны для фокус-режима: «дешёвый» уровень отбора опирается на один
`fetch_tickers()` на всю биржу, «точный» — на `fetch_ohlcv()` по каждому
кандидату. Именно второй вызов определяет, сколько секунд длится пересчёт
отбора по волатильности.

| Биржа | `load_markets` | `fetch_tickers(None)` (вся биржа) | `fetch_ohlcv(1m,400)` медиана | 12 свечей параллельно | USDT-инструментов |
|---|---|---|---|---|---|
| binanceusdm | 0.23 c | **0.21 c** (788) | 0.70 c | 1.23 c | 871 |
| bybit | 1.26 c | **0.16 c** (897) | 0.67 c | 1.22 c | 789 |
| okx | 0.95 c | **0.18 c** (500) | 0.52 c | 0.82 c | 485 |
| gate | 9.24 c | **0.31 c** (1025) | 0.40 c | 0.68 c | 1025 |
| mexc | 2.16 c | **0.46 c** (1216) | **5.78 c** | 6.24 c | 1099 |

Выводы, которые легли в код:

1. `fetch_tickers(None)` стоит 0.16–0.46 c на ЛЮБОЙ бирже, даже на MEXC с 1216
   инструментами. Поэтому «дешёвый» уровень отбора (пул кандидатов по объёму)
   бесплатен: данные уже лежат в STORE после тикер-цикла коллектора, отдельных
   запросов фокус-режим не делает.
2. `fetch_ohlcv` у MEXC в **8 раз медленнее**, чем у Binance/Bybit (5.78 c против
   0.7 c). Один батч из 60 параллельных запросов к MEXC — это ~6 c, то есть
   интерактивный бюджет (5 c) на MEXC почти всегда исчерпывается одним батчем,
   и отбор по NATR добирается фоновыми проходами. Это ожидаемое поведение, UI
   показывает «у N монет свечи ещё не загружены — отбор уточнится».
3. `load_markets` у Gate — 9.24 c (в прошлых замерах доходило до 18.9 c), что
   меньше дефолтного таймаута ccxt (10 c) лишь ненамного: подтверждает выбор
   `REQUEST_TIMEOUT=20000` и ретраев.

Замер воспроизводится: 12 параллельных `fetch_ohlcv` на первых 12 USDT-парах
биржи, `enableRateLimit=True`, таймаут 25 c.
