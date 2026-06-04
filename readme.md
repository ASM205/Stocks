
📎 [Setup Guide](setup.md)

# Sector Intelligence Pipeline — Bronze Layer

**Platform:** Microsoft Fabric
**Source:** Polygon.io REST API (Free Tier, 5 calls/min)
**Table:** `Avi.dbo.bronze_bars_v2`
**Tickers:** 29 across 6 GICS sub-industries
**Granularities:** 1min (2d), 5min (30d), daily (180d)

---

## 1. Overview

The Bronze Layer is the raw ingestion tier of a medallion architecture. It stores data exactly as received from Polygon with minimal transformation — only type casting is applied. No indicators, business logic, or derived metrics are computed here.

**Sectors covered:**

- Health Care → Pharmaceuticals, Biotechnology, Health Care Equipment
- Industrials → Air Freight & Logistics, Marine Ports & Services, Airport Services

---

## 2. Watchlist

| Sub-Industry | Count | Tickers |
|---|---|---|
| Pharmaceuticals | 5 | LLY, JNJ, ABBV, MRK, PFE |
| Biotechnology | 5 | AMGN, VRTX, REGN, GILD, MRNA |
| Health Care Equipment | 5 | ISRG, ABT, SYK, BSX, MDT |
| Air Freight & Logistics | 5 | UPS, FDX, EXPD, CHRW, GXO |
| Marine Ports & Services | 5 | ZIM, MATX, GSL, SFL, ESEA |
| Airport Services | 4 | GATX, AAWW, SKYW, FLGT |
| **Total** | **29** | |

Tickers selected by market cap (top 5 per sub-industry). Each row in the Bronze table carries `sector` and `sub_industry` columns, so every record is self-describing without needing a join back to the watchlist.

**Design note:** The watchlist is currently a hardcoded Python dict. Recommended upgrade: migrate to `Avi.dbo.ref_watchlist` Delta table so tickers can be added/removed without touching pipeline code. Or USe OpenFIGI Api

---

## 3. Data Ingestion

**Endpoint:**
```
GET https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{mult}/{timespan}/{from}/{to}
```
Parameters: `adjusted=true`, `sort=asc`, `limit=50000`. Pagination follows `next_url` cursor automatically.

**Granularities:**

| Granularity | Lookback | Use Case |
|---|---|---|
| 1min | 2 days | Intraday momentum |
| 5min | 30 days | Session patterns |
| daily | 180 days | Macro trend, sector correlation |

**Rate limit strategy:** Free tier allows 5 calls/min. Pipeline uses batches of 4 tickers with a 62s sleep between batches. On HTTP 429, exponential back-off of 60s/120s/180s with up to 3 retries.

| Metric | Full Backfill | Incremental |
|---|---|---|
| API calls | 87 (29 × 3) | 58 (29 × 2) |
| Est. runtime | ~25 mins | ~15 mins |

---

## 4. Bronze Table Schema — `Avi.dbo.bronze_bars_v2`

| Column | Type | Source | Description |
|---|---|---|---|
| `ticker` | STRING | WATCHLIST | Ticker symbol |
| `sector` | STRING | WATCHLIST | GICS sector |
| `sub_industry` | STRING | WATCHLIST | GICS sub-industry |
| `granularity` | STRING | Pipeline | 1min / 5min / daily |
| `t` | BIGINT | Polygon | Bar open timestamp (Unix ms, UTC) |
| `o` | DOUBLE | Polygon | Open price |
| `h` | DOUBLE | Polygon | High price |
| `l` | DOUBLE | Polygon | Low price |
| `c` | DOUBLE | Polygon | Close price |
| `v` | DOUBLE | Polygon | Volume |
| `vw` | DOUBLE | Polygon | VWAP |
| `n` | BIGINT | Polygon | Number of transactions |

**Deduplication:** Left anti-join on `(ticker, granularity, t)` before every write — pipeline is fully idempotent.

**Type safety:** All API values are first loaded as strings, then cast to numeric types inside Spark to avoid inference errors on nulls.

---

## 5. Pipeline Modes

| Flag | Default | Behaviour |
|---|---|---|
| `INCREMENTAL` | `False` | `False` = full backfill all 3 granularities. `True` = 1min + 5min only. |
| `REFRESH_DAILY` | `False` | When `INCREMENTAL=True`, also re-fetch daily bars. Set once/day after market close. |
| `SECTOR_FILTER` | `None` | `None` = all tickers. `'Health Care'` or `'Industrials'` to run one sector at a time. |

**Recommended schedule:**

- First run: `INCREMENTAL=False` — full backfill
- Daily after close (~16:30 ET): `INCREMENTAL=True, REFRESH_DAILY=True`
- Intraday refresh: `INCREMENTAL=True, REFRESH_DAILY=False`

**Design note:**  Currently requires manual change of flag in the code. Find a way to automate if possible.

---

## 6. Function Reference

**`fetch_bars(ticker, multiplier, timespan, from_date, to_date)`**
Fetches all OHLCV bars for one ticker + granularity. Handles pagination and retries. Returns `list[dict]`, empty list on failure.

**`fetch_all(tickers, granularity)`**
Iterates all tickers for one granularity with batch rate limiting. Returns `dict[ticker → list[bars]]`.

**`write_bronze(raw, granularity)`**
Flattens raw dicts → STRING schema → cast to types → deduplicate → append to Delta. Enriches each row with `sector` and `sub_industry` from WATCHLIST.

---

## 7. Known Issues

**Zero-bar tickers:** Smaller caps (e.g. ESEA, FLGT) may return 0 bars on 1min/5min for low-volume days. Expected behaviour, not a pipeline error.

**No `tier` column:** The v2 schema removed `tier` (anchor/midtier/highbeta) from v1. Any query referencing `tier` against `bronze_bars_v2` will raise an `AnalysisException`.

**Free tier delay:** Polygon free tier 1min bars may be 15 minutes delayed during market hours.

---

## 8. Sanity Check

```python
# Row counts by sub-industry and granularity
df_bronze.groupBy("sub_industry", "granularity") \
    .agg(F.count("*").alias("rows"), F.countDistinct("ticker").alias("tickers")) \
    .orderBy("sub_industry", "granularity") \
    .show()

# Row counts by ticker (daily only)
df_bronze.filter(F.col("granularity") == "daily") \
    .groupBy("ticker", "sector", "sub_industry") \
    .agg(F.count("*").alias("days")) \
    .orderBy("sector", "sub_industry", "ticker") \
    .show(50)
```

Expected: ~122 daily bars per active ticker on a 180-day backfill.

---

## 9. Change Log

| Version | Date | Changes |
|---|---|---|
| v2.0 | Jun 2026 | Expanded to 29 tickers / 6 sub-industries. Batch rate limiting. Added `sector`, `sub_industry` columns. Removed `tier`. Renamed table to `bronze_bars_v2`. |
| v1.0 | Jun 2026 | Initial pipeline. 12 CNS tickers. Flat 12s delay. Table: `bronze_bars`. |

v1 table exists but code has been removed.

# Sector Intelligence Pipeline — Silver Layer Documentation

## Overview

The Silver Layer is the second stage of a three-tier data pipeline (Bronze → Silver → Gold). It reads raw stock price data from the Bronze table, cleans and enriches it, computes technical indicators, and writes three output tables split by time granularity.

**Source table:** `Avi.dbo.bronze_bars_v2`

**Output tables:**
| Table | Description |
|---|---|
| `Avi.dbo.silver_daily` | End-of-day price bars with daily calendar fields |
| `Avi.dbo.silver_5min` | 5-minute intraday bars with session labels |
| `Avi.dbo.silver_1min` | 1-minute intraday bars with session labels |

---

## Pipeline Architecture

The pipeline runs through 7 sequential steps, all orchestrated by `run_silver_pipeline()`.

```
Bronze Table
     │
     ├── filter(granularity == "daily")
     │        │
     │   base_transform()  →  enrich_daily()  →  add_indicators("daily")  →  silver_daily
     │
     ├── filter(granularity == "5min")
     │        │
     │   base_transform()  →  enrich_intraday()  →  add_indicators("5min")  →  silver_5min
     │
     └── filter(granularity == "1min")
              │
         base_transform()  →  enrich_intraday()  →  add_indicators("1min")  →  silver_1min
```

---

## Step-by-Step Breakdown

### Step 1 — Read Bronze

Reads the entire bronze Delta table into a Spark DataFrame. Prints row count and schema for verification.

```python
BRONZE_TABLE = "Avi.dbo.bronze_bars_v2"
bronze = spark.read.format("delta").table(BRONZE_TABLE)
```

---

### Step 2 — Base Transform (`base_transform`)

Applied to **all three granularities** before any splitting. Does three things:

**Rename columns** — The raw Polygon.io API returns single-character column names. These are renamed to human-readable equivalents:

| Raw | Renamed | Meaning |
|---|---|---|
| `t` | `timestamp_utc` | Unix millisecond timestamp |
| `o` | `open` | Opening price |
| `h` | `high` | Highest price in the bar |
| `l` | `low` | Lowest price in the bar |
| `c` | `close` | Closing price |
| `v` | `volume` | Number of shares traded |
| `vw` | `vwap` | Volume-weighted average price |
| `n` | `transaction_count` | Number of individual trades |

**Data quality filters** — Drops any rows that are clearly bad data:

- Any OHLCV field that is `null` or `<= 0`
- Any bar where `high < low` (physically impossible)

After these checks, the raw `timestamp_utc` and `granularity` columns are dropped as they are no longer needed.

---

### Step 3 — Time Enrichment

Two separate functions handle daily vs intraday bars because they need different time columns.

#### `enrich_daily` — for daily bars

Adds calendar-based columns useful for period-over-period analysis:

| Column | Example | Description |
|---|---|---|
| `date` | `2024-03-15` | Calendar date |
| `day_of_week` | `Friday` | Full weekday name |
| `week_of_year` | `11` | ISO week number |
| `month` | `3` | Month as integer |
| `quarter` | `Q1` | Quarter label |
| `year` | `2024` | Year as integer |
| `is_month_end` | `true/false` | Whether date is the last day of the month |
| `is_quarter_end` | `true/false` | Whether date is the last day of a quarter (Mar/Jun/Sep/Dec) |

#### `enrich_intraday` — for 1min and 5min bars

Adds time-of-day columns and converts timestamps to US Eastern Time (ET), since US stock markets operate on ET:

| Column | Example | Description |
|---|---|---|
| `timestamp_et` | `2024-03-15 09:30:00` | Timestamp in US Eastern Time |
| `date` | `2024-03-15` | Trading date (ET) |
| `hour` | `9` | Hour of day (ET) |
| `minute` | `30` | Minute of hour (ET) |
| `day_of_week` | `Friday` | Full weekday name |
| `session` | `regular` | Trading session label (see below) |

**Session labels** are assigned based on ET time:

| Session | Time Range (ET) | Description |
|---|---|---|
| `pre` | 04:00 – 09:29 | Pre-market trading |
| `regular` | 09:30 – 15:59 | Normal market hours |
| `after` | 16:00 – 19:59 | After-hours trading |
| `extended` | All other times | Outside standard extended hours |

---

### Step 4 — Technical Indicators (`add_indicators`)

All indicators are computed **per ticker** using Spark Window functions — meaning each stock's calculations are completely isolated from every other stock. The `granularity` parameter adjusts the lookback periods: shorter windows for intraday data (more bars per day) vs longer windows for daily data.

#### Daily Return
The percentage change in closing price from the previous bar:
```
daily_return = (close - previous_close) / previous_close
```

#### SMAs — Simple Moving Averages
Rolling average of the closing price. Granularity-aware periods:
- Daily: `sma_20`, `sma_50`
- Intraday: `sma_10`, `sma_20`

#### EMAs — Exponential Moving Averages (`ema_12`, `ema_26`)
Like SMA but more weight is given to recent prices. Used as building blocks for MACD.

> **Note:** True EMA requires recursive computation (each value depends on the previous EMA), which Spark cannot do natively in a distributed setting. The code uses a rolling average over a wide window as a close approximation — sufficient for cross-ticker trend comparison.

#### MACD
```
macd = ema_12 - ema_26
```
Positive MACD means short-term momentum is above long-term trend (bullish). Negative means momentum is fading.

#### RSI 14 — Relative Strength Index
A 0–100 momentum score calculated over the last 14 bars:
- Separates each bar's move into a gain or a loss
- Averages the gains and losses separately
- `rsi_14 = 100 - (100 / (1 + avg_gain / avg_loss))`
- Clamped to 100 when there are no losses

Typical interpretation: above 70 = potentially overbought; below 30 = potentially oversold.

#### Bollinger Bands (`bb_upper`, `bb_mid`, `bb_lower`)
A volatility envelope around a moving average. Granularity-aware periods:
- Daily: 20-period
- Intraday: 10-period

```
bb_mid   = rolling average of close
bb_upper = bb_mid + (2 × standard deviation)
bb_lower = bb_mid - (2 × standard deviation)
```

The internal `bb_std` column is computed and immediately dropped to keep the schema clean.

#### Rolling Volatility
Standard deviation of `daily_return` over the last 20 bars (10 for intraday). Measures how erratically a stock's price has been moving recently.

#### Price Indexed
Normalises every stock's price to 100 at its first available bar:
```
price_indexed = (close / first_close) × 100
```
This makes cross-ticker performance comparison possible regardless of absolute price levels (e.g. a ₹5,000 stock and a ₹50 stock can be compared on the same chart).

---

### Step 5 — Column Order

Two constant lists define the final column order for each output table, enforcing a consistent schema:

**`DAILY_COLS`** — Identity → Time → OHLCV → Indicators

**`INTRADAY_COLS`** — Identity → Time (includes `timestamp_et`, `hour`, `minute`, `session`) → OHLCV → Indicators

---

### Step 6 — Write Silver (`write_silver`)

Each table is written as a Delta table using **full overwrite** mode:

```python
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
```

Silver is always fully recomputed from Bronze — there is no incremental/merge logic. After writing, the row count is printed for verification.

---

## Sanity Checks

After the pipeline completes, two validation queries run automatically:

**Coverage check** — For each of the three silver tables, groups by `sub_industry` and shows:
- Total row count
- Number of distinct tickers
- Earliest and latest date in the data

**Spot check** — Reads the last 5 rows of `silver_daily` for ticker `LLY` and displays key indicator columns (`close`, `daily_return`, `price_indexed`, `sma_20`, `rsi_14`, `macd`, `rolling_volatility`) to visually confirm the indicators are computing correctly.

---

## Output Schema Summary

### `silver_daily`

| Column | Type | Description |
|---|---|---|
| `ticker` | string | Stock ticker symbol |
| `sector` | string | Market sector |
| `sub_industry` | string | Sub-industry classification |
| `timestamp` | timestamp | Bar timestamp (UTC) |
| `date` | date | Calendar date |
| `day_of_week` | string | Full weekday name |
| `week_of_year` | int | ISO week number |
| `month` | int | Month (1–12) |
| `quarter` | string | Quarter label (Q1–Q4) |
| `year` | int | Year |
| `is_month_end` | boolean | Last day of month flag |
| `is_quarter_end` | boolean | Last day of quarter flag |
| `open` | double | Opening price |
| `high` | double | High price |
| `low` | double | Low price |
| `close` | double | Closing price |
| `volume` | double | Shares traded |
| `vwap` | double | Volume-weighted average price |
| `transaction_count` | long | Number of trades |
| `daily_return` | double | % change from previous close |
| `price_indexed` | double | Price normalised to 100 at first bar |
| `sma_20` | double | 20-period simple moving average |
| `sma_50` | double | 50-period simple moving average |
| `ema_12` | double | 12-period exponential moving average |
| `ema_26` | double | 26-period exponential moving average |
| `macd` | double | MACD line (ema_12 − ema_26) |
| `rsi_14` | double | 14-period Relative Strength Index |
| `bb_upper` | double | Bollinger upper band |
| `bb_mid` | double | Bollinger middle band (20-period SMA) |
| `bb_lower` | double | Bollinger lower band |
| `rolling_volatility` | double | 20-period std dev of daily returns |

### `silver_5min` / `silver_1min`

Same as above except time columns replace daily calendar fields with intraday fields: `timestamp_et`, `hour`, `minute`, `session`. SMAs use 10- and 20-period windows instead of 20 and 50.

# Sector Intelligence Pipeline — Gold Layer Documentation

## Overview

The Gold Layer is the final stage of the pipeline. It reads from the Silver tables and produces five analytical tables plus one consolidated dashboard snapshot, all optimised for Power BI consumption.

**Source tables:** `silver_daily`, `silver_5min`

**Output tables:**

| Table | Description |
|---|---|
| `gold_sector_daily` | Daily sector-level rollups with breadth and volume metrics |
| `gold_ticker_signals` | Per-ticker per-day technical signal flags |
| `gold_rankings_daily` | Intra-sector rankings across four dimensions |
| `gold_correlation_daily` | 30-day rolling pairwise return correlations within sectors |
| `gold_intraday_summary` | Session-level (pre/regular/after) OHLCV aggregates from 5min bars |
| `dashboard_measures` | Consolidated snapshot joining all gold tables — direct Power BI source |

---

## Pipeline Architecture

```
silver_daily (cached)
     │
     ├── build_sector_daily()       → gold_sector_daily
     ├── build_ticker_signals()     → gold_ticker_signals
     ├── build_rankings_daily()     → gold_rankings_daily
     └── build_correlation()        → gold_correlation_daily

silver_5min
     └── build_intraday_summary()   → gold_intraday_summary

All gold tables
     └── consolidated join          → dashboard_measures
```

`silver_daily` is cached in memory since it feeds four of the five gold tables.

All gold tables use **full overwrite** — gold is always recomputed from silver, no incremental logic.

---

## Table Descriptions

### `gold_sector_daily`

One row per sector per date. Aggregates all tickers within each sector into a single daily summary.

**Key metrics:**
- Volume-weighted average open and close prices
- Total volume and transaction count
- Breadth: count of advancing, declining, and unchanged tickers
- Advance/decline ratio
- Averaged RSI, MACD, volatility, SMA20, SMA50
- Return spread (best return − worst return within sector that day)

---

### `gold_ticker_signals`

One row per ticker per date. Classifies each bar against nine technical signal conditions.

| Signal | Condition |
|---|---|
| `signal_rsi_overbought` | RSI > 70 |
| `signal_rsi_oversold` | RSI < 30 |
| `signal_macd_bullish_cross` | MACD crossed from negative to positive |
| `signal_macd_bearish_cross` | MACD crossed from positive to negative |
| `signal_bb_upper_touch` | Close ≥ Bollinger upper band |
| `signal_bb_lower_touch` | Close ≤ Bollinger lower band |
| `signal_golden_cross` | SMA20 crossed above SMA50 |
| `signal_death_cross` | SMA20 crossed below SMA50 |
| `signal_vol_spike` | Volatility > 2× its own 20-day average |
| `any_signal` | Any of the above is true |

Also carries the underlying indicator values (`rsi_14`, `macd`, `sma_20`, `sma_50`, `bb_upper`, `bb_lower`, `rolling_volatility`) for dashboard context.

---

### `gold_rankings_daily`

One row per ticker per date. Ranks each ticker within its sector on four dimensions. Rank 1 = best in sector that day.

| Rank Column | Ordered By |
|---|---|
| `return_rank_in_sector` | `daily_return` descending |
| `rsi_rank_in_sector` | `rsi_14` descending |
| `volatility_rank_in_sector` | `rolling_volatility` descending |
| `volume_rank_in_sector` | `volume` descending |
| `composite_rank_score` | Average of all four ranks (lower = more notable) |

---

### `gold_correlation_daily`

One row per ticker pair per date. Computes 30-day rolling Pearson correlation of `daily_return` between every pair of tickers **within the same sector**.

Cross-sector correlations are intentionally excluded — within-sector correlations are more actionable for diversification decisions.

Only unique pairs are stored (ticker_a < ticker_b alphabetically) to avoid duplicates.

Schema: `sector | date | ticker_a | ticker_b | correlation`

---

### `gold_intraday_summary`

One row per ticker per date per session. Aggregates 5min bars into session-level OHLCV.

Sessions: `pre` (04:00–09:29 ET), `regular` (09:30–15:59 ET), `after` (16:00–19:59 ET).

Key derived column: `session_return = (session_close − session_open) / session_open` — how much price moved within that session.

---

### `dashboard_measures`

A single wide table joining all five gold tables on `sector`. This is the direct source for Power BI — one connection, all metrics available.

Column names are sanitised (spaces and special characters replaced with underscores) to ensure Power BI compatibility.

---

## Change Log

| Version | Date | Changes |
|---|---|---|
| v1.0 | Jun 2026 | Initial gold layer. Five analytical tables + dashboard snapshot. |