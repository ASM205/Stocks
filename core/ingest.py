import os
import time
import requests
from datetime import datetime, timedelta
from config import RANGES, BATCH_SIZE, BATCH_SLEEP, CALL_SLEEP, INCREMENTAL, WATCHLIST

ALPACA_KEY    = os.getenv("ALPACA_KEY")
ALPACA_SECRET = os.getenv("ALPACA_SECRET")
BASE_URL      = "https://data.alpaca.markets/v2/stocks"
TODAY         = datetime.utcnow().date()

HEADERS = {
    "APCA-API-KEY-ID":     ALPACA_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET,
}

def write_bronze(raw: dict[str, list], granularity: str):
    """
    Flatten raw Polygon results → Spark DataFrame → append to Delta bronze table.
    Deduplicates on (ticker, granularity, t) before writing.
    Now enriches each row with sector, sub_industry, tier from WATCHLIST.
    """
    rows = [
        {
            "ticker":       ticker,
            "sector":       WATCHLIST[ticker]["sector"],
            "sub_industry": WATCHLIST[ticker]["sub_industry"],
            "granularity":  granularity,
            "t":  str(bar["t"])  if bar.get("t")  is not None else None,
            "o":  str(bar["o"])  if bar.get("o")  is not None else None,
            "h":  str(bar["h"])  if bar.get("h")  is not None else None,
            "l":  str(bar["l"])  if bar.get("l")  is not None else None,
            "c":  str(bar["c"])  if bar.get("c")  is not None else None,
            "v":  str(bar["v"])  if bar.get("v")  is not None else None,
            "vw": str(bar["vw"]) if bar.get("vw") is not None else None,
            "n":  str(bar["n"])  if bar.get("n")  is not None else None,
        }
        for ticker, bars in raw.items()
        for bar in bars
    ]

    if not rows:
        print(f"Bronze [{granularity}]: nothing to write")
        return

    # Step 1: uniform string schema — no type inference
    df_raw = spark.createDataFrame(rows, schema=STRING_SCHEMA)

    # Step 2: cast to target types
    df = (df_raw
        .withColumn("t",  F.col("t") .cast(LongType()))
        .withColumn("o",  F.col("o") .cast(DoubleType()))
        .withColumn("h",  F.col("h") .cast(DoubleType()))
        .withColumn("l",  F.col("l") .cast(DoubleType()))
        .withColumn("c",  F.col("c") .cast(DoubleType()))
        .withColumn("v",  F.col("v") .cast(DoubleType()))
        .withColumn("vw", F.col("vw").cast(DoubleType()))
        .withColumn("n",  F.col("n") .cast(LongType()))
        .select("ticker", "sector", "sub_industry",
                "granularity", "t", "o", "h", "l", "c", "v", "vw", "n")
    )

    # Step 3: deduplicate against existing table
    try:
        existing = spark.read.format("delta").table(BRONZE_TABLE)
        new_rows = df.join(
            existing.filter(F.col("granularity") == granularity)
                    .select("ticker", "granularity", "t"),
            on=["ticker", "granularity", "t"],
            how="left_anti",
        )
    except Exception:
        new_rows = df  # first run — table doesn't exist yet

    n = new_rows.count()
    if n > 0:
        (new_rows.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(BRONZE_TABLE))
        print(f"✓ Bronze [{granularity}]: +{n:,} rows")
    else:
        print(f"✓ Bronze [{granularity}]: already up to date")


def fetch_all(tickers: list, granularity: str) -> dict[str, list]:
    """
    Fetch one granularity for all tickers.
    granularity: '1min' | '5min' | 'daily'
    Returns { ticker: [bars] }
    """
    cfg = RANGES[granularity]

    if INCREMENTAL and cfg["incremental_days"] > 0:
        from_date = (TODAY - timedelta(days=cfg["incremental_days"])).isoformat()
    else:
        from_date = (TODAY - timedelta(days=cfg["lookback_days"])).isoformat()
    to_date = TODAY.isoformat()

    print(f"\n── Fetching {granularity} bars ({from_date} → {to_date}) ──")
    results = {}

    for batch_start in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[batch_start : batch_start + BATCH_SIZE]
        batch_num    = (batch_start // BATCH_SIZE) + 1
        total_batches = -(-len(tickers) // BATCH_SIZE)
        print(f"  Batch {batch_num}/{total_batches}: {batch}")

        for i, ticker in enumerate(batch):
            bars = fetch_bars(
                ticker,
                cfg["timeframe"],
                from_date,
                to_date,
            )
            results[ticker] = bars
            if i < len(batch) - 1:
                time.sleep(CALL_SLEEP)

        if batch_start + BATCH_SIZE < len(tickers):
            print(f"  ⏳ Batch pause: {BATCH_SLEEP}s...")
            time.sleep(BATCH_SLEEP)

    return results

def fetch_bars(ticker: str, timeframe: str,
               from_date: str, to_date: str) -> list[dict]:
    """
    Fetch OHLCV bars from Alpaca for one ticker + timeframe.
    Handles cursor pagination. Retries up to 3x on 429 or network error.
    Returns list of raw bar dicts.
    """
    url = f"{BASE_URL}/{ticker}/bars"
    params = {
        "timeframe":  timeframe,
        "start":      from_date,
        "end":        to_date,
        "limit":      10000,
        "adjustment": "all",
        "feed":       "iex",   # free tier feed (use 'sip' if you upgrade)
    }

    all_results = []
    retries = 3

    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=15)

            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                print(f"  ⚠ 429 on {ticker} {timeframe}. Waiting {wait}s...")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()
            all_results.extend(data.get("bars") or [])

            # Follow Alpaca cursor pagination
            while data.get("next_page_token"):
                resp = requests.get(
                    url,
                    headers=HEADERS,
                    params={**params, "page_token": data["next_page_token"]},
                    timeout=15,
                )
                resp.raise_for_status()
                data = resp.json()
                all_results.extend(data.get("bars") or [])
                time.sleep(0.1)  # barely needed but polite

            print(f"  ✓ {ticker:6s} [{timeframe}]: {len(all_results):,} bars")
            return all_results

        except requests.RequestException as e:
            print(f"  ✗ {ticker} attempt {attempt+1}: {e}")
            time.sleep(5)

    print(f"  ✗ {ticker}: all retries exhausted — skipping")
    return []