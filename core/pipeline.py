from datetime import datetime
from ingest import fetch_all, write_bronze
from config import TICKERS, SECTOR_FILTER, WATCHLIST, INCREMENTAL

def run_pipeline():
    start = datetime.utcnow()

    # Apply sector filter if set
    tickers = (
        [t for t, v in WATCHLIST.items() if v["sector"] == SECTOR_FILTER]
        if SECTOR_FILTER
        else TICKERS
    )

    print("=" * 65)
    print("  Sector Intelligence Pipeline")
    print("  Pharmaceuticals × Air Freight & Logistics")
    print(f"  Mode         : {'Incremental' if INCREMENTAL else 'Full backfill'}")
    print(f"  Sector filter: {SECTOR_FILTER or 'All'}")
    print(f"  Tickers      : {len(tickers)} — {tickers}")
    print(f"  Started      : {start.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 65)

    # ── Step 1: Ingest ────────────────────────────────────────
    print("\n[1/3] Ingesting 1min bars...")
    raw_1min = fetch_all(tickers, "1min")
    write_bronze(raw_1min, "1min")

    print("\n[2/3] Ingesting 5min bars...")
    raw_5min = fetch_all(tickers, "5min")
    write_bronze(raw_5min, "5min")

    if not INCREMENTAL:
        print("\n[3/3] Ingesting daily bars...")
        raw_daily = fetch_all(tickers, "daily")
        write_bronze(raw_daily, "daily")
    else:
        print("\n[3/3] Skipping daily bars (incremental mode, REFRESH_DAILY=False)")

    elapsed = (datetime.utcnow() - start).total_seconds() / 60
    print(f"\n✅ Pipeline complete in {elapsed:.1f} minutes")
    print(f"   Bronze table: {BRONZE_TABLE}")