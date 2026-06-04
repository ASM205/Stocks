WATCHLIST = {

    # ── Pharmaceuticals (top 5 by market cap) ────────────────
    "LLY":  {"name": "Eli Lilly",                 "sector": "Health Care", "sub_industry": "Pharmaceuticals"},
    "JNJ":  {"name": "Johnson & Johnson",         "sector": "Health Care", "sub_industry": "Pharmaceuticals"},
    "ABBV": {"name": "AbbVie",                    "sector": "Health Care", "sub_industry": "Pharmaceuticals"},
    "MRK":  {"name": "Merck & Co",                "sector": "Health Care", "sub_industry": "Pharmaceuticals"},
    "PFE":  {"name": "Pfizer",                    "sector": "Health Care", "sub_industry": "Pharmaceuticals"},

    # ── Biotechnology (top 5 by market cap) ──────────────────
    "AMGN": {"name": "Amgen",                     "sector": "Health Care", "sub_industry": "Biotechnology"},
    "VRTX": {"name": "Vertex Pharmaceuticals",    "sector": "Health Care", "sub_industry": "Biotechnology"},
    "REGN": {"name": "Regeneron",                 "sector": "Health Care", "sub_industry": "Biotechnology"},
    "GILD": {"name": "Gilead Sciences",           "sector": "Health Care", "sub_industry": "Biotechnology"},
    "MRNA": {"name": "Moderna",                   "sector": "Health Care", "sub_industry": "Biotechnology"},

    # ── Health Care Equipment (top 5 by market cap) ──────────
    "ISRG": {"name": "Intuitive Surgical",        "sector": "Health Care", "sub_industry": "Health Care Equipment"},
    "ABT":  {"name": "Abbott Laboratories",       "sector": "Health Care", "sub_industry": "Health Care Equipment"},
    "SYK":  {"name": "Stryker",                   "sector": "Health Care", "sub_industry": "Health Care Equipment"},
    "BSX":  {"name": "Boston Scientific",         "sector": "Health Care", "sub_industry": "Health Care Equipment"},
    "MDT":  {"name": "Medtronic",                 "sector": "Health Care", "sub_industry": "Health Care Equipment"},

    # ── Air Freight & Logistics (top 5 by market cap) ────────
    "UPS":  {"name": "United Parcel Service",     "sector": "Industrials", "sub_industry": "Air Freight & Logistics"},
    "FDX":  {"name": "FedEx",                     "sector": "Industrials", "sub_industry": "Air Freight & Logistics"},
    "EXPD": {"name": "Expeditors International",  "sector": "Industrials", "sub_industry": "Air Freight & Logistics"},
    "CHRW": {"name": "C.H. Robinson",             "sector": "Industrials", "sub_industry": "Air Freight & Logistics"},
    "GXO":  {"name": "GXO Logistics",             "sector": "Industrials", "sub_industry": "Air Freight & Logistics"},

    # ── Marine Ports & Services (top 5 by market cap) ────────
    "ZIM":  {"name": "ZIM Integrated Shipping",   "sector": "Industrials", "sub_industry": "Marine Ports & Services"},
    "MATX": {"name": "Matson Inc",                "sector": "Industrials", "sub_industry": "Marine Ports & Services"},
    "GSL":  {"name": "Global Ship Lease",         "sector": "Industrials", "sub_industry": "Marine Ports & Services"},
    "SFL":  {"name": "SFL Corporation",           "sector": "Industrials", "sub_industry": "Marine Ports & Services"},
    "ESEA": {"name": "Euroseas",                  "sector": "Industrials", "sub_industry": "Marine Ports & Services"},

    # ── Airport Services (top 4 → only 4 exist) ──────────────
    "GATX": {"name": "GATX Corporation",          "sector": "Industrials", "sub_industry": "Airport Services"},
    "AAWW": {"name": "Atlas Air Worldwide",       "sector": "Industrials", "sub_industry": "Airport Services"},
    "SKYW": {"name": "SkyWest Inc",               "sector": "Industrials", "sub_industry": "Airport Services"},
    "FLGT": {"name": "Fulgent Genetics",          "sector": "Industrials", "sub_industry": "Airport Services"},
}

TICKERS          = list(WATCHLIST.keys())
PHARMA_TICKERS   = [t for t, v in WATCHLIST.items() if v["sector"] == "Health Care"]
LOGISTICS_TICKERS= [t for t, v in WATCHLIST.items() if v["sector"] == "Industrials"]

TODAY = datetime.utcnow().date()

RANGES = {
    "1min":  {"timeframe": "1Min",  "lookback_days": 2,   "incremental_days": 1},
    "5min":  {"timeframe": "5Min",  "lookback_days": 30,  "incremental_days": 1},
    "daily": {"timeframe": "1Day",  "lookback_days": 180, "incremental_days": 0},
}

STRING_SCHEMA = StructType([
    StructField("ticker",       StringType(), True),
    StructField("sector",       StringType(), True),
    StructField("sub_industry", StringType(), True),
    StructField("granularity",  StringType(), True),
    StructField("t",            StringType(), True),
    StructField("o",            StringType(), True),
    StructField("h",            StringType(), True),
    StructField("l",            StringType(), True),
    StructField("c",            StringType(), True),
    StructField("v",            StringType(), True),
    StructField("vw",           StringType(), True),
    StructField("n",            StringType(), True),
])

# Alpaca free tier: 200 calls/min — no need for heavy throttling
BATCH_SIZE  = 20   # tickers per batch
BATCH_SLEEP = 1    # barely any sleep needed
CALL_SLEEP  = 0.1  # 100ms between calls

INCREMENTAL   = False # Note: After first run change to True
SECTOR_FILTER = None

LAKEHOUSE = "your_lakehouse_name"  # ← user sets this once

BRONZE_TABLE          = f"{LAKEHOUSE}.dbo.bronze_bars_v2"
SILVER_DAILY          = f"{LAKEHOUSE}.dbo.silver_daily"
SILVER_5MIN           = f"{LAKEHOUSE}.dbo.silver_5min"
SILVER_1MIN           = f"{LAKEHOUSE}.dbo.silver_1min"
GOLD_SECTOR_DAILY     = f"{LAKEHOUSE}.dbo.gold_sector_daily"
GOLD_TICKER_SIGNALS   = f"{LAKEHOUSE}.dbo.gold_ticker_signals"
GOLD_RANKINGS_DAILY   = f"{LAKEHOUSE}.dbo.gold_rankings_daily"
GOLD_CORRELATION      = f"{LAKEHOUSE}.dbo.gold_correlation_daily"
GOLD_INTRADAY_SUMMARY = f"{LAKEHOUSE}.dbo.gold_intraday_summary"
DASHBOARD_MEASURES    = f"{LAKEHOUSE}.dbo.dashboard_measures"