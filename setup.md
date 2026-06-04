# Setup Guide

## Prerequisites

- Microsoft Fabric trial or paid account → [app.fabric.microsoft.com](https://app.fabric.microsoft.com)
- Alpaca Markets account (free) → [alpaca.markets](https://alpaca.markets) — choose Paper Trading, no deposit needed

---

## Step 1 — Create a Fabric Workspace

1. Go to [app.fabric.microsoft.com](https://app.fabric.microsoft.com)
2. Click **Workspaces** in the left sidebar → **New workspace**
3. Give it any name (e.g. `sector-intelligence`)
4. Click **Apply**

---

## Step 2 — Create a Lakehouse

1. Inside your workspace click **New** → **Lakehouse**
2. Give it a name — remember this name, you will need it in Step 3
3. Click **Create**

---

## Step 3 — Configure Your Lakehouse Name

1. Open `core/config.py` from this repo
2. Set `LAKEHOUSE` to whatever you named your Lakehouse in Step 2:
```python
LAKEHOUSE = "your_lakehouse_name_here"
```
3. All table references (`BRONZE_TABLE`, `SILVER_DAILY`, etc.) will resolve automatically from this one value

---

## Step 4 — Create the Setup Notebook

1. Inside your Fabric workspace click **New** → **Notebook**
2. Rename it `Setup`
3. Attach it to your Lakehouse: click **Add Lakehouse** in the left panel → select the Lakehouse you created
4. Copy the contents of `notebooks/Setup.ipynb` from this repo into the notebook, cell by cell
5. Click **Run All**

This will:
- Download `config.py`, `ingest.py`, and `pipeline.py` from this repo into your Lakehouse
- Create all Bronze, Silver, and Gold Delta tables

---

## Step 5 — Get Your Alpaca API Keys

1. Log into [alpaca.markets](https://alpaca.markets)
2. Go to **Paper Trading** → **API Keys** → **Generate New Key**
3. Copy the **API Key ID** and **Secret Key** — you will need these in Step 7

---

## Step 6 — Import the Remaining Notebooks

Repeat for each notebook below. For each one: **New → Notebook → rename → attach Lakehouse → paste cells**.

| Notebook | File |
|---|---|
| `Setup` | `notebooks/1_Ingest.ipynb` |
| `ProcessBronze` | `notebooks/2_ProcessBronze.ipynb` |
| `ProcessSilver` | `notebooks/3_ProcessSilver.ipynb` |
| `CreateMeasures` | `notebooks/4_GoldMeasures.ipynb` |

---

## Step 7 — Run the Pipeline

1. Open `Setup` notebook
2. Run it — it will prompt you for your Alpaca keys:
Enter Alpaca API key:
Enter Alpaca secret:
3. **First run only:** before running, set `INCREMENTAL = False` in `core/config.py` to fetch full history. Set it back to `True` after.
4. Run notebooks in order: `Setup` → `ProcessBronze` → `ProcessSilver` → `CreateMeasures`

---
## Step 8 — Connect Power BI Auto-Refresh

To have the pipeline automatically refresh your Power BI semantic model after each run:

1. Open your Power BI workspace in the browser
2. Copy the **Workspace ID** from the URL:
   `app.powerbi.com/groups/{WORKSPACE_ID}/...`
3. Open your semantic model → Settings → copy the **Dataset ID** from the URL:
   `app.powerbi.com/groups/.../datasets/{DATASET_ID}`
4. Paste both into `core/config.py`:
```python
PBI_WORKSPACE_ID = "your_workspace_id_here"
PBI_DATASET_ID   = "your_dataset_id_here"
```
5. If left as placeholders the pipeline will still run — only the Power BI refresh step will fail.

## Expected Run Times

| Mode | Approx Time |
|---|---|
| Full backfill (`INCREMENTAL=False`) | ~2 min |
| Incremental (`INCREMENTAL=True`) | ~1 min |

---

## Troubleshooting

**`ModuleNotFoundError: No module named 'config'`**
The setup notebook did not finish downloading the core files. Re-run `Setup`.

**`AnalysisException: Table not found`**
The Lakehouse name in `config.py` does not match the one you created. Check `LAKEHOUSE` in `core/config.py`.

**`0 bars returned` for a ticker**
Normal for low-volume tickers (e.g. ESEA, FLGT) on quiet days. Not a pipeline error.

**Alpaca 403 error**
Your API keys are from Live Trading, not Paper Trading. Regenerate keys under the Paper Trading dashboard.

---

## Repo Structure

sector-intelligence/
├── core/
│   ├── config.py          ← watchlist, table names, pipeline flags
│   ├── ingest.py          ← fetch_bars, fetch_all, write_bronze
│   └── pipeline.py        ← run_pipeline entry point
├── notebooks/
│   ├── 0_Setup.ipynb      ← run once to create tables
│   ├── 1_Ingest.ipynb     ← fetches data from Alpaca
│   ├── 2_ProcessBronze.ipynb
│   ├── 3_ProcessSilver.ipynb
│   └── 4_GoldMeasures.ipynb
├── requirements.txt
├── setup.md               ← you are here
└── readme.md