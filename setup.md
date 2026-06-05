# Sector Intelligence — Setup Guide

## Prerequisites

- A Microsoft account
- A [Microsoft Fabric free trial](https://app.fabric.microsoft.com) workspace
- A GitHub account with access to this repo
- Your API credentials (key, secret, and base URL) for the data source

---

## Step 1 — Create a Fabric Workspace

1. Go to [app.fabric.microsoft.com](https://app.fabric.microsoft.com)
2. Click **Workspaces** in the left sidebar → **New workspace**
3. Give it a name (e.g. `Sector Intelligence`) and click **Apply**

---

## Step 2 — Open the Setup Notebook

1. Download the notebook Setup.iynb from the notebooks folder
2. Inside your workspace, click Import and Select Notebook   and upload the downloaded notebook

---

## Step 3 — Fill in Your Config

At the top of the notebook, find the **Cell 1: Config** section and fill in your values:

```python
LAKEHOUSE_NAME  = "MyLakehouse"       # name for the lakehouse that will be created
PIPELINE_NAME   = "MyPipeline"        # name for the pipeline that will be created
GITHUB_TOKEN    = ""                  # leave empty — repo is public
                                      # add a GitHub PAT if repo is private

# Your data source API credentials
API_KEY         = "your-api-key-here"
API_SECRET      = "your-api-secret-here"
```

> **Never commit real API keys to the repo.** Only fill these in inside Fabric — they are injected at runtime and never written back to GitHub.

---

## Step 4 — Run the Setup Notebook

1. Click **Run all** at the top of the notebook
2. The notebook will automatically:
   - Create a Lakehouse in your workspace
   - Fetch all notebooks from GitHub and create them in your workspace
   - Inject your API credentials into the ingestion notebook
   - Create a pipeline that runs all notebooks in order
3. Setup takes approximately 3–5 minutes to complete
4. When done you will see a summary printed at the bottom confirming all item IDs

If any step fails, check the error message — the most common cause is an incorrect API key or a Fabric provisioning delay. Simply re-run the notebook from the failed cell.

---

## Step 5 — Schedule the Pipeline

1. In your workspace, open **MyPipeline** (or whatever name you set in config)
2. Click **Schedule** in the top toolbar
3. Set your preferred frequency (minimum interval is **1 minute** on Fabric free trial)
4. Click **Apply**

The pipeline runs all notebooks in order: Ingest → ProcessBronze → ProcessSilver → the final write step.

---

## Step 6 — Connect Power BI

1. In your workspace, open the Lakehouse created in step 4
2. Click **New semantic model** and select the tables written by the pipeline
3. Open Power BI Desktop or use Power BI in the browser
4. Connect to your Fabric Lakehouse via **OneLake data hub**
5. Build your reports on top of the semantic model

---

## Troubleshooting

**Notebook creation fails with `PyToIPynbFailure`**
The notebook `.py` format is incorrect. Make sure all notebooks in the `notebooks/` folder have the `# Fabric notebook source` header as the first line.

**`EntityNotFound` when creating the schedule**
The pipeline was not fully provisioned yet. Wait 30 seconds and try scheduling again from the Fabric UI.

**API keys not injected correctly**
Make sure your ingestion notebook contains the exact placeholder lines:
```python
API_KEY      = ""
API_SECRET   = ""
API_BASE_URL = ""
```

**GitHub fetch returns 404**
If the repo is private, generate a GitHub Personal Access Token at **GitHub → Settings → Developer settings → Personal access tokens → Fine-grained tokens** with `Contents: Read` permission and paste it into `GITHUB_TOKEN` in Cell 1.

**Pipeline runs but no data appears in Lakehouse**
Run the notebooks manually in order (Ingest → ProcessBronze → ProcessSilver → final step) to see which one errors. Check that your API credentials are correct.

---

## Repo Structure

```
sector-intelligence/
├── notebooks/
│   ├── 01_Ingest.ipynb          # Fetches raw data from API
│   ├── 02_ProcessBronze.ipynb   # Initial cleaning and schema enforcement
│   ├── 03_ProcessSilver.ipynb   # Transformation and enrichment
│   ├── 04_...ipynb              # Writes final tables to Lakehouse
│   └── Setup.ipynb              # This setup script (run once)
├── core/
│   ├── config.py                # Shared configuration
│   ├── ingest.py                # Ingestion logic
│   └── pipeline.py              # Pipeline helpers
├── readme.md
├── setup.md                     # You are here
└── requirements.txt
```

---

## Notes on the Free Trial

- The Fabric free trial gives you **60 days** of access with capacity units included
- The minimum pipeline schedule interval is **1 minute**
- Lakehouses, Notebooks, and Pipelines are all available on the free trial
- You do not need a Power BI Pro licence to view reports inside Fabric — only to share them externally