# Deploy to your Fabric account

Two paths: **Portal (UI)** or **CLI script**.

## Prerequisites

1. Microsoft Fabric capacity (trial or paid) assigned to a workspace
2. Permission to create Lakehouse + Notebook items
3. Your 6 Meta CSV files locally
4. For CLI: Azure CLI + `jq` + `curl`

```bash
az login --allow-no-subscriptions
az account get-access-token --resource https://api.fabric.microsoft.com
```

---

## Option A — Fabric Portal (fastest first run)

### 1. Create lakehouse
1. Open [Fabric](https://app.fabric.microsoft.com)
2. Create/open a workspace (must have capacity)
3. **New** → **Lakehouse** → enable **Lakehouse schemas** if prompted  
   Name example: `meta_ads_lh`

### 2. Upload CSVs
1. In the lakehouse, open **Files**
2. Create folder `landing/meta`
3. Upload all `meta_*.csv` files into `Files/landing/meta/`

### 3. Import notebooks
Import these from the repo (or copy-paste cells):

| Notebook | Path |
|----------|------|
| Bronze | `examples/meta-ads-medallion/notebooks/01_bronze_ingest.ipynb` |
| Silver | `examples/meta-ads-medallion/notebooks/02_silver_transform.ipynb` |
| Gold | `examples/meta-ads-medallion/notebooks/03_gold_star_schema.ipynb` |

For each notebook:
1. Open it → **Add lakehouse** / set default lakehouse = `meta_ads_lh`
2. Confirm parameters cell:
   - Bronze: `landing_path = "Files/landing/meta"`
   - Silver/Gold: schemas `bronze` / `silver` / `gold`

### 4. Run in order
1. Run **01 bronze** → creates `bronze.meta_*`
2. Run **02 silver** → creates `silver.meta_*`
3. Run **03 gold** → creates dims, fact, `rpt_meta_ad_performance_daily`, view `vw_meta_ad_performance`

### 5. Verify
In lakehouse **Tables** (or SQL analytics endpoint):

```sql
SELECT COUNT(*) FROM gold.rpt_meta_ad_performance_daily;
SELECT * FROM gold.vw_meta_ad_performance LIMIT 100;
```

Connect Power BI / Direct Lake to `gold.rpt_meta_ad_performance_daily` for reporting.

---

## Option B — CLI deploy script

From your machine (with repo cloned and CSVs available):

```bash
cd examples/meta-ads-medallion

export FABRIC_WORKSPACE_NAME="MetaAds-Dev"
export FABRIC_LAKEHOUSE_NAME="meta_ads_lh"
export META_CSV_DIR="/path/to/your/meta/csvs"
# Only needed if the workspace does not exist yet:
# export FABRIC_CAPACITY_ID="<your-capacity-guid>"

chmod +x scripts/deploy_to_fabric.sh
./scripts/deploy_to_fabric.sh
```

The script will:
1. Login / use existing `az` session
2. Create or reuse workspace + schema-enabled lakehouse
3. Upload CSVs to `Files/landing/meta`
4. Deploy the 3 notebooks (bound to the lakehouse)
5. Run Bronze → Silver → Gold sequentially

Upload only (no run):

```bash
export SKIP_RUN=1
./scripts/deploy_to_fabric.sh
```

---

## What lands in Fabric

```text
meta_ads_lh
├── Files/landing/meta/*.csv
├── bronze.meta_campaigns / meta_adsets / meta_ads / ...
├── silver.meta_* (flattened)
└── gold.
    ├── dim_date, dim_account, dim_campaign, dim_adset, dim_ad
    ├── fact_ad_performance_daily
    ├── rpt_meta_ad_performance_daily   ← use this for BI
    └── vw_meta_ad_performance          ← SQL view
```

Budgets are in **INR** (`*_inr` = paise ÷ 100).

---

## Common issues

| Issue | Fix |
|-------|-----|
| Workspace has no capacity | Assign a Fabric capacity in Admin / workspace settings |
| `az login` “No subscriptions” | `az login --allow-no-subscriptions --tenant <tenantId>` |
| Notebook can’t see `Files/` | Bind default lakehouse on the notebook |
| Gold drops some insight rows | Ad exists in insights but not in ads extract (FK integrity) |
| SQL view not visible in SQL endpoint | Wait for SQL endpoint sync after Gold notebook completes |

---

## Security note

Do not commit production Meta CSVs to git. Keep them in OneLake `Files/` or a private storage account only.
