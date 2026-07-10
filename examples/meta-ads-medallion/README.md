# End-to-end pattern for Meta Marketing API connector extracts using Fabric lakehouse medallion layers (Bronze → Silver → Gold).

## What this covers

| Layer | Purpose |
|-------|---------|
| Bronze | Land connector CSV envelope + `raw_json` as Delta (`bronze.meta_*`) |
| Silver | Flatten, type, dedupe, explode actions → `silver.meta_*` |
| Gold | Star schema (dims + fact), one reporting table, SQL view |

**Budget rule:** Meta budgets arrive in paise → divide by 100 for INR (`*_inr` in Gold).

## Inputs (all 6 files)

| File pattern | Bronze table | Silver table(s) |
|---|---|---|
| `meta_campaigns_latest*.csv` | `bronze.meta_campaigns` | `silver.meta_campaigns` |
| `meta_adsets_latest*.csv` | `bronze.meta_adsets` | `silver.meta_adsets` |
| `meta_ads_latest*.csv` | `bronze.meta_ads` | `silver.meta_ads` |
| `meta_adset_insights_latest*.csv` | `bronze.meta_adset_insights` | `silver.meta_adset_insights_daily` + actions |
| `meta_ad_insights_latest*.csv` | `bronze.meta_ad_insights` | `silver.meta_ad_insights_daily` + actions |
| `meta_path_verify*.csv` | `bronze.meta_path_verify` | `silver.meta_path_verify` |

See [docs/SILVER_SCHEMA.md](docs/SILVER_SCHEMA.md) and [docs/GOLD_STAR_SCHEMA.md](docs/GOLD_STAR_SCHEMA.md).

### Gold star schema

| Object | Role |
|--------|------|
| `dim_date`, `dim_account`, `dim_campaign`, `dim_adset`, `dim_ad` | Dimensions |
| `fact_ad_performance_daily` | Fact (ad × day) |
| `rpt_meta_ad_performance_daily` | Single denormalized reporting table |
| `vw_meta_ad_performance` | SQL view over the star |

SQL view DDL: [schemas/vw_meta_ad_performance.sql](schemas/vw_meta_ad_performance.sql)

## Local validation (no Fabric required)

```bash
python examples/meta-ads-medallion/scripts/bronze_to_silver.py \
  --input-dir /path/to/meta/csvs \
  --output-dir examples/meta-ads-medallion/output/silver

python examples/meta-ads-medallion/scripts/silver_to_gold.py \
  --silver-dir examples/meta-ads-medallion/output/silver \
  --output-dir examples/meta-ads-medallion/output/gold
```

## Fabric execution

1. Create a schema-enabled lakehouse and schemas `bronze` / `silver` / `gold`.
2. Upload CSVs to `Files/landing/meta/`.
3. Run `notebooks/01_bronze_ingest.ipynb`.
4. Run `notebooks/02_silver_transform.ipynb`.
5. Run `notebooks/03_gold_star_schema.ipynb`.
6. Confirm validation: fact rows = reporting rows, orphan FKs = 0.

## Notes

- Do **not** commit production Meta extracts into git (`data/` and `output/` are gitignored).
- Meta budget fields: paise ÷ 100 → INR (`*_amount` in Silver, `*_inr` in Gold).
- Insight `actions[]` are exploded in Silver and pivoted into fact measures (`leads`, `link_clicks`, etc.) in Gold.
