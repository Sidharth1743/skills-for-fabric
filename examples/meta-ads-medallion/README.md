# Meta Ads Medallion Example (Bronze → Silver)

End-to-end pattern for Meta Marketing API connector extracts using Fabric lakehouse medallion layers.

## What this covers

| Layer | Purpose |
|-------|---------|
| Bronze | Land connector CSV envelope + `raw_json` as Delta (`bronze.meta_*`) |
| Silver | Flatten, type, dedupe, explode actions → `silver.meta_*` |

Gold (campaign performance marts) is intentionally out of scope for this step.

## Inputs (all 6 files)

| File pattern | Bronze table | Silver table(s) |
|---|---|---|
| `meta_campaigns_latest*.csv` | `bronze.meta_campaigns` | `silver.meta_campaigns` |
| `meta_adsets_latest*.csv` | `bronze.meta_adsets` | `silver.meta_adsets` |
| `meta_ads_latest*.csv` | `bronze.meta_ads` | `silver.meta_ads` |
| `meta_adset_insights_latest*.csv` | `bronze.meta_adset_insights` | `silver.meta_adset_insights_daily` + actions |
| `meta_ad_insights_latest*.csv` | `bronze.meta_ad_insights` | `silver.meta_ad_insights_daily` + actions |
| `meta_path_verify*.csv` | `bronze.meta_path_verify` | `silver.meta_path_verify` |

See [docs/SILVER_SCHEMA.md](docs/SILVER_SCHEMA.md) for grains, keys, and quality rules.

## Local validation (no Fabric required)

```bash
python examples/meta-ads-medallion/scripts/bronze_to_silver.py \
  --input-dir /path/to/meta/csvs \
  --output-dir examples/meta-ads-medallion/output/silver
```

## Fabric execution

1. Create a schema-enabled lakehouse and schemas `bronze` / `silver`.
2. Upload CSVs to `Files/landing/meta/`.
3. Run `notebooks/01_bronze_ingest.ipynb`.
4. Run `notebooks/02_silver_transform.ipynb`.
5. Confirm validation cell: orphan adsets/ads = 0.

## Notes

- Do **not** commit production Meta extracts into git (`data/` and `output/` are gitignored).
- Meta budget fields are converted from minor units (cents) to major currency amounts in Silver.
- Insight `actions[]` are exploded to long-format `silver.meta_insight_actions` for lead/engagement analysis.
