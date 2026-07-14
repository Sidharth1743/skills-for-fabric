# MIP Medallion Refresh Pipeline

Automatically refreshes **Silver files + Gold `rpt_*` / `vw_*` + `Staging_Gold`** when new Bronze batch files land in **Development or Staging**.

## Fabric items (MarketingIntelligencePlatform)

| Item | Name | Id |
|------|------|----|
| Workspace | MarketingIntelligencePlatform | `718e8176-5d40-4a9c-88ff-50ac97ac49ba` |
| Lakehouse | mip_lakehouse | `981fbe98-2f01-41d8-bf2f-a85e5cd9e2a2` |
| Notebook | `mip_medallion_scheduled_refresh` | `3d829efc-d670-489f-98ad-65770ec13b74` |
| Pipeline | `MIP_Medallion_Refresh_Pipeline` | `5e73aa34-45ef-4875-b640-ef4bc0e5c104` |
| Schedule | Cron every **5 minutes** (UTC, enabled) | `e8d5ac12-6d86-422d-91d9-abe3587cf509` |

## Bronze sources (Development + Staging)

Batch CSVs + nested pointers are discovered from **both** environments:

```text
Files/Development/Bronze/Meta_ads/...
Files/Staging/Bronze/Meta_ads/...
Files/Staging/Bronze/Bronze/Meta_ads/...   # legacy nested copy also scanned

Files/Development/Bronze/Google_ads/...
Files/Staging/Bronze/Google_ads/...
Files/Staging/Bronze/Bronze/Google_ads/...
```

Per entity:

```text
{root}/{entity}/
├── {entity}_{batchId}.csv
└── {tenantId}/{accountId}/{connectorId}/
    └── {entity}_latest_batch.txt
```

Meta entities: `meta_campaigns`, `meta_adsets`, `meta_ads`, `meta_ad_insights`  
Google entities: `google_campaigns`, `google_ad_groups`, `google_ads`, `google_ad_performance`

## Behavior (incremental — does not wipe Gold)

1. Discover nested `*_latest_batch.txt` pointers across **all Bronze roots** and load **batch UUID CSVs** (union; prefers batch files over `*_latest.csv`).
2. **NOOP / skip** when the combined pointer set matches the watermark (avoids overlapping 5-min runs).
3. Parse connector envelope (`raw_json`) for Meta + Google.
4. **MERGE upsert Silver Delta files** into:
   - `Files/Development/Silver/meta_ads/silver_meta_ad_insights`
   - `Files/Staging/Silver/meta_ads/silver_meta_ad_insights`
   - `Files/Silver/meta_ads/silver_meta_ad_insights`
   - `Files/Development/Silver/GoogleAds/silver_google_ad_performance`
   - `Files/Staging/Silver/GoogleAds/silver_google_ad_performance`
5. **MERGE upsert** into managed Gold tables:
   - `Gold.rpt_meta_ad_performance_daily`
   - `Gold.rpt_google_ad_performance_daily`
   - `Gold.rpt_unified_ad_performance`  
   Existing unmatched rows are **kept**.
6. Rematerialize Delta tables (SQL-endpoint safe):
   - `Gold.vw_campaign_performance`
   - `Gold.vw_adset_performance`
   - `Gold.vw_ad_performance`
   - `Gold.vw_unified_ad_performance`
7. **Bidirectional Bronze fill** (copy missing files only; never wipe Staging-only landings):
   - Development ↔ Staging `Bronze/{Meta_ads,Google_ads}`
8. Mirror managed tables → **`Staging_Gold.*`** (same `rpt_*` / `vw_*` names).
9. Export Gold Delta snapshots to:
   - `Files/Development/Gold/tables/{table}`
   - `Files/Staging/Gold/tables/{table}`
10. Write watermark/summaries:
    - `Files/Silver/_control/medallion_pipeline_watermark.json`
    - `Files/Development/Gold/exports/pipeline_refresh_summary.txt`
    - `Files/Staging/Gold/exports/pipeline_refresh_summary.txt`

Grain key: `platform + account_id + campaign_id + adset_id + ad_id + full_date`

## Manual run

```bash
WS=718e8176-5d40-4a9c-88ff-50ac97ac49ba
PIPE=5e73aa34-45ef-4875-b640-ef4bc0e5c104

az rest --method post \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS/items/$PIPE/jobs/instances?jobType=Pipeline" \
  --body '{}'
```

## Notes

- Open **Tables → Gold → `vw_*`** and **Tables → Staging_Gold → `vw_*`** (Delta tables, not Lakehouse Views).
- Connector may land Bronze under Development, Staging, or both — cron watches all listed roots.
- Disable schedule via PATCH `enabled: false` if you need to pause.
