# MIP Medallion Refresh Pipeline

Automatically refreshes **Gold `rpt_*` + consumer `vw_*` Delta tables** when new Bronze batch files land.

## Fabric items (MarketingIntelligencePlatform)

| Item | Name | Id |
|------|------|----|
| Workspace | MarketingIntelligencePlatform | `718e8176-5d40-4a9c-88ff-50ac97ac49ba` |
| Lakehouse | mip_lakehouse | `981fbe98-2f01-41d8-bf2f-a85e5cd9e2a2` |
| Notebook | `mip_medallion_scheduled_refresh` | `3d829efc-d670-489f-98ad-65770ec13b74` |
| Pipeline | `MIP_Medallion_Refresh_Pipeline` | `5e73aa34-45ef-4875-b640-ef4bc0e5c104` |
| Schedule | Cron every **5 minutes** (UTC, enabled) | `e8d5ac12-6d86-422d-91d9-abe3587cf509` |

## Bronze layout (source of truth)

Batch CSVs + nested pointers live under **Development**:

```text
Files/Development/Bronze/Meta_ads/meta_campaigns/
├── meta_campaigns_{batchId}.csv
└── {tenantId}/{accountId}/{connectorId}/
    └── meta_campaigns_latest_batch.txt

Files/Development/Bronze/Google_ads/google_ad_performance/
├── google_ad_performance_{batchId}.csv
└── {tenantId}/{accountId}/{connectorId}/
    └── google_ad_performance_latest_batch.txt
```

Same pattern for Meta: `meta_adsets`, `meta_ads`, `meta_ad_insights`  
Google: `google_campaigns`, `google_ad_groups`, `google_ads`, `google_ad_performance`

## Behavior (incremental — does not wipe Gold)

1. Discover nested `*_latest_batch.txt` pointers and load **batch UUID CSVs** (prefers batch files over `*_latest.csv`).
2. Parse connector envelope (`raw_json`) for Meta + Google.
3. **MERGE upsert** into:
   - `Gold.rpt_meta_ad_performance_daily`
   - `Gold.rpt_google_ad_performance_daily`
   - `Gold.rpt_unified_ad_performance`  
   Existing unmatched rows are **kept**. Matched grain keys are updated; new keys are inserted.
4. Rematerialize Delta tables (SQL-endpoint safe):
   - `Gold.vw_campaign_performance`
   - `Gold.vw_adset_performance`
   - `Gold.vw_ad_performance`
5. Write watermark/summary:
   - `Files/Silver/_control/medallion_pipeline_watermark.json`
   - `Files/Development/Gold/exports/pipeline_refresh_summary.txt`

Grain key: `platform + account_id + campaign_id + adset_id + ad_id + full_date`

## Latest verified refresh (2026-07-13)

| Table | Before | After |
|-------|--------|-------|
| `rpt_meta_ad_performance_daily` | 109 | 451 |
| `rpt_google_ad_performance_daily` | 54 | 144 |
| `rpt_unified_ad_performance` | 163 | 595 |
| `vw_campaign_performance` | 98 | 357 |
| `vw_adset_performance` | 112 | 424 |
| `vw_ad_performance` | 163 | 595 |

## Manual run

```bash
WS=718e8176-5d40-4a9c-88ff-50ac97ac49ba
PIPE=5e73aa34-45ef-4875-b640-ef4bc0e5c104

az rest --method post \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS/items/$PIPE/jobs/instances?jobType=Pipeline" \
  --body '{}'
```

## Notes

- Open **Tables → Gold → `vw_*`** (these are Delta tables, not Lakehouse Views).
- Staging copy is separate; this pipeline updates **Development Gold** schema tables.
- Disable schedule via PATCH `enabled: false` if you need to pause.
