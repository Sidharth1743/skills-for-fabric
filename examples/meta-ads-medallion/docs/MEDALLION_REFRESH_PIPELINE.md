# MIP Medallion Refresh Pipeline

Automatically refreshes **Silver → Gold → consumer `vw_*` Delta tables** after new Bronze batch files land.

## Fabric items (MarketingIntelligencePlatform)

| Item | Name | Id |
|------|------|----|
| Workspace | MarketingIntelligencePlatform | `718e8176-5d40-4a9c-88ff-50ac97ac49ba` |
| Lakehouse | mip_lakehouse | `981fbe98-2f01-41d8-bf2f-a85e5cd9e2a2` |
| Notebook | `mip_medallion_scheduled_refresh` | `3d829efc-d670-489f-98ad-65770ec13b74` |
| Pipeline | `MIP_Medallion_Refresh_Pipeline` | `5e73aa34-45ef-4875-b640-ef4bc0e5c104` |
| Schedule | Cron every **5 minutes** (UTC, enabled) | `e8d5ac12-6d86-422d-91d9-abe3587cf509` |

## Behavior

1. Read `*_latest_batch.txt` pointers under:
   - `Files/Development/Bronze/Meta_ads/`
   - `Files/Development/Bronze/Google_ads/`
2. Compare to watermark `Files/Silver/_control/medallion_pipeline_watermark.json`.
3. **Skip (NOOP)** when batchIds are unchanged (unless `FORCE_RUN` / `FULL_REFRESH`).
4. Prefer `*{batchId}.csv` over `*_latest.csv`.
5. Rebuild Meta + Google Silver, Gold `rpt_*` tables, unified fact, and materialize:
   - `Gold.vw_campaign_performance`
   - `Gold.vw_adset_performance`
   - `Gold.vw_ad_performance`  
   as **managed Delta tables** (SQL endpoint safe).
6. Write summary to `Files/Development/Gold/exports/pipeline_refresh_summary.txt`.

## Parameters (notebook)

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `FULL_REFRESH` | `False` | Force full rebuild regardless of watermark |
| `FORCE_RUN` | `False` | Run even if batchIds unchanged |
| `SCHEMA` | `Gold` | Target schema (Development) |

## Schedule API (reference)

```bash
WS=718e8176-5d40-4a9c-88ff-50ac97ac49ba
PIPE=5e73aa34-45ef-4875-b640-ef4bc0e5c104

az rest --method post \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS/items/$PIPE/jobs/Pipeline/schedules" \
  --body '{
    "enabled": true,
    "configuration": {
      "startDateTime": "2026-07-13T00:00:00",
      "endDateTime": "2028-07-12T23:59:00",
      "localTimeZoneId": "UTC",
      "type": "Cron",
      "interval": 5
    }
  }'
```

## Manual run

```bash
az rest --method post \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS/items/$PIPE/jobs/instances?jobType=Pipeline" \
  --body '{}'
```

## Repo sources

- Notebook export: [`../notebooks/10_medallion_scheduled_refresh.py`](../notebooks/10_medallion_scheduled_refresh.py)
- Pipeline JSON: [`../pipelines/MIP_Medallion_Refresh_Pipeline.json`](../pipelines/MIP_Medallion_Refresh_Pipeline.json)

## Notes

- Does **not** refresh `Staging_Gold` (Staging is not a second prod).
- Capacity: frequent NOOP exits keep cost down when Bronze has not changed.
- Disable schedule in Fabric UI or via PATCH `enabled: false` if you need to pause.
