# LinkedIn Ads Medallion (Development)

Bronze → Silver → Gold for LinkedIn Ads under **Development**.

## Fabric items

| Item | Name | Id | Folder |
|------|------|----|--------|
| Notebook | `linkedin_silver` | `6541f886-b489-4354-b193-ec123c99e12f` | Notebook / Linkedin |
| Notebook | `linkedin_gold` | `ce8c6495-d2e8-4a3d-878b-320eb0e5dc6b` | Notebook / Linkedin |
| Pipeline | `LinkedIn_Pipeline` | `ea5cc6b4-b7ee-41f8-9142-c75bdef5fca6` | Pipeline |
| Schedule | Cron every **8 hours** (480 min) UTC (enabled) | `c867392f-a49b-4256-b78a-b67a63a3cdf1` | |

## Paths

| Layer | Path |
|-------|------|
| Bronze | `Files/Development/Bronze/Linkedin/` |
| Silver | `Files/Development/Silver/Linkedin/` |
| Gold files | `Files/Development/Gold/Linkedin/` |
| Gold tables | schema **`Gold`** |

## Bronze entities (5)

`linkedin_ads`, `linkedin_campaigns`, `linkedin_campaign_groups`, `linkedin_campaign_insights`, `linkedin_creatives`

## Gold model

**Dimensions:** `dim_linkedin_campaign_group`, `dim_linkedin_campaign`, `dim_linkedin_ad`, `dim_linkedin_creative`  
**Fact:** `fact_linkedin_campaign_insights_daily` (campaign × day)  
**Consumer view:** **`Gold.vw_linkedin_ad_performance`** (aligned with `vw_meta_ad_performance` / `vw_google_ad_performance`)

```sql
SELECT TOP 100 * FROM Gold.vw_linkedin_ad_performance ORDER BY report_date DESC;
```

## Pipeline

1. `linkedin_silver` — Bronze→Silver incremental MERGE  
2. `linkedin_gold` — Silver→Gold dims/facts + `vw_linkedin_ad_performance` (depends on silver Succeeded)  
3. Schedule: every **480 minutes (8 hours)** UTC

## Source notebooks (repo)

- `examples/meta-ads-medallion/notebooks/linkedin/linkedin_silver.py`
- `examples/meta-ads-medallion/notebooks/linkedin/linkedin_gold.py`
