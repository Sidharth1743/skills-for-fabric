# Google Ads — Silver / Gold (Development)

## Paths in `mip_lakehouse`

| Layer | OneLake path |
|-------|----------------|
| Bronze | `Files/Development/Bronze/Google_ads/` |
| Silver | `Files/Development/Silver/GoogleAds/` |
| Gold | `Files/Development/Gold/GoogleAds/` |

## Notebooks

| Notebook | Purpose |
|----------|---------|
| `04_google_ads_silver.ipynb` | Bronze → Silver (entities + daily performance) |
| `05_google_ads_gold.ipynb` | Silver → Gold star schema + reporting + unified view |

## Silver tables

- `silver_google_campaigns`
- `silver_google_adgroups`
- `silver_google_ads`
- `silver_google_campaign_performance`
- `silver_google_ad_group_performance`
- `silver_google_ad_performance`

Money: prefer `budget_amount` / `cost`; else `*_micros / 1_000_000`.

## Gold star schema

- Dims: `dim_google_account`, `dim_google_campaign`, `dim_google_adgroup`, `dim_google_ad`, `dim_google_date`
- Fact: `fact_google_ad_performance_daily` (ad × day)
- Reporting: `rpt_google_ad_performance_daily`
- View: `Gold.vw_google_ad_performance`
- Unified: `Gold.vw_unified_ad_performance` (Meta ∪ Google Ads)

## Entity relationship

```text
dim_google_account
  └── dim_google_campaign
        └── dim_google_adgroup
              └── dim_google_ad
                    └── fact_google_ad_performance_daily ← dim_google_date
```

## Example queries

```sql
SELECT TOP 100 * FROM Gold.vw_google_ad_performance;

SELECT TOP 100 *
FROM Gold.vw_unified_ad_performance
WHERE platform = 'google_ads';
```
