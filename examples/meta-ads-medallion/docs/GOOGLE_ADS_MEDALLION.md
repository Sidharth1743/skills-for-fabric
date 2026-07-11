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

## Unified rebuild (Meta + Google)

Notebook: `notebooks/06_rebuild_unified_both_platforms.ipynb`

- Rebuilds `Gold.rpt_meta_ad_performance_daily` from `Files/Silver/meta_ads`
- Refreshes `Gold.rpt_google_ad_performance_daily` from `Files/Development/Silver/GoogleAds`
- Materializes `Gold.rpt_unified_ad_performance` + `Gold.vw_unified_ad_performance`
- Reporting keys are business IDs only: `account_id`, `campaign_id`, `adset_or_adgroup_id`, `ad_id` (no `account_sk` / `*_sk`)

Verify:

```sql
SELECT platform, COUNT(*) AS rows, SUM(spend_inr) AS spend
FROM Gold.vw_unified_ad_performance
GROUP BY platform;

SELECT platform, account_id, account_name, COUNT(*) AS rows
FROM Gold.vw_unified_ad_performance
GROUP BY platform, account_id, account_name;
```


## Priority insights + age/geo enrichment

Notebook: `notebooks/07_enrich_priority_age_geo.ipynb`

Bronze verification:
- Meta insights `actions[]` → leads, link_clicks, landing_page_views, post_engagement, video_views_3s
- Meta adset `targeting` → age_min/max/age_range, geo_country/regions/cities (configured targeting, not delivery breakdown)
- Google bronze has no age/geo (null in unified)

Gold grain remains: account → campaign → adset → ad → date → metrics
