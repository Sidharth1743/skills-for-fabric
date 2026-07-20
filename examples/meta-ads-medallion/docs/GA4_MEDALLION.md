# GA4 Medallion (Development)

Bronze → Silver → Gold for Google Analytics under **Development**.

## Fabric items

| Item | Name | Id | Folder |
|------|------|----|--------|
| Notebook | `ga4_silver` | `c65a6f42-eb74-4a87-9701-96390aa144f4` | Notebook / Google Analytics |
| Notebook | `ga4_gold` | `30b30bfe-1e90-4b96-a428-6cac5c116793` | Notebook / Google Analytics |
| Pipeline | `Google Analytics_Pipeline` | `9edd99b2-6f76-48fc-91c4-4cf8529db0bf` | Pipeline |
| Schedule | Cron every **30 minutes** UTC (enabled) | (embedded `.schedules`) | |

## Paths

| Layer | Path |
|-------|------|
| Bronze | `Files/Development/Bronze/GoogleAnalytics_Ads/` |
| Silver | `Files/Development/Silver/GoogleAnalytics/` |
| Gold files | `Files/Development/Gold/GoogleAnalytics/` |
| Gold tables | schema **`Gold`** |

## Coverage (all 24 Bronze/Silver entities → Gold)

### Dimensions (config + reference)

| Silver source | Gold table |
|---------------|------------|
| `ga4_accounts` | `dim_ga4_account` |
| `ga4_account_summaries` | `dim_ga4_account_summary` |
| `ga4_properties` | `dim_ga4_property` |
| `ga4_data_streams` | `dim_ga4_data_stream` |
| `ga4_conversion_events` | `dim_ga4_conversion_event` |
| `ga4_key_event_definitions` | `dim_ga4_key_event_definition` |
| `ga4_custom_dimensions` | `dim_ga4_custom_dimension` |
| `ga4_attribution_settings` | `dim_ga4_attribution_settings` |
| campaigns + traffic | `dim_ga4_channel` |
| traffic / key_events / hourly | `dim_ga4_date` |
| `ga4_pages` | `dim_ga4_page` |

### Facts (metrics)

| Silver source | Gold table | In `vw_google_analytics_performance` (`insight_type`) |
|---------------|------------|--------------------------------------------------------|
| `ga4_traffic` | `fact_ga4_traffic_daily` | `traffic` |
| `ga4_campaigns` | `fact_ga4_campaign_performance` | `campaign` |
| `ga4_acquisition` | `fact_ga4_acquisition` | `acquisition` |
| `ga4_key_events` | `fact_ga4_key_events_daily` | `key_event` |
| `ga4_events` | `fact_ga4_events_daily` | `event` (extract-window grain; Bronze has no daily date) |
| `ga4_pages` | `fact_ga4_page_performance` | `page` |
| `ga4_landing_pages` | `fact_ga4_landing_pages` | `landing_page` |
| `ga4_page_device` | `fact_ga4_page_device` | `page_device` |
| `ga4_page_geography` | `fact_ga4_page_geography` | `page_geography` |
| `ga4_page_source` | `fact_ga4_page_source` | `page_source` |
| `ga4_geography` | `fact_ga4_geography` | `geography` |
| `ga4_demographics` | `fact_ga4_demographics` | `demographics` |
| `ga4_devices` | `fact_ga4_devices` | `device` |
| `ga4_technology` | `fact_ga4_technology` | `technology` |
| `ga4_hourly` | `fact_ga4_hourly` | `hourly` |
| `ga4_realtime` | `fact_ga4_realtime` | `realtime` |

### Intentionally special handling

| Dataset | Handling | Reason |
|---------|----------|--------|
| **Realtime** | Gold snapshot table (`fact_ga4_realtime`), also in unified vw as `insight_type='realtime'` | Not a historical grain — overwritten each Gold run with latest Bronze/Silver snapshot. Suitable for ops/AI monitoring, not trend reporting. |

No Bronze/Silver GA4 entity is left without a Gold dim or fact.

## Primary consumer view

**`Gold.vw_google_analytics_performance`** (alias `vw_ga4_unified`, backing `rpt_ga4_unified`)

Null handling for consumers:
- **Metrics** (`sessions`, `total_users`, `active_users`, `conversions`, rates, revenue, etc.) are filled with **0** when not present.
- **`report_date` / `full_date` / year-month** use daily date when available, else extraction window start (realtime → current date).
- **`page_title`** is enriched from `dim_ga4_page` when the source report omits it.
- **`channel_group`** is enriched from `dim_ga4_channel` when missing.
- Dimension columns that do not apply to an `insight_type` (e.g. `age_bracket` on traffic rows) remain null by design — filter with `insight_type`.

```sql
SELECT TOP 100 * FROM Gold.vw_google_analytics_performance;
SELECT * FROM Gold.vw_google_analytics_performance WHERE insight_type = 'landing_page';
SELECT * FROM Gold.vw_google_analytics_performance WHERE insight_type = 'demographics';
```

## Source notebooks (repo)

- `examples/meta-ads-medallion/notebooks/ga4/ga4_silver.py`
- `examples/meta-ads-medallion/notebooks/ga4/ga4_notebook.py`
