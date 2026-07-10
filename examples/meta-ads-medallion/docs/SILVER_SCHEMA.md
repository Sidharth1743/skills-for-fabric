# Meta Ads — Silver Layer Schema

Bronze lands connector envelope rows (`raw_json` intact). Silver flattens, types, deduplicates, and enforces quality rules.

## Tables

| Silver table | Grain | Natural key | Source bronze |
|---|---|---|---|
| `silver.meta_campaigns` | 1 row / campaign / batch | `campaign_id` | `bronze.meta_campaigns` |
| `silver.meta_adsets` | 1 row / ad set / batch | `adset_id` | `bronze.meta_adsets` |
| `silver.meta_ads` | 1 row / ad / batch | `ad_id` | `bronze.meta_ads` |
| `silver.meta_adset_insights_daily` | 1 row / ad set / day | `adset_id + date_start` | `bronze.meta_adset_insights` |
| `silver.meta_ad_insights_daily` | 1 row / ad / day | `ad_id + date_start` | `bronze.meta_ad_insights` |
| `silver.meta_insight_actions` | 1 row / entity / day / action_type | `entity_type + entity_id + date_start + action_type` | both insight bronzes |
| `silver.meta_path_verify` | pipeline smoke check | `ts` | `bronze.meta_path_verify` |

## Quality rules (Bronze → Silver)

1. Drop rows with missing required IDs (`campaign_id` / `adset_id` / `ad_id`).
2. Deduplicate on natural key; keep latest `ingestion_time`.
3. Cast metrics to numeric; cast timestamps to UTC timestamp.
4. Convert Meta budget fields from minor units (cents) → major currency units (`*_amount`).
5. Flatten useful nested fields (`creative_id`, targeting age/geo summary); keep full nested JSON as string for audit.
6. Explode `actions[]` into `meta_insight_actions` (long format).
7. Add audit columns: `silver_processed_at`, `source_batch_id`, `connector_id`, `tenant_id`, `account_id`, `account_name`.

## Partitioning (Fabric Delta)

- Entity tables: partition by `account_id` (optional) or leave unpartitioned at this volume.
- Insights daily: partition by `date_start`.
- Actions: partition by `date_start`.
