-- Unified Meta + Google Ads performance (natural keys only — no *_sk)
-- Prefer the materialized table Gold.rpt_unified_ad_performance when available.

CREATE OR REPLACE VIEW Gold.vw_unified_ad_performance AS
SELECT
  CAST('meta' AS STRING) AS platform,
  full_date, year, month, month_name, day_name,
  account_id, account_name,
  campaign_id, campaign_name, campaign_status,
  campaign_objective AS campaign_channel_or_objective,
  campaign_daily_budget_inr,
  adset_id AS adset_or_adgroup_id,
  adset_name AS adset_or_adgroup_name,
  adset_status AS adset_or_adgroup_status,
  ad_id, ad_name,
  CAST(NULL AS STRING) AS ad_type,
  ad_status,
  CAST(NULL AS STRING) AS final_urls,
  impressions, clicks, ctr, spend_inr, cpc,
  CAST(NULL AS DOUBLE) AS conversions,
  CAST(NULL AS DOUBLE) AS conversions_value,
  CAST(NULL AS DOUBLE) AS cost_per_conversion,
  CAST(NULL AS DOUBLE) AS roas,
  CAST(NULL AS DOUBLE) AS engagements,
  CAST(NULL AS DOUBLE) AS video_views,
  gold_processed_at
FROM Gold.rpt_meta_ad_performance_daily
UNION ALL
SELECT
  platform,
  full_date, year, month, month_name, day_name,
  account_id, account_name,
  campaign_id, campaign_name, campaign_status,
  campaign_channel_or_objective,
  campaign_daily_budget_inr,
  adset_or_adgroup_id,
  adset_or_adgroup_name,
  adset_or_adgroup_status,
  ad_id, ad_name,
  ad_type,
  ad_status,
  final_urls,
  impressions, clicks, ctr, spend_inr, cpc,
  conversions, conversions_value, cost_per_conversion, roas,
  engagements, video_views,
  gold_processed_at
FROM Gold.rpt_google_ad_performance_daily;
