-- Campaign × day performance (Meta + Google)
-- Rolled up from Gold.rpt_unified_ad_performance (ad × day source of truth)
CREATE OR REPLACE VIEW Gold.vw_campaign_performance AS
SELECT
  platform, full_date, year, month, month_name, day_name,
  MAX(tenant_id) AS tenant_id,
  MAX(connector_id) AS connector_id,
  account_id,
  CAST(NULL AS STRING) AS customer_id,
  MAX(account_name) AS account_name,
  campaign_id, MAX(campaign_name) AS campaign_name,
  MAX(campaign_status) AS campaign_status,
  MAX(campaign_channel_or_objective) AS campaign_channel_or_objective,
  MAX(daily_budget_inr) AS daily_budget_inr,
  SUM(impressions) AS impressions, SUM(reach) AS reach,
  SUM(clicks) AS clicks, SUM(spend) AS spend, SUM(leads) AS leads,
  CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE NULL END AS cpc,
  CASE WHEN SUM(impressions) > 0 THEN (SUM(spend) / SUM(impressions)) * 1000 ELSE NULL END AS cpm,
  CASE WHEN SUM(leads) > 0 THEN SUM(spend) / SUM(leads) ELSE NULL END AS cost_per_lead,
  COUNT(DISTINCT adset_id) AS adset_count,
  COUNT(DISTINCT ad_id) AS ad_count
FROM Gold.rpt_unified_ad_performance
GROUP BY platform, full_date, year, month, month_name, day_name, account_id, campaign_id;
