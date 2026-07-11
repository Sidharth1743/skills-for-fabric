-- Unified Meta + Google Ads performance (natural keys only — no *_sk)
-- Prefer the materialized table Gold.rpt_unified_ad_performance when available.
-- Required business/metric columns:
--   tenant_id, account_id, connector_id, campaign_id, adset_id,
--   ctr, cpm, cpp, cpc, frequency, unique_clicks, spend, daily_budget_inr

CREATE OR REPLACE VIEW Gold.vw_unified_ad_performance AS
SELECT * FROM Gold.rpt_unified_ad_performance;
