-- Unified Meta + Google Ads performance — Delta table alias (SQL Analytics Endpoint–safe)
-- Prefer Gold.rpt_unified_ad_performance as source of truth; this is a materialized copy.

CREATE OR REPLACE TABLE Gold.vw_unified_ad_performance AS
SELECT * FROM Gold.rpt_unified_ad_performance;
