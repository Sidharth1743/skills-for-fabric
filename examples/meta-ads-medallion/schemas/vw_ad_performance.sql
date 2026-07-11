-- Gold ad × day performance — Delta table (SQL Analytics Endpoint–safe)
-- Materialized from Gold.rpt_unified_ad_performance (native grain + customer_id).

CREATE OR REPLACE TABLE Gold.vw_ad_performance AS
SELECT
  src.*,
  CAST(NULL AS STRING) AS customer_id
FROM Gold.rpt_unified_ad_performance AS src;
