-- Ad × day performance (Meta + Google) — native grain of unified gold
-- Column thumbnail_url replaces final_urls (Meta creative thumbnail; Google null)
-- customer_id is not present on the gold source table yet; exposed as null for consumers.
CREATE OR REPLACE VIEW Gold.vw_ad_performance AS
SELECT
  src.*,
  CAST(NULL AS STRING) AS customer_id
FROM Gold.rpt_unified_ad_performance AS src;
