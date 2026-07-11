-- Ad × day performance (Meta + Google) — native grain of unified gold
-- Column thumbnail_url replaces final_urls (Meta creative thumbnail; Google null)
CREATE OR REPLACE VIEW Gold.vw_ad_performance AS
SELECT * FROM Gold.rpt_unified_ad_performance;
