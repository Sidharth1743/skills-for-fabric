-- Meta Ads Gold — star-schema SQL view
-- Use on Fabric Lakehouse SQL analytics endpoint or Warehouse after gold tables exist.
-- Budgets and spend are in INR (paise / 100 applied upstream).

CREATE OR REPLACE VIEW gold.vw_meta_ad_performance AS
SELECT
    f.fact_sk,
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    d.day_of_week,
    d.day_name,
    a.account_id,
    a.account_name,
    a.platform,
    c.campaign_id,
    c.campaign_name,
    c.objective          AS campaign_objective,
    c.status             AS campaign_status,
    c.daily_budget_inr   AS campaign_daily_budget_inr,
    c.lifetime_budget_inr AS campaign_lifetime_budget_inr,
    c.budget_remaining_inr AS campaign_budget_remaining_inr,
    s.adset_id,
    s.adset_name,
    s.status             AS adset_status,
    s.optimization_goal,
    s.daily_budget_inr   AS adset_daily_budget_inr,
    s.lifetime_budget_inr AS adset_lifetime_budget_inr,
    ad.ad_id,
    ad.ad_name,
    ad.status            AS ad_status,
    ad.effective_status  AS ad_effective_status,
    ad.creative_id,
    f.impressions,
    f.reach,
    f.frequency,
    f.clicks,
    f.unique_clicks,
    f.inline_link_clicks,
    f.spend_inr,
    f.cpc,
    f.cpm,
    f.ctr,
    f.leads,
    f.link_clicks,
    f.post_engagements,
    f.video_views,
    f.cpl_inr,
    f.source_batch_id,
    f.gold_processed_at
FROM gold.fact_ad_performance_daily AS f
INNER JOIN gold.dim_date      AS d  ON f.date_key    = d.date_key
INNER JOIN gold.dim_account   AS a  ON f.account_sk  = a.account_sk
INNER JOIN gold.dim_campaign  AS c  ON f.campaign_sk = c.campaign_sk
INNER JOIN gold.dim_adset     AS s  ON f.adset_sk    = s.adset_sk
INNER JOIN gold.dim_ad        AS ad ON f.ad_sk       = ad.ad_sk;
