# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "981fbe98-2f01-41d8-bf2f-a85e5cd9e2a2",
# META       "default_lakehouse_name": "mip_lakehouse",
# META       "default_lakehouse_workspace_id": "718e8176-5d40-4a9c-88ff-50ac97ac49ba"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # MIP Medallion Scheduled Refresh
# Runs every N minutes via Fabric pipeline schedule.
# - Skips when no new Bronze batchIds (unless FORCE_RUN / FULL_REFRESH)
# - Prefers `*{batchId}.csv` over `*_latest.csv`
# - Rebuilds Meta + Google Silver/Gold + unified + vw_* Delta tables


# PARAMETERS CELL ********************

FULL_REFRESH = False
FORCE_RUN = False
INCREMENTAL_LOOKBACK_DAYS = 2
INCREMENTAL_MAX_DAYS = 14
SCHEMA = 'Gold'
CONTROL_PATH = 'Files/Silver/_control/medallion_pipeline_watermark.json'
SUMMARY_PATH = 'Files/Development/Gold/exports/pipeline_refresh_summary.txt'


# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from datetime import datetime, timezone, timedelta
import json, re, uuid

META_BRONZE = 'Files/Development/Bronze/Meta_ads'
GOOGLE_BRONZE = 'Files/Development/Bronze/Google_ads'
META_SILVER = 'Files/Development/Silver/meta_ads'
GOOGLE_SILVER = 'Files/Development/Silver/GoogleAds'

spark.sql(f'CREATE SCHEMA IF NOT EXISTS {SCHEMA}')

def _norm(col):
    return F.lower(F.trim(F.coalesce(F.col(col).cast('string'), F.lit(''))))

def _to_double(c):
    return F.regexp_replace(F.col(c).cast('string'), r'[^0-9.\-]', '').cast('double')

def _to_long(c):
    return F.regexp_replace(F.col(c).cast('string'), r'[^0-9\-]', '').cast('long')

def _to_date(c):
    s = F.trim(F.col(c).cast('string'))
    return F.coalesce(F.to_date(s), F.to_date(s, 'M/d/yyyy'), F.to_date(s, 'yyyy/MM/dd'), F.to_date(F.to_timestamp(s)))

def list_csv(folder):
    try:
        return [x.path for x in mssparkutils.fs.ls(folder) if x.path.lower().endswith('.csv')]
    except Exception:
        return []

def prefer_batch(paths, prefix):
    batch, latest = [], []
    for p in paths:
        name = p.rsplit('/',1)[-1]
        if not name.startswith(prefix):
            continue
        if name.endswith('_latest.csv'):
            latest.append(p)
        elif re.match(rf'^{re.escape(prefix)}[0-9a-fA-F-]{{8,}}\.csv$', name):
            batch.append(p)
    return batch if batch else latest

def read_csvs(paths):
    if not paths:
        return None
    dfs = [spark.read.option('header', True).option('inferSchema', False).option('multiLine', True).csv(p) for p in paths]
    df = dfs[0]
    for d in dfs[1:]:
        df = df.unionByName(d, allowMissingColumns=True)
    return df

def read_batch_ptr(folder, name):
    path = f'{folder}/{name}'
    try:
        return mssparkutils.fs.head(path, 256).strip()
    except Exception:
        return None

def read_watermark():
    try:
        return json.loads(mssparkutils.fs.head(CONTROL_PATH, 10000))
    except Exception:
        return {}

def write_json(path, obj):
    mssparkutils.fs.put(path, json.dumps(obj, indent=2), True)

current = {
  'meta_campaigns': read_batch_ptr(META_BRONZE, 'meta_campaigns_latest_batch.txt'),
  'meta_adsets': read_batch_ptr(META_BRONZE, 'meta_adsets_latest_batch.txt'),
  'meta_ads': read_batch_ptr(META_BRONZE, 'meta_ads_latest_batch.txt'),
  'meta_insights': read_batch_ptr(META_BRONZE, 'meta_insights_latest_batch.txt'),
  'google_campaign': read_batch_ptr(GOOGLE_BRONZE, 'google_campaign_latest_batch.txt'),
  'google_adgroup': read_batch_ptr(GOOGLE_BRONZE, 'google_adgroup_latest_batch.txt'),
  'google_ad': read_batch_ptr(GOOGLE_BRONZE, 'google_ad_latest_batch.txt'),
  'google_ad_performance': read_batch_ptr(GOOGLE_BRONZE, 'google_ad_performance_latest_batch.txt'),
}
wm = read_watermark()
prev = wm.get('batches', {})
changed = {k:v for k,v in current.items() if v and prev.get(k) != v}
print('current batches:', json.dumps(current, indent=2))
print('changed:', list(changed.keys()) or '(none)')

should_run = bool(FORCE_RUN or FULL_REFRESH or changed)
if not should_run:
    msg = f'NOOP {datetime.now(timezone.utc).isoformat()} — no new bronze batchIds'
    print(msg)
    mssparkutils.fs.put(SUMMARY_PATH, msg + '\n', True)
    mssparkutils.notebook.exit(json.dumps({'status':'skipped','reason':'no_new_bronze'}))
print('Proceeding with medallion refresh...')


# CELL ********************

# -------- META SILVER + GOLD --------
meta_files = list_csv(META_BRONZE)
camp_p = prefer_batch(meta_files, 'meta_campaigns_')
adset_p = prefer_batch(meta_files, 'meta_adsets_')
ads_p = prefer_batch(meta_files, 'meta_ads_')
ins_p = prefer_batch(meta_files, 'meta_insights_')
print('meta paths', len(camp_p), len(adset_p), len(ads_p), len(ins_p))

camp = read_csvs(camp_p).withColumn('campaign_id', F.coalesce(F.col('campaignId'), F.col('id')).cast('string')) \
    .withColumn('account_id', F.col('accountId').cast('string')) \
    .withColumn('campaign_name', F.col('name').cast('string')) \
    .withColumn('campaign_status', F.col('status').cast('string')) \
    .withColumn('objective', F.col('objective').cast('string')) \
    .withColumn('buying_type', F.col('buyingType').cast('string')) \
    .withColumn('daily_budget', _to_double('dailyBudget')) \
    .withColumn('lifetime_budget', _to_double('lifetimeBudget')) \
    .withColumn('start_time', F.to_timestamp('startTime')) \
    .withColumn('stop_time', F.to_timestamp('stopTime')) \
    .withColumn('created_time', F.to_timestamp('createdTime')) \
    .withColumn('updated_time', F.to_timestamp('updatedTime')) \
    .withColumn('ingestion_ts', F.current_timestamp()) \
    .withColumn('source_system', F.lit('meta_ads')) \
    .withColumn('_rk', F.row_number().over(Window.partitionBy('campaign_id').orderBy(F.col('updated_time').desc_nulls_last(), F.col('ingestion_ts').desc())))
camp = camp.filter(F.col('_rk')==1).drop('_rk').select('campaign_id','account_id','campaign_name','campaign_status','objective','buying_type','daily_budget','lifetime_budget','start_time','stop_time','created_time','updated_time','ingestion_ts','source_system')
camp.write.format('delta').mode('overwrite').option('overwriteSchema','true').save(f'{META_SILVER}/campaigns')

adset = read_csvs(adset_p).withColumn('adset_id', F.coalesce(F.col('adsetId'), F.col('id')).cast('string')) \
    .withColumn('campaign_id', F.col('campaignId').cast('string')) \
    .withColumn('account_id', F.col('accountId').cast('string')) \
    .withColumn('adset_name', F.col('name').cast('string')) \
    .withColumn('adset_status', F.col('status').cast('string')) \
    .withColumn('daily_budget', _to_double('dailyBudget')) \
    .withColumn('lifetime_budget', _to_double('lifetimeBudget')) \
    .withColumn('bid_strategy', F.col('bidStrategy').cast('string')) \
    .withColumn('optimization_goal', F.col('optimizationGoal').cast('string')) \
    .withColumn('billing_event', F.col('billingEvent').cast('string')) \
    .withColumn('start_time', F.to_timestamp('startTime')) \
    .withColumn('end_time', F.to_timestamp('endTime')) \
    .withColumn('created_time', F.to_timestamp('createdTime')) \
    .withColumn('updated_time', F.to_timestamp('updatedTime')) \
    .withColumn('ingestion_ts', F.current_timestamp()) \
    .withColumn('source_system', F.lit('meta_ads')) \
    .withColumn('_rk', F.row_number().over(Window.partitionBy('adset_id').orderBy(F.col('updated_time').desc_nulls_last(), F.col('ingestion_ts').desc())))
adset = adset.filter(F.col('_rk')==1).drop('_rk').select('adset_id','campaign_id','account_id','adset_name','adset_status','daily_budget','lifetime_budget','bid_strategy','optimization_goal','billing_event','start_time','end_time','created_time','updated_time','ingestion_ts','source_system')
adset.write.format('delta').mode('overwrite').option('overwriteSchema','true').save(f'{META_SILVER}/adsets')

ads = read_csvs(ads_p).withColumn('ad_id', F.coalesce(F.col('adId'), F.col('id')).cast('string')) \
    .withColumn('adset_id', F.col('adsetId').cast('string')) \
    .withColumn('campaign_id', F.col('campaignId').cast('string')) \
    .withColumn('account_id', F.col('accountId').cast('string')) \
    .withColumn('ad_name', F.col('name').cast('string')) \
    .withColumn('ad_status', F.col('status').cast('string')) \
    .withColumn('creative_id', F.col('creativeId').cast('string')) \
    .withColumn('created_time', F.to_timestamp('createdTime')) \
    .withColumn('updated_time', F.to_timestamp('updatedTime')) \
    .withColumn('ingestion_ts', F.current_timestamp()) \
    .withColumn('source_system', F.lit('meta_ads')) \
    .withColumn('_rk', F.row_number().over(Window.partitionBy('ad_id').orderBy(F.col('updated_time').desc_nulls_last(), F.col('ingestion_ts').desc())))
ads = ads.filter(F.col('_rk')==1).drop('_rk').select('ad_id','adset_id','campaign_id','account_id','ad_name','ad_status','creative_id','created_time','updated_time','ingestion_ts','source_system')
ads.write.format('delta').mode('overwrite').option('overwriteSchema','true').save(f'{META_SILVER}/ads')

ins = read_csvs(ins_p)
ins = ins.withColumn('account_id', F.col('accountId').cast('string')) \
    .withColumn('campaign_id', F.col('campaignId').cast('string')) \
    .withColumn('adset_id', F.col('adsetId').cast('string')) \
    .withColumn('ad_id', F.col('adId').cast('string')) \
    .withColumn('date', _to_date('dateStart')) \
    .withColumn('impressions', _to_long('impressions')) \
    .withColumn('clicks', _to_long('clicks')) \
    .withColumn('spend', _to_double('spend')) \
    .withColumn('reach', _to_long('reach')) \
    .withColumn('frequency', _to_double('frequency')) \
    .withColumn('cpc', _to_double('cpc')) \
    .withColumn('cpm', _to_double('cpm')) \
    .withColumn('ctr', _to_double('ctr')) \
    .withColumn('unique_clicks', _to_long('uniqueClicks')) \
    .withColumn('inline_link_clicks', _to_long('inlineLinkClicks')) \
    .withColumn('ingestion_ts', F.current_timestamp()) \
    .withColumn('source_system', F.lit('meta_ads')) \
    .withColumn('_rk', F.row_number().over(Window.partitionBy('account_id','campaign_id','adset_id','ad_id','date').orderBy(F.col('ingestion_ts').desc())))
ins = ins.filter(F.col('_rk')==1).drop('_rk').filter(F.col('date').isNotNull()).select('account_id','campaign_id','adset_id','ad_id','date','impressions','clicks','spend','reach','frequency','cpc','cpm','ctr','unique_clicks','inline_link_clicks','ingestion_ts','source_system')
ins.write.format('delta').mode('overwrite').option('overwriteSchema','true').partitionBy('date').save(f'{META_SILVER}/ad_insights_daily')
print('meta silver', camp.count(), adset.count(), ads.count(), ins.count())

meta_gold = ins.alias('i') \
  .join(camp.alias('c'), 'campaign_id', 'left') \
  .join(adset.alias('s'), 'adset_id', 'left') \
  .join(ads.alias('a'), 'ad_id', 'left') \
  .select(
    F.coalesce(F.col('i.account_id'), F.col('c.account_id'), F.col('s.account_id'), F.col('a.account_id')).alias('account_id'),
    F.col('i.campaign_id'), F.col('c.campaign_name'), F.col('c.campaign_status'), F.col('c.objective'),
    F.col('i.adset_id'), F.col('s.adset_name'), F.col('s.adset_status'),
    F.col('i.ad_id'), F.col('a.ad_name'), F.col('a.ad_status'), F.col('a.creative_id'),
    F.col('i.date'), F.col('i.impressions'), F.col('i.clicks'), F.col('i.spend'), F.col('i.reach'), F.col('i.frequency'),
    F.col('i.cpc'), F.col('i.cpm'), F.col('i.ctr'), F.col('i.unique_clicks'), F.col('i.inline_link_clicks'),
    F.current_timestamp().alias('gold_refresh_ts'), F.lit('meta_ads').alias('source_system')
  )
meta_gold.write.format('delta').mode('overwrite').option('overwriteSchema','true').partitionBy('date').saveAsTable(f'{SCHEMA}.rpt_meta_ad_performance_daily')
print('meta gold', meta_gold.count())


# CELL ********************

# -------- GOOGLE SILVER + GOLD --------
g_files = list_csv(GOOGLE_BRONZE)
gc_p = prefer_batch(g_files, 'google_campaign_')
ga_p = prefer_batch(g_files, 'google_adgroup_')
gad_p = prefer_batch(g_files, 'google_ad_')
# exclude performance from ad entity prefer by filtering exact prefixes carefully
gad_p = [p for p in gad_p if 'google_ad_performance_' not in p.rsplit('/',1)[-1] and not p.rsplit('/',1)[-1].startswith('google_adgroup_')]
gp_p = prefer_batch(g_files, 'google_ad_performance_')
print('google paths', len(gc_p), len(ga_p), len(gad_p), len(gp_p))

gc = read_csvs(gc_p).withColumn('campaign_id', F.coalesce(F.col('campaignId'), F.col('id')).cast('string')) \
    .withColumn('account_id', F.coalesce(F.col('customerId'), F.col('accountId')).cast('string')) \
    .withColumn('campaign_name', F.col('name').cast('string')) \
    .withColumn('campaign_status', F.col('status').cast('string')) \
    .withColumn('channel_type', F.col('advertisingChannelType').cast('string')) \
    .withColumn('bidding_strategy_type', F.col('biddingStrategyType').cast('string')) \
    .withColumn('ingestion_ts', F.current_timestamp()) \
    .withColumn('source_system', F.lit('google_ads')) \
    .withColumn('_rk', F.row_number().over(Window.partitionBy('campaign_id').orderBy(F.col('ingestion_ts').desc())))
gc = gc.filter(F.col('_rk')==1).drop('_rk')
gc_out = gc.select('campaign_id','account_id','campaign_name','campaign_status','channel_type','bidding_strategy_type','ingestion_ts','source_system')
gc_out.write.format('delta').mode('overwrite').option('overwriteSchema','true').save(f'{GOOGLE_SILVER}/campaigns')

ga = read_csvs(ga_p).withColumn('adset_id', F.coalesce(F.col('adGroupId'), F.col('id')).cast('string')) \
    .withColumn('campaign_id', F.col('campaignId').cast('string')) \
    .withColumn('account_id', F.coalesce(F.col('customerId'), F.col('accountId')).cast('string')) \
    .withColumn('adset_name', F.col('name').cast('string')) \
    .withColumn('adset_status', F.col('status').cast('string')) \
    .withColumn('ingestion_ts', F.current_timestamp()) \
    .withColumn('source_system', F.lit('google_ads')) \
    .withColumn('_rk', F.row_number().over(Window.partitionBy('adset_id').orderBy(F.col('ingestion_ts').desc())))
ga = ga.filter(F.col('_rk')==1).drop('_rk').select('adset_id','campaign_id','account_id','adset_name','adset_status','ingestion_ts','source_system')
ga.write.format('delta').mode('overwrite').option('overwriteSchema','true').save(f'{GOOGLE_SILVER}/adgroups')

gad = read_csvs(gad_p).withColumn('ad_id', F.coalesce(F.col('adId'), F.col('id')).cast('string')) \
    .withColumn('adset_id', F.col('adGroupId').cast('string')) \
    .withColumn('campaign_id', F.col('campaignId').cast('string')) \
    .withColumn('account_id', F.coalesce(F.col('customerId'), F.col('accountId')).cast('string')) \
    .withColumn('ad_name', F.coalesce(F.col('name'), F.col('adName')).cast('string')) \
    .withColumn('ad_status', F.col('status').cast('string')) \
    .withColumn('ingestion_ts', F.current_timestamp()) \
    .withColumn('source_system', F.lit('google_ads')) \
    .withColumn('_rk', F.row_number().over(Window.partitionBy('ad_id').orderBy(F.col('ingestion_ts').desc())))
gad = gad.filter(F.col('_rk')==1).drop('_rk').select('ad_id','adset_id','campaign_id','account_id','ad_name','ad_status','ingestion_ts','source_system')
gad.write.format('delta').mode('overwrite').option('overwriteSchema','true').save(f'{GOOGLE_SILVER}/ads')

gp = read_csvs(gp_p)
# flexible date/metric columns
cols = {c.lower(): c for c in gp.columns}
def pick(*names):
    for n in names:
        if n.lower() in cols: return cols[n.lower()]
    return names[0]
date_c = pick('date','segments.date','Date')
imp_c = pick('impressions','metrics.impressions')
clk_c = pick('clicks','metrics.clicks')
cost_c = pick('costMicros','metrics.cost_micros','cost','spend')
gp2 = gp.withColumn('account_id', F.coalesce(F.col(pick('customerId','accountId')), F.lit(None)).cast('string')) \
    .withColumn('campaign_id', F.col(pick('campaignId')).cast('string')) \
    .withColumn('adset_id', F.col(pick('adGroupId')).cast('string')) \
    .withColumn('ad_id', F.col(pick('adId')).cast('string')) \
    .withColumn('date', _to_date(date_c)) \
    .withColumn('impressions', _to_long(imp_c)) \
    .withColumn('clicks', _to_long(clk_c)) \
    .withColumn('cost_micros', _to_double(cost_c)) \
    .withColumn('spend', F.when(F.col('cost_micros') > 1000, F.col('cost_micros')/F.lit(1_000_000.0)).otherwise(F.col('cost_micros'))) \
    .withColumn('ingestion_ts', F.current_timestamp()) \
    .withColumn('source_system', F.lit('google_ads')) \
    .withColumn('_rk', F.row_number().over(Window.partitionBy('account_id','campaign_id','adset_id','ad_id','date').orderBy(F.col('ingestion_ts').desc())))
gp2 = gp2.filter(F.col('_rk')==1).drop('_rk').filter(F.col('date').isNotNull()).select('account_id','campaign_id','adset_id','ad_id','date','impressions','clicks','spend','ingestion_ts','source_system')
gp2.write.format('delta').mode('overwrite').option('overwriteSchema','true').partitionBy('date').save(f'{GOOGLE_SILVER}/ad_performance_daily')
print('google silver', gc_out.count(), ga.count(), gad.count(), gp2.count())

gg = gp2.alias('i') \
  .join(gc_out.alias('c'), 'campaign_id', 'left') \
  .join(ga.alias('s'), 'adset_id', 'left') \
  .join(gad.alias('a'), 'ad_id', 'left') \
  .select(
    F.coalesce(F.col('i.account_id'), F.col('c.account_id'), F.col('s.account_id'), F.col('a.account_id')).alias('account_id'),
    F.col('i.campaign_id'), F.col('c.campaign_name'), F.col('c.campaign_status'), F.col('c.channel_type'),
    F.col('i.adset_id'), F.col('s.adset_name'), F.col('s.adset_status'),
    F.col('i.ad_id'), F.col('a.ad_name'), F.col('a.ad_status'),
    F.col('i.date'), F.col('i.impressions'), F.col('i.clicks'), F.col('i.spend'),
    F.current_timestamp().alias('gold_refresh_ts'), F.lit('google_ads').alias('source_system')
  )
gg.write.format('delta').mode('overwrite').option('overwriteSchema','true').partitionBy('date').saveAsTable(f'{SCHEMA}.rpt_google_ad_performance_daily')
print('google gold', gg.count())


# CELL ********************

# -------- UNIFIED + VIEWS --------
meta_u = spark.table(f'{SCHEMA}.rpt_meta_ad_performance_daily').select(
  F.lit('meta').alias('platform'), 'account_id','campaign_id','campaign_name','campaign_status',
  'adset_id','adset_name','adset_status','ad_id','ad_name','ad_status','date',
  'impressions','clicks','spend', F.col('reach').cast('long').alias('reach'), F.col('inline_link_clicks').cast('long').alias('link_clicks'),
  'gold_refresh_ts'
)
google_u = spark.table(f'{SCHEMA}.rpt_google_ad_performance_daily').select(
  F.lit('google').alias('platform'), 'account_id','campaign_id','campaign_name','campaign_status',
  'adset_id','adset_name','adset_status','ad_id','ad_name','ad_status','date',
  'impressions','clicks','spend', F.lit(None).cast('long').alias('reach'), F.lit(None).cast('long').alias('link_clicks'),
  'gold_refresh_ts'
)
unified = meta_u.unionByName(google_u, allowMissingColumns=True)
unified.write.format('delta').mode('overwrite').option('overwriteSchema','true').partitionBy('date').saveAsTable(f'{SCHEMA}.rpt_unified_ad_performance')
u = spark.table(f'{SCHEMA}.rpt_unified_ad_performance')
print('unified', u.count())

def mat(name, df):
    spark.sql(f'DROP TABLE IF EXISTS {SCHEMA}.{name}')
    try: spark.sql(f'DROP VIEW IF EXISTS {SCHEMA}.{name}')
    except Exception: pass
    df.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(f'{SCHEMA}.{name}')
    print(name, df.count())

mat('vw_campaign_performance', u.groupBy('platform','account_id','campaign_id','campaign_name','campaign_status','date').agg(
    F.sum('impressions').alias('impressions'), F.sum('clicks').alias('clicks'), F.sum('spend').alias('spend'),
    F.sum('reach').alias('reach'), F.sum('link_clicks').alias('link_clicks'), F.countDistinct('ad_id').alias('ads_count'), F.countDistinct('adset_id').alias('adsets_count')
).withColumn('ctr', F.when(F.col('impressions')>0, F.col('clicks')/F.col('impressions')).otherwise(F.lit(None))) \
 .withColumn('cpc', F.when(F.col('clicks')>0, F.col('spend')/F.col('clicks')).otherwise(F.lit(None))) \
 .withColumn('cpm', F.when(F.col('impressions')>0, F.col('spend')*1000/F.col('impressions')).otherwise(F.lit(None))) \
 .withColumn('tenant_id', F.lit(None).cast('string')).withColumn('connector_id', F.lit(None).cast('string')).withColumn('customer_id', F.lit(None).cast('string')) \
 .withColumn('gold_refresh_ts', F.current_timestamp()))

mat('vw_adset_performance', u.groupBy('platform','account_id','campaign_id','campaign_name','adset_id','adset_name','adset_status','date').agg(
    F.sum('impressions').alias('impressions'), F.sum('clicks').alias('clicks'), F.sum('spend').alias('spend'),
    F.sum('reach').alias('reach'), F.sum('link_clicks').alias('link_clicks'), F.countDistinct('ad_id').alias('ads_count')
).withColumn('ctr', F.when(F.col('impressions')>0, F.col('clicks')/F.col('impressions')).otherwise(F.lit(None))) \
 .withColumn('cpc', F.when(F.col('clicks')>0, F.col('spend')/F.col('clicks')).otherwise(F.lit(None))) \
 .withColumn('cpm', F.when(F.col('impressions')>0, F.col('spend')*1000/F.col('impressions')).otherwise(F.lit(None))) \
 .withColumn('tenant_id', F.lit(None).cast('string')).withColumn('connector_id', F.lit(None).cast('string')).withColumn('customer_id', F.lit(None).cast('string')) \
 .withColumn('gold_refresh_ts', F.current_timestamp()))

mat('vw_ad_performance', u.select(
  'platform','account_id','campaign_id','campaign_name','adset_id','adset_name','ad_id','ad_name','ad_status','date',
  'impressions','clicks','spend','reach','link_clicks',
  F.when(F.col('impressions')>0, F.col('clicks')/F.col('impressions')).otherwise(F.lit(None)).alias('ctr'),
  F.when(F.col('clicks')>0, F.col('spend')/F.col('clicks')).otherwise(F.lit(None)).alias('cpc'),
  F.when(F.col('impressions')>0, F.col('spend')*1000/F.col('impressions')).otherwise(F.lit(None)).alias('cpm'),
  F.lit(None).cast('string').alias('tenant_id'), F.lit(None).cast('string').alias('connector_id'), F.lit(None).cast('string').alias('customer_id'),
  F.current_timestamp().alias('gold_refresh_ts')
))

wm_out = {
  'updated_at_utc': datetime.now(timezone.utc).isoformat(),
  'batches': current,
  'changed_batches': list(changed.keys()),
  'full_refresh': bool(FULL_REFRESH),
  'counts': {
    'unified': u.count(),
    'vw_campaign': spark.table(f'{SCHEMA}.vw_campaign_performance').count(),
    'vw_adset': spark.table(f'{SCHEMA}.vw_adset_performance').count(),
    'vw_ad': spark.table(f'{SCHEMA}.vw_ad_performance').count(),
  }
}
write_json(CONTROL_PATH, wm_out)
mssparkutils.fs.put(SUMMARY_PATH, json.dumps(wm_out, indent=2), True)
print('DONE', json.dumps(wm_out))
mssparkutils.notebook.exit(json.dumps({'status':'success', **wm_out['counts']}))

