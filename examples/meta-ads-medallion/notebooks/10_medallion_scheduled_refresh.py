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

# CELL ********************

from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from notebookutils import mssparkutils
from datetime import datetime, timezone
import json, re

@F.udf(T.StructType([
    T.StructField("age_min", T.IntegerType()),
    T.StructField("age_max", T.IntegerType()),
    T.StructField("age_range", T.StringType()),
    T.StructField("geo_country", T.StringType()),
    T.StructField("geo_regions", T.StringType()),
    T.StructField("geo_cities", T.StringType()),
]))
def parse_targeting(raw):
    age_min = age_max = None
    age_range = geo_country = geo_regions = geo_cities = None
    try:
        obj = json.loads(raw) if raw else {}
        t = obj.get("targeting") or {}
        age_min = t.get("age_min"); age_max = t.get("age_max")
        ar = t.get("age_range")
        if isinstance(ar, list) and len(ar) >= 2:
            age_range = f"{ar[0]}-{ar[1]}"
        elif age_min is not None and age_max is not None:
            age_range = f"{age_min}-{age_max}"
        geo = t.get("geo_locations") or {}
        countries = list(geo.get("countries") or []); regions=[]; city_names=[]
        def uniq(seq):
            seen=set(); out=[]
            for x in seq:
                if x and x not in seen:
                    seen.add(x); out.append(x)
            return out
        for c in geo.get("cities") or []:
            if isinstance(c, dict):
                if c.get("name"): city_names.append(c["name"])
                if c.get("country"): countries.append(c["country"])
                if c.get("region"): regions.append(c["region"])
        for pl in geo.get("places") or []:
            if isinstance(pl, dict) and pl.get("name"):
                city_names.append(pl["name"])
                if pl.get("country"): countries.append(pl["country"])
        for n in geo.get("neighborhoods") or []:
            if isinstance(n, dict) and n.get("name"):
                city_names.append(n["name"])
                if n.get("region"): regions.append(n["region"])
                if n.get("country"): countries.append(n["country"])
        for z in geo.get("zips") or []:
            if isinstance(z, dict) and z.get("name"):
                city_names.append("zip:" + str(z["name"]))
                if z.get("country"): countries.append(z["country"])
        for r in geo.get("regions") or []:
            if isinstance(r, dict) and r.get("name"): regions.append(r["name"])
            elif isinstance(r, str): regions.append(r)
        countries, regions, city_names = uniq(countries), uniq(regions), uniq(city_names)
        geo_country = ", ".join(countries) if countries else None
        geo_regions = ", ".join(regions) if regions else None
        geo_cities = ", ".join(city_names) if city_names else None
        if age_min is not None: age_min = int(age_min)
        if age_max is not None: age_max = int(age_max)
    except Exception:
        pass
    return (age_min, age_max, age_range, geo_country, geo_regions, geo_cities)


SCHEMA = "Gold"
META_ROOT = "Files/Development/Bronze/Meta_ads"
GOOGLE_ROOT = "Files/Development/Bronze/Google_ads"
CONTROL = "Files/Silver/_control/medallion_pipeline_watermark.json"
SUMMARY = "Files/Development/Gold/exports/pipeline_refresh_summary.txt"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

def rel(p):
    return ("Files/" + p.split("/Files/", 1)[1]) if "/Files/" in p else p

def list_batch_csvs(folder):
    try:
        items = mssparkutils.fs.ls(folder)
    except Exception as e:
        print("[MISS]", folder, e)
        return []
    batch = [rel(i.path) for i in items if (not i.isDir) and i.name.endswith(".csv") and not i.name.endswith("_latest.csv")]
    latest = [rel(i.path) for i in items if (not i.isDir) and i.name.endswith("_latest.csv")]
    return batch if batch else latest

def read_csvs(paths):
    if not paths:
        return None
    return (
        spark.read.option("header", True).option("inferSchema", False)
        .option("multiLine", True).option("quote", '"').option("escape", '"')
        .csv(paths)
    )

def collect_batch_ptrs(root, entity):
    """Read nested tenant/account/connector/*_latest_batch.txt values."""
    base = f"{root}/{entity}"
    found = []
    def walk(path, depth=0):
        if depth > 6:
            return
        try:
            items = mssparkutils.fs.ls(path)
        except Exception:
            return
        for it in items:
            rp = rel(it.path)
            if it.isDir:
                walk(rp, depth + 1)
            elif it.name.endswith("_latest_batch.txt"):
                try:
                    found.append(mssparkutils.fs.head(rp, 200).strip())
                except Exception:
                    pass
    walk(base)
    return sorted(set([x for x in found if x]))

def merge_into_table(df, table_name, keys):
    """MERGE upsert — never deletes existing unmatched rows."""
    full = f"{SCHEMA}.{table_name}"
    if df is None or df.rdd.isEmpty():
        print("[SKIP empty]", full)
        return {"table": full, "skipped": True}
    # align to existing columns when table exists
    if spark.catalog.tableExists(full):
        target = spark.table(full)
        tcols = target.columns
        for c in tcols:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None))
        df = df.select(*tcols)
        df = df.dropDuplicates(keys)
        loc = spark.sql(f"DESCRIBE DETAIL {full}").collect()[0]["location"]
        cond = " AND ".join([f"t.`{k}` <=> s.`{k}`" for k in keys])
        (
            DeltaTable.forPath(spark, loc).alias("t")
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        cnt = spark.table(full).count()
        print("[MERGE]", full, "rows=", cnt)
        return {"table": full, "count": cnt, "merged": True}
    else:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(full)
        cnt = spark.table(full).count()
        print("[CREATE]", full, "rows=", cnt)
        return {"table": full, "count": cnt, "created": True}

# -------- discover --------
meta_ptrs = {
    "meta_campaigns": collect_batch_ptrs(META_ROOT, "meta_campaigns"),
    "meta_adsets": collect_batch_ptrs(META_ROOT, "meta_adsets"),
    "meta_ads": collect_batch_ptrs(META_ROOT, "meta_ads"),
    "meta_ad_insights": collect_batch_ptrs(META_ROOT, "meta_ad_insights"),
}
google_ptrs = {
    "google_campaigns": collect_batch_ptrs(GOOGLE_ROOT, "google_campaigns"),
    "google_ad_groups": collect_batch_ptrs(GOOGLE_ROOT, "google_ad_groups"),
    "google_ads": collect_batch_ptrs(GOOGLE_ROOT, "google_ads"),
    "google_ad_performance": collect_batch_ptrs(GOOGLE_ROOT, "google_ad_performance"),
}
print("META batch ptrs", {k: v for k, v in meta_ptrs.items()})
print("GOOGLE batch ptrs", {k: v for k, v in google_ptrs.items()})

# -------- META dimensions + insights --------
camp = read_csvs(list_batch_csvs(f"{META_ROOT}/meta_campaigns"))
adset = read_csvs(list_batch_csvs(f"{META_ROOT}/meta_adsets"))
ads = read_csvs(list_batch_csvs(f"{META_ROOT}/meta_ads"))
ins = read_csvs(list_batch_csvs(f"{META_ROOT}/meta_ad_insights"))

def parse_meta_dim(df, entity, id_field, extra_schema):
    if df is None:
        return None
    j = F.from_json(F.col("raw_json"), extra_schema)
    return (
        df.filter(F.col("entity_type") == entity)
        .withColumn("j", j)
        .select(
            F.col("tenant_id"), F.col("connector_id"), F.col("account_id"), F.col("account_name"),
            F.col("batch_id"), F.col("ingestion_time").cast("timestamp").alias("ingestion_time"),
            F.col("j.id").alias(id_field),
            F.col("j.name").alias(id_field.replace("_id", "_name") if id_field.endswith("_id") else "name"),
            F.col("j.status").alias("status"),
            F.col("j.*"),
        )
    )

# Insights are the grain for gold facts
if ins is None:
    raise Exception("No meta_ad_insights CSV found under Development Bronze")

ins_j = F.from_json(
    F.col("raw_json"),
    "campaign_id STRING, adset_id STRING, ad_id STRING, campaign_name STRING, adset_name STRING, ad_name STRING, "
    "impressions STRING, reach STRING, frequency STRING, clicks STRING, unique_clicks STRING, inline_link_clicks STRING, "
    "spend STRING, ctr STRING, cpc STRING, cpm STRING, cpp STRING, unique_ctr STRING, date_start STRING, date_stop STRING, "
    "actions STRING"
)
meta_fact = (
    ins.filter(F.col("entity_type") == "ad_insight")
    .withColumn("j", ins_j)
    .select(
        F.lit("meta").alias("platform"),
        F.col("tenant_id"), F.col("connector_id"), F.col("account_id"), F.col("account_name"),
        F.col("j.campaign_id").alias("campaign_id"), F.col("j.campaign_name").alias("campaign_name"),
        F.col("j.adset_id").alias("adset_id"), F.col("j.adset_name").alias("adset_name"),
        F.col("j.ad_id").alias("ad_id"), F.col("j.ad_name").alias("ad_name"),
        F.to_date("j.date_start").alias("full_date"),
        F.col("j.impressions").cast("long").alias("impressions"),
        F.col("j.reach").cast("long").alias("reach"),
        F.col("j.frequency").cast("double").alias("frequency"),
        F.col("j.clicks").cast("long").alias("clicks"),
        F.col("j.unique_clicks").cast("long").alias("unique_clicks"),
        F.col("j.inline_link_clicks").cast("long").alias("inline_link_clicks"),
        F.col("j.spend").cast("double").alias("spend"),
        F.col("j.ctr").cast("double").alias("ctr"),
        F.col("j.cpc").cast("double").alias("cpc"),
        F.col("j.cpm").cast("double").alias("cpm"),
        F.col("j.cpp").cast("double").alias("cpp"),
        F.col("j.unique_ctr").cast("double").alias("unique_ctr"),
        F.col("j.inline_link_clicks").cast("long").alias("link_clicks"),
        F.current_timestamp().alias("gold_processed_at"),
        F.col("batch_id"),
        F.col("ingestion_time").cast("timestamp").alias("ingestion_time"),
    )
    .filter(F.col("full_date").isNotNull() & F.col("ad_id").isNotNull())
)
meta_fact = meta_fact.withColumn(
    "_rk",
    F.row_number().over(
        Window.partitionBy("tenant_id", "account_id", "campaign_id", "adset_id", "ad_id", "full_date")
        .orderBy(F.col("ingestion_time").desc_nulls_last())
    ),
).filter(F.col("_rk") == 1).drop("_rk")

# enrich calendar fields
meta_fact = (
    meta_fact
    .withColumn("year", F.year("full_date").cast("string"))
    .withColumn("month", F.month("full_date").cast("string"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("day_name", F.date_format("full_date", "EEEE"))
    .withColumn("spend_inr", F.col("spend"))
    .withColumn("campaign_status", F.lit(None).cast("string"))
    .withColumn("campaign_objective", F.lit(None).cast("string"))
    .withColumn("campaign_channel_or_objective", F.lit(None).cast("string"))
    .withColumn("daily_budget_inr", F.lit(None).cast("string"))
    .withColumn("campaign_daily_budget_inr", F.lit(None).cast("string"))
    .withColumn("buying_type", F.lit(None).cast("string"))
    .withColumn("campaign_bid_strategy", F.lit(None).cast("string"))
    .withColumn("budget_remaining", F.lit(None).cast("string"))
    .withColumn("adset_status", F.lit(None).cast("string"))
    .withColumn("optimization_goal", F.lit(None).cast("string"))
    .withColumn("billing_event", F.lit(None).cast("string"))
    .withColumn("adset_bid_strategy", F.lit(None).cast("string"))
    .withColumn("age_min", F.lit(None).cast("string"))
    .withColumn("age_max", F.lit(None).cast("string"))
    .withColumn("age_range", F.lit(None).cast("string"))
    .withColumn("geo_country", F.lit(None).cast("string"))
    .withColumn("geo_regions", F.lit(None).cast("string"))
    .withColumn("geo_cities", F.lit(None).cast("string"))
    .withColumn("ad_type", F.lit(None).cast("string"))
    .withColumn("ad_status", F.lit(None).cast("string"))
    .withColumn("creative_id", F.lit(None).cast("string"))
    .withColumn("ad_title", F.lit(None).cast("string"))
    .withColumn("ad_body", F.lit(None).cast("string"))
    .withColumn("headline", F.lit(None).cast("string"))
    .withColumn("description", F.lit(None).cast("string"))
    .withColumn("thumbnail_url", F.lit(None).cast("string"))
    .withColumn("leads", F.lit(None).cast("double"))
    .withColumn("cost_per_lead", F.lit(None).cast("double"))
    .withColumn("landing_page_views", F.lit(None).cast("long"))
    .withColumn("post_engagement", F.lit(None).cast("long"))
    .withColumn("video_views_3s", F.lit(None).cast("long"))
    .withColumn("conversions", F.lit(None).cast("double"))
    .withColumn("conversions_value", F.lit(None).cast("double"))
    .withColumn("cost_per_conversion", F.lit(None).cast("double"))
    .withColumn("roas", F.lit(None).cast("double"))
    .withColumn("engagements", F.lit(None).cast("long"))
    .withColumn("video_views", F.lit(None).cast("long"))
)
print("meta_fact", meta_fact.count())

# -------- GOOGLE --------
gperf = read_csvs(list_batch_csvs(f"{GOOGLE_ROOT}/google_ad_performance"))
gcamp = read_csvs(list_batch_csvs(f"{GOOGLE_ROOT}/google_campaigns"))
gadg = read_csvs(list_batch_csvs(f"{GOOGLE_ROOT}/google_ad_groups"))
gads = read_csvs(list_batch_csvs(f"{GOOGLE_ROOT}/google_ads"))

if gperf is None:
    raise Exception("No google_ad_performance CSV found")

gp_j = F.from_json(
    F.col("raw_json"),
    "campaign_id STRING, adgroup_id STRING, ad_id STRING, date STRING, impressions STRING, clicks STRING, "
    "ctr STRING, average_cpc STRING, cost_micros STRING, cost STRING, conversions STRING, conversions_value STRING, "
    "cost_per_conversion STRING, roas STRING"
)
# dim names
gc_j = F.from_json(F.col("raw_json"), "campaign_id STRING, campaign_name STRING, status STRING, channel_type STRING")
ga_j = F.from_json(F.col("raw_json"), "adgroup_id STRING, adgroup_name STRING, campaign_id STRING, status STRING")
gad_j = F.from_json(F.col("raw_json"), "ad_id STRING, ad_name STRING, adgroup_id STRING, campaign_id STRING, status STRING")

gc_dim = None
if gcamp is not None:
    gc_dim = (
        gcamp.withColumn("j", gc_j)
        .select(F.col("j.campaign_id").alias("campaign_id"), F.col("j.campaign_name").alias("campaign_name"),
                F.col("j.status").alias("campaign_status"), F.col("j.channel_type").alias("campaign_channel_or_objective"),
                F.col("ingestion_time").cast("timestamp").alias("ingestion_time"))
        .withColumn("_rk", F.row_number().over(Window.partitionBy("campaign_id").orderBy(F.col("ingestion_time").desc_nulls_last())))
        .filter(F.col("_rk") == 1).drop("_rk", "ingestion_time")
    )
ga_dim = None
if gadg is not None:
    ga_dim = (
        gadg.withColumn("j", ga_j)
        .select(F.col("j.adgroup_id").alias("adset_id"), F.col("j.adgroup_name").alias("adset_name"),
                F.col("j.status").alias("adset_status"), F.col("ingestion_time").cast("timestamp").alias("ingestion_time"))
        .withColumn("_rk", F.row_number().over(Window.partitionBy("adset_id").orderBy(F.col("ingestion_time").desc_nulls_last())))
        .filter(F.col("_rk") == 1).drop("_rk", "ingestion_time")
    )
gad_dim = None
if gads is not None:
    gad_dim = (
        gads.withColumn("j", gad_j)
        .select(F.col("j.ad_id").alias("ad_id"), F.col("j.ad_name").alias("ad_name"),
                F.col("j.status").alias("ad_status"), F.col("ingestion_time").cast("timestamp").alias("ingestion_time"))
        .withColumn("_rk", F.row_number().over(Window.partitionBy("ad_id").orderBy(F.col("ingestion_time").desc_nulls_last())))
        .filter(F.col("_rk") == 1).drop("_rk", "ingestion_time")
    )

google_fact = (
    gperf.filter(F.col("entity_type") == "ad_performance")
    .withColumn("j", gp_j)
    .select(
        F.lit("google").alias("platform"),
        F.col("tenant_id"), F.col("connector_id"), F.col("account_id"), F.col("account_name"),
        F.col("j.campaign_id").alias("campaign_id"),
        F.col("j.adgroup_id").alias("adset_id"),
        F.col("j.ad_id").alias("ad_id"),
        F.to_date("j.date").alias("full_date"),
        F.col("j.impressions").cast("long").alias("impressions"),
        F.col("j.clicks").cast("long").alias("clicks"),
        F.col("j.cost").cast("double").alias("spend"),
        F.col("j.ctr").cast("double").alias("ctr"),
        F.col("j.average_cpc").cast("double").alias("cpc"),
        F.col("j.conversions").cast("double").alias("conversions"),
        F.col("j.conversions_value").cast("double").alias("conversions_value"),
        F.col("j.cost_per_conversion").cast("double").alias("cost_per_conversion"),
        F.col("j.roas").cast("double").alias("roas"),
        F.current_timestamp().alias("gold_processed_at"),
        F.col("batch_id"),
        F.col("ingestion_time").cast("timestamp").alias("ingestion_time"),
    )
    .filter(F.col("full_date").isNotNull() & F.col("ad_id").isNotNull())
)
if gc_dim is not None:
    google_fact = google_fact.join(gc_dim, "campaign_id", "left")
else:
    google_fact = google_fact.withColumn("campaign_name", F.lit(None).cast("string")).withColumn("campaign_status", F.lit(None).cast("string")).withColumn("campaign_channel_or_objective", F.lit(None).cast("string"))
if ga_dim is not None:
    google_fact = google_fact.join(ga_dim, "adset_id", "left")
else:
    google_fact = google_fact.withColumn("adset_name", F.lit(None).cast("string")).withColumn("adset_status", F.lit(None).cast("string"))
if gad_dim is not None:
    google_fact = google_fact.join(gad_dim, "ad_id", "left")
else:
    google_fact = google_fact.withColumn("ad_name", F.lit(None).cast("string")).withColumn("ad_status", F.lit(None).cast("string"))

google_fact = google_fact.withColumn(
    "_rk",
    F.row_number().over(
        Window.partitionBy("tenant_id", "account_id", "campaign_id", "adset_id", "ad_id", "full_date")
        .orderBy(F.col("ingestion_time").desc_nulls_last())
    ),
).filter(F.col("_rk") == 1).drop("_rk")

google_fact = (
    google_fact
    .withColumn("year", F.year("full_date").cast("string"))
    .withColumn("month", F.month("full_date").cast("string"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("day_name", F.date_format("full_date", "EEEE"))
    .withColumn("spend_inr", F.col("spend"))
    .withColumn("reach", F.lit(None).cast("long"))
    .withColumn("frequency", F.lit(None).cast("double"))
    .withColumn("unique_clicks", F.lit(None).cast("long"))
    .withColumn("inline_link_clicks", F.lit(None).cast("long"))
    .withColumn("link_clicks", F.lit(None).cast("long"))
    .withColumn("cpm", F.when(F.col("impressions") > 0, F.col("spend") * 1000 / F.col("impressions")).otherwise(F.lit(None)))
    .withColumn("cpp", F.lit(None).cast("double"))
    .withColumn("unique_ctr", F.lit(None).cast("double"))
)
print("google_fact", google_fact.count())

keys = ["platform", "account_id", "campaign_id", "adset_id", "ad_id", "full_date"]
# optional stronger key with tenant
keys_tenant = ["platform", "tenant_id", "account_id", "campaign_id", "adset_id", "ad_id", "full_date"]

before = {}
for t in ["rpt_meta_ad_performance_daily", "rpt_google_ad_performance_daily", "rpt_unified_ad_performance",
          "vw_campaign_performance", "vw_adset_performance", "vw_ad_performance", "vw_unified_ad_performance"]:
    try:
        before[t] = spark.table(f"{SCHEMA}.{t}").count() if spark.catalog.tableExists(f"{SCHEMA}.{t}") else 0
    except Exception:
        before[t] = 0
print("BEFORE", before)

# MERGE platform facts
meta_for_rpt = meta_fact.drop("batch_id", "ingestion_time")
google_for_rpt = google_fact.drop("batch_id", "ingestion_time")

# Use keys without requiring tenant match if historical rows lack tenant consistency
r1 = merge_into_table(meta_for_rpt, "rpt_meta_ad_performance_daily", ["account_id", "campaign_id", "adset_id", "ad_id", "full_date"])
r2 = merge_into_table(google_for_rpt, "rpt_google_ad_performance_daily", ["account_id", "campaign_id", "adset_id", "ad_id", "full_date"])

# unified = union of both facts aligned to existing unified schema
meta_u = spark.table(f"{SCHEMA}.rpt_meta_ad_performance_daily").withColumn("platform", F.coalesce(F.col("platform"), F.lit("meta")))
google_u = spark.table(f"{SCHEMA}.rpt_google_ad_performance_daily").withColumn("platform", F.coalesce(F.col("platform"), F.lit("google")))
# Prefer merging incoming facts into unified directly
incoming = meta_for_rpt.withColumn("platform", F.lit("meta")).unionByName(
    google_for_rpt.withColumn("platform", F.lit("google")), allowMissingColumns=True
)
r3 = merge_into_table(incoming, "rpt_unified_ad_performance", ["platform", "account_id", "campaign_id", "adset_id", "ad_id", "full_date"])

# Enrich geo/age from Meta adset targeting raw_json onto Gold facts (keeps existing rows)
_adset_paths = list_batch_csvs(f"{META_ROOT}/meta_adsets")
if _adset_paths:
    _adsets = read_csvs(_adset_paths)
    _tg = parse_targeting(F.col("raw_json"))
    adset_geo = (
        _adsets.filter(F.col("entity_type") == "adset")
        .select(
            F.get_json_object("raw_json", "$.id").alias("adset_id"),
            _tg["age_min"].alias("age_min"), _tg["age_max"].alias("age_max"), _tg["age_range"].alias("age_range"),
            _tg["geo_country"].alias("geo_country"), _tg["geo_regions"].alias("geo_regions"), _tg["geo_cities"].alias("geo_cities"),
            F.col("ingestion_time").cast("timestamp").alias("ingestion_time"),
        )
        .filter(F.col("adset_id").isNotNull())
        .withColumn("_rk", F.row_number().over(Window.partitionBy("adset_id").orderBy(F.col("ingestion_time").desc_nulls_last())))
        .filter(F.col("_rk") == 1).drop("_rk", "ingestion_time")
    )
    print("adset_geo", adset_geo.count(), "geo_nonnull", adset_geo.filter(F.col("geo_cities").isNotNull()).count())
    for _tbl in ["rpt_meta_ad_performance_daily", "rpt_unified_ad_performance"]:
        _full = f"{SCHEMA}.{_tbl}"
        if not spark.catalog.tableExists(_full):
            continue
        _loc = spark.sql(f"DESCRIBE DETAIL {_full}").collect()[0]["location"]
        _upd = (
            spark.table(_full).alias("t").join(adset_geo.alias("g"), "adset_id", "inner")
            .select(
                F.col("t.platform"), F.col("t.account_id"), F.col("t.campaign_id"), F.col("t.adset_id"), F.col("t.ad_id"), F.col("t.full_date"),
                F.col("g.age_min").cast("string").alias("age_min"), F.col("g.age_max").cast("string").alias("age_max"),
                F.col("g.age_range"), F.col("g.geo_country"), F.col("g.geo_regions"), F.col("g.geo_cities"),
            ).dropDuplicates(["platform","account_id","campaign_id","adset_id","ad_id","full_date"])
        )
        if _upd.rdd.isEmpty():
            continue
        _cond = " AND ".join([
            "t.platform <=> s.platform","t.account_id <=> s.account_id","t.campaign_id <=> s.campaign_id",
            "t.adset_id <=> s.adset_id","t.ad_id <=> s.ad_id","t.full_date <=> s.full_date",
        ])
        (DeltaTable.forPath(spark, _loc).alias("t").merge(_upd.alias("s"), _cond)
         .whenMatchedUpdate(set={
             "age_min":"s.age_min","age_max":"s.age_max","age_range":"s.age_range",
             "geo_country":"s.geo_country","geo_regions":"s.geo_regions","geo_cities":"s.geo_cities",
         }).execute())
        print("geo merged into", _full, "geo_nonnull", spark.table(_full).filter(F.col("geo_cities").isNotNull()).count())

# Rematerialize views from unified (full rebuild of vw only; fact data preserved via merge above)
SRC = f"{SCHEMA}.rpt_unified_ad_performance"
u = spark.table(SRC)
campaign_sql = f"""
SELECT platform, full_date, year, month, month_name, day_name,
  MAX(tenant_id) AS tenant_id, MAX(connector_id) AS connector_id, account_id,
  CAST(NULL AS STRING) AS customer_id, MAX(account_name) AS account_name,
  campaign_id, MAX(campaign_name) AS campaign_name, MAX(campaign_status) AS campaign_status,
  MAX(campaign_channel_or_objective) AS campaign_channel_or_objective, MAX(daily_budget_inr) AS daily_budget_inr,
  SUM(impressions) AS impressions, SUM(reach) AS reach, SUM(clicks) AS clicks, SUM(spend) AS spend, SUM(leads) AS leads,
  CASE WHEN SUM(clicks) > 0 THEN SUM(spend)/SUM(clicks) END AS cpc,
  CASE WHEN SUM(impressions) > 0 THEN (SUM(spend)/SUM(impressions))*1000 END AS cpm,
  CASE WHEN SUM(leads) > 0 THEN SUM(spend)/SUM(leads) END AS cost_per_lead,
  COUNT(DISTINCT adset_id) AS adset_count, COUNT(DISTINCT ad_id) AS ad_count
FROM {SRC}
GROUP BY platform, full_date, year, month, month_name, day_name, account_id, campaign_id
"""
adset_sql = f"""
SELECT platform, full_date, year, month, month_name, day_name,
  MAX(tenant_id) AS tenant_id, MAX(connector_id) AS connector_id, account_id,
  CAST(NULL AS STRING) AS customer_id, MAX(account_name) AS account_name,
  campaign_id, MAX(campaign_name) AS campaign_name,
  adset_id, MAX(adset_name) AS adset_name, MAX(adset_status) AS adset_status,
  MAX(optimization_goal) AS optimization_goal, MAX(age_range) AS age_range,
  MAX(geo_cities) AS geo_cities, MAX(geo_regions) AS geo_regions,
  SUM(impressions) AS impressions, SUM(reach) AS reach, SUM(clicks) AS clicks, SUM(spend) AS spend, SUM(leads) AS leads,
  CASE WHEN SUM(clicks) > 0 THEN SUM(spend)/SUM(clicks) END AS cpc,
  CASE WHEN SUM(impressions) > 0 THEN (SUM(spend)/SUM(impressions))*1000 END AS cpm,
  CASE WHEN SUM(leads) > 0 THEN SUM(spend)/SUM(leads) END AS cost_per_lead,
  COUNT(DISTINCT ad_id) AS ad_count
FROM {SRC}
GROUP BY platform, full_date, year, month, month_name, day_name, account_id, campaign_id, adset_id
"""

def mat(name, df):
    spark.sql(f"DROP TABLE IF EXISTS {SCHEMA}.{name}")
    try:
        spark.sql(f"DROP VIEW IF EXISTS {SCHEMA}.{name}")
    except Exception:
        pass
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{SCHEMA}.{name}")
    print("[VW]", name, spark.table(f"{SCHEMA}.{name}").count())

mat("vw_campaign_performance", spark.sql(campaign_sql))
mat("vw_adset_performance", spark.sql(adset_sql))
mat("vw_ad_performance", u.withColumn("customer_id", F.lit(None).cast("string")))
mat("vw_unified_ad_performance", u)
print("vw_unified sreevatsa", spark.table(f"{SCHEMA}.vw_unified_ad_performance").filter(F.lower(F.col("account_name")).contains("sreevatsa")).count())
print("vw_unified geo_nonnull", spark.table(f"{SCHEMA}.vw_unified_ad_performance").filter(F.col("geo_cities").isNotNull()).count())

after = {t: spark.table(f"{SCHEMA}.{t}").count() for t in before}
wm = {
    "updated_at_utc": datetime.now(timezone.utc).isoformat(),
    "mode": "incremental_merge",
    "meta_ptrs": meta_ptrs,
    "google_ptrs": google_ptrs,
    "before": before,
    "after": after,
    "note": "Existing Gold rows kept; new/changed grain keys upserted via MERGE",
}
mssparkutils.fs.put(CONTROL, json.dumps(wm, indent=2), True)
mssparkutils.fs.put(SUMMARY, json.dumps(wm, indent=2), True)
print("DONE", json.dumps(wm, indent=2))
mssparkutils.notebook.exit(json.dumps({"status": "success", "after": after}))
