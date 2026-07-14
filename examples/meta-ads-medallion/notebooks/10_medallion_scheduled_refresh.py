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
STG_SCHEMA = "Staging_Gold"
DEV_ROOT = "Files/Development"
STG_ROOT = "Files/Staging"
CONTROL = "Files/Silver/_control/medallion_pipeline_watermark.json"
SUMMARY = "Files/Development/Gold/exports/pipeline_refresh_summary.txt"
SUMMARY_STG = "Files/Staging/Gold/exports/pipeline_refresh_summary.txt"

# Bronze lands in Development and/or Staging (incl. legacy nested Staging/Bronze/Bronze)
META_ROOTS = [
    f"{DEV_ROOT}/Bronze/Meta_ads",
    f"{STG_ROOT}/Bronze/Meta_ads",
    f"{STG_ROOT}/Bronze/Bronze/Meta_ads",
]
GOOGLE_ROOTS = [
    f"{DEV_ROOT}/Bronze/Google_ads",
    f"{STG_ROOT}/Bronze/Google_ads",
    f"{STG_ROOT}/Bronze/Bronze/Google_ads",
]

# Silver Delta file roots written after each successful refresh
SILVER_META_ROOTS = [
    f"{DEV_ROOT}/Silver/meta_ads",
    f"{STG_ROOT}/Silver/meta_ads",
    "Files/Silver/meta_ads",
]
SILVER_GOOGLE_ROOTS = [
    f"{DEV_ROOT}/Silver/GoogleAds",
    f"{STG_ROOT}/Silver/GoogleAds",
]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {STG_SCHEMA}")

def rel(p):
    return ("Files/" + p.split("/Files/", 1)[1]) if "/Files/" in p else p

def _exists(path):
    try:
        mssparkutils.fs.ls(path)
        return True
    except Exception:
        return False

def _list_files(path, acc=None):
    if acc is None:
        acc = []
    try:
        items = mssparkutils.fs.ls(path)
    except Exception:
        return acc
    for it in items:
        if it.isDir:
            _list_files(rel(it.path), acc)
        else:
            acc.append(rel(it.path))
    return acc

def list_batch_csvs(folder):
    try:
        items = mssparkutils.fs.ls(folder)
    except Exception as e:
        print("[MISS]", folder, e)
        return []
    batch = [rel(i.path) for i in items if (not i.isDir) and i.name.endswith(".csv") and not i.name.endswith("_latest.csv")]
    latest = [rel(i.path) for i in items if (not i.isDir) and i.name.endswith("_latest.csv")]
    return batch if batch else latest

def list_batch_csvs_multi(roots, entity):
    """Union batch CSVs across Development + Staging bronze roots; dedupe by filename."""
    seen = {}
    for root in roots:
        for p in list_batch_csvs(f"{root}/{entity}"):
            seen[p.rsplit("/", 1)[-1]] = p
    paths = list(seen.values())
    print(f"[CSV] {entity}: {len(paths)} files from {len(roots)} roots")
    return paths

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

def collect_batch_ptrs_multi(roots, entity):
    found = []
    for root in roots:
        found.extend(collect_batch_ptrs(root, entity))
    return sorted(set(found))

def merge_into_table(df, table_name, keys, schema=SCHEMA):
    """MERGE upsert — never deletes existing unmatched rows."""
    full = f"{schema}.{table_name}"
    if df is None or df.rdd.isEmpty():
        print("[SKIP empty]", full)
        return {"table": full, "skipped": True}
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

def merge_into_path(df, path, keys):
    """MERGE upsert into a Files/ Delta path (Silver layer)."""
    if df is None or df.rdd.isEmpty():
        return {"path": path, "skipped": True}
    df = df.dropDuplicates(keys)
    try:
        is_delta = DeltaTable.isDeltaTable(spark, path)
    except Exception:
        is_delta = False
    if is_delta:
        existing = spark.read.format("delta").load(path)
        for c in existing.columns:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None))
        # keep source columns even if new
        cols = list(dict.fromkeys(list(existing.columns) + [c for c in df.columns if c not in existing.columns]))
        for c in cols:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None))
        df = df.select(*cols)
        cond = " AND ".join([f"t.`{k}` <=> s.`{k}`" for k in keys if k in cols])
        (
            DeltaTable.forPath(spark, path).alias("t")
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        cnt = spark.read.format("delta").load(path).count()
        print("[SILVER MERGE]", path, "rows=", cnt)
        return {"path": path, "count": cnt, "merged": True}
    mssparkutils.fs.mkdirs(path.rsplit("/", 1)[0])
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(path)
    cnt = spark.read.format("delta").load(path).count()
    print("[SILVER CREATE]", path, "rows=", cnt)
    return {"path": path, "count": cnt, "created": True}

def write_silver_multi(df, relative_name, keys, roots):
    results = []
    for root in roots:
        results.append(merge_into_path(df, f"{root}/{relative_name}", keys))
    return results

def sync_missing_files(src_root, dst_root):
    """Copy files present in src but missing under dst (same relative path). Does not delete."""
    if not _exists(src_root):
        return {"copied": 0, "reason": "source_missing", "src": src_root, "dst": dst_root}
    mssparkutils.fs.mkdirs(dst_root)
    src_files = _list_files(src_root)
    copied = 0
    errors = []
    for sp in src_files:
        rel_part = sp[len(src_root):].lstrip("/")
        dp = f"{dst_root}/{rel_part}"
        try:
            # exists check via ls parent + name is expensive; try head/ls
            parent = dp.rsplit("/", 1)[0]
            name = dp.rsplit("/", 1)[-1]
            present = False
            try:
                present = any((not it.isDir) and it.name == name for it in mssparkutils.fs.ls(parent))
            except Exception:
                present = False
            if present:
                continue
            mssparkutils.fs.mkdirs(parent)
            mssparkutils.fs.cp(sp, dp, False)
            copied += 1
        except Exception as e:
            errors.append({"src": sp, "dst": dp, "error": str(e)[:200]})
    return {"src": src_root, "dst": dst_root, "src_files": len(src_files), "copied": copied, "errors": errors[:10]}

# -------- discover (Development + Staging Bronze) --------
meta_ptrs = {
    "meta_campaigns": collect_batch_ptrs_multi(META_ROOTS, "meta_campaigns"),
    "meta_adsets": collect_batch_ptrs_multi(META_ROOTS, "meta_adsets"),
    "meta_ads": collect_batch_ptrs_multi(META_ROOTS, "meta_ads"),
    "meta_ad_insights": collect_batch_ptrs_multi(META_ROOTS, "meta_ad_insights"),
}
google_ptrs = {
    "google_campaigns": collect_batch_ptrs_multi(GOOGLE_ROOTS, "google_campaigns"),
    "google_ad_groups": collect_batch_ptrs_multi(GOOGLE_ROOTS, "google_ad_groups"),
    "google_ads": collect_batch_ptrs_multi(GOOGLE_ROOTS, "google_ads"),
    "google_ad_performance": collect_batch_ptrs_multi(GOOGLE_ROOTS, "google_ad_performance"),
}
print("META batch ptrs", {k: v for k, v in meta_ptrs.items()})
print("GOOGLE batch ptrs", {k: v for k, v in google_ptrs.items()})

# Early exit when Bronze batch pointers unchanged (avoids overlapping 5-min runs)
FORCE_RUN = False
try:
    prev_wm = json.loads(mssparkutils.fs.head(CONTROL, 20000))
except Exception:
    prev_wm = {}
prev_batches = {
    "meta_ptrs": prev_wm.get("meta_ptrs"),
    "google_ptrs": prev_wm.get("google_ptrs"),
}
curr_batches = {"meta_ptrs": meta_ptrs, "google_ptrs": google_ptrs}
if (not FORCE_RUN) and prev_batches["meta_ptrs"] == curr_batches["meta_ptrs"] and prev_batches["google_ptrs"] == curr_batches["google_ptrs"]:
    msg = {
        "status": "skipped",
        "reason": "no_new_bronze_batch_ptrs",
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "bronze_roots": {"meta": META_ROOTS, "google": GOOGLE_ROOTS},
        "meta_ptrs": meta_ptrs,
        "google_ptrs": google_ptrs,
    }
    print("NOOP", json.dumps(msg))
    mssparkutils.fs.put(SUMMARY, json.dumps(msg, indent=2), True)
    try:
        mssparkutils.fs.mkdirs("Files/Staging/Gold/exports")
        mssparkutils.fs.put(SUMMARY_STG, json.dumps(msg, indent=2), True)
    except Exception:
        pass
    mssparkutils.notebook.exit(json.dumps(msg))

# -------- META dimensions + insights (union Dev + Staging CSVs) --------
camp = read_csvs(list_batch_csvs_multi(META_ROOTS, "meta_campaigns"))
adset = read_csvs(list_batch_csvs_multi(META_ROOTS, "meta_adsets"))
ads = read_csvs(list_batch_csvs_multi(META_ROOTS, "meta_ads"))
ins = read_csvs(list_batch_csvs_multi(META_ROOTS, "meta_ad_insights"))

if ins is None:
    raise Exception("No meta_ad_insights CSV found under Development or Staging Bronze")

ins_j = F.from_json(
    F.col("raw_json"),
    "campaign_id STRING, adset_id STRING, ad_id STRING, campaign_name STRING, adset_name STRING, ad_name STRING, "
    "impressions STRING, reach STRING, frequency STRING, clicks STRING, unique_clicks STRING, inline_link_clicks STRING, "
    "spend STRING, ctr STRING, cpc STRING, cpm STRING, cpp STRING, unique_ctr STRING, date_start STRING, date_stop STRING, "
    "actions STRING"
)
meta_silver = (
    ins.filter(F.col("entity_type") == "ad_insight")
    .withColumn("j", ins_j)
    .select(
        F.col("connector_id"), F.col("tenant_id"), F.col("account_id"), F.col("account_name"),
        F.lit("meta").alias("platform"),
        F.col("batch_id"),
        F.col("ingestion_time").cast("timestamp").alias("ingestion_time"),
        F.col("j.ad_id").alias("ad_id"),
        F.col("j.adset_id").alias("adset_id"),
        F.col("j.campaign_id").alias("campaign_id"),
        F.col("j.ad_name").alias("ad_name"),
        F.col("j.adset_name").alias("adset_name"),
        F.col("j.campaign_name").alias("campaign_name"),
        F.to_date("j.date_start").alias("date_start"),
        F.to_date("j.date_stop").alias("date_stop"),
        F.col("j.impressions").cast("long").alias("impressions"),
        F.col("j.clicks").cast("long").alias("clicks"),
        F.col("j.unique_clicks").cast("long").alias("unique_clicks"),
        F.col("j.inline_link_clicks").cast("long").alias("inline_link_clicks"),
        F.col("j.reach").cast("long").alias("reach"),
        F.col("j.spend").cast("double").alias("spend"),
        F.col("j.frequency").cast("double").alias("frequency"),
        F.col("j.ctr").cast("double").alias("ctr"),
        F.col("j.unique_ctr").cast("double").alias("unique_ctr"),
        F.col("j.cpc").cast("double").alias("cpc"),
        F.col("j.cpm").cast("double").alias("cpm"),
        F.col("j.cpp").cast("double").alias("cpp"),
    )
    .filter(F.col("date_start").isNotNull() & F.col("ad_id").isNotNull())
)
meta_silver = meta_silver.withColumn(
    "_rk",
    F.row_number().over(
        Window.partitionBy("tenant_id", "account_id", "campaign_id", "adset_id", "ad_id", "date_start")
        .orderBy(F.col("ingestion_time").desc_nulls_last())
    ),
).filter(F.col("_rk") == 1).drop("_rk")

meta_fact = (
    meta_silver
    .withColumn("full_date", F.col("date_start"))
    .withColumn("link_clicks", F.col("inline_link_clicks"))
    .withColumn("gold_processed_at", F.current_timestamp())
)
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
gperf = read_csvs(list_batch_csvs_multi(GOOGLE_ROOTS, "google_ad_performance"))
gcamp = read_csvs(list_batch_csvs_multi(GOOGLE_ROOTS, "google_campaigns"))
gadg = read_csvs(list_batch_csvs_multi(GOOGLE_ROOTS, "google_ad_groups"))
gads = read_csvs(list_batch_csvs_multi(GOOGLE_ROOTS, "google_ads"))

if gperf is None:
    raise Exception("No google_ad_performance CSV found under Development or Staging Bronze")

gp_j = F.from_json(
    F.col("raw_json"),
    "campaign_id STRING, adgroup_id STRING, ad_id STRING, date STRING, impressions STRING, clicks STRING, "
    "ctr STRING, average_cpc STRING, cost_micros STRING, cost STRING, conversions STRING, conversions_value STRING, "
    "cost_per_conversion STRING, roas STRING"
)
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

google_silver = (
    gperf.filter(F.col("entity_type") == "ad_performance")
    .withColumn("j", gp_j)
    .select(
        F.col("connector_id"), F.col("tenant_id"), F.col("account_id"), F.col("account_name"),
        F.lit("google").alias("platform"),
        F.col("batch_id"),
        F.col("ingestion_time").cast("timestamp").alias("ingestion_time"),
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
    )
    .filter(F.col("full_date").isNotNull() & F.col("ad_id").isNotNull())
)
if gc_dim is not None:
    google_silver = google_silver.join(gc_dim, "campaign_id", "left")
else:
    google_silver = google_silver.withColumn("campaign_name", F.lit(None).cast("string")).withColumn("campaign_status", F.lit(None).cast("string")).withColumn("campaign_channel_or_objective", F.lit(None).cast("string"))
if ga_dim is not None:
    google_silver = google_silver.join(ga_dim, "adset_id", "left")
else:
    google_silver = google_silver.withColumn("adset_name", F.lit(None).cast("string")).withColumn("adset_status", F.lit(None).cast("string"))
if gad_dim is not None:
    google_silver = google_silver.join(gad_dim, "ad_id", "left")
else:
    google_silver = google_silver.withColumn("ad_name", F.lit(None).cast("string")).withColumn("ad_status", F.lit(None).cast("string"))

google_silver = google_silver.withColumn(
    "_rk",
    F.row_number().over(
        Window.partitionBy("tenant_id", "account_id", "campaign_id", "adset_id", "ad_id", "full_date")
        .orderBy(F.col("ingestion_time").desc_nulls_last())
    ),
).filter(F.col("_rk") == 1).drop("_rk")

google_fact = (
    google_silver
    .withColumn("gold_processed_at", F.current_timestamp())
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

before = {}
for t in ["rpt_meta_ad_performance_daily", "rpt_google_ad_performance_daily", "rpt_unified_ad_performance",
          "vw_campaign_performance", "vw_adset_performance", "vw_ad_performance", "vw_unified_ad_performance"]:
    try:
        before[t] = spark.table(f"{SCHEMA}.{t}").count() if spark.catalog.tableExists(f"{SCHEMA}.{t}") else 0
    except Exception:
        before[t] = 0
print("BEFORE", before)

# -------- Silver file writes (Development + Staging + Files/Silver) --------
silver_sync = {"meta": [], "google": []}
silver_sync["meta"] = write_silver_multi(
    meta_silver,
    "silver_meta_ad_insights",
    ["tenant_id", "account_id", "campaign_id", "adset_id", "ad_id", "date_start"],
    SILVER_META_ROOTS,
)
silver_sync["google"] = write_silver_multi(
    google_silver,
    "silver_google_ad_performance",
    ["tenant_id", "account_id", "campaign_id", "adset_id", "ad_id", "full_date"],
    SILVER_GOOGLE_ROOTS,
)

# -------- Gold MERGE --------
meta_for_rpt = meta_fact.drop("batch_id", "ingestion_time", "date_start", "date_stop")
google_for_rpt = google_fact.drop("batch_id", "ingestion_time")

r1 = merge_into_table(meta_for_rpt, "rpt_meta_ad_performance_daily", ["account_id", "campaign_id", "adset_id", "ad_id", "full_date"])
r2 = merge_into_table(google_for_rpt, "rpt_google_ad_performance_daily", ["account_id", "campaign_id", "adset_id", "ad_id", "full_date"])

incoming = meta_for_rpt.withColumn("platform", F.lit("meta")).unionByName(
    google_for_rpt.withColumn("platform", F.lit("google")), allowMissingColumns=True
)
r3 = merge_into_table(incoming, "rpt_unified_ad_performance", ["platform", "account_id", "campaign_id", "adset_id", "ad_id", "full_date"])

# Enrich geo/age from Meta adset targeting (all bronze roots)
_adset_paths = list_batch_csvs_multi(META_ROOTS, "meta_adsets")
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

# Rematerialize views from unified
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

after = {t: spark.table(f"{SCHEMA}.{t}").count() for t in before}

# -------- Bidirectional Bronze sync (do NOT wipe Staging-only landings) --------
bronze_sync = {"dev_to_stg": [], "stg_to_dev": []}
for platform_folder in ["Meta_ads", "Google_ads"]:
    pairs = [
        (f"{DEV_ROOT}/Bronze/{platform_folder}", f"{STG_ROOT}/Bronze/{platform_folder}"),
        (f"{STG_ROOT}/Bronze/{platform_folder}", f"{DEV_ROOT}/Bronze/{platform_folder}"),
        # pull legacy nested Staging copy into canonical Staging + Development
        (f"{STG_ROOT}/Bronze/Bronze/{platform_folder}", f"{STG_ROOT}/Bronze/{platform_folder}"),
        (f"{STG_ROOT}/Bronze/Bronze/{platform_folder}", f"{DEV_ROOT}/Bronze/{platform_folder}"),
    ]
    for src, dst in pairs:
        info = sync_missing_files(src, dst)
        key = "dev_to_stg" if src.startswith(DEV_ROOT) else "stg_to_dev"
        bronze_sync[key].append(info)
        print("BRONZE SYNC", info)

# -------- Mirror Gold managed tables → Staging_Gold --------
SYNC_TABLES = [
    "rpt_meta_ad_performance_daily",
    "rpt_google_ad_performance_daily",
    "rpt_unified_ad_performance",
    "vw_campaign_performance",
    "vw_adset_performance",
    "vw_ad_performance",
    "vw_unified_ad_performance",
]
staging_tables = {}
for t in SYNC_TABLES:
    src_t = f"{SCHEMA}.{t}"
    dst_t = f"{STG_SCHEMA}.{t}"
    try:
        if not spark.catalog.tableExists(src_t):
            staging_tables[t] = {"copied": False, "reason": "source_missing"}
            continue
        src_cnt = spark.table(src_t).count()
        spark.sql(f"DROP TABLE IF EXISTS {dst_t}")
        try:
            spark.sql(f"DROP VIEW IF EXISTS {dst_t}")
        except Exception:
            pass
        spark.table(src_t).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(dst_t)
        dst_cnt = spark.table(dst_t).count()
        staging_tables[t] = {
            "copied": True, "src": src_t, "dst": dst_t,
            "src_count": src_cnt, "dst_count": dst_cnt, "match": src_cnt == dst_cnt,
        }
        print("STAGING_GOLD", t, src_cnt, "->", dst_cnt)
    except Exception as e:
        staging_tables[t] = {"copied": False, "error": str(e)}
        print("STAGING_GOLD FAIL", t, e)

# Also export Gold Delta snapshots under Development + Staging Files/Gold/tables
gold_file_sync = []
for t in SYNC_TABLES:
    if not spark.catalog.tableExists(f"{SCHEMA}.{t}"):
        continue
    df = spark.table(f"{SCHEMA}.{t}")
    for root in [f"{DEV_ROOT}/Gold/tables", f"{STG_ROOT}/Gold/tables"]:
        path = f"{root}/{t}"
        try:
            mssparkutils.fs.mkdirs(root)
            df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(path)
            gold_file_sync.append({"path": path, "rows": df.count(), "ok": True})
            print("GOLD FILES", path)
        except Exception as e:
            gold_file_sync.append({"path": path, "ok": False, "error": str(e)[:200]})

wm = {
    "updated_at_utc": datetime.now(timezone.utc).isoformat(),
    "mode": "incremental_merge_dual_bronze",
    "bronze_roots": {"meta": META_ROOTS, "google": GOOGLE_ROOTS},
    "meta_ptrs": meta_ptrs,
    "google_ptrs": google_ptrs,
    "before": before,
    "after": after,
    "silver_sync": silver_sync,
    "bronze_sync": bronze_sync,
    "staging_gold": staging_tables,
    "gold_file_sync": gold_file_sync,
    "note": "Ingests Development+Staging Bronze; MERGEs Gold; writes Silver to Dev+Staging; mirrors Staging_Gold; bidirectional Bronze fill",
}
mssparkutils.fs.put(CONTROL, json.dumps(wm, indent=2), True)
mssparkutils.fs.mkdirs("Files/Development/Gold/exports")
mssparkutils.fs.put(SUMMARY, json.dumps(wm, indent=2), True)
mssparkutils.fs.mkdirs("Files/Staging/Gold/exports")
mssparkutils.fs.put(SUMMARY_STG, json.dumps(wm, indent=2), True)
mssparkutils.fs.put(f"{STG_ROOT}/_copy_from_development_summary.json", json.dumps({
    "bronze_sync": bronze_sync,
    "silver_sync": silver_sync,
    "staging_gold": staging_tables,
}, indent=2), True)
print("DONE", json.dumps({k: wm[k] for k in ["updated_at_utc", "before", "after"]}, indent=2))
mssparkutils.notebook.exit(json.dumps({
    "status": "success",
    "after": after,
    "staging_ok": all(v.get("match") for v in staging_tables.values() if v.get("copied")),
}))
