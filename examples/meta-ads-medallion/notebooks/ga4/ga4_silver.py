# Fabric notebook source: ga4_silver
# Bronze -> Silver for Google Analytics (GA4) under Development

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from notebookutils import mssparkutils
from datetime import datetime, timezone
import json

WS = "718e8176-5d40-4a9c-88ff-50ac97ac49ba"
LH = "981fbe98-2f01-41d8-bf2f-a85e5cd9e2a2"
BRONZE_ROOT = "Files/Development/Bronze/GoogleAnalytics_Ads"
SILVER_ROOT = "Files/Development/Silver/GoogleAnalytics"
CONTROL = f"{SILVER_ROOT}/_control/ga4_silver_watermark.json"

ENTITIES = [
    "ga4_account_summaries", "ga4_accounts", "ga4_properties", "ga4_data_streams",
    "ga4_attribution_settings", "ga4_conversion_events", "ga4_custom_dimensions",
    "ga4_key_event_definitions", "ga4_campaigns", "ga4_acquisition", "ga4_traffic",
    "ga4_events", "ga4_key_events", "ga4_demographics", "ga4_devices", "ga4_geography",
    "ga4_hourly", "ga4_landing_pages", "ga4_pages", "ga4_page_device",
    "ga4_page_geography", "ga4_page_source", "ga4_realtime", "ga4_technology",
]

def rel(p):
    return ("Files/" + p.split("/Files/", 1)[1]) if "/Files/" in p else p

def collect_ptrs(entity):
    base = f"{BRONZE_ROOT}/{entity}"
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
    return sorted(set(x for x in found if x))

def list_batch_csvs(entity):
    folder = f"{BRONZE_ROOT}/{entity}"
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

def merge_path(df, path, keys):
    if df is None or df.rdd.isEmpty():
        return {"path": path, "skipped": True}
    use_keys = [k for k in keys if k in df.columns]
    if not use_keys:
        use_keys = [c for c in ["entity_id", "batch_id"] if c in df.columns]
    df = df.dropDuplicates(use_keys) if use_keys else df
    try:
        is_delta = DeltaTable.isDeltaTable(spark, path)
    except Exception:
        is_delta = False
    if is_delta:
        existing = spark.read.format("delta").load(path)
        for c in existing.columns:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None))
        # allow new columns
        cols = list(dict.fromkeys(list(existing.columns) + [c for c in df.columns if c not in existing.columns]))
        df = df.select(*[c for c in cols if c in df.columns])
        use_keys = [k for k in use_keys if k in cols]
        cond = " AND ".join([f"t.`{k}` <=> s.`{k}`" for k in use_keys]) if use_keys else "1=0"
        (
            DeltaTable.forPath(spark, path).alias("t")
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        cnt = spark.read.format("delta").load(path).count()
        print("[MERGE]", path, cnt)
        return {"path": path, "count": cnt, "merged": True}
    mssparkutils.fs.mkdirs(path.rsplit("/", 1)[0])
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(path)
    cnt = spark.read.format("delta").load(path).count()
    print("[CREATE]", path, cnt)
    return {"path": path, "count": cnt, "created": True}

def flatten_entity(entity, df):
    """Flatten connector envelope + raw_json into typed silver columns."""
    if df is None:
        return None
    base = (
        df.select(
            F.col("connector_id"), F.col("tenant_id"), F.col("account_id"), F.col("account_name"),
            F.col("platform"), F.col("entity_type"), F.col("entity_id"),
            F.col("parent_entity_id"), F.col("batch_id"),
            F.col("ingestion_time").cast("timestamp").alias("ingestion_time"),
            F.to_date("extraction_start_date").alias("extraction_start_date"),
            F.to_date("extraction_end_date").alias("extraction_end_date"),
            F.col("raw_json"),
        )
        .withColumn("property_id", F.get_json_object("raw_json", "$._propertyId"))
        .withColumn("api_name", F.get_json_object("raw_json", "$._api"))
        .withColumn("silver_processed_at", F.current_timestamp())
        .withColumn("silver_entity", F.lit(entity))
    )

    # Report-specific flatteners
    if entity == "ga4_traffic":
        out = base.select(
            "*",
            F.get_json_object("raw_json", "$.date").alias("date_str"),
            F.to_date(F.get_json_object("raw_json", "$.date"), "yyyyMMdd").alias("full_date"),
            F.get_json_object("raw_json", "$.sessionSource").alias("session_source"),
            F.get_json_object("raw_json", "$.sessionMedium").alias("session_medium"),
            F.get_json_object("raw_json", "$.sessionCampaignName").alias("session_campaign"),
            F.get_json_object("raw_json", "$.sessions").cast("long").alias("sessions"),
            F.get_json_object("raw_json", "$.totalUsers").cast("long").alias("total_users"),
            F.get_json_object("raw_json", "$.engagedSessions").cast("long").alias("engaged_sessions"),
            F.get_json_object("raw_json", "$.engagementRate").cast("double").alias("engagement_rate"),
        )
    elif entity in ("ga4_campaigns", "ga4_acquisition"):
        out = base.select(
            "*",
            F.get_json_object("raw_json", "$.sessionSource").alias("session_source"),
            F.get_json_object("raw_json", "$.sessionMedium").alias("session_medium"),
            F.get_json_object("raw_json", "$.sessionCampaignName").alias("session_campaign"),
            F.get_json_object("raw_json", "$.sessionDefaultChannelGroup").alias("channel_group"),
            F.get_json_object("raw_json", "$.firstUserSource").alias("first_user_source"),
            F.get_json_object("raw_json", "$.firstUserMedium").alias("first_user_medium"),
            F.get_json_object("raw_json", "$.sessions").cast("long").alias("sessions"),
            F.get_json_object("raw_json", "$.totalUsers").cast("long").alias("total_users"),
            F.get_json_object("raw_json", "$.conversions").cast("double").alias("conversions"),
            F.get_json_object("raw_json", "$.engagedSessions").cast("long").alias("engaged_sessions"),
            F.get_json_object("raw_json", "$.bounceRate").cast("double").alias("bounce_rate"),
        )
    elif entity in ("ga4_events", "ga4_key_events"):
        out = base.select(
            "*",
            F.get_json_object("raw_json", "$.eventName").alias("event_name"),
            F.to_date(F.get_json_object("raw_json", "$.date"), "yyyyMMdd").alias("full_date"),
            F.get_json_object("raw_json", "$.sessionSource").alias("session_source"),
            F.get_json_object("raw_json", "$.sessionMedium").alias("session_medium"),
            F.get_json_object("raw_json", "$.sessionCampaignName").alias("session_campaign"),
            F.get_json_object("raw_json", "$.eventCount").cast("long").alias("event_count"),
            F.get_json_object("raw_json", "$.keyEvents").cast("long").alias("key_events"),
            F.get_json_object("raw_json", "$.totalUsers").cast("long").alias("total_users"),
        )
    elif entity == "ga4_hourly":
        out = base.select(
            "*",
            F.get_json_object("raw_json", "$.dateHour").alias("date_hour"),
            F.to_date(F.substring(F.get_json_object("raw_json", "$.dateHour"), 1, 8), "yyyyMMdd").alias("full_date"),
            F.substring(F.get_json_object("raw_json", "$.dateHour"), 9, 2).cast("int").alias("hour"),
            F.get_json_object("raw_json", "$.sessionSource").alias("session_source"),
            F.get_json_object("raw_json", "$.sessionMedium").alias("session_medium"),
            F.get_json_object("raw_json", "$.sessionCampaignName").alias("session_campaign"),
            F.get_json_object("raw_json", "$.sessions").cast("long").alias("sessions"),
            F.get_json_object("raw_json", "$.totalUsers").cast("long").alias("total_users"),
            F.get_json_object("raw_json", "$.conversions").cast("double").alias("conversions"),
        )
    elif entity in ("ga4_pages", "ga4_landing_pages", "ga4_page_source", "ga4_page_device", "ga4_page_geography"):
        out = base.select(
            "*",
            F.get_json_object("raw_json", "$.pagePath").alias("page_path"),
            F.get_json_object("raw_json", "$.pageTitle").alias("page_title"),
            F.get_json_object("raw_json", "$.landingPagePlusQueryString").alias("landing_page"),
            F.get_json_object("raw_json", "$.sessionSource").alias("session_source"),
            F.get_json_object("raw_json", "$.sessionMedium").alias("session_medium"),
            F.get_json_object("raw_json", "$.sessionCampaignName").alias("session_campaign"),
            F.get_json_object("raw_json", "$.deviceCategory").alias("device_category"),
            F.get_json_object("raw_json", "$.operatingSystem").alias("operating_system"),
            F.get_json_object("raw_json", "$.browser").alias("browser"),
            F.get_json_object("raw_json", "$.country").alias("country"),
            F.get_json_object("raw_json", "$.region").alias("region"),
            F.get_json_object("raw_json", "$.city").alias("city"),
            F.get_json_object("raw_json", "$.sessions").cast("long").alias("sessions"),
            F.get_json_object("raw_json", "$.totalUsers").cast("long").alias("total_users"),
            F.get_json_object("raw_json", "$.activeUsers").cast("long").alias("active_users"),
            F.get_json_object("raw_json", "$.screenPageViews").cast("long").alias("screen_page_views"),
            F.get_json_object("raw_json", "$.bounceRate").cast("double").alias("bounce_rate"),
            F.get_json_object("raw_json", "$.averageSessionDuration").cast("double").alias("avg_session_duration"),
            F.get_json_object("raw_json", "$.conversions").cast("double").alias("conversions"),
            F.get_json_object("raw_json", "$.totalRevenue").cast("double").alias("total_revenue"),
            F.get_json_object("raw_json", "$.purchaseRevenue").cast("double").alias("purchase_revenue"),
            F.get_json_object("raw_json", "$.transactions").cast("long").alias("transactions"),
            F.get_json_object("raw_json", "$.eventCount").cast("long").alias("event_count"),
            F.get_json_object("raw_json", "$.keyEvents").cast("long").alias("key_events"),
        )
    elif entity in ("ga4_devices", "ga4_technology", "ga4_demographics", "ga4_geography"):
        out = base.select(
            "*",
            F.get_json_object("raw_json", "$.deviceCategory").alias("device_category"),
            F.get_json_object("raw_json", "$.deviceModel").alias("device_model"),
            F.get_json_object("raw_json", "$.operatingSystem").alias("operating_system"),
            F.get_json_object("raw_json", "$.browser").alias("browser"),
            F.get_json_object("raw_json", "$.platform").alias("tech_platform"),
            F.get_json_object("raw_json", "$.country").alias("country"),
            F.get_json_object("raw_json", "$.region").alias("region"),
            F.get_json_object("raw_json", "$.city").alias("city"),
            F.get_json_object("raw_json", "$.userAgeBracket").alias("age_bracket"),
            F.get_json_object("raw_json", "$.userGender").alias("gender"),
            F.get_json_object("raw_json", "$.brandingInterest").alias("branding_interest"),
            F.get_json_object("raw_json", "$.sessionSource").alias("session_source"),
            F.get_json_object("raw_json", "$.sessionMedium").alias("session_medium"),
            F.get_json_object("raw_json", "$.sessionCampaignName").alias("session_campaign"),
            F.get_json_object("raw_json", "$.sessions").cast("long").alias("sessions"),
            F.get_json_object("raw_json", "$.totalUsers").cast("long").alias("total_users"),
            F.get_json_object("raw_json", "$.activeUsers").cast("long").alias("active_users"),
            F.get_json_object("raw_json", "$.newUsers").cast("long").alias("new_users"),
            F.get_json_object("raw_json", "$.conversions").cast("double").alias("conversions"),
        )
    elif entity in ("ga4_accounts", "ga4_properties", "ga4_data_streams", "ga4_account_summaries",
                    "ga4_attribution_settings", "ga4_conversion_events", "ga4_custom_dimensions",
                    "ga4_key_event_definitions"):
        out = base.select(
            "*",
            F.get_json_object("raw_json", "$.name").alias("resource_name"),
            F.get_json_object("raw_json", "$.displayName").alias("display_name"),
            F.get_json_object("raw_json", "$.eventName").alias("event_name"),
            F.get_json_object("raw_json", "$.parameterName").alias("parameter_name"),
            F.get_json_object("raw_json", "$.scope").alias("scope"),
            F.get_json_object("raw_json", "$.timeZone").alias("time_zone"),
            F.get_json_object("raw_json", "$.currencyCode").alias("currency_code"),
            F.get_json_object("raw_json", "$.industryCategory").alias("industry_category"),
            F.get_json_object("raw_json", "$.webStreamData.measurementId").alias("measurement_id"),
            F.get_json_object("raw_json", "$.webStreamData.defaultUri").alias("default_uri"),
            F.get_json_object("raw_json", "$.type").alias("stream_type"),
            F.get_json_object("raw_json", "$.account").alias("ga_account_resource"),
            F.get_json_object("raw_json", "$.regionCode").alias("region_code"),
        )
    elif entity == "ga4_realtime":
        out = base.select(
            "*",
            F.get_json_object("raw_json", "$.minutesAgo").alias("minutes_ago"),
            F.get_json_object("raw_json", "$.unifiedScreenName").alias("screen_name"),
            F.get_json_object("raw_json", "$.deviceCategory").alias("device_category"),
            F.get_json_object("raw_json", "$.country").alias("country"),
            F.get_json_object("raw_json", "$.activeUsers").cast("long").alias("active_users"),
            F.get_json_object("raw_json", "$.screenPageViews").cast("long").alias("screen_page_views"),
            F.get_json_object("raw_json", "$.eventCount").cast("long").alias("event_count"),
            F.get_json_object("raw_json", "$.conversions").cast("double").alias("conversions"),
        )
    else:
        out = base

    # Prefer newest ingestion per natural key within batch load
    key_cols = [c for c in ["tenant_id", "account_id", "entity_id", "full_date", "batch_id"] if c in out.columns]
    if "entity_id" in out.columns and "ingestion_time" in out.columns:
        w = Window.partitionBy(*([c for c in ["tenant_id", "account_id", "entity_id", "full_date"] if c in out.columns] or ["entity_id"])) \
            .orderBy(F.col("ingestion_time").desc_nulls_last())
        out = out.withColumn("_rk", F.row_number().over(w)).filter(F.col("_rk") == 1).drop("_rk")
    return out

# -------- watermark / incremental discovery --------
FORCE_RUN = False
try:
    prev = json.loads(mssparkutils.fs.head(CONTROL, 50000))
except Exception:
    prev = {}
prev_ptrs = prev.get("entity_ptrs") or {}

curr_ptrs = {e: collect_ptrs(e) for e in ENTITIES}
print("PTRS", {k: v for k, v in curr_ptrs.items() if v})

changed = [e for e in ENTITIES if sorted(curr_ptrs.get(e) or []) != sorted(prev_ptrs.get(e) or [])]
if FORCE_RUN:
    changed = ENTITIES[:]

if not changed:
    msg = {
        "status": "skipped",
        "reason": "no_new_ga4_bronze_batch_ptrs",
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "entity_ptrs": curr_ptrs,
    }
    print("NOOP", json.dumps(msg))
    mssparkutils.fs.mkdirs(f"{SILVER_ROOT}/_control")
    mssparkutils.fs.put(CONTROL, json.dumps(msg, indent=2), True)
    mssparkutils.notebook.exit(json.dumps(msg))

results = {}
for entity in changed:
    paths = list_batch_csvs(entity)
    # If incremental and we know new batch ids, prefer those files only
    new_batches = set(curr_ptrs.get(entity) or []) - set(prev_ptrs.get(entity) or [])
    if new_batches and not FORCE_RUN:
        filtered = [p for p in paths if any(b in p for b in new_batches)]
        if filtered:
            paths = filtered
    print(f"[LOAD] {entity} files={len(paths)} new_batches={sorted(new_batches)}")
    raw = read_csvs(paths)
    flat = flatten_entity(entity, raw)
    keys = ["tenant_id", "account_id", "entity_id", "batch_id"]
    if flat is not None and "full_date" in flat.columns:
        keys = ["tenant_id", "account_id", "entity_id", "full_date", "batch_id"]
    results[entity] = merge_path(flat, f"{SILVER_ROOT}/{entity}", keys)

wm = {
    "status": "success",
    "updated_at_utc": datetime.now(timezone.utc).isoformat(),
    "mode": "incremental_merge",
    "changed_entities": changed,
    "entity_ptrs": curr_ptrs,
    "results": results,
}
mssparkutils.fs.mkdirs(f"{SILVER_ROOT}/_control")
mssparkutils.fs.put(CONTROL, json.dumps(wm, indent=2), True)
mssparkutils.fs.put(f"{SILVER_ROOT}/_control/ga4_silver_summary.json", json.dumps(wm, indent=2), True)
print("DONE", json.dumps({"changed": changed, "counts": {k: v.get("count") for k, v in results.items()}}, indent=2))
mssparkutils.notebook.exit(json.dumps({"status": "success", "changed": changed}))
