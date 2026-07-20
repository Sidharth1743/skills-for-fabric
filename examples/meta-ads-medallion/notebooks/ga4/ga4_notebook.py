# Fabric notebook source: ga4_notebook
# Silver -> Gold star schema (dims + facts + views) into Development Gold

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from notebookutils import mssparkutils
from datetime import datetime, timezone
import json

WS = "718e8176-5d40-4a9c-88ff-50ac97ac49ba"
LH = "981fbe98-2f01-41d8-bf2f-a85e5cd9e2a2"
SILVER_ROOT = "Files/Development/Silver/GoogleAnalytics"
GOLD_FILES = "Files/Development/Gold/GoogleAnalytics"
SCHEMA = "Gold"  # Development Gold managed tables
CONTROL = f"{GOLD_FILES}/_control/ga4_gold_watermark.json"
SILVER_WM = f"{SILVER_ROOT}/_control/ga4_silver_watermark.json"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
mssparkutils.fs.mkdirs(f"{GOLD_FILES}/_control")
mssparkutils.fs.mkdirs(f"{GOLD_FILES}/tables")

def read_silver(name):
    path = f"{SILVER_ROOT}/{name}"
    try:
        if DeltaTable.isDeltaTable(spark, path):
            df = spark.read.format("delta").load(path)
            print(f"[SILVER] {name}: {df.count()}")
            return df
    except Exception as e:
        print(f"[MISS] {name}: {e}")
    return None

def merge_table(df, table, keys):
    full = f"{SCHEMA}.{table}"
    if df is None or df.rdd.isEmpty():
        print("[SKIP]", full)
        return {"table": full, "skipped": True}
    df = df.dropDuplicates(keys)
    if spark.catalog.tableExists(full):
        target = spark.table(full)
        for c in target.columns:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None))
        df = df.select(*target.columns)
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
        print("[MERGE]", full, cnt)
        return {"table": full, "count": cnt, "merged": True}
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(full)
    cnt = spark.table(full).count()
    print("[CREATE]", full, cnt)
    return {"table": full, "count": cnt, "created": True}

def mat_view(name, df):
    full = f"{SCHEMA}.{name}"
    spark.sql(f"DROP TABLE IF EXISTS {full}")
    try:
        spark.sql(f"DROP VIEW IF EXISTS {full}")
    except Exception:
        pass
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(full)
    # also file snapshot under Development/Gold/GoogleAnalytics
    path = f"{GOLD_FILES}/tables/{name}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(path)
    print("[VW]", full, spark.table(full).count())
    return {"table": full, "count": spark.table(full).count()}

def export_table(table):
    full = f"{SCHEMA}.{table}"
    if not spark.catalog.tableExists(full):
        return
    path = f"{GOLD_FILES}/tables/{table}"
    spark.table(full).write.format("delta").mode("overwrite").option("overwriteSchema", True).save(path)

# Incremental gate: run when silver watermark newer than gold, or FORCE
FORCE_RUN = False
try:
    silver_wm = json.loads(mssparkutils.fs.head(SILVER_WM, 50000))
except Exception:
    silver_wm = {}
try:
    gold_wm = json.loads(mssparkutils.fs.head(CONTROL, 50000))
except Exception:
    gold_wm = {}

silver_ts = silver_wm.get("updated_at_utc")
gold_ts = gold_wm.get("silver_updated_at_utc")
if (not FORCE_RUN) and silver_wm.get("status") == "skipped":
    msg = {"status": "skipped", "reason": "silver_noop", "updated_at_utc": datetime.now(timezone.utc).isoformat()}
    mssparkutils.fs.put(CONTROL, json.dumps(msg, indent=2), True)
    mssparkutils.notebook.exit(json.dumps(msg))
if (not FORCE_RUN) and silver_ts and gold_ts and silver_ts == gold_ts and gold_wm.get("status") == "success":
    msg = {"status": "skipped", "reason": "gold_already_current", "silver_updated_at_utc": silver_ts}
    mssparkutils.fs.put(CONTROL, json.dumps(msg, indent=2), True)
    mssparkutils.notebook.exit(json.dumps(msg))

results = {}

# ===================== DIMENSIONS =====================
props = read_silver("ga4_properties")
accounts = read_silver("ga4_accounts")
streams = read_silver("ga4_data_streams")

dim_property = None
if props is not None:
    dim_property = (
        props.select(
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"),
            F.col("property_id"), F.col("display_name").alias("property_name"),
            F.col("time_zone"), F.col("currency_code"), F.col("industry_category"),
            F.col("ga_account_resource"), F.col("resource_name"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(Window.partitionBy("tenant_id", "account_id", "property_id").orderBy(F.col("ingestion_time").desc_nulls_last())))
        .filter(F.col("_rk") == 1).drop("_rk")
    )
    results["dim_ga4_property"] = merge_table(dim_property, "dim_ga4_property", ["tenant_id", "account_id", "property_id"])

if streams is not None:
    dim_stream = (
        streams.select(
            F.col("tenant_id"), F.col("account_id"), F.col("property_id"),
            F.col("measurement_id"), F.col("default_uri"), F.col("stream_type"),
            F.col("display_name").alias("stream_name"), F.col("resource_name"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(Window.partitionBy("tenant_id", "account_id", "measurement_id").orderBy(F.col("ingestion_time").desc_nulls_last())))
        .filter(F.col("_rk") == 1).drop("_rk")
    )
    results["dim_ga4_data_stream"] = merge_table(dim_stream, "dim_ga4_data_stream", ["tenant_id", "account_id", "measurement_id"])


if accounts is not None:
    dim_account = (
        accounts.select(
            F.col("tenant_id"), F.col("account_id"),
            F.col("display_name").alias("account_display_name"),
            F.col("account_name"), F.col("resource_name"), F.col("region_code"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(Window.partitionBy("tenant_id", "account_id").orderBy(F.col("ingestion_time").desc_nulls_last())))
        .filter(F.col("_rk") == 1).drop("_rk")
    )
    results["dim_ga4_account"] = merge_table(dim_account.drop("ingestion_time"), "dim_ga4_account", ["tenant_id", "account_id"])

acct_sum = read_silver("ga4_account_summaries")
if acct_sum is not None:
    dim_as = (
        acct_sum.select(
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"),
            F.col("display_name"), F.col("resource_name"), F.col("region_code"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(Window.partitionBy("tenant_id", "account_id", "resource_name").orderBy(F.col("ingestion_time").desc_nulls_last())))
        .filter(F.col("_rk") == 1).drop("_rk")
    )
    results["dim_ga4_account_summary"] = merge_table(dim_as.drop("ingestion_time"), "dim_ga4_account_summary", ["tenant_id", "account_id", "resource_name"])

conv_ev = read_silver("ga4_conversion_events")
if conv_ev is not None:
    dim_ce = (
        conv_ev.select(
            F.col("tenant_id"), F.col("account_id"), F.col("property_id"),
            F.col("event_name"), F.col("resource_name"), F.col("display_name"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(Window.partitionBy("tenant_id", "account_id", "event_name").orderBy(F.col("ingestion_time").desc_nulls_last())))
        .filter(F.col("_rk") == 1).drop("_rk")
    )
    results["dim_ga4_conversion_event"] = merge_table(dim_ce.drop("ingestion_time"), "dim_ga4_conversion_event", ["tenant_id", "account_id", "event_name"])

ked = read_silver("ga4_key_event_definitions")
if ked is not None:
    dim_ked = (
        ked.select(
            F.col("tenant_id"), F.col("account_id"), F.col("property_id"),
            F.col("event_name"), F.col("resource_name"), F.col("display_name"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(Window.partitionBy("tenant_id", "account_id", "event_name").orderBy(F.col("ingestion_time").desc_nulls_last())))
        .filter(F.col("_rk") == 1).drop("_rk")
    )
    results["dim_ga4_key_event_definition"] = merge_table(dim_ked.drop("ingestion_time"), "dim_ga4_key_event_definition", ["tenant_id", "account_id", "event_name"])

cust_dim = read_silver("ga4_custom_dimensions")
if cust_dim is not None:
    dim_cd = (
        cust_dim.select(
            F.col("tenant_id"), F.col("account_id"), F.col("property_id"),
            F.col("parameter_name"), F.col("scope"), F.col("display_name"), F.col("resource_name"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(Window.partitionBy("tenant_id", "account_id", "parameter_name", "scope").orderBy(F.col("ingestion_time").desc_nulls_last())))
        .filter(F.col("_rk") == 1).drop("_rk")
    )
    results["dim_ga4_custom_dimension"] = merge_table(dim_cd.drop("ingestion_time"), "dim_ga4_custom_dimension", ["tenant_id", "account_id", "parameter_name", "scope"])

attr = read_silver("ga4_attribution_settings")
if attr is not None:
    dim_attr = (
        attr.select(
            F.col("tenant_id"), F.col("account_id"), F.col("property_id"),
            F.col("resource_name"), F.col("display_name"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(Window.partitionBy("tenant_id", "account_id", "resource_name").orderBy(F.col("ingestion_time").desc_nulls_last())))
        .filter(F.col("_rk") == 1).drop("_rk")
    )
    results["dim_ga4_attribution_settings"] = merge_table(dim_attr.drop("ingestion_time"), "dim_ga4_attribution_settings", ["tenant_id", "account_id", "resource_name"])


# Channel / campaign dimension from campaigns + traffic
camp = read_silver("ga4_campaigns")
traffic = read_silver("ga4_traffic")
channel_parts = []
for src in [camp, traffic]:
    if src is None:
        continue
    cols = src.columns
    channel_parts.append(
        src.select(
            F.col("tenant_id"), F.col("account_id"), F.col("property_id"),
            F.col("session_source"), F.col("session_medium"), F.col("session_campaign"),
            F.col("channel_group") if "channel_group" in cols else F.lit(None).cast("string").alias("channel_group"),
            F.col("ingestion_time"),
        )
    )
if channel_parts:
    dim_channel = (
        channel_parts[0] if len(channel_parts) == 1 else channel_parts[0].unionByName(channel_parts[1], allowMissingColumns=True)
    )
    dim_channel = (
        dim_channel
        .withColumn("channel_key", F.concat_ws("|",
            F.coalesce(F.col("session_source"), F.lit("(none)")),
            F.coalesce(F.col("session_medium"), F.lit("(none)")),
            F.coalesce(F.col("session_campaign"), F.lit("(none)")),
        ))
        .withColumn("_rk", F.row_number().over(Window.partitionBy("tenant_id", "account_id", "channel_key").orderBy(F.col("ingestion_time").desc_nulls_last())))
        .filter(F.col("_rk") == 1).drop("_rk")
        .withColumn("gold_processed_at", F.current_timestamp())
    )
    results["dim_ga4_channel"] = merge_table(
        dim_channel.select("tenant_id", "account_id", "property_id", "channel_key", "session_source", "session_medium", "session_campaign", "channel_group", "gold_processed_at"),
        "dim_ga4_channel",
        ["tenant_id", "account_id", "channel_key"],
    )

# Date dimension from traffic / key_events / hourly
date_frames = []
for name in ["ga4_traffic", "ga4_key_events", "ga4_hourly"]:
    df = read_silver(name)
    if df is not None and "full_date" in df.columns:
        date_frames.append(df.select(F.col("full_date")).filter(F.col("full_date").isNotNull()))
if date_frames:
    d = date_frames[0]
    for x in date_frames[1:]:
        d = d.unionByName(x)
    dim_date = (
        d.dropDuplicates(["full_date"])
        .withColumn("year", F.year("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("year_month", F.date_format("full_date", "yyyy-MM"))
        .withColumn("gold_processed_at", F.current_timestamp())
    )
    results["dim_ga4_date"] = merge_table(dim_date, "dim_ga4_date", ["full_date"])

pages = read_silver("ga4_pages")
if pages is not None:
    dim_page = (
        pages.select(
            F.col("tenant_id"), F.col("account_id"), F.col("property_id"),
            F.col("page_path"), F.col("page_title"),
            F.col("ingestion_time"),
        )
        .filter(F.col("page_path").isNotNull())
        .withColumn("_rk", F.row_number().over(Window.partitionBy("tenant_id", "account_id", "page_path").orderBy(F.col("ingestion_time").desc_nulls_last())))
        .filter(F.col("_rk") == 1).drop("_rk", "ingestion_time")
        .withColumn("gold_processed_at", F.current_timestamp())
    )
    results["dim_ga4_page"] = merge_table(dim_page, "dim_ga4_page", ["tenant_id", "account_id", "page_path"])

# ===================== FACTS (incremental MERGE) =====================
if traffic is not None:
    fact_traffic = (
        traffic
        .filter(F.col("full_date").isNotNull())
        .withColumn("channel_key", F.concat_ws("|",
            F.coalesce(F.col("session_source"), F.lit("(none)")),
            F.coalesce(F.col("session_medium"), F.lit("(none)")),
            F.coalesce(F.col("session_campaign"), F.lit("(none)")),
        ))
        .select(
            F.lit("google_analytics").alias("platform"),
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.col("full_date"), F.col("channel_key"),
            F.col("session_source"), F.col("session_medium"), F.col("session_campaign"),
            F.col("sessions"), F.col("total_users"), F.col("engaged_sessions"), F.col("engagement_rate"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy("tenant_id", "account_id", "full_date", "channel_key")
            .orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["fact_ga4_traffic_daily"] = merge_table(
        fact_traffic.drop("ingestion_time"),
        "fact_ga4_traffic_daily",
        ["tenant_id", "account_id", "full_date", "channel_key"],
    )

if camp is not None:
    fact_camp = (
        camp
        .withColumn("channel_key", F.concat_ws("|",
            F.coalesce(F.col("session_source"), F.lit("(none)")),
            F.coalesce(F.col("session_medium"), F.lit("(none)")),
            F.coalesce(F.col("session_campaign"), F.lit("(none)")),
        ))
        .select(
            F.lit("google_analytics").alias("platform"),
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.col("extraction_start_date"), F.col("extraction_end_date"),
            F.col("channel_key"), F.col("session_source"), F.col("session_medium"),
            F.col("session_campaign"), F.col("channel_group"),
            F.col("sessions"), F.col("total_users"), F.col("conversions"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy("tenant_id", "account_id", "channel_key", "extraction_start_date", "extraction_end_date")
            .orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["fact_ga4_campaign_performance"] = merge_table(
        fact_camp.drop("ingestion_time"),
        "fact_ga4_campaign_performance",
        ["tenant_id", "account_id", "channel_key", "extraction_start_date", "extraction_end_date"],
    )

key_events = read_silver("ga4_key_events")
if key_events is not None:
    fact_ke = (
        key_events.filter(F.col("full_date").isNotNull())
        .withColumn("channel_key", F.concat_ws("|",
            F.coalesce(F.col("session_source"), F.lit("(none)")),
            F.coalesce(F.col("session_medium"), F.lit("(none)")),
            F.coalesce(F.col("session_campaign"), F.lit("(none)")),
        ))
        .select(
            F.lit("google_analytics").alias("platform"),
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.col("full_date"), F.col("event_name"), F.col("channel_key"),
            F.col("session_source"), F.col("session_medium"), F.col("session_campaign"),
            F.col("key_events"), F.col("total_users"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy("tenant_id", "account_id", "full_date", "event_name", "channel_key")
            .orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["fact_ga4_key_events_daily"] = merge_table(
        fact_ke.drop("ingestion_time"),
        "fact_ga4_key_events_daily",
        ["tenant_id", "account_id", "full_date", "event_name", "channel_key"],
    )

if pages is not None:
    fact_page = (
        pages
        .select(
            F.lit("google_analytics").alias("platform"),
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.col("extraction_start_date"), F.col("extraction_end_date"),
            F.col("page_path"), F.col("page_title"),
            F.col("sessions"), F.col("total_users"), F.col("active_users"),
            F.col("screen_page_views"), F.col("bounce_rate"),
            F.col("event_count"), F.col("key_events"), F.col("transactions"),
            F.col("purchase_revenue"), F.col("total_revenue"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .filter(F.col("page_path").isNotNull())
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy("tenant_id", "account_id", "page_path", "extraction_start_date", "extraction_end_date")
            .orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["fact_ga4_page_performance"] = merge_table(
        fact_page.drop("ingestion_time"),
        "fact_ga4_page_performance",
        ["tenant_id", "account_id", "page_path", "extraction_start_date", "extraction_end_date"],
    )

geo = read_silver("ga4_geography")
if geo is not None:
    fact_geo = (
        geo.select(
            F.lit("google_analytics").alias("platform"),
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.col("extraction_start_date"), F.col("extraction_end_date"),
            F.col("country"), F.col("region"), F.col("city"),
            F.col("sessions"), F.col("active_users"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy("tenant_id", "account_id", "country", "region", "city", "extraction_start_date", "extraction_end_date")
            .orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["fact_ga4_geography"] = merge_table(
        fact_geo.drop("ingestion_time"),
        "fact_ga4_geography",
        ["tenant_id", "account_id", "country", "region", "city", "extraction_start_date", "extraction_end_date"],
    )

demo = read_silver("ga4_demographics")
if demo is not None:
    fact_demo = (
        demo.select(
            F.lit("google_analytics").alias("platform"),
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.col("extraction_start_date"), F.col("extraction_end_date"),
            F.col("age_bracket"), F.col("gender"), F.col("branding_interest"),
            F.col("sessions"), F.col("active_users"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy(
                "tenant_id", "account_id", "age_bracket", "gender", "branding_interest",
                "extraction_start_date", "extraction_end_date",
            ).orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["fact_ga4_demographics"] = merge_table(
        fact_demo.drop("ingestion_time"),
        "fact_ga4_demographics",
        ["tenant_id", "account_id", "age_bracket", "gender", "branding_interest", "extraction_start_date", "extraction_end_date"],
    )

hourly = read_silver("ga4_hourly")
if hourly is not None:
    fact_hourly = (
        hourly.filter(F.col("full_date").isNotNull())
        .withColumn("channel_key", F.concat_ws("|",
            F.coalesce(F.col("session_source"), F.lit("(none)")),
            F.coalesce(F.col("session_medium"), F.lit("(none)")),
            F.coalesce(F.col("session_campaign"), F.lit("(none)")),
        ))
        .select(
            F.lit("google_analytics").alias("platform"),
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.col("full_date"), F.col("hour"), F.col("date_hour"), F.col("channel_key"),
            F.col("session_source"), F.col("session_medium"), F.col("session_campaign"),
            F.col("sessions"), F.col("total_users"), F.col("conversions"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy("tenant_id", "account_id", "date_hour", "channel_key")
            .orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["fact_ga4_hourly"] = merge_table(
        fact_hourly.drop("ingestion_time"),
        "fact_ga4_hourly",
        ["tenant_id", "account_id", "date_hour", "channel_key"],
    )

devices = read_silver("ga4_devices")
if devices is not None:
    fact_devices = (
        devices.select(
            F.lit("google_analytics").alias("platform"),
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.col("extraction_start_date"), F.col("extraction_end_date"),
            F.col("device_category"), F.col("device_model"),
            F.col("operating_system"), F.col("browser"), F.col("tech_platform"),
            F.col("sessions"), F.col("total_users"), F.col("active_users"), F.col("new_users"),
            F.col("conversions"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy(
                "tenant_id", "account_id", "device_category", "device_model",
                "operating_system", "browser", "extraction_start_date", "extraction_end_date",
            ).orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["fact_ga4_devices"] = merge_table(
        fact_devices.drop("ingestion_time"),
        "fact_ga4_devices",
        [
            "tenant_id", "account_id", "device_category", "device_model",
            "operating_system", "browser", "extraction_start_date", "extraction_end_date",
        ],
    )


# ---- Remaining Silver → Gold facts (full GA4 coverage for AI / reporting) ----
acq = read_silver("ga4_acquisition")
if acq is not None:
    fact_acq = (
        acq.withColumn("channel_key", F.concat_ws("|",
            F.coalesce(F.col("session_source"), F.lit("(none)")),
            F.coalesce(F.col("session_medium"), F.lit("(none)")),
            F.coalesce(F.col("session_campaign"), F.lit("(none)")),
        ))
        .select(
            F.lit("google_analytics").alias("platform"),
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.col("extraction_start_date"), F.col("extraction_end_date"),
            F.col("channel_key"), F.col("session_source"), F.col("session_medium"), F.col("session_campaign"),
            F.col("channel_group"), F.col("first_user_source"), F.col("first_user_medium"),
            F.col("sessions"), F.col("total_users"), F.col("conversions"),
            F.col("engaged_sessions"), F.col("bounce_rate"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy("tenant_id", "account_id", "channel_key", "first_user_source", "first_user_medium", "extraction_start_date", "extraction_end_date")
            .orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["fact_ga4_acquisition"] = merge_table(
        fact_acq.drop("ingestion_time"), "fact_ga4_acquisition",
        ["tenant_id", "account_id", "channel_key", "first_user_source", "first_user_medium", "extraction_start_date", "extraction_end_date"],
    )

events = read_silver("ga4_events")
if events is not None:
    # Bronze ga4_events is extract-window grain (no per-day date in payload)
    fact_ev = (
        events
        .withColumn("channel_key", F.concat_ws("|",
            F.coalesce(F.col("session_source"), F.lit("(none)")),
            F.coalesce(F.col("session_medium"), F.lit("(none)")),
            F.coalesce(F.col("session_campaign"), F.lit("(none)")),
        ))
        .select(
            F.lit("google_analytics").alias("platform"),
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.col("extraction_start_date"), F.col("extraction_end_date"),
            F.col("event_name"), F.col("channel_key"),
            F.col("session_source"), F.col("session_medium"), F.col("session_campaign"),
            F.col("event_count"), F.col("key_events"), F.col("total_users"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .filter(F.col("event_name").isNotNull())
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy(
                "tenant_id", "account_id", "event_name", "channel_key",
                "extraction_start_date", "extraction_end_date",
            ).orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["fact_ga4_events_daily"] = merge_table(
        fact_ev.drop("ingestion_time"), "fact_ga4_events_daily",
        ["tenant_id", "account_id", "event_name", "channel_key", "extraction_start_date", "extraction_end_date"],
    )

landing = read_silver("ga4_landing_pages")
if landing is not None:
    fact_lp = (
        landing.select(
            F.lit("google_analytics").alias("platform"),
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.col("extraction_start_date"), F.col("extraction_end_date"),
            F.col("landing_page"), F.col("page_path"), F.col("page_title"),
            F.col("sessions"), F.col("total_users"), F.col("active_users"),
            F.col("screen_page_views"), F.col("bounce_rate"), F.col("avg_session_duration"),
            F.col("conversions"), F.col("event_count"), F.col("key_events"),
            F.col("transactions"), F.col("purchase_revenue"), F.col("total_revenue"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("landing_key", F.coalesce(F.col("landing_page"), F.col("page_path")))
        .filter(F.col("landing_key").isNotNull())
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy("tenant_id", "account_id", "landing_key", "extraction_start_date", "extraction_end_date")
            .orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["fact_ga4_landing_pages"] = merge_table(
        fact_lp.drop("ingestion_time"), "fact_ga4_landing_pages",
        ["tenant_id", "account_id", "landing_key", "extraction_start_date", "extraction_end_date"],
    )

page_dev = read_silver("ga4_page_device")
if page_dev is not None:
    fact_pd = (
        page_dev.select(
            F.lit("google_analytics").alias("platform"),
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.col("extraction_start_date"), F.col("extraction_end_date"),
            F.col("page_path"), F.col("page_title"),
            F.col("device_category"), F.col("operating_system"), F.col("browser"),
            F.col("sessions"), F.col("total_users"), F.col("active_users"),
            F.col("screen_page_views"), F.col("bounce_rate"), F.col("conversions"),
            F.col("event_count"), F.col("key_events"), F.col("transactions"),
            F.col("purchase_revenue"), F.col("total_revenue"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .filter(F.col("page_path").isNotNull())
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy("tenant_id", "account_id", "page_path", "device_category", "operating_system", "browser", "extraction_start_date", "extraction_end_date")
            .orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["fact_ga4_page_device"] = merge_table(
        fact_pd.drop("ingestion_time"), "fact_ga4_page_device",
        ["tenant_id", "account_id", "page_path", "device_category", "operating_system", "browser", "extraction_start_date", "extraction_end_date"],
    )

page_geo = read_silver("ga4_page_geography")
if page_geo is not None:
    fact_pg = (
        page_geo.select(
            F.lit("google_analytics").alias("platform"),
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.col("extraction_start_date"), F.col("extraction_end_date"),
            F.col("page_path"), F.col("page_title"),
            F.col("country"), F.col("region"), F.col("city"),
            F.col("sessions"), F.col("total_users"), F.col("active_users"),
            F.col("screen_page_views"), F.col("bounce_rate"), F.col("conversions"),
            F.col("event_count"), F.col("key_events"), F.col("transactions"),
            F.col("purchase_revenue"), F.col("total_revenue"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .filter(F.col("page_path").isNotNull())
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy("tenant_id", "account_id", "page_path", "country", "region", "city", "extraction_start_date", "extraction_end_date")
            .orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["fact_ga4_page_geography"] = merge_table(
        fact_pg.drop("ingestion_time"), "fact_ga4_page_geography",
        ["tenant_id", "account_id", "page_path", "country", "region", "city", "extraction_start_date", "extraction_end_date"],
    )

page_src = read_silver("ga4_page_source")
if page_src is not None:
    fact_ps = (
        page_src.withColumn("channel_key", F.concat_ws("|",
            F.coalesce(F.col("session_source"), F.lit("(none)")),
            F.coalesce(F.col("session_medium"), F.lit("(none)")),
            F.coalesce(F.col("session_campaign"), F.lit("(none)")),
        ))
        .select(
            F.lit("google_analytics").alias("platform"),
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.col("extraction_start_date"), F.col("extraction_end_date"),
            F.col("page_path"), F.col("page_title"), F.col("channel_key"),
            F.col("session_source"), F.col("session_medium"), F.col("session_campaign"),
            F.col("sessions"), F.col("total_users"), F.col("active_users"),
            F.col("screen_page_views"), F.col("bounce_rate"), F.col("conversions"),
            F.col("event_count"), F.col("key_events"), F.col("transactions"),
            F.col("purchase_revenue"), F.col("total_revenue"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .filter(F.col("page_path").isNotNull())
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy("tenant_id", "account_id", "page_path", "channel_key", "extraction_start_date", "extraction_end_date")
            .orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["fact_ga4_page_source"] = merge_table(
        fact_ps.drop("ingestion_time"), "fact_ga4_page_source",
        ["tenant_id", "account_id", "page_path", "channel_key", "extraction_start_date", "extraction_end_date"],
    )

tech = read_silver("ga4_technology")
if tech is not None:
    fact_tech = (
        tech.select(
            F.lit("google_analytics").alias("platform"),
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.col("extraction_start_date"), F.col("extraction_end_date"),
            F.col("device_category"), F.col("device_model"),
            F.col("operating_system"), F.col("browser"), F.col("tech_platform"),
            F.col("sessions"), F.col("total_users"), F.col("active_users"), F.col("new_users"),
            F.col("conversions"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy(
                "tenant_id", "account_id", "device_category", "device_model",
                "operating_system", "browser", "tech_platform",
                "extraction_start_date", "extraction_end_date",
            ).orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["fact_ga4_technology"] = merge_table(
        fact_tech.drop("ingestion_time"), "fact_ga4_technology",
        ["tenant_id", "account_id", "device_category", "device_model", "operating_system", "browser", "tech_platform", "extraction_start_date", "extraction_end_date"],
    )

# Realtime is a snapshot (not historical grain) — overwrite each Gold run for AI/ops monitoring
rt = read_silver("ga4_realtime")
if rt is not None:
    fact_rt = (
        rt.select(
            F.lit("google_analytics").alias("platform"),
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.col("minutes_ago"), F.col("screen_name"), F.col("device_category"), F.col("country"),
            F.col("active_users"), F.col("screen_page_views"), F.col("event_count"), F.col("conversions"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy("tenant_id", "account_id", "minutes_ago", "screen_name", "device_category", "country")
            .orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    # overwrite snapshot table
    full_rt = f"{SCHEMA}.fact_ga4_realtime"
    fact_rt.drop("ingestion_time").write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(full_rt)
    results["fact_ga4_realtime"] = {"table": full_rt, "count": spark.table(full_rt).count(), "overwritten": True, "note": "snapshot"}
    print("[CREATE]", full_rt, results["fact_ga4_realtime"]["count"])


# ===================== CONSUMER VIEWS (managed Delta tables) =====================
if spark.catalog.tableExists(f"{SCHEMA}.fact_ga4_traffic_daily"):
    results["vw_ga4_traffic_daily"] = mat_view(
        "vw_ga4_traffic_daily",
        spark.table(f"{SCHEMA}.fact_ga4_traffic_daily"),
    )

if spark.catalog.tableExists(f"{SCHEMA}.fact_ga4_campaign_performance"):
    results["vw_ga4_campaign_performance"] = mat_view(
        "vw_ga4_campaign_performance",
        spark.table(f"{SCHEMA}.fact_ga4_campaign_performance"),
    )

if spark.catalog.tableExists(f"{SCHEMA}.fact_ga4_page_performance"):
    results["vw_ga4_page_performance"] = mat_view(
        "vw_ga4_page_performance",
        spark.table(f"{SCHEMA}.fact_ga4_page_performance"),
    )

# ONE consumer performance table (like vw_google_ad_performance / vw_meta_ad_performance)
# insight_type: traffic|campaign|acquisition|demographics|geography|page|landing_page|page_device|page_geography|page_source|key_event|event|device|technology|hourly|realtime
def _null_str():
    return F.lit(None).cast("string")

def _null_long():
    return F.lit(None).cast("long")

def _null_dbl():
    return F.lit(None).cast("double")

def _null_date():
    return F.lit(None).cast("date")

def _source_medium(src_col="session_source", med_col="session_medium"):
    return F.concat_ws(
        " / ",
        F.coalesce(F.col(src_col), F.lit("(direct)")),
        F.coalesce(F.col(med_col), F.lit("(none)")),
    ).alias("source_medium")

def _blank_device_cols():
    return [
        _null_str().alias("device_category"),
        _null_str().alias("device_model"),
        _null_str().alias("operating_system"),
        _null_str().alias("browser"),
        _null_str().alias("tech_platform"),
        _null_long().alias("new_users"),
    ]

def _blank_landing():
    return _null_str().alias("landing_page")

unified_parts = []
if spark.catalog.tableExists(f"{SCHEMA}.fact_ga4_traffic_daily"):
    unified_parts.append(
        spark.table(f"{SCHEMA}.fact_ga4_traffic_daily").select(
            F.col("platform"), F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.lit("traffic").alias("insight_type"),
            F.col("full_date"), _null_date().alias("extraction_start_date"), _null_date().alias("extraction_end_date"),
            F.col("session_source"), F.col("session_medium"), F.col("session_campaign"), F.col("channel_key"),
            _source_medium(), _null_str().alias("channel_group"),
            _null_str().alias("age_bracket"), _null_str().alias("gender"), _null_str().alias("branding_interest"),
            _null_str().alias("country"), _null_str().alias("region"), _null_str().alias("city"),
            *_blank_device_cols(),
            _null_str().alias("page_path"), _null_str().alias("page_title"), _blank_landing(), _null_str().alias("event_name"),
            F.col("sessions"), F.col("total_users"), _null_long().alias("active_users"),
            F.col("engaged_sessions"), F.col("engagement_rate"), _null_dbl().alias("bounce_rate"),
            _null_long().alias("screen_page_views"), _null_long().alias("key_events"),
            _null_long().alias("event_count"), _null_long().alias("transactions"),
            _null_dbl().alias("conversions"), _null_dbl().alias("purchase_revenue"), _null_dbl().alias("total_revenue"),
            F.col("gold_processed_at"),
        )
    )
if spark.catalog.tableExists(f"{SCHEMA}.fact_ga4_campaign_performance"):
    unified_parts.append(
        spark.table(f"{SCHEMA}.fact_ga4_campaign_performance").select(
            F.col("platform"), F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.lit("campaign").alias("insight_type"),
            _null_date().alias("full_date"), F.col("extraction_start_date"), F.col("extraction_end_date"),
            F.col("session_source"), F.col("session_medium"), F.col("session_campaign"), F.col("channel_key"),
            _source_medium(), F.col("channel_group"),
            _null_str().alias("age_bracket"), _null_str().alias("gender"), _null_str().alias("branding_interest"),
            _null_str().alias("country"), _null_str().alias("region"), _null_str().alias("city"),
            *_blank_device_cols(),
            _null_str().alias("page_path"), _null_str().alias("page_title"), _blank_landing(), _null_str().alias("event_name"),
            F.col("sessions"), F.col("total_users"), _null_long().alias("active_users"),
            _null_long().alias("engaged_sessions"), _null_dbl().alias("engagement_rate"), _null_dbl().alias("bounce_rate"),
            _null_long().alias("screen_page_views"), _null_long().alias("key_events"),
            _null_long().alias("event_count"), _null_long().alias("transactions"),
            F.col("conversions"), _null_dbl().alias("purchase_revenue"), _null_dbl().alias("total_revenue"),
            F.col("gold_processed_at"),
        )
    )
if spark.catalog.tableExists(f"{SCHEMA}.fact_ga4_demographics"):
    unified_parts.append(
        spark.table(f"{SCHEMA}.fact_ga4_demographics").select(
            F.col("platform"), F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.lit("demographics").alias("insight_type"),
            _null_date().alias("full_date"), F.col("extraction_start_date"), F.col("extraction_end_date"),
            _null_str().alias("session_source"), _null_str().alias("session_medium"),
            _null_str().alias("session_campaign"), _null_str().alias("channel_key"),
            _null_str().alias("source_medium"), _null_str().alias("channel_group"),
            F.col("age_bracket"), F.col("gender"), F.col("branding_interest"),
            _null_str().alias("country"), _null_str().alias("region"), _null_str().alias("city"),
            *_blank_device_cols(),
            _null_str().alias("page_path"), _null_str().alias("page_title"), _blank_landing(), _null_str().alias("event_name"),
            F.col("sessions"), _null_long().alias("total_users"), F.col("active_users"),
            _null_long().alias("engaged_sessions"), _null_dbl().alias("engagement_rate"), _null_dbl().alias("bounce_rate"),
            _null_long().alias("screen_page_views"), _null_long().alias("key_events"),
            _null_long().alias("event_count"), _null_long().alias("transactions"),
            _null_dbl().alias("conversions"), _null_dbl().alias("purchase_revenue"), _null_dbl().alias("total_revenue"),
            F.col("gold_processed_at"),
        )
    )
if spark.catalog.tableExists(f"{SCHEMA}.fact_ga4_geography"):
    unified_parts.append(
        spark.table(f"{SCHEMA}.fact_ga4_geography").select(
            F.col("platform"), F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.lit("geography").alias("insight_type"),
            _null_date().alias("full_date"), F.col("extraction_start_date"), F.col("extraction_end_date"),
            _null_str().alias("session_source"), _null_str().alias("session_medium"),
            _null_str().alias("session_campaign"), _null_str().alias("channel_key"),
            _null_str().alias("source_medium"), _null_str().alias("channel_group"),
            _null_str().alias("age_bracket"), _null_str().alias("gender"), _null_str().alias("branding_interest"),
            F.col("country"), F.col("region"), F.col("city"),
            *_blank_device_cols(),
            _null_str().alias("page_path"), _null_str().alias("page_title"), _blank_landing(), _null_str().alias("event_name"),
            F.col("sessions"), _null_long().alias("total_users"), F.col("active_users"),
            _null_long().alias("engaged_sessions"), _null_dbl().alias("engagement_rate"), _null_dbl().alias("bounce_rate"),
            _null_long().alias("screen_page_views"), _null_long().alias("key_events"),
            _null_long().alias("event_count"), _null_long().alias("transactions"),
            _null_dbl().alias("conversions"), _null_dbl().alias("purchase_revenue"), _null_dbl().alias("total_revenue"),
            F.col("gold_processed_at"),
        )
    )
if spark.catalog.tableExists(f"{SCHEMA}.fact_ga4_page_performance"):
    unified_parts.append(
        spark.table(f"{SCHEMA}.fact_ga4_page_performance").select(
            F.col("platform"), F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.lit("page").alias("insight_type"),
            _null_date().alias("full_date"), F.col("extraction_start_date"), F.col("extraction_end_date"),
            _null_str().alias("session_source"), _null_str().alias("session_medium"),
            _null_str().alias("session_campaign"), _null_str().alias("channel_key"),
            _null_str().alias("source_medium"), _null_str().alias("channel_group"),
            _null_str().alias("age_bracket"), _null_str().alias("gender"), _null_str().alias("branding_interest"),
            _null_str().alias("country"), _null_str().alias("region"), _null_str().alias("city"),
            *_blank_device_cols(),
            F.col("page_path"), F.col("page_title"), _blank_landing(), _null_str().alias("event_name"),
            F.col("sessions"), F.col("total_users"), F.col("active_users"),
            _null_long().alias("engaged_sessions"), _null_dbl().alias("engagement_rate"), F.col("bounce_rate"),
            F.col("screen_page_views"), F.col("key_events"), F.col("event_count"), F.col("transactions"),
            _null_dbl().alias("conversions"), F.col("purchase_revenue"), F.col("total_revenue"),
            F.col("gold_processed_at"),
        )
    )
if spark.catalog.tableExists(f"{SCHEMA}.fact_ga4_key_events_daily"):
    unified_parts.append(
        spark.table(f"{SCHEMA}.fact_ga4_key_events_daily").select(
            F.col("platform"), F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.lit("key_event").alias("insight_type"),
            F.col("full_date"), _null_date().alias("extraction_start_date"), _null_date().alias("extraction_end_date"),
            F.col("session_source"), F.col("session_medium"), F.col("session_campaign"), F.col("channel_key"),
            _source_medium(), _null_str().alias("channel_group"),
            _null_str().alias("age_bracket"), _null_str().alias("gender"), _null_str().alias("branding_interest"),
            _null_str().alias("country"), _null_str().alias("region"), _null_str().alias("city"),
            *_blank_device_cols(),
            _null_str().alias("page_path"), _null_str().alias("page_title"), _blank_landing(), F.col("event_name"),
            _null_long().alias("sessions"), F.col("total_users"), _null_long().alias("active_users"),
            _null_long().alias("engaged_sessions"), _null_dbl().alias("engagement_rate"), _null_dbl().alias("bounce_rate"),
            _null_long().alias("screen_page_views"), F.col("key_events"),
            _null_long().alias("event_count"), _null_long().alias("transactions"),
            _null_dbl().alias("conversions"), _null_dbl().alias("purchase_revenue"), _null_dbl().alias("total_revenue"),
            F.col("gold_processed_at"),
        )
    )
if spark.catalog.tableExists(f"{SCHEMA}.fact_ga4_devices"):
    unified_parts.append(
        spark.table(f"{SCHEMA}.fact_ga4_devices").select(
            F.col("platform"), F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.lit("device").alias("insight_type"),
            _null_date().alias("full_date"), F.col("extraction_start_date"), F.col("extraction_end_date"),
            _null_str().alias("session_source"), _null_str().alias("session_medium"),
            _null_str().alias("session_campaign"), _null_str().alias("channel_key"),
            _null_str().alias("source_medium"), _null_str().alias("channel_group"),
            _null_str().alias("age_bracket"), _null_str().alias("gender"), _null_str().alias("branding_interest"),
            _null_str().alias("country"), _null_str().alias("region"), _null_str().alias("city"),
            F.col("device_category"), F.col("device_model"), F.col("operating_system"),
            F.col("browser"), F.col("tech_platform"), F.col("new_users"),
            _null_str().alias("page_path"), _null_str().alias("page_title"), _blank_landing(), _null_str().alias("event_name"),
            F.col("sessions"), F.col("total_users"), F.col("active_users"),
            _null_long().alias("engaged_sessions"), _null_dbl().alias("engagement_rate"), _null_dbl().alias("bounce_rate"),
            _null_long().alias("screen_page_views"), _null_long().alias("key_events"),
            _null_long().alias("event_count"), _null_long().alias("transactions"),
            F.col("conversions"), _null_dbl().alias("purchase_revenue"), _null_dbl().alias("total_revenue"),
            F.col("gold_processed_at"),
        )
    )


if spark.catalog.tableExists(f"{SCHEMA}.fact_ga4_acquisition"):
    unified_parts.append(
        spark.table(f"{SCHEMA}.fact_ga4_acquisition").select(
            F.col("platform"), F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.lit("acquisition").alias("insight_type"),
            _null_date().alias("full_date"), F.col("extraction_start_date"), F.col("extraction_end_date"),
            F.col("session_source"), F.col("session_medium"), F.col("session_campaign"), F.col("channel_key"),
            _source_medium(), F.col("channel_group"),
            _null_str().alias("age_bracket"), _null_str().alias("gender"), _null_str().alias("branding_interest"),
            _null_str().alias("country"), _null_str().alias("region"), _null_str().alias("city"),
            *_blank_device_cols(),
            _null_str().alias("page_path"), _null_str().alias("page_title"), _blank_landing(), _null_str().alias("event_name"),
            F.col("sessions"), F.col("total_users"), _null_long().alias("active_users"),
            F.col("engaged_sessions"), _null_dbl().alias("engagement_rate"), F.col("bounce_rate"),
            _null_long().alias("screen_page_views"), _null_long().alias("key_events"),
            _null_long().alias("event_count"), _null_long().alias("transactions"),
            F.col("conversions"), _null_dbl().alias("purchase_revenue"), _null_dbl().alias("total_revenue"),
            F.col("gold_processed_at"),
        )
    )
if spark.catalog.tableExists(f"{SCHEMA}.fact_ga4_events_daily"):
    unified_parts.append(
        spark.table(f"{SCHEMA}.fact_ga4_events_daily").select(
            F.col("platform"), F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.lit("event").alias("insight_type"),
            _null_date().alias("full_date"), F.col("extraction_start_date"), F.col("extraction_end_date"),
            F.col("session_source"), F.col("session_medium"), F.col("session_campaign"), F.col("channel_key"),
            _source_medium(), _null_str().alias("channel_group"),
            _null_str().alias("age_bracket"), _null_str().alias("gender"), _null_str().alias("branding_interest"),
            _null_str().alias("country"), _null_str().alias("region"), _null_str().alias("city"),
            *_blank_device_cols(),
            _null_str().alias("page_path"), _null_str().alias("page_title"), _blank_landing(), F.col("event_name"),
            _null_long().alias("sessions"), F.col("total_users"), _null_long().alias("active_users"),
            _null_long().alias("engaged_sessions"), _null_dbl().alias("engagement_rate"), _null_dbl().alias("bounce_rate"),
            _null_long().alias("screen_page_views"), F.col("key_events"),
            F.col("event_count"), _null_long().alias("transactions"),
            _null_dbl().alias("conversions"), _null_dbl().alias("purchase_revenue"), _null_dbl().alias("total_revenue"),
            F.col("gold_processed_at"),
        )
    )
if spark.catalog.tableExists(f"{SCHEMA}.fact_ga4_landing_pages"):
    unified_parts.append(
        spark.table(f"{SCHEMA}.fact_ga4_landing_pages").select(
            F.col("platform"), F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.lit("landing_page").alias("insight_type"),
            _null_date().alias("full_date"), F.col("extraction_start_date"), F.col("extraction_end_date"),
            _null_str().alias("session_source"), _null_str().alias("session_medium"),
            _null_str().alias("session_campaign"), _null_str().alias("channel_key"),
            _null_str().alias("source_medium"), _null_str().alias("channel_group"),
            _null_str().alias("age_bracket"), _null_str().alias("gender"), _null_str().alias("branding_interest"),
            _null_str().alias("country"), _null_str().alias("region"), _null_str().alias("city"),
            *_blank_device_cols(),
            F.col("page_path"), F.col("page_title"), F.col("landing_page"), _null_str().alias("event_name"),
            F.col("sessions"), F.col("total_users"), F.col("active_users"),
            _null_long().alias("engaged_sessions"), _null_dbl().alias("engagement_rate"), F.col("bounce_rate"),
            F.col("screen_page_views"), F.col("key_events"), F.col("event_count"), F.col("transactions"),
            F.col("conversions"), F.col("purchase_revenue"), F.col("total_revenue"),
            F.col("gold_processed_at"),
        )
    )
if spark.catalog.tableExists(f"{SCHEMA}.fact_ga4_page_device"):
    unified_parts.append(
        spark.table(f"{SCHEMA}.fact_ga4_page_device").select(
            F.col("platform"), F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.lit("page_device").alias("insight_type"),
            _null_date().alias("full_date"), F.col("extraction_start_date"), F.col("extraction_end_date"),
            _null_str().alias("session_source"), _null_str().alias("session_medium"),
            _null_str().alias("session_campaign"), _null_str().alias("channel_key"),
            _null_str().alias("source_medium"), _null_str().alias("channel_group"),
            _null_str().alias("age_bracket"), _null_str().alias("gender"), _null_str().alias("branding_interest"),
            _null_str().alias("country"), _null_str().alias("region"), _null_str().alias("city"),
            F.col("device_category"), _null_str().alias("device_model"), F.col("operating_system"),
            F.col("browser"), _null_str().alias("tech_platform"), _null_long().alias("new_users"),
            F.col("page_path"), F.col("page_title"), _blank_landing(), _null_str().alias("event_name"),
            F.col("sessions"), F.col("total_users"), F.col("active_users"),
            _null_long().alias("engaged_sessions"), _null_dbl().alias("engagement_rate"), F.col("bounce_rate"),
            F.col("screen_page_views"), F.col("key_events"), F.col("event_count"), F.col("transactions"),
            F.col("conversions"), F.col("purchase_revenue"), F.col("total_revenue"),
            F.col("gold_processed_at"),
        )
    )
if spark.catalog.tableExists(f"{SCHEMA}.fact_ga4_page_geography"):
    unified_parts.append(
        spark.table(f"{SCHEMA}.fact_ga4_page_geography").select(
            F.col("platform"), F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.lit("page_geography").alias("insight_type"),
            _null_date().alias("full_date"), F.col("extraction_start_date"), F.col("extraction_end_date"),
            _null_str().alias("session_source"), _null_str().alias("session_medium"),
            _null_str().alias("session_campaign"), _null_str().alias("channel_key"),
            _null_str().alias("source_medium"), _null_str().alias("channel_group"),
            _null_str().alias("age_bracket"), _null_str().alias("gender"), _null_str().alias("branding_interest"),
            F.col("country"), F.col("region"), F.col("city"),
            *_blank_device_cols(),
            F.col("page_path"), F.col("page_title"), _blank_landing(), _null_str().alias("event_name"),
            F.col("sessions"), F.col("total_users"), F.col("active_users"),
            _null_long().alias("engaged_sessions"), _null_dbl().alias("engagement_rate"), F.col("bounce_rate"),
            F.col("screen_page_views"), F.col("key_events"), F.col("event_count"), F.col("transactions"),
            F.col("conversions"), F.col("purchase_revenue"), F.col("total_revenue"),
            F.col("gold_processed_at"),
        )
    )
if spark.catalog.tableExists(f"{SCHEMA}.fact_ga4_page_source"):
    unified_parts.append(
        spark.table(f"{SCHEMA}.fact_ga4_page_source").select(
            F.col("platform"), F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.lit("page_source").alias("insight_type"),
            _null_date().alias("full_date"), F.col("extraction_start_date"), F.col("extraction_end_date"),
            F.col("session_source"), F.col("session_medium"), F.col("session_campaign"), F.col("channel_key"),
            _source_medium(), _null_str().alias("channel_group"),
            _null_str().alias("age_bracket"), _null_str().alias("gender"), _null_str().alias("branding_interest"),
            _null_str().alias("country"), _null_str().alias("region"), _null_str().alias("city"),
            *_blank_device_cols(),
            F.col("page_path"), F.col("page_title"), _blank_landing(), _null_str().alias("event_name"),
            F.col("sessions"), F.col("total_users"), F.col("active_users"),
            _null_long().alias("engaged_sessions"), _null_dbl().alias("engagement_rate"), F.col("bounce_rate"),
            F.col("screen_page_views"), F.col("key_events"), F.col("event_count"), F.col("transactions"),
            F.col("conversions"), F.col("purchase_revenue"), F.col("total_revenue"),
            F.col("gold_processed_at"),
        )
    )
if spark.catalog.tableExists(f"{SCHEMA}.fact_ga4_technology"):
    unified_parts.append(
        spark.table(f"{SCHEMA}.fact_ga4_technology").select(
            F.col("platform"), F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.lit("technology").alias("insight_type"),
            _null_date().alias("full_date"), F.col("extraction_start_date"), F.col("extraction_end_date"),
            _null_str().alias("session_source"), _null_str().alias("session_medium"),
            _null_str().alias("session_campaign"), _null_str().alias("channel_key"),
            _null_str().alias("source_medium"), _null_str().alias("channel_group"),
            _null_str().alias("age_bracket"), _null_str().alias("gender"), _null_str().alias("branding_interest"),
            _null_str().alias("country"), _null_str().alias("region"), _null_str().alias("city"),
            F.col("device_category"), F.col("device_model"), F.col("operating_system"),
            F.col("browser"), F.col("tech_platform"), F.col("new_users"),
            _null_str().alias("page_path"), _null_str().alias("page_title"), _blank_landing(), _null_str().alias("event_name"),
            F.col("sessions"), F.col("total_users"), F.col("active_users"),
            _null_long().alias("engaged_sessions"), _null_dbl().alias("engagement_rate"), _null_dbl().alias("bounce_rate"),
            _null_long().alias("screen_page_views"), _null_long().alias("key_events"),
            _null_long().alias("event_count"), _null_long().alias("transactions"),
            F.col("conversions"), _null_dbl().alias("purchase_revenue"), _null_dbl().alias("total_revenue"),
            F.col("gold_processed_at"),
        )
    )
if spark.catalog.tableExists(f"{SCHEMA}.fact_ga4_hourly"):
    unified_parts.append(
        spark.table(f"{SCHEMA}.fact_ga4_hourly").select(
            F.col("platform"), F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.lit("hourly").alias("insight_type"),
            F.col("full_date"), _null_date().alias("extraction_start_date"), _null_date().alias("extraction_end_date"),
            F.col("session_source"), F.col("session_medium"), F.col("session_campaign"), F.col("channel_key"),
            _source_medium(), _null_str().alias("channel_group"),
            _null_str().alias("age_bracket"), _null_str().alias("gender"), _null_str().alias("branding_interest"),
            _null_str().alias("country"), _null_str().alias("region"), _null_str().alias("city"),
            *_blank_device_cols(),
            _null_str().alias("page_path"), _null_str().alias("page_title"), _blank_landing(), _null_str().alias("event_name"),
            F.col("sessions"), F.col("total_users"), _null_long().alias("active_users"),
            _null_long().alias("engaged_sessions"), _null_dbl().alias("engagement_rate"), _null_dbl().alias("bounce_rate"),
            _null_long().alias("screen_page_views"), _null_long().alias("key_events"),
            _null_long().alias("event_count"), _null_long().alias("transactions"),
            F.col("conversions"), _null_dbl().alias("purchase_revenue"), _null_dbl().alias("total_revenue"),
            F.col("gold_processed_at"),
        )
    )
if spark.catalog.tableExists(f"{SCHEMA}.fact_ga4_realtime"):
    unified_parts.append(
        spark.table(f"{SCHEMA}.fact_ga4_realtime").select(
            F.col("platform"), F.col("tenant_id"), F.col("account_id"), F.col("account_name"), F.col("property_id"),
            F.lit("realtime").alias("insight_type"),
            _null_date().alias("full_date"), _null_date().alias("extraction_start_date"), _null_date().alias("extraction_end_date"),
            _null_str().alias("session_source"), _null_str().alias("session_medium"),
            _null_str().alias("session_campaign"), _null_str().alias("channel_key"),
            _null_str().alias("source_medium"), _null_str().alias("channel_group"),
            _null_str().alias("age_bracket"), _null_str().alias("gender"), _null_str().alias("branding_interest"),
            F.col("country"), _null_str().alias("region"), _null_str().alias("city"),
            F.col("device_category"), _null_str().alias("device_model"), _null_str().alias("operating_system"),
            _null_str().alias("browser"), _null_str().alias("tech_platform"), _null_long().alias("new_users"),
            F.col("screen_name").alias("page_path"), _null_str().alias("page_title"), _blank_landing(), _null_str().alias("event_name"),
            _null_long().alias("sessions"), _null_long().alias("total_users"), F.col("active_users"),
            _null_long().alias("engaged_sessions"), _null_dbl().alias("engagement_rate"), _null_dbl().alias("bounce_rate"),
            F.col("screen_page_views"), _null_long().alias("key_events"),
            F.col("event_count"), _null_long().alias("transactions"),
            F.col("conversions"), _null_dbl().alias("purchase_revenue"), _null_dbl().alias("total_revenue"),
            F.col("gold_processed_at"),
        )
    )

if unified_parts:
    u = unified_parts[0]
    for part in unified_parts[1:]:
        u = u.unionByName(part, allowMissingColumns=True)

    # Enrich property / measurement id from dims when available
    if spark.catalog.tableExists(f"{SCHEMA}.dim_ga4_property"):
        dp = spark.table(f"{SCHEMA}.dim_ga4_property").select(
            "tenant_id", "account_id",
            F.col("property_id").alias("_pid"),
            F.col("property_name"), F.col("time_zone"), F.col("currency_code"),
        )
        u = (
            u.join(dp, ["tenant_id", "account_id"], "left")
            .withColumn("property_id", F.coalesce(F.col("property_id"), F.col("_pid")))
            .drop("_pid")
        )
    else:
        u = u.withColumn("property_name", F.lit(None).cast("string")) \
             .withColumn("time_zone", F.lit(None).cast("string")) \
             .withColumn("currency_code", F.lit(None).cast("string"))
    if spark.catalog.tableExists(f"{SCHEMA}.dim_ga4_data_stream"):
        ds = spark.table(f"{SCHEMA}.dim_ga4_data_stream").select(
            "tenant_id", "account_id", "measurement_id", "default_uri",
        )
        u = u.join(ds, ["tenant_id", "account_id"], "left")
    else:
        u = u.withColumn("measurement_id", F.lit(None).cast("string")) \
             .withColumn("default_uri", F.lit(None).cast("string"))

    u = (
        u.withColumn("year", F.year("full_date"))
         .withColumn("month", F.month("full_date"))
         .withColumn("month_name", F.date_format("full_date", "MMMM"))
         .withColumn("day_name", F.date_format("full_date", "EEEE"))
         .withColumn("unified_key", F.concat_ws("||",
            F.coalesce(F.col("insight_type"), F.lit("")),
            F.coalesce(F.col("full_date").cast("string"), F.lit("")),
            F.coalesce(F.col("extraction_start_date").cast("string"), F.lit("")),
            F.coalesce(F.col("extraction_end_date").cast("string"), F.lit("")),
            F.coalesce(F.col("channel_key"), F.lit("")),
            F.coalesce(F.col("age_bracket"), F.lit("")),
            F.coalesce(F.col("gender"), F.lit("")),
            F.coalesce(F.col("branding_interest"), F.lit("")),
            F.coalesce(F.col("country"), F.lit("")),
            F.coalesce(F.col("region"), F.lit("")),
            F.coalesce(F.col("city"), F.lit("")),
            F.coalesce(F.col("device_category"), F.lit("")),
            F.coalesce(F.col("device_model"), F.lit("")),
            F.coalesce(F.col("operating_system"), F.lit("")),
            F.coalesce(F.col("browser"), F.lit("")),
            F.coalesce(F.col("page_path"), F.lit("")),
            F.coalesce(F.col("landing_page"), F.lit("")),
            F.coalesce(F.col("event_name"), F.lit("")),
         ))
    )

    # Rebuild report table with full schema (consumer view source of truth)
    rpt_full = f"{SCHEMA}.rpt_ga4_unified"
    if spark.catalog.tableExists(rpt_full):
        spark.sql(f"DROP TABLE IF EXISTS {rpt_full}")
    u.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(rpt_full)
    results["rpt_ga4_unified"] = {"table": rpt_full, "count": spark.table(rpt_full).count(), "overwritten": True}
    print("[CREATE]", rpt_full, results["rpt_ga4_unified"]["count"])

    # ONE major consumer view — naming aligned with vw_google_ad_performance / vw_meta_ad_performance
    perf = spark.table(rpt_full)
    results["vw_google_analytics_performance"] = mat_view("vw_google_analytics_performance", perf)
    results["vw_ga4_unified"] = mat_view("vw_ga4_unified", perf)
    results["vw_ga4_unified_sessions"] = mat_view(
        "vw_ga4_unified_sessions",
        perf.filter(F.col("insight_type") == "traffic"),
    )

# Export dim/fact tables to Development Gold files
for t in [
    "dim_ga4_property", "dim_ga4_data_stream", "dim_ga4_account", "dim_ga4_account_summary",
    "dim_ga4_channel", "dim_ga4_date", "dim_ga4_page",
    "dim_ga4_conversion_event", "dim_ga4_key_event_definition", "dim_ga4_custom_dimension",
    "dim_ga4_attribution_settings",
    "fact_ga4_traffic_daily", "fact_ga4_campaign_performance", "fact_ga4_acquisition",
    "fact_ga4_key_events_daily", "fact_ga4_events_daily",
    "fact_ga4_page_performance", "fact_ga4_landing_pages",
    "fact_ga4_page_device", "fact_ga4_page_geography", "fact_ga4_page_source",
    "fact_ga4_geography", "fact_ga4_demographics", "fact_ga4_hourly",
    "fact_ga4_devices", "fact_ga4_technology", "fact_ga4_realtime",
    "rpt_ga4_unified", "vw_google_analytics_performance", "vw_ga4_unified",
]:
    export_table(t)

after = {}
for t in results:
    name = results[t].get("table", f"{SCHEMA}.{t}").split(".")[-1]
    try:
        if spark.catalog.tableExists(f"{SCHEMA}.{name}"):
            after[name] = spark.table(f"{SCHEMA}.{name}").count()
    except Exception:
        pass

wm = {
    "status": "success",
    "updated_at_utc": datetime.now(timezone.utc).isoformat(),
    "silver_updated_at_utc": silver_ts,
    "mode": "incremental_merge_star_schema",
    "schema": SCHEMA,
    "gold_files_root": GOLD_FILES,
    "results": results,
    "after": after,
    "note": "GA4 dims/facts/views written to Development Gold schema + Files/Development/Gold/GoogleAnalytics",
}
mssparkutils.fs.put(CONTROL, json.dumps(wm, indent=2), True)
mssparkutils.fs.put(f"{GOLD_FILES}/_control/ga4_gold_summary.json", json.dumps(wm, indent=2), True)
print("DONE", json.dumps({"after": after}, indent=2))
mssparkutils.notebook.exit(json.dumps({"status": "success", "after": after}))
