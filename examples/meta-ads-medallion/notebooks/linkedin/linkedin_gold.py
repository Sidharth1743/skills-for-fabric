# Fabric notebook source: linkedin_gold
# Silver -> Gold star schema for LinkedIn Ads under Development

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from notebookutils import mssparkutils
from datetime import datetime, timezone
import json

WS = "718e8176-5d40-4a9c-88ff-50ac97ac49ba"
LH = "981fbe98-2f01-41d8-bf2f-a85e5cd9e2a2"
SILVER_ROOT = "Files/Development/Silver/Linkedin"
GOLD_FILES = "Files/Development/Gold/Linkedin"
SCHEMA = "Gold"
CONTROL = f"{GOLD_FILES}/_control/linkedin_gold_watermark.json"
SILVER_WM = f"{SILVER_ROOT}/_control/linkedin_silver_watermark.json"

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
    path = f"{GOLD_FILES}/tables/{name}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(path)
    cnt = spark.table(full).count()
    print("[VW]", full, cnt)
    return {"table": full, "count": cnt}

def export_table(table):
    full = f"{SCHEMA}.{table}"
    if not spark.catalog.tableExists(full):
        return
    path = f"{GOLD_FILES}/tables/{table}"
    spark.table(full).write.format("delta").mode("overwrite").option("overwriteSchema", True).save(path)

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
groups = read_silver("linkedin_campaign_groups")
if groups is not None:
    dim_g = (
        groups.select(
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"),
            F.coalesce(F.col("campaign_group_id"), F.col("entity_id")).alias("campaign_group_id"),
            F.col("campaign_group_name"), F.col("status"),
            F.col("sponsored_account_id"), F.col("total_budget"), F.col("budget_currency"),
            F.col("is_test"), F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy("tenant_id", "account_id", "campaign_group_id")
            .orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["dim_linkedin_campaign_group"] = merge_table(
        dim_g.drop("ingestion_time"), "dim_linkedin_campaign_group",
        ["tenant_id", "account_id", "campaign_group_id"],
    )

camps = read_silver("linkedin_campaigns")
if camps is not None:
    dim_c = (
        camps.select(
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"),
            F.coalesce(F.col("campaign_id"), F.col("entity_id")).alias("campaign_id"),
            F.col("campaign_name"), F.col("status"), F.col("campaign_type"),
            F.col("cost_type"), F.col("objective_type"),
            F.col("campaign_group_id"), F.col("sponsored_account_id"),
            F.col("daily_budget"), F.col("budget_currency"),
            F.col("unit_cost"), F.col("unit_cost_currency"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy("tenant_id", "account_id", "campaign_id")
            .orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["dim_linkedin_campaign"] = merge_table(
        dim_c.drop("ingestion_time"), "dim_linkedin_campaign",
        ["tenant_id", "account_id", "campaign_id"],
    )

ads = read_silver("linkedin_ads")
if ads is not None:
    dim_a = (
        ads.select(
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"),
            F.coalesce(F.col("ad_id"), F.col("entity_id")).alias("ad_id"),
            F.col("ad_name"), F.col("status"), F.col("ad_type"), F.col("ad_format"),
            F.col("campaign_id"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy("tenant_id", "account_id", "ad_id")
            .orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["dim_linkedin_ad"] = merge_table(
        dim_a.drop("ingestion_time"), "dim_linkedin_ad",
        ["tenant_id", "account_id", "ad_id"],
    )

creatives = read_silver("linkedin_creatives")
if creatives is not None:
    dim_cr = (
        creatives.select(
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"),
            F.coalesce(F.col("creative_id"), F.col("entity_id")).alias("creative_id"),
            F.col("status"), F.col("creative_type"),
            F.col("campaign_id"), F.col("ad_id"),
            F.col("headline"), F.col("description"), F.col("landing_page"),
            F.col("review_status"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy("tenant_id", "account_id", "creative_id")
            .orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["dim_linkedin_creative"] = merge_table(
        dim_cr.drop("ingestion_time"), "dim_linkedin_creative",
        ["tenant_id", "account_id", "creative_id"],
    )

# ===================== FACTS =====================
insights = read_silver("linkedin_campaign_insights")
if insights is not None:
    fact = (
        insights.filter(F.col("full_date").isNotNull())
        .select(
            F.lit("linkedin").alias("platform"),
            F.col("tenant_id"), F.col("account_id"), F.col("account_name"),
            F.col("campaign_id"), F.col("full_date"),
            F.col("impressions"), F.col("clicks"), F.col("spend"),
            F.col("conversions"), F.col("one_click_leads"),
            F.col("ctr"), F.col("cpc"), F.col("cpm"),
            F.col("extraction_start_date"), F.col("extraction_end_date"),
            F.col("batch_id"), F.col("ingestion_time"),
            F.current_timestamp().alias("gold_processed_at"),
        )
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy("tenant_id", "account_id", "campaign_id", "full_date")
            .orderBy(F.col("ingestion_time").desc_nulls_last())
        )).filter(F.col("_rk") == 1).drop("_rk")
    )
    results["fact_linkedin_campaign_insights_daily"] = merge_table(
        fact.drop("ingestion_time"), "fact_linkedin_campaign_insights_daily",
        ["tenant_id", "account_id", "campaign_id", "full_date"],
    )

# ===================== CONSUMER VIEW =====================
# Naming aligned with vw_meta_ad_performance / vw_google_ad_performance
if spark.catalog.tableExists(f"{SCHEMA}.fact_linkedin_campaign_insights_daily"):
    perf = spark.table(f"{SCHEMA}.fact_linkedin_campaign_insights_daily").alias("f")
    if spark.catalog.tableExists(f"{SCHEMA}.dim_linkedin_campaign"):
        dc = spark.table(f"{SCHEMA}.dim_linkedin_campaign").select(
            "tenant_id", "account_id", "campaign_id",
            "campaign_name", "status", "campaign_type", "cost_type", "objective_type",
            "campaign_group_id", "daily_budget", "budget_currency",
        ).alias("c")
        perf = perf.join(dc, ["tenant_id", "account_id", "campaign_id"], "left")
    else:
        for c in ["campaign_name", "status", "campaign_type", "cost_type", "objective_type",
                  "campaign_group_id", "daily_budget", "budget_currency"]:
            perf = perf.withColumn(c, F.lit(None).cast("string" if c not in ("daily_budget",) else "double"))

    if spark.catalog.tableExists(f"{SCHEMA}.dim_linkedin_campaign_group"):
        dg = spark.table(f"{SCHEMA}.dim_linkedin_campaign_group").select(
            "tenant_id", "account_id", "campaign_group_id",
            F.col("campaign_group_name"), F.col("status").alias("campaign_group_status"),
            F.col("total_budget"),
        )
        perf = perf.join(dg, ["tenant_id", "account_id", "campaign_group_id"], "left")
    else:
        perf = perf.withColumn("campaign_group_name", F.lit(None).cast("string")) \
                   .withColumn("campaign_group_status", F.lit(None).cast("string")) \
                   .withColumn("total_budget", F.lit(None).cast("double"))

    # Optional ad/creative rollup attributes (first ad per campaign)
    if spark.catalog.tableExists(f"{SCHEMA}.dim_linkedin_ad"):
        da = (
            spark.table(f"{SCHEMA}.dim_linkedin_ad")
            .withColumn("_rk", F.row_number().over(
                Window.partitionBy("tenant_id", "account_id", "campaign_id").orderBy(F.col("ad_id"))
            )).filter(F.col("_rk") == 1).drop("_rk")
            .select(
                "tenant_id", "account_id", "campaign_id",
                F.col("ad_id"), F.col("ad_name"), F.col("ad_type"), F.col("ad_format"),
            )
        )
        perf = perf.join(da, ["tenant_id", "account_id", "campaign_id"], "left")
    else:
        for c in ["ad_id", "ad_name", "ad_type", "ad_format"]:
            perf = perf.withColumn(c, F.lit(None).cast("string"))

    if spark.catalog.tableExists(f"{SCHEMA}.dim_linkedin_creative"):
        dcr = (
            spark.table(f"{SCHEMA}.dim_linkedin_creative")
            .withColumn("_rk", F.row_number().over(
                Window.partitionBy("tenant_id", "account_id", "campaign_id").orderBy(F.col("creative_id"))
            )).filter(F.col("_rk") == 1).drop("_rk")
            .select(
                "tenant_id", "account_id", "campaign_id",
                F.col("creative_id"), F.col("headline"), F.col("landing_page"), F.col("creative_type"),
            )
        )
        perf = perf.join(dcr, ["tenant_id", "account_id", "campaign_id"], "left")
    else:
        for c in ["creative_id", "headline", "landing_page", "creative_type"]:
            perf = perf.withColumn(c, F.lit(None).cast("string"))

    # Metric null -> 0
    for c, typ in [
        ("impressions", "long"), ("clicks", "long"), ("spend", "double"),
        ("conversions", "long"), ("one_click_leads", "long"),
        ("ctr", "double"), ("cpc", "double"), ("cpm", "double"),
    ]:
        if c in perf.columns:
            zero = F.lit(0).cast(typ) if typ == "long" else F.lit(0.0)
            perf = perf.withColumn(c, F.coalesce(F.col(c), zero))

    perf = (
        perf.withColumn("report_date", F.col("full_date"))
            .withColumn("year", F.year("full_date"))
            .withColumn("month", F.month("full_date"))
            .withColumn("month_name", F.date_format("full_date", "MMMM"))
            .withColumn("day_name", F.date_format("full_date", "EEEE"))
    )

    # Rebuild report + consumer view
    rpt = f"{SCHEMA}.rpt_linkedin_ad_performance"
    if spark.catalog.tableExists(rpt):
        spark.sql(f"DROP TABLE IF EXISTS {rpt}")
    perf.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(rpt)
    results["rpt_linkedin_ad_performance"] = {"table": rpt, "count": spark.table(rpt).count(), "overwritten": True}
    print("[CREATE]", rpt, results["rpt_linkedin_ad_performance"]["count"])

    view_df = spark.table(rpt)
    results["vw_linkedin_ad_performance"] = mat_view("vw_linkedin_ad_performance", view_df)

for t in [
    "dim_linkedin_campaign_group", "dim_linkedin_campaign", "dim_linkedin_ad", "dim_linkedin_creative",
    "fact_linkedin_campaign_insights_daily",
    "rpt_linkedin_ad_performance", "vw_linkedin_ad_performance",
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
    "note": "LinkedIn dims/facts/views written to Development Gold schema + Files/Development/Gold/Linkedin",
}
mssparkutils.fs.put(CONTROL, json.dumps(wm, indent=2), True)
mssparkutils.fs.put(f"{GOLD_FILES}/_control/linkedin_gold_summary.json", json.dumps(wm, indent=2), True)
print("DONE", json.dumps({"after": after}, indent=2))
mssparkutils.notebook.exit(json.dumps({"status": "success", "after": after}))
