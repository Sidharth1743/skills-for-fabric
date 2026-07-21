# Fabric notebook source: linkedin_silver
# Bronze -> Silver for LinkedIn Ads under Development

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from notebookutils import mssparkutils
from datetime import datetime, timezone
import json

WS = "718e8176-5d40-4a9c-88ff-50ac97ac49ba"
LH = "981fbe98-2f01-41d8-bf2f-a85e5cd9e2a2"
BRONZE_ROOT = "Files/Development/Bronze/Linkedin"
SILVER_ROOT = "Files/Development/Silver/Linkedin"
CONTROL = f"{SILVER_ROOT}/_control/linkedin_silver_watermark.json"

ENTITIES = [
    "linkedin_ads",
    "linkedin_campaigns",
    "linkedin_campaign_groups",
    "linkedin_campaign_insights",
    "linkedin_creatives",
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

def urn_id(col_expr):
    # urn:li:sponsoredCampaign:600111111 -> 600111111
    return F.element_at(F.split(col_expr, ":"), -1)

def flatten_entity(entity, df):
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
        .withColumn("silver_processed_at", F.current_timestamp())
        .withColumn("silver_entity", F.lit(entity))
    )

    if entity == "linkedin_campaign_groups":
        out = base.select(
            "*",
            F.get_json_object("raw_json", "$.id").alias("campaign_group_id"),
            F.get_json_object("raw_json", "$.name").alias("campaign_group_name"),
            F.get_json_object("raw_json", "$.status").alias("status"),
            F.get_json_object("raw_json", "$.account").alias("account_urn"),
            urn_id(F.get_json_object("raw_json", "$.account")).alias("sponsored_account_id"),
            F.get_json_object("raw_json", "$.totalBudget.amount").cast("double").alias("total_budget"),
            F.get_json_object("raw_json", "$.totalBudget.currencyCode").alias("budget_currency"),
            F.get_json_object("raw_json", "$.runSchedule.start").cast("long").alias("run_start_ms"),
            F.get_json_object("raw_json", "$.runSchedule.end").cast("long").alias("run_end_ms"),
            F.get_json_object("raw_json", "$.test").alias("is_test"),
        )
    elif entity == "linkedin_campaigns":
        out = base.select(
            "*",
            F.get_json_object("raw_json", "$.id").alias("campaign_id"),
            F.get_json_object("raw_json", "$.name").alias("campaign_name"),
            F.get_json_object("raw_json", "$.status").alias("status"),
            F.get_json_object("raw_json", "$.type").alias("campaign_type"),
            F.get_json_object("raw_json", "$.costType").alias("cost_type"),
            F.get_json_object("raw_json", "$.objectiveType").alias("objective_type"),
            F.get_json_object("raw_json", "$.account").alias("account_urn"),
            urn_id(F.get_json_object("raw_json", "$.account")).alias("sponsored_account_id"),
            F.get_json_object("raw_json", "$.campaignGroup").alias("campaign_group_urn"),
            urn_id(F.get_json_object("raw_json", "$.campaignGroup")).alias("campaign_group_id"),
            F.get_json_object("raw_json", "$.dailyBudget.amount").cast("double").alias("daily_budget"),
            F.get_json_object("raw_json", "$.dailyBudget.currencyCode").alias("budget_currency"),
            F.get_json_object("raw_json", "$.unitCost.amount").cast("double").alias("unit_cost"),
            F.get_json_object("raw_json", "$.unitCost.currencyCode").alias("unit_cost_currency"),
            F.get_json_object("raw_json", "$.runSchedule.start").cast("long").alias("run_start_ms"),
            F.get_json_object("raw_json", "$.runSchedule.end").cast("long").alias("run_end_ms"),
        )
    elif entity == "linkedin_ads":
        out = base.select(
            "*",
            F.get_json_object("raw_json", "$.id").alias("ad_id"),
            F.get_json_object("raw_json", "$.name").alias("ad_name"),
            F.get_json_object("raw_json", "$.status").alias("status"),
            F.get_json_object("raw_json", "$.type").alias("ad_type"),
            F.get_json_object("raw_json", "$.format").alias("ad_format"),
            F.get_json_object("raw_json", "$.campaign").alias("campaign_urn"),
            urn_id(F.get_json_object("raw_json", "$.campaign")).alias("campaign_id"),
        )
    elif entity == "linkedin_creatives":
        out = base.select(
            "*",
            F.get_json_object("raw_json", "$.id").alias("creative_id"),
            F.get_json_object("raw_json", "$.status").alias("status"),
            F.get_json_object("raw_json", "$.type").alias("creative_type"),
            F.get_json_object("raw_json", "$.campaign").alias("campaign_urn"),
            urn_id(F.get_json_object("raw_json", "$.campaign")).alias("campaign_id"),
            F.get_json_object("raw_json", "$.ad").alias("ad_urn"),
            urn_id(F.get_json_object("raw_json", "$.ad")).alias("ad_id"),
            F.get_json_object("raw_json", "$.content.headline").alias("headline"),
            F.get_json_object("raw_json", "$.content.description").alias("description"),
            F.get_json_object("raw_json", "$.content.landingPage").alias("landing_page"),
            F.get_json_object("raw_json", "$.review.status").alias("review_status"),
        )
    elif entity == "linkedin_campaign_insights":
        start_y = F.get_json_object("raw_json", "$.dateRange.start.year")
        start_m = F.lpad(F.get_json_object("raw_json", "$.dateRange.start.month"), 2, "0")
        start_d = F.lpad(F.get_json_object("raw_json", "$.dateRange.start.day"), 2, "0")
        out = base.select(
            "*",
            F.get_json_object("raw_json", "$.pivotValue").alias("pivot_value"),
            urn_id(F.get_json_object("raw_json", "$.pivotValue")).alias("campaign_id"),
            F.to_date(F.concat_ws("-", start_y, start_m, start_d)).alias("full_date"),
            F.get_json_object("raw_json", "$.impressions").cast("long").alias("impressions"),
            F.get_json_object("raw_json", "$.clicks").cast("long").alias("clicks"),
            F.get_json_object("raw_json", "$.costInLocalCurrency").cast("double").alias("spend"),
            F.get_json_object("raw_json", "$.externalWebsiteConversions").cast("long").alias("conversions"),
            F.get_json_object("raw_json", "$.oneClickLeads").cast("long").alias("one_click_leads"),
        ).withColumn(
            "ctr",
            F.when(F.col("impressions") > 0, F.col("clicks") / F.col("impressions")).otherwise(F.lit(0.0)),
        ).withColumn(
            "cpc",
            F.when(F.col("clicks") > 0, F.col("spend") / F.col("clicks")).otherwise(F.lit(0.0)),
        ).withColumn(
            "cpm",
            F.when(F.col("impressions") > 0, F.col("spend") * F.lit(1000.0) / F.col("impressions")).otherwise(F.lit(0.0)),
        )
    else:
        out = base

    if "entity_id" in out.columns and "ingestion_time" in out.columns:
        part_cols = [c for c in ["tenant_id", "account_id", "entity_id", "full_date", "batch_id"] if c in out.columns]
        if not part_cols:
            part_cols = ["entity_id"]
        w = Window.partitionBy(*[c for c in part_cols if c != "batch_id"] or part_cols) \
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
mssparkutils.fs.mkdirs(f"{SILVER_ROOT}/_control")
curr_ptrs = {e: collect_ptrs(e) for e in ENTITIES}
changed = [e for e in ENTITIES if sorted(curr_ptrs.get(e) or []) != sorted(prev_ptrs.get(e) or [])]
if FORCE_RUN:
    changed = ENTITIES[:]

if not changed:
    msg = {
        "status": "skipped",
        "reason": "no_new_linkedin_bronze_batch_ptrs",
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "entity_ptrs": curr_ptrs,
    }
    mssparkutils.fs.put(CONTROL, json.dumps(msg, indent=2), True)
    mssparkutils.notebook.exit(json.dumps(msg))

results = {}
for entity in changed:
    paths = list_batch_csvs(entity)
    new_batches = set(curr_ptrs.get(entity) or []) - set(prev_ptrs.get(entity) or [])
    if new_batches and not FORCE_RUN:
        # prefer files whose names contain new batch ids
        filtered = [p for p in paths if any(b in p for b in new_batches)]
        if filtered:
            paths = filtered
    print(f"[LOAD] {entity} files={len(paths)} new_batches={sorted(new_batches)}")
    raw = read_csvs(paths)
    flat = flatten_entity(entity, raw)
    keys = ["tenant_id", "account_id", "entity_id", "batch_id"]
    if entity == "linkedin_campaign_insights":
        keys = ["tenant_id", "account_id", "campaign_id", "full_date", "batch_id"]
    results[entity] = merge_path(flat, f"{SILVER_ROOT}/{entity}", keys)

wm = {
    "status": "success",
    "updated_at_utc": datetime.now(timezone.utc).isoformat(),
    "mode": "incremental_merge",
    "changed_entities": changed,
    "entity_ptrs": curr_ptrs,
    "results": results,
}
mssparkutils.fs.put(CONTROL, json.dumps(wm, indent=2), True)
mssparkutils.fs.put(f"{SILVER_ROOT}/_control/linkedin_silver_summary.json", json.dumps(wm, indent=2), True)
print("DONE", json.dumps({"changed": changed, "results": results}, indent=2, default=str))
mssparkutils.notebook.exit(json.dumps({"status": "success", "changed": changed, "results": results}, default=str))
