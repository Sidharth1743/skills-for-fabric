"""
Incremental Delta helpers for Meta/Google Ads medallion notebooks.

Default behavior:
  - FULL_REFRESH=False → MERGE (upsert) new/changed rows; keep existing history
  - Date-based facts use watermark = max(target.date) - LOOKBACK_DAYS
  - Safety: if incremental batch spans > INCREMENTAL_MAX_DAYS, abort
    (prevents accidentally loading a full year when you meant a 2-day append)

Set FULL_REFRESH=True only for intentional full rebuilds.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from delta.tables import DeltaTable


def table_exists(spark, name: str) -> bool:
    try:
        spark.table(name).limit(1).collect()
        return True
    except Exception:
        return False


def path_is_delta(spark, path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


def max_date(spark, table_or_path: str, date_col: str, *, is_path: bool = False):
    try:
        df = spark.read.format("delta").load(table_or_path) if is_path else spark.table(table_or_path)
        row = df.agg(F.max(F.col(date_col)).alias("m")).collect()[0]
        return row["m"]
    except Exception:
        return None


def filter_by_watermark(
    spark,
    df: DataFrame,
    date_col: str,
    target: str,
    *,
    full_refresh: bool,
    lookback_days: int = 2,
    max_days: int = 14,
    is_path: bool = False,
) -> DataFrame:
    """Keep rows on/after (max(target.date) - lookback). Abort if span too large."""
    if full_refresh:
        print(f"[FULL_REFRESH] skip watermark filter for {target}")
        return df

    exists = path_is_delta(spark, target) if is_path else table_exists(spark, target)
    if not exists:
        print(f"[INCR] target missing → first load (full batch) for {target}")
        return df

    wm = max_date(spark, target, date_col, is_path=is_path)
    if wm is None:
        print(f"[INCR] no watermark on {target}.{date_col} → full batch")
        return df

    cutoff = F.date_sub(F.lit(wm), int(lookback_days))
    out = df.filter(F.col(date_col).isNotNull() & (F.col(date_col) >= cutoff))
    stats = out.agg(F.min(date_col).alias("mn"), F.max(date_col).alias("mx"), F.count(F.lit(1)).alias("n")).collect()[0]
    print(f"[INCR] {target} watermark={wm} lookback={lookback_days}d → cutoff, batch_rows={stats['n']} range={stats['mn']}..{stats['mx']}")

    if stats["n"] and stats["mn"] is not None and stats["mx"] is not None:
        span = (stats["mx"] - stats["mn"]).days if hasattr(stats["mx"] - stats["mn"], "days") else None
        # also compare against watermark gap
        gap = (stats["mx"] - wm).days if hasattr(stats["mx"] - wm, "days") else None
        check = span if span is not None else gap
        if check is not None and check > int(max_days):
            raise ValueError(
                f"Incremental batch for {target} spans {check} days (> INCREMENTAL_MAX_DAYS={max_days}). "
                f"This looks like a large backfill. Set FULL_REFRESH=True intentionally, "
                f"or load bronze in smaller windows. Refusing to merge."
            )
    return out


def merge_or_overwrite_table(
    spark,
    df: DataFrame,
    target: str,
    keys: list[str],
    *,
    full_refresh: bool,
    partition_cols: list[str] | None = None,
    stamp_col: str | None = "silver_processed_at",
):
    if stamp_col and stamp_col not in df.columns:
        df = df.withColumn(stamp_col, F.current_timestamp())
    df = df.dropDuplicates(keys)

    if full_refresh or not table_exists(spark, target):
        w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition_cols:
            w = w.partitionBy(*partition_cols)
        w.saveAsTable(target)
        mode = "OVERWRITE"
    else:
        cond = " AND ".join([f"t.`{k}` <=> s.`{k}`" for k in keys])
        (
            DeltaTable.forName(spark, target)
            .alias("t")
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        mode = "MERGE"

    n = spark.table(target).count()
    print(f"[OK] {target} ({mode}) total_rows={n:,}")
    return n


def merge_or_overwrite_path(
    spark,
    df: DataFrame,
    path: str,
    keys: list[str],
    *,
    full_refresh: bool,
    partition_cols: list[str] | None = None,
    stamp_col: str | None = "_silver_processed_at",
):
    if stamp_col and stamp_col not in df.columns:
        df = df.withColumn(stamp_col, F.current_timestamp())
    df = df.dropDuplicates(keys)

    if full_refresh or not path_is_delta(spark, path):
        w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("mergeSchema", "true")
        if partition_cols:
            w = w.partitionBy(*partition_cols)
        w.save(path)
        mode = "OVERWRITE"
    else:
        cond = " AND ".join([f"t.`{k}` <=> s.`{k}`" for k in keys])
        (
            DeltaTable.forPath(spark, path)
            .alias("t")
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        mode = "MERGE"

    n = spark.read.format("delta").load(path).count()
    print(f"[OK] {path} ({mode}) total_rows={n:,}")
    return n
