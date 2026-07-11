#!/usr/bin/env python3
"""Bronze → Silver transform for Meta Ads connector extracts (local / CI validation).

Reads bronze-style CSVs (envelope + raw_json), writes cleaned silver tables as
CSV + Parquet under --output-dir. Mirrors the Fabric notebook logic so local
validation matches lakehouse silver schemas.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


ENVELOPE_COLS = [
    "connector_id",
    "tenant_id",
    "account_id",
    "account_name",
    "platform",
    "entity_type",
    "entity_id",
    "parent_entity_id",
    "batch_id",
    "ingestion_time",
    "extraction_start_date",
    "extraction_end_date",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def parse_raw(series: pd.Series) -> pd.Series:
    def _loads(v: Any) -> dict:
        if isinstance(v, dict):
            return v
        if pd.isna(v) or v == "":
            return {}
        return json.loads(v)

    return series.map(_loads)


def to_float(v: Any) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)) or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def cents_to_amount(v: Any) -> float | None:
    n = to_float(v)
    return None if n is None else n / 100.0


def first_non_null(*vals: Any) -> Any:
    for v in vals:
        if v is None:
            continue
        if isinstance(v, float) and pd.isna(v):
            continue
        if v == "":
            continue
        return v
    return None


def load_bronze(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = [c for c in ENVELOPE_COLS if c not in df.columns and c != "parent_entity_id"]
    # parent_entity_id may be absent in some extracts
    for c in ENVELOPE_COLS:
        if c not in df.columns:
            df[c] = ""
    if "raw_json" not in df.columns:
        raise ValueError(f"{path.name}: missing raw_json column")
    df["_raw"] = parse_raw(df["raw_json"])
    df["ingestion_time"] = pd.to_datetime(df["ingestion_time"], utc=True, errors="coerce")
    return df


def dedup_latest(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.sort_values("ingestion_time", ascending=False, na_position="last")
    return out.drop_duplicates(subset=keys, keep="first").reset_index(drop=True)


def transform_campaigns(bronze: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in bronze.iterrows():
        j = r["_raw"]
        campaign_id = first_non_null(j.get("id"), r["entity_id"])
        if not campaign_id:
            continue
        cats = j.get("special_ad_categories") or []
        rows.append(
            {
                "campaign_id": str(campaign_id),
                "campaign_name": j.get("name"),
                "meta_account_id": j.get("account_id"),
                "objective": j.get("objective"),
                "status": j.get("status"),
                "configured_status": j.get("configured_status"),
                "effective_status": j.get("effective_status"),
                "buying_type": j.get("buying_type"),
                "bid_strategy": j.get("bid_strategy"),
                "daily_budget_amount": cents_to_amount(j.get("daily_budget")),
                "lifetime_budget_amount": cents_to_amount(j.get("lifetime_budget")),
                "budget_remaining_amount": cents_to_amount(j.get("budget_remaining")),
                "special_ad_categories": json.dumps(cats),
                "created_time": j.get("created_time"),
                "updated_time": j.get("updated_time"),
                "start_time": j.get("start_time"),
                "stop_time": j.get("stop_time"),
                "connector_id": r["connector_id"],
                "tenant_id": r["tenant_id"],
                "account_id": r["account_id"],
                "account_name": r["account_name"],
                "platform": r["platform"],
                "source_batch_id": r["batch_id"],
                "ingestion_time": r["ingestion_time"],
                "extraction_start_date": r["extraction_start_date"],
                "extraction_end_date": r["extraction_end_date"],
            }
        )
    df = pd.DataFrame(rows)
    df = dedup_latest(df, ["campaign_id"])
    df["silver_processed_at"] = utc_now()
    return df


def _targeting_summary(targeting: Any) -> dict[str, Any]:
    if not isinstance(targeting, dict):
        return {
            "age_min": None,
            "age_max": None,
            "genders": None,
            "geo_countries": None,
            "targeting_json": None,
        }
    geos = targeting.get("geo_locations") or {}
    countries = geos.get("countries") or []
    return {
        "age_min": targeting.get("age_min"),
        "age_max": targeting.get("age_max"),
        "genders": json.dumps(targeting.get("genders")) if targeting.get("genders") is not None else None,
        "geo_countries": json.dumps(countries) if countries else None,
        "targeting_json": json.dumps(targeting),
    }


def transform_adsets(bronze: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in bronze.iterrows():
        j = r["_raw"]
        adset_id = first_non_null(j.get("id"), r["entity_id"])
        campaign_id = first_non_null(j.get("campaign_id"), r["parent_entity_id"])
        if not adset_id or not campaign_id:
            continue
        t = _targeting_summary(j.get("targeting"))
        rows.append(
            {
                "adset_id": str(adset_id),
                "campaign_id": str(campaign_id),
                "adset_name": j.get("name"),
                "status": j.get("status"),
                "optimization_goal": j.get("optimization_goal"),
                "billing_event": j.get("billing_event"),
                "bid_strategy": j.get("bid_strategy"),
                "daily_budget_amount": cents_to_amount(j.get("daily_budget")),
                "lifetime_budget_amount": cents_to_amount(j.get("lifetime_budget")),
                "age_min": t["age_min"],
                "age_max": t["age_max"],
                "genders": t["genders"],
                "geo_countries": t["geo_countries"],
                "targeting_json": t["targeting_json"],
                "created_time": j.get("created_time"),
                "updated_time": j.get("updated_time"),
                "connector_id": r["connector_id"],
                "tenant_id": r["tenant_id"],
                "account_id": r["account_id"],
                "account_name": r["account_name"],
                "platform": r["platform"],
                "source_batch_id": r["batch_id"],
                "ingestion_time": r["ingestion_time"],
                "extraction_start_date": r["extraction_start_date"],
                "extraction_end_date": r["extraction_end_date"],
            }
        )
    df = pd.DataFrame(rows)
    df = dedup_latest(df, ["adset_id"])
    df["silver_processed_at"] = utc_now()
    return df


def transform_ads(bronze: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in bronze.iterrows():
        j = r["_raw"]
        ad_id = first_non_null(j.get("id"), r["entity_id"])
        adset_id = first_non_null(j.get("adset_id"), r["parent_entity_id"])
        campaign_id = j.get("campaign_id")
        if not ad_id or not adset_id:
            continue
        creative = j.get("creative") if isinstance(j.get("creative"), dict) else {}
        tracking = j.get("tracking_specs")
        rows.append(
            {
                "ad_id": str(ad_id),
                "adset_id": str(adset_id),
                "campaign_id": str(campaign_id) if campaign_id else None,
                "ad_name": j.get("name"),
                "status": j.get("status"),
                "effective_status": j.get("effective_status"),
                "creative_id": creative.get("id"),
                "creative_thumbnail_url": creative.get("thumbnail_url"),
                "preview_shareable_link": j.get("preview_shareable_link"),
                "tracking_specs_json": json.dumps(tracking) if tracking is not None else None,
                "created_time": j.get("created_time"),
                "updated_time": j.get("updated_time"),
                "connector_id": r["connector_id"],
                "tenant_id": r["tenant_id"],
                "account_id": r["account_id"],
                "account_name": r["account_name"],
                "platform": r["platform"],
                "source_batch_id": r["batch_id"],
                "ingestion_time": r["ingestion_time"],
                "extraction_start_date": r["extraction_start_date"],
                "extraction_end_date": r["extraction_end_date"],
            }
        )
    df = pd.DataFrame(rows)
    df = dedup_latest(df, ["ad_id"])
    df["silver_processed_at"] = utc_now()
    return df


def _insight_metrics(j: dict) -> dict[str, Any]:
    return {
        "impressions": to_float(j.get("impressions")),
        "reach": to_float(j.get("reach")),
        "frequency": to_float(j.get("frequency")),
        "clicks": to_float(j.get("clicks")),
        "unique_clicks": to_float(j.get("unique_clicks")),
        "inline_link_clicks": to_float(j.get("inline_link_clicks")),
        "spend": to_float(j.get("spend")),
        "cpc": to_float(j.get("cpc")),
        "cpm": to_float(j.get("cpm")),
        "cpp": to_float(j.get("cpp")),
        "ctr": to_float(j.get("ctr")),
        "unique_ctr": to_float(j.get("unique_ctr")),
    }


def transform_adset_insights(bronze: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in bronze.iterrows():
        j = r["_raw"]
        adset_id = first_non_null(j.get("adset_id"), r["entity_id"])
        date_start = j.get("date_start")
        if not adset_id or not date_start:
            continue
        row = {
            "adset_id": str(adset_id),
            "campaign_id": str(j.get("campaign_id")) if j.get("campaign_id") else None,
            "date_start": date_start,
            "date_stop": j.get("date_stop"),
            **_insight_metrics(j),
            "connector_id": r["connector_id"],
            "tenant_id": r["tenant_id"],
            "account_id": r["account_id"],
            "account_name": r["account_name"],
            "platform": r["platform"],
            "source_batch_id": r["batch_id"],
            "ingestion_time": r["ingestion_time"],
            "extraction_start_date": r["extraction_start_date"],
            "extraction_end_date": r["extraction_end_date"],
        }
        rows.append(row)
    df = pd.DataFrame(rows)
    df = dedup_latest(df, ["adset_id", "date_start"])
    df["silver_processed_at"] = utc_now()
    return df


def transform_ad_insights(bronze: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in bronze.iterrows():
        j = r["_raw"]
        ad_id = first_non_null(j.get("ad_id"), r["entity_id"])
        date_start = j.get("date_start")
        if not ad_id or not date_start:
            continue
        row = {
            "ad_id": str(ad_id),
            "ad_name": j.get("ad_name"),
            "adset_id": str(j.get("adset_id")) if j.get("adset_id") else None,
            "adset_name": j.get("adset_name"),
            "campaign_id": str(j.get("campaign_id")) if j.get("campaign_id") else None,
            "campaign_name": j.get("campaign_name"),
            "date_start": date_start,
            "date_stop": j.get("date_stop"),
            **_insight_metrics(j),
            "connector_id": r["connector_id"],
            "tenant_id": r["tenant_id"],
            "account_id": r["account_id"],
            "account_name": r["account_name"],
            "platform": r["platform"],
            "source_batch_id": r["batch_id"],
            "ingestion_time": r["ingestion_time"],
            "extraction_start_date": r["extraction_start_date"],
            "extraction_end_date": r["extraction_end_date"],
        }
        rows.append(row)
    df = pd.DataFrame(rows)
    df = dedup_latest(df, ["ad_id", "date_start"])
    df["silver_processed_at"] = utc_now()
    return df


def explode_actions(bronze: pd.DataFrame, entity_type: str, id_field: str) -> pd.DataFrame:
    rows = []
    for _, r in bronze.iterrows():
        j = r["_raw"]
        entity_id = first_non_null(j.get(id_field), r["entity_id"])
        date_start = j.get("date_start")
        if not entity_id or not date_start:
            continue
        actions = j.get("actions") or []
        if not isinstance(actions, list):
            continue
        for a in actions:
            if not isinstance(a, dict):
                continue
            action_type = a.get("action_type")
            if not action_type:
                continue
            rows.append(
                {
                    "entity_type": entity_type,
                    "entity_id": str(entity_id),
                    "campaign_id": str(j.get("campaign_id")) if j.get("campaign_id") else None,
                    "adset_id": str(j.get("adset_id")) if j.get("adset_id") else None,
                    "ad_id": str(j.get("ad_id")) if j.get("ad_id") else None,
                    "date_start": date_start,
                    "date_stop": j.get("date_stop"),
                    "action_type": action_type,
                    "action_value": to_float(a.get("value")),
                    "connector_id": r["connector_id"],
                    "tenant_id": r["tenant_id"],
                    "account_id": r["account_id"],
                    "account_name": r["account_name"],
                    "platform": r["platform"],
                    "source_batch_id": r["batch_id"],
                    "ingestion_time": r["ingestion_time"],
                }
            )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = dedup_latest(df, ["entity_type", "entity_id", "date_start", "action_type"])
    df["silver_processed_at"] = utc_now()
    return df


def transform_path_verify(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df["silver_processed_at"] = utc_now()
    return df


def write_table(df: pd.DataFrame, out_dir: Path, name: str) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"{name}.csv"
    parquet_path = out_dir / f"{name}.parquet"
    df.to_csv(csv_path, index=False)
    try:
        df.to_parquet(parquet_path, index=False)
    except Exception as exc:  # pragma: no cover - optional engine issues
        parquet_path = None
        print(f"WARN: parquet write skipped for {name}: {exc}")
    return {
        "table": name,
        "rows": len(df),
        "cols": len(df.columns),
        "csv": str(csv_path),
        "parquet": str(parquet_path) if parquet_path else None,
    }


def resolve_inputs(input_dir: Path) -> dict[str, Path]:
    """Map logical names to files; accept either canonical or uploaded filenames."""
    aliases = {
        "campaigns": ["meta_campaigns_latest.csv", "meta_campaigns_latest_eddd.csv"],
        "adsets": ["meta_adsets_latest.csv", "meta_adsets_latest_b79f.csv"],
        "ads": ["meta_ads_latest.csv", "meta_ads_latest__1__63d9.csv"],
        "adset_insights": [
            "meta_adset_insights_latest.csv",
            "meta_adset_insights_latest_a6b9.csv",
        ],
        "ad_insights": [
            "meta_ad_insights_latest.csv",
            "meta_ad_insights_latest_fdb9.csv",
        ],
        "path_verify": [
            "meta_path_verify2_latest.csv",
            "meta_path_verify2_latest_4663.csv",
        ],
    }
    found: dict[str, Path] = {}
    for key, names in aliases.items():
        for name in names:
            p = input_dir / name
            if p.exists():
                found[key] = p
                break
        if key not in found:
            # fuzzy: startswith prefix
            prefix = names[0].replace(".csv", "")
            matches = sorted(input_dir.glob(f"{prefix}*.csv"))
            if matches:
                found[key] = matches[0]
    return found


def main() -> None:
    parser = argparse.ArgumentParser(description="Meta Ads Bronze → Silver local transform")
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Directory containing bronze Meta CSV extracts",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory for silver CSV/Parquet outputs",
    )
    args = parser.parse_args()

    inputs = resolve_inputs(args.input_dir)
    required = ["campaigns", "adsets", "ads", "adset_insights", "ad_insights"]
    missing = [k for k in required if k not in inputs]
    if missing:
        raise SystemExit(f"Missing bronze inputs for: {missing}. Found: {list(inputs)}")

    print("Resolved inputs:")
    for k, p in inputs.items():
        print(f"  {k}: {p.name}")

    campaigns_b = load_bronze(inputs["campaigns"])
    adsets_b = load_bronze(inputs["adsets"])
    ads_b = load_bronze(inputs["ads"])
    adset_ins_b = load_bronze(inputs["adset_insights"])
    ad_ins_b = load_bronze(inputs["ad_insights"])

    silver = {
        "meta_campaigns": transform_campaigns(campaigns_b),
        "meta_adsets": transform_adsets(adsets_b),
        "meta_ads": transform_ads(ads_b),
        "meta_adset_insights_daily": transform_adset_insights(adset_ins_b),
        "meta_ad_insights_daily": transform_ad_insights(ad_ins_b),
        "meta_insight_actions": pd.concat(
            [
                explode_actions(adset_ins_b, "adset", "adset_id"),
                explode_actions(ad_ins_b, "ad", "ad_id"),
            ],
            ignore_index=True,
        ),
    }
    if "path_verify" in inputs:
        silver["meta_path_verify"] = transform_path_verify(inputs["path_verify"])

    # Referential quality checks
    camp_ids = set(silver["meta_campaigns"]["campaign_id"])
    adset_ids = set(silver["meta_adsets"]["adset_id"])
    orphan_adsets = silver["meta_adsets"][~silver["meta_adsets"]["campaign_id"].isin(camp_ids)]
    orphan_ads = silver["meta_ads"][~silver["meta_ads"]["adset_id"].isin(adset_ids)]

    summaries = []
    for name, df in silver.items():
        summaries.append(write_table(df, args.output_dir, name))

    print("\nSilver write summary:")
    for s in summaries:
        print(f"  {s['table']}: rows={s['rows']:,} cols={s['cols']}")

    print("\nQuality checks:")
    print(f"  orphan adsets (campaign missing): {len(orphan_adsets)}")
    print(f"  orphan ads (adset missing): {len(orphan_ads)}")
    spend = silver["meta_ad_insights_daily"]["spend"].fillna(0).sum()
    leads = silver["meta_insight_actions"]
    lead_sum = leads.loc[
        (leads["entity_type"] == "ad") & (leads["action_type"] == "lead"), "action_value"
    ].fillna(0).sum()
    print(f"  ad insight spend total: {spend:,.2f}")
    print(f"  ad lead actions total: {lead_sum:,.0f}")
    print(f"\nOutputs written to: {args.output_dir}")


if __name__ == "__main__":
    main()
