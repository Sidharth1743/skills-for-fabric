#!/usr/bin/env python3
"""Silver → Gold star schema for Meta Ads (local / CI validation).

Builds:
  - dim_date, dim_account, dim_campaign, dim_adset, dim_ad
  - fact_ad_performance_daily
  - rpt_meta_ad_performance_daily (single reporting table)
  - vw_meta_ad_performance.csv (materialized view equivalent for local use)

Budget rule: Silver `*_amount` columns are already paise/100 (INR).
Gold exposes them as `*_inr` without dividing again.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def write_table(df: pd.DataFrame, out_dir: Path, name: str) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"{name}.csv"
    df.to_csv(csv_path, index=False)
    try:
        df.to_parquet(out_dir / f"{name}.parquet", index=False)
    except Exception as exc:
        print(f"WARN parquet skipped for {name}: {exc}")
    return {"table": name, "rows": len(df), "cols": len(df.columns)}


def build_dim_date(dates: pd.Series) -> pd.DataFrame:
    uniq = sorted({d for d in pd.to_datetime(dates, errors="coerce").dropna().dt.normalize()})
    rows = []
    for ts in uniq:
        rows.append(
            {
                "date_key": int(ts.strftime("%Y%m%d")),
                "full_date": ts.date().isoformat(),
                "year": ts.year,
                "month": ts.month,
                "day": ts.day,
                "month_name": ts.strftime("%B"),
                "day_of_week": int(ts.dayofweek) + 1,  # Mon=1
                "day_name": ts.strftime("%A"),
                "is_weekend": int(ts.dayofweek >= 5),
            }
        )
    return pd.DataFrame(rows)


def sk_map(ids: pd.Series) -> dict[str, int]:
    uniq = sorted({str(x) for x in ids.dropna().astype(str).unique() if str(x) not in ("", "nan", "None")})
    return {v: i + 1 for i, v in enumerate(uniq)}


def pivot_ad_actions(actions: pd.DataFrame) -> pd.DataFrame:
    ad = actions[actions["entity_type"] == "ad"].copy()
    if ad.empty:
        return pd.DataFrame(columns=["ad_id", "date_start", "leads", "link_clicks", "post_engagements", "video_views"])
    wanted = {
        "lead": "leads",
        "link_click": "link_clicks",
        "post_engagement": "post_engagements",
        "video_view": "video_views",
    }
    ad = ad[ad["action_type"].isin(wanted.keys())]
    ad["metric"] = ad["action_type"].map(wanted)
    piv = (
        ad.groupby(["entity_id", "date_start", "metric"], as_index=False)["action_value"]
        .sum()
        .pivot(index=["entity_id", "date_start"], columns="metric", values="action_value")
        .reset_index()
        .rename(columns={"entity_id": "ad_id"})
    )
    for c in ["leads", "link_clicks", "post_engagements", "video_views"]:
        if c not in piv.columns:
            piv[c] = 0.0
        piv[c] = piv[c].fillna(0.0)
    return piv


def main() -> None:
    parser = argparse.ArgumentParser(description="Meta Ads Silver → Gold star schema")
    parser.add_argument("--silver-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    silver = args.silver_dir
    campaigns = pd.read_csv(silver / "meta_campaigns.csv", dtype={"campaign_id": str})
    adsets = pd.read_csv(silver / "meta_adsets.csv", dtype={"adset_id": str, "campaign_id": str})
    ads = pd.read_csv(silver / "meta_ads.csv", dtype={"ad_id": str, "adset_id": str, "campaign_id": str})
    insights = pd.read_csv(
        silver / "meta_ad_insights_daily.csv",
        dtype={"ad_id": str, "adset_id": str, "campaign_id": str},
    )
    actions = pd.read_csv(
        silver / "meta_insight_actions.csv",
        dtype={"entity_id": str, "ad_id": str, "adset_id": str, "campaign_id": str},
    )

    processed_at = utc_now()

    # --- dimensions ---
    account_ids = sk_map(campaigns["account_id"])
    dim_account = (
        campaigns[["account_id", "account_name", "platform", "tenant_id", "connector_id"]]
        .drop_duplicates("account_id")
        .assign(account_sk=lambda d: d["account_id"].map(account_ids), gold_processed_at=processed_at)
    )

    campaign_ids = sk_map(campaigns["campaign_id"])
    dim_campaign = campaigns.assign(
        campaign_sk=lambda d: d["campaign_id"].map(campaign_ids),
        account_sk=lambda d: d["account_id"].map(account_ids),
        # Silver *_amount already = paise/100 (INR)
        daily_budget_inr=lambda d: d["daily_budget_amount"],
        lifetime_budget_inr=lambda d: d["lifetime_budget_amount"],
        budget_remaining_inr=lambda d: d["budget_remaining_amount"],
        gold_processed_at=processed_at,
    )[
        [
            "campaign_sk",
            "campaign_id",
            "account_sk",
            "campaign_name",
            "objective",
            "status",
            "configured_status",
            "effective_status",
            "buying_type",
            "bid_strategy",
            "daily_budget_inr",
            "lifetime_budget_inr",
            "budget_remaining_inr",
            "start_time",
            "stop_time",
            "created_time",
            "updated_time",
            "gold_processed_at",
        ]
    ]

    adset_ids = sk_map(adsets["adset_id"])
    dim_adset = adsets.assign(
        adset_sk=lambda d: d["adset_id"].map(adset_ids),
        campaign_sk=lambda d: d["campaign_id"].map(campaign_ids),
        daily_budget_inr=lambda d: d["daily_budget_amount"],
        lifetime_budget_inr=lambda d: d["lifetime_budget_amount"],
        gold_processed_at=processed_at,
    )[
        [
            "adset_sk",
            "adset_id",
            "campaign_sk",
            "adset_name",
            "status",
            "optimization_goal",
            "billing_event",
            "bid_strategy",
            "daily_budget_inr",
            "lifetime_budget_inr",
            "age_min",
            "age_max",
            "geo_countries",
            "created_time",
            "updated_time",
            "gold_processed_at",
        ]
    ]

    ad_ids = sk_map(ads["ad_id"])
    dim_ad = ads.assign(
        ad_sk=lambda d: d["ad_id"].map(ad_ids),
        adset_sk=lambda d: d["adset_id"].map(adset_ids),
        campaign_sk=lambda d: d["campaign_id"].map(campaign_ids),
        gold_processed_at=processed_at,
    )[
        [
            "ad_sk",
            "ad_id",
            "adset_sk",
            "campaign_sk",
            "ad_name",
            "status",
            "effective_status",
            "creative_id",
            "preview_shareable_link",
            "created_time",
            "updated_time",
            "gold_processed_at",
        ]
    ]

    dim_date = build_dim_date(insights["date_start"]).assign(gold_processed_at=processed_at)

    # --- fact ---
    action_piv = pivot_ad_actions(actions)
    fact = insights.merge(action_piv, on=["ad_id", "date_start"], how="left")
    for c in ["leads", "link_clicks", "post_engagements", "video_views"]:
        fact[c] = fact[c].fillna(0.0)

    # Resolve hierarchy FKs from dim_ad (authoritative), not raw insight IDs
    ad_keys = dim_ad[["ad_id", "ad_sk", "adset_sk", "campaign_sk"]].drop_duplicates("ad_id")
    acct_by_campaign = dim_campaign[["campaign_sk", "account_sk"]].drop_duplicates("campaign_sk")

    fact["date_key"] = pd.to_datetime(fact["date_start"]).dt.strftime("%Y%m%d").astype(int)
    fact = fact.merge(ad_keys, on="ad_id", how="left")
    fact = fact.merge(acct_by_campaign, on="campaign_sk", how="left")
    # Fallback account_sk from insight account_id if campaign missing
    fact["account_sk"] = fact["account_sk"].fillna(fact["account_id"].map(account_ids))

    fact["spend_inr"] = fact["spend"]  # Meta spend already in account currency (INR)
    fact["cpl_inr"] = fact.apply(
        lambda r: (r["spend_inr"] / r["leads"]) if r["leads"] and r["leads"] > 0 else None,
        axis=1,
    )
    fact = fact.sort_values(["date_key", "ad_sk"], na_position="last").reset_index(drop=True)
    fact.insert(0, "fact_sk", range(1, len(fact) + 1))
    fact["gold_processed_at"] = processed_at

    fact_out = fact[
        [
            "fact_sk",
            "date_key",
            "account_sk",
            "campaign_sk",
            "adset_sk",
            "ad_sk",
            "impressions",
            "reach",
            "frequency",
            "clicks",
            "unique_clicks",
            "inline_link_clicks",
            "spend_inr",
            "cpc",
            "cpm",
            "ctr",
            "leads",
            "link_clicks",
            "post_engagements",
            "video_views",
            "cpl_inr",
            "source_batch_id",
            "gold_processed_at",
        ]
    ].copy()

    # Drop facts that cannot resolve dimension FKs
    before = len(fact_out)
    fact_out = fact_out.dropna(subset=["account_sk", "campaign_sk", "adset_sk", "ad_sk", "date_key"])
    dropped = before - len(fact_out)
    # Re-number fact_sk after filter
    fact_out = fact_out.reset_index(drop=True)
    fact_out["fact_sk"] = fact_out.index + 1

    # --- reporting table (single denormalized) ---
    d_date = dim_date[
        ["date_key", "full_date", "year", "month", "month_name", "day_name"]
    ]
    d_acct = dim_account[["account_sk", "account_id", "account_name", "platform"]]
    d_camp = dim_campaign[
        [
            "campaign_sk",
            "campaign_id",
            "campaign_name",
            "objective",
            "status",
            "daily_budget_inr",
            "lifetime_budget_inr",
            "budget_remaining_inr",
        ]
    ].rename(
        columns={
            "objective": "campaign_objective",
            "status": "campaign_status",
            "daily_budget_inr": "campaign_daily_budget_inr",
            "lifetime_budget_inr": "campaign_lifetime_budget_inr",
            "budget_remaining_inr": "campaign_budget_remaining_inr",
        }
    )
    d_adset = dim_adset[
        [
            "adset_sk",
            "adset_id",
            "adset_name",
            "status",
            "optimization_goal",
            "daily_budget_inr",
            "lifetime_budget_inr",
        ]
    ].rename(
        columns={
            "status": "adset_status",
            "daily_budget_inr": "adset_daily_budget_inr",
            "lifetime_budget_inr": "adset_lifetime_budget_inr",
        }
    )
    d_ad = dim_ad[
        ["ad_sk", "ad_id", "ad_name", "status", "effective_status", "creative_id"]
    ].rename(columns={"status": "ad_status", "effective_status": "ad_effective_status"})

    rpt_out = (
        fact_out.merge(d_date, on="date_key", how="left")
        .merge(d_acct, on="account_sk", how="left")
        .merge(d_camp, on="campaign_sk", how="left")
        .merge(d_adset, on="adset_sk", how="left")
        .merge(d_ad, on="ad_sk", how="left")
    )[
        [
            "full_date",
            "year",
            "month",
            "month_name",
            "day_name",
            "account_id",
            "account_name",
            "platform",
            "campaign_id",
            "campaign_name",
            "campaign_objective",
            "campaign_status",
            "campaign_daily_budget_inr",
            "campaign_lifetime_budget_inr",
            "campaign_budget_remaining_inr",
            "adset_id",
            "adset_name",
            "adset_status",
            "optimization_goal",
            "adset_daily_budget_inr",
            "adset_lifetime_budget_inr",
            "ad_id",
            "ad_name",
            "ad_status",
            "ad_effective_status",
            "creative_id",
            "impressions",
            "reach",
            "frequency",
            "clicks",
            "unique_clicks",
            "inline_link_clicks",
            "spend_inr",
            "cpc",
            "cpm",
            "ctr",
            "leads",
            "link_clicks",
            "post_engagements",
            "video_views",
            "cpl_inr",
            "source_batch_id",
            "gold_processed_at",
        ]
    ]

    # Local materialized equivalent of SQL view (same columns as reporting table)
    view_out = rpt_out.copy()

    summaries = [
        write_table(dim_date, args.output_dir, "dim_date"),
        write_table(dim_account, args.output_dir, "dim_account"),
        write_table(dim_campaign, args.output_dir, "dim_campaign"),
        write_table(dim_adset, args.output_dir, "dim_adset"),
        write_table(dim_ad, args.output_dir, "dim_ad"),
        write_table(fact_out, args.output_dir, "fact_ad_performance_daily"),
        write_table(rpt_out, args.output_dir, "rpt_meta_ad_performance_daily"),
        write_table(view_out, args.output_dir, "vw_meta_ad_performance"),
    ]

    print("Gold write summary:")
    for s in summaries:
        print(f"  {s['table']}: rows={s['rows']:,} cols={s['cols']}")
    print(f"\nFacts dropped (unresolved FK): {dropped}")
    print(f"Total spend_inr: {fact_out['spend_inr'].fillna(0).sum():,.2f}")
    print(f"Total leads: {fact_out['leads'].fillna(0).sum():,.0f}")
    print(f"Campaign budget INR sample:\n{dim_campaign[['campaign_name','daily_budget_inr','lifetime_budget_inr']].head(3)}")
    print(f"\nOutputs: {args.output_dir}")


if __name__ == "__main__":
    main()
