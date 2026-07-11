# Incremental Silver / Gold refresh

## Default behavior

| Setting | Default | Meaning |
|---------|---------|---------|
| `FULL_REFRESH` | `False` | Keep existing Silver/Gold history; **MERGE** new rows |
| `INCREMENTAL_LOOKBACK_DAYS` | `2` | Re-process last 2 days + anything newer (late arrivals) |
| `INCREMENTAL_MAX_DAYS` | `14` | **Safety gate** — refuse batches spanning > 14 days |

After a normal 2-day bronze extract, re-running Silver then Gold **appends/upserts** only new (and last-2-day) dates. Older months already in Silver/Gold are **not** wiped.

## Do **not** run a one-year bronze backfill with `FULL_REFRESH=False` unless you intend to merge that whole span

If bronze suddenly contains ~365 new days beyond the watermark, the safety gate raises an error. Options:

1. Keep loading daily/2-day extracts with incremental (recommended), or  
2. Set `FULL_REFRESH = True` only when you intentionally rebuild everything from Bronze.

## Notebooks updated

- `02_silver_transform.ipynb` — Meta Silver MERGE
- `03_gold_star_schema.ipynb` — Meta Gold MERGE
- `04_google_ads_silver.ipynb` — Google Silver (entities + daily MERGE + watermark)
- `05_google_ads_gold.ipynb` — Google Gold MERGE
- `06_rebuild_unified_both_platforms.ipynb` — unified rpt MERGE
- `07_enrich_priority_age_geo.ipynb` — enrich path MERGE
- `12_materialize_views_as_delta.ipynb` — rebuilds `vw_*` rollups from **current full Gold** (derived tables; safe)

## Typical 2-day run order

1. Land new Bronze (2 days)
2. Silver notebooks (`02` / `04` or enrich silver in `07`)
3. Gold / unified (`03` / `05` / `06` / `07`)
4. `12_materialize_views_as_delta.ipynb`
5. Optional: staging refresh + SQL endpoint metadata refresh
