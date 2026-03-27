"""
Brand-agnostic SKU lifecycle stage derivation.

Thin wrapper around the HOKA-specific logic in lifecycle.py,
parameterized by brand name. Reads from data/raw/{brand}/ and
writes to data/processed/{brand}/.

Since the `temporada` field is empty, we derive product lifecycle stages
from sales patterns:

1. LAUNCH:     First 4 weeks of sales (introduction period)
2. GROWTH:     Sales trending upward vs. rolling average
3. PEAK:       Highest velocity period
4. STEADY:     Stable sales around the mean
5. DECLINE:    Sales trending downward, velocity below historical average
6. CLEARANCE:  Deep in decline, high discount rates, end of life

Also computes:
- Lifecycle position (0.0 = launch, 1.0 = clearance) as continuous feature
- Estimated weeks remaining (based on velocity decay rate)
- Season assignment via sales pattern clustering
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent

LIFECYCLE_STAGES = ["launch", "growth", "peak", "steady", "decline", "clearance"]


def _raw_dir(brand: str) -> Path:
    return PROJECT_ROOT / "data" / "raw" / brand.lower()


def _processed_dir(brand: str) -> Path:
    return PROJECT_ROOT / "data" / "processed" / brand.lower()


def compute_parent_weekly_sales(txn, brand: str):
    """Aggregate sales to parent SKU-store-week level."""
    raw = _raw_dir(brand)
    products = pd.read_parquet(raw / "products.parquet")
    sku_to_parent = products.set_index("material")["codigo_padre"].to_dict()

    sales = txn[txn["cantidad"] > 0].copy()
    sales["week"] = sales["fecha"].dt.to_period("W").dt.start_time
    sales["codigo_padre"] = sales["sku"].map(sku_to_parent)

    # Pre-compute discount components (avoids slow lambda inside groupby.agg)
    parent_weekly = (
        sales.groupby(["codigo_padre", "centro", "week"])
        .agg(
            units=("cantidad", "sum"),
            revenue=("precio_final", "sum"),
            _total_descuento=("descuento", "sum"),
            _total_precio_lista=("precio_lista", "sum"),
            n_sizes=("sku", "nunique"),
        )
        .reset_index()
    )
    parent_weekly["avg_discount_rate"] = (
        parent_weekly.pop("_total_descuento")
        / parent_weekly.pop("_total_precio_lista").clip(lower=1)
    )
    parent_weekly = parent_weekly.sort_values(["codigo_padre", "centro", "week"])

    return parent_weekly


def assign_lifecycle_stage(parent_weekly):
    """
    Assign lifecycle stage to each parent SKU-store-week.

    Logic:
    - LAUNCH: first 4 weeks of sales
    - GROWTH: 4w velocity > 8w velocity AND velocity > median
    - PEAK: in the top 20% velocity period for this SKU-store
    - STEADY: velocity within 0.5-1.5x of overall median
    - DECLINE: velocity < 0.5x median OR falling for 4+ weeks
    - CLEARANCE: decline + discount rate > 15%

    Vectorized implementation — fills week gaps, computes rolling metrics,
    and assigns stages without per-row Python iteration.
    """
    df = parent_weekly.sort_values(["codigo_padre", "centro", "week"]).copy()
    gk = ["codigo_padre", "centro"]

    # ── Split small groups (< 3 observations) → "launch" directly ──
    raw_counts = df.groupby(gk).size().rename("_raw_n").reset_index()
    small_pairs = raw_counts.loc[raw_counts["_raw_n"] < 3, gk]
    large_pairs = raw_counts.loc[raw_counts["_raw_n"] >= 3, gk]

    small_data = None
    if len(small_pairs) > 0:
        small_data = df.merge(small_pairs, on=gk)
        small_data["lifecycle_stage"] = "launch"
        small_data["lifecycle_position"] = 0.0
        small_data["est_weeks_remaining"] = np.nan

    if len(large_pairs) == 0:
        return small_data if small_data is not None else pd.DataFrame()

    # ── Build complete week grid (vectorized, no per-group Python loop) ──
    large_data = df.merge(large_pairs, on=gk)
    group_ranges = (
        large_data.groupby(gk)["week"].agg(["min", "max"]).reset_index()
    )
    group_ranges["_n_weeks"] = (
        (group_ranges["max"] - group_ranges["min"]).dt.days // 7 + 1
    ).astype(int)

    # Expand: repeat each group row n_weeks times, add week offsets
    grid = group_ranges.loc[
        group_ranges.index.repeat(group_ranges["_n_weeks"])
    ].reset_index(drop=True)
    grid["week"] = grid["min"] + pd.to_timedelta(
        grid.groupby(gk).cumcount() * 7, unit="D"
    )
    full_grid = grid[gk + ["week"]]

    # Left-join actual data; gap rows become NaN → filled with 0
    df_full = full_grid.merge(large_data, on=gk + ["week"], how="left")
    df_full[["units", "revenue", "avg_discount_rate"]] = (
        df_full[["units", "revenue", "avg_discount_rate"]].fillna(0)
    )
    df_full = df_full.sort_values(gk + ["week"]).reset_index(drop=True)

    # ── Rolling velocity metrics (groupby + transform) ──
    grp_units = df_full.groupby(gk)["units"]
    df_full["vel_4w"] = grp_units.transform(lambda x: x.rolling(4, min_periods=1).mean())
    df_full["vel_8w"] = grp_units.transform(lambda x: x.rolling(8, min_periods=1).mean())
    df_full["vel_expanding"] = grp_units.transform(lambda x: x.expanding().mean())

    # ── Per-group statistics via transform (no Python loop) ──
    _peak_vel = df_full.groupby(gk)["vel_4w"].transform("max")
    _median_vel = df_full.groupby(gk)["units"].transform("median").clip(lower=0.1)

    df_full["vel_ratio"] = df_full["vel_4w"] / _median_vel
    df_full["vel_trend"] = df_full["vel_4w"] / df_full["vel_8w"].clip(lower=0.01)
    df_full["week_idx"] = df_full.groupby(gk).cumcount()

    # ── Vectorized lifecycle stage (np.select = first match wins, like if/elif) ──
    wi = df_full["week_idx"].values
    dr = df_full["avg_discount_rate"].values
    vr = df_full["vel_ratio"].values
    vt = df_full["vel_trend"].values
    v4 = df_full["vel_4w"].values
    pv = _peak_vel.values

    conditions = [
        wi < 4,
        (dr > 0.15) & (vr < 0.7),
        (vr < 0.5) | ((vt < 0.7) & (wi > 8)),
        (v4 >= pv * 0.8) & (pv > 0),
        (vt > 1.1) & (vr > 0.8),
    ]
    choices = ["launch", "clearance", "decline", "peak", "growth"]
    df_full["lifecycle_stage"] = np.select(conditions, choices, default="steady")

    # ── Lifecycle position (0.0 = start, 1.0 = end) ──
    group_max_idx = df_full.groupby(gk)["week_idx"].transform("max").clip(lower=1)
    df_full["lifecycle_position"] = df_full["week_idx"] / group_max_idx

    # ── Estimated weeks remaining (fully vectorized) ──
    rev_rank = df_full.groupby(gk).cumcount(ascending=False)
    n_per_group = df_full.groupby(gk)["week_idx"].transform("count")

    last_rows = (
        df_full.loc[rev_rank == 0, gk + ["vel_4w"]]
        .rename(columns={"vel_4w": "_last_vel"}).copy()
    )
    eighth_rows = (
        df_full.loc[rev_rank == 7, gk + ["vel_4w"]]
        .rename(columns={"vel_4w": "_eighth_vel"}).copy()
    )
    n_rows = df_full.loc[rev_rank == 0, gk].copy()
    n_rows["_n"] = n_per_group.loc[rev_rank == 0].values

    est = last_rows.merge(eighth_rows, on=gk, how="left").merge(n_rows, on=gk, how="left")

    has_decay = (
        (est["_n"] > 8)
        & (est["_last_vel"] > 0)
        & (est["_eighth_vel"] > 0)
        & (est["_last_vel"] != est["_eighth_vel"])
    )
    # clip to avoid log(0) warnings — has_decay mask filters results anyway
    _safe_eighth = est["_eighth_vel"].clip(lower=1e-10).values
    decay = np.where(
        has_decay,
        np.log(est["_last_vel"].values / _safe_eighth) / 8,
        np.nan,
    )
    est["est_weeks_remaining"] = np.where(
        (decay < 0) & (est["_last_vel"].values > 0.5),
        np.maximum(0.0, np.log(0.5 / est["_last_vel"].values) / decay),
        np.nan,
    )
    est.loc[(est["_n"] > 8) & (est["_last_vel"] <= 0), "est_weeks_remaining"] = 0.0

    df_full = df_full.merge(est[gk + ["est_weeks_remaining"]], on=gk, how="left")

    # ── Combine with small groups ──
    parts = [df_full]
    if small_data is not None:
        parts.append(small_data)
    result = pd.concat(parts, ignore_index=True)

    return result


def derive_season_clusters(parent_weekly):
    """
    Cluster SKUs into seasons based on their launch month.

    Uses a simple heuristic:
    - Group by first sale month -> assign season
    - Chile retail seasons: SS (Sep-Feb), FW (Mar-Aug)
    """
    first_sale = (
        parent_weekly[parent_weekly["units"] > 0]
        .groupby("codigo_padre")["week"]
        .min()
        .rename("first_sale_week")
        .reset_index()
    )
    first_sale["first_sale_month"] = first_sale["first_sale_week"].dt.month

    # Chilean seasons (Southern Hemisphere)
    def month_to_season(month):
        if month in [9, 10, 11, 12, 1, 2]:
            return "SS"  # Spring-Summer
        else:
            return "FW"  # Fall-Winter

    first_sale["derived_season"] = first_sale["first_sale_month"].apply(month_to_season)

    # Add year
    first_sale["derived_season_year"] = first_sale["first_sale_week"].dt.year
    # Adjust: if month is Jan/Feb, season started previous year
    mask = first_sale["first_sale_month"].isin([1, 2])
    first_sale.loc[mask, "derived_season_year"] -= 1

    first_sale["derived_season_full"] = (
        first_sale["derived_season"] + first_sale["derived_season_year"].astype(str).str[-2:]
    )

    return first_sale[["codigo_padre", "first_sale_week", "derived_season", "derived_season_year",
                        "derived_season_full"]]


def build_lifecycle_for_brand(brand: str):
    """Main lifecycle feature pipeline for a given brand."""
    processed = _processed_dir(brand)
    raw = _raw_dir(brand)
    processed.mkdir(parents=True, exist_ok=True)

    print(f"[{brand}] Loading transaction data...")
    txn = pd.read_parquet(raw / "transactions.parquet")

    print(f"[{brand}] Computing parent-level weekly sales...")
    parent_weekly = compute_parent_weekly_sales(txn, brand)
    print(f"  {len(parent_weekly):,} parent-store-week rows, {parent_weekly['codigo_padre'].nunique()} parents")

    print(f"\n[{brand}] Assigning lifecycle stages...")
    lifecycle = assign_lifecycle_stage(parent_weekly)
    lifecycle.to_parquet(processed / "lifecycle_stages.parquet", index=False)

    stage_counts = lifecycle["lifecycle_stage"].value_counts()
    print(f"\n  Lifecycle stage distribution:")
    for stage in LIFECYCLE_STAGES:
        count = stage_counts.get(stage, 0)
        pct = count / len(lifecycle) * 100
        bar = "=" * int(pct / 2)
        print(f"    {stage:12s} {count:>6,} ({pct:5.1f}%) {bar}")

    print(f"\n[{brand}] Deriving season assignments...")
    seasons = derive_season_clusters(parent_weekly)
    seasons.to_parquet(processed / "derived_seasons.parquet", index=False)

    season_counts = seasons["derived_season_full"].value_counts().sort_index()
    print(f"\n  Season distribution:")
    for season, count in season_counts.items():
        print(f"    {season}: {count} parent SKUs")

    # Summary by lifecycle + season
    lifecycle_with_season = lifecycle.merge(
        seasons[["codigo_padre", "derived_season_full"]],
        on="codigo_padre", how="left"
    )

    print(f"\n  Avg velocity by lifecycle stage:")
    stage_vel = lifecycle.groupby("lifecycle_stage")["units"].mean()
    for stage in LIFECYCLE_STAGES:
        vel = stage_vel.get(stage, 0)
        print(f"    {stage:12s} {vel:.2f} units/week")

    print(f"\n  Avg discount rate by lifecycle stage:")
    stage_disc = lifecycle.groupby("lifecycle_stage")["avg_discount_rate"].mean()
    for stage in LIFECYCLE_STAGES:
        disc = stage_disc.get(stage, 0)
        print(f"    {stage:12s} {disc:.1%}")

    return lifecycle, seasons


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--brand", type=str, required=True, help="Brand name (e.g. HOKA, BOLD)")
    args = parser.parse_args()
    build_lifecycle_for_brand(args.brand)
