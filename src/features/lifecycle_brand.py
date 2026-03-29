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

    Per-group processing with vectorized stage assignment (np.select).
    Processes one group at a time for constant memory usage, but avoids
    per-row Python iteration within each group.
    """
    df = parent_weekly.sort_values(["codigo_padre", "centro", "week"]).copy()
    all_groups = []

    for (parent, store), group in df.groupby(["codigo_padre", "centro"]):
        g = group.sort_values("week").reset_index(drop=True)

        if len(g) < 3:
            g["lifecycle_stage"] = "launch"
            g["lifecycle_position"] = 0.0
            g["est_weeks_remaining"] = np.nan
            all_groups.append(g)
            continue

        # Fill week gaps with zeros — but only if the group is dense enough.
        # Sparse groups (e.g. 5 sales weeks over 52-week span) create massive
        # zero-filled DataFrames for little benefit. Skip gap-filling if too sparse.
        # Also require >= 8 observed weeks so rolling(8) windows are meaningful.
        week_span = (g["week"].max() - g["week"].min()).days // 7 + 1
        density = len(g) / max(week_span, 1)
        if density >= 0.2 and len(g) >= 8:
            all_weeks = pd.date_range(g["week"].min(), g["week"].max(), freq="W-MON")
            g = g.set_index("week").reindex(all_weeks).rename_axis("week").reset_index()
            g["codigo_padre"] = parent
            g["centro"] = store
            g[["units", "revenue", "avg_discount_rate"]] = (
                g[["units", "revenue", "avg_discount_rate"]].fillna(0)
            )

        n = len(g)

        # Rolling metrics
        g["vel_4w"] = g["units"].rolling(4, min_periods=1).mean()
        g["vel_8w"] = g["units"].rolling(8, min_periods=1).mean()
        g["vel_expanding"] = g["units"].expanding().mean()

        peak_vel = g["vel_4w"].max()
        median_vel = max(g["units"].median(), 0.1)

        g["vel_ratio"] = g["vel_4w"] / median_vel
        g["vel_trend"] = g["vel_4w"] / g["vel_8w"].clip(lower=0.01)
        week_idx = np.arange(n)
        g["week_idx"] = week_idx

        # Vectorized stage assignment (np.select = first match wins, like if/elif)
        wi = week_idx
        dr = g["avg_discount_rate"].values
        vr = g["vel_ratio"].values
        vt = g["vel_trend"].values
        v4 = g["vel_4w"].values

        conditions = [
            wi < 4,
            (dr > 0.15) & (vr < 0.7),
            (vr < 0.5) | ((vt < 0.7) & (wi > 8)),
            (v4 >= peak_vel * 0.8) & (peak_vel > 0),
            (vt > 1.1) & (vr > 0.8),
        ]
        choices = ["launch", "clearance", "decline", "peak", "growth"]
        g["lifecycle_stage"] = np.select(conditions, choices, default="steady")

        # Lifecycle position (0.0 = start, 1.0 = end)
        g["lifecycle_position"] = week_idx / max(n - 1, 1)

        # Estimated weeks remaining
        est_remaining = np.nan
        if n > 8:
            recent_vel = g["vel_4w"].iloc[-1]
            if recent_vel <= 0:
                est_remaining = 0.0
            else:
                recent = g["vel_4w"].iloc[-8:].values
                if recent[0] > 0 and recent[-1] > 0 and recent[0] != recent[-1]:
                    decay_rate = np.log(recent[-1] / recent[0]) / 8
                    if decay_rate < 0 and recent_vel > 0.5:
                        est_remaining = max(0.0, np.log(0.5 / recent_vel) / decay_rate)
        g["est_weeks_remaining"] = est_remaining

        all_groups.append(g)

    return pd.concat(all_groups, ignore_index=True) if all_groups else pd.DataFrame()


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
