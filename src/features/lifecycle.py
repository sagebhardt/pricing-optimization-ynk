"""
SKU lifecycle stage derivation for HOKA products.

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

RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).parent.parent.parent / "data" / "processed"

LIFECYCLE_STAGES = ["launch", "growth", "peak", "steady", "decline", "clearance"]


def compute_parent_weekly_sales(txn):
    """Aggregate sales to parent SKU-store-week level."""
    products = pd.read_parquet(RAW_DIR / "hoka_products.parquet")
    sku_to_parent = products.set_index("material")["codigo_padre"].to_dict()

    sales = txn[txn["cantidad"] > 0].copy()
    sales["week"] = sales["fecha"].dt.to_period("W").dt.start_time
    sales["codigo_padre"] = sales["sku"].map(sku_to_parent)

    parent_weekly = (
        sales.groupby(["codigo_padre", "centro", "week"])
        .agg(
            units=("cantidad", "sum"),
            revenue=("precio_final", "sum"),
            avg_discount_rate=("descuento", lambda x: x.sum() / max(sales.loc[x.index, "precio_lista"].sum(), 1)),
            n_sizes=("sku", "nunique"),
        )
        .reset_index()
        .sort_values(["codigo_padre", "centro", "week"])
    )

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
    """
    df = parent_weekly.copy()

    lifecycle_rows = []

    for (parent, store), group in df.groupby(["codigo_padre", "centro"]):
        g = group.sort_values("week").reset_index(drop=True)

        if len(g) < 3:
            for _, row in g.iterrows():
                lifecycle_rows.append({**row.to_dict(), "lifecycle_stage": "launch", "lifecycle_position": 0.0})
            continue

        # Fill week gaps with zeros
        all_weeks = pd.date_range(g["week"].min(), g["week"].max(), freq="W-MON")
        g = g.set_index("week").reindex(all_weeks).rename_axis("week").reset_index()
        g["codigo_padre"] = parent
        g["centro"] = store
        g["units"] = g["units"].fillna(0)
        g["revenue"] = g["revenue"].fillna(0)
        g["avg_discount_rate"] = g["avg_discount_rate"].fillna(0)

        n = len(g)

        # Rolling metrics
        g["vel_4w"] = g["units"].rolling(4, min_periods=1).mean()
        g["vel_8w"] = g["units"].rolling(8, min_periods=1).mean()
        g["vel_expanding"] = g["units"].expanding().mean()

        # Peak velocity
        peak_vel = g["vel_4w"].max()
        median_vel = g["units"].median()
        if median_vel == 0:
            median_vel = 0.1  # avoid division by zero

        # Velocity ratio vs median
        g["vel_ratio"] = g["vel_4w"] / median_vel

        # Velocity trend (is it rising or falling?)
        g["vel_trend"] = g["vel_4w"] / g["vel_8w"].clip(lower=0.01)

        # Week index
        g["week_idx"] = range(n)

        stages = []
        for i, row in g.iterrows():
            week_idx = row["week_idx"]
            vel_4w = row["vel_4w"]
            vel_ratio = row["vel_ratio"]
            vel_trend = row["vel_trend"]
            disc_rate = row["avg_discount_rate"]

            if week_idx < 4:
                stage = "launch"
            elif disc_rate > 0.15 and vel_ratio < 0.7:
                stage = "clearance"
            elif vel_ratio < 0.5 or (vel_trend < 0.7 and week_idx > 8):
                stage = "decline"
            elif vel_4w >= peak_vel * 0.8 and peak_vel > 0:
                stage = "peak"
            elif vel_trend > 1.1 and vel_ratio > 0.8:
                stage = "growth"
            else:
                stage = "steady"

            stages.append(stage)

        g["lifecycle_stage"] = stages

        # Lifecycle position (0.0 = start, 1.0 = end)
        g["lifecycle_position"] = g["week_idx"] / max(n - 1, 1)

        # Estimated weeks remaining based on velocity decay
        if n > 8:
            recent_vel = g["vel_4w"].iloc[-1]
            if recent_vel > 0:
                # Simple: at current velocity, how many weeks until cumulative units plateau?
                # Use exponential decay fit on last 8 weeks
                recent = g["vel_4w"].iloc[-8:].values
                if recent[0] > 0 and recent[-1] > 0:
                    decay_rate = np.log(recent[-1] / recent[0]) / 8 if recent[0] != recent[-1] else 0
                    if decay_rate < 0:
                        # Weeks until velocity drops below 0.5 units/week
                        weeks_to_min = np.log(0.5 / recent_vel) / decay_rate if recent_vel > 0.5 else 0
                        g["est_weeks_remaining"] = max(0, weeks_to_min)
                    else:
                        g["est_weeks_remaining"] = np.nan  # Not declining
                else:
                    g["est_weeks_remaining"] = np.nan
            else:
                g["est_weeks_remaining"] = 0
        else:
            g["est_weeks_remaining"] = np.nan

        for _, row in g.iterrows():
            lifecycle_rows.append(row.to_dict())

    result = pd.DataFrame(lifecycle_rows)
    return result


def derive_season_clusters(parent_weekly):
    """
    Cluster SKUs into seasons based on their launch month.

    Uses a simple heuristic:
    - Group by first sale month → assign season
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


def build_lifecycle_features():
    """Main lifecycle feature pipeline."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading transaction data...")
    txn = pd.read_parquet(RAW_DIR / "hoka_transactions.parquet")

    print("Computing parent-level weekly sales...")
    parent_weekly = compute_parent_weekly_sales(txn)
    print(f"  {len(parent_weekly):,} parent-store-week rows, {parent_weekly['codigo_padre'].nunique()} parents")

    print("\nAssigning lifecycle stages...")
    lifecycle = assign_lifecycle_stage(parent_weekly)
    lifecycle.to_parquet(PROCESSED_DIR / "lifecycle_stages.parquet", index=False)

    stage_counts = lifecycle["lifecycle_stage"].value_counts()
    print(f"\n  Lifecycle stage distribution:")
    for stage in LIFECYCLE_STAGES:
        count = stage_counts.get(stage, 0)
        pct = count / len(lifecycle) * 100
        bar = "█" * int(pct / 2)
        print(f"    {stage:12s} {count:>6,} ({pct:5.1f}%) {bar}")

    print("\nDeriving season assignments...")
    seasons = derive_season_clusters(parent_weekly)
    seasons.to_parquet(PROCESSED_DIR / "derived_seasons.parquet", index=False)

    season_counts = seasons["derived_season_full"].value_counts().sort_index()
    print(f"\n  Season distribution:")
    for season, count in season_counts.items():
        print(f"    {season}: {count} parent SKUs")

    # Summary by lifecycle + season
    lifecycle_with_season = lifecycle.merge(
        seasons[["codigo_padre", "derived_season_full"]],
        on="codigo_padre", how="left"
    )

    print("\n  Avg velocity by lifecycle stage:")
    stage_vel = lifecycle.groupby("lifecycle_stage")["units"].mean()
    for stage in LIFECYCLE_STAGES:
        vel = stage_vel.get(stage, 0)
        print(f"    {stage:12s} {vel:.2f} units/week")

    print("\n  Avg discount rate by lifecycle stage:")
    stage_disc = lifecycle.groupby("lifecycle_stage")["avg_discount_rate"].mean()
    for stage in LIFECYCLE_STAGES:
        disc = stage_disc.get(stage, 0)
        print(f"    {stage:12s} {disc:.1%}")

    return lifecycle, seasons


if __name__ == "__main__":
    build_lifecycle_features()
