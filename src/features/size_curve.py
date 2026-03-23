"""
Size curve depletion analysis for HOKA products.

Without inventory data, we approximate size availability from sales patterns:
- A size that was selling and stops is likely depleted
- A parent SKU losing sizes is a leading indicator for markdown

Computes:
- Sizes actively selling per parent SKU-store-week
- Size attrition rate (how fast sizes are disappearing)
- Core size availability (sizes 7-10 for footwear)
- Time since last core size sold (early warning)
- Size fragmentation index
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
from pathlib import Path

RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).parent.parent.parent / "data" / "processed"

# Core footwear sizes (most common, drive the most volume)
CORE_SIZES = {"7", "7,5", "8", "8,5", "9", "9,5", "10", "10,5"}
EXTENDED_SIZES = {"6", "6,5", "11", "11,5", "12", "13"}


def build_size_availability(txn, products):
    """
    Track which sizes are 'available' per parent SKU-store-week.
    Proxy: a size is available if it sold in the trailing 4-week window.
    """
    sales = txn[txn["cantidad"] > 0].copy()
    sales["week"] = sales["fecha"].dt.to_period("W").dt.start_time

    sku_info = products[["material", "codigo_padre", "talla", "primera_jerarquia"]].rename(
        columns={"material": "sku"}
    ).drop_duplicates(subset=["sku"])

    sales = sales.merge(sku_info, on="sku", how="left")

    # Only footwear (sizes are meaningful)
    footwear = sales[sales["primera_jerarquia"] == "Footwear"].copy()

    # Aggregate: which sizes sold per parent-store-week
    size_sales = (
        footwear.groupby(["codigo_padre", "centro", "week", "talla"])["cantidad"]
        .sum()
        .reset_index()
    )

    # For each parent-store-week, compute rolling 4w size availability
    all_weeks = sorted(size_sales["week"].unique())
    parent_store_pairs = size_sales[["codigo_padre", "centro"]].drop_duplicates()

    results = []

    for _, (parent, store) in parent_store_pairs.iterrows():
        ps_data = size_sales[
            (size_sales["codigo_padre"] == parent) & (size_sales["centro"] == store)
        ]

        # All sizes ever sold for this parent-store
        all_sizes = set(ps_data["talla"].unique())
        total_sizes = len(all_sizes)
        core_sizes_available_total = all_sizes & CORE_SIZES
        n_core_total = len(core_sizes_available_total)

        # Get first and last week for this parent-store
        first_week = ps_data["week"].min()
        last_week = ps_data["week"].max()

        # Iterate through weeks
        active_weeks = sorted(ps_data["week"].unique())
        for i, week in enumerate(active_weeks):
            # Trailing 4-week window
            window_start = week - pd.Timedelta(weeks=3)
            window_data = ps_data[
                (ps_data["week"] >= window_start) & (ps_data["week"] <= week)
            ]

            active_sizes = set(window_data["talla"].unique())
            n_active = len(active_sizes)
            lost_sizes = all_sizes - active_sizes
            n_lost = len(lost_sizes)

            # Core size status
            core_active = active_sizes & CORE_SIZES
            core_lost = core_sizes_available_total - core_active
            n_core_active = len(core_active)
            n_core_lost = len(core_lost)

            # Size attrition rate vs peak
            peak_sizes = 0
            for j in range(max(0, i - 12), i + 1):
                w = active_weeks[j]
                w_start = w - pd.Timedelta(weeks=3)
                w_data = ps_data[(ps_data["week"] >= w_start) & (ps_data["week"] <= w)]
                peak_sizes = max(peak_sizes, w_data["talla"].nunique())

            attrition_rate = 1 - (n_active / max(peak_sizes, 1))

            # Size fragmentation: are remaining sizes contiguous or scattered?
            # Convert sizes to numeric for gap analysis
            numeric_sizes = []
            for s in active_sizes:
                try:
                    numeric_sizes.append(float(s.replace(",", ".")))
                except (ValueError, AttributeError):
                    pass

            if len(numeric_sizes) > 1:
                numeric_sizes.sort()
                gaps = [numeric_sizes[i+1] - numeric_sizes[i] for i in range(len(numeric_sizes)-1)]
                fragmentation = np.std(gaps) / max(np.mean(gaps), 0.01)
            else:
                fragmentation = 0

            results.append({
                "codigo_padre": parent,
                "centro": store,
                "week": week,
                "total_sizes_ever": total_sizes,
                "active_sizes_4w": n_active,
                "lost_sizes": n_lost,
                "size_completeness": n_active / max(total_sizes, 1),
                "peak_sizes": peak_sizes,
                "attrition_rate": attrition_rate,
                "core_sizes_total": n_core_total,
                "core_sizes_active": n_core_active,
                "core_sizes_lost": n_core_lost,
                "core_completeness": n_core_active / max(n_core_total, 1),
                "fragmentation_index": fragmentation,
            })

    return pd.DataFrame(results)


def analyze_size_markdown_relationship(size_df, lifecycle_df):
    """
    Quantify the relationship between size curve breakage and subsequent markdown.
    """
    # Merge size data with lifecycle (which has discount info)
    merged = size_df.merge(
        lifecycle_df[["codigo_padre", "centro", "week", "avg_discount_rate", "lifecycle_stage"]],
        on=["codigo_padre", "centro", "week"],
        how="inner",
    )

    # Forward-looking: what discount rate follows different attrition levels?
    merged = merged.sort_values(["codigo_padre", "centro", "week"])
    merged["future_disc_4w"] = (
        merged.groupby(["codigo_padre", "centro"])["avg_discount_rate"]
        .transform(lambda x: x.shift(-1).rolling(4, min_periods=1).mean())
    )

    return merged


def build_size_alerts(size_df, threshold_attrition=0.3, threshold_core_lost=2):
    """
    Generate markdown alerts based on size curve depletion.

    Alert conditions:
    - Attrition rate > 30% (lost more than 30% of peak sizes)
    - Lost 2+ core sizes
    - Core completeness dropped below 50%
    """
    alerts = size_df[
        (size_df["attrition_rate"] > threshold_attrition)
        | (size_df["core_sizes_lost"] >= threshold_core_lost)
        | ((size_df["core_completeness"] < 0.5) & (size_df["core_sizes_total"] > 0))
    ].copy()

    alerts["alert_reasons"] = ""
    mask_attrition = alerts["attrition_rate"] > threshold_attrition
    mask_core = alerts["core_sizes_lost"] >= threshold_core_lost
    mask_core_pct = (alerts["core_completeness"] < 0.5) & (alerts["core_sizes_total"] > 0)

    alerts.loc[mask_attrition, "alert_reasons"] += "high_attrition;"
    alerts.loc[mask_core, "alert_reasons"] += "core_sizes_lost;"
    alerts.loc[mask_core_pct, "alert_reasons"] += "low_core_completeness;"

    return alerts


def run_size_curve_analysis():
    """Main size curve analysis pipeline."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading data...")
    txn = pd.read_parquet(RAW_DIR / "hoka_transactions.parquet")
    products = pd.read_parquet(RAW_DIR / "hoka_products.parquet")

    print("Building size availability tracking...")
    size_df = build_size_availability(txn, products)
    size_df.to_parquet(PROCESSED_DIR / "size_curve_tracking.parquet", index=False)

    print(f"  {len(size_df):,} parent-store-week rows")
    print(f"  {size_df['codigo_padre'].nunique()} parent SKUs tracked")

    print(f"\n  Size curve health distribution:")
    bins = [(0, 0.25), (0.25, 0.5), (0.5, 0.75), (0.75, 1.0), (1.0, 1.01)]
    labels = ["Critical (<25%)", "Poor (25-50%)", "Fair (50-75%)", "Good (75-99%)", "Full (100%)"]
    for (lo, hi), label in zip(bins, labels):
        count = ((size_df["size_completeness"] >= lo) & (size_df["size_completeness"] < hi)).sum()
        pct = count / len(size_df) * 100
        bar = "█" * int(pct / 2)
        print(f"    {label:25s} {count:>5,} ({pct:5.1f}%) {bar}")

    print(f"\n  Core size availability (footwear with core sizes):")
    has_core = size_df[size_df["core_sizes_total"] > 0]
    if len(has_core) > 0:
        print(f"    Avg core sizes active: {has_core['core_sizes_active'].mean():.1f} / {has_core['core_sizes_total'].mean():.1f}")
        print(f"    Core completeness: {has_core['core_completeness'].mean():.1%}")
        print(f"    Parent SKUs with any core loss: {(has_core.groupby('codigo_padre')['core_sizes_lost'].max() > 0).sum()}")

    # Alerts
    print("\nGenerating size curve alerts...")
    alerts = build_size_alerts(size_df)
    alerts.to_parquet(PROCESSED_DIR / "size_curve_alerts.parquet", index=False)
    print(f"  {len(alerts):,} alert rows")
    print(f"  {alerts['codigo_padre'].nunique()} parent SKUs with alerts")

    # Relationship with markdown
    print("\nAnalyzing size-markdown relationship...")
    try:
        lifecycle = pd.read_parquet(PROCESSED_DIR / "lifecycle_stages.parquet")
        merged = analyze_size_markdown_relationship(size_df, lifecycle)

        print(f"\n  Avg future discount by attrition level:")
        merged["attrition_bucket"] = pd.cut(
            merged["attrition_rate"],
            bins=[0, 0.1, 0.2, 0.3, 0.5, 1.0],
            labels=["0-10%", "10-20%", "20-30%", "30-50%", "50%+"],
        )
        attrition_disc = merged.groupby("attrition_bucket", observed=True)["future_disc_4w"].mean()
        for bucket, disc in attrition_disc.items():
            print(f"    Attrition {bucket}: avg future discount = {disc:.1%}")

        print(f"\n  Avg future discount by core completeness:")
        merged["core_bucket"] = pd.cut(
            merged["core_completeness"],
            bins=[0, 0.25, 0.5, 0.75, 1.01],
            labels=["<25%", "25-50%", "50-75%", "75-100%"],
        )
        core_disc = merged.groupby("core_bucket", observed=True)["future_disc_4w"].mean()
        for bucket, disc in core_disc.items():
            print(f"    Core completeness {bucket}: avg future discount = {disc:.1%}")

    except FileNotFoundError:
        print("  (Lifecycle data not available — run lifecycle.py first)")

    # Most at-risk parent SKUs right now
    latest_week = size_df["week"].max()
    current = size_df[size_df["week"] == latest_week].sort_values("attrition_rate", ascending=False)
    print(f"\n  Current week ({latest_week.date()}) — Most depleted parent SKUs:")
    print(f"  {'Parent SKU':<25} {'Store':>6} {'Active':>6} {'Peak':>5} {'Attrition':>10} {'Core Active':>11}")
    print("  " + "-" * 70)
    for _, row in current.head(15).iterrows():
        print(f"  {row['codigo_padre']:<25} {row['centro']:>6} {row['active_sizes_4w']:>6.0f} "
              f"{row['peak_sizes']:>5.0f} {row['attrition_rate']:>9.0%} "
              f"{row['core_sizes_active']:>5.0f}/{row['core_sizes_total']:>3.0f}")

    return size_df, alerts


if __name__ == "__main__":
    run_size_curve_analysis()
