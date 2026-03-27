"""
Brand-agnostic size curve depletion analysis.

Thin wrapper around the HOKA-specific logic in size_curve.py,
parameterized by brand name. Reads from data/raw/{brand}/ and
writes to data/processed/{brand}/.

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

PROJECT_ROOT = Path(__file__).parent.parent.parent

# Core footwear sizes (most common, drive the most volume)
CORE_SIZES = {"7", "7,5", "8", "8,5", "9", "9,5", "10", "10,5"}
EXTENDED_SIZES = {"6", "6,5", "11", "11,5", "12", "13"}


def _raw_dir(brand: str) -> Path:
    return PROJECT_ROOT / "data" / "raw" / brand.lower()


def _processed_dir(brand: str) -> Path:
    return PROJECT_ROOT / "data" / "processed" / brand.lower()


def build_size_availability_from_stock(stock, products):
    """
    Track size availability using actual inventory data.
    A size is available if stock_on_hand > 0.

    Vectorized implementation — avoids nested Python loops that OOM on
    brands with large stock tables (BOLD: 44K+ SKUs × 35 stores).
    """
    stock = stock.copy()
    stock["centro"] = stock["store_id"].str.split("-", n=1).str[0]
    stock["week"] = stock["fecha"].dt.to_period("W").dt.start_time

    sku_info = products[["material", "codigo_padre", "talla", "primera_jerarquia"]].rename(
        columns={"material": "sku"}
    ).drop_duplicates(subset=["sku"])

    stock = stock.merge(sku_info, on="sku", how="inner")

    # Only footwear
    footwear = stock[stock["primera_jerarquia"] == "Footwear"].copy()
    if len(footwear) == 0:
        return pd.DataFrame()

    # End-of-week stock snapshot per SKU-store-week-size
    footwear = footwear.sort_values("fecha")
    eow = footwear.groupby(["codigo_padre", "centro", "week", "talla"]).last().reset_index()
    eow["in_stock"] = (eow["stock_on_hand_units"] > 0).astype(int)
    del footwear  # free memory

    in_stock = eow[eow["in_stock"] == 1]

    # ── Step 1: Active sizes per parent-store-week ──
    active_counts = (
        in_stock.groupby(["codigo_padre", "centro", "week"])["talla"]
        .nunique().rename("active_sizes_4w").reset_index()
    )

    # ── Step 2: Core sizes active per parent-store-week ──
    core_in_stock = in_stock[in_stock["talla"].isin(CORE_SIZES)]
    if len(core_in_stock) > 0:
        core_counts = (
            core_in_stock.groupby(["codigo_padre", "centro", "week"])["talla"]
            .nunique().rename("core_sizes_active").reset_index()
        )
    else:
        core_counts = pd.DataFrame(columns=["codigo_padre", "centro", "week", "core_sizes_active"])

    # ── Step 3: Total sizes ever per parent-store ──
    total_ever = (
        in_stock.groupby(["codigo_padre", "centro"])["talla"]
        .nunique().rename("total_sizes_ever").reset_index()
    )

    # Core sizes ever per parent-store
    if len(core_in_stock) > 0:
        core_ever = (
            core_in_stock.groupby(["codigo_padre", "centro"])["talla"]
            .nunique().rename("core_sizes_total").reset_index()
        )
    else:
        core_ever = pd.DataFrame(columns=["codigo_padre", "centro", "core_sizes_total"])

    # ── Step 4: Assemble result ──
    result = active_counts.merge(total_ever, on=["codigo_padre", "centro"], how="left")
    result = result.merge(core_counts, on=["codigo_padre", "centro", "week"], how="left")
    result = result.merge(core_ever, on=["codigo_padre", "centro"], how="left")

    result["core_sizes_active"] = result["core_sizes_active"].fillna(0).astype(int)
    result["core_sizes_total"] = result["core_sizes_total"].fillna(0).astype(int)
    result["lost_sizes"] = result["total_sizes_ever"] - result["active_sizes_4w"]
    result["size_completeness"] = result["active_sizes_4w"] / result["total_sizes_ever"].clip(lower=1)
    result["core_sizes_lost"] = result["core_sizes_total"] - result["core_sizes_active"]
    result["core_completeness"] = np.where(
        result["core_sizes_total"] > 0,
        result["core_sizes_active"] / result["core_sizes_total"],
        0.0,
    )

    # ── Step 5: Peak sizes (rolling 12-observation max) ──
    result = result.sort_values(["codigo_padre", "centro", "week"])
    result["peak_sizes"] = (
        result.groupby(["codigo_padre", "centro"])["active_sizes_4w"]
        .transform(lambda x: x.rolling(12, min_periods=1).max())
        .astype(int)
    )
    result["attrition_rate"] = 1 - (result["active_sizes_4w"] / result["peak_sizes"].clip(lower=1))

    # ── Step 6: Fragmentation index ──
    in_stock_num = in_stock.copy()
    in_stock_num["talla_num"] = pd.to_numeric(
        in_stock_num["talla"].str.replace(",", "."), errors="coerce"
    )
    num_data = in_stock_num.dropna(subset=["talla_num"])
    del in_stock, in_stock_num  # free memory

    if len(num_data) > 0:
        def _fragmentation(sizes):
            vals = np.sort(sizes.values)
            if len(vals) < 2:
                return 0.0
            gaps = np.diff(vals)
            mean_gap = gaps.mean()
            return float(gaps.std() / max(mean_gap, 0.01)) if mean_gap > 0 else 0.0

        frag = (
            num_data.groupby(["codigo_padre", "centro", "week"])["talla_num"]
            .agg(_fragmentation).rename("fragmentation_index").reset_index()
        )
        result = result.merge(frag, on=["codigo_padre", "centro", "week"], how="left")

    result["fragmentation_index"] = result.get("fragmentation_index", pd.Series(0.0)).fillna(0.0)
    result["source"] = "stock"

    return result


def build_size_availability(txn, products):
    """
    Track which sizes are 'available' per parent SKU-store-week.
    Fallback proxy: a size is available if it sold in the trailing 4-week window.
    Used when actual stock data is not available.

    Vectorized implementation — avoids per-group Python loops.
    """
    sales = txn[txn["cantidad"] > 0].copy()
    sales["week"] = sales["fecha"].dt.to_period("W").dt.start_time

    sku_info = products[["material", "codigo_padre", "talla", "primera_jerarquia"]].rename(
        columns={"material": "sku"}
    ).drop_duplicates(subset=["sku"])

    sales = sales.merge(sku_info, on="sku", how="left")

    # Only footwear (sizes are meaningful)
    footwear = sales[sales["primera_jerarquia"] == "Footwear"].copy()
    if len(footwear) == 0:
        return pd.DataFrame()

    # ── Step 1: Unique (parent, store, week, size) sales events ──
    size_sales = (
        footwear.groupby(["codigo_padre", "centro", "week", "talla"])["cantidad"]
        .sum().reset_index()
    )
    print(f"    {len(size_sales):,} size-level sales events")

    # ── Step 2: Expand forward — a size sold in week W is "active" in W..W+3 ──
    key_cols = ["codigo_padre", "centro", "talla"]
    expanded_frames = []
    for offset in range(4):
        tmp = size_sales[key_cols + ["week"]].copy()
        tmp["week"] = tmp["week"] + pd.Timedelta(weeks=offset)
        expanded_frames.append(tmp)
    expanded = pd.concat(expanded_frames).drop_duplicates()
    print(f"    {len(expanded):,} expanded active-size records")

    # ── Step 3: Active sizes per parent-store-week ──
    active_counts = (
        expanded.groupby(["codigo_padre", "centro", "week"])["talla"]
        .nunique().rename("active_sizes_4w").reset_index()
    )

    # ── Step 4: Core sizes active per parent-store-week ──
    core_expanded = expanded[expanded["talla"].isin(CORE_SIZES)]
    if len(core_expanded) > 0:
        core_counts = (
            core_expanded.groupby(["codigo_padre", "centro", "week"])["talla"]
            .nunique().rename("core_sizes_active").reset_index()
        )
    else:
        core_counts = pd.DataFrame(columns=["codigo_padre", "centro", "week", "core_sizes_active"])

    # ── Step 5: Total sizes ever and core sizes ever per parent-store ──
    total_ever = (
        size_sales.groupby(["codigo_padre", "centro"])["talla"]
        .nunique().rename("total_sizes_ever").reset_index()
    )
    core_sales = size_sales[size_sales["talla"].isin(CORE_SIZES)]
    if len(core_sales) > 0:
        core_ever = (
            core_sales.groupby(["codigo_padre", "centro"])["talla"]
            .nunique().rename("core_sizes_total").reset_index()
        )
    else:
        core_ever = pd.DataFrame(columns=["codigo_padre", "centro", "core_sizes_total"])

    # ── Step 6: Assemble result ──
    result = active_counts.merge(total_ever, on=["codigo_padre", "centro"], how="left")
    result = result.merge(core_counts, on=["codigo_padre", "centro", "week"], how="left")
    result = result.merge(core_ever, on=["codigo_padre", "centro"], how="left")

    result["core_sizes_active"] = result["core_sizes_active"].fillna(0).astype(int)
    result["core_sizes_total"] = result["core_sizes_total"].fillna(0).astype(int)
    result["lost_sizes"] = result["total_sizes_ever"] - result["active_sizes_4w"]
    result["size_completeness"] = result["active_sizes_4w"] / result["total_sizes_ever"].clip(lower=1)
    result["core_sizes_lost"] = result["core_sizes_total"] - result["core_sizes_active"]
    result["core_completeness"] = np.where(
        result["core_sizes_total"] > 0,
        result["core_sizes_active"] / result["core_sizes_total"],
        0.0,
    )

    # ── Step 7: Peak sizes (rolling 12-observation max of active_sizes_4w) ──
    result = result.sort_values(["codigo_padre", "centro", "week"])
    result["peak_sizes"] = (
        result.groupby(["codigo_padre", "centro"])["active_sizes_4w"]
        .transform(lambda x: x.rolling(12, min_periods=1).max())
        .astype(int)
    )
    result["attrition_rate"] = 1 - (result["active_sizes_4w"] / result["peak_sizes"].clip(lower=1))

    # ── Step 8: Fragmentation index ──
    expanded["talla_num"] = pd.to_numeric(
        expanded["talla"].str.replace(",", "."), errors="coerce"
    )
    num_expanded = expanded.dropna(subset=["talla_num"])

    if len(num_expanded) > 0:
        def _fragmentation(sizes):
            vals = np.sort(sizes.values)
            if len(vals) < 2:
                return 0.0
            gaps = np.diff(vals)
            mean_gap = gaps.mean()
            return float(gaps.std() / max(mean_gap, 0.01)) if mean_gap > 0 else 0.0

        frag = (
            num_expanded.groupby(["codigo_padre", "centro", "week"])["talla_num"]
            .agg(_fragmentation).rename("fragmentation_index").reset_index()
        )
        result = result.merge(frag, on=["codigo_padre", "centro", "week"], how="left")

    result["fragmentation_index"] = result.get("fragmentation_index", pd.Series(0.0)).fillna(0.0)
    result["source"] = "sales_proxy"

    return result


def analyze_size_markdown_relationship(size_df, lifecycle_df):
    """
    Quantify the relationship between size curve breakage and subsequent markdown.
    """
    merged = size_df.merge(
        lifecycle_df[["codigo_padre", "centro", "week", "avg_discount_rate", "lifecycle_stage"]],
        on=["codigo_padre", "centro", "week"],
        how="inner",
    )

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


def run_size_curve_for_brand(brand: str):
    """Main size curve analysis pipeline for a given brand."""
    processed = _processed_dir(brand)
    raw = _raw_dir(brand)
    processed.mkdir(parents=True, exist_ok=True)

    print(f"[{brand}] Loading data...")
    txn = pd.read_parquet(raw / "transactions.parquet")
    products = pd.read_parquet(raw / "products.parquet")

    # Use actual stock data when available, fall back to sales proxy
    stock_path = raw / "stock.parquet"
    if stock_path.exists():
        print(f"[{brand}] Building size availability from STOCK DATA...")
        stock = pd.read_parquet(stock_path)
        size_df = build_size_availability_from_stock(stock, products)
        if len(size_df) == 0:
            print(f"  Stock data didn't match footwear — falling back to sales proxy")
            size_df = build_size_availability(txn, products)
    else:
        print(f"[{brand}] Building size availability from sales proxy (no stock data)...")
        size_df = build_size_availability(txn, products)
    size_df.to_parquet(processed / "size_curve_tracking.parquet", index=False)

    print(f"  {len(size_df):,} parent-store-week rows")
    print(f"  {size_df['codigo_padre'].nunique()} parent SKUs tracked")

    print(f"\n  Size curve health distribution:")
    bins = [(0, 0.25), (0.25, 0.5), (0.5, 0.75), (0.75, 1.0), (1.0, 1.01)]
    labels = ["Critical (<25%)", "Poor (25-50%)", "Fair (50-75%)", "Good (75-99%)", "Full (100%)"]
    for (lo, hi), label in zip(bins, labels):
        count = ((size_df["size_completeness"] >= lo) & (size_df["size_completeness"] < hi)).sum()
        pct = count / len(size_df) * 100
        bar = "=" * int(pct / 2)
        print(f"    {label:25s} {count:>5,} ({pct:5.1f}%) {bar}")

    print(f"\n  Core size availability (footwear with core sizes):")
    has_core = size_df[size_df["core_sizes_total"] > 0]
    if len(has_core) > 0:
        print(f"    Avg core sizes active: {has_core['core_sizes_active'].mean():.1f} / {has_core['core_sizes_total'].mean():.1f}")
        print(f"    Core completeness: {has_core['core_completeness'].mean():.1%}")
        print(f"    Parent SKUs with any core loss: {(has_core.groupby('codigo_padre')['core_sizes_lost'].max() > 0).sum()}")

    # Alerts
    print(f"\n[{brand}] Generating size curve alerts...")
    alerts = build_size_alerts(size_df)
    alerts.to_parquet(processed / "size_curve_alerts.parquet", index=False)
    print(f"  {len(alerts):,} alert rows")
    print(f"  {alerts['codigo_padre'].nunique()} parent SKUs with alerts")

    # Relationship with markdown
    print(f"\n[{brand}] Analyzing size-markdown relationship...")
    try:
        lifecycle = pd.read_parquet(processed / "lifecycle_stages.parquet")
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
        print("  (Lifecycle data not available -- run lifecycle_brand.py first)")

    # Most at-risk parent SKUs right now
    latest_week = size_df["week"].max()
    current = size_df[size_df["week"] == latest_week].sort_values("attrition_rate", ascending=False)
    print(f"\n  Current week ({latest_week.date()}) -- Most depleted parent SKUs:")
    print(f"  {'Parent SKU':<25} {'Store':>6} {'Active':>6} {'Peak':>5} {'Attrition':>10} {'Core Active':>11}")
    print("  " + "-" * 70)
    for _, row in current.head(15).iterrows():
        print(f"  {row['codigo_padre']:<25} {row['centro']:>6} {row['active_sizes_4w']:>6.0f} "
              f"{row['peak_sizes']:>5.0f} {row['attrition_rate']:>9.0%} "
              f"{row['core_sizes_active']:>5.0f}/{row['core_sizes_total']:>3.0f}")

    return size_df, alerts


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--brand", type=str, required=True, help="Brand name (e.g. HOKA, BOLD)")
    args = parser.parse_args()
    run_size_curve_for_brand(args.brand)
