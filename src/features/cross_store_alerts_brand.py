"""
Cross-store pricing consistency alerts.

Detects when the same parent SKU has significantly different prices,
discounts, or stock levels across stores within the same brand.

Usage:
    python src/features/cross_store_alerts_brand.py BOLD
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
from pathlib import Path
from config.vendor_brands import is_ecomm_store

PROJECT_ROOT = Path(__file__).parent.parent.parent


def _processed_dir(brand: str) -> Path:
    return PROJECT_ROOT / "data" / "processed" / brand.lower()


def build_cross_store_alerts(
    features: pd.DataFrame,
    threshold_price_spread: float = 0.10,
    threshold_discount_spread: float = 0.10,
    threshold_ecomm_gap: float = 0.15,
    threshold_woc_excess: float = 12.0,
):
    """
    Generate alerts for pricing inconsistencies across stores.

    Parameters
    ----------
    features : DataFrame
        Parent-level features with columns: codigo_padre, centro, week,
        avg_precio_final, discount_rate, stock_on_hand, weeks_of_cover, velocity_4w.
    threshold_price_spread : float
        Minimum relative price spread within B&M stores to trigger alert.
    threshold_discount_spread : float
        Minimum discount range (pp) across stores to trigger alert.
    threshold_ecomm_gap : float
        Minimum ecomm-vs-B&M price gap to trigger alert.
    threshold_woc_excess : float
        Weeks-of-cover threshold for "excess stock" in imbalance check.
    """
    required = ["codigo_padre", "centro", "week", "avg_precio_final"]
    missing = [c for c in required if c not in features.columns]
    if missing:
        print(f"  Missing columns for cross-store alerts: {missing}")
        return pd.DataFrame()

    # Latest week only
    latest_week = features["week"].max()
    df = features[features["week"] == latest_week].copy()

    if len(df) == 0:
        return pd.DataFrame()

    # Classify channels
    df["channel"] = np.where(df["centro"].apply(is_ecomm_store), "ecomm", "bm")

    # Only parents with 2+ stores
    store_counts = df.groupby("codigo_padre")["centro"].nunique()
    multi_store = store_counts[store_counts >= 2].index
    df = df[df["codigo_padre"].isin(multi_store)]

    if len(df) == 0:
        return pd.DataFrame()

    # Compute per-parent cross-store metrics
    grouped = df.groupby("codigo_padre")

    parent_stats = grouped.agg(
        n_stores=("centro", "nunique"),
        median_price=("avg_precio_final", "median"),
        min_price=("avg_precio_final", "min"),
        max_price=("avg_precio_final", "max"),
        min_discount=("discount_rate", "min"),
        max_discount=("discount_rate", "max"),
    ).reset_index()

    parent_stats["price_spread"] = np.where(
        parent_stats["median_price"] > 0,
        (parent_stats["max_price"] - parent_stats["min_price"]) / parent_stats["median_price"],
        0,
    )
    parent_stats["discount_spread"] = parent_stats["max_discount"] - parent_stats["min_discount"]

    # B&M-only price spread (exclude ecomm stores)
    bm = df[df["channel"] == "bm"]
    if len(bm) > 0:
        bm_grouped = bm.groupby("codigo_padre")
        bm_stats = bm_grouped.agg(
            bm_min_price=("avg_precio_final", "min"),
            bm_max_price=("avg_precio_final", "max"),
            bm_median_price=("avg_precio_final", "median"),
            bm_stores=("centro", "nunique"),
        ).reset_index()
        bm_stats["bm_price_spread"] = np.where(
            bm_stats["bm_median_price"] > 0,
            (bm_stats["bm_max_price"] - bm_stats["bm_min_price"]) / bm_stats["bm_median_price"],
            0,
        )
        parent_stats = parent_stats.merge(bm_stats, on="codigo_padre", how="left")
    else:
        parent_stats["bm_price_spread"] = 0.0
        parent_stats["bm_stores"] = 0
        parent_stats["bm_median_price"] = parent_stats["median_price"]

    # Ecomm vs B&M gap
    ecomm = df[df["channel"] == "ecomm"]
    if len(ecomm) > 0 and "bm_median_price" in parent_stats.columns:
        ecomm_prices = ecomm.groupby("codigo_padre")["avg_precio_final"].median().rename("ecomm_price")
        parent_stats = parent_stats.merge(ecomm_prices, on="codigo_padre", how="left")
        parent_stats["ecomm_gap"] = np.where(
            parent_stats["bm_median_price"] > 0,
            (parent_stats["bm_median_price"] - parent_stats["ecomm_price"]) / parent_stats["bm_median_price"],
            0,
        )
    else:
        parent_stats["ecomm_gap"] = 0.0

    # Markdown split: some stores discounted, others not
    disc_flag = df.copy()
    disc_flag["is_discounted"] = disc_flag["discount_rate"] > 0.05
    disc_counts = disc_flag.groupby("codigo_padre")["is_discounted"].agg(
        stores_discounted="sum",
        stores_full_price=lambda x: (~x).sum(),
    ).reset_index()
    disc_counts["has_markdown_split"] = (disc_counts["stores_discounted"] > 0) & (disc_counts["stores_full_price"] > 0)
    parent_stats = parent_stats.merge(disc_counts, on="codigo_padre", how="left")

    # Stock imbalance: one store stockout + another excess
    has_stock = "stock_on_hand" in df.columns and "weeks_of_cover" in df.columns
    if has_stock:
        stock_check = df[df["stock_on_hand"].notna()].copy()
        if len(stock_check) > 0:
            stock_agg = stock_check.groupby("codigo_padre").agg(
                any_stockout=("stock_on_hand", lambda x: (x == 0).any()),
                any_excess=("weeks_of_cover", lambda x: (x > threshold_woc_excess).any()),
            ).reset_index()
            stock_agg["has_stock_imbalance"] = stock_agg["any_stockout"] & stock_agg["any_excess"]
            parent_stats = parent_stats.merge(stock_agg[["codigo_padre", "has_stock_imbalance"]], on="codigo_padre", how="left")
        else:
            parent_stats["has_stock_imbalance"] = False
    else:
        parent_stats["has_stock_imbalance"] = False

    parent_stats["has_stock_imbalance"] = parent_stats["has_stock_imbalance"].fillna(False)
    parent_stats["has_markdown_split"] = parent_stats["has_markdown_split"].fillna(False)
    parent_stats["bm_price_spread"] = parent_stats["bm_price_spread"].fillna(0)
    parent_stats["ecomm_gap"] = parent_stats["ecomm_gap"].fillna(0)

    # Velocity-weighted sync price per parent
    vel_col = "velocity_4w" if "velocity_4w" in df.columns else None
    if vel_col:
        df["_vel_price"] = df[vel_col].clip(lower=0) * df["avg_precio_final"]
        sync = df.groupby("codigo_padre").agg(
            _total_vel_price=("_vel_price", "sum"),
            _total_vel=("velocity_4w", lambda x: x.clip(lower=0).sum()),
        ).reset_index()
        sync["sync_price"] = np.where(
            sync["_total_vel"] > 0,
            sync["_total_vel_price"] / sync["_total_vel"],
            np.nan,
        )
        parent_stats = parent_stats.merge(sync[["codigo_padre", "sync_price"]], on="codigo_padre", how="left")
        # Fallback: use median price where velocity is zero everywhere
        parent_stats["sync_price"] = parent_stats["sync_price"].fillna(parent_stats["median_price"])
    else:
        parent_stats["sync_price"] = parent_stats["median_price"]

    # Apply alert thresholds
    is_alert = (
        ((parent_stats["bm_price_spread"] > threshold_price_spread) & (parent_stats["bm_stores"].fillna(0) >= 2))
        | (parent_stats["discount_spread"] > threshold_discount_spread)
        | parent_stats["has_markdown_split"]
        | parent_stats["has_stock_imbalance"]
        | (parent_stats["ecomm_gap"].abs() > threshold_ecomm_gap)
    )
    alerted_parents = parent_stats[is_alert]["codigo_padre"]

    if len(alerted_parents) == 0:
        return pd.DataFrame()

    # Build output: one row per (parent, store) for alerted parents
    result = df[df["codigo_padre"].isin(alerted_parents)].copy()
    result = result.merge(
        parent_stats[["codigo_padre", "n_stores", "median_price", "price_spread",
                       "discount_spread", "bm_price_spread", "ecomm_gap",
                       "sync_price", "has_markdown_split", "has_stock_imbalance"]],
        on="codigo_padre", how="left",
    )

    # Build alert reasons
    result["alert_reasons"] = ""

    mask_bm_price = (result["bm_price_spread"] > threshold_price_spread)
    result.loc[mask_bm_price, "alert_reasons"] += "price_inconsistency_bm;"

    mask_disc = (result["discount_spread"] > threshold_discount_spread)
    result.loc[mask_disc, "alert_reasons"] += "discount_spread;"

    mask_md_split = result["has_markdown_split"]
    result.loc[mask_md_split, "alert_reasons"] += "markdown_split;"

    mask_stock = result["has_stock_imbalance"]
    result.loc[mask_stock, "alert_reasons"] += "stock_imbalance;"

    mask_ecomm = (result["ecomm_gap"].abs() > threshold_ecomm_gap)
    result.loc[mask_ecomm, "alert_reasons"] += "ecomm_gap;"

    # Select output columns
    out_cols = [
        "codigo_padre", "centro", "week", "channel",
        "avg_precio_final", "discount_rate",
        "n_stores", "median_price", "price_spread", "discount_spread",
        "bm_price_spread", "ecomm_gap", "sync_price", "alert_reasons",
    ]
    if "stock_on_hand" in result.columns:
        out_cols.append("stock_on_hand")
    if "weeks_of_cover" in result.columns:
        out_cols.append("weeks_of_cover")
    if "velocity_4w" in result.columns:
        out_cols.append("velocity_4w")

    result = result[[c for c in out_cols if c in result.columns]]
    return result.sort_values(["price_spread", "codigo_padre"], ascending=[False, True])


def run_cross_store_alerts_for_brand(brand: str):
    """Main entry point for cross-store consistency alerts."""
    processed = _processed_dir(brand)

    print(f"[{brand}] Cross-store pricing consistency analysis...")
    features_path = processed / "features_parent.parquet"
    if not features_path.exists():
        print(f"  features_parent.parquet not found — skipping")
        return None

    features = pd.read_parquet(features_path)
    print(f"  Loaded {len(features):,} parent-store-week rows")
    print(f"  Stores: {features['centro'].nunique()}, Parents: {features['codigo_padre'].nunique()}")

    alerts = build_cross_store_alerts(features)

    if len(alerts) == 0:
        print(f"  No cross-store inconsistencies detected.")
        return None

    alerts.to_parquet(processed / "cross_store_alerts.parquet", index=False)

    n_parents = alerts["codigo_padre"].nunique()
    n_stores = alerts["centro"].nunique()
    print(f"\n  {len(alerts):,} alert rows ({n_parents} parents x {n_stores} stores)")

    # Summary by reason
    reason_counts = {}
    for reasons in alerts["alert_reasons"]:
        for r in reasons.strip(";").split(";"):
            if r:
                reason_counts[r] = reason_counts.get(r, 0) + 1
    for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
        print(f"    {reason}: {count}")

    avg_spread = alerts.groupby("codigo_padre")["price_spread"].first().mean()
    print(f"  Avg price spread among alerted parents: {avg_spread:.1%}")
    print(f"  Saved to: {processed / 'cross_store_alerts.parquet'}")

    return alerts


if __name__ == "__main__":
    brand = sys.argv[1] if len(sys.argv) > 1 else "BOLD"
    run_cross_store_alerts_for_brand(brand)
