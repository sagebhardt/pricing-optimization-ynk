"""
Enhanced feature engineering: integrates base features with elasticity,
lifecycle stages, size curve tracking, and derived seasons.

Produces the v2 feature table for model retraining.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
from pathlib import Path

RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).parent.parent.parent / "data" / "processed"


def build_enhanced_features():
    """Merge all feature sources into a single enhanced feature table."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading base features...")
    features = pd.read_parquet(PROCESSED_DIR / "hoka_features.parquet")
    print(f"  Base: {len(features):,} rows × {len(features.columns)} cols")

    products = pd.read_parquet(RAW_DIR / "hoka_products.parquet")
    sku_to_parent = products.set_index("material")["codigo_padre"].to_dict()

    # Ensure codigo_padre exists
    if "codigo_padre" not in features.columns:
        features["codigo_padre"] = features["sku"].map(sku_to_parent)

    # 1. Elasticity
    print("Adding price elasticity...")
    try:
        sku_elast = pd.read_parquet(PROCESSED_DIR / "elasticity_by_sku.parquet")
        cat_elast = pd.read_parquet(PROCESSED_DIR / "elasticity_by_category.parquet")

        reliable = sku_elast[sku_elast["confidence"].isin(["high", "medium"])]
        elast_map = reliable.set_index("codigo_padre")["elasticity"].to_dict()
        features["price_elasticity_sku"] = features["codigo_padre"].map(elast_map)

        cat_map = cat_elast.set_index(["primera_jerarquia", "segunda_jerarquia"])["elasticity"].to_dict()
        features["price_elasticity_cat"] = features.apply(
            lambda r: cat_map.get((r["primera_jerarquia"], r.get("segunda_jerarquia")), np.nan), axis=1
        )
        features["price_elasticity"] = features["price_elasticity_sku"].fillna(features["price_elasticity_cat"])
        features.drop(columns=["price_elasticity_sku", "price_elasticity_cat"], inplace=True)

        coverage = features["price_elasticity"].notna().mean()
        print(f"  Coverage: {coverage:.1%}")
    except FileNotFoundError:
        print("  (not available)")
        features["price_elasticity"] = np.nan

    # 2. Lifecycle stage
    print("Adding lifecycle stages...")
    try:
        lifecycle = pd.read_parquet(PROCESSED_DIR / "lifecycle_stages.parquet")
        lifecycle_feats = lifecycle[["codigo_padre", "centro", "week",
                                     "lifecycle_stage", "lifecycle_position"]].copy()

        stage_map = {"launch": 0, "growth": 1, "peak": 2, "steady": 3, "decline": 4, "clearance": 5}
        lifecycle_feats["lifecycle_stage_code"] = lifecycle_feats["lifecycle_stage"].map(stage_map)

        # Need to map parent-store-week to SKU-store-week
        # Each SKU inherits its parent's lifecycle
        features = features.merge(
            lifecycle_feats[["codigo_padre", "centro", "week", "lifecycle_stage_code", "lifecycle_position"]],
            on=["codigo_padre", "centro", "week"],
            how="left",
        )
        coverage = features["lifecycle_stage_code"].notna().mean()
        print(f"  Coverage: {coverage:.1%}")
    except FileNotFoundError:
        print("  (not available)")
        features["lifecycle_stage_code"] = np.nan
        features["lifecycle_position"] = np.nan

    # 3. Size curve
    print("Adding size curve features...")
    try:
        size_curves = pd.read_parquet(PROCESSED_DIR / "size_curve_tracking.parquet")
        size_feats = size_curves[[
            "codigo_padre", "centro", "week",
            "attrition_rate", "core_completeness", "fragmentation_index",
            "active_sizes_4w", "total_sizes_ever",
        ]].copy()

        features = features.merge(size_feats, on=["codigo_padre", "centro", "week"], how="left")
        coverage = features["attrition_rate"].notna().mean()
        print(f"  Coverage: {coverage:.1%}")
    except FileNotFoundError:
        print("  (not available)")

    # 4. Derived season
    print("Adding derived season...")
    try:
        seasons = pd.read_parquet(PROCESSED_DIR / "derived_seasons.parquet")
        season_map = seasons.set_index("codigo_padre")["derived_season"].to_dict()
        features["is_fall_winter"] = features["codigo_padre"].map(
            lambda x: 1 if season_map.get(x) == "FW" else 0
        )
        coverage = features["is_fall_winter"].notna().mean()
        print(f"  Coverage: {coverage:.1%}")
    except FileNotFoundError:
        print("  (not available)")

    # Save
    features.to_parquet(PROCESSED_DIR / "hoka_features_v2.parquet", index=False)

    new_cols = [c for c in features.columns if c not in
                pd.read_parquet(PROCESSED_DIR / "hoka_features.parquet").columns]
    print(f"\n--- Enhanced Feature Table ---")
    print(f"  Rows: {len(features):,}")
    print(f"  Total columns: {len(features.columns)}")
    print(f"  New columns: {len(new_cols)}")
    for col in new_cols:
        print(f"    + {col:35s}  nulls={features[col].isna().sum():,} ({features[col].isna().mean():.1%})")

    return features


if __name__ == "__main__":
    build_enhanced_features()
