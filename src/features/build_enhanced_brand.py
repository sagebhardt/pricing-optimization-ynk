"""
Brand-agnostic enhanced feature engineering.

Thin wrapper around the HOKA-specific logic in build_enhanced_features.py,
parameterized by brand name. Reads from data/processed/{brand}/ and
data/raw/{brand}/, writes to data/processed/{brand}/features_v2.parquet.

Integrates base features with elasticity, lifecycle stages, size curve
tracking, and derived seasons. Produces the v2 feature table for model
retraining.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent


def _raw_dir(brand: str) -> Path:
    return PROJECT_ROOT / "data" / "raw" / brand.lower()


def _processed_dir(brand: str) -> Path:
    return PROJECT_ROOT / "data" / "processed" / brand.lower()


def build_enhanced_for_brand(brand: str):
    """Merge all feature sources into a single enhanced feature table for a given brand."""
    processed = _processed_dir(brand)
    raw = _raw_dir(brand)
    processed.mkdir(parents=True, exist_ok=True)

    print(f"[{brand}] Loading base features...")
    features = pd.read_parquet(processed / "features.parquet")
    print(f"  Base: {len(features):,} rows x {len(features.columns)} cols")

    products = pd.read_parquet(raw / "products.parquet")
    sku_to_parent = products.set_index("material")["codigo_padre"].to_dict()

    # Ensure codigo_padre exists
    if "codigo_padre" not in features.columns:
        features["codigo_padre"] = features["sku"].map(sku_to_parent)

    # 1. Elasticity
    print(f"[{brand}] Adding price elasticity...")
    try:
        sku_elast = pd.read_parquet(processed / "elasticity_by_sku.parquet")
        cat_elast = pd.read_parquet(processed / "elasticity_by_category.parquet")

        reliable = sku_elast[sku_elast["confidence"].isin(["high", "medium"])]
        elast_map = reliable.set_index("codigo_padre")["elasticity"].to_dict()
        features["price_elasticity_sku"] = features["codigo_padre"].map(elast_map)

        # Category elasticity fallback (handle empty cat_elast gracefully)
        if len(cat_elast) > 0 and "primera_jerarquia" in cat_elast.columns:
            cat_map = cat_elast.set_index(["primera_jerarquia", "segunda_jerarquia"])["elasticity"].to_dict()
            features["price_elasticity_cat"] = [
                cat_map.get(k, np.nan)
                for k in zip(features["primera_jerarquia"], features["segunda_jerarquia"])
            ]
        else:
            features["price_elasticity_cat"] = np.nan

        features["price_elasticity"] = features["price_elasticity_sku"].fillna(features["price_elasticity_cat"])
        features.drop(columns=["price_elasticity_sku", "price_elasticity_cat"], inplace=True)

        coverage = features["price_elasticity"].notna().mean()
        print(f"  Coverage: {coverage:.1%}")
    except (FileNotFoundError, KeyError):
        print("  (not available)")
        features["price_elasticity"] = np.nan

    # 2. Lifecycle stage
    print(f"[{brand}] Adding lifecycle stages...")
    try:
        lifecycle = pd.read_parquet(processed / "lifecycle_stages.parquet")
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
    print(f"[{brand}] Adding size curve features...")
    try:
        size_curves = pd.read_parquet(processed / "size_curve_tracking.parquet")
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

    # 4. Competitor pricing features
    print(f"[{brand}] Adding competitor pricing features...")
    try:
        comp = pd.read_parquet(processed / "competitor_prices.parquet")
        if len(comp) > 0:
            from src.features.competitor_features import add_competitor_features
            features = add_competitor_features(features, comp)
            coverage = (features["comp_count"] > 0).mean()
            print(f"  Coverage: {coverage:.1%} ({(features['comp_count'] > 0).sum():,} rows)")
        else:
            features["comp_count"] = 0
            print("  (empty file)")
    except FileNotFoundError:
        features["comp_count"] = 0
        print("  (not available)")

    # 5. Category interaction features
    print(f"[{brand}] Adding category interaction features...")
    try:
        from src.features.category_interactions import add_category_interactions
        features = add_category_interactions(features)
        interaction_cols = [c for c in features.columns if c.startswith("cat_x_")]
        print(f"  Added {len(interaction_cols)} interaction features: {interaction_cols}")
    except Exception as e:
        print(f"  Category interactions failed: {e}")

    # 6. Derived season
    print(f"[{brand}] Adding derived season...")
    try:
        seasons = pd.read_parquet(processed / "derived_seasons.parquet")
        season_map = seasons.set_index("codigo_padre")["derived_season"].to_dict()
        features["is_fall_winter"] = features["codigo_padre"].map(
            lambda x: 1 if season_map.get(x) == "FW" else 0
        )
        coverage = features["is_fall_winter"].notna().mean()
        print(f"  Coverage: {coverage:.1%}")
    except FileNotFoundError:
        print("  (not available)")

    # Compute lifecycle-filtered empirical lift table for next run's margin targets
    # This runs AFTER lifecycle data is available, avoiding the step-order issue.
    print(f"[{brand}] Computing lifecycle-filtered empirical lift table...")
    try:
        from src.features.build_features_brand import compute_empirical_lift, DISCOUNT_STEPS, DEFAULT_LIFT
        if "lifecycle_stage" in features.columns and "discount_rate" in features.columns:
            clean = features[features["lifecycle_stage"].isin(["steady", "growth", "peak", "launch"])]
            if len(clean) > 150:
                lift = compute_empirical_lift(clean, min_obs=50)
                import json
                lift_path = processed / "empirical_lift.json"
                lift_path.write_text(json.dumps({str(k): v for k, v in lift.items()}, indent=2))
                n_data = sum(1 for s in DISCOUNT_STEPS if lift.get(s) != DEFAULT_LIFT.get(s))
                print(f"  Saved lifecycle-filtered lift table ({n_data}/9 steps from data)")
                print(f"    {lift}")
            else:
                print(f"  Not enough clean rows ({len(clean):,}) — skipping")
        else:
            print("  Lifecycle stage not available in features — skipping")
    except Exception as e:
        print(f"  Error computing lift table: {e}")

    # Save
    features.to_parquet(processed / "features_v2.parquet", index=False)

    base_features = pd.read_parquet(processed / "features.parquet")
    new_cols = [c for c in features.columns if c not in base_features.columns]
    print(f"\n--- [{brand}] Enhanced Feature Table ---")
    print(f"  Rows: {len(features):,}")
    print(f"  Total columns: {len(features.columns)}")
    print(f"  New columns: {len(new_cols)}")
    for col in new_cols:
        print(f"    + {col:35s}  nulls={features[col].isna().sum():,} ({features[col].isna().mean():.1%})")

    return features


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--brand", type=str, required=True, help="Brand name (e.g. HOKA, BOLD)")
    args = parser.parse_args()
    build_enhanced_for_brand(args.brand)
