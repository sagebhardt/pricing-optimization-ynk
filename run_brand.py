#!/usr/bin/env python3
"""
Run the full pricing pipeline for any brand.

Usage:
    python run_brand.py BOLD
    python run_brand.py HOKA
    python run_brand.py BOLD --steps extract features
"""

import argparse
import gc
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.data.extract_brand import extract_brand
from src.features.build_features_brand import build_features_for_brand
from src.features.price_elasticity_brand import run_elasticity_for_brand
from src.features.lifecycle_brand import build_lifecycle_for_brand
from src.features.size_curve_brand import run_size_curve_for_brand
from src.features.build_enhanced_brand import build_enhanced_for_brand
from src.features.aggregate_parent import aggregate_to_parent
from src.models.train_brand import train_brand_models
from src.models.weekly_pricing_brand import generate_weekly_actions_for_brand
from src.features.outcome_brand import compute_outcomes_for_brand

ALL_STEPS = ["extract", "elasticity", "features", "lifecycle", "size_curve", "enhance", "aggregate", "train", "pricing", "outcome", "sync"]

PROJECT_ROOT = Path(__file__).parent


def sync_to_gcs(brand: str):
    """Upload pipeline outputs to GCS so the API can serve them without redeploy."""
    bucket_name = os.getenv("GCS_BUCKET", "")
    if not bucket_name:
        print("  GCS_BUCKET not set — skipping sync")
        return

    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    brand_lower = brand.lower()
    uploaded = 0

    def _upload(local_path, gcs_path):
        nonlocal uploaded
        try:
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(str(local_path))
            uploaded += 1
            print(f"  -> gs://{bucket_name}/{gcs_path}")
        except Exception as e:
            print(f"  WARN: failed to upload {gcs_path}: {e}")

    # 1. Weekly actions — latest CSV only
    actions_dir = PROJECT_ROOT / "weekly_actions" / brand_lower
    csvs = sorted(actions_dir.glob("pricing_actions_*.csv"))
    if csvs:
        latest = csvs[-1]
        _upload(latest, f"weekly_actions/{brand_lower}/{latest.name}")

    # 2. Size curve alerts
    alerts_path = PROJECT_ROOT / "data" / "processed" / brand_lower / "size_curve_alerts.parquet"
    if alerts_path.exists():
        _upload(alerts_path, f"alerts/{brand_lower}/size_curve_alerts.parquet")

    # 3. Training metadata
    meta_path = PROJECT_ROOT / "models" / brand_lower / "training_metadata.json"
    if meta_path.exists():
        _upload(meta_path, f"models/{brand_lower}/training_metadata.json")

    # 4. SHAP feature importance (for analytics panel)
    for shap_file in ["classifier_shap.csv", "regressor_shap.csv"]:
        sp = PROJECT_ROOT / "models" / brand_lower / shap_file
        if sp.exists():
            _upload(sp, f"models/{brand_lower}/{shap_file}")

    # 5. Elasticity data (for analytics panel)
    elast_sku_path = PROJECT_ROOT / "data" / "processed" / brand_lower / "elasticity_by_sku.parquet"
    if elast_sku_path.exists():
        _upload(elast_sku_path, f"models/{brand_lower}/elasticity_by_sku.parquet")

    # 6. Features parquet (for offline analysis / clustering experiments)
    feat_path = PROJECT_ROOT / "data" / "processed" / brand_lower / "features_parent.parquet"
    if feat_path.exists():
        _upload(feat_path, f"data/processed/{brand_lower}/features_parent.parquet")

    # 7. Outcome results (prediction vs actual feedback loop)
    outcome_path = PROJECT_ROOT / "data" / "processed" / brand_lower / "outcome_results.parquet"
    if outcome_path.exists():
        _upload(outcome_path, f"outcomes/{brand_lower}/outcome_results.parquet")

    print(f"  Synced {uploaded} files to GCS")


def main():
    parser = argparse.ArgumentParser(description="Run pricing pipeline for a brand")
    parser.add_argument("brand", type=str, help="Brand name (e.g., BOLD, HOKA)")
    parser.add_argument("--steps", nargs="+", default=ALL_STEPS, choices=ALL_STEPS)
    parser.add_argument("--week", type=str, default=None)
    args = parser.parse_args()

    brand = args.brand.upper()
    print(f"{'=' * 60}")
    print(f"PRICING PIPELINE — {brand}")
    print(f"Steps: {', '.join(args.steps)}")
    print(f"{'=' * 60}")

    start = time.time()

    step_fns = {
        "extract": lambda: extract_brand(brand),
        "features": lambda: build_features_for_brand(brand),
        "elasticity": lambda: run_elasticity_for_brand(brand),
        "lifecycle": lambda: build_lifecycle_for_brand(brand),
        "size_curve": lambda: run_size_curve_for_brand(brand),
        "enhance": lambda: build_enhanced_for_brand(brand),
        "aggregate": lambda: aggregate_to_parent(brand),
        "train": lambda: train_brand_models(brand),
        "pricing": lambda: generate_weekly_actions_for_brand(brand, target_week=args.week),
        "outcome": lambda: compute_outcomes_for_brand(brand),
        "sync": lambda: sync_to_gcs(brand),
    }

    for step in args.steps:
        t0 = time.time()
        print(f"\n{'=' * 60}")
        print(f"STEP: {step.upper()}")
        print(f"{'=' * 60}")
        try:
            step_fns[step]()
            print(f"\n  >> {step} completed in {time.time()-t0:.1f}s")
        except Exception as e:
            print(f"\n  >> {step} FAILED: {e}")
            raise
        gc.collect()  # free large intermediates between steps

    print(f"\n{'=' * 60}")
    print(f"{brand} pipeline completed in {time.time()-start:.1f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
