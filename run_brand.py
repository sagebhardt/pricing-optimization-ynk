#!/usr/bin/env python3
"""
Run the full pricing pipeline for any brand.

Usage:
    python run_brand.py BOLD
    python run_brand.py HOKA
    python run_brand.py BOLD --steps extract features
"""

import argparse
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

ALL_STEPS = ["extract", "features", "elasticity", "lifecycle", "size_curve", "enhance", "aggregate", "train", "pricing"]


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

    print(f"\n{'=' * 60}")
    print(f"{brand} pipeline completed in {time.time()-start:.1f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
