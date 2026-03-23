#!/usr/bin/env python3
"""
Pipeline orchestrator for HOKA markdown optimization.

Usage:
    python run_pipeline.py                         # Run full pipeline
    python run_pipeline.py --steps extract         # Run specific step(s)
    python run_pipeline.py --steps extract features train_v2
    python run_pipeline.py --steps recommend --week 2026-03-16
    python run_pipeline.py --steps report
"""

import argparse
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

ALL_STEPS = [
    "extract", "features", "elasticity", "lifecycle", "size_curve",
    "enhance", "train_v1", "train_v2", "first_markdown",
    "backtest", "recommend", "report",
]

DEFAULT_STEPS = [
    "extract", "features", "elasticity", "lifecycle", "size_curve",
    "enhance", "train_v2", "first_markdown", "backtest", "recommend", "report",
]


def run_extract():
    print("\n" + "=" * 60)
    print("STEP: DATA EXTRACTION")
    print("=" * 60)
    from src.data.extract import run_full_extract
    return run_full_extract()


def run_features():
    print("\n" + "=" * 60)
    print("STEP: BASE FEATURE ENGINEERING")
    print("=" * 60)
    from src.features.build_features import build_all_features
    return build_all_features()


def run_elasticity():
    print("\n" + "=" * 60)
    print("STEP: PRICE ELASTICITY ESTIMATION")
    print("=" * 60)
    from src.features.price_elasticity import run_elasticity_analysis
    return run_elasticity_analysis()


def run_lifecycle():
    print("\n" + "=" * 60)
    print("STEP: LIFECYCLE STAGE DERIVATION")
    print("=" * 60)
    from src.features.lifecycle import build_lifecycle_features
    return build_lifecycle_features()


def run_size_curve():
    print("\n" + "=" * 60)
    print("STEP: SIZE CURVE ANALYSIS")
    print("=" * 60)
    from src.features.size_curve import run_size_curve_analysis
    return run_size_curve_analysis()


def run_enhance():
    print("\n" + "=" * 60)
    print("STEP: ENHANCED FEATURE TABLE (v2)")
    print("=" * 60)
    from src.features.build_enhanced_features import build_enhanced_features
    return build_enhanced_features()


def run_train_v1():
    print("\n" + "=" * 60)
    print("STEP: MODEL TRAINING (v1 baseline)")
    print("=" * 60)
    from src.models.train import run_training
    return run_training()


def run_train_v2():
    print("\n" + "=" * 60)
    print("STEP: MODEL TRAINING (v2 enhanced)")
    print("=" * 60)
    from src.models.train_v2 import run_v2_training
    return run_v2_training()


def run_first_markdown():
    print("\n" + "=" * 60)
    print("STEP: FIRST-MARKDOWN TIMING MODEL")
    print("=" * 60)
    from src.models.first_markdown import train_first_markdown_model
    return train_first_markdown_model()


def run_backtest():
    print("\n" + "=" * 60)
    print("STEP: BACKTESTING")
    print("=" * 60)
    from src.models.backtest import run_backtest as _run_backtest
    return _run_backtest()


def run_recommend(week=None):
    print("\n" + "=" * 60)
    print("STEP: RECOMMENDATIONS")
    print("=" * 60)
    from src.models.recommend import generate_recommendations
    return generate_recommendations(target_week=week, min_confidence="medium", top_n=50)


def run_report():
    print("\n" + "=" * 60)
    print("STEP: REPORT GENERATION")
    print("=" * 60)
    from src.reports.generate_report import generate_full_report
    return generate_full_report()


STEP_FUNCTIONS = {
    "extract": run_extract,
    "features": run_features,
    "elasticity": run_elasticity,
    "lifecycle": run_lifecycle,
    "size_curve": run_size_curve,
    "enhance": run_enhance,
    "train_v1": run_train_v1,
    "train_v2": run_train_v2,
    "first_markdown": run_first_markdown,
    "backtest": run_backtest,
    "recommend": run_recommend,
    "report": run_report,
}


def main():
    parser = argparse.ArgumentParser(description="HOKA Markdown Optimization Pipeline")
    parser.add_argument(
        "--steps", nargs="+", default=DEFAULT_STEPS,
        choices=ALL_STEPS,
        help=f"Steps to run. Options: {', '.join(ALL_STEPS)}"
    )
    parser.add_argument("--week", type=str, default=None, help="Target week for recommendations (YYYY-MM-DD)")
    args = parser.parse_args()

    print("=" * 60)
    print("HOKA MARKDOWN OPTIMIZATION PIPELINE")
    print(f"Steps: {', '.join(args.steps)}")
    print("=" * 60)

    start = time.time()
    results = {}
    completed = []
    failed = []

    for step in args.steps:
        step_start = time.time()
        try:
            if step == "recommend":
                results[step] = STEP_FUNCTIONS[step](args.week)
            else:
                results[step] = STEP_FUNCTIONS[step]()
            elapsed = time.time() - step_start
            completed.append((step, elapsed))
            print(f"\n  >> {step} completed in {elapsed:.1f}s")
        except Exception as e:
            elapsed = time.time() - step_start
            failed.append((step, elapsed, str(e)))
            print(f"\n  >> {step} FAILED after {elapsed:.1f}s: {e}")
            raise

    total = time.time() - start
    print(f"\n{'=' * 60}")
    print(f"Pipeline completed in {total:.1f}s")
    for step, elapsed in completed:
        print(f"  {step:20s} {elapsed:>6.1f}s")
    if failed:
        print(f"\nFailed steps:")
        for step, elapsed, err in failed:
            print(f"  {step:20s} {elapsed:>6.1f}s  {err}")
    print(f"{'=' * 60}")

    return results


if __name__ == "__main__":
    main()
