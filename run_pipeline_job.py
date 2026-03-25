#!/usr/bin/env python3
"""
Cloud Run Job entrypoint — runs the full weekly pipeline for all brands.

Designed to run as a Cloud Run Job triggered by Cloud Scheduler every Monday.
Each brand runs sequentially: extract → features → enhance → aggregate → train → pricing → sync.
Failures in one brand don't block others.
"""

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from run_brand import main as run_brand_main

BRANDS = os.getenv("PIPELINE_BRANDS", "HOKA,BOLD,BAMERS,OAKLEY").split(",")
STEPS = os.getenv("PIPELINE_STEPS", "extract,features,elasticity,lifecycle,size_curve,enhance,aggregate,train,pricing,sync").split(",")


def run():
    start = time.time()
    print(f"{'=' * 60}")
    print(f"WEEKLY PIPELINE JOB — {time.strftime('%Y-%m-%d %H:%M')}")
    print(f"Brands: {', '.join(BRANDS)}")
    print(f"Steps: {', '.join(STEPS)}")
    print(f"GCS_BUCKET: {os.getenv('GCS_BUCKET', 'NOT SET')}")
    print(f"{'=' * 60}")

    failed = []

    for brand in BRANDS:
        brand = brand.strip().upper()
        print(f"\n{'=' * 60}")
        print(f"--- {brand} ---")
        print(f"{'=' * 60}")

        try:
            sys.argv = ["run_brand.py", brand, "--steps"] + STEPS
            run_brand_main()
        except SystemExit:
            pass  # argparse calls sys.exit(0) on success
        except Exception as e:
            print(f"\n  {brand} FAILED: {e}")
            failed.append(brand)

    elapsed = time.time() - start
    print(f"\n{'=' * 60}")
    if failed:
        print(f"DONE with errors ({elapsed:.0f}s): {', '.join(failed)} failed")
        sys.exit(1)
    else:
        print(f"DONE — all brands completed ({elapsed:.0f}s)")


if __name__ == "__main__":
    run()
