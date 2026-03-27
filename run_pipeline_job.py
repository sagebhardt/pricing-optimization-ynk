#!/usr/bin/env python3
"""
Cloud Run Job entrypoint — runs the full weekly pipeline for all brands.

Designed to run as a Cloud Run Job triggered by Cloud Scheduler every Monday.
Each brand runs as a subprocess so the OS fully reclaims memory between brands
(Python's gc doesn't return memory to the OS, causing OOM on multi-brand runs).
"""

import os
import subprocess
import sys
import time
from pathlib import Path

BRANDS = os.getenv("PIPELINE_BRANDS", "HOKA,BOLD,BAMERS,OAKLEY").split(",")
STEPS = os.getenv("PIPELINE_STEPS", "extract,features,elasticity,lifecycle,size_curve,enhance,aggregate,train,pricing,sync").split(",")


def run():
    start = time.time()
    print(f"{'=' * 60}")
    print(f"WEEKLY PIPELINE JOB — {time.strftime('%Y-%m-%d %H:%M')}")
    print(f"Brands: {', '.join(BRANDS)}")
    print(f"Steps: {', '.join(STEPS)}")
    print(f"GCS_BUCKET: {os.getenv('GCS_BUCKET', 'NOT SET')}")
    print(f"{'=' * 60}", flush=True)

    failed = []

    for brand in BRANDS:
        brand = brand.strip().upper()
        print(f"\n{'=' * 60}")
        print(f"--- {brand} ---")
        print(f"{'=' * 60}", flush=True)

        # Run each brand as a subprocess — OS reclaims ALL memory on exit
        # (vectorized lifecycle/size_curve create multi-GB intermediates)
        result = subprocess.run(
            [sys.executable, "run_brand.py", brand, "--steps"] + STEPS,
            cwd=str(Path(__file__).parent),
            env=os.environ.copy(),
        )

        if result.returncode != 0:
            print(f"\n  {brand} FAILED (exit code {result.returncode})")
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
