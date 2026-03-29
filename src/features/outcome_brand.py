"""
Feedback loop: compare predictions to actuals.

For each past week's approved decisions, computes:
  - Predicted vs actual velocity, revenue, margin
  - Lift vs baseline (pre-decision velocity)
  - Direction correctness (did the action move velocity the right way?)

Downloads historical pricing CSVs and decisions from GCS since the
pipeline container starts with no historical data on disk.

Usage:
    from src.features.outcome_brand import compute_outcomes_for_brand
    df = compute_outcomes_for_brand("HOKA", lookback_weeks=4)
"""

import io
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

PROJECT_ROOT = Path(__file__).parent.parent.parent

# Statuses that represent an implemented decision
IMPLEMENTED_STATUSES = {"approved", "planner_approved", "manual", "bm_approved", "bm_manual"}


def _gcs_bucket():
    """Get GCS bucket (returns None if not configured)."""
    bucket_name = os.getenv("GCS_BUCKET", "")
    if not bucket_name:
        return None
    from google.cloud import storage
    client = storage.Client()
    return client.bucket(bucket_name)


def _download_historical_csvs(brand: str, bucket) -> dict[str, pd.DataFrame]:
    """Download pricing_actions CSVs from GCS. Returns {week_str: DataFrame}."""
    prefix = f"weekly_actions/{brand.lower()}/pricing_actions_"
    result = {}
    if bucket is None:
        # Local fallback
        local_dir = PROJECT_ROOT / "weekly_actions" / brand.lower()
        if local_dir.exists():
            for f in sorted(local_dir.glob("pricing_actions_*.csv")):
                week = f.stem.replace("pricing_actions_", "")
                try:
                    result[week] = pd.read_csv(f)
                except Exception:
                    pass
        return result

    try:
        for blob in bucket.list_blobs(prefix=prefix):
            week = blob.name.split("pricing_actions_")[1].replace(".csv", "")
            try:
                df = pd.read_csv(io.StringIO(blob.download_as_text()))
                result[week] = df
            except Exception:
                pass
    except Exception as e:
        print(f"  WARN: could not list pricing CSVs from GCS: {e}")
    return result


def _download_historical_decisions(brand: str, bucket) -> dict[str, dict]:
    """Download decisions JSONs from GCS. Returns {week_str: decisions_dict}."""
    prefix = f"decisions/{brand.lower()}/decisions_"
    result = {}
    if bucket is None:
        # Local fallback
        local_dir = PROJECT_ROOT / "decisions" / brand.lower()
        if local_dir.exists():
            for f in sorted(local_dir.glob("decisions_*.json")):
                week = f.stem.replace("decisions_", "")
                try:
                    with open(f) as fh:
                        result[week] = json.load(fh)
                except Exception:
                    pass
        return result

    try:
        for blob in bucket.list_blobs(prefix=prefix):
            week = blob.name.split("decisions_")[1].replace(".json", "")
            try:
                result[week] = json.loads(blob.download_as_text())
            except Exception:
                pass
    except Exception as e:
        print(f"  WARN: could not list decisions from GCS: {e}")
    return result


def _load_costs(brand: str, bucket) -> dict[str, float]:
    """Load cost map {sku: cost_clp}. Tries GCS then local."""
    costs_path = PROJECT_ROOT / "data" / "raw" / brand.lower() / "costs.parquet"

    # Try GCS download if not on disk
    if not costs_path.exists() and bucket is not None:
        try:
            blob = bucket.blob(f"data/raw/{brand.lower()}/costs.parquet")
            if blob.exists():
                costs_path.parent.mkdir(parents=True, exist_ok=True)
                blob.download_to_filename(str(costs_path))
        except Exception:
            pass

    if not costs_path.exists():
        return {}

    try:
        df = pd.read_parquet(costs_path)
        return dict(zip(df["sku"].astype(str), df["cost"].astype(float)))
    except Exception:
        return {}


def _load_sku_parent_map(brand: str) -> dict[str, str]:
    """Load child SKU -> parent SKU mapping from products.parquet."""
    products_path = PROJECT_ROOT / "data" / "raw" / brand.lower() / "products.parquet"
    if not products_path.exists():
        return {}
    try:
        prods = pd.read_parquet(products_path, columns=["material", "codigo_padre"])
        prods = prods.dropna(subset=["codigo_padre"])
        return dict(zip(prods["material"].astype(str), prods["codigo_padre"].astype(str)))
    except Exception:
        return {}


def _compute_actual_metrics(
    txn: pd.DataFrame,
    sku_parent_map: dict,
    decision_week_start: pd.Timestamp,
    lookforward_weeks: int = 4,
) -> pd.DataFrame:
    """
    Aggregate child-level transactions to parent-level weekly actuals.

    For a decision made in week W, we measure actuals starting at W+7 days
    through W+7*lookforward_weeks days.

    Returns DataFrame with columns:
        codigo_padre, centro, actual_units, actual_revenue, actual_velocity, n_weeks_observed
    """
    measure_start = decision_week_start + timedelta(days=7)
    measure_end = decision_week_start + timedelta(days=7 * (lookforward_weeks + 1))

    mask = (txn["fecha"] >= measure_start) & (txn["fecha"] < measure_end)
    window = txn[mask].copy()
    if len(window) == 0:
        return pd.DataFrame()

    # Map child SKUs to parent
    if "codigo_padre" not in window.columns:
        window["codigo_padre"] = window["sku"].astype(str).map(sku_parent_map)
    window = window.dropna(subset=["codigo_padre"])
    if len(window) == 0:
        return pd.DataFrame()

    # Assign week number for each transaction relative to measure_start
    window["_week_num"] = ((window["fecha"] - measure_start).dt.days // 7).clip(lower=0)

    # Compute revenue column before groupby (safer than lambda closing over outer df)
    window["_revenue"] = window["precio_final"] * window["cantidad"]

    # Cast centro to string for consistent joins with pricing CSV store column
    window["centro"] = window["centro"].astype(str)

    # Aggregate to parent + store
    agg = window.groupby(["codigo_padre", "centro"]).agg(
        actual_units=("cantidad", "sum"),
        actual_revenue=("_revenue", "sum"),
        n_weeks_observed=("_week_num", "nunique"),
    ).reset_index()

    # Compute weekly velocity = total units / weeks observed
    agg["actual_velocity"] = agg["actual_units"] / agg["n_weeks_observed"].clip(lower=1)
    agg["actual_weekly_rev"] = agg["actual_revenue"] / agg["n_weeks_observed"].clip(lower=1)

    return agg


def compute_outcomes_for_brand(brand: str, lookback_weeks: int = 4) -> pd.DataFrame | None:
    """
    Compare past predictions to actual outcomes.

    For each of the last `lookback_weeks` decision weeks, joins predictions
    with post-decision actuals to compute velocity error, revenue lift, etc.

    Returns DataFrame or None if no historical decisions exist.
    """
    brand = brand.upper()
    print(f"  Computing outcomes for {brand} (lookback={lookback_weeks} weeks)")

    # 1. Load fresh transactions from local disk (just extracted by pipeline)
    txn_path = PROJECT_ROOT / "data" / "raw" / brand.lower() / "transactions.parquet"
    if not txn_path.exists():
        print(f"  No transactions found at {txn_path} — skipping outcomes")
        return None

    txn = pd.read_parquet(txn_path)
    txn["fecha"] = pd.to_datetime(txn["fecha"])

    # 2. Load supporting data
    bucket = _gcs_bucket()
    cost_map = _load_costs(brand, bucket)
    sku_parent_map = _load_sku_parent_map(brand)

    # 3. Download historical pricing CSVs and decisions from GCS
    csv_map = _download_historical_csvs(brand, bucket)
    dec_map = _download_historical_decisions(brand, bucket)

    if not csv_map or not dec_map:
        print(f"  No historical CSVs or decisions found — skipping outcomes")
        return None

    # 4. Determine which weeks to evaluate (W-1 to W-lookback_weeks)
    all_weeks = sorted(set(csv_map.keys()) & set(dec_map.keys()))
    if not all_weeks:
        print(f"  No overlapping weeks between CSVs and decisions — skipping")
        return None

    # Take the most recent lookback_weeks (excluding the current/latest week which
    # won't have enough post-decision actuals yet)
    eval_weeks = all_weeks[-(lookback_weeks + 1):-1] if len(all_weeks) > 1 else []
    if not eval_weeks:
        # If only 1 week, try it (might have partial data)
        eval_weeks = all_weeks[-1:]

    print(f"  Evaluating {len(eval_weeks)} weeks: {eval_weeks}")

    # 5. Process each decision week
    all_outcomes = []

    for week_str in eval_weeks:
        csv_df = csv_map[week_str]
        dec_data = dec_map[week_str]
        decisions = dec_data.get("decisions", {})
        if not decisions:
            continue

        # Parse week start date
        try:
            week_start = pd.Timestamp(week_str)
        except Exception:
            continue

        # Get actuals for this decision week
        actuals = _compute_actual_metrics(txn, sku_parent_map, week_start)
        if len(actuals) == 0:
            continue

        # Process each decided action
        for _, row in csv_df.iterrows():
            parent_sku = str(row.get("parent_sku", ""))
            store = str(row.get("store", ""))
            key = f"{parent_sku}-{store}"

            dec = decisions.get(key, {})
            if not isinstance(dec, dict):
                continue
            status = dec.get("status", "")
            if status not in IMPLEMENTED_STATUSES:
                continue

            # Implemented price: manual_price if set, else recommended_price
            manual_price = dec.get("manual_price")
            implemented_price = manual_price if manual_price is not None else row.get("recommended_price")
            if pd.isna(implemented_price) or implemented_price is None:
                continue
            implemented_price = float(implemented_price)

            # Predicted values from CSV
            pred_velocity = _safe_float(row.get("expected_velocity"))
            pred_weekly_rev = _safe_float(row.get("expected_weekly_rev"))
            baseline_velocity = _safe_float(row.get("current_velocity"))
            baseline_weekly_rev = _safe_float(row.get("current_weekly_rev"))
            confidence_tier = row.get("confidence_tier", "LOW")
            action_type = row.get("action_type", "decrease")

            # Find actual metrics for this parent+store
            actual_row = actuals[
                (actuals["codigo_padre"] == parent_sku) & (actuals["centro"] == store)
            ]
            if len(actual_row) == 0:
                continue
            actual_row = actual_row.iloc[0]

            actual_units = float(actual_row["actual_units"])
            actual_velocity = float(actual_row["actual_velocity"])
            actual_weekly_rev = float(actual_row["actual_weekly_rev"])
            n_weeks = int(actual_row["n_weeks_observed"])

            # Sparse data guard
            data_quality = "normal"
            if actual_units < 3:
                data_quality = "sparse"
                actual_velocity = None
                actual_weekly_rev = None

            # Compute deltas (only for non-sparse data)
            velocity_error = None
            velocity_error_pct = None
            rev_error = None
            rev_error_pct = None
            actual_lift_vs_baseline = None
            predicted_lift_vs_baseline = None
            direction_correct = None

            if data_quality != "sparse" and pred_velocity is not None and pred_velocity > 0:
                velocity_error = actual_velocity - pred_velocity
                velocity_error_pct = velocity_error / pred_velocity * 100

            if data_quality != "sparse" and pred_weekly_rev is not None and pred_weekly_rev > 0:
                rev_error = actual_weekly_rev - pred_weekly_rev
                rev_error_pct = rev_error / pred_weekly_rev * 100

            if data_quality != "sparse" and baseline_velocity is not None and baseline_velocity > 0:
                actual_lift_vs_baseline = (actual_velocity - baseline_velocity) / baseline_velocity * 100

            if pred_velocity is not None and baseline_velocity is not None and baseline_velocity > 0:
                predicted_lift_vs_baseline = (pred_velocity - baseline_velocity) / baseline_velocity * 100

            if (data_quality != "sparse" and actual_lift_vs_baseline is not None
                    and predicted_lift_vs_baseline is not None):
                # Direction correct = both positive or both negative (or both zero)
                direction_correct = (
                    (actual_lift_vs_baseline >= 0 and predicted_lift_vs_baseline >= 0) or
                    (actual_lift_vs_baseline < 0 and predicted_lift_vs_baseline < 0)
                )

            # Margin (IVA-stripped)
            unit_cost = cost_map.get(parent_sku)
            actual_margin_pct = None
            if unit_cost and implemented_price > 0:
                neto = implemented_price / 1.19
                if neto > 0:
                    actual_margin_pct = round((neto - unit_cost) / neto * 100, 1)

            all_outcomes.append({
                "decision_week": week_str,
                "parent_sku": parent_sku,
                "store": store,
                "action_type": action_type,
                "confidence_tier": confidence_tier,
                "status": status,
                "implemented_price": int(implemented_price),
                "baseline_velocity": baseline_velocity,
                "predicted_velocity": pred_velocity,
                "actual_velocity": actual_velocity,
                "velocity_error": round(velocity_error, 2) if velocity_error is not None else None,
                "velocity_error_pct": round(velocity_error_pct, 1) if velocity_error_pct is not None else None,
                "baseline_weekly_rev": baseline_weekly_rev,
                "predicted_weekly_rev": pred_weekly_rev,
                "actual_weekly_rev": round(actual_weekly_rev, 0) if actual_weekly_rev is not None else None,
                "rev_error": round(rev_error, 0) if rev_error is not None else None,
                "rev_error_pct": round(rev_error_pct, 1) if rev_error_pct is not None else None,
                "actual_lift_vs_baseline": round(actual_lift_vs_baseline, 1) if actual_lift_vs_baseline is not None else None,
                "predicted_lift_vs_baseline": round(predicted_lift_vs_baseline, 1) if predicted_lift_vs_baseline is not None else None,
                "direction_correct": direction_correct,
                "actual_margin_pct": actual_margin_pct,
                "data_quality": data_quality,
                "n_weeks_observed": n_weeks,
                "actual_units": actual_units,
            })

    if not all_outcomes:
        print(f"  No outcome rows produced — no matching actuals for past decisions")
        return None

    result = pd.DataFrame(all_outcomes)
    print(f"  Outcome results: {len(result)} rows across {result['decision_week'].nunique()} weeks")

    # Summary stats
    valid = result[result["data_quality"] == "normal"]
    if len(valid) > 0:
        med_err = valid["velocity_error_pct"].dropna().median()
        dir_pct = valid["direction_correct"].dropna().mean() * 100
        print(f"  Median velocity error: {med_err:.1f}%")
        print(f"  Direction correct: {dir_pct:.0f}%")

    # Save
    out_dir = PROJECT_ROOT / "data" / "processed" / brand.lower()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "outcome_results.parquet"
    result.to_parquet(out_path, index=False)
    print(f"  Saved to {out_path}")

    return result


def _safe_float(val) -> float | None:
    """Convert to float, returning None for NaN/empty."""
    if val is None or val == "":
        return None
    try:
        f = float(val)
        return f if not np.isnan(f) else None
    except (ValueError, TypeError):
        return None


if __name__ == "__main__":
    brand = sys.argv[1] if len(sys.argv) > 1 else "HOKA"
    compute_outcomes_for_brand(brand)
