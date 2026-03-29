#!/usr/bin/env python3
"""
Store clustering experiment for heterogeneous brands.

Tests whether training separate models per store cluster improves
holdout performance vs. a single model. Designed for BELSPORT (66 stores)
but works on any brand.

Downloads features_parent.parquet from GCS if not available locally.

Usage:
    python scripts/cluster_experiment.py BELSPORT
    python scripts/cluster_experiment.py BOLD --k 2,3,4,5
"""

import argparse
import os
import sys
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import (
    silhouette_score, roc_auc_score, r2_score,
    mean_absolute_error, mean_squared_error,
)

warnings.filterwarnings("ignore")

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

HOLDOUT_WEEKS = 4
EXCLUDE_COLS = [
    "sku", "centro", "week", "codigo_padre", "first_sale_date",
    "will_discount_4w", "future_max_disc_4w", "future_velocity_2w", "velocity_lift",
    "color1", "tercera_jerarquia",
    "should_reprice", "optimal_disc_margin", "optimal_profit",
]
CATEGORICAL_COLS = ["primera_jerarquia", "segunda_jerarquia", "genero", "grupo_etario"]


# ── Data loading ──────────────────────────────────────────────────────────

def load_features(brand: str) -> pd.DataFrame:
    """Load features from local or GCS."""
    local = PROJECT_ROOT / "data" / "processed" / brand.lower() / "features_parent.parquet"
    if local.exists():
        print(f"  Loading from local: {local}")
        return pd.read_parquet(local)

    # Try GCS
    print(f"  Not found locally. Downloading from GCS...")
    try:
        from google.cloud import storage
        os.environ.setdefault("GCS_BUCKET", "ynk-pricing-decisions")
        client = storage.Client()
        bucket = client.bucket(os.environ["GCS_BUCKET"])
        blob = bucket.blob(f"data/processed/{brand.lower()}/features_parent.parquet")
        if blob.exists():
            import io
            data = blob.download_as_bytes()
            # Save locally for next time
            local.parent.mkdir(parents=True, exist_ok=True)
            local.write_bytes(data)
            print(f"  Downloaded and cached: {local} ({len(data)/1e6:.1f} MB)")
            return pd.read_parquet(io.BytesIO(data))
        else:
            print(f"  ERROR: Not found on GCS either. Run the pipeline first.")
            sys.exit(1)
    except Exception as e:
        print(f"  ERROR downloading from GCS: {e}")
        sys.exit(1)


def prepare(df, target):
    """Prepare features (same as train_brand.py)."""
    df = df.dropna(subset=[target]).copy()
    for col in CATEGORICAL_COLS:
        if col in df.columns:
            df[col] = df[col].astype("category").cat.codes
    feat_cols = [c for c in df.columns if c not in EXCLUDE_COLS and c != target and c != "store_cluster"]
    return df[feat_cols], df[target], feat_cols


# ── Store clustering ──────────────────────────────────────────────────────

def cluster_stores(df: pd.DataFrame, k: int) -> dict:
    """Cluster stores by behavioral profile. Returns {store_code: cluster_id}."""
    profile_cols = {
        "avg_velocity": ("velocity_4w", "mean"),
        "std_velocity": ("velocity_4w", "std"),
        "avg_discount": ("discount_rate", "mean"),
        "avg_price": ("avg_precio_final", "mean"),
        "n_skus": ("codigo_padre", "nunique"),
        "pct_discounted": ("has_discount", "mean"),
    }
    # Add optional columns
    if "stock_on_hand" in df.columns:
        profile_cols["avg_stock"] = ("stock_on_hand", "mean")
    if "weeks_of_cover" in df.columns:
        profile_cols["avg_woc"] = ("weeks_of_cover", "mean")

    agg_dict = {name: col for name, col in profile_cols.items()}
    store_profiles = df.groupby("centro").agg(**agg_dict).fillna(0)

    feat_cols = list(store_profiles.columns)
    X_scaled = StandardScaler().fit_transform(store_profiles[feat_cols])
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    store_profiles["cluster"] = km.fit_predict(X_scaled)

    sil = silhouette_score(X_scaled, store_profiles["cluster"])
    return store_profiles["cluster"].to_dict(), store_profiles, sil


# ── Model training & evaluation ───────────────────────────────────────────

def train_and_eval(df_train, df_hold, cls_target, reg_target, mode, label=""):
    """Train classifier + regressor, return holdout metrics."""
    import lightgbm as lgb
    import xgboost as xgb

    results = {}

    # Classifier (XGBoost)
    X_tr, y_tr, fc = prepare(df_train, cls_target)
    X_ho, y_ho, _ = prepare(df_hold, cls_target)
    for c in fc:
        if c not in X_ho.columns:
            X_ho[c] = 0
    X_ho = X_ho[fc]

    if len(X_tr) > 0 and len(X_ho) > 0 and y_tr.nunique() > 1 and y_ho.nunique() > 1:
        pos = float(y_tr.mean())
        cls = xgb.XGBClassifier(
            n_estimators=300, max_depth=6, learning_rate=0.05,
            scale_pos_weight=(1 - pos) / pos if 0 < pos < 1 else 1,
            n_jobs=-1, random_state=42, verbosity=0,
        )
        cls.fit(X_tr, y_tr)
        proba = cls.predict_proba(X_ho)[:, 1]
        results["cls_auc"] = roc_auc_score(y_ho, proba)
        results["cls_proba"] = pd.Series(proba, index=y_ho.index)
    else:
        results["cls_auc"] = None
        results["cls_proba"] = pd.Series(dtype=float)

    # Regressor (LightGBM)
    if mode == "revenue":
        df_tr_r = df_train[df_train[cls_target] == 1]
        df_ho_r = df_hold[df_hold[cls_target] == 1]
    else:
        df_tr_r = df_train.dropna(subset=[reg_target])
        df_ho_r = df_hold.dropna(subset=[reg_target])

    X_tr_r, y_tr_r, fc_r = prepare(df_tr_r, reg_target)
    X_ho_r, y_ho_r, _ = prepare(df_ho_r, reg_target)
    for c in fc_r:
        if c not in X_ho_r.columns:
            X_ho_r[c] = 0
    X_ho_r = X_ho_r[fc_r]

    if len(X_tr_r) > 50 and len(X_ho_r) > 10:
        reg = lgb.LGBMRegressor(
            n_estimators=500, max_depth=7, learning_rate=0.03,
            reg_alpha=0.1, reg_lambda=1.0, n_jobs=-1, random_state=42, verbose=-1,
        )
        reg.fit(X_tr_r, y_tr_r, callbacks=[lgb.log_evaluation(-1)])
        pred = reg.predict(X_ho_r)
        results["reg_r2"] = r2_score(y_ho_r, pred)
        results["reg_mae"] = mean_absolute_error(y_ho_r, pred)
        results["reg_pred"] = pd.Series(pred, index=y_ho_r.index)
    else:
        results["reg_r2"] = None
        results["reg_mae"] = None
        results["reg_pred"] = pd.Series(dtype=float)

    return results


# ── Main experiment ───────────────────────────────────────────────────────

def run_experiment(brand: str, k_values: list[int]):
    print(f"\n{'='*70}")
    print(f"  STORE CLUSTERING EXPERIMENT — {brand}")
    print(f"{'='*70}")

    df = load_features(brand)
    n_stores = df["centro"].nunique()
    print(f"  {len(df):,} rows, {n_stores} stores, {df['week'].nunique()} weeks")

    # Detect mode
    has_margin = "should_reprice" in df.columns and df["should_reprice"].notna().sum() >= 100
    if has_margin:
        cls_target, reg_target, mode = "should_reprice", "optimal_disc_margin", "margin"
    else:
        cls_target, reg_target, mode = "will_discount_4w", "future_max_disc_4w", "revenue"
    print(f"  Mode: {mode}")

    # Train/holdout split
    weeks = sorted(df["week"].dropna().unique())
    holdout_cutoff = weeks[-HOLDOUT_WEEKS]
    df_train = df[df["week"] < holdout_cutoff]
    df_hold = df[df["week"] >= holdout_cutoff]
    print(f"  Train: {len(df_train):,} | Holdout: {len(df_hold):,}")

    # ── Baseline: single model ──
    print(f"\n  BASELINE (single model)...")
    t0 = time.time()
    baseline = train_and_eval(df_train, df_hold, cls_target, reg_target, mode)
    t_base = time.time() - t0
    print(f"    AUC={baseline['cls_auc']:.4f}  R²={baseline['reg_r2']:.4f}  MAE={baseline['reg_mae']*100:.1f}pp  [{t_base:.0f}s]")

    # ── Cluster + feature approach ──
    all_results = [{"approach": "Single model", "k": "-",
                    "AUC": baseline["cls_auc"], "R²": baseline["reg_r2"],
                    "MAE_pp": round(baseline["reg_mae"] * 100, 1), "time_s": round(t_base)}]

    for k in k_values:
        if k >= n_stores:
            continue

        store_map, profiles, sil = cluster_stores(df_train, k)
        cluster_sizes = profiles["cluster"].value_counts().sort_index()

        print(f"\n  K={k} CLUSTERS (silhouette={sil:.3f}):")
        for cl, count in cluster_sizes.items():
            grp = profiles[profiles["cluster"] == cl]
            print(f"    Cluster {cl}: {count} stores, vel={grp['avg_velocity'].mean():.2f}, "
                  f"disc={grp['avg_discount'].mean():.1%}")

        # ── Approach A: cluster as feature ──
        df_train_cf = df_train.copy()
        df_hold_cf = df_hold.copy()
        df_train_cf["store_cluster"] = df_train_cf["centro"].map(store_map).fillna(-1).astype(int)
        df_hold_cf["store_cluster"] = df_hold_cf["centro"].map(store_map).fillna(-1).astype(int)

        t0 = time.time()
        res_feat = train_and_eval(df_train_cf, df_hold_cf, cls_target, reg_target, mode)
        t_feat = time.time() - t0
        if res_feat["cls_auc"] and res_feat["reg_r2"]:
            print(f"    + cluster feature:  AUC={res_feat['cls_auc']:.4f}  R²={res_feat['reg_r2']:.4f}  "
                  f"MAE={res_feat['reg_mae']*100:.1f}pp  [{t_feat:.0f}s]")
            all_results.append({"approach": f"K={k} as feature", "k": k,
                                "AUC": res_feat["cls_auc"], "R²": res_feat["reg_r2"],
                                "MAE_pp": round(res_feat["reg_mae"] * 100, 1), "time_s": round(t_feat)})

        # ── Approach B: separate models per cluster ──
        t0 = time.time()
        all_proba = pd.Series(dtype=float)
        all_pred = pd.Series(dtype=float)
        all_y_cls = pd.Series(dtype=float)
        all_y_reg = pd.Series(dtype=float)

        for cl in sorted(cluster_sizes.index):
            cl_stores = [s for s, c in store_map.items() if c == cl]
            d_tr = df_train[df_train["centro"].isin(cl_stores)]
            d_ho = df_hold[df_hold["centro"].isin(cl_stores)]
            if len(d_ho) < 20:
                continue

            res_cl = train_and_eval(d_tr, d_ho, cls_target, reg_target, mode)
            if res_cl["cls_proba"] is not None:
                all_proba = pd.concat([all_proba, res_cl["cls_proba"]])
            if res_cl["reg_pred"] is not None:
                all_pred = pd.concat([all_pred, res_cl["reg_pred"]])

        t_sep = time.time() - t0

        # Global metrics from per-cluster predictions
        _, y_ho_cls, _ = prepare(df_hold, cls_target)
        if mode == "revenue":
            _, y_ho_reg, _ = prepare(df_hold[df_hold[cls_target] == 1], reg_target)
        else:
            _, y_ho_reg, _ = prepare(df_hold.dropna(subset=[reg_target]), reg_target)

        valid_cls = all_proba.dropna()
        valid_reg = all_pred.dropna()

        if len(valid_cls) > 0 and len(valid_reg) > 0:
            auc_sep = roc_auc_score(y_ho_cls.loc[valid_cls.index], valid_cls)
            r2_sep = r2_score(y_ho_reg.loc[valid_reg.index], valid_reg)
            mae_sep = mean_absolute_error(y_ho_reg.loc[valid_reg.index], valid_reg)
            print(f"    Separate models:   AUC={auc_sep:.4f}  R²={r2_sep:.4f}  "
                  f"MAE={mae_sep*100:.1f}pp  [{t_sep:.0f}s]")
            all_results.append({"approach": f"K={k} separate", "k": k,
                                "AUC": auc_sep, "R²": r2_sep,
                                "MAE_pp": round(mae_sep * 100, 1), "time_s": round(t_sep)})

    # ── Summary table ──
    print(f"\n{'='*70}")
    print(f"  RESULTS SUMMARY — {brand}")
    print(f"{'='*70}")
    print(f"  {'Approach':<25s} {'AUC':>8s} {'R²':>8s} {'MAE':>7s} {'Time':>6s}")
    print(f"  {'-'*54}")
    for r in all_results:
        auc_s = f"{r['AUC']:.4f}" if r["AUC"] else "  N/A "
        r2_s = f"{r['R²']:.4f}" if r["R²"] else "  N/A "
        print(f"  {r['approach']:<25s} {auc_s:>8s} {r2_s:>8s} {r['MAE_pp']:>6.1f}pp {r['time_s']:>5}s")

    # Best approach
    valid = [r for r in all_results if r["R²"] is not None]
    if valid:
        best = max(valid, key=lambda r: r["R²"])
        base_r2 = all_results[0]["R²"]
        delta = best["R²"] - base_r2
        print(f"\n  Best regressor: {best['approach']} (R²={best['R²']:.4f}, Δ={delta:+.4f} vs baseline)")
        if delta > 0.005:
            print(f"  ✓ Clustering helps! Consider implementing for production.")
        else:
            print(f"  ✗ No significant improvement from clustering.")


def main():
    parser = argparse.ArgumentParser(description="Store clustering experiment")
    parser.add_argument("brand", help="Brand name (e.g., BELSPORT)")
    parser.add_argument("--k", default="2,3,4,5", help="Comma-separated K values to test")
    args = parser.parse_args()
    k_values = [int(x) for x in args.k.split(",")]
    run_experiment(args.brand.upper(), k_values)


if __name__ == "__main__":
    main()
