#!/usr/bin/env python3
"""
Model comparison: XGBoost vs LightGBM vs CatBoost.

Standalone local script — does NOT modify the pipeline.
Reuses the same data prep and holdout split as train_brand.py,
then benchmarks all three frameworks on the same holdout set.

Usage:
    python scripts/compare_models.py HOKA
    python scripts/compare_models.py BOLD
    python scripts/compare_models.py --all
"""

import argparse
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import (
    roc_auc_score, average_precision_score, mean_absolute_error,
    mean_squared_error, r2_score, precision_score, recall_score,
)

warnings.filterwarnings("ignore", category=UserWarning)

PROJECT_ROOT = Path(__file__).parent.parent
HOLDOUT_WEEKS = 4

EXCLUDE_COLS = [
    "sku", "centro", "week", "codigo_padre", "first_sale_date",
    "will_discount_4w", "future_max_disc_4w", "future_velocity_2w", "velocity_lift",
    "color1", "tercera_jerarquia",
    "should_reprice", "optimal_disc_margin", "optimal_profit",
]
CATEGORICAL_COLS = ["primera_jerarquia", "segunda_jerarquia", "genero", "grupo_etario"]


# ── Data prep (same as train_brand.py) ────────────────────────────────────

def load_data(brand: str):
    """Load features and split into train/holdout."""
    data_dir = PROJECT_ROOT / "data" / "processed" / brand.lower()
    fp = data_dir / "features_parent.parquet"
    if not fp.exists():
        fp = data_dir / "features_v2.parquet"
    df = pd.read_parquet(fp)

    # Detect training mode
    has_margin = ("should_reprice" in df.columns and
                  df["should_reprice"].notna().sum() >= 100)
    if has_margin:
        cls_target, reg_target = "should_reprice", "optimal_disc_margin"
        mode = "margin"
    else:
        cls_target, reg_target = "will_discount_4w", "future_max_disc_4w"
        mode = "revenue"

    # Train/holdout split
    all_weeks = sorted(df["week"].dropna().unique())
    if len(all_weeks) > HOLDOUT_WEEKS + 4:
        holdout_cutoff = all_weeks[-HOLDOUT_WEEKS]
        df_train = df[df["week"] < holdout_cutoff].copy()
        df_holdout = df[df["week"] >= holdout_cutoff].copy()
    else:
        df_train = df
        df_holdout = None

    return df_train, df_holdout, cls_target, reg_target, mode


def prepare(df, target):
    """Prepare features and target (same encoding as train_brand.py)."""
    df = df.dropna(subset=[target]).copy()
    cat_cols_present = []
    for col in CATEGORICAL_COLS:
        if col in df.columns:
            df[col] = df[col].astype("category").cat.codes
            cat_cols_present.append(col)
    feat_cols = [c for c in df.columns if c not in EXCLUDE_COLS and c != target]
    return df[feat_cols], df[target], feat_cols, cat_cols_present


# ── Model factories ───────────────────────────────────────────────────────

def make_xgb(is_cls, params):
    import xgboost as xgb
    if is_cls:
        return xgb.XGBClassifier(**params)
    return xgb.XGBRegressor(**params)


def make_lgbm(is_cls, params):
    import lightgbm as lgb
    if is_cls:
        return lgb.LGBMClassifier(**params)
    return lgb.LGBMRegressor(**params)


def make_catboost(is_cls, params):
    from catboost import CatBoostClassifier, CatBoostRegressor
    if is_cls:
        return CatBoostClassifier(**params)
    return CatBoostRegressor(**params)


# ── Hyperparameter configs (comparable across frameworks) ─────────────────

def get_params(framework, is_cls, n_samples, pos_rate=None):
    """Get comparable hyperparameters for each framework."""
    # Adapt for large datasets
    large = n_samples > 500_000

    if framework == "xgboost":
        base = {
            "n_estimators": 400 if large else 300,
            "max_depth": 7 if large else 6,
            "learning_rate": 0.05,
            "subsample": 0.5 if large else 0.8,
            "colsample_bytree": 0.6 if large else 0.8,
            "n_jobs": -1,
            "random_state": 42,
            "verbosity": 0,
        }
        if is_cls and pos_rate:
            base["scale_pos_weight"] = (1 - pos_rate) / pos_rate
        if not is_cls:
            base.update({"n_estimators": 500, "max_depth": 7, "learning_rate": 0.03,
                         "reg_alpha": 0.1, "reg_lambda": 1.0,
                         "colsample_bytree": 0.5 if large else 0.7})
        return base

    elif framework == "lightgbm":
        base = {
            "n_estimators": 400 if large else 300,
            "max_depth": 7 if large else 6,
            "learning_rate": 0.05,
            "subsample": 0.5 if large else 0.8,
            "colsample_bytree": 0.6 if large else 0.8,
            "n_jobs": -1,
            "random_state": 42,
            "verbose": -1,
        }
        if is_cls and pos_rate:
            base["scale_pos_weight"] = (1 - pos_rate) / pos_rate
        if not is_cls:
            base.update({"n_estimators": 500, "max_depth": 7, "learning_rate": 0.03,
                         "reg_alpha": 0.1, "reg_lambda": 1.0,
                         "colsample_bytree": 0.5 if large else 0.7})
        return base

    elif framework == "catboost":
        base = {
            "iterations": 400 if large else 300,
            "depth": 7 if large else 6,
            "learning_rate": 0.05,
            "subsample": 0.5 if large else 0.8,
            "random_seed": 42,
            "verbose": 0,
            "thread_count": -1,
        }
        if is_cls and pos_rate:
            base["auto_class_weights"] = "Balanced"
        if not is_cls:
            base.update({"iterations": 500, "depth": 7, "learning_rate": 0.03,
                         "l2_leaf_reg": 1.0})
        return base


FACTORIES = {
    "xgboost": make_xgb,
    "lightgbm": make_lgbm,
    "catboost": make_catboost,
}


# ── Evaluation ────────────────────────────────────────────────────────────

def eval_classifier(model, X_test, y_test):
    proba = model.predict_proba(X_test)[:, 1]
    pred = (proba >= 0.5).astype(int)
    return {
        "AUC": roc_auc_score(y_test, proba),
        "AP": average_precision_score(y_test, proba),
        "Precision": precision_score(y_test, pred, zero_division=0),
        "Recall": recall_score(y_test, pred, zero_division=0),
    }


def eval_regressor(model, X_test, y_test):
    pred = model.predict(X_test)
    return {
        "R²": r2_score(y_test, pred),
        "MAE (pp)": round(mean_absolute_error(y_test, pred) * 100, 1),
        "RMSE": round(np.sqrt(mean_squared_error(y_test, pred)), 4),
    }


# ── Main comparison ───────────────────────────────────────────────────────

def compare_brand(brand: str):
    print(f"\n{'='*70}")
    print(f"  MODEL COMPARISON — {brand}")
    print(f"{'='*70}")

    # Load data
    df_train, df_holdout, cls_target, reg_target, mode = load_data(brand)
    print(f"  Mode: {mode} | Train: {len(df_train):,} rows | Holdout: {len(df_holdout) if df_holdout is not None else 0:,} rows")

    if df_holdout is None or len(df_holdout) < 50:
        print("  ⚠ Not enough holdout data for comparison. Skipping.")
        return None

    # ── Classifier ────────────────────────────────────────────────────
    print(f"\n  CLASSIFIER (target: {cls_target})")
    print(f"  {'-'*60}")

    X_train_c, y_train_c, fcols_c, cat_cols = prepare(df_train, cls_target)
    X_hold_c, y_hold_c, _, _ = prepare(df_holdout, cls_target)
    # Align columns
    for c in fcols_c:
        if c not in X_hold_c.columns:
            X_hold_c[c] = 0
    X_hold_c = X_hold_c[fcols_c]

    pos_rate = float(y_train_c.mean())
    n_samples = len(X_train_c)
    print(f"  Samples: {n_samples:,} train, {len(X_hold_c):,} holdout | Pos rate: {pos_rate:.1%}")

    cls_results = {}
    for fw_name in ["xgboost", "lightgbm", "catboost"]:
        params = get_params(fw_name, is_cls=True, n_samples=n_samples, pos_rate=pos_rate)
        model = FACTORIES[fw_name](is_cls=True, params=params)
        t0 = time.time()
        try:
            model.fit(X_train_c, y_train_c)
            elapsed = time.time() - t0
            metrics = eval_classifier(model, X_hold_c, y_hold_c)
            metrics["Time (s)"] = round(elapsed, 1)
            cls_results[fw_name] = metrics
            print(f"  {fw_name:12s} | AUC={metrics['AUC']:.4f}  AP={metrics['AP']:.4f}  "
                  f"P={metrics['Precision']:.3f}  R={metrics['Recall']:.3f}  [{elapsed:.1f}s]")
        except Exception as e:
            print(f"  {fw_name:12s} | ERROR: {e}")
            cls_results[fw_name] = {"error": str(e)}

    # ── Regressor ─────────────────────────────────────────────────────
    print(f"\n  REGRESSOR (target: {reg_target})")
    print(f"  {'-'*60}")

    # Regressor only on positive/reprice samples
    if mode == "margin":
        df_train_r = df_train.dropna(subset=[reg_target])
        df_hold_r = df_holdout.dropna(subset=[reg_target]) if df_holdout is not None else None
    else:
        df_train_r = df_train[df_train[cls_target] == 1]
        df_hold_r = df_holdout[df_holdout[cls_target] == 1] if df_holdout is not None else None

    if df_hold_r is None or len(df_hold_r) < 20:
        print("  ⚠ Not enough regressor holdout data. Skipping.")
        return {"brand": brand, "classifier": cls_results, "regressor": {}}

    X_train_r, y_train_r, fcols_r, _ = prepare(df_train_r, reg_target)
    X_hold_r, y_hold_r, _, _ = prepare(df_hold_r, reg_target)
    for c in fcols_r:
        if c not in X_hold_r.columns:
            X_hold_r[c] = 0
    X_hold_r = X_hold_r[fcols_r]

    print(f"  Samples: {len(X_train_r):,} train, {len(X_hold_r):,} holdout")

    reg_results = {}
    for fw_name in ["xgboost", "lightgbm", "catboost"]:
        params = get_params(fw_name, is_cls=False, n_samples=len(X_train_r))
        model = FACTORIES[fw_name](is_cls=False, params=params)
        t0 = time.time()
        try:
            model.fit(X_train_r, y_train_r)
            elapsed = time.time() - t0
            metrics = eval_regressor(model, X_hold_r, y_hold_r)
            metrics["Time (s)"] = round(elapsed, 1)
            reg_results[fw_name] = metrics
            print(f"  {fw_name:12s} | R²={metrics['R²']:.4f}  MAE={metrics['MAE (pp)']}pp  "
                  f"RMSE={metrics['RMSE']}  [{elapsed:.1f}s]")
        except Exception as e:
            print(f"  {fw_name:12s} | ERROR: {e}")
            reg_results[fw_name] = {"error": str(e)}

    # ── Summary ───────────────────────────────────────────────────────
    print(f"\n  {'='*60}")
    print(f"  WINNER SUMMARY — {brand}")
    print(f"  {'='*60}")

    # Best classifier by AUC
    valid_cls = {k: v for k, v in cls_results.items() if "AUC" in v}
    if valid_cls:
        best_cls = max(valid_cls, key=lambda k: valid_cls[k]["AUC"])
        print(f"  Classifier: {best_cls.upper()} (AUC={valid_cls[best_cls]['AUC']:.4f})")
        for k, v in valid_cls.items():
            if k != best_cls:
                delta = valid_cls[best_cls]["AUC"] - v["AUC"]
                print(f"    vs {k}: +{delta:.4f} AUC")

    # Best regressor by R²
    valid_reg = {k: v for k, v in reg_results.items() if "R²" in v}
    if valid_reg:
        best_reg = max(valid_reg, key=lambda k: valid_reg[k]["R²"])
        print(f"  Regressor:  {best_reg.upper()} (R²={valid_reg[best_reg]['R²']:.4f})")
        for k, v in valid_reg.items():
            if k != best_reg:
                delta = valid_reg[best_reg]["R²"] - v["R²"]
                print(f"    vs {k}: +{delta:.4f} R²")

    return {"brand": brand, "classifier": cls_results, "regressor": reg_results}


def main():
    parser = argparse.ArgumentParser(description="Compare XGBoost vs LightGBM vs CatBoost")
    parser.add_argument("brand", nargs="?", help="Brand name (e.g., HOKA, BOLD)")
    parser.add_argument("--all", action="store_true", help="Run all brands")
    args = parser.parse_args()

    brands = ["HOKA", "BOLD", "BAMERS", "OAKLEY"] if args.all else [args.brand.upper()]

    if not brands[0]:
        parser.error("Provide a brand name or use --all")

    all_results = []
    for brand in brands:
        result = compare_brand(brand)
        if result:
            all_results.append(result)

    if len(all_results) > 1:
        print(f"\n\n{'='*70}")
        print("  CROSS-BRAND SUMMARY")
        print(f"{'='*70}")
        for r in all_results:
            b = r["brand"]
            cls = {k: v for k, v in r["classifier"].items() if "AUC" in v}
            reg = {k: v for k, v in r["regressor"].items() if "R²" in v}
            best_c = max(cls, key=lambda k: cls[k]["AUC"]) if cls else "?"
            best_r = max(reg, key=lambda k: reg[k]["R²"]) if reg else "?"
            print(f"  {b:10s} | Cls: {best_c:10s} | Reg: {best_r:10s}")


if __name__ == "__main__":
    main()
