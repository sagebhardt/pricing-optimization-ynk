"""
Brand-agnostic model training on enhanced v2 features.

Thin wrapper around the HOKA-specific logic in train_v2.py,
parameterized by brand name. Reads from data/processed/{brand}/
and writes models to models/{brand}/.

Compares performance vs. v1 baseline when available.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import (
    roc_auc_score, average_precision_score, precision_score,
    recall_score, f1_score, mean_absolute_error, mean_squared_error, r2_score,
)
import shap
import pickle
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent

EXCLUDE_COLS = [
    "sku", "centro", "week", "codigo_padre", "first_sale_date",
    "will_discount_4w", "future_max_disc_4w", "future_velocity_2w", "velocity_lift",
    "color1", "tercera_jerarquia",
    "should_reprice", "optimal_disc_margin", "optimal_profit",
]

CATEGORICAL_COLS = ["primera_jerarquia", "segunda_jerarquia", "genero", "grupo_etario"]


def _processed_dir(brand: str) -> Path:
    return PROJECT_ROOT / "data" / "processed" / brand.lower()


def _model_dir(brand: str) -> Path:
    return PROJECT_ROOT / "models" / brand.lower()


def prepare(df, target):
    df = df.dropna(subset=[target]).copy()
    for col in CATEGORICAL_COLS:
        if col in df.columns:
            df[col] = df[col].astype("category").cat.codes
    feat_cols = [c for c in df.columns if c not in EXCLUDE_COLS and c != target]
    return df[feat_cols], df[target], df["week"], feat_cols


def ts_cv(X, y, weeks, params, n_splits=4, is_cls=True):
    unique_weeks = sorted(weeks.unique())
    fold_size = len(unique_weeks) // (n_splits + 1)
    results = []

    for fold in range(n_splits):
        train_end = (fold + 1) * fold_size
        val_end = (fold + 2) * fold_size
        train_w = set(unique_weeks[:train_end])
        val_w = set(unique_weeks[train_end:val_end])

        tm, vm = weeks.isin(train_w), weeks.isin(val_w)
        Xt, yt = X[tm], y[tm]
        Xv, yv = X[vm], y[vm]

        if len(Xt) == 0 or len(Xv) == 0:
            continue

        if is_cls:
            m = xgb.XGBClassifier(**params)
            m.fit(Xt, yt, eval_set=[(Xv, yv)], verbose=False)
            pp = m.predict_proba(Xv)[:, 1]
            p = m.predict(Xv)
            results.append({
                "fold": fold,
                "auc": roc_auc_score(yv, pp),
                "avg_precision": average_precision_score(yv, pp),
                "precision": precision_score(yv, p, zero_division=0),
                "recall": recall_score(yv, p, zero_division=0),
                "f1": f1_score(yv, p, zero_division=0),
            })
        else:
            m = xgb.XGBRegressor(**params)
            m.fit(Xt, yt, eval_set=[(Xv, yv)], verbose=False)
            p = m.predict(Xv)
            results.append({
                "fold": fold,
                "mae": mean_absolute_error(yv, p),
                "rmse": np.sqrt(mean_squared_error(yv, p)),
                "r2": r2_score(yv, p),
            })

    return results


def train_brand_models(brand: str):
    """Main training pipeline for a given brand."""
    processed = _processed_dir(brand)
    model_dir = _model_dir(brand)
    model_dir.mkdir(parents=True, exist_ok=True)

    # Prefer parent-level features; fall back to child-level v2
    parent_path = processed / "features_parent.parquet"
    child_path = processed / "features_v2.parquet"
    if parent_path.exists():
        print(f"[{brand}] Loading PARENT-level features...")
        df = pd.read_parquet(parent_path)
    else:
        print(f"[{brand}] Loading child-level v2 features (parent not available)...")
        df = pd.read_parquet(child_path)
    print(f"  {len(df):,} rows x {len(df.columns)} cols")

    # Load v1 baseline for comparison (if available)
    v1_auc = v1_ap = v1_mae = v1_r2 = None
    try:
        with open(model_dir / "training_metadata.json") as f:
            v1_meta = json.load(f)
        v1_auc = v1_meta["classifier"]["avg_auc"]
        v1_ap = v1_meta["classifier"]["avg_precision"]
        v1_mae = v1_meta["regressor"]["avg_mae"]
        v1_r2 = v1_meta["regressor"]["avg_r2"]
    except (FileNotFoundError, KeyError):
        pass

    # ================================================================
    # Determine training mode: margin-optimized (prescriptive) or legacy (descriptive)
    # ================================================================
    has_margin_targets = "should_reprice" in df.columns and df["should_reprice"].notna().sum() > 100
    if has_margin_targets:
        cls_target = "should_reprice"
        reg_target = "optimal_disc_margin"
        training_mode = "margin"
        print(f"\n  >>> MARGIN-OPTIMIZED training (costs available)")
        print(f"      Classifier target: should_reprice (prescriptive)")
        print(f"      Regressor target:  optimal_disc_margin (profit-maximizing)")
    else:
        cls_target = "will_discount_4w"
        reg_target = "future_max_disc_4w"
        training_mode = "revenue"
        print(f"\n  >>> REVENUE-BASED training (no costs — using historical markdown patterns)")

    # ================================================================
    # Classifier
    # ================================================================
    print(f"\n{'=' * 60}")
    label = "Should Reprice (Margin)" if has_margin_targets else "Markdown Probability"
    print(f"[{brand}] MODEL 1: {label}")
    print("=" * 60)

    X, y, w, fcols = prepare(df, cls_target)
    print(f"  Features: {len(fcols)}")
    print(f"  Positive rate: {y.mean():.3f}")

    cls_params = {
        "n_estimators": 300,
        "max_depth": 6,
        "learning_rate": 0.05,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "scale_pos_weight": (1 - y.mean()) / y.mean(),
        "early_stopping_rounds": 20,
        "random_state": 42,
    }

    cls_results = ts_cv(X, y, w, cls_params, n_splits=4, is_cls=True)
    v2_auc = np.mean([r["auc"] for r in cls_results])
    v2_ap = np.mean([r["avg_precision"] for r in cls_results])

    for r in cls_results:
        print(f"  Fold {r['fold']}: AUC={r['auc']:.3f} AP={r['avg_precision']:.3f} P={r['precision']:.3f} R={r['recall']:.3f}")
    print(f"\n  Avg AUC:  {v2_auc:.3f}", end="")
    if v1_auc:
        print(f"  (v1: {v1_auc:.3f}, delta: {v2_auc-v1_auc:+.3f})")
    else:
        print()
    print(f"  Avg AP:   {v2_ap:.3f}", end="")
    if v1_ap:
        print(f"  (v1: {v1_ap:.3f}, delta: {v2_ap-v1_ap:+.3f})")
    else:
        print()

    # Train final model
    final_cls_params = {k: v for k, v in cls_params.items() if k != "early_stopping_rounds"}
    cls_model = xgb.XGBClassifier(**final_cls_params)
    cls_model.fit(X, y, verbose=False)

    # SHAP
    explainer = shap.TreeExplainer(cls_model)
    X_sample = X.sample(min(2000, len(X)), random_state=42)
    sv = explainer.shap_values(X_sample)
    shap_cls = pd.DataFrame({
        "feature": fcols,
        "mean_abs_shap": np.abs(sv).mean(axis=0),
    }).sort_values("mean_abs_shap", ascending=False)
    shap_cls.to_csv(model_dir / "classifier_shap.csv", index=False)
    print("\n  Top 10 features:")
    for _, row in shap_cls.head(10).iterrows():
        print(f"    {row['feature']:35s} {row['mean_abs_shap']:.4f}")

    with open(model_dir / "markdown_classifier.pkl", "wb") as f:
        pickle.dump(cls_model, f)

    # ================================================================
    # Regressor: Discount Depth
    # ================================================================
    print(f"\n{'=' * 60}")
    label2 = "Optimal Discount (Margin)" if has_margin_targets else "Discount Depth"
    print(f"[{brand}] MODEL 2: {label2}")
    print("=" * 60)

    if has_margin_targets:
        # Train on ALL rows with margin targets — includes "already optimal" as signal
        df_disc = df[df["optimal_disc_margin"].notna()].copy()
    else:
        df_disc = df[df["will_discount_4w"] == 1].copy()
    X, y, w, fcols = prepare(df_disc, reg_target)
    print(f"  Features: {len(fcols)}")
    print(f"  Samples: {len(X):,}")
    print(f"  Target stats: mean={y.mean():.4f} std={y.std():.4f} min={y.min():.4f} max={y.max():.4f}")
    print(f"  Target distribution: {np.histogram(y, bins=[0,0.05,0.1,0.15,0.2,0.25,0.3,0.35,0.4,1.0])[0]}")

    reg_params = {
        "n_estimators": 500,
        "max_depth": 7,
        "learning_rate": 0.03,
        "subsample": 0.8,
        "colsample_bytree": 0.7,
        "reg_alpha": 0.1,
        "reg_lambda": 1.0,
        "early_stopping_rounds": 30,
        "random_state": 42,
    }

    reg_results = ts_cv(X, y, w, reg_params, n_splits=4, is_cls=False)
    v2_mae = np.mean([r["mae"] for r in reg_results])
    v2_r2 = np.mean([r["r2"] for r in reg_results])

    for r in reg_results:
        print(f"  Fold {r['fold']}: MAE={r['mae']:.4f} R2={r['r2']:.3f}")
    print(f"\n  Avg MAE:  {v2_mae:.4f} ({v2_mae*100:.1f}pp)", end="")
    if v1_mae:
        print(f"  (v1: {v1_mae:.4f}, delta: {v2_mae-v1_mae:+.4f})")
    else:
        print()
    print(f"  Avg R2:   {v2_r2:.3f}", end="")
    if v1_r2:
        print(f"  (v1: {v1_r2:.3f}, delta: {v2_r2-v1_r2:+.3f})")
    else:
        print()

    final_reg_params = {k: v for k, v in reg_params.items() if k != "early_stopping_rounds"}
    reg_model = xgb.XGBRegressor(**final_reg_params)
    reg_model.fit(X, y, verbose=False)

    explainer = shap.TreeExplainer(reg_model)
    X_sample = X.sample(min(2000, len(X)), random_state=42)
    sv = explainer.shap_values(X_sample)
    shap_reg = pd.DataFrame({
        "feature": fcols,
        "mean_abs_shap": np.abs(sv).mean(axis=0),
    }).sort_values("mean_abs_shap", ascending=False)
    shap_reg.to_csv(model_dir / "regressor_shap.csv", index=False)
    print("\n  Top 10 features:")
    for _, row in shap_reg.head(10).iterrows():
        print(f"    {row['feature']:35s} {row['mean_abs_shap']:.6f}")

    with open(model_dir / "depth_regressor.pkl", "wb") as f:
        pickle.dump(reg_model, f)

    # Save metadata
    meta = {
        "classifier": {
            "cv_results": cls_results,
            "avg_auc": v2_auc,
            "avg_precision": v2_ap,
            "n_features": len(X.columns),
            "improvement_vs_v1": {
                "auc_delta": v2_auc - v1_auc if v1_auc else None,
                "ap_delta": v2_ap - v1_ap if v1_ap else None,
            },
        },
        "regressor": {
            "cv_results": reg_results,
            "avg_mae": v2_mae,
            "avg_r2": v2_r2,
            "n_samples": len(X),
            "improvement_vs_v1": {
                "mae_delta": v2_mae - v1_mae if v1_mae else None,
                "r2_delta": v2_r2 - v1_r2 if v1_r2 else None,
            },
        },
        "brand": brand,
        "version": "v2",
        "training_mode": training_mode,
        "classifier_target": cls_target,
        "regressor_target": reg_target,
        "note": f"{'Margin-optimized (prescriptive)' if has_margin_targets else 'Revenue-based (descriptive)'}. Features: elasticity, lifecycle, size curve, seasons, inventory.",
    }
    with open(model_dir / "training_metadata.json", "w") as f:
        json.dump(meta, f, indent=2, default=str)

    print(f"\n{'=' * 60}")
    print(f"[{brand}] TRAINING COMPLETE")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--brand", type=str, required=True, help="Brand name (e.g. HOKA, BOLD)")
    args = parser.parse_args()
    train_brand_models(args.brand)
