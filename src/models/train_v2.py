"""
Retrain models on enhanced v2 features.
Compares performance vs. v1 baseline.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import roc_auc_score, average_precision_score, precision_score, recall_score, f1_score, mean_absolute_error, mean_squared_error, r2_score
import shap
import pickle
import json
from pathlib import Path

PROCESSED_DIR = Path(__file__).parent.parent.parent / "data" / "processed"
MODEL_DIR = Path(__file__).parent.parent.parent / "models"

EXCLUDE_COLS = [
    "sku", "centro", "week", "codigo_padre", "first_sale_date",
    "will_discount_4w", "future_max_disc_4w", "future_velocity_2w", "velocity_lift",
    "color1", "tercera_jerarquia",
]

CATEGORICAL_COLS = ["primera_jerarquia", "segunda_jerarquia", "genero", "grupo_etario"]


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


def run_v2_training():
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading v2 features...")
    df = pd.read_parquet(PROCESSED_DIR / "hoka_features_v2.parquet")
    print(f"  {len(df):,} rows × {len(df.columns)} cols")

    # Load v1 baseline for comparison
    try:
        with open(MODEL_DIR / "training_metadata.json") as f:
            v1_meta = json.load(f)
        v1_auc = v1_meta["classifier"]["avg_auc"]
        v1_ap = v1_meta["classifier"]["avg_precision"]
        v1_mae = v1_meta["regressor"]["avg_mae"]
        v1_r2 = v1_meta["regressor"]["avg_r2"]
    except FileNotFoundError:
        v1_auc = v1_ap = v1_mae = v1_r2 = None

    # ================================================================
    # Classifier v2
    # ================================================================
    print("\n" + "=" * 60)
    print("MODEL 1 v2: Markdown Probability")
    print("=" * 60)

    X, y, w, fcols = prepare(df, "will_discount_4w")
    print(f"  Features: {len(fcols)} (was 41 in v1)")
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
    if v1_auc: print(f"  (v1: {v1_auc:.3f}, delta: {v2_auc-v1_auc:+.3f})")
    else: print()
    print(f"  Avg AP:   {v2_ap:.3f}", end="")
    if v1_ap: print(f"  (v1: {v1_ap:.3f}, delta: {v2_ap-v1_ap:+.3f})")
    else: print()

    # Train final
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
    shap_cls.to_csv(MODEL_DIR / "classifier_v2_shap.csv", index=False)
    print("\n  Top 10 features:")
    for _, row in shap_cls.head(10).iterrows():
        print(f"    {row['feature']:35s} {row['mean_abs_shap']:.4f}")

    with open(MODEL_DIR / "markdown_classifier_v2.pkl", "wb") as f:
        pickle.dump(cls_model, f)

    # ================================================================
    # Regressor v2
    # ================================================================
    print("\n" + "=" * 60)
    print("MODEL 2 v2: Discount Depth")
    print("=" * 60)

    df_disc = df[df["will_discount_4w"] == 1].copy()
    X, y, w, fcols = prepare(df_disc, "future_max_disc_4w")
    print(f"  Features: {len(fcols)}")
    print(f"  Samples: {len(X):,}")

    reg_params = {
        "n_estimators": 300,
        "max_depth": 5,
        "learning_rate": 0.05,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "early_stopping_rounds": 20,
        "random_state": 42,
    }

    reg_results = ts_cv(X, y, w, reg_params, n_splits=4, is_cls=False)
    v2_mae = np.mean([r["mae"] for r in reg_results])
    v2_r2 = np.mean([r["r2"] for r in reg_results])

    for r in reg_results:
        print(f"  Fold {r['fold']}: MAE={r['mae']:.4f} R²={r['r2']:.3f}")
    print(f"\n  Avg MAE:  {v2_mae:.4f} ({v2_mae*100:.1f}pp)", end="")
    if v1_mae: print(f"  (v1: {v1_mae:.4f}, delta: {v2_mae-v1_mae:+.4f})")
    else: print()
    print(f"  Avg R²:   {v2_r2:.3f}", end="")
    if v1_r2: print(f"  (v1: {v1_r2:.3f}, delta: {v2_r2-v1_r2:+.3f})")
    else: print()

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
    shap_reg.to_csv(MODEL_DIR / "regressor_v2_shap.csv", index=False)
    print("\n  Top 10 features:")
    for _, row in shap_reg.head(10).iterrows():
        print(f"    {row['feature']:35s} {row['mean_abs_shap']:.6f}")

    with open(MODEL_DIR / "depth_regressor_v2.pkl", "wb") as f:
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
            "improvement_vs_v1": {
                "mae_delta": v2_mae - v1_mae if v1_mae else None,
                "r2_delta": v2_r2 - v1_r2 if v1_r2 else None,
            },
        },
        "version": "v2",
        "note": "Enhanced with price elasticity, lifecycle stages, size curve, derived seasons.",
    }
    with open(MODEL_DIR / "training_metadata_v2.json", "w") as f:
        json.dump(meta, f, indent=2, default=str)

    print(f"\n{'=' * 60}")
    print("v2 TRAINING COMPLETE")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    run_v2_training()
