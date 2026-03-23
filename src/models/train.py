"""
XGBoost model training for HOKA markdown optimization.

Two models:
1. Classification: Will this SKU-store need a markdown in the next 4 weeks?
2. Regression: What discount depth will be applied?

Uses time-series cross-validation (no random splits).
Includes SHAP explainability.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import (
    roc_auc_score, precision_score, recall_score, f1_score,
    mean_absolute_error, mean_squared_error, r2_score,
    precision_recall_curve, average_precision_score,
)
from sklearn.model_selection import TimeSeriesSplit
import shap
import json
import pickle
from pathlib import Path

PROCESSED_DIR = Path(__file__).parent.parent.parent / "data" / "processed"
MODEL_DIR = Path(__file__).parent.parent.parent / "models"

# Features to use (excluding targets, identifiers, and leak-prone columns)
EXCLUDE_COLS = [
    # Identifiers
    "sku", "centro", "week", "codigo_padre", "first_sale_date",
    # Targets
    "will_discount_4w", "future_max_disc_4w", "future_velocity_2w", "velocity_lift",
    # High-cardinality categoricals (handle separately)
    "color1", "tercera_jerarquia",
]

CATEGORICAL_COLS = [
    "primera_jerarquia", "segunda_jerarquia", "genero", "grupo_etario",
]


def prepare_data(df: pd.DataFrame, target_col: str):
    """Prepare features and target for modeling."""
    df = df.copy()

    # Drop rows where target is missing
    df = df.dropna(subset=[target_col])

    # Encode categoricals
    for col in CATEGORICAL_COLS:
        if col in df.columns:
            df[col] = df[col].astype("category").cat.codes

    # Select feature columns
    feature_cols = [
        c for c in df.columns
        if c not in EXCLUDE_COLS and c != target_col
    ]

    X = df[feature_cols]
    y = df[target_col]
    weeks = df["week"]

    return X, y, weeks, feature_cols


def time_series_cv(X, y, weeks, model_params, n_splits=4, is_classification=True):
    """Time-series cross-validation. Splits by week, not randomly."""
    unique_weeks = sorted(weeks.unique())
    n_weeks = len(unique_weeks)
    fold_size = n_weeks // (n_splits + 1)

    results = []

    for fold in range(n_splits):
        # Train on first (fold+1) chunks, validate on the next chunk
        train_end_idx = (fold + 1) * fold_size
        val_end_idx = (fold + 2) * fold_size

        train_weeks = set(unique_weeks[:train_end_idx])
        val_weeks = set(unique_weeks[train_end_idx:val_end_idx])

        train_mask = weeks.isin(train_weeks)
        val_mask = weeks.isin(val_weeks)

        X_train, y_train = X[train_mask], y[train_mask]
        X_val, y_val = X[val_mask], y[val_mask]

        if len(X_train) == 0 or len(X_val) == 0:
            continue

        if is_classification:
            model = xgb.XGBClassifier(**model_params)
            model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
            y_pred_proba = model.predict_proba(X_val)[:, 1]
            y_pred = model.predict(X_val)

            fold_metrics = {
                "fold": fold,
                "train_weeks": f"{min(train_weeks).date()} to {max(train_weeks).date()}",
                "val_weeks": f"{min(val_weeks).date()} to {max(val_weeks).date()}",
                "train_size": len(X_train),
                "val_size": len(X_val),
                "auc": roc_auc_score(y_val, y_pred_proba),
                "avg_precision": average_precision_score(y_val, y_pred_proba),
                "precision": precision_score(y_val, y_pred, zero_division=0),
                "recall": recall_score(y_val, y_pred, zero_division=0),
                "f1": f1_score(y_val, y_pred, zero_division=0),
                "pos_rate_train": y_train.mean(),
                "pos_rate_val": y_val.mean(),
            }
        else:
            model = xgb.XGBRegressor(**model_params)
            model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
            y_pred = model.predict(X_val)

            fold_metrics = {
                "fold": fold,
                "train_weeks": f"{min(train_weeks).date()} to {max(train_weeks).date()}",
                "val_weeks": f"{min(val_weeks).date()} to {max(val_weeks).date()}",
                "train_size": len(X_train),
                "val_size": len(X_val),
                "mae": mean_absolute_error(y_val, y_pred),
                "rmse": np.sqrt(mean_squared_error(y_val, y_pred)),
                "r2": r2_score(y_val, y_pred),
                "mean_actual": y_val.mean(),
                "mean_predicted": y_pred.mean(),
            }

        results.append(fold_metrics)
        print(f"  Fold {fold}: {fold_metrics}")

    return results


def train_final_model(X, y, model_params, is_classification=True):
    """Train final model on all data."""
    if is_classification:
        model = xgb.XGBClassifier(**model_params)
    else:
        model = xgb.XGBRegressor(**model_params)

    model.fit(X, y, verbose=False)
    return model


def compute_shap_values(model, X, feature_cols, n_samples=2000):
    """Compute SHAP values for model explainability."""
    if len(X) > n_samples:
        X_sample = X.sample(n_samples, random_state=42)
    else:
        X_sample = X

    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_sample)

    # Mean absolute SHAP value per feature
    mean_shap = pd.DataFrame({
        "feature": feature_cols,
        "mean_abs_shap": np.abs(shap_values).mean(axis=0),
    }).sort_values("mean_abs_shap", ascending=False)

    return shap_values, X_sample, mean_shap


def run_training():
    """Main training pipeline."""
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading feature table...")
    df = pd.read_parquet(PROCESSED_DIR / "hoka_features.parquet")
    print(f"  {len(df):,} rows, {df['week'].min().date()} to {df['week'].max().date()}")

    # ================================================================
    # Model 1: Markdown Probability (Classification)
    # ================================================================
    print("\n" + "=" * 60)
    print("MODEL 1: Markdown Probability (will_discount_4w)")
    print("=" * 60)

    X_cls, y_cls, weeks_cls, feature_cols_cls = prepare_data(df, "will_discount_4w")
    print(f"  Features: {len(feature_cols_cls)}")
    print(f"  Positive rate: {y_cls.mean():.3f}")

    cls_params = {
        "n_estimators": 300,
        "max_depth": 6,
        "learning_rate": 0.05,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "scale_pos_weight": (1 - y_cls.mean()) / y_cls.mean(),
        "early_stopping_rounds": 20,
        "random_state": 42,
        "enable_categorical": False,
    }

    print("\nTime-series cross-validation:")
    cls_results = time_series_cv(X_cls, y_cls, weeks_cls, cls_params, n_splits=4, is_classification=True)

    avg_auc = np.mean([r["auc"] for r in cls_results])
    avg_ap = np.mean([r["avg_precision"] for r in cls_results])
    print(f"\n  Average AUC: {avg_auc:.3f}")
    print(f"  Average Precision: {avg_ap:.3f}")

    # Train final model
    print("\nTraining final classifier...")
    final_cls_params = {k: v for k, v in cls_params.items() if k != "early_stopping_rounds"}
    cls_model = train_final_model(X_cls, y_cls, final_cls_params, is_classification=True)

    # SHAP
    print("Computing SHAP values...")
    shap_vals_cls, X_shap_cls, shap_importance_cls = compute_shap_values(
        cls_model, X_cls, feature_cols_cls
    )
    print("\nTop 15 features (by SHAP importance):")
    print(shap_importance_cls.head(15).to_string(index=False))

    # Save
    with open(MODEL_DIR / "markdown_classifier.pkl", "wb") as f:
        pickle.dump(cls_model, f)
    shap_importance_cls.to_csv(MODEL_DIR / "classifier_shap_importance.csv", index=False)

    # ================================================================
    # Model 2: Discount Depth (Regression)
    # ================================================================
    print("\n" + "=" * 60)
    print("MODEL 2: Discount Depth (future_max_disc_4w)")
    print("=" * 60)

    # Only train on rows where a discount will occur
    df_disc = df[df["will_discount_4w"] == 1].copy()
    X_reg, y_reg, weeks_reg, feature_cols_reg = prepare_data(df_disc, "future_max_disc_4w")
    print(f"  Features: {len(feature_cols_reg)}")
    print(f"  Samples: {len(X_reg):,}")
    print(f"  Mean target: {y_reg.mean():.3f}")

    reg_params = {
        "n_estimators": 300,
        "max_depth": 5,
        "learning_rate": 0.05,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "early_stopping_rounds": 20,
        "random_state": 42,
    }

    print("\nTime-series cross-validation:")
    reg_results = time_series_cv(X_reg, y_reg, weeks_reg, reg_params, n_splits=4, is_classification=False)

    avg_mae = np.mean([r["mae"] for r in reg_results])
    avg_r2 = np.mean([r["r2"] for r in reg_results])
    print(f"\n  Average MAE: {avg_mae:.4f}")
    print(f"  Average R²: {avg_r2:.3f}")

    # Train final model
    print("\nTraining final regressor...")
    final_reg_params = {k: v for k, v in reg_params.items() if k != "early_stopping_rounds"}
    reg_model = train_final_model(X_reg, y_reg, final_reg_params, is_classification=False)

    # SHAP
    print("Computing SHAP values...")
    shap_vals_reg, X_shap_reg, shap_importance_reg = compute_shap_values(
        reg_model, X_reg, feature_cols_reg
    )
    print("\nTop 15 features (by SHAP importance):")
    print(shap_importance_reg.head(15).to_string(index=False))

    # Save
    with open(MODEL_DIR / "depth_regressor.pkl", "wb") as f:
        pickle.dump(reg_model, f)
    shap_importance_reg.to_csv(MODEL_DIR / "regressor_shap_importance.csv", index=False)

    # Save metadata
    metadata = {
        "classifier": {
            "cv_results": cls_results,
            "avg_auc": avg_auc,
            "avg_precision": avg_ap,
            "features": feature_cols_cls,
            "n_features": len(feature_cols_cls),
            "n_samples": len(X_cls),
        },
        "regressor": {
            "cv_results": reg_results,
            "avg_mae": avg_mae,
            "avg_r2": avg_r2,
            "features": feature_cols_reg,
            "n_features": len(feature_cols_reg),
            "n_samples": len(X_reg),
        },
        "data_range": f"{df['week'].min().date()} to {df['week'].max().date()}",
        "note": "POC model without inventory or cost data. Targets are revenue-based proxies.",
    }
    with open(MODEL_DIR / "training_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2, default=str)

    print("\n" + "=" * 60)
    print("Training complete. Models saved to:", MODEL_DIR)
    print("=" * 60)


if __name__ == "__main__":
    run_training()
