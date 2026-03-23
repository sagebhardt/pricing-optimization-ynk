"""
First-markdown timing model.

Fixes the circularity problem in the main model: instead of predicting
"will discount continue?" (which is trivially learnable from discount history),
this model predicts WHEN a never-yet-discounted SKU-store should receive its
first markdown.

Framing: Survival-style classification.
- For each SKU-store, we observe the pre-discount selling period.
- At each week, the model predicts: "Is this the right week to start discounting?"
- Target: 1 if first markdown happens within the next 4 weeks, 0 otherwise.
- Training data: only weeks BEFORE the first discount event.

This is the prescriptive model — it tells the commercial team when to pull the trigger.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import roc_auc_score, average_precision_score, precision_score, recall_score
import shap
import pickle
import json
from pathlib import Path

PROCESSED_DIR = Path(__file__).parent.parent.parent / "data" / "processed"
RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"
MODEL_DIR = Path(__file__).parent.parent.parent / "models"


def build_first_markdown_dataset():
    """
    Build training data: only pre-discount periods.
    For each SKU-store, include all weeks from first sale to first discount.
    """
    features = pd.read_parquet(PROCESSED_DIR / "hoka_features.parquet")

    # Load enhanced features
    try:
        lifecycle = pd.read_parquet(PROCESSED_DIR / "lifecycle_stages.parquet")
        size_curves = pd.read_parquet(PROCESSED_DIR / "size_curve_tracking.parquet")
        sku_elasticity = pd.read_parquet(PROCESSED_DIR / "elasticity_by_sku.parquet")
        seasons = pd.read_parquet(PROCESSED_DIR / "derived_seasons.parquet")
        has_enhanced = True
    except FileNotFoundError:
        has_enhanced = False

    # Map SKU to parent
    products = pd.read_parquet(RAW_DIR / "hoka_products.parquet")
    sku_to_parent = products.set_index("material")["codigo_padre"].to_dict()
    features["codigo_padre"] = features["sku"].map(sku_to_parent)

    # Find first discount week per SKU-store
    discounted = features[features["has_discount"] == 1]
    first_discount = (
        discounted.groupby(["sku", "centro"])["week"]
        .min()
        .rename("first_discount_week")
        .reset_index()
    )

    # Merge
    df = features.merge(first_discount, on=["sku", "centro"], how="left")

    # Pre-discount period: weeks before first discount
    # Include SKUs that were never discounted (they're negative examples throughout)
    pre_discount = df[
        (df["first_discount_week"].isna())  # Never discounted
        | (df["week"] < df["first_discount_week"])  # Before first discount
    ].copy()

    # Target: will first discount happen within next 4 weeks?
    pre_discount["target_first_markdown_4w"] = 0
    has_disc = pre_discount["first_discount_week"].notna()
    weeks_to_disc = (pre_discount["first_discount_week"] - pre_discount["week"]).dt.days / 7
    pre_discount.loc[has_disc & (weeks_to_disc <= 4) & (weeks_to_disc > 0), "target_first_markdown_4w"] = 1

    # Enhanced features
    if has_enhanced:
        # Lifecycle stage
        lifecycle_feats = lifecycle[["codigo_padre", "centro", "week", "lifecycle_stage",
                                     "lifecycle_position"]].copy()
        lifecycle_feats["lifecycle_stage_code"] = lifecycle_feats["lifecycle_stage"].map(
            {"launch": 0, "growth": 1, "peak": 2, "steady": 3, "decline": 4, "clearance": 5}
        )
        pre_discount = pre_discount.merge(
            lifecycle_feats[["codigo_padre", "centro", "week", "lifecycle_stage_code", "lifecycle_position"]],
            on=["codigo_padre", "centro", "week"],
            how="left",
        )

        # Size curve
        pre_discount = pre_discount.merge(
            size_curves[["codigo_padre", "centro", "week", "attrition_rate",
                         "core_completeness", "fragmentation_index", "active_sizes_4w"]],
            on=["codigo_padre", "centro", "week"],
            how="left",
        )

        # Elasticity (parent level)
        elast_map = sku_elasticity.set_index("codigo_padre")["elasticity"].to_dict()
        pre_discount["price_elasticity"] = pre_discount["codigo_padre"].map(elast_map)

        # Season
        season_map = seasons.set_index("codigo_padre")["derived_season"].to_dict()
        pre_discount["derived_season"] = pre_discount["codigo_padre"].map(season_map)
        pre_discount["is_fw"] = (pre_discount["derived_season"] == "FW").astype(int)

    return pre_discount


def train_first_markdown_model():
    """Train the first-markdown timing model."""
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    print("Building first-markdown dataset...")
    df = build_first_markdown_dataset()

    target_col = "target_first_markdown_4w"
    print(f"  Total rows: {len(df):,}")
    print(f"  Positive rate: {df[target_col].mean():.3f}")
    print(f"  SKUs that eventually get discounted: {df[df['first_discount_week'].notna()]['sku'].nunique()}")
    print(f"  SKUs never discounted: {df[df['first_discount_week'].isna()]['sku'].nunique()}")

    # Feature selection — exclude leak-prone and identifier columns
    exclude = [
        "sku", "centro", "week", "codigo_padre", "first_sale_date",
        "first_discount_week", target_col,
        # Remove discount-history features (would leak since we're pre-discount)
        "has_discount", "discount_rate", "weeks_since_discount",
        "cumulative_disc_weeks", "disc_exposure_rate", "max_discount_rate",
        "total_discount", "total_list_value",
        # Remove future-looking targets
        "will_discount_4w", "future_max_disc_4w", "future_velocity_2w", "velocity_lift",
        # High cardinality
        "color1", "tercera_jerarquia", "derived_season",
    ]

    categorical = ["primera_jerarquia", "segunda_jerarquia", "genero", "grupo_etario"]
    for col in categorical:
        if col in df.columns:
            df[col] = df[col].astype("category").cat.codes

    feature_cols = [c for c in df.columns if c not in exclude]
    print(f"  Features: {len(feature_cols)}")

    X = df[feature_cols]
    y = df[target_col]
    weeks = df["week"]

    # Time-series CV
    print("\nTime-series cross-validation:")
    unique_weeks = sorted(weeks.unique())
    n_weeks = len(unique_weeks)
    fold_size = n_weeks // 5

    params = {
        "n_estimators": 300,
        "max_depth": 6,
        "learning_rate": 0.05,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "scale_pos_weight": (1 - y.mean()) / max(y.mean(), 0.001),
        "early_stopping_rounds": 20,
        "random_state": 42,
    }

    cv_results = []
    for fold in range(4):
        train_end = (fold + 1) * fold_size
        val_end = (fold + 2) * fold_size

        train_weeks = set(unique_weeks[:train_end])
        val_weeks = set(unique_weeks[train_end:val_end])

        train_mask = weeks.isin(train_weeks)
        val_mask = weeks.isin(val_weeks)

        X_train, y_train = X[train_mask], y[train_mask]
        X_val, y_val = X[val_mask], y[val_mask]

        if len(X_train) == 0 or len(X_val) == 0 or y_val.sum() == 0:
            continue

        model = xgb.XGBClassifier(**params)
        model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)

        y_pred_proba = model.predict_proba(X_val)[:, 1]
        y_pred = model.predict(X_val)

        auc = roc_auc_score(y_val, y_pred_proba)
        ap = average_precision_score(y_val, y_pred_proba)
        prec = precision_score(y_val, y_pred, zero_division=0)
        rec = recall_score(y_val, y_pred, zero_division=0)

        fold_result = {
            "fold": fold,
            "train_end": str(max(train_weeks).date()),
            "val_range": f"{min(val_weeks).date()} to {max(val_weeks).date()}",
            "val_size": len(X_val),
            "pos_rate": y_val.mean(),
            "auc": auc,
            "avg_precision": ap,
            "precision": prec,
            "recall": rec,
        }
        cv_results.append(fold_result)
        print(f"  Fold {fold}: AUC={auc:.3f}  AP={ap:.3f}  P={prec:.3f}  R={rec:.3f}  pos_rate={y_val.mean():.3f}")

    if cv_results:
        avg_auc = np.mean([r["auc"] for r in cv_results])
        avg_ap = np.mean([r["avg_precision"] for r in cv_results])
        print(f"\n  Average AUC: {avg_auc:.3f}")
        print(f"  Average Precision: {avg_ap:.3f}")
    else:
        avg_auc = 0
        avg_ap = 0

    # Train final model
    print("\nTraining final first-markdown model...")
    final_params = {k: v for k, v in params.items() if k != "early_stopping_rounds"}
    final_model = xgb.XGBClassifier(**final_params)
    final_model.fit(X, y, verbose=False)

    # SHAP
    print("Computing SHAP values...")
    n_sample = min(2000, len(X))
    X_sample = X.sample(n_sample, random_state=42)
    explainer = shap.TreeExplainer(final_model)
    shap_values = explainer.shap_values(X_sample)

    mean_shap = pd.DataFrame({
        "feature": feature_cols,
        "mean_abs_shap": np.abs(shap_values).mean(axis=0),
    }).sort_values("mean_abs_shap", ascending=False)

    print("\nTop 15 features (SHAP):")
    for _, row in mean_shap.head(15).iterrows():
        print(f"  {row['feature']:35s} {row['mean_abs_shap']:.4f}")

    # Save
    with open(MODEL_DIR / "first_markdown_model.pkl", "wb") as f:
        pickle.dump(final_model, f)
    mean_shap.to_csv(MODEL_DIR / "first_markdown_shap.csv", index=False)

    metadata = {
        "cv_results": cv_results,
        "avg_auc": avg_auc,
        "avg_precision": avg_ap,
        "features": feature_cols,
        "n_features": len(feature_cols),
        "n_samples": len(X),
        "positive_rate": float(y.mean()),
        "note": "Prescriptive model: predicts optimal first-markdown timing for undiscounted SKUs.",
    }
    with open(MODEL_DIR / "first_markdown_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2, default=str)

    print(f"\nModel saved to: {MODEL_DIR / 'first_markdown_model.pkl'}")
    return final_model, cv_results


if __name__ == "__main__":
    train_first_markdown_model()
